"""FastAPI 入口：上传、分页、合并、游标生命周期。"""
from __future__ import annotations

import asyncio
import contextlib
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse

from . import config, cursors, storage
from .db import LOCK, get_conn, next_file_seq
from .merger import MergeConflict, manager
from .storage import ParquetValidationError

# 上传暂存专用线程池：Parquet 校验/规范化/写 tmp 是 CPU+IO 密集操作，
# 全部放到这里执行，事件循环全程自由；与 anyio 默认线程池（同步端点用）
# 相互隔离，上传风暴不会挤占 /health、游标分页等请求。
_STAGE_EXECUTOR = ThreadPoolExecutor(
    max_workers=config.UPLOAD_STAGE_WORKERS,
    thread_name_prefix="upload-stage",
)


@contextlib.asynccontextmanager
async def lifespan(_app: FastAPI):
    # 启动：初始化 schema/目录并执行崩溃恢复
    get_conn()
    _app.state.recovery = manager.recover_on_startup(
        auto_merge=config.AUTO_MERGE_ON_START
    )
    yield
    # 关停：等待后台合并线程退出；排队中的暂存任务取消，运行中的随进程退出，
    # 遗留的 tmp 半成品由下次启动恢复清理
    for t in list(manager._threads.values()):  # noqa: SLF001
        t.join(timeout=5)
    _STAGE_EXECUTOR.shutdown(wait=False, cancel_futures=True)


app = FastAPI(
    title="气象站小文件入湖与合并服务",
    version="1.0.0",
    lifespan=lifespan,
    description=(
        "单进程湖仓维护 API：追加上传 ≤8MiB 的 Parquet（固定 "
        "station_id / observed_at(UTC) / temperature 三列），"
        "按上传顺序与文件内行序分页读取；后台把开始时的文件合并为单个 "
        "Parquet，原子切换，旧文件等游标读完或关闭后回收，全程崩溃可恢复。"
    ),
)


@app.exception_handler(ParquetValidationError)
def _validation_handler(_request: Request, exc: ParquetValidationError):
    return JSONResponse(status_code=422, content={"detail": str(exc)})


@app.exception_handler(MergeConflict)
def _conflict_handler(_request: Request, exc: MergeConflict):
    return JSONResponse(status_code=409, content={"detail": str(exc)})


@app.exception_handler(cursors.CursorError)
def _cursor_handler(_request: Request, exc: cursors.CursorError):
    # “已关闭/不存在”语义统一用 404
    return JSONResponse(status_code=404, content={"detail": str(exc)})


@app.get("/health", tags=["meta"], summary="健康检查与启动恢复结果")
def health() -> dict:
    return {"status": "ok", "recovery": getattr(app.state, "recovery", None)}


# ------------------------------- 上传 -------------------------------

def _cleanup_unregistered_tmp(fut: "asyncio.Future") -> None:
    """暂存线程跑完但请求已被取消：删除永远不会被登记的临时文件。"""
    try:
        _table, _row_count, tmp_path = fut.result()
    except Exception:
        # 暂存失败：validate_and_stage 内部已保证不留 tmp 半成品
        return
    with contextlib.suppress(OSError):
        os.remove(tmp_path)


def _silence_future(fut: "asyncio.Future") -> None:
    """取回后台 future 的异常，避免事件循环记录“异常未被取回”告警。"""
    if not fut.cancelled():
        fut.exception()


def _register_staged_file(
    tmp_path: str, row_count: int, cancel_event: threading.Event
) -> dict | None:
    """把暂存文件登记为正式文件（在上传线程池线程中执行）。

    整个临界区在进程锁内原子完成：
    - 请求已取消且登记尚未生效：不登记，清理 tmp，返回 None；
    - 否则 os.replace + 元数据事务一次提交，文件立即可读；
      事务失败回滚并清理，绝不留下半成品。
    """
    conn = get_conn()
    with LOCK:
        if cancel_event.is_set():
            # 取消抢先到达：未登记的文件不得稍后自行发布
            with contextlib.suppress(OSError):
                os.remove(tmp_path)
            return None
        seq = next_file_seq(conn)
        # 新文件的读取位置排在所有现存文件之后
        max_order = conn.execute(
            "SELECT COALESCE(MAX(read_order), -1) AS m FROM files"
        ).fetchone()["m"]
        read_order = max_order + 1
        file_uid = uuid.uuid4().hex
        final_name = f"{seq:012d}-{file_uid}.parquet"
        final_path = os.path.join(config.FILES_DIR, final_name)
        os.replace(tmp_path, final_path)
        size = os.path.getsize(final_path)
        try:
            conn.execute(
                """INSERT INTO files
                   (file_id, kind, path, size_bytes, row_count, seq_order,
                    read_order, status)
                   VALUES (?, 'upload', ?, ?, ?, ?, ?, 'active')""",
                (file_uid, final_path, size, row_count, seq, read_order),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            with contextlib.suppress(OSError):
                os.remove(final_path)
            raise
    return {
        "file_id": file_uid,
        "seq_order": seq,
        "read_order": read_order,
        "size_bytes": size,
        "row_count": row_count,
    }


def _client_gone_response() -> JSONResponse:
    # 客户端已断开，响应无法送达；499 语义仅供服务端日志与测试观察
    return JSONResponse(
        status_code=499, content={"detail": "客户端已断开，上传未登记"}
    )


@app.post(
    "/uploads",
    tags=["upload"],
    summary="追加上传一份 Parquet（≤8MiB）",
    openapi_extra={
        # 让 Swagger UI 渲染文件选择器，/docs 里可直接上传二进制 Parquet
        "requestBody": {
            "required": True,
            "content": {
                "application/vnd.apache.parquet": {
                    "schema": {"type": "string", "format": "binary"}
                },
                "application/octet-stream": {
                    "schema": {"type": "string", "format": "binary"}
                },
            },
        }
    },
)
async def upload_parquet(request: Request) -> dict:
    """按字节流式接收，超过 8MiB 立即拒绝（413），不落盘多余字节。

    校验/规范化/暂存与正式登记都在上传线程池中执行，慢处理期间
    /health、游标分页和其他上传照常响应。请求在暂存期间被取消时，
    未登记的文件不会稍后自行发布，临时文件由回调清理；取消与登记
    相撞时，登记临界区原子地完成——已提交文件完整可读，未提交文件
    不出现在清单中。合并进行中也可以正常上传，新文件不会被进行中
    的合并吞掉。
    """
    chunks: list[bytes] = []
    total = 0
    too_large = False
    async for chunk in request.stream():
        total += len(chunk)
        if total > config.MAX_UPLOAD_BYTES:
            too_large = True
            break
        chunks.append(chunk)
    if too_large:
        return JSONResponse(
            status_code=413,
            content={
                "detail": f"上传体超过 {config.MAX_UPLOAD_BYTES} 字节（8MiB）上限",
                "received_bytes": total,
            },
        )

    data = b"".join(chunks)
    if not data:
        return JSONResponse(
            status_code=422, content={"detail": "上传体为空"}
        )

    loop = asyncio.get_running_loop()

    # 断开监听：uvicorn 不会主动取消处理中的协程，客户端断开只能通过
    # receive() 返回 http.disconnect 感知。body 已读完，此监听与后续的
    # 暂存/登记等待竞速；框架直接取消协程（CancelledError）的情形同样覆盖。
    disconnect_event = asyncio.Event()

    async def _watch_disconnect() -> None:
        while True:
            try:
                message = await request.receive()
            except Exception:  # noqa: BLE001 - 接收通道异常：放弃监听，不影响主流程
                return
            if message["type"] == "http.disconnect":
                disconnect_event.set()
                return

    watcher = asyncio.create_task(_watch_disconnect())
    try:
        # ---- 暂存：线程池执行，事件循环不被校验/写盘阻塞 ----
        stage_future = loop.run_in_executor(
            _STAGE_EXECUTOR, storage.validate_and_stage, data
        )
        stage_disconnect = asyncio.create_task(disconnect_event.wait())
        try:
            await asyncio.wait(
                [stage_future, stage_disconnect],
                return_when=asyncio.FIRST_COMPLETED,
            )
        except asyncio.CancelledError:
            # 协程被服务器/框架取消：后台线程不可取消，跑完后由回调
            # 删除未登记的 tmp；文件绝不进入登记流程
            stage_future.add_done_callback(_cleanup_unregistered_tmp)
            raise
        finally:
            stage_disconnect.cancel()
        if disconnect_event.is_set():
            # 暂存期间客户端断开：不登记；tmp 由回调在线程跑完后清理
            stage_future.add_done_callback(_cleanup_unregistered_tmp)
            return _client_gone_response()
        table, row_count, tmp_path = stage_future.result()

        # ---- 登记：原子临界区，同样移出事件循环 ----
        cancel_event = threading.Event()
        if disconnect_event.is_set():
            # 暂存完成后、登记启动前发现客户端已断开
            with contextlib.suppress(OSError):
                os.remove(tmp_path)
            return _client_gone_response()
        register_future = loop.run_in_executor(
            _STAGE_EXECUTOR,
            _register_staged_file,
            tmp_path,
            row_count,
            cancel_event,
        )
        register_disconnect = asyncio.create_task(disconnect_event.wait())
        try:
            await asyncio.wait(
                [register_future, register_disconnect],
                return_when=asyncio.FIRST_COMPLETED,
            )
        except asyncio.CancelledError:
            # 通知登记线程：尚未生效的登记必须放弃并清理 tmp；
            # 已开始的登记不可打断，会原子地提交或放弃
            cancel_event.set()
            register_future.add_done_callback(_silence_future)
            raise
        finally:
            register_disconnect.cancel()
        if disconnect_event.is_set():
            # 取消与登记相撞：登记线程原子地提交（文件完整可读）
            # 或放弃（不出现在清单中），绝不留下半成品
            cancel_event.set()
            register_future.add_done_callback(_silence_future)
            return _client_gone_response()
        registered = register_future.result()
        if registered is None:
            # 登记线程发现请求已取消而放弃（防御性分支）
            return _client_gone_response()
        return {
            **registered,
            "columns": list(table.column_names),
            "status": "active",
        }
    finally:
        watcher.cancel()


@app.get("/files", tags=["upload"], summary="查看文件清单（含合并产物与 obsolete 状态）")
def list_files() -> dict:
    conn = get_conn()
    with LOCK:
        rows = conn.execute(
            """SELECT id, file_id, kind, path, size_bytes, row_count, seq_order,
                      read_order, status, created_at, obsolete_at
               FROM files ORDER BY read_order, seq_order"""
        ).fetchall()
    return {
        "files": [
            {
                "file_pk": r["id"],
                "file_id": r["file_id"],
                "file_name": os.path.basename(r["path"]),
                "kind": r["kind"],
                "size_bytes": r["size_bytes"],
                "row_count": r["row_count"],
                "seq_order": r["seq_order"],
                "read_order": r["read_order"],
                "status": r["status"],
                "created_at": r["created_at"],
                "obsolete_at": r["obsolete_at"],
            }
            for r in rows
        ]
    }


# ------------------------------- 游标 -------------------------------

@app.post("/cursors", tags=["cursor"], summary="创建分页游标（固定当前已提交行）")
def create_cursor() -> dict:
    return cursors.create_cursor()


@app.get("/cursors", tags=["cursor"], summary="列出全部游标及其读取进度")
def get_cursors() -> dict:
    return cursors.list_cursors()


@app.get(
    "/cursors/{cursor_id}/pages",
    tags=["cursor"],
    summary="按上传顺序 + 文件内行序翻页（保留重复行）",
)
def read_page(
    cursor_id: str,
    limit: int = Query(
        config.DEFAULT_PAGE_LIMIT,
        ge=1,
        le=config.MAX_PAGE_LIMIT,
        description="本页最多返回多少行",
    ),
) -> dict:
    return cursors.next_page(cursor_id, limit)


@app.post(
    "/cursors/{cursor_id}/close",
    tags=["cursor"],
    summary="显式关闭游标，可能触发无引用旧文件回收",
)
def close_cursor(cursor_id: str) -> dict:
    return cursors.close_cursor(cursor_id)


# ------------------------------- 合并 -------------------------------

@app.post(
    "/merges",
    tags=["merge"],
    summary="启动一次后台合并（同期只合并一次）",
    status_code=202,
)
def start_merge() -> dict:
    return manager.start_merge()


@app.get("/merges/status", tags=["merge"], summary="查看最近一次合并任务状态")
def merge_status() -> dict:
    return manager.status()


@app.post("/merges/pause", tags=["merge"], summary="暂停后台合并（演示用）")
def pause_merge() -> dict:
    return manager.pause()


@app.post("/merges/resume", tags=["merge"], summary="恢复已暂停的合并")
def resume_merge() -> dict:
    return manager.resume()


@app.post("/merges/reclaim", tags=["merge"], summary="立即回收无游标引用的 obsolete 文件")
def reclaim_now() -> dict:
    return manager.reclaim()


@app.post(
    "/merges/fail-inject/{step}",
    tags=["merge"],
    summary="演示用：在下一次合并的 stream 或 switch 阶段注入故障",
)
def fail_inject(step: str) -> dict:
    if step not in ("stream", "switch"):
        return JSONResponse(
            status_code=422,
            content={"detail": "step 只能是 stream 或 switch"},
        )
    conn = get_conn()
    with LOCK:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES ('fail_inject', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (step,),
        )
        conn.commit()
    return {"fail_inject": step, "note": "将在下一次合并触发，触发后自动清除"}
