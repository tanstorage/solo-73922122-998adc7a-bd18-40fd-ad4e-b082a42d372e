"""FastAPI 入口：上传、分页、合并、游标生命周期。"""
from __future__ import annotations

import contextlib
import os
import uuid

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse

from . import config, cursors, storage
from .db import LOCK, get_conn, next_file_seq
from .merger import MergeConflict, manager
from .storage import ParquetValidationError


@contextlib.asynccontextmanager
async def lifespan(_app: FastAPI):
    # 启动：初始化 schema/目录并执行崩溃恢复
    get_conn()
    _app.state.recovery = manager.recover_on_startup(
        auto_merge=config.AUTO_MERGE_ON_START
    )
    yield
    # 关停：等待后台合并线程退出
    for t in list(manager._threads.values()):  # noqa: SLF001
        t.join(timeout=5)


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

@app.post("/uploads", tags=["upload"], summary="追加上传一份 Parquet（≤8MiB）")
async def upload_parquet(request: Request) -> dict:
    """按字节流式接收，超过 8MiB 立即拒绝（413），不落盘多余字节。

    校验固定三列并规范化后写入 files/，元数据在事务中登记；
    合并进行中也可以正常上传，新文件不会被进行中的合并吞掉。
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

    # CPU/IO 部分在线程池外直接做（单进程，锁内时间很短）
    table, row_count, tmp_path = storage.validate_and_stage(data)

    conn = get_conn()
    with LOCK:
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
            if os.path.exists(final_path):
                os.remove(final_path)
            raise

    return {
        "file_id": file_uid,
        "seq_order": seq,
        "read_order": read_order,
        "size_bytes": size,
        "row_count": row_count,
        "columns": list(table.column_names),
        "status": "active",
    }


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
