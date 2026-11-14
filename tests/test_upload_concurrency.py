"""上传并发与取消的回归测试。

覆盖修复目标：
- 慢暂存（校验/写盘被人为拖慢）期间，/health、游标分页、其他上传照常响应；
- 暂存期间取消（两种路径：uvicorn 的 http.disconnect 监听、框架直接取消协程）：
  未登记的文件不自行发布，临时文件被清理；
- 取消与登记相撞：已提交文件完整可读，未提交文件不出现在清单中；
- 并发上传下 seq_order/read_order 唯一，游标快照与全局行序保持不变。
"""
from __future__ import annotations

import asyncio
import io
import os
import threading
import time

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from starlette.requests import Request

from app import config, main, storage
from app.main import app
from fastapi.testclient import TestClient

sync_client = TestClient(app)


def _parquet(rows: int, tag: int) -> bytes:
    """每份文件用 tag 区分温度区间，便于在合并后的行流里识别来源文件。"""
    base = tag * 10_000
    stations = [f"ST-{(i + tag) % 4 + 1:03d}" for i in range(rows)]
    ts = [pa.scalar(base + i, type=pa.timestamp("us", tz="UTC")).as_py()
          for i in range(rows)]
    temps = [20.0 + (i % 7) + tag for i in range(rows)]
    table = pa.table({
        "station_id": pa.array(stations, pa.string()),
        "observed_at": pa.array(ts, pa.timestamp("us", tz="UTC")),
        "temperature": pa.array(temps, pa.float64()),
    })
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _make_upload_request(
    data: bytes, disconnect_on: asyncio.Event | None = None
) -> Request:
    """构造可直接传给 endpoint 函数的 Request。

    disconnect_on 被 set 后，下一次 receive 返回 http.disconnect，
    模拟 uvicorn 下客户端断开（生产服务器不会主动取消处理协程）。
    """
    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/uploads",
        "raw_path": b"/uploads",
        "query_string": b"",
        "headers": [(b"content-type", b"application/vnd.apache.parquet")],
        "client": ("127.0.0.1", 12345),
        "server": ("test", 80),
    }
    sent = False

    async def receive():
        nonlocal sent
        if not sent:
            sent = True
            return {"type": "http.request", "body": data, "more_body": False}
        if disconnect_on is not None:
            await disconnect_on.wait()
            return {"type": "http.disconnect"}
        await asyncio.sleep(3600)  # 不会再被读取；协程取消时被打断

    return Request(scope, receive)


def _tmp_files() -> set[str]:
    return {n for n in os.listdir(config.TMP_DIR) if n.endswith(".tmp")}


def _file_ids() -> set[str]:
    return {f["file_id"] for f in sync_client.get("/files").json()["files"]}


def _close_all_cursors() -> None:
    for c in sync_client.get("/cursors").json()["cursors"]:
        if not c["closed"]:
            sync_client.post(f"/cursors/{c['cursor_id']}/close")


async def _wait_tmp_restore(expected: set[str]) -> None:
    for _ in range(200):
        await asyncio.sleep(0.02)
        if _tmp_files() == expected:
            return
    raise AssertionError("临时文件未被清理")


def test_slow_staging_does_not_block_event_loop(monkeypatch):
    """慢暂存期间：/health、游标分页、同期上传都必须照常响应。"""
    # 准备：先有一份已提交文件和一个游标（用正常速度上传）
    r = sync_client.post("/uploads", content=_parquet(30, 501))
    assert r.status_code == 200
    cur = sync_client.post("/cursors").json()
    cid = cur["cursor_id"]

    entered = threading.Event()
    release = threading.Event()
    real_stage = storage.validate_and_stage

    def slow_stage(data: bytes):
        entered.set()
        # 模拟慢校验/慢磁盘：卡住暂存线程，直到测试放行
        assert release.wait(timeout=5), "测试未及时放行暂存线程"
        return real_stage(data)

    monkeypatch.setattr(storage, "validate_and_stage", slow_stage)

    async def scenario():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            upload1 = asyncio.create_task(
                client.post("/uploads", content=_parquet(20, 502))
            )
            while not entered.is_set():
                await asyncio.sleep(0.01)

            # 暂存线程卡住期间，事件循环必须仍然自由
            t0 = time.monotonic()
            r_health = await client.get("/health")
            r_page = await client.get(f"/cursors/{cid}/pages?limit=5")
            elapsed = time.monotonic() - t0
            assert r_health.status_code == 200
            assert r_health.json()["status"] == "ok"
            assert r_page.status_code == 200
            assert r_page.json()["returned"] == 5
            # 未修复时这里会卡住数秒（事件循环被暂存线程的 sleep 阻塞）
            assert elapsed < 2, f"慢暂存阻塞了事件循环: {elapsed:.2f}s"

            # 同期第二个上传也能进入暂存（线程池并行，不排队等第一个）
            upload2 = asyncio.create_task(
                client.post("/uploads", content=_parquet(20, 503))
            )
            t0 = time.monotonic()
            r_health2 = await client.get("/health")
            assert r_health2.status_code == 200
            assert time.monotonic() - t0 < 2

            release.set()
            r1, r2 = await asyncio.gather(upload1, upload2)
            assert r1.status_code == 200, r1.text
            assert r2.status_code == 200, r2.text
            assert r1.json()["file_id"] != r2.json()["file_id"]

    asyncio.run(scenario())
    sync_client.post(f"/cursors/{cid}/close")


def test_disconnect_during_staging_never_registers(monkeypatch):
    """暂存期间客户端断开（http.disconnect）：不登记、不发布、tmp 清理。"""
    entered = threading.Event()
    release = threading.Event()
    real_stage = storage.validate_and_stage

    def slow_stage(data: bytes):
        entered.set()
        assert release.wait(timeout=5), "测试未及时放行暂存线程"
        return real_stage(data)

    monkeypatch.setattr(storage, "validate_and_stage", slow_stage)
    files_before = _file_ids()
    tmp_before = _tmp_files()

    async def scenario():
        disconnect = asyncio.Event()
        request = _make_upload_request(_parquet(15, 601), disconnect_on=disconnect)
        task = asyncio.create_task(main.upload_parquet(request))
        while not entered.is_set():
            await asyncio.sleep(0.01)
        # 暂存处理期间客户端断开
        disconnect.set()
        result = await task
        assert result.status_code == 499
        # 放行后台暂存线程：它会正常产出 tmp，由清理回调负责删除
        release.set()
        await _wait_tmp_restore(tmp_before)

    asyncio.run(scenario())

    # 未登记的文件没有出现在清单中，也没有稍后自行发布
    assert _file_ids() == files_before
    assert _tmp_files() == tmp_before

    # 服务不受影响，后续上传与分页正常
    r = sync_client.post("/uploads", content=_parquet(10, 602))
    assert r.status_code == 200
    cur = sync_client.post("/cursors").json()
    page = sync_client.get(
        f"/cursors/{cur['cursor_id']}/pages?limit=3"
    ).json()
    assert page["returned"] == 3
    sync_client.post(f"/cursors/{cur['cursor_id']}/close")


def test_task_cancel_during_staging_cleans_up(monkeypatch):
    """暂存期间协程被直接取消（hypercorn/测试客户端路径）：同样不登记、清理。"""
    entered = threading.Event()
    release = threading.Event()
    real_stage = storage.validate_and_stage

    def slow_stage(data: bytes):
        entered.set()
        assert release.wait(timeout=5), "测试未及时放行暂存线程"
        return real_stage(data)

    monkeypatch.setattr(storage, "validate_and_stage", slow_stage)
    files_before = _file_ids()
    tmp_before = _tmp_files()

    async def scenario():
        task = asyncio.create_task(
            main.upload_parquet(_make_upload_request(_parquet(15, 603)))
        )
        while not entered.is_set():
            await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        release.set()
        await _wait_tmp_restore(tmp_before)

    asyncio.run(scenario())
    assert _file_ids() == files_before
    assert _tmp_files() == tmp_before


def test_disconnect_racing_registration_commits_atomically(monkeypatch):
    """断开与登记相撞（登记线程已进入临界区）：已提交文件完整可读。"""
    entered = threading.Event()
    release = threading.Event()
    real_next_seq = main.next_file_seq

    def slow_next_seq(conn):
        # 在登记临界区内（cancel 检查之后）阻塞，模拟慢事务
        entered.set()
        assert release.wait(timeout=5), "测试未及时放行登记线程"
        return real_next_seq(conn)

    monkeypatch.setattr(main, "next_file_seq", slow_next_seq)
    files_before = _file_ids()

    async def scenario():
        disconnect = asyncio.Event()
        request = _make_upload_request(_parquet(15, 701), disconnect_on=disconnect)
        task = asyncio.create_task(main.upload_parquet(request))
        while not entered.is_set():
            await asyncio.sleep(0.01)
        # 登记线程已在临界区内：断开打不断它，登记会原子地跑完
        disconnect.set()
        result = await task
        assert result.status_code == 499
        release.set()
        for _ in range(200):
            await asyncio.sleep(0.02)
            if _file_ids() - files_before:
                break
        else:
            raise AssertionError("登记线程未完成提交")

    asyncio.run(scenario())

    new_ids = _file_ids() - files_before
    assert len(new_ids) == 1
    listing = sync_client.get("/files").json()["files"]
    committed = next(f for f in listing if f["file_id"] in new_ids)
    assert committed["status"] == "active"
    assert committed["row_count"] == 15
    assert os.path.exists(
        os.path.join(config.FILES_DIR, committed["file_name"])
    )

    # 已提交文件完整可读：新游标能读到它的全部 15 行
    cur = sync_client.post("/cursors").json()
    rows = []
    while True:
        p = sync_client.get(
            f"/cursors/{cur['cursor_id']}/pages?limit=100"
        ).json()
        rows.extend(p["rows"])
        if p["exhausted"]:
            break
    assert len(rows) == cur["snapshot_rows"]
    assert sum(1 for r in rows if 20 + 701 <= r["temperature"] < 20 + 701 + 7) == 15
    sync_client.post(f"/cursors/{cur['cursor_id']}/close")


def test_task_cancel_racing_registration_commits_atomically(monkeypatch):
    """协程取消与登记相撞（CancelledError 路径）：已提交文件同样完整可读。"""
    entered = threading.Event()
    release = threading.Event()
    real_next_seq = main.next_file_seq

    def slow_next_seq(conn):
        entered.set()
        assert release.wait(timeout=5), "测试未及时放行登记线程"
        return real_next_seq(conn)

    monkeypatch.setattr(main, "next_file_seq", slow_next_seq)
    files_before = _file_ids()

    async def scenario():
        task = asyncio.create_task(
            main.upload_parquet(_make_upload_request(_parquet(15, 702)))
        )
        while not entered.is_set():
            await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        release.set()
        for _ in range(200):
            await asyncio.sleep(0.02)
            if _file_ids() - files_before:
                break
        else:
            raise AssertionError("登记线程未完成提交")

    asyncio.run(scenario())

    new_ids = _file_ids() - files_before
    assert len(new_ids) == 1
    listing = sync_client.get("/files").json()["files"]
    committed = next(f for f in listing if f["file_id"] in new_ids)
    assert committed["status"] == "active"
    assert committed["row_count"] == 15


def test_cancel_before_registration_leaves_no_trace():
    """取消先于登记生效：登记线程放弃，tmp 清理，清单不出现该文件。"""
    table, row_count, tmp_path = storage.validate_and_stage(_parquet(15, 703))
    assert os.path.exists(tmp_path)
    files_before = _file_ids()

    cancel_event = threading.Event()
    cancel_event.set()  # 请求在登记生效前已被取消
    result = main._register_staged_file(tmp_path, row_count, cancel_event)

    assert result is None
    assert not os.path.exists(tmp_path)
    assert _file_ids() == files_before


def test_concurrent_uploads_keep_order_and_snapshot(monkeypatch):
    """并发上传：序号唯一、游标快照固定、全局行序按 read_order 拼接。"""
    _close_all_cursors()
    sync_client.post("/merges/reclaim")
    # 用配置钩子给所有暂存加延迟，制造真实的并发窗口
    monkeypatch.setattr(config, "UPLOAD_STAGE_DELAY_SECONDS", 0.3)

    cur = sync_client.post("/cursors").json()
    snap_rows = cur["snapshot_rows"]

    tags = [801, 802, 803, 804]

    async def scenario():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            tasks = [
                asyncio.create_task(
                    client.post("/uploads", content=_parquet(10, tag))
                )
                for tag in tags
            ]
            return await asyncio.gather(*tasks)

    responses = asyncio.run(scenario())
    assert all(r.status_code == 200 for r in responses)
    infos = [r.json() for r in responses]  # 与 tags 一一对应

    # seq_order / read_order 全局唯一且连续分配
    assert len({i["seq_order"] for i in infos}) == len(tags)
    assert len({i["read_order"] for i in infos}) == len(tags)

    # 旧游标快照不受并发上传影响
    page = sync_client.get(
        f"/cursors/{cur['cursor_id']}/pages?limit=1"
    ).json()
    assert page["total_rows"] == snap_rows
    sync_client.post(f"/cursors/{cur['cursor_id']}/close")

    # 新游标看到全部行；新上传的 40 行位于尾部且按 read_order 顺序拼接
    cur2 = sync_client.post("/cursors").json()
    assert cur2["snapshot_rows"] == snap_rows + 10 * len(tags)
    rows = []
    while True:
        p = sync_client.get(
            f"/cursors/{cur2['cursor_id']}/pages?limit=7"
        ).json()
        rows.extend(p["rows"])
        if p["exhausted"]:
            break
    assert len(rows) == cur2["snapshot_rows"]

    tail = rows[-10 * len(tags):]
    by_read_order = sorted(zip(tags, infos), key=lambda t: t[1]["read_order"])
    pos = 0
    for tag, _info in by_read_order:
        segment = tail[pos:pos + 10]
        assert len(segment) == 10
        assert all(
            20 + tag <= r["temperature"] < 20 + tag + 7 for r in segment
        ), f"read_order={_info['read_order']} 的文件行序错乱"
        pos += 10
    sync_client.post(f"/cursors/{cur2['cursor_id']}/close")
