"""核心语义测试：上传校验、游标快照、合并切换、回收安全、崩溃恢复。"""
from __future__ import annotations

import io
import os
import time

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi.testclient import TestClient

from app import config
from app.main import app
from app.merger import manager


def _parquet(rows: int, tag: int, dup_from: pa.Table | None = None) -> bytes:
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
    if dup_from is not None:
        head = dup_from.slice(dup_from.num_rows - 2, 2)
        table = pa.concat_tables([head, table.slice(2, rows - 2)])
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue(), table


client = TestClient(app)


def _wait_merge(state, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        m = client.get("/merges/status").json()["active_merge"]
        if m and m["state"] == state:
            return m
        time.sleep(0.1)
    raise AssertionError("merge never reached %s" % state)


def _wait_terminal(timeout=30):
    """等所有后台合并任务走到终态，消除测试间的线程竞争。"""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        m = client.get("/merges/status").json()["active_merge"]
        last = m
        if m is None or m["state"] in ("completed", "failed", "cancelled"):
            return m
        time.sleep(0.1)
    raise AssertionError("merge did not settle, last=%s" % last)


def _start_merge_retry():
    """启动合并；若残留活动任务则等其终态后重试。"""
    for _ in range(3):
        r = client.post("/merges")
        if r.status_code == 202:
            return r
        assert r.status_code == 409, r.json()
        _wait_terminal()
    status = client.get("/merges/status").json()
    files = client.get("/files").json()["files"]
    raise AssertionError(
        f"无法启动新的合并任务 status={status} "
        f"active_files={[f['file_id'] for f in files if f['status']=='active']}")


def _active_files():
    return [f for f in client.get("/files").json()["files"]
            if f["status"] == "active"]


def _close_all_cursors():
    """关闭全部遗留游标，消除测试间的回收引用耦合。"""
    for c in client.get("/cursors").json()["cursors"]:
        if not c["closed"]:
            client.post(f"/cursors/{c['cursor_id']}/close")


def test_health_and_empty_listing():
    r = client.get("/health")
    assert r.status_code == 200
    assert client.get("/files").json()["files"] == []


def test_upload_validation_errors():
    # 非 parquet
    r = client.post("/uploads", content=b"not parquet")
    assert r.status_code == 422
    # 空 body
    assert client.post("/uploads", content=b"").status_code == 422
    # 缺列
    bad = pa.table({"station_id": pa.array(["a"]),
                    "observed_at": pa.array(
                        [pa.scalar(1, type=pa.timestamp("us", tz="UTC")).as_py()],
                        pa.timestamp("us", tz="UTC"))})
    buf = io.BytesIO()
    pq.write_table(bad, buf)
    assert client.post("/uploads", content=buf.getvalue()).status_code == 422
    # 多余列
    bad2 = pa.table({
        "station_id": pa.array(["a"]),
        "observed_at": pa.array(
            [pa.scalar(1, type=pa.timestamp("us", tz="UTC")).as_py()],
            pa.timestamp("us", tz="UTC")),
        "temperature": pa.array([1.0]),
        "extra": pa.array([1]),
    })
    buf = io.BytesIO()
    pq.write_table(bad2, buf)
    assert client.post("/uploads", content=buf.getvalue()).status_code == 422
    # 空表
    empty = bad2.schema.empty_table()
    buf = io.BytesIO()
    pq.write_table(empty, buf)
    assert client.post("/uploads", content=buf.getvalue()).status_code == 422


def test_size_limit_413(monkeypatch):
    monkeypatch.setattr(config, "MAX_UPLOAD_BYTES", 64)
    data, _ = _parquet(50, 1)
    r = client.post("/uploads", content=data)
    assert r.status_code == 413
    monkeypatch.setattr(config, "MAX_UPLOAD_BYTES", 8 * 1024 * 1024)


def test_full_flow_cursor_merge_reclaim():
    # 清理前序测试遗留的游标/obsolete 文件，保证本测试的计数断言精确
    _close_all_cursors()
    client.post("/merges/reclaim")
    # 上传 4 个文件，相邻文件含 2 行重复
    total = 0
    prev = None
    for tag in range(4):
        data, table = _parquet(50, tag, dup_from=prev)
        prev = table
        r = client.post("/uploads", content=data)
        assert r.status_code == 200
        total += r.json()["row_count"]
    assert total == 200

    # 游标快照固定
    cur = client.post("/cursors").json()
    cid = cur["cursor_id"]
    assert cur["snapshot_rows"] == total

    # 分页 + 重复行保留
    rows = []
    while True:
        page = client.get(f"/cursors/{cid}/pages?limit=37").json()
        rows.extend(page["rows"])
        if page["exhausted"]:
            break
    assert len(rows) == total
    keys = [(r["station_id"], r["observed_at"], r["temperature"]) for r in rows]
    assert len(keys) - len(set(keys)) >= 6  # 3 个接缝 x 2 行重复

    # 第二个游标，合并期间上传的文件不进旧快照
    cur2 = client.post("/cursors").json()
    cid2 = cur2["cursor_id"]

    # 启动合并（同期只能一个）
    assert client.post("/merges").status_code == 202
    assert client.post("/merges").status_code == 409
    m = _wait_merge("completed")
    assert m["output_file_id"]

    # 合并期间/之后上传新文件
    data, extra_table = _parquet(30, 99)
    extra = client.post("/uploads", content=data).json()
    assert extra["row_count"] == 30

    files = client.get("/files").json()["files"]
    active = [f for f in files if f["status"] == "active"]
    obsolete = [f for f in files if f["status"] == "obsolete"]
    merged = [f for f in active if f["kind"] == "merged"]
    assert len(merged) == 1 and merged[0]["row_count"] == total
    assert len(obsolete) == 4

    # 旧游标仍然完整可读（obsolete 文件未回收），内容不变
    old_rows = []
    while True:
        p = client.get(f"/cursors/{cid2}/pages?limit=64").json()
        old_rows.extend(p["rows"])
        if p["exhausted"]:
            break
    assert old_rows == rows

    # 新游标看到 merged + 新文件，且合并段行序与旧游标一致
    cur3 = client.post("/cursors").json()
    assert cur3["snapshot_rows"] == total + 30
    cid3 = cur3["cursor_id"]
    new_rows = []
    while True:
        p = client.get(f"/cursors/{cid3}/pages?limit=128").json()
        new_rows.extend(p["rows"])
        if p["exhausted"]:
            break
    assert new_rows[:total] == rows

    # 旧游标未关闭时 reclaim 不能删 obsolete 文件
    rec = client.post("/merges/reclaim").json()
    assert rec["reclaimed"] == []
    disk = set(os.listdir(config.FILES_DIR))
    assert len(disk) == 6  # 4 obsolete + 1 merged + 1 新上传

    section_step = None
    # 关闭全部旧游标后回收，active 文件不受影响
    r1 = client.post(f"/cursors/{cid}/close").json()
    r2 = client.post(f"/cursors/{cid2}/close").json()
    r3 = client.post(f"/cursors/{cid3}/close").json()
    rec = client.post("/merges/reclaim").json()
    reclaimed = set(r1["reclaimed"] + r2["reclaimed"] + r3["reclaimed"]
                    + rec["reclaimed"])
    assert len(reclaimed) == 4
    remaining = client.get("/files").json()["files"]
    assert all(f["status"] == "active" for f in remaining)
    disk_after = set(os.listdir(config.FILES_DIR))
    assert disk_after == {f["file_name"] for f in remaining}


def test_pagination_across_file_boundaries():
    """页大小恰好等于文件行数时，必须跨文件继续读完，不能在边界提前耗尽。"""
    _wait_terminal()
    _close_all_cursors()
    client.post("/merges/reclaim")
    per_file = 40
    n_files = 3
    for tag in range(n_files):
        data, _ = _parquet(per_file, tag + 200)
        client.post("/uploads", content=data)
    created_rows = per_file * n_files
    snap = client.post("/cursors").json()
    cid = snap["cursor_id"]
    total = snap["snapshot_rows"]
    assert total >= created_rows

    # 每页正好一个文件的行数，验证跨文件边界不会提前耗尽或多出空页
    rows = []
    while True:
        p = client.get(f"/cursors/{cid}/pages?limit={per_file}").json()
        rows.extend(p["rows"])
        if p["exhausted"]:
            # 最后一页之外不允许出现空页
            assert p["returned"] > 0
            break
        assert p["returned"] == per_file
    assert len(rows) == total

    # limit=1 逐行翻页也必须完整且行序一致
    cid2 = client.post("/cursors").json()["cursor_id"]
    rows2 = []
    while True:
        p = client.get(f"/cursors/{cid2}/pages?limit=1").json()
        rows2.extend(p["rows"])
        if p["exhausted"]:
            break
    assert len(rows2) == total
    assert rows2 == rows
    _close_all_cursors()


def test_pause_resume_and_concurrent_upload():
    _wait_terminal()
    # 当前 active：merged + 额外上传，再来一轮合并并暂停
    client.post("/merges/pause")  # 没有任务时 409 可接受
    r = _start_merge_retry()
    assert r.status_code == 202
    pr = client.post("/merges/pause")
    assert pr.status_code == 200
    time.sleep(0.3)
    # 暂停期间上传仍可用
    data, _ = _parquet(10, 33)
    assert client.post("/uploads", content=data).status_code == 200
    client.post("/merges/resume")
    _wait_merge("completed")


def test_fail_injection_keeps_original_data():
    _wait_terminal()
    _close_all_cursors()
    before = client.get("/files").json()["files"]
    before_merged = [f["file_id"] for f in before if f["kind"] == "merged"]
    client.post("/merges/fail-inject/switch")
    r = _start_merge_retry()
    assert r.status_code == 202
    m = _wait_merge("failed")
    assert m["fail_at_step"] == "switch"
    after = client.get("/files").json()["files"]
    # 失败不改变文件集合，也不产生新的 merged 产物
    assert [f["file_id"] for f in after] == [f["file_id"] for f in before]
    assert all(f["status"] == "active" for f in after)
    assert [f["file_id"] for f in after if f["kind"] == "merged"] == before_merged
    # tmp 被清理
    assert not [n for n in os.listdir(config.TMP_DIR) if n.endswith(".tmp")]

    # 再注入 stream 故障：同样不得改变文件集合
    _wait_terminal()
    client.post("/merges/fail-inject/stream")
    _start_merge_retry()
    _wait_merge("failed")
    after2 = client.get("/files").json()["files"]
    assert [f["file_id"] for f in after2] == [f["file_id"] for f in before]
    assert all(f["status"] == "active" for f in after2)

    # 清除后合并成功
    _wait_terminal()
    _start_merge_retry()
    _wait_merge("completed")
    _wait_terminal()


def test_restart_recovery_open_cursor_and_reclaim():
    """模拟进程重启：重新执行 startup 恢复钩子，校验游标与回收。"""
    _wait_terminal()
    # 保证至少有一个 active 文件可供新游标快照
    if not _active_files():
        data, _ = _parquet(25, 77)
        client.post("/uploads", content=data)
    # 先建一个引用当前 active 文件的游标并读一部分，保持打开
    cur = client.post("/cursors").json()
    cid = cur["cursor_id"]
    total = cur["snapshot_rows"]
    assert total > 0
    page = client.get(f"/cursors/{cid}/pages?limit=11").json()
    assert page["returned"] == min(11, total)

    # 重启恢复
    result = manager.recover_on_startup(auto_merge=False)
    assert isinstance(result["reclaimed"], list)

    # 游标仍可续读且总量不变
    got = page["rows_read"]
    while True:
        p = client.get(f"/cursors/{cid}/pages?limit=97").json()
        got += p["returned"]
        if p["exhausted"]:
            break
    assert got == total

    # 关闭后回收，磁盘与元数据一致，无 active 被误删
    client.post(f"/cursors/{cid}/close")
    client.post("/merges/reclaim")
    files = client.get("/files").json()["files"]
    disk = {f["file_name"] for f in files}
    assert set(os.listdir(config.FILES_DIR)) == disk
    assert all(f["status"] == "active" for f in files)


def test_restart_during_switching_marks_failed_keeps_inputs():
    # 手工制造一个 switching 状态的合并，再走恢复逻辑
    _wait_terminal()
    from app.db import get_conn
    conn = get_conn()
    active_ids = [r["id"] for r in conn.execute(
        "SELECT id FROM files WHERE status='active'").fetchall()]
    cur = conn.execute(
        "INSERT INTO merges(state) VALUES ('switching')")
    mid = cur.lastrowid
    for pos, fid in enumerate(active_ids):
        conn.execute(
            "INSERT INTO merge_files(merge_id,file_id,position) VALUES (?,?,?)",
            (mid, fid, pos))
    conn.commit()
    # 放一个伪 tmp，恢复时应被清理
    tmp = os.path.join(config.TMP_DIR, f"merge-{mid}.parquet.tmp")
    with open(tmp, "wb") as f:
        f.write(b"x")

    manager.recover_on_startup(auto_merge=False)
    state = conn.execute("SELECT state FROM merges WHERE id=?", (mid,)).fetchone()
    assert state["state"] == "failed"
    assert all(r["status"] == "active" for r in conn.execute(
        "SELECT status FROM files WHERE id IN (%s)"
        % ",".join("?" * len(active_ids)), active_ids).fetchall())
    assert not os.path.exists(tmp)
