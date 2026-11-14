"""后台合并管理器。

职责：
- 捕获“开始时”已提交的 active 文件，后台流式合并为单个 Parquet；
- 合并期间上传与分页不受影响，合并全程只发生一次（同时只允许一个任务）；
- 完整产物通过校验后在一个 SQLite 事务里原子切换，失败则原数据原样保留；
- 旧文件进入 obsolete，等引用它的游标全部读完或显式关闭后才从磁盘回收；
- 进程在切换/回收任意阶段退出，重启后元数据与未关闭游标仍可读取，
  并继续回收无引用旧文件，绝不删除现用文件。
"""
from __future__ import annotations

import os
import threading
import uuid
from datetime import datetime, timezone

import pyarrow.parquet as pq

from . import config, storage
from .db import LOCK, get_conn, next_file_seq

TERMINAL_STATES = ("completed", "failed", "cancelled")
ACTIVE_STATES = ("running", "paused", "switching")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tmp_output_path(merge_id: int) -> str:
    return os.path.join(config.TMP_DIR, f"merge-{merge_id}.parquet.tmp")


def _output_file_name(merge_id: int) -> str:
    return f"merged-{merge_id}.parquet"


class MergeManager:
    def __init__(self) -> None:
        self._threads: dict[int, threading.Thread] = {}
        # 每个合并任务一个暂停事件；set()=运行中，clear()=已暂停
        self._resume_events: dict[int, threading.Event] = {}

    # ----------------------------- 查询 -----------------------------

    def status(self) -> dict:
        conn = get_conn()
        with LOCK:
            row = conn.execute(
                "SELECT * FROM merges ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row is None:
                return {"active_merge": None}
            inputs = conn.execute(
                """SELECT mf.file_id, f.file_id AS fid, f.path, mf.position,
                          mf.rows_written, f.row_count
                   FROM merge_files mf JOIN files f ON f.id = mf.file_id
                   WHERE mf.merge_id = ? ORDER BY mf.position""",
                (row["id"],),
            ).fetchall()
            return {
                "active_merge": {
                    "merge_id": row["id"],
                    "state": row["state"],
                    "fail_at_step": row["fail_at_step"],
                    "error": row["error"],
                    "output_file_id": row["output_file_id"],
                    "inputs": [
                        {
                            "file_pk": r["file_id"],
                            "position": r["position"],
                            "rows_written": r["rows_written"],
                            "row_count": r["row_count"],
                        }
                        for r in inputs
                    ],
                }
            }

    # --------------------------- 暂停/恢复 ---------------------------

    def pause(self) -> dict:
        conn = get_conn()
        with LOCK:
            row = conn.execute(
                "SELECT * FROM merges ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row is None or row["state"] not in ("running", "paused"):
                raise MergeConflict("当前没有可暂停的合并任务")
            if row["state"] == "running":
                conn.execute(
                    "UPDATE merges SET state='paused', updated_at=? WHERE id=?",
                    (_now(), row["id"]),
                )
                conn.commit()
            ev = self._resume_events.get(row["id"])
            if ev is not None:
                ev.clear()
        return self.status()

    def resume(self) -> dict:
        conn = get_conn()
        with LOCK:
            row = conn.execute(
                "SELECT * FROM merges ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row is None or row["state"] != "paused":
                raise MergeConflict("当前没有已暂停的合并任务")
            conn.execute(
                "UPDATE merges SET state='running', updated_at=? WHERE id=?",
                (_now(), row["id"]),
            )
            conn.commit()
            ev = self._resume_events.get(row["id"])
            if ev is not None:
                ev.set()
        return self.status()

    # ----------------------------- 启动 -----------------------------

    def start_merge(self) -> dict:
        conn = get_conn()
        with LOCK:
            row = conn.execute(
                "SELECT id, state FROM merges ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row is not None and row["state"] in ACTIVE_STATES:
                raise MergeConflict(
                    f"合并任务 {row['id']} 正在进行中（{row['state']}），同期只合并一次"
                )
            active_files = conn.execute(
                """SELECT id, path, row_count FROM files
                   WHERE status='active' ORDER BY read_order, seq_order"""
            ).fetchall()
            if not active_files:
                raise MergeConflict("当前没有可合并的 active 文件")

            cur = conn.execute(
                "INSERT INTO merges(state, fail_at_step, error) VALUES "
                "('running', NULL, NULL)"
            )
            merge_id = cur.lastrowid
            for pos, f in enumerate(active_files):
                conn.execute(
                    "INSERT INTO merge_files(merge_id, file_id, position) "
                    "VALUES (?, ?, ?)",
                    (merge_id, f["id"], pos),
                )
            conn.commit()

        self._spawn_thread(merge_id, paused=False)
        return self.status()

    def _spawn_thread(self, merge_id: int, paused: bool) -> None:
        ev = threading.Event()
        if not paused:
            ev.set()
        self._resume_events[merge_id] = ev
        t = threading.Thread(
            target=self._run_merge,
            args=(merge_id, ev),
            name=f"merge-{merge_id}",
            daemon=True,
        )
        self._threads[merge_id] = t
        t.start()

    # --------------------------- 后台主流程 ---------------------------

    def _wait_if_paused(self, merge_id: int, ev: threading.Event) -> None:
        conn = get_conn()
        while True:
            with LOCK:
                state = conn.execute(
                    "SELECT state FROM merges WHERE id=?", (merge_id,)
                ).fetchone()["state"]
            if state != "paused":
                return
            ev.wait(timeout=0.5)

    def _fail_inject(self, step: str) -> None:
        conn = get_conn()
        row = conn.execute(
            "SELECT value FROM settings WHERE key='fail_inject'"
        ).fetchone()
        if row is not None and row["value"] == step:
            raise RuntimeError(f"演示用故障注入：{step} 阶段故意失败")

    def _run_merge(self, merge_id: int, ev: threading.Event) -> None:
        conn = get_conn()
        tmp_path = _tmp_output_path(merge_id)
        try:
            with LOCK:
                inputs = conn.execute(
                    """SELECT mf.file_id, f.path, f.row_count, mf.position,
                              mf.rows_written
                       FROM merge_files mf JOIN files f ON f.id = mf.file_id
                       WHERE mf.merge_id=? ORDER BY mf.position""",
                    (merge_id,),
                ).fetchall()
                # 恢复场景下的半成品一律重写
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
                conn.execute(
                    "UPDATE merge_files SET rows_written=0 WHERE merge_id=?",
                    (merge_id,),
                )
                conn.commit()

            writer = storage.MergeStreamWriter(tmp_path)
            total_rows = 0
            for inp in inputs:
                self._wait_if_paused(merge_id, ev)
                self._fail_inject("stream")
                n = writer.append_file(inp["path"])
                total_rows += n
                with LOCK:
                    conn.execute(
                        "UPDATE merge_files SET rows_written=? "
                        "WHERE merge_id=? AND file_id=?",
                        (n, merge_id, inp["file_id"]),
                    )
                    conn.execute(
                        "UPDATE merges SET updated_at=? WHERE id=?",
                        (_now(), merge_id),
                    )
                    conn.commit()
            writer.close()

            # 完整性校验通过后才允许切换
            storage.verify_output(tmp_path, total_rows)
            try:
                self._finish_switch(merge_id, tmp_path, total_rows)
            except RuntimeError as exc:
                # 切换阶段（含故障注入）失败：原输入文件保持 active
                self._mark_failed(merge_id, "switch", exc, tmp_path)
                return
        except Exception as exc:  # noqa: BLE001 - 后台线程兜底
            self._mark_failed(merge_id, "merge", exc, tmp_path)

    def _finish_switch(
        self, merge_id: int, tmp_path: str, total_rows: int
    ) -> None:
        conn = get_conn()
        with LOCK:
            # 切换阶段的故障注入：发生在任何元数据变更之前，原数据不受影响
            self._fail_inject("switch")
            conn.execute(
                "UPDATE merges SET state='switching', updated_at=? WHERE id=?",
                (_now(), merge_id),
            )
            conn.commit()

            inputs = conn.execute(
                "SELECT file_id FROM merge_files WHERE merge_id=? ORDER BY position",
                (merge_id,),
            ).fetchall()
            input_ids = [r["file_id"] for r in inputs]

            seq = next_file_seq(conn)
            # 合并产物占据其输入文件中最早的读取位置，保证全局行序不乱
            min_read_order = conn.execute(
                "SELECT MIN(read_order) AS m FROM files WHERE id IN (%s)"
                % ",".join("?" * len(input_ids)),
                input_ids,
            ).fetchone()["m"]

            file_uid = uuid.uuid4().hex
            final_name = f"{seq:012d}-{file_uid}-{_output_file_name(merge_id)}"
            final_path = os.path.join(config.FILES_DIR, final_name)
            os.replace(tmp_path, final_path)
            size = os.path.getsize(final_path)

            try:
                conn.execute(
                    """INSERT INTO files
                       (file_id, kind, path, size_bytes, row_count, seq_order,
                        read_order, status)
                       VALUES (?, 'merged', ?, ?, ?, ?, ?, 'active')""",
                    (file_uid, final_path, size, total_rows, seq, min_read_order),
                )
                conn.execute(
                    "UPDATE files SET status='obsolete', obsolete_at=? "
                    "WHERE id IN (%s)" % ",".join("?" * len(input_ids)),
                    (_now(), *input_ids),
                )
                conn.execute(
                    "UPDATE merges SET state='completed', output_file_id=?, "
                    "updated_at=? WHERE id=?",
                    (file_uid, _now(), merge_id),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                # 事务失败：把产物退回 tmp，输入文件始终保持 active
                if os.path.exists(final_path):
                    try:
                        os.replace(final_path, tmp_path)
                    except OSError:
                        pass
                raise
        # 事务提交后再回收磁盘文件（锁内逐个做，避免与分页竞争）
        self.reclaim()

    def _mark_failed(
        self, merge_id: int, step: str, exc: Exception, tmp_path: str
    ) -> None:
        conn = get_conn()
        with LOCK:
            conn.execute(
                "UPDATE merges SET state='failed', fail_at_step=?, error=?, "
                "updated_at=? WHERE id=? AND state NOT IN ('completed','cancelled')",
                (step, str(exc), _now(), merge_id),
            )
            conn.commit()
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
            # 故障注入是一次性的，触发后自动清除
            conn.execute("DELETE FROM settings WHERE key='fail_inject'")
            conn.commit()

    # ----------------------------- 回收 -----------------------------

    def reclaim(self) -> dict:
        """删除没有任何未关闭游标引用的 obsolete 文件。

        active 文件永远不会被本函数触碰；obsolete 文件只要还被某个
        closed=0 的游标快照引用，就保留在磁盘上。
        """
        conn = get_conn()
        removed: list[str] = []
        with LOCK:
            candidates = conn.execute(
                """SELECT f.id, f.file_id, f.path FROM files f
                   WHERE f.status='obsolete'
                     AND NOT EXISTS (
                       SELECT 1 FROM cursor_files cf
                       JOIN cursors c ON c.id = cf.cursor_id
                       WHERE cf.file_id = f.id AND c.closed = 0)"""
            ).fetchall()
            for c in candidates:
                if os.path.exists(c["path"]):
                    os.remove(c["path"])
                    removed.append(c["file_id"])
                conn.execute("DELETE FROM files WHERE id=?", (c["id"],))
            conn.commit()
        return {"reclaimed": removed}

    # --------------------------- 崩溃恢复 ---------------------------

    def recover_on_startup(self, auto_merge: bool = False) -> dict:
        """进程启动时的恢复逻辑。"""
        conn = get_conn()
        result: dict = {"resumed_merges": [], "failed_merges": [], "reclaimed": []}
        with LOCK:
            # 1. 清理 tmp 里的半成品（上传临时文件/合并临时文件）
            for name in os.listdir(config.TMP_DIR):
                if name.endswith(".tmp"):
                    try:
                        os.remove(os.path.join(config.TMP_DIR, name))
                    except OSError:
                        pass

            # 2. 处理未走完的合并任务
            interrupted = conn.execute(
                "SELECT id, state FROM merges WHERE state IN "
                "('running','paused','switching') ORDER BY id"
            ).fetchall()
            for m in interrupted:
                tmp_path = _tmp_output_path(m["id"])
                if m["state"] == "switching":
                    # 切换事务未提交：输入文件仍是 active，原数据完好
                    conn.execute(
                        "UPDATE merges SET state='failed', fail_at_step='switch', "
                        "error=?, updated_at=? WHERE id=?",
                        ("进程在切换阶段退出，事务未提交，原输入文件保持不变",
                         _now(), m["id"]),
                    )
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                    result["failed_merges"].append(m["id"])
                else:
                    # running/paused：保留任务，重启后台线程从头重写 tmp
                    conn.execute(
                        "UPDATE merge_files SET rows_written=0 WHERE merge_id=?",
                        (m["id"],),
                    )
                    conn.execute("UPDATE merges SET updated_at=? WHERE id=?",
                                 (_now(), m["id"]))
                    conn.commit()
                    result["resumed_merges"].append(
                        {"merge_id": m["id"], "paused": m["state"] == "paused"}
                    )
            conn.commit()

            # 3. 回收无引用旧文件（含磁盘文件已缺失的陈旧元数据）
            rec = self.reclaim()
            result["reclaimed"] = rec["reclaimed"]

        for item in result["resumed_merges"]:
            self._spawn_thread(item["merge_id"], paused=item["paused"])

        # 4. 可选：启动时自动合并现存 active 文件
        if auto_merge:
            try:
                self.start_merge()
                result["auto_started"] = True
            except MergeConflict:
                result["auto_started"] = False
        return result


class MergeConflict(RuntimeError):
    """合并状态冲突，API 层映射为 409。"""


manager = MergeManager()
