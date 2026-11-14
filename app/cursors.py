"""游标：创建时固定当时已提交的行，之后按上传顺序 + 文件内行序分页。

- 快照只包含创建时 status=active 的文件；合并切换产生的新文件不会
  追加进旧游标，旧游标引用的输入文件转为 obsolete 但在游标读完/关闭前不回收。
- 保留重复行（不做任何去重）。
- 读取位置在每页返回时持久化，进程重启后未关闭游标可继续翻页。
"""
from __future__ import annotations

import uuid

from . import config, storage
from .db import LOCK, get_conn


class CursorError(RuntimeError):
    """游标状态错误，API 层映射为 404/409。"""


def create_cursor() -> dict:
    conn = get_conn()
    cursor_id = uuid.uuid4().hex
    with LOCK:
        files = conn.execute(
            """SELECT id, row_count, read_order, seq_order FROM files
               WHERE status='active' ORDER BY read_order, seq_order"""
        ).fetchall()
        conn.execute("INSERT INTO cursors(id) VALUES (?)", (cursor_id,))
        for pos, f in enumerate(files):
            conn.execute(
                "INSERT INTO cursor_files(cursor_id, file_id, position, row_count) "
                "VALUES (?, ?, ?, ?)",
                (cursor_id, f["id"], pos, f["row_count"]),
            )
            conn.execute(
                "INSERT INTO cursor_positions(cursor_id, file_id, rows_read) "
                "VALUES (?, ?, 0)",
                (cursor_id, f["id"]),
            )
        conn.commit()
    return {
        "cursor_id": cursor_id,
        "snapshot_files": len(files),
        "snapshot_rows": sum(f["row_count"] for f in files),
        "closed": False,
    }


def _get_cursor(conn, cursor_id: str):
    row = conn.execute(
        "SELECT * FROM cursors WHERE id=?", (cursor_id,)
    ).fetchone()
    if row is None:
        raise CursorError(f"游标 {cursor_id} 不存在")
    return row


def next_page(cursor_id: str, limit: int) -> dict:
    conn = get_conn()
    with LOCK:
        cur = _get_cursor(conn, cursor_id)
        if cur["closed"]:
            raise CursorError(f"游标 {cursor_id} 已关闭")

        snapshot = conn.execute(
            """SELECT cf.file_id, cf.position, cf.row_count, f.path,
                      COALESCE(cp.rows_read, 0) AS rows_read
               FROM cursor_files cf
               LEFT JOIN cursor_positions cp
                 ON cp.cursor_id = cf.cursor_id AND cp.file_id = cf.file_id
               JOIN files f ON f.id = cf.file_id
               WHERE cf.cursor_id=?
               ORDER BY cf.position""",
            (cursor_id,),
        ).fetchall()

        remaining = limit
        rows: list[dict] = []
        touched: list[tuple[int, int]] = []
        truncated = False

        for entry in snapshot:
            if remaining <= 0:
                break
            unread = entry["row_count"] - entry["rows_read"]
            if unread <= 0:
                continue
            take = min(remaining, unread)
            # 关键：文件读取在 LOCK 内完成，reclaim 不可能同时删除该文件
            batch = storage.read_rows(entry["path"], entry["rows_read"], take)
            rows.extend(batch)
            new_read = entry["rows_read"] + len(batch)
            touched.append((entry["file_id"], new_read))
            remaining -= len(batch)
            if len(batch) < take:
                # 物理文件行数与元数据不符，保护性中断，避免误推进位置
                truncated = True
                break

        # 是否耗尽取决于读取位置而不是本页是否拿满：
        # 最后一页恰好拿满（remaining=0）也可能已经读到快照末尾
        updated = dict(touched)
        read_total = sum(
            updated.get(e["file_id"], e["rows_read"]) for e in snapshot
        )
        total_rows = sum(e["row_count"] for e in snapshot)
        exhausted = (not truncated) and read_total >= total_rows

        for file_id, rows_read in touched:
            conn.execute(
                "UPDATE cursor_positions SET rows_read=? "
                "WHERE cursor_id=? AND file_id=?",
                (rows_read, cursor_id, file_id),
            )
        if exhausted:
            conn.execute(
                "UPDATE cursors SET exhausted=1 WHERE id=?", (cursor_id,)
            )
        conn.commit()

        total_rows = sum(e["row_count"] for e in snapshot)
        # 精确统计持久化后的已读行数
        pos_rows = conn.execute(
            "SELECT COALESCE(SUM(rows_read),0) AS n FROM cursor_positions "
            "WHERE cursor_id=?",
            (cursor_id,),
        ).fetchone()["n"]
        return {
            "cursor_id": cursor_id,
            "limit": limit,
            "returned": len(rows),
            "rows": rows,
            "exhausted": bool(exhausted),
            "total_rows": total_rows,
            "rows_read": pos_rows,
            "remaining": total_rows - pos_rows,
        }


def close_cursor(cursor_id: str) -> dict:
    conn = get_conn()
    with LOCK:
        cur = _get_cursor(conn, cursor_id)
        already = bool(cur["closed"])
        if not already:
            conn.execute(
                "UPDATE cursors SET closed=1 WHERE id=?", (cursor_id,)
            )
            conn.commit()
        # 关闭后立即尝试回收因此失去最后引用的 obsolete 文件
        from .merger import manager

        rec = manager.reclaim()
    return {
        "cursor_id": cursor_id,
        "closed": True,
        "already_closed": already,
        "reclaimed": rec["reclaimed"],
    }


def list_cursors() -> dict:
    conn = get_conn()
    with LOCK:
        rows = conn.execute(
            """SELECT c.id, c.created_at, c.closed, c.exhausted,
                      (SELECT COALESCE(SUM(cf.row_count),0) FROM cursor_files cf
                       WHERE cf.cursor_id=c.id) AS total_rows,
                      (SELECT COALESCE(SUM(cp.rows_read),0)
                       FROM cursor_positions cp WHERE cp.cursor_id=c.id) AS rows_read
               FROM cursors c ORDER BY c.created_at"""
        ).fetchall()
    return {
        "cursors": [
            {
                "cursor_id": r["id"],
                "created_at": r["created_at"],
                "closed": bool(r["closed"]),
                "exhausted": bool(r["exhausted"]),
                "total_rows": r["total_rows"],
                "rows_read": r["rows_read"],
            }
            for r in rows
        ]
    }
