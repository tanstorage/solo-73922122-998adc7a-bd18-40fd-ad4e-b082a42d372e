"""SQLite 元数据库：文件清单、合并任务、游标快照。

所有写操作都在进程内的一把可重入锁保护下进行（单进程模型），
配合 WAL 模式保证后台合并线程与 API 线程之间的一致性。
"""
from __future__ import annotations

import os
import sqlite3
import threading

from . import config

# 进程级锁：保护所有元数据变更以及“读 parquet + 删除文件”的临界区
LOCK = threading.RLock()


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(config.DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(config.DB_PATH), exist_ok=True)
    os.makedirs(config.FILES_DIR, exist_ok=True)
    os.makedirs(config.TMP_DIR, exist_ok=True)
    conn = _connect()
    with LOCK:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            );

            -- Parquet 文件清单（上传文件与合并产物共用一张表）
            CREATE TABLE IF NOT EXISTS files (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id      TEXT NOT NULL UNIQUE,
                kind         TEXT NOT NULL CHECK (kind IN ('upload', 'merged')),
                path         TEXT NOT NULL,
                size_bytes   INTEGER NOT NULL,
                row_count    INTEGER NOT NULL,
                seq_order    INTEGER NOT NULL UNIQUE,
                -- 全局读取顺序：合并产物继承其输入的最早位置，
                -- 切换后新上传的文件排在其后；seq_order 仅用于同位置内的次序
                read_order   INTEGER NOT NULL,
                status       TEXT NOT NULL CHECK (status IN ('active', 'obsolete')),
                created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                obsolete_at  TEXT
            );

            -- 后台合并任务
            CREATE TABLE IF NOT EXISTS merges (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                output_file_id TEXT,
                state         TEXT NOT NULL CHECK (state IN (
                                  'running', 'paused', 'switching',
                                  'completed', 'failed', 'cancelled')),
                fail_at_step  TEXT,
                error         TEXT,
                created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                updated_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );

            -- 某次合并在“开始时”捕获的输入文件
            CREATE TABLE IF NOT EXISTS merge_files (
                merge_id        INTEGER NOT NULL REFERENCES merges(id) ON DELETE CASCADE,
                file_id         INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
                position        INTEGER NOT NULL,
                rows_written    INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (merge_id, file_id)
            );

            -- 游标（创建时固定已提交行的快照）
            CREATE TABLE IF NOT EXISTS cursors (
                id          TEXT PRIMARY KEY,
                created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                closed      INTEGER NOT NULL DEFAULT 0,
                exhausted   INTEGER NOT NULL DEFAULT 0
            );

            -- 游标快照：游标能看到哪些文件、各自的行数
            CREATE TABLE IF NOT EXISTS cursor_files (
                cursor_id   TEXT NOT NULL REFERENCES cursors(id) ON DELETE CASCADE,
                file_id     INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
                position    INTEGER NOT NULL,
                row_count   INTEGER NOT NULL,
                PRIMARY KEY (cursor_id, file_id)
            );

            -- 游标读取位置：在每个快照文件里已经读过多少行
            CREATE TABLE IF NOT EXISTS cursor_positions (
                cursor_id   TEXT NOT NULL REFERENCES cursors(id) ON DELETE CASCADE,
                file_id     INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
                rows_read   INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (cursor_id, file_id)
            );

            CREATE INDEX IF NOT EXISTS idx_files_status ON files(status);
            CREATE INDEX IF NOT EXISTS idx_files_read_order
                ON files(read_order, seq_order);
            CREATE INDEX IF NOT EXISTS idx_cursors_closed ON cursors(closed);
            """
        )
        conn.execute(
            "INSERT OR IGNORE INTO settings(key, value) VALUES ('file_seq', 0)"
        )
        conn.commit()
    return conn


_conn: sqlite3.Connection | None = None


def get_conn() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        _conn = init()
    return _conn


def next_file_seq(conn: sqlite3.Connection) -> int:
    """自增并返回下一个文件序号（调用方需持有 LOCK 与事务）。"""
    cur = conn.execute(
        "UPDATE settings SET value = value + 1 WHERE key = 'file_seq'"
    )
    assert cur.rowcount == 1
    row = conn.execute(
        "SELECT value FROM settings WHERE key = 'file_seq'"
    ).fetchone()
    return int(row["value"])
