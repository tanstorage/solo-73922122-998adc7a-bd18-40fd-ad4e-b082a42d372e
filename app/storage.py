"""Parquet 读写：上传校验/规范化、分页读取、流式合并。"""
from __future__ import annotations

import os
import uuid
from typing import Iterator

import pyarrow as pa
import pyarrow.parquet as pq

from . import config

REQUIRED_COLUMNS = ("station_id", "observed_at", "temperature")


class ParquetValidationError(ValueError):
    """上传文件不满足契约时抛出，API 层映射为 422。"""


def _canonical_table(table: pa.Table) -> pa.Table:
    """把输入表强制规范为固定三列：station_id(str)/observed_at(UTC ts[us])/temperature(float64)。

    - 缺失列、出现多余列 => 422
    - observed_at 只接受时间戳类型，统一转 UTC；naive 时间戳按 UTC 处理
    - station_id 统一转 UTF-8 字符串（数字站点号也接受）
    - temperature 转 float64（布尔值不接受，避免 True/False 被当成 1/0）
    """
    names = table.column_names
    missing = [c for c in REQUIRED_COLUMNS if c not in names]
    if missing:
        raise ParquetValidationError(f"缺少必需列: {', '.join(missing)}")
    extra = [c for c in names if c not in REQUIRED_COLUMNS]
    if extra:
        raise ParquetValidationError(f"存在未声明的额外列: {', '.join(extra)}")
    if table.num_rows == 0:
        raise ParquetValidationError("文件不包含任何数据行")

    # station_id
    sid = table.column("station_id")
    if pa.types.is_string(sid.type) or pa.types.is_large_string(sid.type):
        sid = sid.cast(pa.string())
    elif pa.types.is_integer(sid.type):
        sid = sid.cast(pa.string())
    else:
        raise ParquetValidationError(
            f"station_id 列类型 {sid.type} 不受支持，需要 string 或整数"
        )

    # observed_at
    ts = table.column("observed_at")
    if not pa.types.is_timestamp(ts.type):
        raise ParquetValidationError(
            f"observed_at 列类型 {ts.type} 不受支持，需要 timestamp"
        )
    if ts.type.tz is None:
        ts = ts.cast(pa.timestamp("us", tz="UTC"))
    else:
        ts = ts.cast(pa.timestamp("us", tz="UTC"))

    # temperature
    temp = table.column("temperature")
    if pa.types.is_boolean(temp.type):
        raise ParquetValidationError("temperature 列不能是布尔类型")
    if not (pa.types.is_floating(temp.type) or pa.types.is_integer(temp.type)):
        raise ParquetValidationError(
            f"temperature 列类型 {temp.type} 不受支持，需要数值类型"
        )
    temp = temp.cast(pa.float64())

    return pa.table(
        {
            "station_id": sid,
            "observed_at": ts,
            "temperature": temp,
        }
    )


def validate_and_stage(data: bytes) -> tuple[pa.Table, int, str]:
    """校验上传字节流并规范化后落到 tmp，返回(规范化表, 行数, tmp路径)。"""
    try:
        table = pq.read_table(pa.BufferReader(data))
    except Exception as exc:  # noqa: BLE001 - pyarrow 的异常类型很多
        raise ParquetValidationError(f"无法解析为 Parquet: {exc}") from exc
    table = _canonical_table(table)

    tmp_name = f"upload-{uuid.uuid4().hex}.parquet.tmp"
    os.makedirs(config.TMP_DIR, exist_ok=True)
    tmp_path = os.path.join(config.TMP_DIR, tmp_name)
    pq.write_table(table, tmp_path, compression="snappy")
    return table, table.num_rows, tmp_path


def read_rows(path: str, offset: int, limit: int) -> list[dict]:
    """读取单个 parquet 文件 [offset, offset+limit) 的行，转成 JSON 友好字典。"""
    table = pq.read_table(path)
    if offset:
        table = table.slice(offset, limit)
    else:
        table = table.slice(0, limit)
    return table_to_rows(table)


def table_to_rows(table: pa.Table) -> list[dict]:
    sids = table.column("station_id").to_pylist()
    # UTC 时间戳转成带偏移的 ISO8601 字符串
    ts = table.column("observed_at").to_pylist()
    temps = table.column("temperature").to_pylist()
    rows: list[dict] = []
    for sid, t, temp in zip(sids, ts, temps):
        if t is not None and t.tzinfo is not None:
            observed = t.isoformat()
        elif t is not None:
            observed = t.isoformat() + "+00:00"
        else:
            observed = None
        rows.append(
            {
                "station_id": sid,
                "observed_at": observed,
                "temperature": temp,
            }
        )
    return rows


class MergeStreamWriter:
    """把多个 parquet 按顺序流式写入一个输出文件。

    使用 ParquetWriter 逐文件 append，内存占用不随输入总大小增长。
    """

    def __init__(self, output_path: str):
        self.output_path = output_path
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        self._writer: pq.ParquetWriter | None = None
        self.rows_written = 0

    def append_file(self, path: str) -> int:
        table = pq.read_table(path)
        if self._writer is None:
            self._writer = pq.ParquetWriter(
                self.output_path, table.schema, compression="snappy"
            )
        self._writer.write_table(table)
        self.rows_written += table.num_rows
        return table.num_rows

    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            self._writer = None

    def __enter__(self) -> "MergeStreamWriter":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def verify_output(path: str, expected_rows: int) -> int:
    """合并产物落地后的完整性校验，返回实际行数；不匹配则抛错。"""
    if not os.path.exists(path):
        raise RuntimeError("合并产物不存在")
    pf = pq.ParquetFile(path)
    actual = pf.metadata.num_rows
    if actual != expected_rows:
        raise RuntimeError(
            f"合并产物行数校验失败: 期望 {expected_rows}, 实际 {actual}"
        )
    # 再完整读一遍，确保文件尾部/footer 没有损坏
    table = pq.read_table(path)
    if table.num_rows != expected_rows:
        raise RuntimeError("合并产物重新读取行数不一致")
    if table.column_names != list(REQUIRED_COLUMNS):
        raise RuntimeError("合并产物列结构不符合契约")
    return actual


def iter_merge_inputs(paths: list[str]) -> Iterator[tuple[str, pa.Table]]:
    for p in paths:
        yield p, pq.read_table(p)
