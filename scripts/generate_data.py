#!/usr/bin/env python3
"""生成合成气象站 Parquet 文件（固定三列，故意保留重复行）。

用法:
    python generate_data.py --out /data/synthetic --files 6 --rows 200
生成的文件可直接用原始字节 POST 到 /uploads（不是 multipart 表单）。
"""
from __future__ import annotations

import argparse
import os
import random
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq

COLUMNS = ("station_id", "observed_at", "temperature")


def generate(out_dir: str, n_files: int, rows_per_file: int, seed: int) -> list[str]:
    random.seed(seed)
    os.makedirs(out_dir, exist_ok=True)
    base = datetime(2026, 11, 13, 0, 0, 0, tzinfo=timezone.utc)
    paths: list[str] = []
    for i in range(n_files):
        # 每个文件前两行故意与上一个文件末尾重复，验证“保留重复行”
        stations = [f"ST-{ (i * 7 + j) % 5 + 1:03d}" for j in range(rows_per_file)]
        timestamps = [
            base + timedelta(minutes=i * rows_per_file + j)
            for j in range(rows_per_file)
        ]
        temps = [round(15 + 10 * random.random() + i, 3) for _ in range(rows_per_file)]
        if i > 0:
            stations[0:2] = stations_prev_tail
            timestamps[0:2] = ts_prev_tail
            temps[0:2] = temp_prev_tail
        table = pa.table(
            {
                "station_id": pa.array(stations, pa.string()),
                "observed_at": pa.array(timestamps, pa.timestamp("us", tz="UTC")),
                "temperature": pa.array(temps, pa.float64()),
            }
        )
        path = os.path.join(out_dir, f"weather-{i:03d}.parquet")
        pq.write_table(table, path, compression="snappy")
        paths.append(path)
        stations_prev_tail = stations[-2:]
        ts_prev_tail = timestamps[-2:]
        temp_prev_tail = temps[-2:]
    return paths


def main() -> None:
    ap = argparse.ArgumentParser(description="生成合成气象 Parquet")
    ap.add_argument("--out", default="/data/synthetic")
    ap.add_argument("--files", type=int, default=6)
    ap.add_argument("--rows", type=int, default=200)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    paths = generate(args.out, args.files, args.rows, args.seed)
    total = 0
    for p in paths:
        size = os.path.getsize(p)
        rows = pq.ParquetFile(p).metadata.num_rows
        total += rows
        print(f"  {p}  rows={rows}  bytes={size}")
    print(f"已生成 {len(paths)} 个文件，共 {total} 行（含跨文件重复行）-> {args.out}")


if __name__ == "__main__":
    main()
