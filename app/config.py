"""运行期配置，全部可通过环境变量覆盖。"""
from __future__ import annotations

import os

DATA_DIR = os.environ.get("DATA_DIR", "/data")
DB_PATH = os.path.join(DATA_DIR, "meta", "lakehouse.db")
FILES_DIR = os.path.join(DATA_DIR, "files")
TMP_DIR = os.path.join(DATA_DIR, "tmp")

# 每份上传 Parquet 的硬上限：8 MiB
MAX_UPLOAD_BYTES = int(os.environ.get("MAX_UPLOAD_BYTES", str(8 * 1024 * 1024)))

# 启动时若存在已提交的 active 文件，是否自动发起一次后台合并
AUTO_MERGE_ON_START = os.environ.get("AUTO_MERGE_ON_START", "false").lower() == "true"

# 分页参数保护
DEFAULT_PAGE_LIMIT = int(os.environ.get("DEFAULT_PAGE_LIMIT", "1000"))
MAX_PAGE_LIMIT = int(os.environ.get("MAX_PAGE_LIMIT", "100000"))

CANONICAL_COLUMNS = ("station_id", "observed_at", "temperature")
