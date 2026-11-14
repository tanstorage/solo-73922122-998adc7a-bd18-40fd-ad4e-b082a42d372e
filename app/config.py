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

# 上传暂存线程池大小：Parquet 校验/规范化/写 tmp 在该池中执行，
# 避免阻塞 FastAPI 事件循环（/health、分页、其他上传不受影响）
UPLOAD_STAGE_WORKERS = int(os.environ.get("UPLOAD_STAGE_WORKERS", "4"))

# 演示/测试钩子：暂存处理的人为延迟秒数（0=关闭），模拟慢校验/慢磁盘
UPLOAD_STAGE_DELAY_SECONDS = float(os.environ.get("UPLOAD_STAGE_DELAY_SECONDS", "0"))

CANONICAL_COLUMNS = ("station_id", "observed_at", "temperature")
