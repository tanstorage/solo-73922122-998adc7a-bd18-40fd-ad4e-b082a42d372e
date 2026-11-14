import os
import tempfile

_tmp = tempfile.mkdtemp(prefix="lakehouse-test-")
os.environ["DATA_DIR"] = _tmp
os.environ["AUTO_MERGE_ON_START"] = "false"
