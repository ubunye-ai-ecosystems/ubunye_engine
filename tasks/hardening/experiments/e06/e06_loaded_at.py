"""Does the Spark run record hash the rows that were written?

A task adds a `loaded_at` column (a common ingest column). Run with --lineage on
Spark, then hash the written parquet with the same method and compare with the
record (F-040).

    python e06_loaded_at.py WORK_DIR "current_timestamp()"
    python e06_loaded_at.py WORK_DIR "timestamp'2026-09-29 12:00:00'"
"""

import glob
import json
import os
import shutil
import subprocess
import sys
import sysconfig
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession

from ubunye.adapters.spark.content_hash import fingerprint_spark

root = Path(sys.argv[1]).resolve()
column = sys.argv[2]  # a Spark expression, e.g. current_timestamp() or lit(1)
shutil.rmtree(root, ignore_errors=True)
task = root / "uc" / "pkg" / "t"
task.mkdir(parents=True)
pq.write_table(pa.table({"id": list(range(100_000))}), root / "in.parquet")
r = root.as_posix()
(task / "config.yaml").write_text(f"""MODEL: etl
VERSION: "1.0.0"
CONFIG:
  inputs:
    src: {{format: s3, path: "{r}/in.parquet", file_format: parquet}}
  transform: {{}}
  outputs:
    out: {{format: s3, path: "{r}/out", file_format: parquet, mode: overwrite}}
""")
(task / "transformations.py").write_text("""from pyspark.sql import functions as F
from ubunye.core.interfaces import Task


class T(Task):
    def transform(self, sources):
        return {"out": sources["src"].withColumn("loaded_at", F.expr("COLUMN"))}
""".replace("COLUMN", column))
exe = os.path.join(sysconfig.get_path("scripts"), "ubunye")
cmd = [exe, "run", "-d", str(root), "-u", "uc", "-p", "pkg", "-t", "t", "--lineage"]
cmd += ["--backend", "spark", "-dt", "1"]
p = subprocess.run(cmd, capture_output=True, text=True)
print("rc", p.returncode)
rec = json.load(
    open(glob.glob(str(root / ".ubunye" / "lineage" / "**" / "*.json"), recursive=True)[0])
)
recorded = rec["outputs"][0]
spark = SparkSession.builder.config("spark.sql.session.timeZone", "UTC").getOrCreate()
written = fingerprint_spark(spark.read.parquet(str(root / "out")))
print("record  :", recorded["row_count"], recorded["data_hash"])
print("written :", written.row_count, written.data_hash)
print("same    :", recorded["data_hash"] == written.data_hash)
