"""The engine on a real multi-executor Spark: results do not depend on partitioning.

The same task runs twice in fresh processes: on one local core, and on a
two-executor ``local-cluster`` (separate executor JVMs, as on a cluster, with
no Docker). Input is split into many pieces and shuffled into seven. Both runs
must leave the same row counts and the same data hash in their run records.

And nothing may run in the driver by accident: ``spark.driver.maxResultSize``
is set far below the data, so if reading, writing, counting rows or hashing
them (ADR 006) ever pulled the rows back to the driver, Spark itself would fail
the run.

Marked integration (needs a JVM); starts two extra Spark processes.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

pytestmark = pytest.mark.integration

ROWS = 300_000  # about 10 MB of CSV, ten times the driver's result limit
RESULT_LIMIT = "1m"

TRANSFORM = """\
from pyspark.sql import functions as F

from ubunye.core.interfaces import Task


class Spread(Task):
    def transform(self, sources):
        events = sources["events"].withColumn("band", F.col("value") % 10)
        summary = events.groupBy("region", "band").agg(
            F.count("*").alias("events"),
            F.sum("value").alias("total"),
            F.max("label").alias("last_label"),
        )
        return {"events": events, "summary": summary}
"""

CONFIG = """\
MODEL: etl
VERSION: "1.0.0"
CONFIG:
  inputs:
    events:
      format: s3
      path: "{{ task_dir }}/events.csv"
      file_format: csv
      options:
        header: "true"
        inferSchema: "true"
  transform: {}
  outputs:
    events:
      format: s3
      path: "{{ task_dir }}/out/events"
      file_format: parquet
      mode: overwrite
    summary:
      format: s3
      path: "{{ task_dir }}/out/summary"
      file_format: parquet
      mode: overwrite
"""


def _task(root: Path) -> Path:
    task = root / "uc" / "pkg" / "spread"
    task.mkdir(parents=True)
    (task / "transformations.py").write_text(TRANSFORM, encoding="utf-8")
    (task / "config.yaml").write_text(CONFIG, encoding="utf-8")
    regions = ["gauteng", "limpopo", "kwazulu-natal", "western cape", "free state"]
    with open(task / "events.csv", "w", encoding="utf-8", newline="\n") as f:
        f.write("id,region,value,label\n")
        for i in range(ROWS):
            f.write(f"{i},{regions[i % 5]},{(i * 7919) % 1000},event number {i:06d}\n")
    return task


def _run(task: Path, master: str) -> dict:
    import pyspark

    env = dict(os.environ)
    # A local-cluster starts executors with Spark's own scripts.
    env.setdefault("SPARK_HOME", str(Path(pyspark.__file__).parent))
    script = Path(__file__).with_name("distributed_run.py")
    result = task.parent / f"result-{task.name}.json"
    done = subprocess.run(
        [sys.executable, str(script), str(task), master, RESULT_LIMIT, str(result)],
        capture_output=True,
        text=True,
        env=env,
        timeout=900,
    )
    assert done.returncode == 0, done.stderr[-4000:]
    return json.loads(result.read_text(encoding="utf-8"))


def test_two_executors_give_the_receipt_one_core_gives(tmp_path):
    one = _run(_task(tmp_path / "one"), "local[1]")
    many = _run(_task(tmp_path / "many"), "local-cluster[2,1,1024]")

    assert many["executors"] == 2, "the run did not get its two executors"
    assert one["status"] == many["status"] == "success"
    assert one["outputs"]["events"]["rows"] == ROWS
    for name in ("events", "summary"):
        a, b = one["outputs"][name], many["outputs"][name]
        assert a["error"] is None and b["error"] is None, (a, b)
        assert a["rows"] == b["rows"], name
        assert a["data_hash"] == b["data_hash"], name
