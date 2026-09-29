"""On live Spark, the run record hashes the rows that were written (F-040, ADR 009).

A task adds ``loaded_at = current_timestamp()``, a column that differs every
time the plan is computed. Before ADR 009 the record hashed a second computation
of the output, so its digest never matched the written files.
"""

from __future__ import annotations

import glob
import json
import textwrap
from pathlib import Path

import pytest

pytest.importorskip("pyspark", reason="pyspark not installed")
pytest.importorskip("pyarrow")

from pyspark.sql import SparkSession  # noqa: E402

import ubunye  # noqa: E402
from ubunye.adapters.spark import materialise  # noqa: E402
from ubunye.adapters.spark.content_hash import fingerprint_spark  # noqa: E402

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        # A Python UDF counts its calls below; the Arrow worker is not needed.
        .config("spark.sql.execution.pythonUDF.arrow.enabled", "false")
        .getOrCreate()
    )


def _task(
    root: Path, spark: SparkSession, expectations: str = "", quarantine: bool = False
) -> Path:
    spark.range(2000).selectExpr("id", "id % 7 as qty").write.mode("overwrite").parquet(
        str(root / "in")
    )
    task = root / "uc" / "pkg" / "t"
    task.mkdir(parents=True)
    r = root.as_posix()
    (task / "config.yaml").write_text(
        textwrap.dedent(f"""\
            MODEL: etl
            VERSION: "1.0.0"
            CONFIG:
              inputs:
                src: {{format: s3, path: "{r}/in", file_format: parquet}}
              transform: {{}}
              outputs:
                out: {{format: s3, path: "{r}/out", file_format: parquet, mode: overwrite}}
                bad: {{format: s3, path: "{r}/bad", file_format: parquet, mode: overwrite}}
            """)
        + (
            f'    q: {{format: s3, path: "{r}/q", file_format: parquet, mode: overwrite}}\n'
            if quarantine
            else ""
        )
        + expectations
    )
    (task / "transformations.py").write_text(textwrap.dedent("""\
            from pyspark.sql import functions as F
            from ubunye.core.interfaces import Task


            class T(Task):
                def transform(self, sources):
                    out = sources["src"].withColumn("loaded_at", F.current_timestamp())
                    return {"out": out, "bad": out.filter("qty = 0")}
            """))
    return task


def _record(root: Path) -> dict:
    [path] = glob.glob(str(root / ".ubunye" / "lineage" / "**" / "*.json"), recursive=True)
    return json.loads(Path(path).read_text())


def _cached_rdds(spark: SparkSession) -> int:
    return len(spark.sparkContext._jsc.sc().getRDDStorageInfo())


# Both ways a Spark run is made: a notebook's session (the Databricks backend) and
# `ubunye run` (the Spark backend). Only the first was tested, so a Spark backend
# that returned its frame unheld passed every test (skeptic attack 8).
@pytest.mark.parametrize("via", ["session", "cli-backend"])
def test_the_record_hashes_the_rows_that_were_written(spark, tmp_path, via):
    task = _task(tmp_path, spark)
    before = _cached_rdds(spark)
    how = {"spark": spark} if via == "session" else {"backend": "spark"}
    ubunye.run_task(str(task), lineage=True, lineage_dir=".ubunye/lineage", dt="1", **how)
    rec = _record(tmp_path)
    for step in rec["outputs"]:
        written = fingerprint_spark(spark.read.parquet(str(tmp_path / step["name"])))
        assert step["hash_basis"] == "materialised"
        assert step["data_hash"] == written.data_hash, step["name"]
    assert rec["inputs"][0]["hash_basis"] == "recomputed"
    assert _cached_rdds(spark) == before  # the held rows were released


def test_quarantined_rows_are_cut_from_the_same_rows(spark, tmp_path):
    rules = textwrap.indent(
        textwrap.dedent("""\
            expectations:
              out:
                quarantine: q
                rules:
                  - between: {column: qty, min: 1}
                    severity: quarantine
            """),
        "  ",
    )
    task = _task(tmp_path, spark, rules, quarantine=True)
    back = ubunye.run_task(str(task), spark=spark, lineage=True, dt="1")
    rec = _record(tmp_path)
    steps = {s["name"]: s for s in rec["outputs"]}
    for name in ("out", "q"):
        written = fingerprint_spark(spark.read.parquet(str(tmp_path / name)))
        assert steps[name]["data_hash"] == written.data_hash, name
        assert steps[name]["hash_basis"] == "materialised"
    # The frames handed back still work after the held rows were released.
    assert back["out"].count() + back["q"].count() == 2000


def test_a_lost_block_fails_loudly_instead_of_recomputing(spark):
    df = spark.range(1000).selectExpr("id", "rand() as r")
    held = materialise.materialise(df)
    assert held is not None
    materialise.release(held)
    with pytest.raises(Exception, match="CHECKPOINT_RDD_BLOCK_ID_NOT_FOUND|not found"):
        held.count()


def test_a_streaming_frame_is_not_held(spark):
    stream = spark.readStream.format("rate").load()
    assert materialise.materialise(stream) is None


def _failing_task(root: Path, spark: SparkSession, calls: Path) -> Path:
    # One input file, so one partition: the calls before the failure repeat exactly.
    spark.range(300).coalesce(1).write.mode("overwrite").parquet(str(root / "in"))
    task = root / "uc" / "pkg" / "f"
    task.mkdir(parents=True)
    r = root.as_posix()
    (task / "config.yaml").write_text(textwrap.dedent(f"""            MODEL: etl
            VERSION: "1.0.0"
            CONFIG:
              inputs:
                src: {{format: s3, path: "{r}/in", file_format: parquet}}
              transform: {{}}
              outputs:
                out: {{format: s3, path: "{r}/out", file_format: parquet, mode: overwrite}}
            """))
    (task / "transformations.py").write_text(
        textwrap.dedent(f"""            from pyspark.sql import functions as F
            from ubunye.core.interfaces import Task


            def call_service(i):
                # Stands in for a paid API call: one character per call.
                with open(r"{calls}", "a") as f:
                    f.write("x")
                if i == 299:
                    raise ValueError("the service returned 500")
                return i


            class T(Task):
                def transform(self, sources):
                    udf = F.udf(call_service, "long")
                    return {{"out": sources["src"].withColumn("v", udf("id"))}}
            """)
    )
    return task


def test_a_failing_transform_runs_once_when_its_output_is_held(spark, tmp_path):
    counts = {}
    for lineage in (False, True):
        root = tmp_path / ("held" if lineage else "plain")
        calls = root / "calls.txt"
        root.mkdir()
        task = _failing_task(root, spark, calls)
        with pytest.raises(Exception, match="the service returned 500"):
            ubunye.run_task(str(task), spark=spark, lineage=lineage, dt="1")
        counts[lineage] = len(calls.read_text())
    # Before the fix the held run failed, fell back and ran the plan again.
    assert counts[True] == counts[False] > 0
