"""On live Spark, the record says whether an input's digest is of what was read (F-046).

An input is hashed at task end by reading its source again. Each test changes the
source from inside the transform, after the read and before the hash, and checks
what the record says.
"""

from __future__ import annotations

import glob
import importlib.util
import json
import os
import textwrap
from pathlib import Path

import pytest

pytest.importorskip("pyspark", reason="pyspark not installed")
pytest.importorskip("pyarrow")

from pyspark.sql import SparkSession  # noqa: E402

import ubunye  # noqa: E402
from ubunye.adapters.spark.content_hash import fingerprint_spark  # noqa: E402

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    # Reuses the session the conftest launched (with Delta's jars when available).
    return (
        SparkSession.builder.master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def _requires_delta(spark: SparkSession) -> None:
    if importlib.util.find_spec("delta") is None:
        pytest.skip("delta-spark not installed")
    if "DeltaSparkSessionExtension" not in (spark.conf.get("spark.sql.extensions", "") or ""):
        pytest.skip("Delta jars unavailable in this JVM")


def _task(root: Path, source: str, meddle: str) -> Path:
    """A task that reads ``src``, runs ``meddle`` (the source changes), writes 3 rows."""
    task = root / "uc" / "pkg" / "t"
    task.mkdir(parents=True)
    r = root.as_posix()
    (task / "config.yaml").write_text(textwrap.dedent(f"""\
            MODEL: etl
            VERSION: "1.0.0"
            CONFIG:
              inputs:
                src: {source}
              transform: {{}}
              outputs:
                out: {{format: s3, path: "{r}/out", file_format: parquet, mode: overwrite}}
            """))
    body = textwrap.indent(textwrap.dedent(meddle), " " * 8)
    (task / "transformations.py").write_text(textwrap.dedent("""\
            import os

            from ubunye.core.interfaces import Task


            class T(Task):
                def transform(self, sources):
                    spark = sources["src"].sparkSession
            """) + body + '\n        return {"out": spark.range(3)}\n')
    return task


def _input(root: Path) -> dict:
    [path] = glob.glob(str(root / ".ubunye" / "lineage" / "**" / "*.json"), recursive=True)
    return json.loads(Path(path).read_text())["inputs"][0]


def _parquet_source(spark: SparkSession, root: Path) -> str:
    spark.range(100).selectExpr("id", "id % 7 as qty").repartition(2).write.parquet(
        str(root / "in")
    )
    return f'{{format: s3, path: "{root.as_posix()}/in", file_format: parquet}}'


def test_an_unchanged_source_says_the_digest_is_of_what_was_read(spark, tmp_path):
    source = _parquet_source(spark, tmp_path)
    task = _task(tmp_path, source, "pass\n")
    ubunye.run_task(str(task), spark=spark, lineage=True, dt="1")
    step = _input(tmp_path)
    assert step["hash_basis"] == "recomputed"
    assert step["source_version"]["kind"] == "files"
    assert step["source_version"]["files"] == 2
    assert step["source_changed"] is False
    assert "of what the task read" in step["source_note"]
    assert (
        step["data_hash"] == fingerprint_spark(spark.read.parquet(str(tmp_path / "in"))).data_hash
    )


def test_a_file_added_after_the_read_is_not_in_the_digest(spark, tmp_path):
    """Spark's file index keeps the files listed at the read: the hash reads those."""
    source = _parquet_source(spark, tmp_path)
    before = fingerprint_spark(spark.read.parquet(str(tmp_path / "in"))).data_hash
    task = _task(
        tmp_path,
        source,
        f"""\
        spark.range(1000, 1010).selectExpr("id", "id % 7 as qty").write.mode("append").parquet(
            "{(tmp_path / 'in').as_posix()}"
        )
        """,
    )
    ubunye.run_task(str(task), spark=spark, lineage=True, dt="1")
    step = _input(tmp_path)
    assert step["source_changed"] is False
    assert step["row_count"] == 100
    assert step["data_hash"] == before


def test_a_file_rewritten_after_the_read_is_flagged(spark, tmp_path):
    source = _parquet_source(spark, tmp_path)
    first = sorted(glob.glob(str(tmp_path / "in" / "part-*.parquet")))[0]
    task = _task(
        tmp_path,
        source,
        f"""\
        import pyarrow as pa
        import pyarrow.parquet as pq

        target = r"{first}"
        pq.write_table(pa.table({{"id": [1, 2, 3, 4, 5, 6, 7, 8, 9], "qty": [0] * 9}}), target)
        stamp = os.path.getmtime(target) + 60
        os.utime(target, (stamp, stamp))
        """,
    )
    ubunye.run_task(str(task), spark=spark, lineage=True, dt="1")
    step = _input(tmp_path)
    assert step["source_changed"] is True
    assert step["source_version"]["listing_hash"] != step["source_version_at_hash"]["listing_hash"]
    assert "not of what the task read" in step["source_note"]


def test_a_delta_table_appended_after_the_read_is_flagged(spark, tmp_path):
    _requires_delta(spark)
    path = (tmp_path / "d").as_posix()
    spark.range(10).write.format("delta").save(path)
    task = _task(
        tmp_path,
        f'{{format: delta, path: "{path}"}}',
        f"""\
        spark.range(10, 13).write.format("delta").mode("append").save("{path}")
        """,
    )
    ubunye.run_task(str(task), spark=spark, lineage=True, dt="1")
    step = _input(tmp_path)
    assert step["source_version"]["kind"] == "delta"
    assert step["source_version"]["version"] == 0
    assert step["source_version_at_hash"]["version"] == 1
    assert step["source_changed"] is True
    # The lazy Delta frame read the table again: the digest is of version 1, 13 rows.
    assert step["row_count"] == 13
    assert "not of what the task read" in step["source_note"]
    assert os.path.isdir(path)
