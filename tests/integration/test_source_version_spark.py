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
    # A local file system gives no content tags: the note claims no more than that.
    assert "sizes and modification times are unchanged since the read" in step["source_note"]
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


def test_a_same_size_same_time_rewrite_is_not_claimed_as_read(spark, tmp_path):
    """Skeptic bug 1: without content tags, "unchanged" is names, sizes and times only."""
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "a.csv").write_text("id,v\n1,100\n")
    target = tmp_path / "in" / "b.csv"
    target.write_text("id,v\n2,200\n")
    source = (
        f'{{format: s3, path: "{(tmp_path / "in").as_posix()}", file_format: csv, '
        'options: {header: "true"}}'
    )
    task = _task(
        tmp_path,
        source,
        f"""\
        st = os.stat(r"{target}")
        with open(r"{target}", "w") as f:
            f.write("id,v" + chr(10) + "2,999" + chr(10))
        os.utime(r"{target}", ns=(st.st_atime_ns, st.st_mtime_ns))
        """,
    )
    ubunye.run_task(str(task), spark=spark, lineage=True, dt="1")
    step = _input(tmp_path)
    assert step["source_changed"] is False and step["source_version"]["etags"] is False
    assert "This digest is of what the task read" not in step["source_note"]
    assert "cannot be ruled out" in step["source_note"]


def test_a_delta_read_is_pinned_so_an_append_after_it_is_not_read(spark, tmp_path):
    """Skeptic bug 3: the engine pins the read, so the hash reads what the task read."""
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
    assert step["source_version"]["pinned_by"] == "engine"
    assert step["source_version_at_hash"]["latest_version"] == 1
    assert step["source_changed"] is False
    assert step["row_count"] == 10  # version 0, not the 13 rows of version 1
    v0 = spark.read.format("delta").option("versionAsOf", 0).load(path)
    assert step["data_hash"] == fingerprint_spark(v0).data_hash
    assert "pinned to Delta version 0 (by the engine)" in step["source_note"]


def test_two_outputs_of_one_delta_input_see_one_version(spark, tmp_path, monkeypatch):
    """Skeptic bug 3: an append between two outputs made them count 10 and 13 rows."""
    _requires_delta(spark)
    from ubunye.adapters.spark import materialise as M

    path = (tmp_path / "d").as_posix()
    spark.range(10).write.format("delta").save(path)
    task = tmp_path / "uc" / "pkg" / "t"
    task.mkdir(parents=True)
    r = tmp_path.as_posix()
    (task / "config.yaml").write_text(textwrap.dedent(f"""\
            MODEL: etl
            VERSION: "1.0.0"
            CONFIG:
              inputs:
                src: {{format: delta, path: "{path}"}}
              transform: {{}}
              outputs:
                a_out: {{format: s3, path: "{r}/a", file_format: parquet, mode: overwrite}}
                b_out: {{format: s3, path: "{r}/b", file_format: parquet, mode: overwrite}}
            """))
    (task / "transformations.py").write_text(textwrap.dedent("""\
            from ubunye.core.interfaces import Task


            class T(Task):
                def transform(self, sources):
                    df = sources["src"]
                    return {"a_out": df.groupBy().count(), "b_out": df.groupBy().count()}
            """))
    original, calls = M.materialise, []

    def meddling(frame):
        held = original(frame)
        calls.append(1)
        if len(calls) == 1:  # another job appends between the two outputs
            spark.range(10, 13).write.format("delta").mode("append").save(path)
        return held

    monkeypatch.setattr(M, "materialise", meddling)
    ubunye.run_task(str(task), spark=spark, lineage=True, dt="1")
    assert len(calls) == 2
    a = spark.read.parquet(f"{r}/a").collect()[0][0]
    b = spark.read.parquet(f"{r}/b").collect()[0][0]
    assert (a, b) == (10, 10)
    assert os.path.isdir(path)


def test_a_delta_pin_in_options_is_the_version_recorded(spark, tmp_path):
    """Skeptic bug 2: options.versionAsOf was read, but the latest version was recorded."""
    _requires_delta(spark)
    path = (tmp_path / "d").as_posix()
    spark.range(10).write.format("delta").save(path)
    spark.range(10, 15).write.format("delta").mode("append").save(path)
    task = _task(
        tmp_path, f'{{format: delta, path: "{path}", options: {{versionAsOf: 0}}}}', "pass\n"
    )
    ubunye.run_task(str(task), spark=spark, lineage=True, dt="1")
    step = _input(tmp_path)
    assert step["source_version"]["version"] == 0
    assert step["source_version"]["pinned_by"] == "config"
    assert step["source_changed"] is False and step["row_count"] == 10


def test_an_optimize_after_a_pinned_read_is_not_a_change(spark, tmp_path):
    """Skeptic bug 6: OPTIMIZE (same rows) was called 'the source changed'."""
    _requires_delta(spark)
    path = (tmp_path / "d").as_posix()
    for i in range(3):
        spark.range(i * 10, i * 10 + 10).write.format("delta").mode("append").save(path)
    task = _task(
        tmp_path,
        f'{{format: delta, path: "{path}"}}',
        f"""\
        spark.sql("OPTIMIZE delta.`{path}`").collect()
        """,
    )
    ubunye.run_task(str(task), spark=spark, lineage=True, dt="1")
    step = _input(tmp_path)
    assert step["source_changed"] is False and step["row_count"] == 30
