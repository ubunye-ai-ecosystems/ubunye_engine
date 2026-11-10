"""On live Spark, a JDBC append with ``replace_where`` lands its batch once (F-072).

Spark's JDBC writer commits each partition in its own transaction, so a JDBC append
cannot be claimed or taken back: a run that failed after it left the batch for the
rerun to insert again, and ``--rerun`` inserted a finished batch a second time.

An append output that names its batch's rows with ``replace_where`` is now written
into a staging table, then moved in by one database transaction (delete the batch's
rows, insert the staged rows), so the target holds the batch once whatever happened
before. The database here is Apache Derby, embedded and in memory: its jar ships
with Spark, so this runs offline.
"""

from __future__ import annotations

import textwrap
import uuid
from pathlib import Path

import pytest

pytest.importorskip("pyspark", reason="pyspark not installed")

from pyspark.sql import SparkSession  # noqa: E402

import ubunye  # noqa: E402
from ubunye.core import runs  # noqa: E402
from ubunye.core.runs import BatchFinished  # noqa: E402

pytestmark = pytest.mark.integration

DT1, DT2 = "2024-01-01", "2024-01-02"
DRIVER = "org.apache.derby.jdbc.EmbeddedDriver"


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


@pytest.fixture(autouse=True)
def _quick_takeover(monkeypatch):
    monkeypatch.setattr(runs, "TAKEOVER_SETTLE", 0.05)


@pytest.fixture
def url() -> str:
    return f"jdbc:derby:memory:ubunye_{uuid.uuid4().hex[:8]};create=true"


def _rows(root: Path, n: int) -> None:
    (root / "in.csv").write_text("ID\n" + "".join(f"{i}\n" for i in range(1, n + 1)))


def _task(
    root: Path, url: str, *, where: bool = True, also_missing: bool = False, rows: int = 3
) -> Path:
    task = root / "uc" / "pkg" / "t"
    task.mkdir(parents=True, exist_ok=True)
    r = root.as_posix()
    _rows(root, rows)
    replace = ", replace_where: \"BATCH = '{{ dt }}'\"" if where else ""
    missing = (
        f'    zz_missing: {{format: s3, path: "{r}/out/other", file_format: parquet}}\n'
        if also_missing
        else ""
    )
    (task / "config.yaml").write_text(
        "MODEL: etl\n"
        'VERSION: "1.0.0"\n'
        "CONFIG:\n"
        "  inputs:\n"
        f'    src: {{format: s3, path: "{r}/in.csv", file_format: csv, '
        "options: {header: 'true'}}\n"
        "  transform:\n"
        "    params:\n"
        '      batch: "{{ dt }}"\n'
        "  outputs:\n"
        f'    events: {{format: jdbc, url: "{url}", table: EVENTS, driver: {DRIVER}, '
        f"mode: append{replace}, "
        # Derby makes a Spark string a CLOB, which a WHERE cannot compare.
        "options: {createTableColumnTypes: 'ID VARCHAR(10), BATCH VARCHAR(20)'}}\n" + missing,
        encoding="utf-8",
    )
    (task / "transformations.py").write_text(textwrap.dedent("""\
            from pyspark.sql import functions as F
            from ubunye.core.interfaces import Task


            class T(Task):
                def transform(self, sources):
                    batch = self.config["CONFIG"]["transform"]["params"]["batch"]
                    return {"events": sources["src"].withColumn("BATCH", F.lit(batch))}
            """))
    return task


def _batches(spark, url: str, table: str = "EVENTS") -> dict:
    df = spark.read.jdbc(url, table, properties={"driver": DRIVER})
    return {str(r["BATCH"]): r["count"] for r in df.groupBy("BATCH").count().collect()}


def _tables(spark, url: str) -> list:
    q = "(SELECT TABLENAME FROM SYS.SYSTABLES WHERE TABLETYPE = 'T') t"
    return sorted(r[0] for r in spark.read.jdbc(url, q, properties={"driver": DRIVER}).collect())


def test_a_run_that_failed_after_its_jdbc_append_lands_once(spark, tmp_path, url):
    # Before, replace_where was ignored on an append and the rerun inserted the batch
    # a second time (6 rows).
    task = _task(tmp_path, url, also_missing=True)
    with pytest.raises(Exception):
        ubunye.run_task(str(task), dt=DT2, spark=spark)
    assert _batches(spark, url) == {DT2: 3}

    _task(tmp_path, url, rows=4)  # the cause fixed, and the data corrected
    ubunye.run_task(str(task), dt=DT2, spark=spark)

    assert _batches(spark, url) == {DT2: 4}
    assert _tables(spark, url) == ["EVENTS"]  # no staging table left


def test_rerun_replaces_a_finished_jdbc_batch_and_leaves_the_other(spark, tmp_path, url):
    task = _task(tmp_path, url)
    ubunye.run_task(str(task), dt=DT1, spark=spark)
    ubunye.run_task(str(task), dt=DT2, spark=spark)
    with pytest.raises(BatchFinished):
        ubunye.run_task(str(task), dt=DT2, spark=spark)

    _rows(tmp_path, 2)
    ubunye.run_task(str(task), dt=DT2, spark=spark, rerun=True)

    assert _batches(spark, url) == {DT1: 3, DT2: 2}


def test_a_failure_before_the_move_changes_nothing(spark, tmp_path, url, monkeypatch):
    # The batch is staged, then the database transaction fails: the target is as it
    # was, the staging table is dropped, and the rerun lands the batch once.
    from ubunye.adapters.spark import jdbc_batch

    task = _task(tmp_path, url)
    ubunye.run_task(str(task), dt=DT1, spark=spark)

    def boom(*args, **kwargs):
        raise RuntimeError("connection lost")

    with monkeypatch.context() as m:
        m.setattr(jdbc_batch, "_move_in", boom)
        with pytest.raises(Exception):
            ubunye.run_task(str(task), dt=DT2, spark=spark)
    assert _batches(spark, url) == {DT1: 3}
    assert _tables(spark, url) == ["EVENTS"]

    ubunye.run_task(str(task), dt=DT2, spark=spark)
    assert _batches(spark, url) == {DT1: 3, DT2: 3}


def test_an_append_without_replace_where_is_unchanged(spark, tmp_path, url):
    # No batch predicate: Spark's own JDBC append, as before (a rerun after a failure
    # inserts again; the run says so). Pinned so the opt in stays an opt in.
    task = _task(tmp_path, url, where=False, also_missing=True)
    with pytest.raises(Exception):
        ubunye.run_task(str(task), dt=DT2, spark=spark)
    _task(tmp_path, url, where=False)
    ubunye.run_task(str(task), dt=DT2, spark=spark)
    assert _batches(spark, url) == {DT2: 6}
