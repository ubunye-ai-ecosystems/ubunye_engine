"""On live Spark and Delta, an append of a named batch lands once (ADR 008, F-071).

A Delta append is one Delta commit, so ADR 008 could not claim its files: a run killed
after the commit was appended again by the rerun, a run that failed after it left its
rows for the rerun to add again, and ``--rerun`` appended a finished batch a second
time. A production job that pays customers had to bypass the writer with a hand-written
MERGE for this reason.

Now a Delta append under a run lease is an idempotent write (Delta's ``txnAppId`` and
``txnVersion``, named by the task, output and batch): a retry of the same batch
commits nothing. An append output with ``replace_where`` replaces its batch's rows in
one commit instead, so a rerun corrects a bad batch. Each test here runs the real
writers on the real delta-spark.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import textwrap
import uuid
from pathlib import Path

import pytest

pytest.importorskip("pyspark", reason="pyspark not installed")
pytest.importorskip("delta", reason="delta-spark not installed")

from pyspark.sql import SparkSession  # noqa: E402

import ubunye  # noqa: E402
from ubunye.core import runs  # noqa: E402
from ubunye.core.runs import BatchFinished  # noqa: E402

pytestmark = pytest.mark.integration

DT1, DT2 = "2024-01-01", "2024-01-02"

CHILD = """\
import os, sys
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder.master("local[1]")
    .config("spark.driver.memory", "512m")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
configure_spark_with_delta_pip(builder).getOrCreate()

from ubunye.core import runs

written = runs.written


def _written(name):  # the Delta commit is made; the run has not ended
    written(name)
    if name == "events":
        os._exit(9)


runs.written = _written

import ubunye

ubunye.run_task(sys.argv[1], dt=sys.argv[2])
os._exit(0)
"""


@pytest.fixture(scope="module")
def spark(tmp_path_factory) -> SparkSession:
    session = SparkSession.builder.getOrCreate()
    try:
        session.sql("SELECT 1 AS x").write.format("delta").save(
            str(tmp_path_factory.mktemp("delta-probe") / "t")
        )
    except Exception:  # noqa: BLE001
        pytest.skip("Delta is not available in this Spark session")
    return session


@pytest.fixture(autouse=True)
def _quick_takeover(monkeypatch):
    monkeypatch.setattr(runs, "TAKEOVER_SETTLE", 0.05)


def _task(root: Path, output: str, *, also_missing: bool = False, rows: int = 3) -> Path:
    """A task that appends its batch (``rows`` rows, column ``batch`` = dt) to ``output``."""
    task = root / "uc" / "pkg" / "t"
    task.mkdir(parents=True, exist_ok=True)
    r = root.as_posix()
    _rows(root, rows)
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
        "      batch: \"{{ dt | default('snapshot') }}\"\n"
        "  outputs:\n"
        f"    events: {output}\n" + missing,
        encoding="utf-8",
    )
    (task / "transformations.py").write_text(textwrap.dedent("""\
            from pyspark.sql import functions as F
            from ubunye.core.interfaces import Task


            class T(Task):
                def transform(self, sources):
                    batch = self.config["CONFIG"]["transform"]["params"]["batch"]
                    return {"events": sources["src"].withColumn("batch", F.lit(batch))}
            """))
    return task


def _rows(root: Path, n: int) -> None:
    (root / "in.csv").write_text("id\n" + "".join(f"{i}\n" for i in range(1, n + 1)))


def _path_output(root: Path, extra: str = "") -> str:
    return f'{{format: delta, path: "{root.as_posix()}/out/events", mode: append{extra}}}'


def _batches(df) -> dict:
    return {str(r["batch"]): r["count"] for r in df.groupBy("batch").count().collect()}


def _at_path(spark, root: Path) -> dict:
    return _batches(spark.read.format("delta").load(str(root / "out" / "events")))


def _kill(task: Path, dt: str) -> None:
    """Run the task in a child process that dies (os._exit) right after its append."""
    script = task.parent.parent.parent / "child.py"
    script.write_text(CHILD, encoding="utf-8")
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(p for p in sys.path if p)
    env.setdefault("PYSPARK_PYTHON", sys.executable)
    done = subprocess.run(
        [sys.executable, str(script), str(task), dt], env=env, capture_output=True, timeout=600
    )
    assert done.returncode == 9, done.stderr.decode(errors="replace")[-3000:]


@pytest.fixture
def table(spark, tmp_path):
    """An external Delta table in the session catalog, dropped afterwards."""
    name = f"default.ubunye_events_{uuid.uuid4().hex[:8]}"
    spark.sql(
        f"CREATE TABLE {name} (id STRING, batch STRING) USING delta "
        f"LOCATION '{(tmp_path / 'tbl').as_posix()}'"
    )
    yield name
    spark.sql(f"DROP TABLE IF EXISTS {name}")


def _table_outputs(name: str) -> dict:
    return {
        "delta": f'{{format: delta, table: "{name}", mode: append}}',
        "hive": (
            f'{{format: hive, db_name: default, tbl_name: "{name.split(".")[1]}", '
            "file_format: delta, mode: append}"
        ),
        "unity": f'{{format: unity, table: "{name}", mode: append}}',
    }


def test_a_run_killed_after_its_delta_append_lands_once_after_the_rerun(spark, tmp_path):
    # E-01 on Delta: before, the rerun appended the killed run's batch again (6 rows).
    task = _task(tmp_path, _path_output(tmp_path))
    ubunye.run_task(str(task), dt=DT1, spark=spark)
    _kill(task, DT2)
    assert _at_path(spark, tmp_path) == {DT1: 3, DT2: 3}  # it had committed

    ubunye.run_task(str(task), dt=DT2, spark=spark)

    assert _at_path(spark, tmp_path) == {DT1: 3, DT2: 3}


@pytest.mark.parametrize("writer", ["delta", "hive", "unity"])
def test_a_run_that_failed_after_its_delta_table_append_lands_once(spark, tmp_path, table, writer):
    # The append commits, then the run fails on its next output. Before, the rerun
    # added the batch to the table a second time.
    task = _task(tmp_path, _table_outputs(table)[writer], also_missing=True)
    with pytest.raises(Exception):
        ubunye.run_task(str(task), dt=DT2, spark=spark)
    assert _batches(spark.table(table)) == {DT2: 3}

    _task(tmp_path, _table_outputs(table)[writer])  # the cause fixed
    ubunye.run_task(str(task), dt=DT2, spark=spark)

    assert _batches(spark.table(table)) == {DT2: 3}


def test_rerun_with_replace_where_replaces_a_finished_delta_batch(spark, tmp_path):
    # F-031 on Delta: before, --rerun appended the batch again (replace_where was
    # ignored for an append). Now the batch's rows are replaced in one commit, and the
    # other batch is untouched.
    task = _task(tmp_path, _path_output(tmp_path, ", replace_where: \"batch = '{{ dt }}'\""))
    ubunye.run_task(str(task), dt=DT1, spark=spark)
    ubunye.run_task(str(task), dt=DT2, spark=spark)
    with pytest.raises(BatchFinished):
        ubunye.run_task(str(task), dt=DT2, spark=spark)

    _rows(tmp_path, 4)  # the corrected batch
    ubunye.run_task(str(task), dt=DT2, spark=spark, rerun=True)

    assert _at_path(spark, tmp_path) == {DT1: 3, DT2: 4}


def test_rerun_with_replace_where_replaces_a_finished_batch_in_a_table(spark, tmp_path, table):
    out = f'{{format: delta, table: "{table}", mode: append, replace_where: "batch = \'{{{{ dt }}}}\'"}}'
    task = _task(tmp_path, out)
    ubunye.run_task(str(task), dt=DT1, spark=spark)
    ubunye.run_task(str(task), dt=DT2, spark=spark)
    _rows(tmp_path, 2)
    ubunye.run_task(str(task), dt=DT2, spark=spark, rerun=True)

    assert _batches(spark.table(table)) == {DT1: 3, DT2: 2}


def test_a_failed_run_is_corrected_by_the_next_run_when_replace_where_is_set(spark, tmp_path):
    # With replace_where the next run of the batch writes its own data, not the failed
    # run's: a bad batch can always be corrected.
    out = _path_output(tmp_path, ", replace_where: \"batch = '{{ dt }}'\"")
    task = _task(tmp_path, out, also_missing=True)
    with pytest.raises(Exception):
        ubunye.run_task(str(task), dt=DT2, spark=spark)
    _task(tmp_path, out, rows=5)
    ubunye.run_task(str(task), dt=DT2, spark=spark)

    assert _at_path(spark, tmp_path) == {DT2: 5}


def test_a_retry_that_finds_its_batch_already_committed_says_so(spark, tmp_path, caplog):
    # The failed run's rows were removed by hand: the retry's write is skipped by Delta
    # (it is the same batch), and the run says so instead of passing in silence.
    task = _task(tmp_path, _path_output(tmp_path), also_missing=True)
    with pytest.raises(Exception):
        ubunye.run_task(str(task), dt=DT2, spark=spark)
    spark.sql(f"DELETE FROM delta.`{(tmp_path / 'out' / 'events').as_posix()}`")
    _task(tmp_path, _path_output(tmp_path))

    with caplog.at_level(logging.WARNING):
        ubunye.run_task(str(task), dt=DT2, spark=spark)

    assert _at_path(spark, tmp_path) == {}
    assert "replace_where" in caplog.text and "already" in caplog.text


def test_a_run_with_no_named_batch_still_appends_each_time(spark, tmp_path):
    # A snapshot job (no dt, no --var) appends every run: unchanged.
    task = _task(tmp_path, _path_output(tmp_path))
    ubunye.run_task(str(task), spark=spark)
    ubunye.run_task(str(task), spark=spark)
    assert sum(_at_path(spark, tmp_path).values()) == 6
