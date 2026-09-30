"""On live Spark, a path append lands once however a run ends (ADR 008, F-070).

The pandas backend claims each file in the run's lease before it lands, so a failed
or killed run has exactly its own files taken back and ``--rerun`` replaces a
finished batch. Spark path appends were not claimed: a run killed after its append
was appended again by the rerun (the batch twice), a failed run left its append,
and ``--rerun`` appended the batch a second time. These are the E-01 and E-02
experiments ported to Spark, on tiny data. A kill is a real one: a child process
running the task ends with ``os._exit`` at the chosen moment, as a power cut would.

Spark now writes the batch into a staging folder the lease names, the files found
there (this run's by construction) are claimed, then moved in. No test here, and no
code, decides what is a run's by listing the output folder: a file some other job
put there is never removed.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import threading
from pathlib import Path

import pytest

pytest.importorskip("pyspark", reason="pyspark not installed")

from pyspark.sql import SparkSession  # noqa: E402

import ubunye  # noqa: E402
from ubunye.core import runs  # noqa: E402
from ubunye.core.runs import BatchFinished, RunLeaseHeld  # noqa: E402

pytestmark = pytest.mark.integration

DT1, DT2 = "2024-01-01", "2024-01-02"

CHILD = """\
import os, sys
from pyspark.sql import SparkSession

SparkSession.builder.master("local[1]").config("spark.driver.memory", "512m").config(
    "spark.ui.enabled", "false"
).config("spark.sql.shuffle.partitions", "1").getOrCreate()

from ubunye.core import runs

kill = os.environ["KILL_AT"]
if kill == "after_append":  # the append is committed; the run has not ended
    written = runs.written

    def _written(name):
        written(name)
        if name == "events":
            os._exit(9)

    runs.written = _written
elif kill == "before_claims":  # Spark has written the batch; nothing is in the output yet
    def _claim_all(paths):
        os._exit(9)

    runs.claim_all = _claim_all

import ubunye

ubunye.run_task(sys.argv[1], dt=sys.argv[2])
os._exit(0)
"""


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


@pytest.fixture(autouse=True)
def _quick_takeover(monkeypatch):
    monkeypatch.setattr(runs, "TAKEOVER_SETTLE", 0.05)


def _task(root: Path, *, also_missing: bool = False, fmt: str = "parquet", by: str = "") -> Path:
    """A task that appends its batch (3 rows, 2 part files) to ``out/events``."""
    part = f", partitionBy: [{by}]" if by else ""
    task = root / "uc" / "pkg" / "t"
    task.mkdir(parents=True)
    r = root.as_posix()
    (root / "in.csv").write_text("id\n1\n2\n3\n", encoding="utf-8")
    (root / "gate").write_text("", encoding="utf-8")
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
        f'      gate: "{r}/gate"\n'
        "  outputs:\n"
        f'    events: {{format: s3, path: "{r}/out/events", file_format: {fmt}, '
        f"mode: append{part}}}\n" + missing,
        encoding="utf-8",
    )
    (task / "transformations.py").write_text(textwrap.dedent("""\
            import os
            import time

            from pyspark.sql import functions as F
            from ubunye.core.interfaces import Task


            class T(Task):
                def transform(self, sources):
                    params = self.config["CONFIG"]["transform"]["params"]
                    while not os.path.exists(params["gate"]):
                        time.sleep(0.05)
                    df = sources["src"].withColumn("batch", F.lit(params["batch"]))
                    df = df.withColumn("_part", F.col("id").cast("int") % 2)
                    return {"events": df.repartition(2)}
            """))
    return task


def _batches(spark: SparkSession, root: Path) -> dict:
    rows = spark.read.parquet(str(root / "out" / "events")).groupBy("batch").count().collect()
    return {str(r["batch"]): r["count"] for r in rows}  # a partition reads back as a date


def _kill(task: Path, dt: str, at: str) -> None:
    """Run the task in a child process that dies (os._exit) at ``at``."""
    script = task.parent.parent.parent / "child.py"
    script.write_text(CHILD, encoding="utf-8")
    env = dict(os.environ, KILL_AT=at)
    env["PYTHONPATH"] = os.pathsep.join(p for p in sys.path if p)
    done = subprocess.run(
        [sys.executable, str(script), str(task), dt], env=env, capture_output=True, timeout=600
    )
    assert done.returncode == 9, done.stderr.decode(errors="replace")[-3000:]


def _debris(root: Path) -> list:
    """Staging folders left anywhere: beside the output (the first design) or in it."""
    beside = [p.name for p in (root / "out").iterdir() if ".ubunye-" in p.name]
    events = root / "out" / "events"
    inside = [p.name for p in events.glob("_ubunye-*")] if events.exists() else []
    return sorted(beside + inside)


def test_a_run_killed_after_its_append_is_taken_back_and_the_batch_lands_once(spark, tmp_path):
    # E-01 on Spark: before, the rerun appended the killed run's batch again (6 rows).
    task = _task(tmp_path)
    ubunye.run_task(str(task), dt=DT1, spark=spark)
    _kill(task, DT2, "after_append")
    assert _batches(spark, tmp_path) == {DT1: 3, DT2: 3}  # it had landed

    ubunye.run_task(str(task), dt=DT2, spark=spark)

    assert _batches(spark, tmp_path) == {DT1: 3, DT2: 3}
    assert _debris(tmp_path) == []


def test_a_run_killed_before_its_files_moved_in_leaves_nothing_behind(spark, tmp_path):
    # Killed between Spark's write and the claims: nothing is in the output, and the
    # rerun removes the dead run's staging folder (named in its lease).
    task = _task(tmp_path)
    ubunye.run_task(str(task), dt=DT1, spark=spark)
    _kill(task, DT2, "before_claims")
    assert _batches(spark, tmp_path) == {DT1: 3}
    assert len(_debris(tmp_path)) == 1  # the dead run's staging folder

    ubunye.run_task(str(task), dt=DT2, spark=spark)

    assert _batches(spark, tmp_path) == {DT1: 3, DT2: 3}
    assert _debris(tmp_path) == []


def test_a_file_another_job_put_in_the_folder_is_never_removed(spark, tmp_path):
    # Between the kill and the rerun, another job appends to the same folder with
    # plain Spark, and a file named like a part file is dropped in. Neither is claimed
    # by the killed run, so neither is removed, by the takeover or by --rerun.
    task = _task(tmp_path)
    _kill(task, DT2, "after_append")
    events = tmp_path / "out" / "events"
    # The nastiest stranger: a copy of one of the killed run's own part files, same
    # batch, part-like name, landed after the kill. It is not the killed run's file.
    one = sorted(events.glob("part-*.parquet"))[0]
    copy = events / "part-00000-foreign.c000.snappy.parquet"
    copy.write_bytes(one.read_bytes())
    copied = spark.read.parquet(str(copy)).count()
    spark.createDataFrame([(9, "foreign")], "id int, batch string").write.mode("append").parquet(
        str(events)
    )
    assert _batches(spark, tmp_path) == {DT2: 3 + copied, "foreign": 1}

    ubunye.run_task(str(task), dt=DT2, spark=spark)
    after = _batches(spark, tmp_path)
    # The killed run's 3 rows are out, the rerun's 3 are in; the others stay.
    assert after == {DT2: 3 + copied, "foreign": 1}
    assert copy.exists()

    ubunye.run_task(str(task), dt=DT2, spark=spark, rerun=True)
    assert _batches(spark, tmp_path) == after
    assert copy.exists()


def test_rerun_replaces_a_finished_batch_and_a_plain_rerun_is_refused(spark, tmp_path):
    # F-031 on Spark: before, --rerun appended the batch again (6 rows).
    task = _task(tmp_path)
    ubunye.run_task(str(task), dt=DT2, spark=spark)
    with pytest.raises(BatchFinished):
        ubunye.run_task(str(task), dt=DT2, spark=spark)
    ubunye.run_task(str(task), dt=DT2, spark=spark, rerun=True)
    assert _batches(spark, tmp_path) == {DT2: 3}


@pytest.mark.parametrize("fmt", ["parquet", "csv", "json", "orc"])
def test_rerun_replaces_a_partitioned_batch_in_each_file_format(spark, tmp_path, fmt):
    # Partition folders are moved in with their files; a partition column named with a
    # leading "_" is data to Spark ("_part=1" holds rows), so it is moved too.
    task = _task(tmp_path, fmt=fmt, by="_part")
    ubunye.run_task(str(task), dt=DT2, spark=spark)
    ubunye.run_task(str(task), dt=DT2, spark=spark, rerun=True)
    events = tmp_path / "out" / "events"
    assert spark.read.format(fmt).load(str(events)).count() == 3
    assert sorted(p.name for p in events.iterdir() if p.is_dir()) == ["_part=0", "_part=1"]
    assert (events / "_SUCCESS").exists()
    assert _debris(tmp_path) == []


@pytest.mark.parametrize("by", ["", "batch"], ids=["flat", "partitioned"])
def test_readers_of_the_output_and_of_its_parent_never_see_a_staging_batch(spark, tmp_path, by):
    # F-070 skeptic review: a staging folder beside the output was read by a glob of
    # the parent (lake/*). Now it is inside the output as _ubunye-<id>, which Spark,
    # pyarrow and Ubunye's own reader skip, partition discovery included.
    import pyarrow.dataset as ds

    task = _task(tmp_path, by=by)
    ubunye.run_task(str(task), dt=DT1, spark=spark)
    _kill(task, DT2, "before_claims")  # its staging folder stays, full of dt=2
    events = tmp_path / "out" / "events"
    left = _debris(tmp_path)
    assert len(left) == 1 and left[0].startswith("_ubunye-") and (events / left[0]).is_dir()
    assert list((events / left[0]).rglob("part-*"))  # it does hold the batch

    assert _batches(spark, tmp_path) == {DT1: 3}
    parent = spark.read.parquet(str(tmp_path / "out" / "*"))
    assert parent.count() == 3
    arrow = ds.dataset(str(events), format="parquet", partitioning="hive")
    assert arrow.count_rows() == 3
    from ubunye.backends.pandas_backend import PandasBackend

    frame = PandasBackend().read_frame("parquet", str(events))
    assert frame.count() == 3
    assert "_ubunye" not in " ".join(frame.schema)


def _other_filesystem(tmp_path: Path):
    """A folder on another disk or file system than ``tmp_path``, or None."""
    here = os.stat(tmp_path).st_dev
    if sys.platform == "win32":
        for letter in "DEFGHIJKLMNOPQRSTUVWXYZ":
            root = f"{letter}:\\"
            if os.path.isdir(root) and os.stat(root).st_dev != here:
                try:
                    d = Path(root) / f"ubunye-f070-{os.getpid()}-{tmp_path.name}"
                    d.mkdir()
                    return d
                except OSError:
                    continue
        return None
    shm = Path("/dev/shm")
    if shm.is_dir() and os.stat(shm).st_dev != here:
        d = shm / f"ubunye-f070-{os.getpid()}-{tmp_path.name}"
        d.mkdir()
        return d
    return None


def test_an_output_mounted_on_another_disk_is_appended_and_taken_back(spark, tmp_path):
    # F-070 skeptic review: the output is a junction (Windows) or symlink to another
    # disk. A staging folder beside it could not be renamed in (WinError 17, EXDEV).
    other = _other_filesystem(tmp_path)
    if other is None:
        pytest.skip("no second disk or file system here")
    try:
        task = _task(tmp_path)
        (tmp_path / "out").mkdir()
        link = tmp_path / "out" / "events"
        if sys.platform == "win32":
            subprocess.run(
                ["cmd", "/c", "mklink", "/J", str(link), str(other)],
                check=True,
                capture_output=True,
            )
        else:
            os.symlink(other, link)
        ubunye.run_task(str(task), dt=DT2, spark=spark)
        ubunye.run_task(str(task), dt=DT2, spark=spark, rerun=True)
        assert _batches(spark, tmp_path) == {DT2: 3}
        assert _debris(tmp_path) == []
    finally:
        import shutil

        if sys.platform == "win32":
            os.rmdir(tmp_path / "out" / "events")  # the junction, not its target
        shutil.rmtree(other, ignore_errors=True)


def test_an_output_with_a_long_folder_name_is_appended(spark, tmp_path):
    # F-070 skeptic review: `.<name>.ubunye-<id>` beside a 240 character folder name
    # was over the 255 character limit. The staging name inside is 20 characters.
    task = _task(tmp_path)
    config = task / "config.yaml"
    config.write_text(
        config.read_text(encoding="utf-8").replace("out/events", "out/" + "e" * 240),
        encoding="utf-8",
    )
    ubunye.run_task(str(task), dt=DT2, spark=spark)
    ubunye.run_task(str(task), dt=DT2, spark=spark, rerun=True)
    assert spark.read.parquet(str(tmp_path / "out" / ("e" * 240))).count() == 3


def test_a_run_that_fails_after_its_append_removes_it(spark, tmp_path):
    # A later output fails the run: its Spark append is taken back (before: it stayed,
    # and the rerun added the batch a second time).
    task = _task(tmp_path, also_missing=True)
    with pytest.raises(Exception, match="zz_missing"):
        ubunye.run_task(str(task), dt=DT2, spark=spark)
    events = tmp_path / "out" / "events"
    assert not (events.exists() and list(events.rglob("*.parquet")))
    assert _debris(tmp_path) == []


def test_two_runs_of_one_batch_at_once_append_it_once(spark, tmp_path):
    # E-02 on Spark: the second run is refused while the first holds the batch.
    task = _task(tmp_path)
    (tmp_path / "gate").unlink()  # the first run waits inside its transform
    first: dict = {}

    def run_first() -> None:
        try:
            ubunye.run_task(str(task), dt=DT2, spark=spark)
        except BaseException as exc:  # noqa: BLE001 (reported by the assert below)
            first["error"] = exc

    thread = threading.Thread(target=run_first)
    thread.start()
    lease_dir = tmp_path / ".ubunye" / "leases"
    for _ in range(600):
        if list(lease_dir.rglob("*.json")):
            break
        thread.join(0.05)
    try:
        with pytest.raises(RunLeaseHeld):
            ubunye.run_task(str(task), dt=DT2, spark=spark)
    finally:
        (tmp_path / "gate").write_text("", encoding="utf-8")
        thread.join(300)
    assert "error" not in first, first.get("error")
    assert _batches(spark, tmp_path) == {DT2: 3}
