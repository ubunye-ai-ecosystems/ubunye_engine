"""An input's source version, at the read and after the hash (F-046).

On Spark an input's hash reads the source again at task end. If the source moved
in between, the digest is of a later state than the one the task read, and the
record used to say nothing. Now it takes the source's version at the read and
again after the hash, and says whether the digest is of what was read.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from ubunye.core.gate import WARN, evaluate
from ubunye.core.runtime import EngineContext
from ubunye.lineage import source_version as sv
from ubunye.lineage.context import RunContext, StepRecord
from ubunye.lineage.recorder import LineageRecorder

# --- fakes: a Spark-like frame, without Spark ---------------------------------------


class _Session:
    def __init__(self, versions=None, fail=None):
        self.versions = list(versions or [])
        self.fail = fail
        self.queries = []

    def sql(self, text):
        self.queries.append(text)
        if self.fail:
            raise RuntimeError(self.fail)
        version = self.versions.pop(0) if len(self.versions) > 1 else self.versions[0]
        row = {"version": version, "timestamp": datetime(2026, 9, 30, 1, 2, 3)}
        return type("R", (), {"collect": lambda self_: [row]})()


class SparkFrame:
    """Enough of a Spark DataFrame: its session and its file index."""

    def __init__(self, files=(), session=None):
        self.sparkSession = session or _Session([0])
        self._files = list(files)

    def inputFiles(self):
        return list(self._files)


SparkFrame.__module__ = "pyspark.sql.classic.dataframe"

FILES = ["file:///data/in/part-0.parquet", "file:///data/in/part-1.parquet"]
PARQUET = {"format": "s3", "path": "/data/in", "file_format": "parquet"}


def _stats(table):
    """A hadoop_stats stand-in that answers from ``table`` (path -> (size, ms))."""
    return lambda frame, paths: {p: table.get(p) for p in paths}


# --- the files version -------------------------------------------------------------------


def test_a_files_version_counts_the_files_and_names_no_machine():
    a = sv.files_version({"/x/a/p0": (10, 1000), "/x/a/p1": (5, 2000)})
    b = sv.files_version({"/y/b/p0": (10, 1000), "/y/b/p1": (5, 2000)})
    assert a["kind"] == "files" and a["files"] == 2 and a["bytes"] == 15
    assert a["latest_modified"].startswith("1970-01-01T00:00:02")
    assert a["listing_hash"] == b["listing_hash"]  # the folder they share is not in it


def test_a_rewritten_or_deleted_file_changes_the_version():
    before = sv.files_version({"/a/p0": (10, 1000), "/a/p1": (5, 2000)})
    rewritten = sv.files_version({"/a/p0": (10, 1500), "/a/p1": (5, 2000)})
    deleted = sv.files_version({"/a/p0": (10, 1000), "/a/p1": None})
    assert sv.changed(before, dict(before, seconds=9.0)) is False  # the cost is not identity
    assert sv.changed(before, rewritten) is True
    assert sv.changed(before, deleted) is True and deleted["missing"] == 1


def test_a_spark_file_input_is_versioned_from_its_file_index():
    table = {FILES[0]: (100, 1_000), FILES[1]: (50, 2_000)}
    version = sv.capture(SparkFrame(FILES), PARQUET, hadoop_stats=_stats(table))
    assert version["kind"] == "files" and version["files"] == 2 and version["bytes"] == 150
    assert version["seconds"] >= 0


def test_a_pandas_read_is_versioned_from_the_files_it_read(tmp_path):
    f = tmp_path / "in.csv"
    f.write_text("id\n1\n", encoding="utf-8")
    frame = type("PandasRead", (), {})()
    frame.source_files = [str(f)]
    version = sv.capture(frame, {"format": "s3", "path": str(f)})
    assert version["kind"] == "files" and version["bytes"] == f.stat().st_size


# --- delta, catalog tables, and what has no version ------------------------------------


def test_a_delta_input_is_versioned_from_its_log():
    session = _Session([7])
    version = sv.capture(SparkFrame(session=session), {"format": "delta", "path": "/d/t"})
    assert version["kind"] == "delta" and version["version"] == 7
    assert session.queries == ["DESCRIBE HISTORY delta.`/d/t` LIMIT 1"]
    via_s3 = sv.capture(
        SparkFrame(session=_Session([3])), {"format": "s3", "path": "/d/t", "file_format": "delta"}
    )
    assert via_s3["version"] == 3


def test_a_pinned_delta_read_is_its_version_and_asks_nothing():
    session = _Session([9])
    cfg = {"format": "delta", "table": "db.t", "version_as_of": 4}
    version = sv.capture(SparkFrame(session=session), cfg)
    assert version["version"] == 4 and version["pinned"] is True
    assert session.queries == []


def test_a_catalog_table_gets_its_delta_version_or_says_it_has_none():
    delta = sv.capture(
        SparkFrame(session=_Session([2])), {"format": "hive", "db_name": "d", "tbl_name": "t"}
    )
    assert delta["kind"] == "delta" and delta["version"] == 2
    plain = sv.capture(
        SparkFrame(session=_Session(fail="not a Delta table")),
        {"format": "unity", "table": "c.s.t"},
    )
    assert plain["kind"] == "none" and "not Delta" in plain["reason"]


def test_what_has_no_version_says_why_and_never_raises():
    sql = sv.capture(SparkFrame(), {"format": "hive", "sql": "SELECT 1"})
    assert sql["kind"] == "none" and "SQL" in sql["reason"]
    jdbc = sv.capture(SparkFrame([]), {"format": "jdbc", "url": "jdbc:x", "table": "t"})
    assert jdbc["kind"] == "none" and "no files" in jdbc["reason"]

    def broken(frame, paths):
        raise OSError("storage said no")

    failed = sv.capture(SparkFrame(FILES), PARQUET, hadoop_stats=broken)
    assert failed["kind"] == "none" and "storage said no" in failed["reason"]
    assert sv.capture(object(), PARQUET) is None  # not a frame it knows: nothing to say


# --- the recorder: checked again after the hash -----------------------------------------


def _record(tmp_path, frame, read_version, cfg=PARQUET):
    rec = LineageRecorder(base_dir=str(tmp_path))
    ctx = EngineContext(run_id="r1", task_name="uc/pkg/t")
    config = {"CONFIG": {"inputs": {"src": cfg}, "outputs": {}}}
    rec.task_start(context=ctx, config=config)
    rec.task_end(
        context=ctx,
        config=config,
        outputs={},
        status="success",
        duration_sec=1.0,
        inputs={"src": frame},
        source_versions={"src": read_version},
    )
    return rec._store.load("uc/pkg/t", "r1").inputs[0]  # type: ignore[attr-defined]


def test_a_source_that_moved_before_the_hash_is_flagged(tmp_path, monkeypatch):
    read = sv.capture(
        SparkFrame(FILES), PARQUET, hadoop_stats=_stats({FILES[0]: (1, 1), FILES[1]: (1, 1)})
    )
    # By the hash, one file was rewritten.
    monkeypatch.setattr(sv, "_hadoop_stats", _stats({FILES[0]: (1, 1), FILES[1]: (2, 5)}))
    step = _record(tmp_path, SparkFrame(FILES), read)
    assert step.hash_basis == "recomputed"
    assert step.source_version == read
    assert step.source_version_at_hash["bytes"] == 3
    assert step.source_changed is True
    assert "not of what the task read" in step.source_note


def test_a_source_that_did_not_move_says_the_digest_is_of_what_was_read(tmp_path, monkeypatch):
    table = {FILES[0]: (1, 1), FILES[1]: (1, 1)}
    read = sv.capture(SparkFrame(FILES), PARQUET, hadoop_stats=_stats(table))
    monkeypatch.setattr(sv, "_hadoop_stats", _stats(table))
    step = _record(tmp_path, SparkFrame(FILES), read)
    assert step.source_changed is False
    assert "This digest is of what the task read" in step.source_note


def test_a_delta_table_appended_to_before_the_hash_is_flagged(tmp_path):
    cfg = {"format": "delta", "path": "/d/t"}
    read = sv.capture(SparkFrame(session=_Session([3])), cfg)
    step = _record(tmp_path, SparkFrame(session=_Session([4])), read, cfg)
    assert step.source_changed is True
    assert step.source_version["version"] == 3 and step.source_version_at_hash["version"] == 4
    assert "Delta version 3 -> Delta version 4" in step.source_note


def test_a_source_without_a_version_is_not_known_and_not_checked_again(tmp_path):
    cfg = {"format": "hive", "sql": "SELECT 1"}
    read = sv.capture(SparkFrame(), cfg)
    step = _record(tmp_path, SparkFrame(), read, cfg)
    assert step.source_changed is None and step.source_version_at_hash is None
    assert "is not known" in step.source_note


def test_an_old_record_loads_with_no_source_fields():
    step = StepRecord.from_dict(
        {"name": "a", "direction": "input", "format": "s3", "location": "x"}
    )
    assert step.source_version is None and step.source_changed is None
    assert step.source_note is None


# --- the gate and compare: that digest is not evidence -------------------------------------


def _run(run_id, data_hash, moved=None):
    ctx = RunContext(
        run_id=run_id,
        task_path="u/p/t",
        usecase="u",
        package="p",
        task_name="t",
        profile="dev",
        model="etl",
        version="1.0.0",
        config_hash="sha256:c",
        started_at="2026-09-30T00:00:00",
        status="success",
    )
    ctx.inputs = [
        StepRecord(
            "src",
            "input",
            "s3",
            "/in",
            data_hash="sha256:i",
            hash_method="rows-v1",
            source_changed=moved,
        )
    ]
    ctx.outputs = [
        StepRecord("out", "output", "s3", "/out", data_hash=data_hash, hash_method="rows-v1")
    ]
    return ctx


def test_the_gate_warns_and_does_not_call_it_nondeterminism():
    findings = evaluate(_run("a", "sha256:1"), _run("b", "sha256:2", moved=True))
    warned = [f for f in findings if f.rule == "input"]
    assert warned and warned[0].status == WARN and warned[0].output == "src"
    data = [f for f in findings if f.rule == "data"][0]
    assert "not deterministic" not in data.detail
    assert "source changed during the run" in data.detail


def test_compare_calls_that_input_digest_unknown():
    from ubunye.cli.lineage import compare_records

    report = compare_records(_run("a", "sha256:1"), _run("b", "sha256:1", moved=True))
    state = report["inputs"]["src"]["data_hash"]
    assert state["state"] == "unknown" and "source changed" in state["why"]


# --- the engine: taken at the read, only when the run is recorded ----------------------------

pd = pytest.importorskip("pandas")
pytest.importorskip("pyarrow")


def _task(root: Path) -> Path:
    task = root / "uc" / "pkg" / "t"
    task.mkdir(parents=True)
    (root / "in.csv").write_text("id,qty\n1,2\n2,0\n", encoding="utf-8")
    (task / "transformations.py").write_text(
        "from ubunye.core.interfaces import Task\n\n\n"
        "class Copy(Task):\n"
        "    def transform(self, sources):\n"
        "        return {'out': sources['src']}\n",
        encoding="utf-8",
    )
    (task / "config.yaml").write_text(
        f"""\
CONFIG:
  inputs:
    src:
      format: s3
      path: "{(root / 'in.csv').as_posix()}"
      file_format: csv
      options: {{header: "true"}}
  transform: {{}}
  outputs:
    out:
      format: s3
      path: "{(root / 'out').as_posix()}"
      file_format: parquet
      mode: overwrite
""",
        encoding="utf-8",
    )
    return task


def test_a_pandas_run_records_the_files_it_read(tmp_path):
    import ubunye

    task = _task(tmp_path)
    ubunye.run_task(str(task), backend="pandas", lineage=True, dt="1")
    [path] = (tmp_path / ".ubunye" / "lineage").rglob("*.json")
    step = RunContext.from_dict(__import__("json").loads(path.read_text()))
    src = step.inputs[0]
    assert src.source_version["kind"] == "files"
    assert src.source_version["files"] == 1
    assert src.source_version["bytes"] == (tmp_path / "in.csv").stat().st_size
    # In memory: the digest is of the rows read, so nothing is checked again.
    assert src.hash_basis == "materialised" and src.source_changed is None


def test_a_plain_run_takes_no_version(tmp_path, monkeypatch):
    import ubunye

    calls = []
    monkeypatch.setattr(sv, "capture", lambda *a, **k: calls.append(a))
    ubunye.run_task(str(_task(tmp_path)), backend="pandas", dt="1")
    assert calls == []
