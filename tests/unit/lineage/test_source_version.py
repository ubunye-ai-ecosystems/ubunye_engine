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
    """A Delta table's history: ``DESCRIBE HISTORY ... LIMIT n`` gives the n newest commits."""

    def __init__(self, versions=None, fail=None):
        latest = (list(versions or [0]) or [0])[0]
        self.history = [(v, "WRITE") for v in range(latest + 1)]
        self.fail = fail
        self.queries = []

    def commit(self, operation="WRITE"):
        self.history.append((len(self.history), operation))

    def sql(self, text):
        self.queries.append(text)
        if self.fail:
            raise RuntimeError(self.fail)
        n = int(text.rsplit("LIMIT", 1)[1])
        rows = [
            {"version": v, "operation": op, "timestamp": datetime(2026, 9, 30, 1, 2, 3)}
            for v, op in reversed(self.history[-n:])
        ]
        return type("R", (), {"collect": lambda self_: rows})()


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
    return lambda frame, paths, **_: {p: table.get(p) for p in paths}


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
    assert version["pinned_by"] == "config"
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


def test_without_etags_unchanged_means_names_sizes_and_times_only(tmp_path, monkeypatch):
    """Skeptic bug 1: a same size, same time rewrite passed as 'of what the task read'."""
    table = {FILES[0]: (1, 1), FILES[1]: (1, 1)}
    read = sv.capture(SparkFrame(FILES), PARQUET, hadoop_stats=_stats(table))
    monkeypatch.setattr(sv, "_hadoop_stats", _stats(table))
    step = _record(tmp_path, SparkFrame(FILES), read)
    assert step.source_changed is False and step.source_version["etags"] is False
    assert "This digest is of what the task read" not in step.source_note
    assert "sizes and modification times are unchanged" in step.source_note
    assert "cannot be ruled out" in step.source_note


def test_with_etags_a_same_size_same_time_rewrite_is_seen(tmp_path, monkeypatch):
    read = sv.capture(
        SparkFrame(FILES),
        PARQUET,
        hadoop_stats=_stats({FILES[0]: (1, 1, "e0"), FILES[1]: (1, 1, "e1")}),
    )
    assert read["etags"] is True
    monkeypatch.setattr(
        sv, "_hadoop_stats", _stats({FILES[0]: (1, 1, "e0"), FILES[1]: (1, 1, "e2")})
    )
    step = _record(tmp_path, SparkFrame(FILES), read)
    assert step.source_changed is True
    monkeypatch.setattr(
        sv, "_hadoop_stats", _stats({FILES[0]: (1, 1, "e0"), FILES[1]: (1, 1, "e1")})
    )
    same = _record(tmp_path / "again", SparkFrame(FILES), read)
    assert same.source_changed is False
    assert "This digest is of what the task read" in same.source_note


def test_an_unpinned_delta_table_appended_to_before_the_hash_is_flagged(tmp_path):
    cfg = {"format": "hive", "db_name": "d", "tbl_name": "t"}  # a catalog table: not pinned
    session = _Session([3])
    frame = SparkFrame(session=session)
    read = sv.capture(frame, cfg)
    session.commit("WRITE")
    step = _record(tmp_path, frame, read, cfg)
    assert step.source_changed is True
    assert step.source_version["version"] == 3 and step.source_version_at_hash["version"] == 4
    assert step.source_version_at_hash["commits_since_read"] == ["WRITE"]
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


# --- skeptic review (2026-09-30): one test per confirmed bug ---------------------------------


@pytest.mark.parametrize(
    "cfg",
    [
        {"format": "delta", "path": "/d/t", "options": {"versionAsOf": 0}},
        {"format": "delta", "path": "/d/t", "options": {"VERSIONASOF": "0"}},
        {"format": "s3", "path": "/d/t", "file_format": "delta", "options": {"versionasof": 0}},
    ],
)
def test_a_pin_in_options_is_a_pin_in_any_case(cfg):
    """Skeptic bug 2: options.versionAsOf was ignored, so the latest version was recorded."""
    session = _Session([5])
    version = sv.capture(SparkFrame(session=session), cfg)
    assert version["version"] == 0 and version["pinned"] is True
    assert session.queries == []


class _Reader:
    """Enough of spark.read: records the options and returns a markable frame."""

    def __init__(self, session):
        self.session, self.given = session, {}

    def format(self, name):
        return self

    def option(self, key, value):
        self.given[key] = value
        return self

    def options(self, **kw):
        self.given.update(kw)
        return self

    def schema(self, ddl):
        return self

    def load(self, path):
        return SparkFrame(session=self.session)

    table = load


def _spark_with_reader(version):
    session = _Session([version])
    session.read = _Reader(session)
    return session


def test_the_delta_reader_pins_an_unpinned_read_to_the_version_it_saw():
    """Skeptic bug 3: two outputs of one Delta input read two versions (10 and 13 rows)."""
    from ubunye.adapters.spark.delta_pin import ATTR
    from ubunye.plugins.readers.delta import DeltaReader

    spark = _spark_with_reader(7)
    backend = type("B", (), {"spark": spark})()
    frame = DeltaReader().read({"path": "/d/t"}, backend)
    assert spark.read.given["versionAsOf"] == "7"
    assert getattr(frame, ATTR)["version"] == 7
    version = sv.capture(frame, {"format": "delta", "path": "/d/t"})
    assert version["pinned"] is True and version["pinned_by"] == "engine"


def test_a_user_pin_is_left_as_written():
    from ubunye.adapters.spark.delta_pin import ATTR
    from ubunye.plugins.readers.delta import DeltaReader

    spark = _spark_with_reader(7)
    backend = type("B", (), {"spark": spark})()
    cfg = {"path": "/d/t", "options": {"timestampasof": "2026-01-01"}}
    frame = DeltaReader().read(cfg, backend)
    assert "versionAsOf" not in spark.read.given
    assert not hasattr(frame, ATTR) and spark.queries == []


def test_a_delta_path_read_through_s3_is_pinned_too():
    from ubunye.adapters.spark import frame_io
    from ubunye.adapters.spark.delta_pin import ATTR

    spark = _spark_with_reader(3)
    frame = frame_io.read_frame(spark, "delta", "/d/t")
    assert getattr(frame, ATTR)["version"] == 3
    parquet = frame_io.read_frame(spark, "parquet", "/p")
    assert not hasattr(parquet, ATTR)


def test_a_pinned_read_is_not_changed_when_the_table_moves(tmp_path):
    session = _Session([3])
    frame = SparkFrame(session=session)
    frame.ubunye_source_pin = {"version": 3, "timestamp": "t", "pinned_by": "engine"}
    cfg = {"format": "delta", "path": "/d/t"}
    read = sv.capture(frame, cfg)
    session.commit("WRITE")
    step = _record(tmp_path, frame, read, cfg)
    assert step.source_changed is False
    assert step.source_version_at_hash["latest_version"] == 4
    assert "pinned to Delta version 3 (by the engine)" in step.source_note
    assert "table was at version 4 by the hash" in step.source_note


class _JPath:
    def __init__(self, uri):
        self.uri = uri

    def getName(self):
        return self.uri.rsplit("/", 1)[-1]

    def getFileSystem(self, conf):
        return _FS.current


class _JStatus:
    def __init__(self, uri, size=10, mtime=1000):
        self.p, self.s, self.m = _JPath(uri), size, mtime

    def getPath(self):
        return self.p

    def getLen(self):
        return self.s

    def getModificationTime(self):
        return self.m

    def getEtag(self):
        raise RuntimeError("Method getEtag([]) does not exist")


class _FS:
    current = None

    def __init__(self, error=None, missing=()):
        self.error, self.missing = error, set(missing)
        self.calls = {"getFileStatus": 0, "listStatus": 0}
        _FS.current = self

    def getFileStatus(self, jpath):
        self.calls["getFileStatus"] += 1
        if self.error:
            raise RuntimeError(self.error)
        if jpath.uri in self.missing:
            raise RuntimeError("java.io.FileNotFoundException: " + jpath.uri)
        return _JStatus(jpath.uri)

    def listStatus(self, jpath):
        self.calls["listStatus"] += 1
        if self.error:
            raise RuntimeError(self.error)
        return [_JStatus(f) for f in _JVMFrame.files if f.startswith(jpath.uri + "/")]


class _JVM:
    class org:
        class apache:
            class hadoop:
                class fs:
                    Path = _JPath

    class java:
        class net:
            URI = staticmethod(lambda s: s)


class _JVMSession:
    _jvm = _JVM()
    _jsc = type("JSC", (), {"hadoopConfiguration": lambda self: None})()


class _JVMFrame:
    files: list = []
    sparkSession = _JVMSession()

    def inputFiles(self):
        return list(_JVMFrame.files)


def test_a_listing_error_is_no_version_not_missing_files():
    """Skeptic bug 4: one 503 on the listing read as 'every file is missing': changed."""
    _JVMFrame.files = ["s3a://b/in/a.parquet", "s3a://b/in/b.parquet"]
    _FS()
    before = sv.capture(_JVMFrame(), PARQUET)
    assert before["kind"] == "files" and "missing" not in before
    _FS(error="503 SlowDown (transient)")
    after = sv.capture(_JVMFrame(), PARQUET)
    assert after["kind"] == "none" and "listing failed" in after["reason"]
    assert sv.changed(before, after) is None
    _FS(missing={"s3a://b/in/b.parquet"})  # a definite "not found" is still missing
    gone = sv.capture(_JVMFrame(), PARQUET)
    assert gone["missing"] == 1 and sv.changed(before, gone) is True


def test_few_files_are_asked_one_by_one_and_many_by_folder(monkeypatch):
    """Skeptic bug 5 (ii): one file read from a big folder listed the whole folder."""
    _JVMFrame.files = ["s3a://b/in/a.parquet"]
    fs = _FS()
    sv.capture(_JVMFrame(), PARQUET)
    assert fs.calls == {"getFileStatus": 1, "listStatus": 0}
    monkeypatch.setattr(sv, "PER_FILE_LIMIT", 1)
    _JVMFrame.files = ["s3a://b/in/a.parquet", "s3a://b/in/b.parquet"]
    fs = _FS()
    v = sv.capture(_JVMFrame(), PARQUET)
    assert fs.calls == {"getFileStatus": 0, "listStatus": 1} and v["files"] == 2


def test_a_version_that_takes_too_long_is_no_version(monkeypatch):
    """Skeptic bug 5 (iii): no bound on how long taking a version could take."""
    monkeypatch.setenv("UBUNYE_SOURCE_VERSION_TIMEOUT", "0")
    _JVMFrame.files = ["s3a://b/in/a.parquet"]
    _FS()
    v = sv.capture(_JVMFrame(), PARQUET)
    assert v["kind"] == "none" and "took longer than 0 s" in v["reason"]


def test_no_version_is_taken_when_inputs_are_not_hashed(tmp_path, monkeypatch):
    """Skeptic bug 5 (i): hash_inputs=False still paid for the source versions."""
    import ubunye
    from ubunye.telemetry.hooks.monitors import MonitorHook

    calls = []
    monkeypatch.setattr(sv, "capture", lambda *a, **k: calls.append(a))
    rec = LineageRecorder(base_dir=str(tmp_path / "rec"), hash_inputs=False)
    ubunye.run_task(str(_task(tmp_path)), backend="pandas", dt="1", hooks=[MonitorHook(rec)])
    assert calls == []
    assert LineageRecorder(base_dir=str(tmp_path)).reads_inputs is True


def test_an_optimize_since_the_read_is_not_a_change(tmp_path):
    """Skeptic bug 6: OPTIMIZE (same rows) was called 'the source changed'."""
    cfg = {"format": "unity", "table": "c.s.t"}  # a catalog Delta table: not pinned
    session = _Session([2])
    frame = SparkFrame(session=session)
    read = sv.capture(frame, cfg)
    session.commit("OPTIMIZE")
    session.commit("VACUUM START")
    step = _record(tmp_path, frame, read, cfg)
    assert step.source_changed is False
    assert step.source_version_at_hash["commits_since_read"] == ["OPTIMIZE", "VACUUM START"]
    assert "change no rows: OPTIMIZE, VACUUM START" in step.source_note
