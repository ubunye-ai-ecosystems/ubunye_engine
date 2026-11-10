"""Run record stores are plugins, found by name, chosen in one order (ADR 011)."""

from __future__ import annotations

import itertools
import json
import types
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytest

from ubunye.core.errors import LineageRecordNotFoundError, PluginNotFoundError
from ubunye.lineage import stores
from ubunye.lineage.context import RunContext
from ubunye.lineage.storage import FileSystemLineageStore
from ubunye.testing.lineage_conformance import LineageStoreConformance, full_record

# ---------------------------------------------------------------------------
# A third party store: no base class, registered by name
# ---------------------------------------------------------------------------


class MemoryStore:
    """A store from another package: duck typed, kept in a dict shared by location."""

    shelves: Dict[str, Dict[Tuple[str, str], Tuple[int, Dict[str, Any]]]] = {}
    _clock = itertools.count()

    def __init__(self, location: str) -> None:
        self.location = location
        self.rows = self.shelves.setdefault(location, {})

    def save(self, ctx: RunContext) -> None:
        self.rows[(ctx.task_path, ctx.run_id)] = (
            next(self._clock),
            json.loads(json.dumps(ctx.to_dict())),
        )

    def load(self, task_path: str, run_id: str) -> RunContext:
        row = self.rows.get((task_path, run_id))
        if row is None:
            raise LineageRecordNotFoundError(f"No run '{run_id}' of '{task_path}'.")
        return RunContext.from_dict(row[1])

    def search(
        self,
        task_path: Optional[str] = None,
        status: Optional[str] = None,
        since: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[RunContext]:
        rows = sorted(self.rows.items(), key=lambda kv: kv[1][0], reverse=True)
        out = []
        for (tp, _), (_, doc) in rows:
            if task_path and not (tp == task_path or tp.startswith(task_path.rstrip("/") + "/")):
                continue
            if status and doc["status"] != status:
                continue
            if since and doc["started_at"] < since:
                continue
            out.append(RunContext.from_dict(doc))
        return out[:limit] if limit is not None else out

    def list_runs(self, task_path: str, n: int = 20) -> List[RunContext]:
        return self.search(task_path=task_path, limit=n)


class NotAStore:
    def __init__(self, location: str) -> None:
        pass


def _ep(name: str, cls: Any) -> Any:
    return types.SimpleNamespace(name=name, value=f"tests:{cls.__name__}", load=lambda: cls)


@pytest.fixture
def memory_registered(monkeypatch):
    """Register MemoryStore as 'memory' beside the installed stores."""
    real = stores._entry_points()
    monkeypatch.setattr(
        stores,
        "_entry_points",
        lambda: real + [_ep("memory", MemoryStore), _ep("broken", NotAStore)],
    )
    MemoryStore.shelves.clear()
    yield
    MemoryStore.shelves.clear()


# ---------------------------------------------------------------------------
# The conformance suite, on the filesystem store and on a third party store
# ---------------------------------------------------------------------------


class TestFileSystemStoreConformance(LineageStoreConformance):
    @pytest.fixture
    def store(self, tmp_path):
        return FileSystemLineageStore(str(tmp_path / "lineage"))


class TestThirdPartyStoreConformance(LineageStoreConformance):
    pause = 0.0

    @pytest.fixture
    def store(self, tmp_path, memory_registered):
        return stores.open_store(str(tmp_path / "memory"), "memory")


# ---------------------------------------------------------------------------
# The resolution order
# ---------------------------------------------------------------------------


def test_the_default_is_filesystem(monkeypatch):
    monkeypatch.delenv(stores.ENV_VAR, raising=False)
    assert stores.backend_name() == "filesystem"
    assert stores.backend_name(None) == "filesystem"


def test_the_env_var_beats_the_default(monkeypatch):
    monkeypatch.setenv(stores.ENV_VAR, "Delta")
    assert stores.backend_name() == "delta"


def test_an_explicit_name_beats_the_env_var(monkeypatch):
    monkeypatch.setenv(stores.ENV_VAR, "delta")
    assert stores.backend_name("filesystem") == "filesystem"


def test_the_shipped_stores_are_registered():
    assert {"filesystem", "delta"} <= set(stores.available())
    assert stores.load_class("filesystem") is FileSystemLineageStore


def test_an_unknown_name_lists_the_installed_ones(monkeypatch):
    monkeypatch.delenv(stores.ENV_VAR, raising=False)
    with pytest.raises(PluginNotFoundError) as info:
        stores.open_store("x", "nope")
    text = str(info.value)
    assert "No lineage backend named 'nope'" in text
    assert "filesystem" in text and "delta" in text
    assert "pip install" in text


def test_a_class_that_cannot_store_records_is_refused(memory_registered):
    with pytest.raises(PluginNotFoundError, match="cannot store run records"):
        stores.load_class("broken")


def test_the_filesystem_location_keeps_todays_layout(tmp_path, monkeypatch):
    monkeypatch.delenv(stores.ENV_VAR, raising=False)
    assert stores.location(tmp_path) == str(tmp_path / ".ubunye" / "lineage")
    assert stores.location(tmp_path, "runs") == str(tmp_path / "runs")


def test_another_store_gets_its_location_unchanged(tmp_path):
    assert stores.location(tmp_path, "main.ops.ubunye_runs", "delta") == "main.ops.ubunye_runs"


def test_the_location_is_given_to_the_constructor_unchanged(memory_registered):
    store = stores.open_store("s3://bucket/runs", "memory")
    assert isinstance(store, MemoryStore) and store.location == "s3://bucket/runs"


# ---------------------------------------------------------------------------
# What runs uses the resolver
# ---------------------------------------------------------------------------


def _run_recorder(recorder, task_name="shop/orders/clean", run_id="run-1"):
    ctx = types.SimpleNamespace(
        run_id=run_id, task_name=task_name, profile="dev", variables={"dt": "2026-11-01"}
    )
    cfg = {"MODEL": "etl", "VERSION": "0.1.0", "CONFIG": {"inputs": {}, "outputs": {}}}
    recorder.task_start(context=ctx, config=cfg)
    recorder.task_end(context=ctx, config=cfg, outputs={}, status="success", duration_sec=1.0)


def test_the_recorder_saves_through_the_named_store(memory_registered):
    from ubunye.lineage.recorder import LineageRecorder

    recorder = LineageRecorder(base_dir="shared", backend="memory")
    _run_recorder(recorder)
    saved = MemoryStore("shared").load("shop/orders/clean", "run-1")
    assert saved.status == "success" and saved.variables == {"dt": "2026-11-01"}


def test_the_recorder_follows_the_env_var(memory_registered, monkeypatch):
    from ubunye.lineage.recorder import LineageRecorder

    monkeypatch.setenv(stores.ENV_VAR, "memory")
    _run_recorder(LineageRecorder(base_dir="shared"))
    assert MemoryStore("shared").load("shop/orders/clean", "run-1").status == "success"


def test_the_recorder_default_is_unchanged(tmp_path, monkeypatch):
    from ubunye.lineage.recorder import LineageRecorder

    monkeypatch.delenv(stores.ENV_VAR, raising=False)
    _run_recorder(LineageRecorder(base_dir=str(tmp_path / "lin")))
    assert (tmp_path / "lin" / "shop" / "orders" / "clean" / "run-1.json").exists()


def test_the_old_store_argument_still_works_with_a_warning(memory_registered):
    from ubunye.lineage.recorder import LineageRecorder

    with pytest.warns(DeprecationWarning, match="backend="):
        recorder = LineageRecorder(store="memory", base_dir="old")
    _run_recorder(recorder)
    assert MemoryStore("old").load("shop/orders/clean", "run-1").status == "success"


def test_the_recorder_refuses_an_unknown_store_when_built():
    from ubunye.lineage.recorder import LineageRecorder

    with pytest.raises(PluginNotFoundError):
        LineageRecorder(backend="nope")


def _seed(memory_location: str) -> RunContext:
    record = full_record()
    MemoryStore(memory_location).save(record)
    return record


def _strip(text: str) -> str:
    import re

    return re.sub(r"\x1b\[[0-9;]*m", "", text)


def test_lineage_cli_reads_the_named_store(memory_registered, tmp_path):
    from typer.testing import CliRunner

    from ubunye.cli.main import app

    record = _seed("main.ops.runs")
    args = ["-d", str(tmp_path), "-u", "shop", "-p", "orders", "-t", "clean"]
    store_args = ["--lineage-backend", "memory", "--lineage-dir", "main.ops.runs"]
    res = CliRunner().invoke(app, ["lineage", "list", *args, *store_args, "--json"])
    assert res.exit_code == 0, res.output
    assert [r["run_id"] for r in json.loads(res.output)] == [record.run_id]
    res = CliRunner().invoke(
        app, ["lineage", "show", *args, *store_args, "--run-id", record.run_id, "--json"]
    )
    assert res.exit_code == 0, res.output
    assert json.loads(res.output) == record.to_dict()
    res = CliRunner().invoke(app, ["lineage", "search", "-d", str(tmp_path), *store_args, "--json"])
    assert res.exit_code == 0, res.output
    assert [r["run_id"] for r in json.loads(res.output)] == [record.run_id]


def test_lineage_cli_follows_the_env_var(memory_registered, tmp_path, monkeypatch):
    from typer.testing import CliRunner

    from ubunye.cli.main import app

    monkeypatch.setenv(stores.ENV_VAR, "memory")
    record = _seed("main.ops.runs")
    res = CliRunner().invoke(
        app,
        ["lineage", "list", "-d", str(tmp_path), "-u", "shop", "-p", "orders", "-t", "clean"]
        + ["--lineage-dir", "main.ops.runs", "--json"],
    )
    assert res.exit_code == 0, res.output
    assert [r["run_id"] for r in json.loads(res.output)] == [record.run_id]


def test_lineage_cli_names_an_unknown_store(tmp_path):
    from typer.testing import CliRunner

    from ubunye.cli.main import app

    res = CliRunner().invoke(
        app,
        ["lineage", "list", "-d", str(tmp_path), "-u", "a", "-p", "b", "-t", "c"]
        + ["--lineage-backend", "nope", "--json"],
    )
    assert res.exit_code != 0
    assert "No lineage backend named 'nope'" in _strip(res.output)


def test_gate_reads_the_named_store(memory_registered, tmp_path):
    from typer.testing import CliRunner

    from ubunye.cli.main import app

    base = full_record(started_at="2026-11-01T10:00:00+00:00")
    cand = full_record(started_at="2026-11-02T10:00:00+00:00")
    MemoryStore("runs").save(base)
    MemoryStore("runs").save(cand)
    res = CliRunner().invoke(
        app,
        ["gate", "-d", str(tmp_path), "-u", "shop", "-p", "orders", "-t", "clean"]
        + ["--lineage-backend", "memory", "--lineage-dir", "runs", "--json"],
    )
    assert res.exit_code == 0, res.output
    doc = json.loads(res.output)
    assert doc["baseline"]["run_id"] == base.run_id
    assert doc["candidate"]["run_id"] == cand.run_id


def test_mcp_server_reads_the_named_store(memory_registered, tmp_path):
    from ubunye.mcp_server import _Tools

    record = _seed("main.ops.runs")
    tools = _Tools(tmp_path, lineage_dir="main.ops.runs", lineage_backend="memory")
    assert isinstance(tools._store(), MemoryStore)
    assert tools._store().load("shop/orders/clean", record.run_id).to_dict() == record.to_dict()


def test_run_task_records_through_the_named_store(memory_registered, tmp_path):
    pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    from ubunye.api import run_task

    task = tmp_path / "uc" / "shop" / "orders" / "clean"
    task.mkdir(parents=True)
    (tmp_path / "in.csv").write_text("id\n1\n2\n", encoding="utf-8")
    (task / "config.yaml").write_text(
        "VERSION: 0.1.0\nCONFIG:\n  inputs:\n"
        f"    src: {{format: s3, path: '{(tmp_path / 'in.csv').as_posix()}', file_format: csv,"
        " options: {header: 'true'}}\n  outputs:\n"
        f"    out: {{format: s3, path: '{(tmp_path / 'out').as_posix()}', file_format: parquet,"
        " mode: overwrite}\n",
        encoding="utf-8",
    )
    (task / "transformations.py").write_text(
        "from ubunye.core.interfaces import Task\n"
        "class T(Task):\n"
        "    def transform(self, sources):\n"
        "        return {'out': sources['src']}\n",
        encoding="utf-8",
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        run_task(
            str(task),
            backend="pandas",
            lineage=True,
            lineage_dir="main.ops.runs",
            lineage_backend="memory",
        )
    (saved,) = MemoryStore("main.ops.runs").search(task_path="shop/orders/clean")
    assert saved.status == "success"
    assert saved.outputs[0].row_count == 2
    # Nothing was written to the filesystem store.
    assert not (tmp_path / "uc" / ".ubunye" / "lineage").exists()


def test_filesystem_records_are_where_they_always_were(tmp_path, monkeypatch):
    monkeypatch.delenv(stores.ENV_VAR, raising=False)
    store = stores.open_for(tmp_path)
    record = full_record()
    store.save(record)
    assert Path(tmp_path, ".ubunye/lineage/shop/orders/clean", f"{record.run_id}.json").exists()


def test_sync_replays_whole_records_into_the_named_store(memory_registered, tmp_path, monkeypatch):
    from typer.testing import CliRunner

    from ubunye.cli import sync
    from ubunye.cli.main import app

    monkeypatch.setattr(sync, "_FALLBACK_ROOT", tmp_path / "fallback")
    monkeypatch.setattr(sync, "_SYNCED_ROOT", tmp_path / "fallback" / "synced")
    record = full_record(status="running")
    manifest = tmp_path / "fallback" / record.run_id / "lineage.jsonl"
    manifest.parent.mkdir(parents=True)
    finished = full_record(status="success", run_id=record.run_id)
    with manifest.open("w", encoding="utf-8") as fh:
        for r in (record, finished):
            fh.write(json.dumps({**r.to_dict(), "_fallback_at": "now"}) + "\n")
    res = CliRunner().invoke(
        app, ["sync", "lineage", "--lineage-backend", "memory", "--lineage-dir", "main.ops.runs"]
    )
    assert res.exit_code == 0, res.output
    saved = MemoryStore("main.ops.runs").load(record.task_path, record.run_id)
    assert saved.to_dict() == finished.to_dict()


# ---------------------------------------------------------------------------
# The Delta store without Spark: it says so, it never writes local files quietly
# ---------------------------------------------------------------------------


def test_delta_tells_a_table_from_a_path():
    from ubunye.lineage.delta_store import is_path

    assert not is_path("main.ops.ubunye_runs")
    assert not is_path("ubunye_runs")
    for p in ("s3://bucket/runs", "/dbfs/runs", ".ubunye/lineage", "C:\\runs", "dbfs:/runs"):
        assert is_path(p)


def test_a_delta_record_that_cannot_be_written_is_kept_and_said(tmp_path, monkeypatch, caplog):
    from ubunye.lineage import delta_store

    monkeypatch.setattr("ubunye._internal.metadata_worker._FALLBACK_ROOT", tmp_path / "fb")
    store = delta_store.DeltaLineageBackend("main.ops.runs")

    def no_spark(ctx):
        raise ImportError("no pyspark here")

    monkeypatch.setattr(store, "_append", no_spark)
    record = full_record()
    with caplog.at_level("WARNING", logger="ubunye.lineage.delta_store"):
        store.save(record)
    manifest = tmp_path / "fb" / record.run_id / "lineage.jsonl"
    (line,) = manifest.read_text(encoding="utf-8").splitlines()
    doc = json.loads(line)
    assert RunContext.from_dict(doc).to_dict() == record.to_dict()
    assert doc["_lineage_backend"] == "delta" and doc["_location"] == "main.ops.runs"
    assert "was not written to the Delta lineage store main.ops.runs" in caplog.text
    assert "ubunye sync lineage --lineage-backend delta" in caplog.text
    assert not (Path.cwd() / "main.ops.runs").exists()
