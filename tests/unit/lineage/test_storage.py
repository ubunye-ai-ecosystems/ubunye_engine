"""Unit tests for FileSystemLineageStore."""
import json
import time

import pytest

from ubunye.lineage.context import RunContext, StepRecord
from ubunye.lineage.storage import FileSystemLineageStore, S3LineageStore


def _ctx(
    run_id="run-1", task="fraud/ingestion/etl", status="success", started="2025-03-01T10:00:00Z"
):
    parts = task.split("/")
    usecase, package, task_name = parts[0], parts[1], parts[2]
    return RunContext(
        run_id=run_id,
        task_path=task,
        usecase=usecase,
        package=package,
        task_name=task_name,
        profile="dev",
        model="etl",
        version="0.1.0",
        config_hash="sha256:abc",
        started_at=started,
        status=status,
        duration_sec=1.5,
    )


class TestFileSystemLineageStore:

    # ------------------------------------------------------------------
    # save / load
    # ------------------------------------------------------------------

    def test_save_creates_json_file(self, tmp_path):
        store = FileSystemLineageStore(str(tmp_path))
        ctx = _ctx()
        store.save(ctx)
        expected = tmp_path / "fraud" / "ingestion" / "etl" / "run-1.json"
        assert expected.exists()

    def test_save_writes_valid_json(self, tmp_path):
        store = FileSystemLineageStore(str(tmp_path))
        ctx = _ctx()
        store.save(ctx)
        p = tmp_path / "fraud" / "ingestion" / "etl" / "run-1.json"
        d = json.loads(p.read_text())
        assert d["run_id"] == "run-1"
        assert d["status"] == "success"

    def test_load_round_trips_run_context(self, tmp_path):
        store = FileSystemLineageStore(str(tmp_path))
        ctx = _ctx(run_id="abc-xyz")
        store.save(ctx)
        ctx2 = store.load("fraud/ingestion/etl", "abc-xyz")
        assert ctx2.run_id == "abc-xyz"
        assert ctx2.status == "success"
        assert ctx2.duration_sec == 1.5

    def test_load_with_step_records(self, tmp_path):
        store = FileSystemLineageStore(str(tmp_path))
        ctx = _ctx()
        ctx.inputs = [StepRecord("src", "input", "hive", "raw.claims", row_count=100)]
        ctx.outputs = [StepRecord("sink", "output", "s3", "s3a://b/k", row_count=90)]
        store.save(ctx)
        ctx2 = store.load("fraud/ingestion/etl", "run-1")
        assert len(ctx2.inputs) == 1
        assert ctx2.inputs[0].row_count == 100
        assert len(ctx2.outputs) == 1
        assert ctx2.outputs[0].location == "s3a://b/k"

    def test_load_raises_file_not_found(self, tmp_path):
        store = FileSystemLineageStore(str(tmp_path))
        with pytest.raises(FileNotFoundError):
            store.load("fraud/ingestion/etl", "no-such-run")

    def test_save_overwrites_existing(self, tmp_path):
        store = FileSystemLineageStore(str(tmp_path))
        ctx = _ctx(status="running")
        store.save(ctx)
        ctx.status = "success"
        store.save(ctx)
        ctx2 = store.load("fraud/ingestion/etl", "run-1")
        assert ctx2.status == "success"

    # ------------------------------------------------------------------
    # list_runs
    # ------------------------------------------------------------------

    def test_list_runs_empty_returns_empty(self, tmp_path):
        store = FileSystemLineageStore(str(tmp_path))
        assert store.list_runs("fraud/ingestion/etl") == []

    def test_list_runs_returns_records_newest_first(self, tmp_path):
        store = FileSystemLineageStore(str(tmp_path))
        for i, run_id in enumerate(["run-a", "run-b", "run-c"]):
            store.save(_ctx(run_id=run_id, started=f"2025-03-0{i+1}T10:00:00Z"))
            time.sleep(0.02)  # ensure distinct mtimes

        runs = store.list_runs("fraud/ingestion/etl")
        assert len(runs) == 3
        assert runs[0].run_id == "run-c"  # most recently written

    def test_list_runs_n_limits_results(self, tmp_path):
        store = FileSystemLineageStore(str(tmp_path))
        for i in range(5):
            store.save(_ctx(run_id=f"run-{i}"))
        runs = store.list_runs("fraud/ingestion/etl", n=2)
        assert len(runs) == 2

    def test_list_runs_unknown_task_returns_empty(self, tmp_path):
        store = FileSystemLineageStore(str(tmp_path))
        assert store.list_runs("nonexistent/pkg/task") == []

    # ------------------------------------------------------------------
    # search
    # ------------------------------------------------------------------

    def test_search_no_filters_returns_all(self, tmp_path):
        store = FileSystemLineageStore(str(tmp_path))
        store.save(_ctx(run_id="r1", task="fraud/ingestion/etl", status="success"))
        store.save(_ctx(run_id="r2", task="fraud/ingestion/etl", status="error"))
        runs = store.search()
        assert len(runs) == 2

    def test_search_by_status(self, tmp_path):
        store = FileSystemLineageStore(str(tmp_path))
        store.save(_ctx(run_id="r1", status="success"))
        store.save(_ctx(run_id="r2", status="error"))
        errs = store.search(status="error")
        assert len(errs) == 1
        assert errs[0].run_id == "r2"

    def test_search_since_filters_old_runs(self, tmp_path):
        store = FileSystemLineageStore(str(tmp_path))
        store.save(_ctx(run_id="old", started="2024-01-01T00:00:00Z"))
        store.save(_ctx(run_id="new", started="2025-06-01T00:00:00Z"))
        results = store.search(since="2025-01-01")
        assert len(results) == 1
        assert results[0].run_id == "new"

    def test_search_by_task_path(self, tmp_path):
        store = FileSystemLineageStore(str(tmp_path))
        store.save(_ctx(run_id="r1", task="fraud/ingestion/etl"))
        store.save(_ctx(run_id="r2", task="fraud/ingestion/load"))
        results = store.search(task_path="fraud/ingestion/etl")
        assert len(results) == 1
        assert results[0].run_id == "r1"

    def test_search_empty_store_returns_empty(self, tmp_path):
        store = FileSystemLineageStore(str(tmp_path))
        assert store.search() == []


# ---------------------------------------------------------------------------
# S3LineageStore stub
# ---------------------------------------------------------------------------


class TestS3LineageStoreStub:
    def test_save_raises_not_implemented(self):
        store = S3LineageStore("s3://bucket/lineage")
        with pytest.raises(NotImplementedError):
            store.save(_ctx())

    def test_load_raises_not_implemented(self):
        store = S3LineageStore("s3://bucket/lineage")
        with pytest.raises(NotImplementedError):
            store.load("fraud/ingestion/etl", "run-1")

    def test_list_runs_raises_not_implemented(self):
        store = S3LineageStore("s3://bucket/lineage")
        with pytest.raises(NotImplementedError):
            store.list_runs("fraud/ingestion/etl")

    def test_search_raises_not_implemented(self):
        store = S3LineageStore("s3://bucket/lineage")
        with pytest.raises(NotImplementedError):
            store.search()
