"""Unit tests for LineageRecorder (Monitor protocol integration)."""
import pytest

from ubunye.lineage.recorder import LineageRecorder
from ubunye.lineage.storage import FileSystemLineageStore

# ---------------------------------------------------------------------------
# Minimal stubs
# ---------------------------------------------------------------------------


class FakeContext:
    """Minimal stand-in for EngineContext."""

    def __init__(self, run_id="run-abc", task_name="fraud/ingestion/claim_etl", profile="dev"):
        self.run_id = run_id
        self.task_name = task_name
        self.profile = profile


_BASE_CONFIG = {
    "MODEL": "etl",
    "VERSION": "0.1.0",
    "CONFIG": {
        "inputs": {"source": {"format": "hive", "db_name": "raw", "tbl_name": "claims"},},
        "outputs": {"sink": {"format": "s3", "path": "s3a://bucket/out"},},
    },
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_recorder(tmp_path) -> tuple[LineageRecorder, FileSystemLineageStore, str]:
    base = str(tmp_path / "lineage")
    recorder = LineageRecorder(store="filesystem", base_dir=base)
    store = FileSystemLineageStore(base)
    return recorder, store, base


# ---------------------------------------------------------------------------
# task_start
# ---------------------------------------------------------------------------


class TestLineageRecorderStart:
    def test_task_start_creates_running_record(self, tmp_path):
        recorder, store, _ = _make_recorder(tmp_path)
        ctx = FakeContext()
        recorder.task_start(context=ctx, config=_BASE_CONFIG)

        runs = store.list_runs("fraud/ingestion/claim_etl")
        assert len(runs) == 1
        assert runs[0].status == "running"

    def test_task_start_sets_run_id(self, tmp_path):
        recorder, store, _ = _make_recorder(tmp_path)
        ctx = FakeContext(run_id="unique-id-123")
        recorder.task_start(context=ctx, config=_BASE_CONFIG)

        run = store.load("fraud/ingestion/claim_etl", "unique-id-123")
        assert run.run_id == "unique-id-123"

    def test_task_start_populates_model_and_version(self, tmp_path):
        recorder, store, _ = _make_recorder(tmp_path)
        recorder.task_start(context=FakeContext(), config=_BASE_CONFIG)

        run = store.load("fraud/ingestion/claim_etl", "run-abc")
        assert run.model == "etl"
        assert run.version == "0.1.0"

    def test_task_start_sets_config_hash(self, tmp_path):
        recorder, store, _ = _make_recorder(tmp_path)
        recorder.task_start(context=FakeContext(), config=_BASE_CONFIG)

        run = store.load("fraud/ingestion/claim_etl", "run-abc")
        assert run.config_hash.startswith("sha256:")

    def test_task_start_sets_started_at(self, tmp_path):
        recorder, store, _ = _make_recorder(tmp_path)
        recorder.task_start(context=FakeContext(), config=_BASE_CONFIG)

        run = store.load("fraud/ingestion/claim_etl", "run-abc")
        assert run.started_at  # non-empty ISO string

    def test_task_start_parses_short_task_name(self, tmp_path):
        """A plain task name (not usecase/package/task) should not crash."""
        recorder, store, _ = _make_recorder(tmp_path)
        ctx = FakeContext(task_name="claim_etl")
        recorder.task_start(context=ctx, config=_BASE_CONFIG)
        # Should succeed without error; task_path may be simplified
        assert len(recorder._runs) == 1


# ---------------------------------------------------------------------------
# task_end
# ---------------------------------------------------------------------------


class TestLineageRecorderEnd:
    def _run(self, tmp_path, status="success", outputs=None):
        recorder, store, _ = _make_recorder(tmp_path)
        ctx = FakeContext()
        recorder.task_start(context=ctx, config=_BASE_CONFIG)
        recorder.task_end(
            context=ctx, config=_BASE_CONFIG, outputs=outputs, status=status, duration_sec=3.7,
        )
        return store.load("fraud/ingestion/claim_etl", "run-abc")

    def test_task_end_sets_success_status(self, tmp_path):
        run = self._run(tmp_path, status="success")
        assert run.status == "success"

    def test_task_end_sets_error_status(self, tmp_path):
        run = self._run(tmp_path, status="error")
        assert run.status == "error"

    def test_task_end_sets_duration(self, tmp_path):
        run = self._run(tmp_path)
        assert run.duration_sec == pytest.approx(3.7, abs=0.01)

    def test_task_end_sets_ended_at(self, tmp_path):
        run = self._run(tmp_path)
        assert run.ended_at is not None

    def test_task_end_builds_input_step_records(self, tmp_path):
        run = self._run(tmp_path)
        assert len(run.inputs) == 1
        assert run.inputs[0].name == "source"
        assert run.inputs[0].format == "hive"
        assert run.inputs[0].location == "raw.claims"

    def test_task_end_builds_output_step_records(self, tmp_path):
        run = self._run(tmp_path)
        assert len(run.outputs) == 1
        assert run.outputs[0].name == "sink"
        assert run.outputs[0].format == "s3"

    def test_task_end_no_outputs_leaves_hashes_none(self, tmp_path):
        run = self._run(tmp_path, outputs=None)
        assert run.outputs[0].schema_hash is None
        assert run.outputs[0].data_hash is None

    def test_task_end_clears_in_flight_state(self, tmp_path):
        recorder, _, _ = _make_recorder(tmp_path)
        ctx = FakeContext()
        recorder.task_start(context=ctx, config=_BASE_CONFIG)
        assert ctx.run_id in recorder._runs
        recorder.task_end(
            context=ctx, config=_BASE_CONFIG, outputs=None, status="success", duration_sec=1.0,
        )
        assert ctx.run_id not in recorder._runs

    def test_task_end_without_prior_start_does_not_crash(self, tmp_path):
        recorder, _, _ = _make_recorder(tmp_path)
        ctx = FakeContext(run_id="orphan")
        # Must not raise
        recorder.task_end(
            context=ctx, config=_BASE_CONFIG, outputs=None, status="success", duration_sec=0.0,
        )

    def test_same_config_same_config_hash(self, tmp_path):
        recorder, store, _ = _make_recorder(tmp_path)
        ctx1 = FakeContext(run_id="r1")
        ctx2 = FakeContext(run_id="r2")
        recorder.task_start(context=ctx1, config=_BASE_CONFIG)
        recorder.task_start(context=ctx2, config=_BASE_CONFIG)
        run1 = store.load("fraud/ingestion/claim_etl", "r1")
        run2 = store.load("fraud/ingestion/claim_etl", "r2")
        assert run1.config_hash == run2.config_hash
