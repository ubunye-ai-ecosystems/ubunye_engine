"""Conformance tests for the four protocol interfaces.

Each test verifies that the built-in implementation satisfies its
protocol, exercises the full public surface through the protocol API,
and checks the three production policy decisions:

- Decision 1: Non-blocking metadata writes
- Decision 2: Graceful degradation with fallback manifests
- Decision 3: Schema evolution (strict core + flexible metadata)
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from ubunye.interfaces import (
    AuthBackend,
    Credentials,
    DeployAdapter,
    LineageBackend,
    LineageRecord,
    ModelVersionInfo,
    RegistryBackend,
)

# =====================================================================
# Protocol conformance
# =====================================================================


class TestProtocolConformance:
    """Verify that each built-in implementation satisfies its protocol."""

    def test_deploy_adapter(self):
        from ubunye.deploy.databricks import DatabricksDeployAdapter

        adapter = DatabricksDeployAdapter()
        assert isinstance(adapter, DeployAdapter)
        assert adapter.name == "databricks"

    def test_auth_backend(self):
        from ubunye.deploy.databricks.auth import TokenAuthBackend

        backend = TokenAuthBackend()
        assert isinstance(backend, AuthBackend)

    def test_lineage_backend(self):
        from ubunye.lineage.storage import FileSystemLineageStore

        store = FileSystemLineageStore()
        assert isinstance(store, LineageBackend)

    def test_registry_backend(self, tmp_path):
        from ubunye.models.registry import FilesystemRegistryBackend

        backend = FilesystemRegistryBackend(str(tmp_path))
        assert isinstance(backend, RegistryBackend)


# =====================================================================
# Entry-point discovery
# =====================================================================


class TestEntryPointDiscovery:
    def test_deploy_adapters_discoverable(self):
        from ubunye._internal.discovery import get_deploy_adapter

        cls = get_deploy_adapter("databricks")
        assert cls.__name__ == "DatabricksDeployAdapter"

    def test_registry_backends_discoverable(self):
        from ubunye._internal.discovery import get_registry_backend

        cls = get_registry_backend("filesystem")
        assert cls.__name__ == "FilesystemRegistryBackend"

    def test_lineage_backends_discoverable(self):
        from ubunye._internal.discovery import get_lineage_backend

        cls = get_lineage_backend("filesystem")
        assert cls.__name__ == "FileSystemLineageStore"

    def test_auth_backends_discoverable(self):
        from ubunye._internal.discovery import get_auth_backend

        cls = get_auth_backend("token")
        assert cls.__name__ == "TokenAuthBackend"

    def test_missing_backend_raises(self):
        from ubunye._internal.discovery import get_deploy_adapter
        from ubunye.core.errors import PluginNotFoundError

        with pytest.raises(PluginNotFoundError, match="not found"):
            get_deploy_adapter("nonexistent")


# =====================================================================
# Auto-detect
# =====================================================================


class TestAutoDetect:
    def test_registry_defaults_to_filesystem(self):
        from ubunye._internal.auto_detect import detect_registry_backend

        assert detect_registry_backend() == "filesystem"

    def test_lineage_defaults_to_filesystem(self):
        from ubunye._internal.auto_detect import detect_lineage_backend

        assert detect_lineage_backend() == "filesystem"

    def test_lineage_delta_when_env_set(self, monkeypatch):
        from ubunye._internal.auto_detect import detect_lineage_backend

        monkeypatch.setenv("UBUNYE_LINEAGE_TABLE", "ubunye.metadata.lineage")
        assert detect_lineage_backend() == "delta"

    def test_auth_token_when_env_set(self, monkeypatch):
        from ubunye._internal.auto_detect import detect_auth_backend

        monkeypatch.setenv("DATABRICKS_TOKEN", "dapi123")
        assert detect_auth_backend() == "token"

    def test_auth_sp_when_client_creds_set(self, monkeypatch):
        from ubunye._internal.auto_detect import detect_auth_backend

        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "id")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "secret")
        assert detect_auth_backend() == "service_principal"

    def test_auth_sp_takes_priority_over_token(self, monkeypatch):
        from ubunye._internal.auto_detect import detect_auth_backend

        monkeypatch.setenv("DATABRICKS_TOKEN", "dapi123")
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "id")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "secret")
        assert detect_auth_backend() == "service_principal"

    def test_auth_raises_when_no_creds(self, monkeypatch):
        from ubunye._internal.auto_detect import detect_auth_backend
        from ubunye.core.errors import AuthNotFoundError

        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        with pytest.raises(AuthNotFoundError):
            detect_auth_backend()


# =====================================================================
# LineageBackend conformance (filesystem)
# =====================================================================


class TestLineageBackendConformance:
    def _make_record(self, run_id: str = "run-1", status: str = "success") -> LineageRecord:
        return LineageRecord(
            run_id=run_id,
            recorded_at="2026-01-01T00:00:00Z",
            engine_version="0.1.8",
            task="claim_etl",
            usecase="fraud",
            pipeline="ingestion",
            target="dev",
            status=status,
            duration_sec=1.5,
            metadata={"profile": "dev", "custom_key": "custom_value"},
        )

    def test_record_and_get_run(self, tmp_path):
        from ubunye.lineage.storage import FileSystemLineageStore

        store = FileSystemLineageStore(str(tmp_path / "lineage"))
        record = self._make_record()
        store._record_sync(record)

        loaded = store.get_run("run-1")
        assert loaded.run_id == "run-1"
        assert loaded.task == "claim_etl"
        assert loaded.usecase == "fraud"
        assert loaded.status == "success"

    def test_search_records(self, tmp_path):
        from ubunye.lineage.storage import FileSystemLineageStore

        store = FileSystemLineageStore(str(tmp_path / "lineage"))
        store._record_sync(self._make_record("run-1", "success"))
        store._record_sync(self._make_record("run-2", "error"))

        results = store.search_records(status="error")
        assert len(results) == 1
        assert results[0].run_id == "run-2"

    def test_compact_is_noop(self, tmp_path):
        from ubunye.lineage.storage import FileSystemLineageStore

        store = FileSystemLineageStore(str(tmp_path / "lineage"))
        store.compact()

    def test_get_run_not_found(self, tmp_path):
        from ubunye.core.errors import LineageRecordNotFoundError
        from ubunye.lineage.storage import FileSystemLineageStore

        store = FileSystemLineageStore(str(tmp_path / "lineage"))
        with pytest.raises(LineageRecordNotFoundError):
            store.get_run("nonexistent")

    def test_decision_3_metadata_flexibility(self, tmp_path):
        """Old engine writes record A, new engine writes record B with extra
        metadata — old engine reads both without crashing."""
        from ubunye.lineage.storage import FileSystemLineageStore

        store = FileSystemLineageStore(str(tmp_path / "lineage"))

        old_record = self._make_record("run-old")
        old_record.metadata = {"profile": "dev"}
        store._record_sync(old_record)

        new_record = self._make_record("run-new")
        new_record.engine_version = "0.2.0"
        new_record.metadata = {
            "profile": "dev",
            "new_feature_flag": "enabled",
            "experiment_id": "exp-42",
        }
        store._record_sync(new_record)

        loaded_old = store.get_run("run-old")
        loaded_new = store.get_run("run-new")
        assert loaded_old.run_id == "run-old"
        assert loaded_new.run_id == "run-new"
        assert loaded_new.metadata.get("new_feature_flag") == "enabled"


# =====================================================================
# RegistryBackend conformance (filesystem)
# =====================================================================


class _DummyModel:
    """Minimal model satisfying the UbunyeModel contract for tests."""

    def train(self, df):
        return {"accuracy": 0.95}

    def predict(self, df):
        return df

    def save(self, path):
        Path(path).mkdir(parents=True, exist_ok=True)
        (Path(path) / "model.bin").write_text("dummy", encoding="utf-8")

    @classmethod
    def load(cls, path):
        return cls()

    def metadata(self):
        return {"framework": "dummy", "version": "1.0"}


class TestRegistryBackendConformance:
    def test_register_and_get(self, tmp_path):
        from ubunye.models.registry import FilesystemRegistryBackend

        backend = FilesystemRegistryBackend(str(tmp_path))
        info = backend.register(
            use_case="fraud",
            model_name="RiskModel",
            version="1.0.0",
            model=_DummyModel(),
            metrics={"accuracy": 0.95},
        )
        assert isinstance(info, ModelVersionInfo)
        assert info.version == "1.0.0"
        assert info.stage == "development"
        assert info.metrics["accuracy"] == 0.95

        path, loaded = backend.get(use_case="fraud", model_name="RiskModel", version="1.0.0")
        assert loaded.version == "1.0.0"
        assert "model" in path

    def test_list_versions(self, tmp_path):
        from ubunye.models.registry import FilesystemRegistryBackend

        backend = FilesystemRegistryBackend(str(tmp_path))
        backend.register(
            use_case="fraud",
            model_name="RiskModel",
            version="1.0.0",
            model=_DummyModel(),
            metrics={},
        )
        backend.register(
            use_case="fraud",
            model_name="RiskModel",
            version="1.0.1",
            model=_DummyModel(),
            metrics={},
        )
        versions = backend.list_versions(use_case="fraud", model_name="RiskModel")
        assert len(versions) == 2
        assert all(isinstance(v, ModelVersionInfo) for v in versions)

    def test_promote_and_demote(self, tmp_path):
        from ubunye.models.registry import FilesystemRegistryBackend

        backend = FilesystemRegistryBackend(str(tmp_path))
        backend.register(
            use_case="fraud",
            model_name="RiskModel",
            version="1.0.0",
            model=_DummyModel(),
            metrics={"accuracy": 0.95},
        )
        promoted = backend.promote(
            use_case="fraud",
            model_name="RiskModel",
            version="1.0.0",
            to_stage="staging",
        )
        assert promoted.stage == "staging"

        demoted = backend.demote(
            use_case="fraud",
            model_name="RiskModel",
            version="1.0.0",
            to_stage="development",
        )
        assert demoted.stage == "development"

    def test_delete(self, tmp_path):
        from ubunye.models.registry import FilesystemRegistryBackend

        backend = FilesystemRegistryBackend(str(tmp_path))
        backend.register(
            use_case="fraud",
            model_name="RiskModel",
            version="1.0.0",
            model=_DummyModel(),
            metrics={},
        )
        backend.delete(use_case="fraud", model_name="RiskModel", version="1.0.0")
        versions = backend.list_versions(use_case="fraud", model_name="RiskModel")
        assert len(versions) == 0

    def test_promotion_gate_failure_propagates(self, tmp_path):
        """PromotionBlockedError is NOT caught by graceful degradation."""
        from ubunye.core.errors import PromotionBlockedError
        from ubunye.models.registry import FilesystemRegistryBackend

        backend = FilesystemRegistryBackend(str(tmp_path))
        backend.register(
            use_case="fraud",
            model_name="RiskModel",
            version="1.0.0",
            model=_DummyModel(),
            metrics={"auc": 0.60},
        )
        with pytest.raises(PromotionBlockedError):
            backend.promote(
                use_case="fraud",
                model_name="RiskModel",
                version="1.0.0",
                to_stage="staging",
                gates={"min_auc": 0.80},
            )

    def test_flush_is_noop_for_filesystem(self, tmp_path):
        from ubunye.models.registry import FilesystemRegistryBackend

        backend = FilesystemRegistryBackend(str(tmp_path))
        assert backend.flush() == 0

    def test_decision_3_metadata_in_model_version(self, tmp_path):
        """ModelVersionInfo carries flexible metadata alongside strict core."""
        from ubunye.models.registry import FilesystemRegistryBackend

        backend = FilesystemRegistryBackend(str(tmp_path))
        info = backend.register(
            use_case="fraud",
            model_name="RiskModel",
            version="1.0.0",
            model=_DummyModel(),
            metrics={"accuracy": 0.95},
            metadata={"experiment": "exp-42"},
        )
        assert info.engine_version
        assert info.created_at
        assert isinstance(info.metadata, dict)


# =====================================================================
# Decision 1: Non-blocking metadata writes
# =====================================================================


class TestDecision1NonBlocking:
    def test_slow_backend_does_not_block_pipeline(self, tmp_path):
        """A backend that takes 0.5s per write must not slow a pipeline
        logging 10 records to more than ~1s total (vs 5s if synchronous)."""
        from ubunye._internal.metadata_worker import MetadataWorker

        write_count = 0

        def slow_write(record):
            nonlocal write_count
            time.sleep(0.5)
            write_count += 1

        worker = MetadataWorker(slow_write, kind="test-slow")

        t0 = time.perf_counter()
        for i in range(10):
            worker.submit({"i": i}, run_id=f"run-{i}")
        submit_time = time.perf_counter() - t0

        assert (
            submit_time < 1.0
        ), f"Submitting 10 records took {submit_time:.2f}s — should be near-instant"

        worker.flush(timeout=30.0)
        assert write_count == 10

    def test_queue_overflow_drops_oldest(self):
        import threading

        from ubunye._internal.metadata_worker import MetadataWorker

        gate = threading.Event()
        written: list = []

        def blocking_write(record):
            gate.wait()
            written.append(record)

        worker = MetadataWorker(blocking_write, kind="test-overflow", queue_size=3)

        worker.submit({"i": 0}, run_id="run-0")
        time.sleep(0.05)

        for i in range(1, 7):
            worker.submit({"i": i}, run_id=f"run-{i}")

        gate.set()
        worker.flush(timeout=5.0)
        assert worker.drop_count > 0


# =====================================================================
# Decision 2: Graceful degradation
# =====================================================================


class TestDecision2GracefulDegradation:
    def test_backend_failure_writes_fallback_manifest(self, tmp_path, monkeypatch):
        """When a write raises, the record goes to a local fallback manifest."""
        from ubunye._internal.metadata_worker import MetadataWorker

        fallback_root = tmp_path / "fallback"
        monkeypatch.setattr("ubunye._internal.metadata_worker._FALLBACK_ROOT", fallback_root)

        def fail_write(record):
            raise RuntimeError("simulated network failure")

        worker = MetadataWorker(fail_write, kind="lineage")
        worker.submit({"run_id": "fail-run", "data": "test"}, run_id="fail-run")
        worker.submit({"run_id": "fail-run", "data": "test2"}, run_id="fail-run")
        worker.flush(timeout=5.0)

        assert worker.fallback_count == 2
        manifest = fallback_root / "fail-run" / "lineage.jsonl"
        assert manifest.exists()
        lines = manifest.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 2
        for line in lines:
            rec = json.loads(line)
            assert "_fallback_at" in rec

    def test_sync_replays_fallback_without_duplicates(self, tmp_path, monkeypatch):
        """ubunye sync replays manifest records with deduplication."""
        from ubunye.cli.sync import _dedup_key, _load_manifest

        manifest_dir = tmp_path / "test-run"
        manifest_dir.mkdir(parents=True)
        manifest = manifest_dir / "lineage.jsonl"
        records = [
            {"run_id": "r1", "task": "t1", "recorded_at": "2026-01-01T00:00:00Z"},
            {"run_id": "r1", "task": "t1", "recorded_at": "2026-01-01T00:00:00Z"},
            {"run_id": "r1", "task": "t2", "recorded_at": "2026-01-01T00:00:00Z"},
        ]
        manifest.write_text(
            "\n".join(json.dumps(r) for r in records) + "\n",
            encoding="utf-8",
        )

        loaded = _load_manifest(manifest)
        assert len(loaded) == 3

        seen = set()
        deduped = 0
        for rec in loaded:
            key = _dedup_key(rec)
            if key in seen:
                deduped += 1
            seen.add(key)

        assert deduped == 1

    def test_auth_failure_is_not_degraded(self, monkeypatch):
        """Auth failures must propagate, not degrade gracefully."""
        from ubunye.core.errors import AuthNotFoundError
        from ubunye.deploy.databricks.auth import TokenAuthBackend

        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        backend = TokenAuthBackend()
        with pytest.raises(AuthNotFoundError):
            backend.resolve("https://adb-123.azuredatabricks.net")


# =====================================================================
# Decision 3: Schema evolution
# =====================================================================


class TestDecision3SchemaEvolution:
    def test_lineage_record_has_strict_core_plus_metadata(self):
        """LineageRecord core fields are present; metadata is flexible."""
        record = LineageRecord(
            run_id="r1",
            recorded_at="2026-01-01T00:00:00Z",
            engine_version="0.1.8",
            task="t1",
            usecase="uc1",
            pipeline="p1",
            target="dev",
            metadata={"custom_key": "custom_value", "new_field": "new_value"},
        )
        assert record.run_id == "r1"
        assert record.engine_version == "0.1.8"
        assert record.metadata["custom_key"] == "custom_value"
        assert record.metadata["new_field"] == "new_value"

    def test_model_version_info_has_strict_core_plus_metadata(self):
        """ModelVersionInfo core fields are present; metadata is flexible."""
        info = ModelVersionInfo(
            name="RiskModel",
            version="1.0.0",
            stage="development",
            created_at="2026-01-01T00:00:00Z",
            engine_version="0.1.8",
            metrics={"accuracy": 0.95},
            metadata={"experiment_id": "exp-42", "framework": "sklearn"},
        )
        assert info.name == "RiskModel"
        assert info.engine_version == "0.1.8"
        assert info.metadata["experiment_id"] == "exp-42"

    def test_old_client_ignores_unknown_metadata_keys(self, tmp_path):
        """A record with extra metadata keys can still be read without error."""
        from ubunye.lineage.storage import FileSystemLineageStore

        store = FileSystemLineageStore(str(tmp_path / "lineage"))
        record = LineageRecord(
            run_id="r-future",
            recorded_at="2026-01-01T00:00:00Z",
            engine_version="0.3.0",
            task="t1",
            usecase="uc1",
            pipeline="p1",
            target="dev",
            metadata={
                "profile": "dev",
                "future_key_1": "value_1",
                "future_key_2": "value_2",
            },
        )
        store._record_sync(record)
        loaded = store.get_run("r-future")
        assert loaded.run_id == "r-future"

    def test_credentials_metadata_flexibility(self):
        creds = Credentials(
            host="https://adb-123.azuredatabricks.net",
            auth_type="token",
            token="dapi123",
            metadata={"tenant_id": "abc", "scope": "all-apis"},
        )
        assert creds.metadata["tenant_id"] == "abc"


# =====================================================================
# Sync CLI command
# =====================================================================


class TestSyncCommand:
    def test_sync_help(self):
        from typer.testing import CliRunner

        from ubunye.cli.main import app

        runner = CliRunner()
        result = runner.invoke(app, ["sync", "--help"])
        assert result.exit_code == 0
        assert "lineage" in result.output
        assert "registry" in result.output

    def test_sync_lineage_no_manifests(self):
        from typer.testing import CliRunner

        from ubunye.cli.main import app

        runner = CliRunner()
        result = runner.invoke(app, ["sync", "lineage"])
        assert result.exit_code == 0
