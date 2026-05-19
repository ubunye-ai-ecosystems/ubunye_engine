"""Tests for Phase 2 distributed backends.

Covers:
- ServicePrincipalAuthBackend
- MLflowRegistryBackend
- DeltaLineageBackend
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ubunye.interfaces import (
    AuthBackend,
    LineageBackend,
    LineageRecord,
    RegistryBackend,
)

# =====================================================================
# Step 2.1: ServicePrincipalAuthBackend
# =====================================================================


class TestServicePrincipalAuthBackend:
    def test_conforms_to_protocol(self):
        from ubunye.deploy.databricks.auth import ServicePrincipalAuthBackend

        backend = ServicePrincipalAuthBackend()
        assert isinstance(backend, AuthBackend)

    def test_resolve_with_env_vars(self, monkeypatch):
        from ubunye.deploy.databricks.auth import ServicePrincipalAuthBackend

        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "test-client-id")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "test-secret")
        backend = ServicePrincipalAuthBackend()
        creds = backend.resolve("https://adb-123.azuredatabricks.net")
        assert creds.auth_type == "service_principal"
        assert creds.client_id == "test-client-id"
        assert creds.client_secret == "test-secret"
        assert creds.host == "https://adb-123.azuredatabricks.net"
        assert creds.token is None

    def test_resolve_with_custom_env_vars(self, monkeypatch):
        from ubunye.deploy.databricks.auth import ServicePrincipalAuthBackend

        monkeypatch.setenv("MY_CLIENT_ID", "custom-id")
        monkeypatch.setenv("MY_SECRET", "custom-secret")
        backend = ServicePrincipalAuthBackend()
        creds = backend.resolve(
            "https://adb-123.azuredatabricks.net",
            client_id_env="MY_CLIENT_ID",
            client_secret_env="MY_SECRET",
        )
        assert creds.client_id == "custom-id"
        assert creds.client_secret == "custom-secret"

    def test_resolve_raises_when_missing_client_id(self, monkeypatch):
        from ubunye.core.errors import AuthNotFoundError
        from ubunye.deploy.databricks.auth import ServicePrincipalAuthBackend

        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        backend = ServicePrincipalAuthBackend()
        with pytest.raises(AuthNotFoundError, match="credentials not found"):
            backend.resolve("https://adb-123.azuredatabricks.net")

    def test_resolve_raises_when_missing_secret_only(self, monkeypatch):
        from ubunye.core.errors import AuthNotFoundError
        from ubunye.deploy.databricks.auth import ServicePrincipalAuthBackend

        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "some-id")
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        backend = ServicePrincipalAuthBackend()
        with pytest.raises(AuthNotFoundError):
            backend.resolve("https://adb-123.azuredatabricks.net")

    def test_discoverable_via_entry_point(self):
        from ubunye._internal.discovery import get_auth_backend

        cls = get_auth_backend("service_principal")
        assert cls.__name__ == "ServicePrincipalAuthBackend"

    def test_auto_detect_prefers_sp_over_token(self, monkeypatch):
        from ubunye._internal.auto_detect import detect_auth_backend

        monkeypatch.setenv("DATABRICKS_TOKEN", "dapi123")
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "id")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "secret")
        assert detect_auth_backend() == "service_principal"

    def test_credentials_metadata_carries_env_var_names(self, monkeypatch):
        from ubunye.deploy.databricks.auth import ServicePrincipalAuthBackend

        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "id")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "secret")
        backend = ServicePrincipalAuthBackend()
        creds = backend.resolve("https://adb-123.azuredatabricks.net")
        assert creds.metadata["client_id_env"] == "DATABRICKS_CLIENT_ID"
        assert creds.metadata["client_secret_env"] == "DATABRICKS_CLIENT_SECRET"


# =====================================================================
# Step 2.2: MLflowRegistryBackend
# =====================================================================


class _DummyModel:
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


class TestMLflowRegistryBackend:
    def test_conforms_to_protocol(self, tmp_path):
        from ubunye.models.mlflow_registry import MLflowRegistryBackend

        backend = MLflowRegistryBackend(str(tmp_path))
        assert isinstance(backend, RegistryBackend)

    def test_register_and_get(self, tmp_path):
        from ubunye.models.mlflow_registry import MLflowRegistryBackend

        backend = MLflowRegistryBackend(str(tmp_path))
        info = backend.register(
            use_case="fraud",
            model_name="RiskModel",
            version="1.0.0",
            model=_DummyModel(),
            metrics={"accuracy": 0.95},
        )
        assert info.version == "1.0.0"
        assert info.stage == "development"

        path, loaded = backend.get(use_case="fraud", model_name="RiskModel", version="1.0.0")
        assert loaded.version == "1.0.0"

    def test_promote_and_demote(self, tmp_path):
        from ubunye.models.mlflow_registry import MLflowRegistryBackend

        backend = MLflowRegistryBackend(str(tmp_path))
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

    def test_list_versions(self, tmp_path):
        from ubunye.models.mlflow_registry import MLflowRegistryBackend

        backend = MLflowRegistryBackend(str(tmp_path))
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

    def test_delete(self, tmp_path):
        from ubunye.models.mlflow_registry import MLflowRegistryBackend

        backend = MLflowRegistryBackend(str(tmp_path))
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

    def test_promotion_gate_failure(self, tmp_path):
        from ubunye.core.errors import PromotionBlockedError
        from ubunye.models.mlflow_registry import MLflowRegistryBackend

        backend = MLflowRegistryBackend(str(tmp_path))
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

    def test_discoverable_via_entry_point(self):
        from ubunye._internal.discovery import get_registry_backend

        cls = get_registry_backend("mlflow")
        assert cls.__name__ == "MLflowRegistryBackend"

    def test_metadata_round_trip(self, tmp_path):
        from ubunye.models.mlflow_registry import MLflowRegistryBackend

        backend = MLflowRegistryBackend(str(tmp_path))
        info = backend.register(
            use_case="fraud",
            model_name="RiskModel",
            version="1.0.0",
            model=_DummyModel(),
            metrics={"accuracy": 0.95},
            metadata={"experiment": "exp-42"},
        )
        assert info.metadata.get("experiment") == "exp-42"


# =====================================================================
# Step 2.3: DeltaLineageBackend
# =====================================================================


class TestDeltaLineageBackend:
    def test_conforms_to_protocol(self, tmp_path):
        from ubunye.lineage.delta_store import DeltaLineageBackend

        backend = DeltaLineageBackend(str(tmp_path / "lineage"))
        assert isinstance(backend, LineageBackend)

    def test_record_and_get_run(self, tmp_path):
        from ubunye.lineage.delta_store import DeltaLineageBackend

        backend = DeltaLineageBackend(str(tmp_path / "lineage"))
        record = LineageRecord(
            run_id="run-1",
            recorded_at="2026-01-01T00:00:00Z",
            engine_version="0.2.0",
            task="claim_etl",
            usecase="fraud",
            pipeline="ingestion",
            target="dev",
            status="success",
            duration_sec=1.5,
            metadata={"profile": "dev", "custom_key": "custom_value"},
        )
        backend._record_sync(record)

        loaded = backend.get_run("run-1")
        assert loaded.run_id == "run-1"
        assert loaded.task == "claim_etl"
        assert loaded.status == "success"

    def test_search_records(self, tmp_path):
        from ubunye.lineage.delta_store import DeltaLineageBackend

        backend = DeltaLineageBackend(str(tmp_path / "lineage"))
        r1 = LineageRecord(
            run_id="run-1",
            recorded_at="2026-01-01T00:00:00Z",
            engine_version="0.2.0",
            task="t1",
            usecase="uc1",
            pipeline="p1",
            target="dev",
            status="success",
        )
        r2 = LineageRecord(
            run_id="run-2",
            recorded_at="2026-01-01T00:01:00Z",
            engine_version="0.2.0",
            task="t1",
            usecase="uc1",
            pipeline="p1",
            target="dev",
            status="error",
            error="something broke",
        )
        backend._record_sync(r1)
        backend._record_sync(r2)

        results = backend.search(status="error")
        assert len(results) == 1
        assert results[0].run_id == "run-2"

    def test_search_by_usecase(self, tmp_path):
        from ubunye.lineage.delta_store import DeltaLineageBackend

        backend = DeltaLineageBackend(str(tmp_path / "lineage"))
        r1 = LineageRecord(
            run_id="run-1",
            recorded_at="2026-01-01T00:00:00Z",
            engine_version="0.2.0",
            task="t1",
            usecase="fraud",
            pipeline="p1",
            target="dev",
            status="success",
        )
        r2 = LineageRecord(
            run_id="run-2",
            recorded_at="2026-01-01T00:00:00Z",
            engine_version="0.2.0",
            task="t1",
            usecase="weather",
            pipeline="p1",
            target="dev",
            status="success",
        )
        backend._record_sync(r1)
        backend._record_sync(r2)

        results = backend.search(usecase="fraud")
        assert len(results) == 1
        assert results[0].usecase == "fraud"

    def test_compact_is_noop(self, tmp_path):
        from ubunye.lineage.delta_store import DeltaLineageBackend

        backend = DeltaLineageBackend(str(tmp_path / "lineage"))
        backend.compact()

    def test_get_run_not_found(self, tmp_path):
        from ubunye.core.errors import LineageRecordNotFoundError
        from ubunye.lineage.delta_store import DeltaLineageBackend

        backend = DeltaLineageBackend(str(tmp_path / "lineage"))
        with pytest.raises(LineageRecordNotFoundError):
            backend.get_run("nonexistent")

    def test_metadata_round_trip(self, tmp_path):
        from ubunye.lineage.delta_store import DeltaLineageBackend

        backend = DeltaLineageBackend(str(tmp_path / "lineage"))
        record = LineageRecord(
            run_id="run-meta",
            recorded_at="2026-01-01T00:00:00Z",
            engine_version="0.2.0",
            task="t1",
            usecase="uc1",
            pipeline="p1",
            target="dev",
            status="success",
            metadata={"profile": "dev", "future_key": "future_val"},
        )
        backend._record_sync(record)
        loaded = backend.get_run("run-meta")
        assert loaded.metadata.get("future_key") == "future_val"

    def test_discoverable_via_entry_point(self):
        from ubunye._internal.discovery import get_lineage_backend

        cls = get_lineage_backend("delta")
        assert cls.__name__ == "DeltaLineageBackend"
