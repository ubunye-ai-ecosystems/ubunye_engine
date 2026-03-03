"""Unit tests for Pydantic config schema models."""
import pytest
from pydantic import ValidationError

from ubunye.config.schema import (
    FormatType,
    IOConfig,
    JobType,
    OrchestrationType,
    TransformConfig,
    UbunyeConfig,
    WriteMode,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_HIVE_INPUT = {"format": "hive", "db_name": "db", "tbl_name": "tbl"}
_HIVE_OUTPUT = {"format": "hive", "db_name": "db", "tbl_name": "out"}
_MINIMAL_CONFIG = {
    "inputs": {"source": _HIVE_INPUT},
    "transform": {"type": "noop"},
    "outputs": {"sink": _HIVE_OUTPUT},
}


def _make_config(**overrides) -> dict:
    base = {"MODEL": "etl", "VERSION": "0.1.0", "CONFIG": _MINIMAL_CONFIG}
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# UbunyeConfig — top-level
# ---------------------------------------------------------------------------


class TestUbunyeConfig:
    def test_valid_etl_config(self):
        cfg = UbunyeConfig(**_make_config())
        assert cfg.MODEL == JobType.ETL

    def test_valid_ml_config(self):
        cfg = UbunyeConfig(
            MODEL="ml",
            VERSION="0.2.0",
            CONFIG={
                "inputs": {"features": {"format": "delta", "path": "s3a://bucket/features"}},
                "transform": {"type": "model", "params": {"action": "train"}},
                "outputs": {"predictions": {"format": "delta", "path": "s3a://bucket/preds"}},
            },
        )
        assert cfg.MODEL == JobType.ML

    def test_missing_model_key_raises(self):
        with pytest.raises(ValidationError):
            UbunyeConfig(VERSION="0.1.0", CONFIG=_MINIMAL_CONFIG)

    def test_invalid_model_type_raises(self):
        with pytest.raises(ValidationError):
            UbunyeConfig(**_make_config(MODEL="streaming"))

    def test_missing_version_raises(self):
        with pytest.raises(ValidationError):
            UbunyeConfig(MODEL="etl", CONFIG=_MINIMAL_CONFIG)

    def test_invalid_semver_raises(self):
        with pytest.raises(ValidationError, match="semver"):
            UbunyeConfig(**_make_config(VERSION="1.0"))

    def test_valid_semver_variants(self):
        for v in ("0.0.1", "1.2.3", "10.20.30"):
            cfg = UbunyeConfig(**_make_config(VERSION=v))
            assert cfg.VERSION == v

    def test_empty_inputs_raises(self):
        bad_config = {"inputs": {}, "outputs": {"sink": _HIVE_OUTPUT}}
        with pytest.raises(ValidationError):
            UbunyeConfig(**_make_config(CONFIG=bad_config))

    def test_empty_outputs_raises(self):
        bad_config = {"inputs": {"source": _HIVE_INPUT}, "outputs": {}}
        with pytest.raises(ValidationError):
            UbunyeConfig(**_make_config(CONFIG=bad_config))

    def test_engine_profiles_valid(self):
        cfg = UbunyeConfig(
            **_make_config(
                ENGINE={
                    "spark_conf": {"spark.sql.shuffle.partitions": "50"},
                    "profiles": {
                        "dev": {"spark_conf": {"spark.master": "local[*]"}},
                        "prod": {"spark_conf": {"spark.master": "yarn"}},
                    },
                }
            )
        )
        assert "dev" in cfg.ENGINE.profiles
        assert cfg.ENGINE.profiles["dev"].spark_conf["spark.master"] == "local[*]"

    def test_merged_spark_conf_applies_profile(self):
        cfg = UbunyeConfig(
            **_make_config(
                ENGINE={
                    "spark_conf": {"spark.sql.shuffle.partitions": "50"},
                    "profiles": {
                        "dev": {
                            "spark_conf": {
                                "spark.sql.shuffle.partitions": "8",
                                "spark.master": "local[*]",
                            }
                        },
                    },
                }
            )
        )
        merged = cfg.merged_spark_conf("dev")
        assert merged["spark.sql.shuffle.partitions"] == "8"
        assert merged["spark.master"] == "local[*]"

    def test_merged_spark_conf_no_profile(self):
        cfg = UbunyeConfig(
            **_make_config(ENGINE={"spark_conf": {"spark.sql.shuffle.partitions": "50"}})
        )
        assert cfg.merged_spark_conf()["spark.sql.shuffle.partitions"] == "50"

    def test_orchestration_valid(self):
        cfg = UbunyeConfig(
            **_make_config(
                ORCHESTRATION={
                    "type": "airflow",
                    "schedule": "@daily",
                    "retries": 3,
                    "owner": "fraud-team",
                    "tags": ["fraud", "etl"],
                }
            )
        )
        assert cfg.ORCHESTRATION.type == OrchestrationType.AIRFLOW

    def test_orchestration_invalid_type_raises(self):
        with pytest.raises(ValidationError):
            UbunyeConfig(**_make_config(ORCHESTRATION={"type": "cron"}))

    def test_orchestration_defaults(self):
        cfg = UbunyeConfig(**_make_config(ORCHESTRATION={"type": "dagster"}))
        assert cfg.ORCHESTRATION.retries == 2
        assert cfg.ORCHESTRATION.tags == []


# ---------------------------------------------------------------------------
# IOConfig
# ---------------------------------------------------------------------------


class TestIOConfig:
    def test_hive_with_db_and_table(self):
        io = IOConfig(format="hive", db_name="fraud_db", tbl_name="claims")
        assert io.format == FormatType.HIVE

    def test_hive_with_sql(self):
        io = IOConfig(format="hive", sql="SELECT * FROM fraud_db.claims")
        assert io.sql is not None

    def test_hive_without_db_or_sql_raises(self):
        with pytest.raises(ValidationError, match="hive"):
            IOConfig(format="hive")

    def test_hive_db_without_tbl_raises(self):
        with pytest.raises(ValidationError, match="hive"):
            IOConfig(format="hive", db_name="db")

    def test_jdbc_valid(self):
        io = IOConfig(
            format="jdbc",
            url="jdbc:postgresql://db:5432/insurance",
            table="claims",
            user="{{ env.DB_USER }}",
            password="{{ env.DB_PASS }}",
        )
        assert io.format == FormatType.JDBC

    def test_jdbc_missing_url_raises(self):
        with pytest.raises(ValidationError, match="jdbc"):
            IOConfig(format="jdbc", table="claims")

    def test_jdbc_missing_table_raises(self):
        with pytest.raises(ValidationError, match="jdbc"):
            IOConfig(format="jdbc", url="jdbc:postgresql://db:5432/ins")

    def test_s3_valid(self):
        io = IOConfig(format="s3", path="s3a://bucket/data/")
        assert io.format == FormatType.S3

    def test_s3_missing_path_raises(self):
        with pytest.raises(ValidationError, match="s3"):
            IOConfig(format="s3")

    def test_binary_missing_path_raises(self):
        with pytest.raises(ValidationError, match="binary"):
            IOConfig(format="binary")

    def test_delta_with_path(self):
        io = IOConfig(format="delta", path="s3a://bucket/delta_table")
        assert io.format == FormatType.DELTA

    def test_delta_with_table(self):
        io = IOConfig(format="delta", table="main.fraud.claims")
        assert io.format == FormatType.DELTA

    def test_delta_without_path_or_table_raises(self):
        with pytest.raises(ValidationError, match="delta"):
            IOConfig(format="delta")

    def test_unity_with_table_parts(self):
        io = IOConfig(format="unity", db_name="raw_db", tbl_name="events")
        assert io.format == FormatType.UNITY

    def test_unity_with_full_table_name(self):
        io = IOConfig(format="unity", table="main.fraud.claims")
        assert io.format == FormatType.UNITY

    def test_unity_without_table_raises(self):
        with pytest.raises(ValidationError, match="unity"):
            IOConfig(format="unity")

    def test_rest_api_valid(self):
        io = IOConfig(format="rest_api", url="https://api.example.com/v1/data")
        assert io.format == FormatType.REST_API

    def test_rest_api_missing_url_raises(self):
        with pytest.raises(ValidationError, match="rest_api"):
            IOConfig(format="rest_api")

    def test_invalid_format_raises(self):
        with pytest.raises(ValidationError):
            IOConfig(format="mongodb")

    def test_invalid_mode_raises(self):
        with pytest.raises(ValidationError):
            IOConfig(format="hive", db_name="db", tbl_name="t", mode="upsert")

    def test_valid_modes(self):
        for mode in ("overwrite", "append", "merge"):
            io = IOConfig(format="hive", db_name="db", tbl_name="t", mode=mode)
            assert io.mode == WriteMode(mode)

    def test_options_passthrough(self):
        io = IOConfig(format="delta", path="s3a://bucket/data", options={"overwriteSchema": "true"})
        assert io.options["overwriteSchema"] == "true"

    def test_extra_plugin_fields_allowed(self):
        """Plugin-specific keys (rest_api headers, pagination) should not be rejected."""
        io = IOConfig(
            format="rest_api",
            url="https://api.example.com/v1",
            headers={"Authorization": "Bearer tok"},
            pagination={"type": "offset", "page_size": 100},
        )
        assert io.model_extra["headers"]["Authorization"] == "Bearer tok"

    def test_model_dump_includes_extra_fields(self):
        io = IOConfig(format="rest_api", url="https://api.example.com", batch_size=50)
        d = io.model_dump(mode="json")
        assert d["batch_size"] == 50
        assert d["url"] == "https://api.example.com"


# ---------------------------------------------------------------------------
# TransformConfig
# ---------------------------------------------------------------------------


class TestTransformConfig:
    def test_default_noop(self):
        t = TransformConfig()
        assert t.type == "noop"
        assert t.params == {}

    def test_custom_transform(self):
        t = TransformConfig(type="model", params={"action": "train", "model_class": "FraudModel"})
        assert t.type == "model"
        assert t.params["action"] == "train"
