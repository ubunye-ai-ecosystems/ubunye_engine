"""Lineage integration tests.

Verifies that lineage records are captured correctly during actual pipeline
execution (full Engine.run() with a real Spark session).

Skipped automatically if pyspark is not installed.
Run with: pytest tests/integration -m integration
"""
from __future__ import annotations

import uuid

import pytest

pyspark = pytest.importorskip("pyspark", reason="pyspark not installed")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from ubunye.backends.spark_backend import SparkBackend  # noqa: E402
from ubunye.core.runtime import Engine, EngineContext  # noqa: E402
from ubunye.lineage.recorder import LineageRecorder  # noqa: E402
from ubunye.lineage.storage import FileSystemLineageStore  # noqa: E402

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def spark() -> SparkSession:
    s = (
        SparkSession.builder.master("local[1]")
        .appName("ubunye-lineage-test")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "512m")
        .getOrCreate()
    )
    yield s
    s.stop()


_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("value", StringType(), True),
    StructField("amount", DoubleType(), True),
])

_SAMPLE_ROWS = [(1, "a", 10.0), (2, "b", 20.0), (3, "c", 30.0)]


def _write_csv(spark: SparkSession, path: str) -> None:
    spark.createDataFrame(_SAMPLE_ROWS, _SCHEMA).toPandas().to_csv(path, index=False)


def _good_cfg(input_path: str, output_path: str) -> dict:
    return {
        "MODEL": "etl",
        "VERSION": "0.1.0",
        "CONFIG": {
            "inputs": {
                "source": {
                    "format": "s3",
                    "path": input_path,
                    "options": {"header": "true", "inferSchema": "true"},
                }
            },
            "transform": {"type": "noop"},
            "outputs": {
                "source": {"format": "s3", "path": output_path, "mode": "overwrite"},
            },
        },
    }


def _broken_cfg(input_path: str) -> dict:
    """Config pointing to a non-existent input path — will fail at read."""
    return {
        "MODEL": "etl",
        "VERSION": "0.1.0",
        "CONFIG": {
            "inputs": {"source": {"format": "s3", "path": input_path}},
            "transform": {"type": "noop"},
            "outputs": {"source": {"format": "s3", "path": "/tmp/never_written", "mode": "overwrite"}},
        },
    }


def _run(cfg: dict, spark_session: SparkSession, lineage_base: str, fail: bool = False) -> tuple[str, FileSystemLineageStore]:
    run_id = str(uuid.uuid4())
    backend = SparkBackend(app_name="test")
    backend._spark = spark_session
    recorder = LineageRecorder(store="filesystem", base_dir=lineage_base)
    context = EngineContext(run_id=run_id, profile="test", task_name="test/suite/etl")

    recorder.task_start(context=context, config=cfg)
    try:
        engine = Engine(backend=backend, context=context)
        result = engine.run(cfg)
        recorder.task_end(
            context=context, config=cfg,
            outputs=result, status="success", duration_sec=0.1,
        )
    except Exception:
        recorder.task_end(
            context=context, config=cfg,
            outputs=None, status="error", duration_sec=0.0,
        )
        if not fail:
            raise
    finally:
        backend._spark = None

    return run_id, FileSystemLineageStore(lineage_base)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestLineageIntegration:

    def test_successful_run_produces_lineage_file(self, spark, tmp_path):
        csv = str(tmp_path / "in.csv")
        _write_csv(spark, csv)
        run_id, store = _run(_good_cfg(csv, str(tmp_path / "out")), spark, str(tmp_path / "lineage"))
        files = list((tmp_path / "lineage").rglob("*.json"))
        assert len(files) >= 1

    def test_lineage_status_is_success(self, spark, tmp_path):
        csv = str(tmp_path / "in.csv")
        _write_csv(spark, csv)
        run_id, store = _run(_good_cfg(csv, str(tmp_path / "out")), spark, str(tmp_path / "lineage"))
        ctx = store.load("test/suite/etl", run_id)
        assert ctx.status == "success"

    def test_lineage_has_task_path(self, spark, tmp_path):
        csv = str(tmp_path / "in.csv")
        _write_csv(spark, csv)
        run_id, store = _run(_good_cfg(csv, str(tmp_path / "out")), spark, str(tmp_path / "lineage"))
        ctx = store.load("test/suite/etl", run_id)
        assert ctx.task_path == "test/suite/etl"

    def test_lineage_captures_output_row_count(self, spark, tmp_path):
        csv = str(tmp_path / "in.csv")
        _write_csv(spark, csv)
        run_id, store = _run(_good_cfg(csv, str(tmp_path / "out")), spark, str(tmp_path / "lineage"))
        ctx = store.load("test/suite/etl", run_id)
        assert ctx.outputs[0].row_count == len(_SAMPLE_ROWS)

    def test_lineage_captures_schema_hash(self, spark, tmp_path):
        csv = str(tmp_path / "in.csv")
        _write_csv(spark, csv)
        run_id, store = _run(_good_cfg(csv, str(tmp_path / "out")), spark, str(tmp_path / "lineage"))
        ctx = store.load("test/suite/etl", run_id)
        assert ctx.outputs[0].schema_hash is not None
        assert ctx.outputs[0].schema_hash.startswith("sha256:")

    def test_lineage_has_config_hash(self, spark, tmp_path):
        csv = str(tmp_path / "in.csv")
        _write_csv(spark, csv)
        run_id, store = _run(_good_cfg(csv, str(tmp_path / "out")), spark, str(tmp_path / "lineage"))
        ctx = store.load("test/suite/etl", run_id)
        assert ctx.config_hash.startswith("sha256:")

    def test_failed_run_records_error_status(self, spark, tmp_path):
        """A pipeline that fails should still write a lineage record with status=error."""
        bad_path = str(tmp_path / "does_not_exist.csv")
        run_id, store = _run(
            _broken_cfg(bad_path), spark,
            str(tmp_path / "lineage"),
            fail=True,
        )
        ctx = store.load("test/suite/etl", run_id)
        assert ctx.status == "error"

    def test_lineage_started_at_is_set(self, spark, tmp_path):
        csv = str(tmp_path / "in.csv")
        _write_csv(spark, csv)
        run_id, store = _run(_good_cfg(csv, str(tmp_path / "out")), spark, str(tmp_path / "lineage"))
        ctx = store.load("test/suite/etl", run_id)
        assert ctx.started_at

    def test_lineage_ended_at_is_set(self, spark, tmp_path):
        csv = str(tmp_path / "in.csv")
        _write_csv(spark, csv)
        run_id, store = _run(_good_cfg(csv, str(tmp_path / "out")), spark, str(tmp_path / "lineage"))
        ctx = store.load("test/suite/etl", run_id)
        assert ctx.ended_at is not None
