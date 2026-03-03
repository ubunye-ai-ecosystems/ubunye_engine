"""Reproducibility integration tests.

Runs pipelines twice against the same fixture data and asserts identical outputs.
This catches non-determinism bugs in transforms or writers.

Requires PySpark — the entire module is skipped gracefully if pyspark is not
installed. Run with: pytest tests/integration -m integration
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
# Session-scoped Spark fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    s = (
        SparkSession.builder.master("local[1]")
        .appName("ubunye-reproducibility-test")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "512m")
        .getOrCreate()
    )
    yield s
    s.stop()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("claim_ref", StringType(), True),
        StructField("amount", DoubleType(), True),
    ]
)

_ROWS_A = [(1, "CLM-001", 1000.50), (2, "CLM-002", 2500.00), (3, "CLM-003", 500.75)]
_ROWS_B = [(10, "CLM-010", 9999.99), (11, "CLM-011", 1.00)]


def _write_csv(spark: SparkSession, rows: list, path: str) -> None:
    df = spark.createDataFrame(rows, _SCHEMA)
    df.toPandas().to_csv(path, index=False)


def _build_config(input_path: str, output_path: str) -> dict:
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
            "outputs": {"source": {"format": "s3", "path": output_path, "mode": "overwrite",}},
        },
    }


def _run_with_lineage(cfg: dict, spark_session: SparkSession, lineage_base: str) -> str:
    """Run a pipeline using Engine.run() with LineageRecorder. Returns run_id."""
    run_id = str(uuid.uuid4())
    backend = SparkBackend(app_name="test", conf={"spark.master": "local[1]"})
    backend._spark = spark_session  # reuse existing session

    recorder = LineageRecorder(store="filesystem", base_dir=lineage_base)
    context = EngineContext(run_id=run_id, profile="test", task_name="test/pipeline/etl")

    recorder.task_start(context=context, config=cfg)
    try:
        engine = Engine(backend=backend, context=context)
        engine.backend._spark = spark_session
        result = engine.run(cfg)
        recorder.task_end(
            context=context, config=cfg, outputs=result, status="success", duration_sec=0.1,
        )
    except Exception:
        recorder.task_end(
            context=context, config=cfg, outputs=None, status="error", duration_sec=0.0,
        )
        raise
    finally:
        backend._spark = None  # don't stop the shared session
    return run_id


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestReproducibility:
    def test_same_input_same_schema_hash(self, spark, tmp_path):
        """Running the same pipeline twice produces identical output schema hashes."""
        csv_path = str(tmp_path / "input.csv")
        out1 = str(tmp_path / "out1")
        out2 = str(tmp_path / "out2")
        lineage_base = str(tmp_path / "lineage")

        _write_csv(spark, _ROWS_A, csv_path)

        run_id1 = _run_with_lineage(_build_config(csv_path, out1), spark, lineage_base)
        run_id2 = _run_with_lineage(_build_config(csv_path, out2), spark, lineage_base)

        store = FileSystemLineageStore(lineage_base)
        ctx1 = store.load("test/pipeline/etl", run_id1)
        ctx2 = store.load("test/pipeline/etl", run_id2)

        assert ctx1.outputs[0].schema_hash is not None
        assert ctx1.outputs[0].schema_hash == ctx2.outputs[0].schema_hash

    def test_same_input_same_row_count(self, spark, tmp_path):
        """Row count is identical across two runs on the same input."""
        csv_path = str(tmp_path / "input.csv")
        lineage_base = str(tmp_path / "lineage")
        _write_csv(spark, _ROWS_A, csv_path)

        run_id1 = _run_with_lineage(
            _build_config(csv_path, str(tmp_path / "o1")), spark, lineage_base
        )
        run_id2 = _run_with_lineage(
            _build_config(csv_path, str(tmp_path / "o2")), spark, lineage_base
        )

        store = FileSystemLineageStore(lineage_base)
        ctx1 = store.load("test/pipeline/etl", run_id1)
        ctx2 = store.load("test/pipeline/etl", run_id2)

        assert ctx1.outputs[0].row_count == ctx2.outputs[0].row_count
        assert ctx1.outputs[0].row_count == len(_ROWS_A)

    def test_config_hash_stable_between_runs(self, spark, tmp_path):
        """Same config bytes produce the same config_hash in lineage."""
        csv_path = str(tmp_path / "input.csv")
        lineage_base = str(tmp_path / "lineage")
        _write_csv(spark, _ROWS_A, csv_path)
        cfg = _build_config(csv_path, str(tmp_path / "out"))

        run_id1 = _run_with_lineage(cfg, spark, lineage_base)
        run_id2 = _run_with_lineage(cfg, spark, lineage_base)

        store = FileSystemLineageStore(lineage_base)
        ctx1 = store.load("test/pipeline/etl", run_id1)
        ctx2 = store.load("test/pipeline/etl", run_id2)

        assert ctx1.config_hash == ctx2.config_hash

    def test_different_input_different_data_hash(self, spark, tmp_path):
        """Different input data produces different output data hashes."""
        csv_a = str(tmp_path / "input_a.csv")
        csv_b = str(tmp_path / "input_b.csv")
        lineage_base = str(tmp_path / "lineage")
        _write_csv(spark, _ROWS_A, csv_a)
        _write_csv(spark, _ROWS_B, csv_b)

        run_id_a = _run_with_lineage(
            _build_config(csv_a, str(tmp_path / "out_a")), spark, lineage_base
        )
        run_id_b = _run_with_lineage(
            _build_config(csv_b, str(tmp_path / "out_b")), spark, lineage_base
        )

        store = FileSystemLineageStore(lineage_base)
        ctx_a = store.load("test/pipeline/etl", run_id_a)
        ctx_b = store.load("test/pipeline/etl", run_id_b)

        # Data hashes must differ (different row content)
        assert ctx_a.outputs[0].data_hash != ctx_b.outputs[0].data_hash

    def test_schema_stability_across_runs(self, spark, tmp_path):
        """Output schema never changes between identical runs."""
        csv_path = str(tmp_path / "input.csv")
        lineage_base = str(tmp_path / "lineage")
        _write_csv(spark, _ROWS_A, csv_path)
        cfg = _build_config(csv_path, str(tmp_path / "out"))

        hashes = []
        for i in range(3):
            run_id = _run_with_lineage(cfg, spark, lineage_base)
            store = FileSystemLineageStore(lineage_base)
            ctx = store.load("test/pipeline/etl", run_id)
            hashes.append(ctx.outputs[0].schema_hash)

        assert len(set(hashes)) == 1, f"Schema hash should be stable, got: {hashes}"
