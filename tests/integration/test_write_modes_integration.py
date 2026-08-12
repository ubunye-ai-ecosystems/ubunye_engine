"""Write modes against a real Spark session — not a mock.

The unit tests assert what each writer *hands to* Spark. These assert what Spark
actually *does with it*: that `ignore` really leaves the target alone, that
`overwrite_partitions` really replaces one partition and leaves its neighbours
standing, and that `merge` really upserts.

Delta-backed tests (merge, replace_where) skip unless `delta-spark` is installed
and the session accepts the Delta extensions. Run with:

    pytest tests/integration -m integration
"""

from __future__ import annotations

import importlib.util
import uuid
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark", reason="pyspark not installed")

from pyspark.sql import SparkSession  # noqa: E402

from ubunye.core.errors import SinkWriteError  # noqa: E402
from ubunye.plugins.writers.delta import DeltaWriter  # noqa: E402
from ubunye.plugins.writers.hive import HiveWriter  # noqa: E402
from ubunye.plugins.writers.s3 import S3Writer  # noqa: E402

pytestmark = pytest.mark.integration

_HAS_DELTA = importlib.util.find_spec("delta") is not None

PARTITION_OVERWRITE_KEY = "spark.sql.sources.partitionOverwriteMode"


class _Backend:
    """Minimal Backend holding a real SparkSession."""

    is_spark = True

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def start(self) -> None:  # pragma: no cover - not used
        pass

    def stop(self) -> None:  # pragma: no cover - the fixture owns the session
        pass

    # The data-plane IO seam (#38): delegate to the shared Spark mechanism, so a
    # connector routed through backend.read_frame / backend.execute_write reaches
    # this real SparkSession exactly as it used to reach ``backend.spark``.
    def read_frame(self, file_format, path, *, options=None, schema=None):
        from ubunye.adapters.spark import frame_io

        return frame_io.read_frame(self.spark, file_format, path, options=options, schema=schema)

    def execute_write(self, df, resolved, **kwargs):
        from ubunye.adapters.spark import frame_io

        frame_io.execute_write(self.spark, df, resolved, **kwargs)


@pytest.fixture(scope="module")
def spark():
    """The session the conftest launched the JVM with (Delta-enabled when available)."""
    return (
        SparkSession.builder.master("local[1]")
        .appName("ubunye-integration")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture
def backend(spark) -> _Backend:
    return _Backend(spark)


def _requires_delta(spark) -> None:
    if not _HAS_DELTA:
        pytest.skip("delta-spark not installed")
    if "DeltaSparkSessionExtension" not in (spark.conf.get("spark.sql.extensions", "") or ""):
        pytest.skip("Delta jars unavailable in this JVM")


def _rows(spark, rows):
    return spark.createDataFrame(rows, "id int, dt string, amount double")


def _out(tmp_path: Path) -> str:
    return str(tmp_path / f"out_{uuid.uuid4().hex}")


def _count_by_dt(df) -> dict:
    """Rows per partition, keyed by str.

    Spark infers the type of a partition column when it reads the directory
    layout back, so a `dt` written as "2026-01-01" returns as a datetime.date.
    Keying on the raw value makes a surviving partition look like a missing one.
    """
    return {
        str(r["dt"]): r["n"]
        for r in df.groupBy("dt").count().withColumnRenamed("count", "n").collect()
    }


# ---------------------------------------------------------------------------
# Spark's four native save modes — what Spark really does, not what we asked for
# ---------------------------------------------------------------------------


class TestNativeModes:
    def test_append_adds_rows(self, spark, backend, tmp_path):
        path = _out(tmp_path)
        df = _rows(spark, [(1, "2026-01-01", 10.0)])

        S3Writer().write(df, {"path": path, "mode": "append"}, backend)
        S3Writer().write(df, {"path": path, "mode": "append"}, backend)

        assert spark.read.parquet(path).count() == 2

    def test_overwrite_replaces_rows(self, spark, backend, tmp_path):
        path = _out(tmp_path)

        S3Writer().write(
            _rows(spark, [(1, "2026-01-01", 10.0), (2, "2026-01-01", 20.0)]),
            {"path": path, "mode": "overwrite"},
            backend,
        )
        S3Writer().write(
            _rows(spark, [(3, "2026-01-01", 30.0)]), {"path": path, "mode": "overwrite"}, backend
        )

        result = spark.read.parquet(path)
        assert result.count() == 1
        assert result.collect()[0]["id"] == 3

    def test_default_is_append_not_overwrite(self, spark, backend, tmp_path):
        """The breaking change, verified against real Spark: omitting `mode` on an
        s3 output must add rows, not replace them."""
        path = _out(tmp_path)
        df = _rows(spark, [(1, "2026-01-01", 10.0)])

        S3Writer().write(df, {"path": path}, backend)
        S3Writer().write(df, {"path": path}, backend)

        assert spark.read.parquet(path).count() == 2

    def test_errorifexists_raises_on_existing_target(self, spark, backend, tmp_path):
        path = _out(tmp_path)
        df = _rows(spark, [(1, "2026-01-01", 10.0)])

        S3Writer().write(df, {"path": path, "mode": "overwrite"}, backend)

        with pytest.raises(Exception) as exc:  # Spark raises AnalysisException
            S3Writer().write(df, {"path": path, "mode": "errorifexists"}, backend)
        assert "already exists" in str(exc.value).lower()

    def test_error_alias_behaves_like_errorifexists(self, spark, backend, tmp_path):
        path = _out(tmp_path)
        df = _rows(spark, [(1, "2026-01-01", 10.0)])

        S3Writer().write(df, {"path": path, "mode": "overwrite"}, backend)

        with pytest.raises(Exception):
            S3Writer().write(df, {"path": path, "mode": "error"}, backend)

    def test_ignore_leaves_existing_data_untouched(self, spark, backend, tmp_path):
        path = _out(tmp_path)

        S3Writer().write(
            _rows(spark, [(1, "2026-01-01", 10.0)]), {"path": path, "mode": "overwrite"}, backend
        )
        S3Writer().write(
            _rows(spark, [(2, "2026-01-01", 20.0), (3, "2026-01-01", 30.0)]),
            {"path": path, "mode": "ignore"},
            backend,
        )

        result = spark.read.parquet(path)
        assert result.count() == 1
        assert result.collect()[0]["id"] == 1  # the original row, not the new ones


# ---------------------------------------------------------------------------
# overwrite_partitions — the one that must NOT wipe the table
# ---------------------------------------------------------------------------


class TestOverwritePartitions:
    def test_spark_honours_the_dynamic_conf_on_a_path_write(self, spark, tmp_path):
        """Control experiment — no Ubunye code, just Spark.

        Pins the mechanism the engine relies on for non-Delta paths. Note the
        `str(...)`: Spark *infers the type* of a partition column when reading the
        directory layout back, so `dt` returns as a `datetime.date`, not the string
        that was written. Comparing it against a string silently looks like a
        destroyed partition — which is exactly the false alarm that sent an earlier
        version of this suite hunting a bug that did not exist.
        """
        path = _out(tmp_path)
        _rows(
            spark,
            [(1, "2026-01-01", 10.0), (2, "2026-01-01", 20.0), (3, "2026-01-02", 30.0)],
        ).write.mode("overwrite").partitionBy("dt").format("parquet").save(path)

        previous = spark.conf.get(PARTITION_OVERWRITE_KEY, "STATIC")
        spark.conf.set(PARTITION_OVERWRITE_KEY, "DYNAMIC")
        try:
            _rows(spark, [(4, "2026-01-02", 40.0)]).write.mode("overwrite").partitionBy(
                "dt"
            ).format("parquet").save(path)
        finally:
            spark.conf.set(PARTITION_OVERWRITE_KEY, previous)

        dts = {str(r["dt"]) for r in spark.read.parquet(path).collect()}
        assert dts == {"2026-01-01", "2026-01-02"}, (
            f"Spark {spark.version} did not honour partitionOverwriteMode=DYNAMIC on a "
            "path write — the engine's overwrite_partitions cannot work this way."
        )

    def test_parquet_path_replaces_only_the_incoming_partition(self, spark, backend, tmp_path):
        """The whole point of the mode: re-run one day, leave the others standing."""
        path = _out(tmp_path)

        S3Writer().write(
            _rows(
                spark,
                [(1, "2026-01-01", 10.0), (2, "2026-01-01", 20.0), (3, "2026-01-02", 30.0)],
            ),
            {"path": path, "mode": "overwrite", "partitionBy": ["dt"]},
            backend,
        )

        # Re-run 2026-01-02 only.
        S3Writer().write(
            _rows(spark, [(4, "2026-01-02", 40.0), (5, "2026-01-02", 50.0)]),
            {"path": path, "mode": "overwrite_partitions", "partitionBy": ["dt"]},
            backend,
        )

        result = spark.read.parquet(path)
        by_dt = _count_by_dt(result)

        assert by_dt["2026-01-01"] == 2, "untouched partition was destroyed"
        assert by_dt["2026-01-02"] == 2, "target partition was not replaced"
        assert sorted(r["id"] for r in result.collect()) == [1, 2, 4, 5]

    def test_delta_path_replaces_only_the_incoming_partition(self, spark, backend, tmp_path):
        _requires_delta(spark)
        path = _out(tmp_path)

        DeltaWriter().write(
            _rows(
                spark,
                [(1, "2026-01-01", 10.0), (2, "2026-01-01", 20.0), (3, "2026-01-02", 30.0)],
            ),
            {"path": path, "mode": "overwrite", "partitionBy": ["dt"]},
            backend,
        )
        DeltaWriter().write(
            _rows(spark, [(4, "2026-01-02", 40.0), (5, "2026-01-02", 50.0)]),
            {"path": path, "mode": "overwrite_partitions", "partitionBy": ["dt"]},
            backend,
        )

        result = spark.read.format("delta").load(path)
        by_dt = _count_by_dt(result)

        assert by_dt["2026-01-01"] == 2, "untouched partition was destroyed"
        assert by_dt["2026-01-02"] == 2, "target partition was not replaced"

    def test_non_delta_table_uses_insert_overwrite(self, spark, backend, tmp_path):
        """A parquet *table* can do it — INSERT OVERWRITE is where Spark's conf
        actually bites. This is the route the hive writer takes."""
        table = f"tbl_{uuid.uuid4().hex[:8]}"

        HiveWriter().write(
            _rows(
                spark,
                [(1, "2026-01-01", 10.0), (2, "2026-01-01", 20.0), (3, "2026-01-02", 30.0)],
            ),
            {"table": table, "mode": "overwrite", "partitionBy": ["dt"]},
            backend,
        )

        HiveWriter().write(
            _rows(spark, [(4, "2026-01-02", 40.0)]),
            {"table": table, "mode": "overwrite_partitions", "partitionBy": ["dt"]},
            backend,
        )

        by_dt: dict = {}
        for r in spark.table(table).collect():
            by_dt.setdefault(str(r["dt"]), []).append(r["id"])

        assert sorted(by_dt["2026-01-01"]) == [1, 2], "untouched partition was destroyed"
        assert by_dt["2026-01-02"] == [4], "target partition was not replaced"

        spark.sql(f"DROP TABLE IF EXISTS {table}")

    def test_unpartitioned_target_is_refused(self, spark, backend, tmp_path):
        with pytest.raises(SinkWriteError, match="requires 'partitionBy' or 'replace_where'"):
            S3Writer().write(
                _rows(spark, [(1, "2026-01-01", 10.0)]),
                {"path": _out(tmp_path), "mode": "overwrite_partitions"},
                backend,
            )


# ---------------------------------------------------------------------------
# merge — a real Delta MERGE, the mode that used to crash
# ---------------------------------------------------------------------------


class TestDeltaMerge:
    def test_merge_creates_the_target_on_first_run(self, spark, backend, tmp_path):
        _requires_delta(spark)
        path = _out(tmp_path)

        DeltaWriter().write(
            _rows(spark, [(1, "2026-01-01", 10.0), (2, "2026-01-01", 20.0)]),
            {"path": path, "mode": "merge", "merge_keys": ["id"]},
            backend,
        )

        assert spark.read.format("delta").load(path).count() == 2

    def test_merge_updates_matched_and_inserts_new(self, spark, backend, tmp_path):
        """The behaviour the docs promised for months and the engine never had."""
        _requires_delta(spark)
        path = _out(tmp_path)

        DeltaWriter().write(
            _rows(spark, [(1, "2026-01-01", 10.0), (2, "2026-01-01", 20.0)]),
            {"path": path, "mode": "merge", "merge_keys": ["id"]},
            backend,
        )

        # id=1 updated, id=3 inserted, id=2 left alone.
        DeltaWriter().write(
            _rows(spark, [(1, "2026-01-01", 999.0), (3, "2026-01-01", 30.0)]),
            {"path": path, "mode": "merge", "merge_keys": ["id"]},
            backend,
        )

        rows = {r["id"]: r["amount"] for r in spark.read.format("delta").load(path).collect()}
        assert rows == {1: 999.0, 2: 20.0, 3: 30.0}

    def test_merge_on_composite_keys(self, spark, backend, tmp_path):
        _requires_delta(spark)
        path = _out(tmp_path)

        DeltaWriter().write(
            _rows(spark, [(1, "2026-01-01", 10.0), (1, "2026-01-02", 20.0)]),
            {"path": path, "mode": "merge", "merge_keys": ["id", "dt"]},
            backend,
        )
        # Same id, different dt — must update one and leave the other.
        DeltaWriter().write(
            _rows(spark, [(1, "2026-01-02", 999.0)]),
            {"path": path, "mode": "merge", "merge_keys": ["id", "dt"]},
            backend,
        )

        rows = {
            (r["id"], r["dt"]): r["amount"] for r in spark.read.format("delta").load(path).collect()
        }
        assert rows == {(1, "2026-01-01"): 10.0, (1, "2026-01-02"): 999.0}

    def test_merge_through_the_s3_writer_with_delta_file_format(self, spark, backend, tmp_path):
        _requires_delta(spark)
        path = _out(tmp_path)
        cfg = {
            "path": path,
            "file_format": "delta",
            "mode": "merge",
            "options": {"merge_keys": "id"},  # the comma-string form the docs use
        }

        S3Writer().write(_rows(spark, [(1, "2026-01-01", 10.0)]), cfg, backend)
        S3Writer().write(_rows(spark, [(1, "2026-01-01", 42.0)]), cfg, backend)

        rows = spark.read.format("delta").load(path).collect()
        assert len(rows) == 1 and rows[0]["amount"] == 42.0

    def test_replace_where_overwrites_only_the_predicate(self, spark, backend, tmp_path):
        _requires_delta(spark)
        path = _out(tmp_path)

        DeltaWriter().write(
            _rows(
                spark,
                [
                    (1, "2026-01-01", 10.0),
                    (2, "2026-01-02", 20.0),
                ],
            ),
            {"path": path, "mode": "overwrite", "partitionBy": ["dt"]},
            backend,
        )

        DeltaWriter().write(
            _rows(spark, [(3, "2026-01-02", 30.0)]),
            {
                "path": path,
                "mode": "overwrite_partitions",
                "partitionBy": ["dt"],
                "replace_where": "dt = '2026-01-02'",
            },
            backend,
        )

        rows = {r["id"]: r["dt"] for r in spark.read.format("delta").load(path).collect()}
        assert rows == {1: "2026-01-01", 3: "2026-01-02"}
