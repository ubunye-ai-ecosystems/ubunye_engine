"""Write modes exercised through the real writer plugins.

Spark-free: ``backend.spark`` and ``df`` are MagicMocks. These assert what each
connector actually hands to Spark for every supported mode, and that the modes
a connector *cannot* honour fail loudly instead of silently doing the wrong thing.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from tests.conftest import MockBackend
from ubunye.core.errors import SinkWriteError
from ubunye.plugins.writers.jdbc import JdbcWriter
from ubunye.plugins.writers.rest_api import RestApiWriter
from ubunye.plugins.writers.s3 import S3Writer
from ubunye.plugins.writers.unity import UnityTableWriter


def _df(columns=("id", "dt", "amount")) -> MagicMock:
    """DataFrame mock whose writer chain returns itself, so we can assert on calls."""
    df = MagicMock()
    df.columns = list(columns)
    writer = MagicMock()
    for method in ("mode", "format", "partitionBy", "option"):
        getattr(writer, method).return_value = writer
    df.write = writer
    return df


def _backend(spark: MagicMock | None = None) -> MockBackend:
    return MockBackend(spark_mock=spark or MagicMock())


# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------


class TestS3WriterModes:
    @pytest.mark.parametrize("mode", ["append", "overwrite", "errorifexists", "ignore"])
    def test_native_modes_reach_spark(self, mode):
        df = _df()
        S3Writer().write(df, {"path": "s3a://b/out/", "mode": mode}, _backend())

        df.write.mode.assert_called_once_with(mode)
        df.write.save.assert_called_once_with("s3a://b/out/")

    def test_defaults_to_append_not_overwrite(self):
        """s3 used to default to overwrite while every other writer defaulted to
        append — the same config key with the opposite blast radius."""
        df = _df()
        S3Writer().write(df, {"path": "s3a://b/out/"}, _backend())

        df.write.mode.assert_called_once_with("append")

    def test_options_are_applied(self):
        """Regression: options were documented but silently dropped by S3Writer."""
        df = _df()
        S3Writer().write(
            df,
            {
                "path": "s3a://b/out/",
                "file_format": "delta",
                "mode": "overwrite",
                "options": {"mergeSchema": "true"},
            },
            _backend(),
        )

        df.write.option.assert_any_call("mergeSchema", "true")

    def test_partition_by_is_applied(self):
        df = _df()
        S3Writer().write(
            df, {"path": "s3a://b/out/", "mode": "append", "partitionBy": ["dt"]}, _backend()
        )

        df.write.partitionBy.assert_called_once_with("dt")

    def test_merge_on_existing_target_emits_merge_statement(self):
        spark = MagicMock()  # DESCRIBE DETAIL succeeds -> target exists
        df = _df()

        S3Writer().write(
            df,
            {
                "path": "s3a://b/features/",
                "file_format": "delta",
                "mode": "merge",
                "merge_keys": ["id", "dt"],
            },
            _backend(spark),
        )

        merge_sql = [c.args[0] for c in spark.sql.call_args_list if "MERGE INTO" in c.args[0]]
        assert merge_sql, "expected a MERGE INTO statement"
        assert "ON t.id = s.id AND t.dt = s.dt" in merge_sql[0]
        df.write.save.assert_not_called()

    def test_merge_creates_the_target_on_first_run(self):
        spark = MagicMock()
        spark.sql.side_effect = Exception("not a delta table")  # target does not exist
        df = _df()

        S3Writer().write(
            df,
            {
                "path": "s3a://b/features/",
                "file_format": "delta",
                "mode": "merge",
                "merge_keys": ["id"],
            },
            _backend(spark),
        )

        df.write.mode.assert_called_once_with("overwrite")
        df.write.save.assert_called_once_with("s3a://b/features/")

    def test_merge_keys_are_not_leaked_to_spark_as_an_option(self):
        spark = MagicMock()
        spark.sql.side_effect = Exception("not a delta table")
        df = _df()

        S3Writer().write(
            df,
            {
                "path": "s3a://b/f/",
                "file_format": "delta",
                "mode": "merge",
                "options": {"merge_keys": "id"},
            },
            _backend(spark),
        )

        assert all(c.args[0] != "merge_keys" for c in df.write.option.call_args_list)

    def test_overwrite_partitions_on_delta_uses_the_delta_write_option(self):
        """Spark ignores partitionOverwriteMode on a path write (verified against
        Spark 4.1 in the integration tier). Delta honours it as a write option."""
        df = _df()

        S3Writer().write(
            df,
            {
                "path": "s3a://b/out/",
                "file_format": "delta",
                "mode": "overwrite_partitions",
                "partitionBy": ["dt"],
            },
            _backend(),
        )

        df.write.option.assert_any_call("partitionOverwriteMode", "dynamic")
        df.write.mode.assert_called_once_with("overwrite")

    def test_overwrite_partitions_on_a_non_delta_path_is_refused(self):
        """The dead end: Spark cannot do this on a path write, and doing it anyway
        silently wipes every other partition. Refuse instead."""
        with pytest.raises(SinkWriteError, match="cannot be done safely on a non-Delta path"):
            S3Writer().write(
                _df(),
                {"path": "s3a://b/out/", "mode": "overwrite_partitions", "partitionBy": ["dt"]},
                _backend(),
            )

    def test_overwrite_partitions_with_replace_where_uses_the_predicate(self):
        df = _df()

        S3Writer().write(
            df,
            {
                "path": "s3a://b/out/",
                "file_format": "delta",
                "mode": "overwrite_partitions",
                "replace_where": "dt = '2026-01-01'",
            },
            _backend(),
        )

        df.write.option.assert_any_call("replaceWhere", "dt = '2026-01-01'")

    def test_unpartitioned_overwrite_partitions_refuses_to_wipe_the_table(self):
        with pytest.raises(SinkWriteError, match="requires 'partitionBy' or 'replace_where'"):
            S3Writer().write(
                _df(), {"path": "s3a://b/out/", "mode": "overwrite_partitions"}, _backend()
            )


# ---------------------------------------------------------------------------
# Unity
# ---------------------------------------------------------------------------


class TestUnityWriterModes:
    @pytest.mark.parametrize("mode", ["append", "overwrite", "errorifexists", "ignore"])
    def test_native_modes_reach_spark(self, mode):
        df = _df()
        UnityTableWriter().write(df, {"table": "main.f.c", "mode": mode}, _backend())

        df.write.mode.assert_called_once_with(mode)
        df.write.saveAsTable.assert_called_once_with("main.f.c")

    def test_merge_on_existing_table(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        df = _df()

        UnityTableWriter().write(
            df,
            {"table": "main.f.claims", "mode": "merge", "merge_keys": ["id"]},
            _backend(spark),
        )

        merge_sql = [c.args[0] for c in spark.sql.call_args_list if "MERGE INTO" in c.args[0]]
        assert merge_sql and "MERGE INTO main.f.claims AS t" in merge_sql[0]
        df.write.saveAsTable.assert_not_called()

    def test_merge_creates_the_table_on_first_run(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        df = _df()

        UnityTableWriter().write(
            df,
            {"table": "main.f.claims", "mode": "merge", "merge_keys": ["id"]},
            _backend(spark),
        )

        df.write.mode.assert_called_once_with("overwrite")
        df.write.saveAsTable.assert_called_once_with("main.f.claims")

    def test_overwrite_partitions_with_replace_where(self):
        spark = MagicMock()
        df = _df()

        UnityTableWriter().write(
            df,
            {
                "table": "main.f.c",
                "mode": "overwrite_partitions",
                "replace_where": "ds = '2026-01-01'",
            },
            _backend(spark),
        )

        df.write.option.assert_any_call("replaceWhere", "ds = '2026-01-01'")
        df.write.mode.assert_called_once_with("overwrite")

    def test_overwrite_partitions_without_predicate_uses_delta_dynamic(self):
        """Unity tables are Delta by default, so the Delta write option applies."""
        df = _df()

        UnityTableWriter().write(
            df,
            {"table": "main.f.c", "mode": "overwrite_partitions", "partitionBy": ["ds"]},
            _backend(),
        )

        df.write.option.assert_any_call("partitionOverwriteMode", "dynamic")


# ---------------------------------------------------------------------------
# JDBC — native modes only
# ---------------------------------------------------------------------------


class TestJdbcWriterModes:
    BASE = {"url": "jdbc:postgresql://db:5432/x", "table": "t"}

    @pytest.mark.parametrize("mode", ["append", "overwrite", "errorifexists", "ignore"])
    def test_native_modes_reach_spark(self, mode):
        df = _df()
        JdbcWriter().write(df, {**self.BASE, "mode": mode}, _backend())

        df.write.mode.assert_called_once_with(mode)

    def test_defaults_to_append(self):
        df = _df()
        JdbcWriter().write(df, dict(self.BASE), _backend())

        df.write.mode.assert_called_once_with("append")

    def test_error_alias_normalised(self):
        df = _df()
        JdbcWriter().write(df, {**self.BASE, "mode": "error"}, _backend())

        df.write.mode.assert_called_once_with("errorifexists")

    @pytest.mark.parametrize("mode", ["merge", "overwrite_partitions"])
    def test_lakehouse_modes_rejected(self, mode):
        cfg = {**self.BASE, "mode": mode, "merge_keys": ["id"], "partitionBy": ["dt"]}

        with pytest.raises(SinkWriteError, match="not supported by the 'jdbc' connector"):
            JdbcWriter().write(_df(), cfg, _backend())


# ---------------------------------------------------------------------------
# REST — append-only sink
# ---------------------------------------------------------------------------


class TestRestApiWriterModes:
    @pytest.mark.parametrize("mode", ["merge", "overwrite_partitions"])
    def test_lakehouse_modes_rejected(self, mode):
        cfg = {"url": "https://api.example.com/v1", "mode": mode, "merge_keys": ["id"]}

        with pytest.raises(SinkWriteError, match="not supported by the 'rest_api' connector"):
            RestApiWriter().write(_df(), cfg, _backend())
