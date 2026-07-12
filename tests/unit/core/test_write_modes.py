"""Write-mode resolution and the two lakehouse modes (merge, overwrite_partitions).

Spark-free: ``spark`` and ``df`` are MagicMocks, so these run in the unit tier.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ubunye.core import write_modes
from ubunye.core.errors import SinkWriteError

ALL = write_modes.ALL_MODES


def _df(columns=("id", "dt", "amount")) -> MagicMock:
    df = MagicMock()
    df.columns = list(columns)
    return df


# ---------------------------------------------------------------------------
# resolve() — validation
# ---------------------------------------------------------------------------


class TestResolve:
    @pytest.mark.parametrize("mode", sorted(write_modes.NATIVE_SAVE_MODES))
    def test_native_modes_pass_through_to_spark(self, mode):
        resolved = write_modes.resolve(
            {"mode": mode}, connector="s3", supported=ALL, default="overwrite"
        )
        assert resolved.mode == mode
        assert resolved.save_mode == mode
        assert not resolved.is_merge
        assert not resolved.is_overwrite_partitions

    def test_error_is_an_alias_for_errorifexists(self):
        resolved = write_modes.resolve(
            {"mode": "error"}, connector="s3", supported=ALL, default="append"
        )
        assert resolved.save_mode == "errorifexists"

    def test_mode_is_case_insensitive(self):
        resolved = write_modes.resolve(
            {"mode": "APPEND"}, connector="s3", supported=ALL, default="overwrite"
        )
        assert resolved.save_mode == "append"

    def test_default_applies_when_mode_absent(self):
        resolved = write_modes.resolve({}, connector="jdbc", supported=ALL, default="append")
        assert resolved.save_mode == "append"

    def test_accepts_a_writemode_enum_not_just_a_string(self):
        """Engine.write_outputs() is public: a caller passing a bare model_dump()
        (rather than model_dump(mode="json")) hands the writer a WriteMode enum,
        whose str() is 'WriteMode.APPEND'."""
        from ubunye.config.schema import WriteMode

        resolved = write_modes.resolve(
            {"mode": WriteMode.APPEND}, connector="s3", supported=ALL, default="overwrite"
        )
        assert resolved.save_mode == "append"

    def test_unknown_mode_raises(self):
        with pytest.raises(SinkWriteError, match="Unknown write mode"):
            write_modes.resolve(
                {"mode": "truncate"}, connector="s3", supported=ALL, default="append"
            )

    def test_unsupported_by_connector_raises_and_lists_what_works(self):
        with pytest.raises(SinkWriteError, match="not supported by the 'jdbc' connector"):
            write_modes.resolve(
                {"mode": "merge", "merge_keys": ["id"]},
                connector="jdbc",
                supported=write_modes.NATIVE_SAVE_MODES,
                default="append",
            )


# ---------------------------------------------------------------------------
# resolve() — merge
# ---------------------------------------------------------------------------


class TestResolveMerge:
    def test_merge_keys_from_list(self):
        resolved = write_modes.resolve(
            {"mode": "merge", "merge_keys": ["id", "dt"]},
            connector="unity",
            supported=ALL,
            default="append",
            file_format="delta",
        )
        assert resolved.is_merge
        assert resolved.merge_keys == ("id", "dt")

    def test_merge_keys_from_comma_string_in_options(self):
        """The shape the connector docs use: options.merge_keys: 'id,dt'."""
        resolved = write_modes.resolve(
            {"mode": "merge", "options": {"merge_keys": "id, dt"}},
            connector="s3",
            supported=ALL,
            default="overwrite",
            file_format="delta",
        )
        assert resolved.merge_keys == ("id", "dt")

    def test_merge_without_keys_raises(self):
        with pytest.raises(SinkWriteError, match="requires 'merge_keys'"):
            write_modes.resolve(
                {"mode": "merge"},
                connector="s3",
                supported=ALL,
                default="overwrite",
                file_format="delta",
            )

    def test_merge_on_non_delta_format_raises(self):
        with pytest.raises(SinkWriteError, match="requires a Delta target"):
            write_modes.resolve(
                {"mode": "merge", "merge_keys": ["id"]},
                connector="s3",
                supported=ALL,
                default="overwrite",
                file_format="parquet",
            )

    def test_merge_falls_back_to_overwrite_for_target_creation(self):
        """First run has nothing to merge into — the target is created outright."""
        resolved = write_modes.resolve(
            {"mode": "merge", "merge_keys": ["id"]},
            connector="unity",
            supported=ALL,
            default="append",
            file_format="delta",
        )
        assert resolved.save_mode == "overwrite"


# ---------------------------------------------------------------------------
# resolve() — overwrite_partitions
# ---------------------------------------------------------------------------


class TestResolveOverwritePartitions:
    def test_partitioned_target_uses_dynamic_conf(self):
        resolved = write_modes.resolve(
            {"mode": "overwrite_partitions", "partitionBy": ["dt"]},
            connector="s3",
            supported=ALL,
            default="overwrite",
            file_format="parquet",
        )
        assert resolved.is_overwrite_partitions
        assert resolved.save_mode == "overwrite"
        assert resolved.needs_dynamic_partition_conf

    def test_replace_where_predicate_wins_over_dynamic_conf(self):
        resolved = write_modes.resolve(
            {"mode": "overwrite_partitions", "replace_where": "dt = '2026-01-01'"},
            connector="unity",
            supported=ALL,
            default="append",
            file_format="delta",
        )
        assert resolved.options == {"replaceWhere": "dt = '2026-01-01'"}
        assert not resolved.needs_dynamic_partition_conf

    def test_unpartitioned_without_predicate_raises(self):
        """Dynamic overwrite of an unpartitioned target is a silent full wipe."""
        with pytest.raises(SinkWriteError, match="requires 'partitionBy' or 'replace_where'"):
            write_modes.resolve(
                {"mode": "overwrite_partitions"},
                connector="s3",
                supported=ALL,
                default="overwrite",
                file_format="parquet",
            )

    def test_replace_where_on_non_delta_raises(self):
        with pytest.raises(SinkWriteError, match="'replace_where' requires a Delta target"):
            write_modes.resolve(
                {"mode": "overwrite_partitions", "replace_where": "dt = '2026-01-01'"},
                connector="s3",
                supported=ALL,
                default="overwrite",
                file_format="parquet",
            )


# ---------------------------------------------------------------------------
# dynamic_partition_overwrite() — session conf must not leak
# ---------------------------------------------------------------------------


class TestDynamicPartitionOverwrite:
    KEY = "spark.sql.sources.partitionOverwriteMode"

    def test_sets_dynamic_inside_and_restores_previous(self):
        spark = MagicMock()
        spark.conf.get.return_value = "STATIC"

        with write_modes.dynamic_partition_overwrite(spark):
            spark.conf.set.assert_called_once_with(self.KEY, "DYNAMIC")

        assert spark.conf.set.call_args_list[-1].args == (self.KEY, "STATIC")

    def test_restores_even_when_the_write_raises(self):
        spark = MagicMock()
        spark.conf.get.return_value = "STATIC"

        with pytest.raises(RuntimeError):
            with write_modes.dynamic_partition_overwrite(spark):
                raise RuntimeError("write blew up")

        assert spark.conf.set.call_args_list[-1].args == (self.KEY, "STATIC")

    def test_unsets_when_conf_was_not_previously_set(self):
        spark = MagicMock()
        spark.conf.get.side_effect = Exception("not set")

        with write_modes.dynamic_partition_overwrite(spark):
            pass

        spark.conf.unset.assert_called_once_with(self.KEY)


# ---------------------------------------------------------------------------
# merge_into()
# ---------------------------------------------------------------------------


class TestMergeInto:
    def test_emits_merge_statement_against_a_table(self):
        spark = MagicMock()
        df = _df()

        write_modes.merge_into(
            df, spark, connector="unity", merge_keys=["id", "dt"], table="main.fraud.claims"
        )

        sql = spark.sql.call_args.args[0]
        assert "MERGE INTO main.fraud.claims AS t" in sql
        assert "ON t.id = s.id AND t.dt = s.dt" in sql
        assert "WHEN MATCHED THEN UPDATE SET *" in sql
        assert "WHEN NOT MATCHED THEN INSERT *" in sql

    def test_emits_merge_statement_against_a_path(self):
        spark = MagicMock()
        df = _df()

        write_modes.merge_into(
            df, spark, connector="s3", merge_keys=["id"], path="s3a://bucket/features/"
        )

        sql = spark.sql.call_args.args[0]
        assert "MERGE INTO delta.`s3a://bucket/features/` AS t" in sql

    def test_temp_view_is_registered_and_dropped(self):
        spark = MagicMock()
        df = _df()

        write_modes.merge_into(df, spark, connector="s3", merge_keys=["id"], path="s3a://b/f/")

        view = df.createOrReplaceTempView.call_args.args[0]
        assert view.startswith("ubunye_merge_src_")
        spark.catalog.dropTempView.assert_called_once_with(view)

    def test_temp_view_dropped_even_when_merge_fails(self):
        spark = MagicMock()
        spark.sql.side_effect = RuntimeError("merge exploded")
        df = _df()

        with pytest.raises(RuntimeError):
            write_modes.merge_into(df, spark, connector="s3", merge_keys=["id"], path="s3a://b/f/")

        spark.catalog.dropTempView.assert_called_once()

    def test_merge_key_missing_from_dataframe_raises(self):
        spark = MagicMock()
        df = _df(columns=("dt", "amount"))  # no 'id'

        with pytest.raises(SinkWriteError, match="merge_keys not present"):
            write_modes.merge_into(
                df, spark, connector="unity", merge_keys=["id"], table="main.f.c"
            )

        spark.sql.assert_not_called()

    def test_no_target_raises(self):
        with pytest.raises(SinkWriteError, match="needs a target table or path"):
            write_modes.merge_into(_df(), MagicMock(), connector="s3", merge_keys=["id"])


# ---------------------------------------------------------------------------
# target_exists()
# ---------------------------------------------------------------------------


class TestTargetExists:
    def test_table_present(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        assert write_modes.target_exists(spark, table="main.f.c") is True

    def test_table_absent(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        assert write_modes.target_exists(spark, table="main.f.c") is False

    def test_path_absent_when_describe_fails(self):
        spark = MagicMock()
        spark.sql.side_effect = Exception("not a delta table")
        assert write_modes.target_exists(spark, path="s3a://b/f/") is False

    def test_neither_table_nor_path(self):
        assert write_modes.target_exists(MagicMock()) is False
