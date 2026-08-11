"""Spark execution of a resolved write mode (merge, dynamic partition overwrite).

Spark-free: ``spark`` and ``df`` are MagicMocks, so these run in the unit tier.
This is the mechanism that moved out of ``ubunye.core.write_modes`` under issue
#37; the resolution/grammar tests stay in ``tests/unit/core/test_write_modes.py``.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ubunye.adapters.spark import write_exec
from ubunye.core.errors import SinkWriteError


def _df(columns=("id", "dt", "amount")) -> MagicMock:
    df = MagicMock()
    df.columns = list(columns)
    return df


# ---------------------------------------------------------------------------
# dynamic_partition_overwrite() — session conf must not leak
# ---------------------------------------------------------------------------


class TestDynamicPartitionOverwrite:
    KEY = "spark.sql.sources.partitionOverwriteMode"

    def test_sets_dynamic_inside_and_restores_previous(self):
        spark = MagicMock()
        spark.conf.get.return_value = "STATIC"

        with write_exec.dynamic_partition_overwrite(spark):
            spark.conf.set.assert_called_once_with(self.KEY, "DYNAMIC")

        assert spark.conf.set.call_args_list[-1].args == (self.KEY, "STATIC")

    def test_restores_even_when_the_write_raises(self):
        spark = MagicMock()
        spark.conf.get.return_value = "STATIC"

        with pytest.raises(RuntimeError):
            with write_exec.dynamic_partition_overwrite(spark):
                raise RuntimeError("write blew up")

        assert spark.conf.set.call_args_list[-1].args == (self.KEY, "STATIC")

    def test_unsets_when_conf_was_not_previously_set(self):
        spark = MagicMock()
        spark.conf.get.side_effect = Exception("not set")

        with write_exec.dynamic_partition_overwrite(spark):
            pass

        spark.conf.unset.assert_called_once_with(self.KEY)


# ---------------------------------------------------------------------------
# merge_into()
# ---------------------------------------------------------------------------


class TestMergeInto:
    def test_emits_merge_statement_against_a_table(self):
        spark = MagicMock()
        df = _df()

        write_exec.merge_into(
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

        write_exec.merge_into(
            df, spark, connector="s3", merge_keys=["id"], path="s3a://bucket/features/"
        )

        sql = spark.sql.call_args.args[0]
        assert "MERGE INTO delta.`s3a://bucket/features/` AS t" in sql

    def test_temp_view_is_registered_and_dropped(self):
        spark = MagicMock()
        df = _df()

        write_exec.merge_into(df, spark, connector="s3", merge_keys=["id"], path="s3a://b/f/")

        view = df.createOrReplaceTempView.call_args.args[0]
        assert view.startswith("ubunye_merge_src_")
        spark.catalog.dropTempView.assert_called_once_with(view)

    def test_temp_view_dropped_even_when_merge_fails(self):
        spark = MagicMock()
        spark.sql.side_effect = RuntimeError("merge exploded")
        df = _df()

        with pytest.raises(RuntimeError):
            write_exec.merge_into(df, spark, connector="s3", merge_keys=["id"], path="s3a://b/f/")

        spark.catalog.dropTempView.assert_called_once()

    def test_merge_key_missing_from_dataframe_raises(self):
        spark = MagicMock()
        df = _df(columns=("dt", "amount"))  # no 'id'

        with pytest.raises(SinkWriteError, match="merge_keys not present"):
            write_exec.merge_into(df, spark, connector="unity", merge_keys=["id"], table="main.f.c")

        spark.sql.assert_not_called()

    def test_no_target_raises(self):
        with pytest.raises(SinkWriteError, match="needs a target table or path"):
            write_exec.merge_into(_df(), MagicMock(), connector="s3", merge_keys=["id"])


# ---------------------------------------------------------------------------
# target_exists()
# ---------------------------------------------------------------------------


class TestTargetExists:
    def test_table_present(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        assert write_exec.target_exists(spark, table="main.f.c") is True

    def test_table_absent(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        assert write_exec.target_exists(spark, table="main.f.c") is False

    def test_path_absent_when_describe_fails(self):
        spark = MagicMock()
        spark.sql.side_effect = Exception("not a delta table")
        assert write_exec.target_exists(spark, path="s3a://b/f/") is False

    def test_neither_table_nor_path(self):
        assert write_exec.target_exists(MagicMock()) is False
