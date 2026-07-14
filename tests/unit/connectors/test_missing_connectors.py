"""The connectors that were declared in FormatType and documented but never registered.

`format: delta` (read + write), `format: hive` (write) and `format: binary` (read)
all died with Reader/WriterNotFoundError before these plugins existed. These tests
pin the entry points as well as the behaviour, so a missing registration fails here
rather than in a user's pipeline.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from tests.conftest import MockBackend
from ubunye.core.errors import SinkWriteError, SourceReadError
from ubunye.core.runtime import Registry
from ubunye.plugins.readers.binary import BinaryReader
from ubunye.plugins.readers.delta import DeltaReader
from ubunye.plugins.writers.delta import DeltaWriter
from ubunye.plugins.writers.hive import HiveWriter


def _df(columns=("id", "dt")) -> MagicMock:
    df = MagicMock()
    df.columns = list(columns)
    writer = MagicMock()
    for method in ("mode", "format", "partitionBy", "option"):
        getattr(writer, method).return_value = writer
    df.write = writer
    return df


def _backend(spark: MagicMock | None = None) -> MockBackend:
    return MockBackend(spark_mock=spark or MagicMock())


def _reader_mock() -> MagicMock:
    spark = MagicMock()
    reader = MagicMock()
    reader.format.return_value = reader
    reader.option.return_value = reader
    spark.read = reader
    return spark


# ---------------------------------------------------------------------------
# Entry-point registration — every declared format must resolve to a plugin
# ---------------------------------------------------------------------------


class TestEveryFormatResolves:
    """A format in the schema with no plugin behind it is a trap."""

    # binary is read-only: Spark's binaryFile source cannot write.
    WRITE_ONLY_EXCLUSIONS = {"binary"}

    def test_every_registered_reader_implements_the_contract(self):
        """The registry is the source of truth, so ask IT what exists.

        The old version iterated `FormatType` and asserted each member had a reader --
        checking that the engine's hardcoded list matched the engine's entry points.
        Both were the engine's; it was marking its own homework. And it could not see a
        third-party connector at all.
        """
        from ubunye.core.interfaces import Reader

        registry = Registry.from_entrypoints()
        assert registry.readers, "no readers registered at all"
        for name, cls in registry.readers.items():
            assert issubclass(cls, Reader), f"'{name}' is registered but is not a Reader"
            assert callable(
                getattr(cls, "validate_config", None)
            ), f"'{name}' cannot declare its own requirements"

    def test_every_registered_writer_implements_the_contract(self):
        registry = Registry.from_entrypoints()
        assert registry.writers, "no writers registered at all"
        for name, cls in registry.writers.items():
            assert callable(getattr(cls, "write", None)), f"'{name}' cannot write"
            # Capabilities are read structurally, with defaults for the silent.
            assert isinstance(getattr(cls, "SUPPORTS_MERGE", False), bool)

    def test_binary_has_no_writer(self):
        assert "binary" not in Registry.from_entrypoints().writers


# ---------------------------------------------------------------------------
# Delta reader
# ---------------------------------------------------------------------------


class TestDeltaReader:
    def test_read_by_path(self):
        spark = _reader_mock()
        DeltaReader().read({"path": "s3a://b/delta/claims/"}, _backend(spark))

        spark.read.format.assert_called_once_with("delta")
        spark.read.load.assert_called_once_with("s3a://b/delta/claims/")

    def test_read_by_table(self):
        spark = _reader_mock()
        DeltaReader().read({"table": "main.fraud.claims"}, _backend(spark))

        spark.read.table.assert_called_once_with("main.fraud.claims")

    def test_read_by_db_and_tbl(self):
        spark = _reader_mock()
        DeltaReader().read({"db_name": "fraud", "tbl_name": "claims"}, _backend(spark))

        spark.read.table.assert_called_once_with("fraud.claims")

    def test_sql_wins(self):
        spark = _reader_mock()
        DeltaReader().read({"sql": "SELECT 1"}, _backend(spark))

        spark.sql.assert_called_once_with("SELECT 1")

    def test_time_travel_by_version(self):
        spark = _reader_mock()
        DeltaReader().read({"path": "s3a://b/d/", "version_as_of": 12}, _backend(spark))

        spark.read.option.assert_any_call("versionAsOf", "12")

    def test_time_travel_by_timestamp(self):
        spark = _reader_mock()
        DeltaReader().read({"path": "s3a://b/d/", "timestamp_as_of": "2026-01-01"}, _backend(spark))

        spark.read.option.assert_any_call("timestampAsOf", "2026-01-01")

    def test_no_target_raises(self):
        with pytest.raises(SourceReadError, match="requires 'path', 'table'"):
            DeltaReader().read({}, _backend())


# ---------------------------------------------------------------------------
# Delta writer
# ---------------------------------------------------------------------------


class TestDeltaWriter:
    def test_write_by_path(self):
        df = _df()
        DeltaWriter().write(df, {"path": "s3a://b/d/", "mode": "append"}, _backend())

        df.write.mode.assert_called_once_with("append")
        df.write.format.assert_called_once_with("delta")
        df.write.save.assert_called_once_with("s3a://b/d/")

    def test_write_by_table_uses_save_as_table(self):
        df = _df()
        DeltaWriter().write(df, {"table": "main.f.c", "mode": "overwrite"}, _backend())

        df.write.saveAsTable.assert_called_once_with("main.f.c")
        df.write.save.assert_not_called()

    def test_defaults_to_append(self):
        df = _df()
        DeltaWriter().write(df, {"path": "s3a://b/d/"}, _backend())

        df.write.mode.assert_called_once_with("append")

    def test_merge_upserts_into_existing_table(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        df = _df()

        DeltaWriter().write(
            df, {"table": "main.f.c", "mode": "merge", "merge_keys": ["id"]}, _backend(spark)
        )

        merge_sql = [c.args[0] for c in spark.sql.call_args_list if "MERGE INTO" in c.args[0]]
        assert merge_sql and "MERGE INTO main.f.c AS t" in merge_sql[0]

    def test_overwrite_partitions(self):
        """Delta honours partitionOverwriteMode as a write option — Spark's file
        sources do not (see the integration tier)."""
        df = _df()

        DeltaWriter().write(
            df,
            {"path": "s3a://b/d/", "mode": "overwrite_partitions", "partitionBy": ["dt"]},
            _backend(),
        )

        df.write.option.assert_any_call("partitionOverwriteMode", "dynamic")
        df.write.mode.assert_called_once_with("overwrite")

    def test_no_target_raises(self):
        with pytest.raises(SinkWriteError, match="requires 'path' or 'table'"):
            DeltaWriter().write(_df(), {}, _backend())


# ---------------------------------------------------------------------------
# Hive writer
# ---------------------------------------------------------------------------


class TestHiveWriter:
    def test_write_by_db_and_tbl(self):
        df = _df()
        HiveWriter().write(df, {"db_name": "fraud", "tbl_name": "claims"}, _backend())

        df.write.saveAsTable.assert_called_once_with("fraud.claims")
        df.write.mode.assert_called_once_with("append")  # default

    def test_defaults_to_parquet(self):
        df = _df()
        HiveWriter().write(df, {"table": "fraud.claims"}, _backend())

        df.write.format.assert_called_once_with("parquet")

    def test_merge_requires_delta_file_format(self):
        cfg = {"table": "fraud.claims", "mode": "merge", "merge_keys": ["id"]}

        with pytest.raises(SinkWriteError, match="requires a Delta target"):
            HiveWriter().write(_df(), cfg, _backend())

    def test_no_table_raises(self):
        with pytest.raises(SinkWriteError, match="requires 'table' or both"):
            HiveWriter().write(_df(), {}, _backend())


# ---------------------------------------------------------------------------
# Binary reader
# ---------------------------------------------------------------------------


class TestBinaryReader:
    def test_reads_binary_file_source(self):
        spark = _reader_mock()
        BinaryReader().read({"path": "/mnt/raw/docs/"}, _backend(spark))

        spark.read.format.assert_called_once_with("binaryFile")
        spark.read.load.assert_called_once_with("/mnt/raw/docs/")

    def test_glob_filter_and_recursion(self):
        spark = _reader_mock()
        BinaryReader().read(
            {"path": "/mnt/raw/docs/", "path_glob_filter": "*.pdf", "recursive": True},
            _backend(spark),
        )

        spark.read.option.assert_any_call("pathGlobFilter", "*.pdf")
        spark.read.option.assert_any_call("recursiveFileLookup", "true")

    def test_requires_path(self):
        with pytest.raises(SourceReadError, match="path"):
            BinaryReader().read({}, _backend())
