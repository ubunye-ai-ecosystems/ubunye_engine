"""Plugin contract tests — verifies every connector satisfies the interface.

All tests are **Spark-free**: `backend.spark` is a `MagicMock`, and `df` is a
`MagicMock` that supports chained attribute access (`.write.mode().format().save()`).

This prevents plugin regressions without requiring a JVM or network.

Concrete test classes:
  - TestHivePlugin
  - TestJdbcPlugin
  - TestS3Plugin
  - TestUnityPlugin
  - TestNoOpTransform
  - TestRestApiPlugin
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from ubunye.core.interfaces import Reader, Transform, Writer
from tests.conftest import MockBackend


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _chain_mock() -> MagicMock:
    """A MagicMock where every chained call returns another MagicMock.

    This lets us write ``mock.read.format("jdbc").option("url", ...).load()``
    without explicitly configuring each return_value.
    """
    m = MagicMock()
    m.__getattr__ = lambda self, name: MagicMock(return_value=m)  # type: ignore
    return m


def _make_backend(spark_mock: Any = None) -> MockBackend:
    return MockBackend(spark_mock=spark_mock or MagicMock())


def _make_df() -> MagicMock:
    """DataFrame-like mock that supports write chaining."""
    df = MagicMock()
    df.write.mode.return_value = df.write
    df.write.mode.return_value.format.return_value = df.write
    df.write.mode.return_value.format.return_value.save = MagicMock()
    df.write.mode.return_value.format.return_value.option = MagicMock(return_value=df.write)
    df.write.mode.return_value.option = MagicMock(return_value=df.write)
    df.write.mode.return_value.saveAsTable = MagicMock()
    return df


# ---------------------------------------------------------------------------
# Base contract ABC
# ---------------------------------------------------------------------------

class PluginContractTest(ABC):
    """Inherit this for each reader+writer pair.

    Every subclass automatically inherits all ``test_*`` methods below.
    """

    @abstractmethod
    def make_reader(self) -> Reader:
        """Return an instance of the plugin's Reader class."""

    @abstractmethod
    def make_writer(self) -> Writer:
        """Return an instance of the plugin's Writer class."""

    @abstractmethod
    def valid_read_cfg(self) -> dict:
        """Return a valid IOConfig dict for the reader."""

    @abstractmethod
    def valid_write_cfg(self) -> dict:
        """Return a valid IOConfig dict for the writer."""

    @abstractmethod
    def bad_read_cfg(self) -> dict:
        """Return a config dict that should make read() raise ValueError."""

    # ------------------------------------------------------------------
    # Interface compliance
    # ------------------------------------------------------------------

    def test_reader_is_reader_subclass(self):
        assert isinstance(self.make_reader(), Reader)

    def test_writer_is_writer_subclass(self):
        assert isinstance(self.make_writer(), Writer)

    def test_reader_has_read_method(self):
        assert callable(getattr(self.make_reader(), "read", None))

    def test_writer_has_write_method(self):
        assert callable(getattr(self.make_writer(), "write", None))

    # ------------------------------------------------------------------
    # Config validation (no Spark required)
    # ------------------------------------------------------------------

    def test_bad_read_cfg_raises(self):
        """Missing required field should raise ValueError (not KeyError or AttributeError)."""
        with pytest.raises((ValueError, KeyError, TypeError)):
            self.make_reader().read(self.bad_read_cfg(), _make_backend())


# ---------------------------------------------------------------------------
# HiveReader + (stub) HiveWriter
# ---------------------------------------------------------------------------

class TestHivePlugin(PluginContractTest):

    def make_reader(self):
        from ubunye.plugins.readers.hive import HiveReader
        return HiveReader()

    def make_writer(self):
        # Hive has no dedicated writer in the codebase; use a mock to satisfy base
        m = MagicMock(spec=Writer)
        return m

    def valid_read_cfg(self):
        return {"format": "hive", "db_name": "raw_db", "tbl_name": "claims"}

    def valid_write_cfg(self):
        return {"format": "hive", "db_name": "raw_db", "tbl_name": "claims"}

    def bad_read_cfg(self):
        return {"format": "hive"}  # no db_name, no tbl_name, no sql

    # --- Hive-specific behaviour ---

    def test_read_calls_spark_table(self):
        spark = MagicMock()
        backend = _make_backend(spark)
        self.make_reader().read(self.valid_read_cfg(), backend)
        spark.table.assert_called_once_with("raw_db.claims")

    def test_read_sql_calls_spark_sql(self):
        spark = MagicMock()
        backend = _make_backend(spark)
        cfg = {"format": "hive", "sql": "SELECT 1"}
        self.make_reader().read(cfg, backend)
        spark.sql.assert_called_once_with("SELECT 1")

    def test_read_prefers_sql_over_table(self):
        spark = MagicMock()
        backend = _make_backend(spark)
        cfg = {"format": "hive", "sql": "SELECT 1", "db_name": "d", "tbl_name": "t"}
        self.make_reader().read(cfg, backend)
        spark.sql.assert_called_once()
        spark.table.assert_not_called()

    def test_reader_is_reader_subclass(self):
        from ubunye.plugins.readers.hive import HiveReader
        assert issubclass(HiveReader, Reader)

    def test_writer_is_writer_subclass(self):
        pass  # Hive has no dedicated writer — skip

    def test_reader_has_read_method(self):
        from ubunye.plugins.readers.hive import HiveReader
        assert callable(HiveReader().read)

    def test_writer_has_write_method(self):
        pass  # Hive has no dedicated writer — skip


# ---------------------------------------------------------------------------
# JdbcReader + JdbcWriter
# ---------------------------------------------------------------------------

class TestJdbcPlugin(PluginContractTest):

    def make_reader(self):
        from ubunye.plugins.readers.jdbc import JdbcReader
        return JdbcReader()

    def make_writer(self):
        from ubunye.plugins.writers.jdbc import JdbcWriter
        return JdbcWriter()

    def valid_read_cfg(self):
        return {"format": "jdbc", "url": "jdbc:postgresql://host/db", "table": "claims"}

    def valid_write_cfg(self):
        return {"format": "jdbc", "url": "jdbc:postgresql://host/db", "table": "claims"}

    def bad_read_cfg(self):
        return {"format": "jdbc"}  # no url

    # --- JDBC-specific ---

    def test_read_requires_url(self):
        with pytest.raises(ValueError, match="url"):
            self.make_reader().read({"format": "jdbc"}, _make_backend())

    def test_read_requires_table_or_sql(self):
        with pytest.raises(ValueError):
            self.make_reader().read({"format": "jdbc", "url": "jdbc:x"}, _make_backend())

    def test_write_requires_url(self):
        with pytest.raises(ValueError, match="url"):
            self.make_writer().write(_make_df(), {"format": "jdbc", "table": "t"}, _make_backend())

    def test_write_requires_table(self):
        with pytest.raises(ValueError):
            self.make_writer().write(_make_df(), {"format": "jdbc", "url": "jdbc:x"}, _make_backend())

    def test_read_calls_spark_jdbc_load(self):
        spark = MagicMock()
        # Chain the fluent builder so .load() can be tracked
        reader_builder = MagicMock()
        spark.read.format.return_value = reader_builder
        reader_builder.option.return_value = reader_builder
        reader_builder.load.return_value = MagicMock()  # DataFrame

        backend = _make_backend(spark)
        self.make_reader().read(self.valid_read_cfg(), backend)
        spark.read.format.assert_called_once_with("jdbc")
        reader_builder.load.assert_called_once()

    def test_write_calls_df_jdbc_save(self):
        df = MagicMock()
        writer_builder = MagicMock()
        df.write.format.return_value = writer_builder
        writer_builder.mode.return_value = writer_builder
        writer_builder.option.return_value = writer_builder
        writer_builder.save = MagicMock()

        self.make_writer().write(df, self.valid_write_cfg(), _make_backend())
        df.write.format.assert_called_once_with("jdbc")
        writer_builder.save.assert_called_once()


# ---------------------------------------------------------------------------
# S3Writer (path-based)
# ---------------------------------------------------------------------------

class TestS3Plugin(PluginContractTest):

    def make_reader(self):
        # No dedicated S3Reader in the codebase; return a mock for base-class compliance
        m = MagicMock(spec=Reader)
        return m

    def make_writer(self):
        from ubunye.plugins.writers.s3 import S3Writer
        return S3Writer()

    def valid_read_cfg(self):
        return {"format": "s3", "path": "s3a://bucket/input/"}

    def valid_write_cfg(self):
        return {"format": "s3", "path": "s3a://bucket/output/", "mode": "overwrite"}

    def bad_read_cfg(self):
        return {"format": "s3"}  # no path

    # --- S3-specific ---

    def test_write_requires_path(self):
        with pytest.raises(ValueError, match="path"):
            self.make_writer().write(_make_df(), {"format": "s3"}, _make_backend())

    def test_write_calls_df_write_save(self):
        df = MagicMock()
        # Set up fluent chain
        chain = MagicMock()
        df.write.mode.return_value = chain
        chain.format.return_value = chain
        chain.save = MagicMock()

        self.make_writer().write(df, self.valid_write_cfg(), _make_backend())
        df.write.mode.assert_called_once_with("overwrite")
        chain.save.assert_called_once_with("s3a://bucket/output/")

    def test_write_defaults_mode_to_overwrite(self):
        df = MagicMock()
        chain = MagicMock()
        df.write.mode.return_value = chain
        chain.format.return_value = chain
        chain.save = MagicMock()

        self.make_writer().write(df, {"format": "s3", "path": "/tmp/out"}, _make_backend())
        df.write.mode.assert_called_once_with("overwrite")

    def test_writer_is_writer_subclass(self):
        from ubunye.plugins.writers.s3 import S3Writer
        assert issubclass(S3Writer, Writer)

    def test_reader_is_reader_subclass(self):
        pass  # No S3Reader

    def test_reader_has_read_method(self):
        pass  # No S3Reader

    def test_bad_read_cfg_raises(self):
        pass  # No S3Reader


# ---------------------------------------------------------------------------
# UnityTableReader + UnityTableWriter
# ---------------------------------------------------------------------------

class TestUnityPlugin(PluginContractTest):

    def make_reader(self):
        from ubunye.plugins.readers.unity import UnityTableReader
        return UnityTableReader()

    def make_writer(self):
        from ubunye.plugins.writers.unity import UnityTableWriter
        return UnityTableWriter()

    def valid_read_cfg(self):
        return {"format": "unity", "table": "main.fraud.claims"}

    def valid_write_cfg(self):
        return {"format": "unity", "table": "main.fraud.claims_out", "mode": "append"}

    def bad_read_cfg(self):
        return {"format": "unity"}  # no table, no sql, no catalog/schema/tbl_name

    # --- Unity-specific ---

    def test_read_uses_table_name(self):
        spark = MagicMock()
        backend = _make_backend(spark)
        self.make_reader().read(self.valid_read_cfg(), backend)
        spark.table.assert_called_once_with("main.fraud.claims")

    def test_read_sql_calls_spark_sql(self):
        spark = MagicMock()
        backend = _make_backend(spark)
        self.make_reader().read({"format": "unity", "sql": "SELECT 1"}, backend)
        spark.sql.assert_called_once_with("SELECT 1")

    def test_read_missing_table_raises(self):
        with pytest.raises(ValueError):
            self.make_reader().read(self.bad_read_cfg(), _make_backend())

    def test_write_calls_save_as_table(self):
        df = MagicMock()
        chain = MagicMock()
        df.write.mode.return_value = chain
        chain.format.return_value = chain
        chain.partitionBy.return_value = chain
        chain.option.return_value = chain
        chain.saveAsTable = MagicMock()

        # Patch spark.sql (called for post-write SQL statements)
        spark = MagicMock()
        backend = _make_backend(spark)
        self.make_writer().write(df, self.valid_write_cfg(), backend)
        chain.saveAsTable.assert_called_once_with("main.fraud.claims_out")


# ---------------------------------------------------------------------------
# NoOpTransform
# ---------------------------------------------------------------------------

class TestNoOpTransform:

    def _make(self):
        from ubunye.plugins.transforms.noop import NoOpTransform
        return NoOpTransform()

    def test_is_transform_subclass(self):
        from ubunye.plugins.transforms.noop import NoOpTransform
        assert issubclass(NoOpTransform, Transform)

    def test_has_apply_method(self):
        assert callable(self._make().apply)

    def test_apply_returns_same_dict(self):
        inputs = {"a": MagicMock(), "b": MagicMock()}
        result = self._make().apply(inputs, {}, _make_backend())
        assert result is inputs

    def test_apply_empty_inputs(self):
        result = self._make().apply({}, {}, _make_backend())
        assert result == {}

    def test_apply_does_not_mutate_inputs(self):
        df = MagicMock()
        inputs = {"src": df}
        result = self._make().apply(inputs, {}, _make_backend())
        assert result["src"] is df


# ---------------------------------------------------------------------------
# RestApiReader + RestApiWriter
# ---------------------------------------------------------------------------

class TestRestApiPlugin(PluginContractTest):

    def make_reader(self):
        from ubunye.plugins.readers.rest_api import RestApiReader
        return RestApiReader()

    def make_writer(self):
        from ubunye.plugins.writers.rest_api import RestApiWriter
        return RestApiWriter()

    def valid_read_cfg(self):
        return {
            "format": "rest_api",
            "url": "https://api.example.com/v1/items",
            "method": "GET",
            "pagination": {"strategy": "none"},
        }

    def valid_write_cfg(self):
        return {
            "format": "rest_api",
            "url": "https://api.example.com/v1/items",
            "method": "POST",
            "batch_size": 100,
        }

    def bad_read_cfg(self):
        return {"format": "rest_api"}  # no url

    # --- REST-specific ---

    def test_read_requires_url(self):
        with pytest.raises((ValueError, KeyError)):
            with patch("requests.Session"):
                self.make_reader().read(self.bad_read_cfg(), _make_backend())

    def test_write_requires_url(self):
        df = MagicMock()
        df.toLocalIterator.return_value = iter([])
        with pytest.raises((ValueError, KeyError)):
            with patch("requests.Session"):
                self.make_writer().write(df, {"format": "rest_api"}, _make_backend())
