"""Unit tests for ubunye.core.catalog — mock-based, no real SparkSession."""

from __future__ import annotations

from unittest.mock import MagicMock

from ubunye.core.catalog import set_catalog_and_schema


class TestSetCatalogAndSchema:
    def test_noop_when_both_none(self):
        backend = MagicMock()
        set_catalog_and_schema(backend, catalog=None, schema=None)
        backend.spark.sql.assert_not_called()

    def test_sets_catalog(self):
        backend = MagicMock()
        set_catalog_and_schema(backend, catalog="workspace")
        backend.spark.sql.assert_called_once_with("USE CATALOG `workspace`")

    def test_sets_schema(self):
        backend = MagicMock()
        set_catalog_and_schema(backend, schema="titanic")
        backend.spark.sql.assert_called_once_with("USE SCHEMA `titanic`")

    def test_sets_both(self):
        backend = MagicMock()
        set_catalog_and_schema(backend, catalog="workspace", schema="titanic")
        calls = [c.args[0] for c in backend.spark.sql.call_args_list]
        assert calls == ["USE CATALOG `workspace`", "USE SCHEMA `titanic`"]

    def test_already_backticked_not_double_quoted(self):
        backend = MagicMock()
        set_catalog_and_schema(backend, catalog="`my-catalog`")
        backend.spark.sql.assert_called_once_with("USE CATALOG `my-catalog`")

    def test_hyphenated_catalog_quoted(self):
        backend = MagicMock()
        set_catalog_and_schema(backend, catalog="my-catalog")
        backend.spark.sql.assert_called_once_with("USE CATALOG `my-catalog`")

    def test_no_spark_attr_is_noop(self):
        backend = MagicMock(spec=[])  # no attributes at all
        set_catalog_and_schema(backend, catalog="workspace")
