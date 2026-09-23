"""Backends declare what they can do; a task is checked before anything runs (ADR 002).

The check compares what each input and output needs (its connector, file format,
write mode, partitioning, path) with what the backend declares, and reports every
problem at once, so a run fails in the first second with the whole list rather
than halfway through with the first one.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ubunye.core.capabilities import Capabilities, check_task
from ubunye.core.errors import BackendCapabilityError
from ubunye.core.runtime import Engine, EngineContext, Registry

PANDAS_LIKE = Capabilities(
    features=frozenset({"path_io"}),
    file_formats=frozenset({"csv", "json", "parquet"}),
    write_modes=frozenset({"append", "overwrite", "errorifexists", "ignore"}),
)
SPARK_LIKE = Capabilities(
    features=frozenset({"spark", "path_io", "partitioned_writes", "remote_paths", "catalog"})
)


def _cfg(inputs=None, outputs=None):
    return {"CONFIG": {"inputs": inputs or {}, "outputs": outputs or {}, "transform": {}}}


@pytest.fixture(scope="module")
def reg():
    return Registry.from_entrypoints()


class TestCheck:
    def test_a_pandas_friendly_task_passes(self, reg):
        cfg = _cfg(
            {"a": {"format": "s3", "path": "/in.csv", "file_format": "csv"}},
            {"b": {"format": "s3", "path": "/out", "file_format": "parquet", "mode": "append"}},
        )
        assert check_task(PANDAS_LIKE, cfg, reg, backend_name="pandas") == []

    def test_a_spark_only_connector(self, reg):
        cfg = _cfg({"a": {"format": "hive", "db_name": "d", "tbl_name": "t"}})
        (problem,) = check_task(PANDAS_LIKE, cfg, reg, backend_name="pandas")
        assert "input 'a'" in problem and "'hive'" in problem and "spark" in problem

    def test_a_file_format_the_backend_cannot_read(self, reg):
        cfg = _cfg({"a": {"format": "s3", "path": "/t", "file_format": "delta"}})
        (problem,) = check_task(PANDAS_LIKE, cfg, reg, backend_name="pandas")
        assert "'delta'" in problem and "csv, json, parquet" in problem

    def test_the_default_file_format_is_parquet(self, reg):
        cfg = _cfg({"a": {"format": "s3", "path": "/t"}})
        assert check_task(PANDAS_LIKE, cfg, reg, backend_name="pandas") == []

    def test_a_write_mode_the_backend_cannot_do(self, reg):
        cfg = _cfg(
            outputs={
                "b": {
                    "format": "s3",
                    "path": "/t",
                    "file_format": "parquet",
                    "mode": "merge",
                    "merge_keys": ["id"],
                }
            }
        )
        (problem,) = check_task(PANDAS_LIKE, cfg, reg, backend_name="pandas")
        assert "output 'b'" in problem and "'merge'" in problem

    def test_partitioned_writes(self, reg):
        cfg = _cfg(outputs={"b": {"format": "s3", "path": "/t", "partition_by": ["d"]}})
        (problem,) = check_task(PANDAS_LIKE, cfg, reg, backend_name="pandas")
        assert "partition_by" in problem

    @pytest.mark.parametrize("path", ["s3a://b/k", "abfss://c@a.dfs.core.windows.net/x", "dbfs:/x"])
    def test_remote_paths(self, reg, path):
        cfg = _cfg({"a": {"format": "s3", "path": path, "file_format": "csv"}})
        (problem,) = check_task(PANDAS_LIKE, cfg, reg, backend_name="pandas")
        assert path in problem

    def test_a_local_file_uri_is_not_remote(self, reg):
        cfg = _cfg({"a": {"format": "s3", "path": "file:///tmp/x.csv", "file_format": "csv"}})
        assert check_task(PANDAS_LIKE, cfg, reg, backend_name="pandas") == []

    def test_every_problem_is_reported_at_once(self, reg):
        cfg = _cfg(
            {"a": {"format": "hive", "sql": "select 1"}, "c": {"format": "jdbc", "url": "x"}},
            {"b": {"format": "delta", "path": "/t"}},
        )
        assert len(check_task(PANDAS_LIKE, cfg, reg, backend_name="pandas")) == 3

    def test_spark_can_run_all_of_it(self, reg):
        cfg = _cfg(
            {"a": {"format": "hive", "sql": "select 1"}},
            {"b": {"format": "s3", "path": "s3a://x", "mode": "merge", "partition_by": ["d"]}},
        )
        assert check_task(SPARK_LIKE, cfg, reg, backend_name="spark") == []

    def test_an_undeclared_backend_is_not_second_guessed(self, reg):
        cfg = _cfg({"a": {"format": "hive", "sql": "select 1"}})
        assert check_task(Capabilities.unknown(), cfg, reg, backend_name="x") == []

    def test_an_unknown_connector_is_left_to_the_engine(self, reg):
        cfg = _cfg({"a": {"format": "no_such_plugin"}})
        assert check_task(PANDAS_LIKE, cfg, reg, backend_name="pandas") == []


class TestEnginePreflight:
    def test_the_engine_refuses_before_reading_anything(self, reg):
        backend = MagicMock()
        backend.capabilities = PANDAS_LIKE
        backend.name = "pandas"
        reader = MagicMock()
        reg.register_reader("hive_probe", reader)
        reg.readers["hive_probe"].REQUIRES = frozenset({"spark"})
        engine = Engine(backend=backend, registry=reg, context=EngineContext(run_id="r"), hooks=[])
        cfg = _cfg({"a": {"format": "hive_probe"}})
        with pytest.raises(BackendCapabilityError) as caught:
            engine.run(cfg)
        assert "hive_probe" in str(caught.value)
        reader.assert_not_called()
        backend.start.assert_not_called()

    def test_dry_run_checks_too(self, reg):
        backend = MagicMock()
        backend.capabilities = PANDAS_LIKE
        backend.name = "pandas"
        engine = Engine(backend=backend, registry=reg, context=EngineContext(run_id="r"), hooks=[])
        with pytest.raises(BackendCapabilityError):
            engine.run(_cfg({"a": {"format": "hive", "sql": "x"}}), dry_run=True)


def test_capabilities_describe_themselves():
    described = PANDAS_LIKE.describe()
    assert described["file_formats"] == ["csv", "json", "parquet"]
    assert described["features"] == ["path_io"]
    assert Capabilities.unknown().describe()["declared"] is False
