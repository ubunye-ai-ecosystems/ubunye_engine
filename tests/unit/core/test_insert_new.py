"""Write mode ``insert_new``: insert rows whose keys the target lacks, change nothing else.

The contract (ADR 018): ``merge_keys`` are required; a row whose keys are already in
the target is left out, so the first write of a key wins and a rerun adds nothing.
The decision is in the core (``resolve``); Spark does it with a Delta MERGE that
only inserts; backends and connectors that cannot do it refuse it before a row is
written. Spark's real behaviour is pinned in
``tests/integration/test_write_modes_integration.py::TestDeltaInsertNew``.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from ubunye.core import write_modes
from ubunye.core.errors import SinkWriteError

ALL = write_modes.ALL_MODES


class TestResolve:
    def test_is_a_mode(self):
        assert "insert_new" in write_modes.ALL_MODES
        assert "insert_new" in write_modes.LAKEHOUSE_MODES

    def test_resolves_with_its_keys(self):
        resolved = write_modes.resolve(
            {"mode": "insert_new", "merge_keys": ["id", "dt"]},
            connector="delta",
            supported=ALL,
            default="append",
            file_format="delta",
        )
        assert resolved.mode == "insert_new"
        assert resolved.is_insert_new
        assert not resolved.is_merge
        assert resolved.merge_keys == ("id", "dt")
        # A target that does not exist yet is created; one that exists but is not
        # Delta is refused, never replaced.
        assert resolved.save_mode == "errorifexists"

    def test_keys_as_a_comma_string_under_options(self):
        resolved = write_modes.resolve(
            {"mode": "INSERT_NEW", "options": {"merge_keys": "id, dt"}},
            connector="s3",
            supported=ALL,
            default="append",
            file_format="delta",
        )
        assert resolved.merge_keys == ("id", "dt")

    def test_needs_merge_keys(self):
        with pytest.raises(SinkWriteError, match="'insert_new' requires 'merge_keys'"):
            write_modes.resolve(
                {"mode": "insert_new"}, connector="delta", supported=ALL, default="append"
            )

    def test_needs_a_delta_target(self):
        with pytest.raises(SinkWriteError, match="'insert_new' requires a Delta target"):
            write_modes.resolve(
                {"mode": "insert_new", "merge_keys": ["id"]},
                connector="s3",
                supported=ALL,
                default="append",
                file_format="parquet",
            )

    def test_a_connector_that_declares_it_can_merge_iceberg_may_insert_new_too(self):
        resolved = write_modes.resolve(
            {"mode": "insert_new", "merge_keys": ["id"]},
            connector="iceberg",
            supported=ALL,
            default="append",
            file_format="iceberg",
            merge_formats={"iceberg"},
        )
        assert resolved.is_insert_new

    def test_refused_by_a_connector_that_does_not_list_it(self):
        with pytest.raises(SinkWriteError, match="not supported by the 'jdbc' connector"):
            write_modes.resolve(
                {"mode": "insert_new", "merge_keys": ["id"]},
                connector="jdbc",
                supported=write_modes.NATIVE_SAVE_MODES,
                default="append",
            )

    def test_the_schema_enum_knows_it(self):
        from ubunye.config.schema import WriteMode

        assert WriteMode("insert_new") is WriteMode.INSERT_NEW


# ---------------------------------------------------------------------------
# validate and plan
# ---------------------------------------------------------------------------

S3_IN = {"format": "s3", "path": "in.csv", "file_format": "csv"}


def _cfg(out):
    return {
        "MODEL": "etl",
        "VERSION": "1.0.0",
        "CONFIG": {"inputs": {"src": S3_IN}, "outputs": {"out": out}},
    }


class TestValidate:
    def test_delta_insert_new_with_keys_passes(self):
        from ubunye.config.schema import UbunyeConfig

        UbunyeConfig(
            **_cfg({"format": "delta", "path": "/x", "mode": "insert_new", "merge_keys": ["id"]})
        )

    def test_without_keys_fails_validation(self):
        from ubunye.config.schema import UbunyeConfig

        with pytest.raises(ValidationError, match="'insert_new' requires 'merge_keys'"):
            UbunyeConfig(**_cfg({"format": "delta", "path": "/x", "mode": "insert_new"}))

    def test_on_parquet_fails_validation(self):
        from ubunye.config.schema import UbunyeConfig

        with pytest.raises(ValidationError, match="requires a Delta target"):
            UbunyeConfig(
                **_cfg(
                    {
                        "format": "s3",
                        "path": "/x",
                        "file_format": "parquet",
                        "mode": "insert_new",
                        "merge_keys": ["id"],
                    }
                )
            )

    def test_on_jdbc_fails_validation(self):
        from ubunye.config.schema import UbunyeConfig

        jdbc = {"format": "jdbc", "url": "jdbc:postgresql://db/x", "table": "public.t"}
        with pytest.raises(ValidationError, match="'insert_new' is not supported by the 'jdbc'"):
            UbunyeConfig(**_cfg(dict(jdbc, mode="insert_new", merge_keys=["id"])))

    def test_pandas_backend_refuses_it_before_the_run(self):
        pytest.importorskip("pandas")
        from ubunye.backends.pandas_backend import PandasBackend
        from ubunye.core.capabilities import check_task
        from ubunye.core.runtime import Registry

        cfg = _cfg(
            {
                "format": "s3",
                "path": "/x",
                "file_format": "delta",
                "mode": "insert_new",
                "merge_keys": ["id"],
            }
        )
        problems = check_task(
            PandasBackend.CAPABILITIES,
            cfg,
            Registry.from_entrypoints(),
            backend_name="pandas",
            io_check=PandasBackend.check_io,
        )
        assert any("insert_new" in p for p in problems), problems


def test_plan_shows_the_mode_and_keys_and_an_existing_target_is_fine(tmp_path):
    pytest.importorskip("pandas")
    from ubunye.config import load_config
    from ubunye.core.planning import build_plan

    (tmp_path / "in.csv").write_text("id\n1\n", encoding="utf-8")
    target = tmp_path / "target"
    target.mkdir()
    (target / "part-0.parquet").write_bytes(b"x")  # a target from an earlier run
    task = tmp_path / "uc" / "pkg" / "t"
    task.mkdir(parents=True)
    (task / "transformations.py").write_text(
        "from ubunye.core.interfaces import Task\n\n"
        "class T(Task):\n"
        "    def transform(self, sources):\n"
        "        return {'out': sources['src']}\n",
        encoding="utf-8",
    )
    (task / "config.yaml").write_text(
        f"""\
MODEL: etl
VERSION: "1.0.0"
CONFIG:
  inputs:
    src: {{format: s3, path: "{tmp_path / 'in.csv'}", file_format: csv}}
  transform: {{}}
  outputs:
    out:
      format: delta
      path: "{target}"
      mode: insert_new
      merge_keys: [id]
""",
        encoding="utf-8",
    )
    cfg = load_config(str(task / "config.yaml"))
    report = build_plan(cfg, task_name="t", task_dir=task)
    out = report["outputs"][0]
    assert out["resolved_mode"] == "insert_new"
    assert out["merge_keys"] == ["id"]
    assert report["problems"] == [], report["problems"]


# ---------------------------------------------------------------------------
# Spark execution (mocked here; real Spark in the integration tier)
# ---------------------------------------------------------------------------


def _df(columns=("id", "dt", "amount")) -> MagicMock:
    df = MagicMock()
    df.columns = list(columns)
    return df


def _insert_new(keys=("id",)):
    return write_modes.ResolvedWriteMode(
        mode="insert_new", save_mode="errorifexists", merge_keys=tuple(keys)
    )


class TestSparkExecution:
    def test_merge_only_inserts_on_null_safe_keys(self):
        from ubunye.adapters.spark import write_exec

        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        write_exec.apply(
            _df(),
            spark,
            _insert_new(("id", "dt")),
            connector="unity",
            file_format="delta",
            table="`aws-x`.gold.t",
        )
        sql = spark.sql.call_args.args[0]
        assert "MERGE INTO `aws-x`.gold.t AS t" in sql
        assert "ON t.id <=> s.id AND t.dt <=> s.dt" in sql
        assert "WHEN NOT MATCHED THEN INSERT *" in sql
        assert "WHEN MATCHED" not in sql

    def test_against_a_path(self):
        from ubunye.adapters.spark import write_exec

        spark = MagicMock()  # DESCRIBE DETAIL answers: the target exists
        write_exec.apply(
            _df(), spark, _insert_new(), connector="s3", file_format="delta", path="s3a://b/t/"
        )
        assert "MERGE INTO delta.`s3a://b/t/` AS t" in spark.sql.call_args.args[0]

    def test_first_run_creates_the_target_without_replacing_anything(self):
        from ubunye.adapters.spark import write_exec

        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        df = _df()
        write_exec.apply(
            df, spark, _insert_new(), connector="delta", file_format="delta", table="db.t"
        )
        df.write.mode.assert_called_once_with("errorifexists")
        assert not any("MERGE" in str(c.args[0]) for c in spark.sql.call_args_list)

    def test_a_key_missing_from_the_frame_is_refused(self):
        from ubunye.adapters.spark import write_exec

        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        with pytest.raises(SinkWriteError, match="merge_keys not present"):
            write_exec.apply(
                _df(("dt",)),
                spark,
                _insert_new(),
                connector="delta",
                file_format="delta",
                table="db.t",
            )


def test_pandas_backend_refuses_it_and_writes_nothing(tmp_path):
    pd = pytest.importorskip("pandas")
    from ubunye.adapters.pandas_adapter import PandasDataFrameAdapter
    from ubunye.backends.pandas_backend import PandasBackend

    out = tmp_path / "d"
    with pytest.raises(SinkWriteError, match="does not support write mode 'insert_new'"):
        PandasBackend().execute_write(
            PandasDataFrameAdapter(pd.DataFrame({"id": [1]})),
            _insert_new(),
            connector="s3",
            file_format="parquet",
            path=str(out),
        )
    assert not out.exists()


def test_rest_api_refuses_it():
    from ubunye.plugins.writers.rest_api import _check_mode

    with pytest.raises(SinkWriteError, match="'insert_new' is not supported by the 'rest_api'"):
        _check_mode({"mode": "insert_new"})
