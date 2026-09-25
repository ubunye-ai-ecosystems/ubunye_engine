"""``ubunye plan``: what will happen if I run this now, and what will stop it.

The plan used to print the config's names back. It now checks, without
starting a backend or reading a row, the things that used to fail halfway
through a run, and exits 1 when it finds one.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

import ubunye  # noqa: E402
from ubunye.cli.main import app  # noqa: E402
from ubunye.config import load_config  # noqa: E402
from ubunye.core.planning import build_plans  # noqa: E402
from ubunye.lineage.storage import FileSystemLineageStore  # noqa: E402

runner = CliRunner()

TRANSFORM = """\
from ubunye.core.interfaces import Task


class Copy(Task):
    def transform(self, sources):
        return {"out": sources["src"]}
"""


def _task(
    root: Path,
    name: str,
    *,
    src: str,
    out: str,
    src_format: str = "s3",
    mode: str = "overwrite",
    extra_out: str = "",
    transform: str = TRANSFORM,
) -> Path:
    task = root / "uc" / "pkg" / name
    task.mkdir(parents=True)
    if transform is not None:
        (task / "transformations.py").write_text(transform, encoding="utf-8")
    source = (
        f'format: s3\n      path: "{src}"\n      file_format: csv\n'
        '      options:\n        header: "true"'
        if src_format == "s3"
        else "format: hive\n      db_name: d\n      tbl_name: t"
    )
    (task / "config.yaml").write_text(
        f"""\
MODEL: etl
VERSION: "1.0.0"
CONFIG:
  inputs:
    src:
      {source}
  transform: {{}}
  outputs:
    out:
      format: s3
      path: "{out}"
      file_format: csv
      options:
        header: "true"
      mode: {mode}
{extra_out}""",
        encoding="utf-8",
    )
    return task


def _plan(root: Path, *tasks: str, extra=()):
    args = ["plan", "-d", str(root), "-u", "uc", "-p", "pkg"]
    for t in tasks:
        args += ["-t", t]
    return runner.invoke(app, [*args, *extra])


@pytest.fixture
def data(tmp_path):
    (tmp_path / "in.csv").write_text("id\n1\n", encoding="utf-8")
    return tmp_path


class TestOneTask:
    def test_a_task_that_can_run_plans_clean(self, data):
        _task(data, "a", src=(data / "in.csv").as_posix(), out=(data / "mid").as_posix())
        result = _plan(data, "a")
        assert result.exit_code == 0, result.output
        assert "found: 1 file" in result.output
        assert "Copy" in result.output  # the transform class, resolved
        assert "overwrite" in result.output
        assert "Plan OK" in result.output

    def test_a_missing_input_is_a_problem_and_exit_1(self, data):
        _task(data, "a", src=(data / "nope.csv").as_posix(), out=(data / "mid").as_posix())
        result = _plan(data, "a")
        assert result.exit_code == 1
        assert "inputs.src: nothing at" in result.output

    def test_merge_without_keys_is_caught_before_running(self, data):
        _task(
            data, "a", src=(data / "in.csv").as_posix(), out=(data / "mid").as_posix(), mode="merge"
        )
        result = _plan(data, "a")
        assert result.exit_code == 1 and "outputs.out" in result.output

    def test_a_missing_transformations_file(self, data):
        _task(
            data,
            "a",
            src=(data / "in.csv").as_posix(),
            out=(data / "mid").as_posix(),
            transform=None,
        )
        result = _plan(data, "a")
        assert result.exit_code == 1 and "transformations.py" in result.output

    def test_an_unset_environment_variable_is_a_warning(self, data, monkeypatch):
        monkeypatch.delenv("UBUNYE_TEST_MISSING", raising=False)
        _task(
            data,
            "a",
            src=(data / "in.csv").as_posix(),
            out=(data / "mid").as_posix() + "/{{ env.UBUNYE_TEST_MISSING | default('x') }}",
        )
        result = _plan(data, "a")
        assert result.exit_code == 0, result.output
        assert "UBUNYE_TEST_MISSING" in result.output

    def test_variables_reach_the_plan(self, data):
        _task(
            data,
            "a",
            src=(data / "in.csv").as_posix(),
            out=(data / "out").as_posix() + "/{{ region }}",
        )
        result = _plan(data, "a", extra=["--var", "region=gp"])
        assert result.exit_code == 0, result.output
        assert "out/gp" in result.output


class TestManyTasks:
    def test_an_input_written_by_an_earlier_task_counts(self, data):
        """The September plan failed this: b reads what a writes."""
        mid = (data / "mid").as_posix()
        _task(data, "a", src=(data / "in.csv").as_posix(), out=mid)
        _task(data, "b", src=mid, out=(data / "final").as_posix())
        result = _plan(data, "a", "b")
        assert result.exit_code == 0, result.output
        assert "written by task 'a'" in result.output

    def test_but_not_when_the_order_is_wrong(self, data):
        mid = (data / "mid").as_posix()
        _task(data, "a", src=(data / "in.csv").as_posix(), out=mid)
        _task(data, "b", src=mid, out=(data / "final").as_posix())
        result = _plan(data, "b", "a")
        assert result.exit_code == 1 and "inputs.src: nothing at" in result.output


class TestBackends:
    def test_capabilities_decide_not_a_list(self, data):
        _task(data, "a", src="", src_format="hive", out=(data / "mid").as_posix())
        result = _plan(data, "a", extra=["--backend", "pandas"])
        assert result.exit_code == 1
        assert "'hive' connector, which needs spark" in result.output

    def test_the_same_task_is_fine_on_spark(self, data):
        _task(data, "a", src="", src_format="hive", out=(data / "mid").as_posix())
        result = _plan(data, "a", extra=["--backend", "spark"])
        assert "needs spark" not in result.output

    def test_an_unknown_backend(self, data):
        _task(data, "a", src=(data / "in.csv").as_posix(), out=(data / "mid").as_posix())
        result = _plan(data, "a", extra=["--backend", "duckdb"])
        assert result.exit_code == 1 and "duckdb" in result.output


# pyspark is imported inside the method, so the class loads where pyspark is not
# installed and the plan's verdict is the only thing under test.
SPARK_TRANSFORM = """\
from ubunye.core.interfaces import Task


class Adults(Task):
    def transform(self, sources):
        from pyspark.sql import functions as F

        return {"out": sources["src"].filter(F.col("id") > 0)}
"""


class TestWhatTheTransformIsWrittenFor:
    """ADR 005: portability is detected from the transform's imports, not declared."""

    def test_a_spark_api_transform_warns_before_a_pandas_run(self, data):
        _task(
            data,
            "a",
            src=(data / "in.csv").as_posix(),
            out=(data / "mid").as_posix(),
            transform=SPARK_TRANSFORM,
        )
        result = _plan(data, "a", extra=["--backend", "pandas"])
        assert result.exit_code == 0, result.output  # a warning: it may still be fine
        assert "written for pyspark" in result.output
        assert "warning: transform:" in result.output and "narwhals" in result.output

    def test_it_is_quiet_on_spark(self, data):
        _task(
            data,
            "a",
            src=(data / "in.csv").as_posix(),
            out=(data / "mid").as_posix(),
            transform=SPARK_TRANSFORM,
        )
        result = _plan(data, "a", extra=["--backend", "spark"])
        assert "warning: transform:" not in result.output

    def test_the_json_plan_says_it_too(self, data):
        import json

        _task(
            data,
            "a",
            src=(data / "in.csv").as_posix(),
            out=(data / "mid").as_posix(),
            transform="import narwhals as nw\n" + TRANSFORM,
        )
        result = _plan(data, "a", extra=["--backend", "pandas", "--json"])
        (plan,) = json.loads(result.output)["tasks"]
        assert plan["transform"]["frame_api"] == "narwhals"
        assert plan["transform"]["frame_imports"] == ["narwhals"]


def test_the_plan_and_the_run_record_agree_on_the_config_hash(data):
    task = _task(data, "a", src=(data / "in.csv").as_posix(), out=(data / "mid").as_posix())
    cfg = load_config(str(task), variables={"dt": None, "dtf": None, "mode": "DEV"})
    (plan,) = build_plans([("uc/pkg/a", cfg, task)], backend="pandas")
    ubunye.run_task(str(task), backend="pandas", lineage=True)
    (record,) = FileSystemLineageStore(str(data / ".ubunye" / "lineage")).list_runs("uc/pkg/a")
    assert plan["config_hash"] == record.config_hash


def test_plan_reads_and_writes_nothing(data):
    _task(data, "a", src=(data / "in.csv").as_posix(), out=(data / "mid").as_posix())
    _plan(data, "a")
    assert not (data / "mid").exists()
    assert not (data / ".ubunye").exists()
