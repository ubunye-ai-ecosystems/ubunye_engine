"""Template variables: ``--var key=value`` and ``variables=``, the same everywhere.

``--var`` was documented in the README and three docs pages and never
implemented. It now works on every command that renders a config, and the
Python API takes the same variables, checked the same way.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

import ubunye  # noqa: E402
from ubunye.cli.main import app  # noqa: E402
from ubunye.config.variables import build_variables, parse_var_flags  # noqa: E402
from ubunye.lineage.storage import FileSystemLineageStore  # noqa: E402

runner = CliRunner()

TRANSFORM = """\
from ubunye.core.interfaces import Task


class Copy(Task):
    def transform(self, sources):
        return {"out": sources["src"]}
"""


def _task(root: Path) -> Path:
    """A task whose output folder is named by {{ region }} and {{ dt }}."""
    task = root / "uc" / "pkg" / "copy"
    task.mkdir(parents=True)
    (root / "in.csv").write_text("id\n1\n2\n", encoding="utf-8")
    (task / "transformations.py").write_text(TRANSFORM, encoding="utf-8")
    (task / "config.yaml").write_text(
        f"""\
MODEL: etl
VERSION: "1.0.0"
CONFIG:
  inputs:
    src:
      format: s3
      path: "{(root / "in.csv").as_posix()}"
      file_format: csv
      options:
        header: "true"
  transform: {{}}
  outputs:
    out:
      format: s3
      path: "{(root / "out").as_posix()}/{{{{ region }}}}/{{{{ dt }}}}"
      file_format: parquet
      mode: overwrite
""",
        encoding="utf-8",
    )
    return task


ARGS = ["-d", "{root}", "-u", "uc", "-p", "pkg", "-t", "copy"]


def _args(root: Path, *extra: str):
    return [a.format(root=root) for a in ARGS] + list(extra)


class TestParsing:
    def test_key_value_pairs(self):
        assert parse_var_flags(["region=gauteng", "n=3"]) == {"region": "gauteng", "n": "3"}

    def test_the_value_may_contain_equals_signs(self):
        assert parse_var_flags(["q=a=b"]) == {"q": "a=b"}

    @pytest.mark.parametrize(
        "item, message",
        [
            ("region", "key=value"),
            ("=x", "empty"),
            ("my-var=x", "not a valid template name"),
            ("1st=x", "not a valid template name"),
            ("env=x", "reserved"),
            ("task_dir=x", "reserved"),
            ("mode=PROD", "-m"),
        ],
    )
    def test_bad_flags_say_why(self, item, message):
        with pytest.raises(ValueError, match=message):
            parse_var_flags([item])


class TestMerging:
    def test_extra_variables_join_the_standard_ones(self):
        assert build_variables(dt="2024-01-02", mode="DEV", extra={"region": "gp"}) == {
            "dt": "2024-01-02",
            "dtf": None,
            "mode": "DEV",
            "region": "gp",
        }

    def test_var_dt_works_like_the_dt_flag(self):
        assert build_variables(extra={"dt": "2024-01-02"})["dt"] == "2024-01-02"

    def test_the_same_value_twice_is_fine(self):
        assert build_variables(dt="d", extra={"dt": "d"})["dt"] == "d"

    def test_two_different_values_are_refused(self):
        with pytest.raises(ValueError, match="dt"):
            build_variables(dt="2024-01-02", extra={"dt": "2025-01-01"})


class TestEveryCommand:
    def test_run(self, tmp_path):
        _task(tmp_path)
        result = runner.invoke(
            app,
            [
                "run",
                *_args(
                    tmp_path, "--backend", "pandas", "--var", "region=gp", "--var", "dt=2024-01-02"
                ),
            ],
        )
        assert result.exit_code == 0, result.output
        assert (tmp_path / "out" / "gp" / "2024-01-02" / "_SUCCESS").exists()

    @pytest.mark.parametrize("command", [["validate"], ["plan"], ["config"]])
    def test_commands_that_render_a_config(self, tmp_path, command):
        _task(tmp_path)
        ok = runner.invoke(app, [*command, *_args(tmp_path, "-dt", "d1", "--var", "region=gp")])
        assert ok.exit_code == 0, ok.output

    def test_a_missing_variable_is_still_caught(self, tmp_path):
        _task(tmp_path)
        result = runner.invoke(app, ["validate", *_args(tmp_path, "-dt", "d1")])
        assert result.exit_code == 1 and "region" in result.output

    def test_test_run(self, tmp_path):
        _task(tmp_path)
        result = runner.invoke(
            app,
            [
                "test",
                "run",
                *_args(
                    tmp_path,
                    "--backend",
                    "pandas",
                    "--no-lineage",
                    "--var",
                    "region=gp",
                    "-dt",
                    "d1",
                ),
            ],
        )
        assert result.exit_code == 0, result.output
        assert (tmp_path / "out" / "gp" / "d1" / "_SUCCESS").exists()

    def test_a_bad_flag_is_a_usage_error(self, tmp_path):
        _task(tmp_path)
        result = runner.invoke(app, ["run", *_args(tmp_path, "--var", "region")])
        assert result.exit_code == 2 and "key=value" in result.output

    def test_a_conflict_is_a_usage_error(self, tmp_path):
        _task(tmp_path)
        result = runner.invoke(
            app, ["validate", *_args(tmp_path, "-dt", "d1", "--var", "dt=d2", "--var", "region=x")]
        )
        assert result.exit_code == 2 and "dt" in result.output

    def test_the_run_record_keeps_them(self, tmp_path):
        _task(tmp_path)
        runner.invoke(
            app,
            [
                "run",
                *_args(
                    tmp_path, "--backend", "pandas", "--lineage", "-dt", "d1", "--var", "region=gp"
                ),
            ],
        )
        (record,) = FileSystemLineageStore(str(tmp_path / ".ubunye" / "lineage")).list_runs(
            "uc/pkg/copy"
        )
        assert record.variables["region"] == "gp"


class TestApi:
    def test_run_task(self, tmp_path):
        ubunye.run_task(str(_task(tmp_path)), backend="pandas", dt="d1", variables={"region": "gp"})
        assert (tmp_path / "out" / "gp" / "d1" / "_SUCCESS").exists()

    def test_run_pipeline(self, tmp_path):
        _task(tmp_path)
        ubunye.run_pipeline(
            str(tmp_path),
            "uc",
            "pkg",
            ["copy"],
            backend="pandas",
            variables={"region": "wc", "dt": "d2"},
        )
        assert (tmp_path / "out" / "wc" / "d2" / "_SUCCESS").exists()

    def test_notebook(self, tmp_path):
        nb = ubunye.notebook(
            str(_task(tmp_path)), backend="pandas", variables={"region": "kzn", "dt": "d3"}
        )
        try:
            nb.run()
        finally:
            nb.close()
        assert (tmp_path / "out" / "kzn" / "d3" / "_SUCCESS").exists()

    def test_a_conflict_is_refused(self, tmp_path):
        with pytest.raises(ValueError, match="dt"):
            ubunye.run_task(str(_task(tmp_path)), backend="pandas", dt="a", variables={"dt": "b"})
