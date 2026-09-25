"""``ubunye init``: the first command a new person types must work, and so must
what it makes.

The README said ``ubunye init -d ... -u ... -p ... -t ...`` and the command was
``ubunye init pipeline ...``, so the first command failed; and the scaffold read
a Unity Catalog table and wrote to s3a://, so nothing it made could run on a
laptop. The default scaffold now has its own sample data and a transform that
runs on pandas (no Java) and on Spark alike.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

from ubunye.backends.pandas_backend import PandasBackend  # noqa: E402
from ubunye.cli.main import app  # noqa: E402

runner = CliRunner()
WHERE = ["-d", "pipelines", "-u", "demo", "-p", "starter", "-t", "filter_adults"]
TASK = Path("pipelines/demo/starter/filter_adults")
# CI terminals get colour, and a colour code can split "--usecase" in two.
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


@pytest.fixture
def project(tmp_path, monkeypatch):
    """A fresh project folder, as the current directory (like a terminal)."""
    monkeypatch.chdir(tmp_path)
    return tmp_path


class TestTheReadmeForm:
    def test_init_without_a_subcommand_scaffolds(self, project):
        result = runner.invoke(app, ["init", *WHERE])
        assert result.exit_code == 0, result.output
        for name in ("config.yaml", "transformations.py", "data/people.csv"):
            assert (project / TASK / name).exists(), name

    def test_it_says_what_to_run_next(self, project):
        result = runner.invoke(app, ["init", *WHERE])
        assert (
            "ubunye run -d pipelines -u demo -p starter -t filter_adults --backend pandas"
            in result.output
        )

    def test_the_subcommand_form_still_works(self, project):
        result = runner.invoke(app, ["init", "pipeline", *WHERE])
        assert result.exit_code == 0 and (project / TASK / "data" / "people.csv").exists()

    def test_missing_options_are_named(self, project):
        result = runner.invoke(app, ["init", "-d", "pipelines"])
        assert result.exit_code == 2
        text = _ANSI_RE.sub("", result.output)
        assert "--usecase" in text and "--task-list" in text


class TestWhatItMakesRuns:
    def test_plan_is_clean(self, project):
        runner.invoke(app, ["init", *WHERE])
        result = runner.invoke(app, ["plan", *WHERE, "--backend", "pandas"])
        assert result.exit_code == 0, result.output

    def test_it_runs_with_no_java_and_keeps_the_adults(self, project):
        runner.invoke(app, ["init", *WHERE])
        result = runner.invoke(app, ["run", *WHERE, "--backend", "pandas", "--lineage"])
        assert result.exit_code == 0, result.output
        out = PandasBackend().read_frame("parquet", str(project / TASK / "output" / "adults"))
        ages = out.native["age"].tolist()
        assert ages and min(ages) >= 18
        assert len(ages) < 8  # the sample has minors, and they were dropped

    def test_the_config_is_local(self, project):
        runner.invoke(app, ["init", *WHERE])
        cfg = yaml.safe_load((project / TASK / "config.yaml").read_text(encoding="utf-8"))
        assert cfg["CONFIG"]["inputs"]["people"]["format"] == "s3"
        assert cfg["CONFIG"]["outputs"]["adults"]["path"] == "{{ task_dir }}/output/adults"

    def test_it_runs_from_any_folder(self, project, monkeypatch):
        runner.invoke(app, ["init", *WHERE])
        elsewhere = project / "somewhere" / "else"
        elsewhere.mkdir(parents=True)
        monkeypatch.chdir(elsewhere)
        where = ["-d", str(project / "pipelines"), *WHERE[2:]]
        result = runner.invoke(app, ["run", *where, "--backend", "pandas"])
        assert result.exit_code == 0, result.output
        assert (project / TASK / "output" / "adults" / "_SUCCESS").exists()


class TestTemplates:
    def test_databricks_is_the_old_scaffold(self, project):
        result = runner.invoke(app, ["init", *WHERE, "--template", "databricks"])
        assert result.exit_code == 0, result.output
        cfg = yaml.safe_load((project / TASK / "config.yaml").read_text(encoding="utf-8"))
        assert cfg["CONFIG"]["inputs"]["tx_data"]["format"] == "unity"
        assert not (project / TASK / "data").exists()

    def test_an_unknown_template(self, project):
        result = runner.invoke(app, ["init", *WHERE, "--template", "nope"])
        assert result.exit_code == 2 and "local" in result.output
