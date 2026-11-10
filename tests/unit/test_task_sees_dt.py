"""A Task sees the run's batch variables: self.dt and self.variables (backfills)."""

from __future__ import annotations

from pathlib import Path

import ubunye

TASK = """
from ubunye.core.interfaces import Task


class Clean(Task):
    def transform(self, sources):
        df = sources["people"]
        return {"people_out": df.assign(batch=str(self.dt), extra=self.variables.get("region", ""))}
"""


def _task(tmp_path: Path) -> Path:
    from typer.testing import CliRunner

    from ubunye.cli.main import app

    CliRunner().invoke(
        app, ["init", "-d", str(tmp_path), "-u", "demo", "-p", "sales", "-t", "clean"]
    )
    task = tmp_path / "demo" / "sales" / "clean"
    (task / "transformations.py").write_text(TASK)
    return task


def _out_name(task: Path) -> str:
    import yaml

    return next(iter(yaml.safe_load((task / "config.yaml").read_text())["CONFIG"]["outputs"]))


def test_run_task_gives_the_task_dt_and_variables(tmp_path):
    task = _task(tmp_path)
    TASK_OUT = _out_name(task)
    (task / "transformations.py").write_text(TASK.replace("people_out", TASK_OUT))
    out = ubunye.run_task(str(task), backend="pandas", dt="2024-03-31", variables={"region": "GP"})
    frame = out[TASK_OUT]
    assert set(frame["batch"]) == {"2024-03-31"}
    assert set(frame["extra"]) == {"GP"}


def test_the_notebook_gives_the_task_dt(tmp_path):
    task = _task(tmp_path)
    TASK_OUT = _out_name(task)
    (task / "transformations.py").write_text(TASK.replace("people_out", TASK_OUT))
    with ubunye.notebook(
        str(task), backend="pandas", dt="2024-03-31", lineage=False, quiet=True
    ) as run:
        outputs = run.transform(run.read())
    assert set(outputs[TASK_OUT]["batch"]) == {"2024-03-31"}


def test_a_run_without_dt_gives_none():
    from ubunye.core.interfaces import Task

    class T(Task):
        def transform(self, sources):
            return {}

    assert T(config={}).dt is None
