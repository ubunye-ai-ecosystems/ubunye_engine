"""`ubunye prove observe -t a -t b` observes every task, not only the last (F-056).

`run` and `deploy` take several `-t`; `prove observe` took one, and click kept the last
of several quietly: one observation, of the last task, under the name meant for all.
Now several tasks give one observation each, named `<workload>-<task>`, as
ubunye-infra's proving/observe.sh names them.
"""

from __future__ import annotations

import json

import pytest
from typer.testing import CliRunner

pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

import ubunye  # noqa: E402
from ubunye.cli.main import app  # noqa: E402

CONFIG = """\
MODEL: etl
VERSION: "1.0.0"
CONFIG:
  inputs:
    rows: {format: s3, path: "{{ task_dir }}/../rows.csv", file_format: csv, options: {header: "true"}}
  transform: {}
  outputs:
    out: {format: s3, path: "{{ task_dir }}/out", file_format: parquet, mode: overwrite}
"""

CODE = """\
from ubunye.core.interfaces import Task


class Copy(Task):
    def transform(self, sources):
        return {"out": sources["rows"]}
"""


@pytest.fixture
def two_tasks(tmp_path):
    pkg = tmp_path / "uc" / "pkg"
    for name in ("a", "b"):
        task = pkg / name
        task.mkdir(parents=True)
        (task / "config.yaml").write_text(CONFIG, encoding="utf-8")
        (task / "transformations.py").write_text(CODE, encoding="utf-8")
    (pkg / "rows.csv").write_text("x\n1\n", encoding="utf-8")
    for name in ("a", "b"):
        ubunye.run_task(str(pkg / name), backend="pandas", lineage=True)
    return tmp_path


def _observe(root, *tasks):
    where = ["-d", str(root), "-u", "uc", "-p", "pkg"]
    for t in tasks:
        where += ["-t", t]
    evidence = root / "evidence"
    args = ["prove", "observe", "--workload", "w", "--env", "pandas-local", *where]
    return CliRunner().invoke(app, [*args, "-o", str(evidence)]), evidence


def test_several_tasks_give_one_observation_each(two_tasks):
    result, evidence = _observe(two_tasks, "a", "b")
    assert result.exit_code == 0, result.output
    found = sorted(p.parent.name for p in evidence.rglob("pandas-local.json"))
    assert found == ["w-a", "w-b"]
    for name in ("a", "b"):
        doc = json.loads((evidence / f"w-{name}" / "pandas-local.json").read_text())
        assert json.dumps(doc).count(f"uc/pkg/{name}") >= 1
    assert "observed w-a" in result.output and "observed w-b" in result.output


def test_one_task_keeps_the_workload_name(two_tasks):
    result, evidence = _observe(two_tasks, "b")
    assert result.exit_code == 0, result.output
    assert [p.parent.name for p in evidence.rglob("pandas-local.json")] == ["w"]


def test_a_repeated_task_is_refused(two_tasks):
    result, evidence = _observe(two_tasks, "a", "a")
    assert result.exit_code != 0
    assert "more than once" in result.output
    assert not evidence.exists()
