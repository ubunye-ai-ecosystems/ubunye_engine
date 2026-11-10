"""User hooks are added to the engine's own hooks; the run record is still written (F-101)."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import pytest

pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

import ubunye  # noqa: E402
from ubunye.core.hooks import Hook  # noqa: E402

TRANSFORM = """\
from ubunye.core.interfaces import Task


class Copy(Task):
    def transform(self, sources):
        return {"out": sources["src"]}
"""


class Seen(Hook):
    def __init__(self) -> None:
        self.tasks = 0

    @contextmanager
    def task(self, ctx, cfg, state):
        yield
        self.tasks += 1


def _task(root: Path) -> Path:
    task = root / "uc" / "pkg" / "t"
    task.mkdir(parents=True)
    (root / "in.csv").write_text("id\n1\n2\n", encoding="utf-8")
    (task / "transformations.py").write_text(TRANSFORM, encoding="utf-8")
    (task / "config.yaml").write_text(
        f"""\
VERSION: "1.0.0"
CONFIG:
  inputs:
    src: {{format: s3, path: "{(root / 'in.csv').as_posix()}", file_format: csv}}
  outputs:
    out: {{format: s3, path: "{(root / 'out').as_posix()}", file_format: parquet, mode: overwrite}}
""",
        encoding="utf-8",
    )
    return task


def _records(root: Path) -> list:
    return [p for p in (root / ".ubunye" / "lineage").rglob("*.json") if p.is_file()]


def test_run_task_with_hooks_writes_the_run_record(tmp_path):
    task, seen = _task(tmp_path), Seen()
    ubunye.run_task(str(task), backend="pandas", lineage=True, hooks=[seen])
    assert seen.tasks == 1
    assert _records(tmp_path), "run_task(hooks=...) wrote no run record"


def test_run_pipeline_with_hooks_writes_the_run_record(tmp_path):
    _task(tmp_path)
    seen = Seen()
    ubunye.run_pipeline(
        str(tmp_path), "uc", "pkg", ["t"], backend="pandas", lineage=True, hooks=[seen]
    )
    assert seen.tasks == 1
    assert _records(tmp_path), "run_pipeline(hooks=...) wrote no run record"


def test_notebook_with_hooks_writes_the_run_record(tmp_path):
    task, seen = _task(tmp_path), Seen()
    nb = ubunye.notebook(str(task), backend="pandas", lineage=True, hooks=[seen], auto_env=False)
    try:
        nb.run()
    finally:
        nb.close()
    assert seen.tasks == 1
    assert _records(tmp_path), "notebook(hooks=...) wrote no run record"
