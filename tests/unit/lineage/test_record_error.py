"""A failed run's record says why it failed (F-054).

The record has an ``error`` field, OpenLineage sends it as the FAIL event's message,
and ``ubunye prove`` shows it as the reason a run failed. Nothing filled it: a failed
run's record said ``status: error`` and ``error: null``.
"""

from __future__ import annotations

import json

import pytest

pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

import ubunye  # noqa: E402
from ubunye.lineage.storage import FileSystemLineageStore  # noqa: E402

CONFIG = """\
MODEL: etl
VERSION: "1.0.0"
CONFIG:
  inputs:
    rows: {format: s3, path: "{{ task_dir }}/rows.csv", file_format: csv, options: {header: "true"}}
  transform: {}
  outputs:
    out: {format: s3, path: "{{ task_dir }}/out", file_format: parquet, mode: overwrite}
"""

CODE = """\
from ubunye.core.interfaces import Task


class Boom(Task):
    def transform(self, sources):
        raise ValueError("no price for order 42 (token " + self.config["token"] + ")")
"""


def _task(tmp_path):
    task = tmp_path / "uc" / "pkg" / "t"
    task.mkdir(parents=True)
    (task / "config.yaml").write_text(CONFIG, encoding="utf-8")
    (task / "transformations.py").write_text(CODE, encoding="utf-8")
    (task / "rows.csv").write_text("a\n1\n", encoding="utf-8")
    return task


def test_a_failed_run_records_the_error_and_scrubs_secrets(tmp_path):
    task = _task(tmp_path)
    # The transform puts a secret variable's value in its message; the record must not.
    code = (task / "transformations.py").read_text(encoding="utf-8")
    code = code.replace('self.config["token"]', '"hunter22-secret"')
    (task / "transformations.py").write_text(code, encoding="utf-8")

    with pytest.raises(ValueError):
        ubunye.run_task(
            str(task), backend="pandas", lineage=True, variables={"api_token": "hunter22-secret"}
        )

    store = FileSystemLineageStore(str(tmp_path / ".ubunye" / "lineage"))
    (record,) = store.list_runs("uc/pkg/t")
    assert record.status == "error"
    assert record.error is not None
    assert record.error.startswith("ValueError: no price for order 42")
    assert "hunter22-secret" not in record.error

    # `lineage trace` shows it, as text and as JSON.
    from typer.testing import CliRunner

    from ubunye.cli.main import app

    where = ["lineage", "trace", "-d", str(tmp_path), "-u", "uc", "-p", "pkg", "-t", "t"]
    text = CliRunner().invoke(app, where)
    assert text.exit_code == 0 and "Error:   ValueError: no price for order 42" in text.output
    as_json = CliRunner().invoke(app, [*where, "--json"])
    assert json.loads(as_json.output)["error"] == record.error


def test_a_successful_run_has_no_error(tmp_path):
    task = _task(tmp_path)
    (task / "transformations.py").write_text(
        "from ubunye.core.interfaces import Task\n\n\n"
        "class Ok(Task):\n    def transform(self, sources):\n"
        '        return {"out": sources["rows"]}\n',
        encoding="utf-8",
    )
    ubunye.run_task(str(task), backend="pandas", lineage=True)
    store = FileSystemLineageStore(str(tmp_path / ".ubunye" / "lineage"))
    (record,) = store.list_runs("uc/pkg/t")
    assert record.status == "success" and record.error is None


def test_a_huge_error_is_cut_to_about_4_kb_in_the_record_and_openlineage(tmp_path):
    """A 5 MB message made a 5 MB record (the skeptic's p1 'size' case)."""
    from ubunye.core.secrets import MAX_ERROR_CHARS
    from ubunye.lineage.openlineage import events

    task = _task(tmp_path)
    (task / "transformations.py").write_text(
        "from ubunye.core.interfaces import Task\n\n\n"
        "class Big(Task):\n    def transform(self, sources):\n"
        '        raise RuntimeError("x" * 5_000_000)\n',
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError):
        ubunye.run_task(str(task), backend="pandas", lineage=True)

    store = FileSystemLineageStore(str(tmp_path / ".ubunye" / "lineage"))
    (record,) = store.list_runs("uc/pkg/t")
    head = "RuntimeError: "
    full = len(head) + 5_000_000
    assert record.error == (
        head
        + "x" * (MAX_ERROR_CHARS - len(head))
        + f" ... ({full - MAX_ERROR_CHARS} more characters)"
    )
    (path,) = (tmp_path / ".ubunye" / "lineage").rglob("*.json")
    assert path.stat().st_size < 64_000
    fail = events(record)[-1]
    assert fail["run"]["facets"]["errorMessage"]["message"] == record.error


def test_the_cut_never_shows_half_a_secret():
    from ubunye.core.secrets import MAX_ERROR_CHARS, error_text

    secret = "HalfShown-Secret-987"
    filler = "y" * (MAX_ERROR_CHARS - len("ValueError: ") - 5)
    text = error_text(ValueError(filler + secret + " tail"), {"db_password": secret})
    assert "Half" not in text and text.endswith("more characters)")
