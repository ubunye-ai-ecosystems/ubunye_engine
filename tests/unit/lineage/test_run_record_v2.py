"""Run record v2: the receipt carries the code, the machine, the inputs, the steps.

A record could show that two runs wrote different data but not why: the
transform, the input or the environment could each have changed. v2 records a
hash of the task's code, the environment (Python, platform, package versions),
every input's row hash, per-step timings, and every expectation's result.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ubunye.lineage import evidence
from ubunye.lineage.context import RECORD_VERSION, RunContext
from ubunye.telemetry.hooks.monitors import _evidence

# --- the code hash ------------------------------------------------------------


def _code(root: Path, body: str, newline: str = "\n") -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "transformations.py").write_bytes(body.replace("\n", newline).encode("utf-8"))
    (root / "helpers.py").write_text("X = 1\n", encoding="utf-8")
    return root


def test_the_code_hash_ignores_line_endings(tmp_path):
    lf = _code(tmp_path / "lf", "def f():\n    return 1\n")
    crlf = _code(tmp_path / "crlf", "def f():\n    return 1\n", newline="\r\n")
    assert evidence.code_hash(str(lf)) == evidence.code_hash(str(crlf))


def test_the_code_hash_changes_with_any_python_file(tmp_path):
    task = _code(tmp_path / "t", "def f():\n    return 1\n")
    before = evidence.code_hash(str(task))
    (task / "helpers.py").write_text("X = 2\n", encoding="utf-8")
    assert evidence.code_hash(str(task)) != before


def test_the_code_hash_skips_caches_and_hidden_folders(tmp_path):
    task = _code(tmp_path / "t", "def f():\n    return 1\n")
    before = evidence.code_hash(str(task))
    (task / "__pycache__").mkdir()
    (task / "__pycache__" / "junk.py").write_text("junk", encoding="utf-8")
    (task / ".venv").mkdir()
    (task / ".venv" / "site.py").write_text("junk", encoding="utf-8")
    assert evidence.code_hash(str(task)) == before


def test_no_folder_no_code_hash(tmp_path):
    assert evidence.code_hash(None) is None
    assert evidence.code_hash(str(tmp_path / "missing")) is None


def test_the_environment_names_python_and_the_engine():
    env = evidence.environment()
    assert env["python"] and env["platform"]
    assert "ubunye-engine" in env["packages"]
    assert evidence.environment_hash(env) == evidence.environment_hash(json.loads(json.dumps(env)))


# --- reading older records ------------------------------------------------------


def test_a_v1_record_still_loads_and_says_it_is_v1():
    old = {
        "run_id": "r",
        "task_path": "u/p/t",
        "usecase": "u",
        "package": "p",
        "task_name": "t",
        "started_at": "2026-07-15T00:00:00",
    }
    ctx = RunContext.from_dict(old)
    assert ctx.record_version == 1
    assert ctx.code_hash is None and ctx.timings == [] and ctx.expectations == []
    assert RunContext.from_dict(ctx.to_dict()).record_version == 1


# --- the monitor bridge: older monitors are not handed what they cannot take ----


def test_an_older_monitor_gets_no_new_arguments():
    class Old:
        def task_end(self, *, context, config, outputs, status, duration_sec):
            pass

    class New:
        def task_end(self, *, context, config, outputs, status, duration_sec, timings=None):
            pass

    class Any_:
        def task_end(self, **kwargs):
            pass

    state = {"inputs": {"a": 1}, "expectations": [], "timings": [{"step": "x"}]}
    assert _evidence(Old(), state) == {}
    assert _evidence(New(), state) == {"timings": [{"step": "x"}]}
    assert set(_evidence(Any_(), state)) == {"inputs", "expectations", "timings", "llm_calls"}


# --- end to end on pandas -----------------------------------------------------------

pd = pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

import ubunye  # noqa: E402

TRANSFORM = """\
from ubunye.core.interfaces import Task


class Copy(Task):
    def transform(self, sources):
        return {"out": sources["src"]}
"""


def _task(root: Path, expectations: str = "") -> Path:
    task = root / "uc" / "pkg" / "t"
    task.mkdir(parents=True)
    (root / "in.csv").write_text("id,qty\n1,2\n2,0\n3,5\n", encoding="utf-8")
    (task / "transformations.py").write_text(TRANSFORM, encoding="utf-8")
    (task / "config.yaml").write_text(
        f"""\
CONFIG:
  inputs:
    src:
      format: s3
      path: "{(root / 'in.csv').as_posix()}"
      file_format: csv
      options:
        header: "true"
        inferSchema: "true"
  outputs:
    out:
      format: s3
      path: "{(root / 'out').as_posix()}"
      file_format: parquet
      mode: overwrite
{expectations}""",
        encoding="utf-8",
    )
    return task


def _records(lineage: Path):
    return [
        RunContext.from_dict(json.loads(p.read_text(encoding="utf-8")))
        for p in lineage.rglob("*.json")
    ]


def test_a_run_leaves_a_v2_receipt(tmp_path):
    task = _task(tmp_path)
    lineage = tmp_path / "lineage"
    ubunye.run_task(str(task), backend="pandas", lineage=True, lineage_dir=str(lineage))
    [rec] = _records(lineage)

    assert rec.record_version == RECORD_VERSION == 2
    assert rec.code_hash == evidence.code_hash(str(task))
    assert rec.environment["packages"]["ubunye-engine"]
    assert rec.environment_hash == evidence.environment_hash(rec.environment)

    [src] = rec.inputs
    [out] = rec.outputs
    assert src.row_count == 3 and src.data_hash and src.hash_method == "rows-v1"
    # A copy task: the input and the output are the same rows.
    assert src.data_hash == out.data_hash

    steps = [t["step"] for t in rec.timings]
    assert steps == ["Reader:s3", "Transform:__ubunye_user_task__", "Writer:s3"] or (
        steps[0] == "Reader:s3" and steps[-1] == "Writer:s3" and len(steps) == 3
    )
    assert all(t["seconds"] >= 0 for t in rec.timings)
    assert rec.timings[0]["input"] == "src" and rec.timings[-1]["output"] == "out"


def test_a_failed_expectation_is_in_the_record_of_the_failed_run(tmp_path):
    task = _task(
        tmp_path,
        """\
  expectations:
    out:
      rules:
        - between: {column: qty, min: 1}
""",
    )
    lineage = tmp_path / "lineage"
    with pytest.raises(Exception, match="Expectations failed"):
        ubunye.run_task(str(task), backend="pandas", lineage=True, lineage_dir=str(lineage))
    [rec] = _records(lineage)
    assert rec.status == "error"
    assert rec.expectations == [
        {
            "output": "out",
            "rule": "qty_between",
            "kind": "between",
            "severity": "fail",
            "column": "qty",
            "failed": 1,
            "total": 3,
            "passed": False,
        }
    ]
    # The read and the transform ran and were timed; nothing was written.
    assert [t["step"].split(":")[0] for t in rec.timings] == ["Reader", "Transform"]


def test_compare_says_the_code_changed_and_the_data_did_not(tmp_path):
    """Two runs, same input, a comment added to the transform: only the code moved."""
    from ubunye.cli.lineage import compare_records

    task = _task(tmp_path)
    lineage = tmp_path / "lineage"
    ubunye.run_task(str(task), backend="pandas", lineage=True, lineage_dir=str(lineage))
    source = task / "transformations.py"
    source.write_text(source.read_text(encoding="utf-8") + "# reviewed\n", encoding="utf-8")
    ubunye.run_task(str(task), backend="pandas", lineage=True, lineage_dir=str(lineage))

    a, b = sorted(_records(lineage), key=lambda r: r.started_at)
    report = compare_records(a, b)
    assert report["code_hash"]["changed"] is True
    assert report["config_hash"]["changed"] is False
    assert report["environment_hash"]["changed"] is False
    assert report["environment_changes"] == {}
    assert report["inputs"]["src"]["data_hash"]["state"] == "unchanged"
    assert report["outputs"]["out"]["data_hash"]["state"] == "unchanged"
