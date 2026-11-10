"""The notebook as one run (ADR 016): ``with ubunye.notebook(task_dir, dt=dt) as run:``.

Every failure leaves a run record that says which stage failed (setup, read,
transform, expectations, write, or a step the notebook named), a success leaves
one too, and run records are on as soon as a location is configured: explicit
argument, then the job's ``ubunye_run_records`` parameter, then
``$UBUNYE_RUN_RECORDS``. End to end on the pandas backend with the filesystem
store, so no Spark and no Java.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List

import pytest

pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

import ubunye  # noqa: E402
from ubunye.core.errors import ExpectationError  # noqa: E402

COPY = """\
from ubunye.core.interfaces import Task


class Copy(Task):
    def transform(self, sources):
        return {"out": sources["src"]}
"""

BROKEN_TRANSFORM = """\
from ubunye.core.interfaces import Task


class Broken(Task):
    def transform(self, sources):
        raise RuntimeError("the model file is missing")
"""

BROKEN_SETUP = """\
from ubunye.core.interfaces import Task


class Broken(Task):
    def setup(self):
        raise RuntimeError("cannot reach the feature store")

    def transform(self, sources):
        return {"out": sources["src"]}
"""


def _task(
    root: Path,
    code: str = COPY,
    *,
    source: str = "in.csv",
    out: str = "out",
    expect: str = "",
) -> Path:
    task = root / "uc" / "pkg" / "copy"
    task.mkdir(parents=True)
    (root / "in.csv").write_text("id,city\n1,jhb\n2,cpt\n3,pta\n", encoding="utf-8")
    (task / "transformations.py").write_text(code, encoding="utf-8")
    (task / "config.yaml").write_text(
        f"""\
MODEL: etl
VERSION: "1.0.0"
CONFIG:
  inputs:
    src:
      format: s3
      path: "{(root / source).as_posix()}"
      file_format: csv
      options:
        header: "true"
        inferSchema: "true"
  transform: {{}}
  outputs:
    out:
      format: s3
      path: "{(root / out).as_posix()}"
      file_format: parquet
      mode: overwrite
{expect}""",
        encoding="utf-8",
    )
    return task


def _records(where: Path) -> List[Dict[str, Any]]:
    return [json.loads(p.read_text(encoding="utf-8")) for p in where.rglob("*.json")]


@pytest.fixture(autouse=True)
def _no_job_settings(monkeypatch):
    for name in ("UBUNYE_RUN_RECORDS", "UBUNYE_RUN_RECORDS_FALLBACK", "UBUNYE_LINEAGE_BACKEND"):
        monkeypatch.delenv(name, raising=False)


# ---------------------------------------------------------------------------
# Every stage that fails leaves a record that names it
# ---------------------------------------------------------------------------


class TestEveryFailureIsRecorded:
    def test_a_failed_read_leaves_a_record_that_says_read(self, tmp_path):
        task = _task(tmp_path, source="missing.csv")
        runs = tmp_path / "runs"
        with pytest.raises(Exception):
            with ubunye.notebook(str(task), backend="pandas", run_records=str(runs)) as run:
                run.read()
        (record,) = _records(runs)
        assert record["status"] == "error"
        assert record["failed_stage"] == "read"
        assert "missing.csv" in record["error"]
        assert record["task_path"] == "uc/pkg/copy"

    def test_a_failed_transform_says_transform(self, tmp_path):
        runs = tmp_path / "runs"
        task = _task(tmp_path, BROKEN_TRANSFORM)
        with pytest.raises(RuntimeError, match="model file"):
            with ubunye.notebook(str(task), backend="pandas", run_records=str(runs)) as run:
                run.transform(run.read())
        (record,) = _records(runs)
        assert (record["status"], record["failed_stage"]) == ("error", "transform")
        assert "the model file is missing" in record["error"]

    def test_a_failed_expectation_says_expectations(self, tmp_path):
        expect = """\
  expectations:
    out:
      rules:
        - row_count: {min: 100}
"""
        runs = tmp_path / "runs"
        task = _task(tmp_path, expect=expect)
        with pytest.raises(ExpectationError):
            with ubunye.notebook(str(task), backend="pandas", run_records=str(runs)) as run:
                run.write(run.transform(run.read()))
        (record,) = _records(runs)
        assert (record["status"], record["failed_stage"]) == ("error", "expectations")
        assert not (tmp_path / "out").exists()

    def test_a_failed_write_says_write(self, tmp_path):
        (tmp_path / "blocker").write_text("a file where a folder must go", encoding="utf-8")
        runs = tmp_path / "runs"
        task = _task(tmp_path, out="blocker/out")
        with pytest.raises(Exception):
            with ubunye.notebook(str(task), backend="pandas", run_records=str(runs)) as run:
                run.write(run.transform(run.read()))
        (record,) = _records(runs)
        assert (record["status"], record["failed_stage"]) == ("error", "write")

    def test_a_failed_setup_says_setup(self, tmp_path):
        runs = tmp_path / "runs"
        task = _task(tmp_path, BROKEN_SETUP)
        with pytest.raises(RuntimeError, match="feature store"):
            with ubunye.notebook(str(task), backend="pandas", run_records=str(runs)):
                pass  # pragma: no cover
        (record,) = _records(runs)
        assert (record["status"], record["failed_stage"]) == ("error", "setup")
        assert "cannot reach the feature store" in record["error"]
        assert record["task_path"] == "uc/pkg/copy"

    def test_a_named_step_that_fails_is_named(self, tmp_path):
        runs = tmp_path / "runs"
        task = _task(tmp_path)
        with pytest.raises(ValueError, match="no scores"):
            with ubunye.notebook(str(task), backend="pandas", run_records=str(runs)) as run:
                sources = run.read()
                with run.step("score"):
                    raise ValueError("no scores")
                run.write(run.transform(sources))  # pragma: no cover
        (record,) = _records(runs)
        assert (record["status"], record["failed_stage"]) == ("error", "score")

    def test_code_outside_any_stage_is_the_notebook(self, tmp_path):
        runs = tmp_path / "runs"
        task = _task(tmp_path)
        with pytest.raises(KeyError):
            with ubunye.notebook(str(task), backend="pandas", run_records=str(runs)) as run:
                run.read()["no_such_input"]
        (record,) = _records(runs)
        assert (record["status"], record["failed_stage"]) == ("error", "notebook")


# ---------------------------------------------------------------------------
# A success is one record, with the inputs and outputs and a line per step
# ---------------------------------------------------------------------------


class TestTheThreeStepNotebook:
    def test_three_steps_leave_one_success_record(self, tmp_path, capsys):
        runs = tmp_path / "runs"
        task = _task(tmp_path)
        with ubunye.notebook(
            str(task), dt="202611", backend="pandas", run_records=str(runs)
        ) as run:
            sources = run.read()
            outputs = run.transform(sources)
            run.write(outputs)
        (record,) = _records(runs)
        assert record["status"] == "success" and record["failed_stage"] is None
        assert record["variables"]["dt"] == "202611"
        assert [s["name"] for s in record["inputs"]] == ["src"]
        assert record["inputs"][0]["data_hash"]
        assert record["outputs"][0]["hash_method"] == "rows-v1"
        steps = [t["step"].split(":")[0] for t in record["timings"]]
        assert steps == ["Reader", "Transform", "Writer"]
        printed = capsys.readouterr().out.splitlines()
        lines = [line for line in printed if line.startswith("ubunye:")]
        assert [line.split()[1] for line in lines] == ["read", "transform", "write", "run"]
        assert "success" in lines[-1] and str(runs) in lines[-1]

    def test_quiet_prints_nothing(self, tmp_path, capsys):
        task = _task(tmp_path)
        with ubunye.notebook(str(task), backend="pandas", quiet=True) as run:
            run.write(run.transform(run.read()))
        assert "ubunye:" not in capsys.readouterr().out

    def test_the_old_form_still_works_and_records_at_write(self, tmp_path):
        lineage = tmp_path / "lineage"
        task = _task(tmp_path)
        nb = ubunye.notebook(str(task), backend="pandas", lineage=True, lineage_dir=str(lineage))
        try:
            nb.write(nb.transform(nb.read()))
        finally:
            nb.close()
        (record,) = _records(lineage)
        assert record["status"] == "success"

    def test_without_a_location_nothing_is_recorded(self, tmp_path):
        task = _task(tmp_path)
        with ubunye.notebook(str(task), backend="pandas") as run:
            run.write(run.transform(run.read()))
        assert _records(tmp_path) == []
        assert (tmp_path / "out").exists()

    def test_an_empty_dt_counts_as_not_given(self, tmp_path):
        runs = tmp_path / "runs"
        task = _task(tmp_path)
        with ubunye.notebook(str(task), dt="", backend="pandas", run_records=str(runs)) as run:
            run.write(run.transform(run.read()))
        (record,) = _records(runs)
        assert record["variables"].get("dt") is None


# ---------------------------------------------------------------------------
# Where run records go: explicit > job parameter > environment
# ---------------------------------------------------------------------------


class TestTheConfiguredLocation:
    def test_the_environment_turns_records_on(self, tmp_path, monkeypatch):
        runs = tmp_path / "env_runs"
        monkeypatch.setenv("UBUNYE_RUN_RECORDS", str(runs))
        with ubunye.notebook(str(_task(tmp_path)), backend="pandas") as run:
            run.write(run.transform(run.read()))
        assert [r["status"] for r in _records(runs)] == ["success"]

    def test_a_job_parameter_beats_the_environment(self, tmp_path, monkeypatch):
        from ubunye.lineage import auto

        param, env = tmp_path / "param_runs", tmp_path / "env_runs"
        monkeypatch.setenv("UBUNYE_RUN_RECORDS", str(env))
        monkeypatch.setattr(auto, "_job_parameters", lambda: {"ubunye_run_records": str(param)})
        with ubunye.notebook(str(_task(tmp_path)), backend="pandas") as run:
            run.write(run.transform(run.read()))
        assert len(_records(param)) == 1 and _records(env) == []

    def test_an_explicit_location_beats_both(self, tmp_path, monkeypatch):
        from ubunye.lineage import auto

        mine, param = tmp_path / "mine", tmp_path / "param_runs"
        monkeypatch.setenv("UBUNYE_RUN_RECORDS", str(tmp_path / "env_runs"))
        monkeypatch.setattr(auto, "_job_parameters", lambda: {"ubunye_run_records": str(param)})
        task = _task(tmp_path)
        with ubunye.notebook(str(task), backend="pandas", run_records=str(mine)) as run:
            run.write(run.transform(run.read()))
        assert len(_records(mine)) == 1 and _records(param) == []

    def test_lineage_false_switches_records_off(self, tmp_path, monkeypatch):
        monkeypatch.setenv("UBUNYE_RUN_RECORDS", str(tmp_path / "env_runs"))
        with ubunye.notebook(str(_task(tmp_path)), backend="pandas", lineage=False) as run:
            run.write(run.transform(run.read()))
        assert _records(tmp_path) == []

    @pytest.mark.parametrize(
        "where, backend",
        [
            ("main.ops.ubunye_runs", "delta"),
            ("ubunye_runs", "delta"),
            ("/Volumes/main/ops/runs", "filesystem"),
            ("./runs", "filesystem"),
            ("C:\\runs", "filesystem"),
        ],
    )
    def test_a_table_name_goes_to_delta_and_a_path_to_files(self, where, backend):
        from ubunye.lineage import auto

        choice = auto.choose(Path("/uc"), run_records=where)
        assert choice is not None and choice.backend == backend
        if backend == "delta":
            assert choice.location == where

    def test_a_relative_path_is_under_the_use_case_folder(self):
        from ubunye.lineage import auto

        choice = auto.choose(Path("/uc"), run_records="./runs")
        assert choice is not None and Path(choice.location) == Path("/uc/runs")

    def test_a_named_backend_is_kept(self, monkeypatch):
        from ubunye.lineage import auto

        monkeypatch.setenv("UBUNYE_LINEAGE_BACKEND", "delta")
        choice = auto.choose(Path("/uc"), run_records="s3://bucket/runs")
        assert choice is not None and choice.backend == "delta"

    def test_run_task_reads_the_same_settings(self, tmp_path, monkeypatch):
        runs = tmp_path / "env_runs"
        monkeypatch.setenv("UBUNYE_RUN_RECORDS", str(runs))
        ubunye.run_task(str(_task(tmp_path)), backend="pandas")
        assert [r["status"] for r in _records(runs)] == ["success"]

    def test_run_task_names_the_failed_stage(self, tmp_path):
        runs = tmp_path / "runs"
        task = _task(tmp_path, BROKEN_TRANSFORM)
        with pytest.raises(RuntimeError):
            ubunye.run_task(str(task), backend="pandas", run_records=str(runs))
        (record,) = _records(runs)
        assert (record["status"], record["failed_stage"]) == ("error", "transform")


# ---------------------------------------------------------------------------
# A store that cannot save never fails the job
# ---------------------------------------------------------------------------


class _Refusing:
    def __init__(self, where: str) -> None:
        self.where = where

    def save(self, ctx: Any) -> None:
        raise PermissionError("User does not have CREATE TABLE on Schema 'main.ops'")

    load = list_runs = search = save


@pytest.fixture
def refusing_store(monkeypatch):
    from ubunye.lineage import stores

    real = stores.load_class
    monkeypatch.setattr(
        stores, "load_class", lambda name: _Refusing if name == "refusing" else real(name)
    )


class TestTheFallback:
    def test_the_fallback_goes_to_the_configured_volume(
        self, tmp_path, monkeypatch, refusing_store
    ):
        volume = tmp_path / "volume"
        monkeypatch.setenv("UBUNYE_LINEAGE_BACKEND", "refusing")
        monkeypatch.setenv("UBUNYE_RUN_RECORDS_FALLBACK", str(volume))
        task = _task(tmp_path)
        with ubunye.notebook(str(task), backend="pandas", run_records="main.ops.runs") as run:
            run.write(run.transform(run.read()))
        (manifest,) = volume.rglob("lineage.jsonl")
        docs = [json.loads(line) for line in manifest.read_text(encoding="utf-8").splitlines()]
        assert docs[-1]["status"] == "success"
        assert (tmp_path / "out").exists()

    def test_without_a_volume_it_only_warns(self, tmp_path, monkeypatch, refusing_store, caplog):
        from ubunye._internal import metadata_worker

        home = tmp_path / "home_fallback"
        monkeypatch.setattr(metadata_worker, "_FALLBACK_ROOT", home)
        monkeypatch.setenv("UBUNYE_LINEAGE_BACKEND", "refusing")
        task = _task(tmp_path)
        with caplog.at_level(logging.WARNING):
            with ubunye.notebook(str(task), backend="pandas", run_records="main.ops.runs") as run:
                run.write(run.transform(run.read()))
        assert not home.exists()
        assert "CREATE TABLE" in caplog.text and "ubunye_run_records_fallback" in caplog.text
        assert (tmp_path / "out").exists()


class TestTheDeltaTableMessage:
    def test_a_table_that_cannot_be_created_says_what_to_do(self, monkeypatch):
        pytest.importorskip("pyspark")
        from ubunye.lineage import delta_store
        from ubunye.lineage.delta_store import DeltaLineageBackend, RunRecordTableError

        store = DeltaLineageBackend("main.ops.ubunye_runs")
        monkeypatch.setattr(store, "_session", lambda: object())
        monkeypatch.setattr(store, "_exists", lambda spark: False)

        def refuse(spark: Any) -> None:
            raise RuntimeError("[PERMISSION_DENIED] User does not have CREATE TABLE")

        monkeypatch.setattr(store, "_create_table", refuse)
        with pytest.raises(RunRecordTableError) as err:
            store.save(delta_store.RunContext.from_dict(_minimal()))
        text = str(err.value) + str(getattr(err.value, "hint", ""))
        assert "main.ops.ubunye_runs" in text
        assert "CREATE TABLE" in text and "create it once" in text.lower()


def _minimal() -> Dict[str, Any]:
    return {
        "run_id": "r1",
        "task_path": "uc/pkg/t",
        "usecase": "uc",
        "package": "pkg",
        "task_name": "t",
        "started_at": "2026-11-10T00:00:00+00:00",
    }
