"""The run record (ADR 006): correct, complete, and in one place.

End to end on the pandas backend, so no Spark and no Java: a run with lineage
leaves a record whose data hash is the hash of the files it wrote, found in the
same store under the same identity whether the run came from the CLI, run_task,
run_pipeline or a notebook.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

import ubunye  # noqa: E402
from ubunye.backends.pandas_backend import PandasBackend  # noqa: E402
from ubunye.cli.main import app  # noqa: E402
from ubunye.lineage.content_hash import fingerprint  # noqa: E402
from ubunye.lineage.context import StepRecord  # noqa: E402
from ubunye.lineage.storage import FileSystemLineageStore  # noqa: E402

runner = CliRunner()

TRANSFORM = """\
from ubunye.core.interfaces import Task


class Copy(Task):
    def transform(self, sources):
        return {"out": sources["src"]}
"""


def _task(root: Path) -> Path:
    task = root / "uc" / "pkg" / "copy"
    task.mkdir(parents=True)
    (root / "in.csv").write_text("id,city\n1,jhb\n2,cpt\n3,pta\n", encoding="utf-8")
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
        inferSchema: "true"
  transform: {{}}
  outputs:
    out:
      format: s3
      path: "{(root / "out").as_posix()}"
      file_format: parquet
      mode: overwrite
""",
        encoding="utf-8",
    )
    return task


def _records(root: Path):
    return FileSystemLineageStore(str(root / ".ubunye" / "lineage")).list_runs("uc/pkg/copy", n=10)


def _written_hash(root: Path) -> str:
    return fingerprint(PandasBackend().read_frame("parquet", str(root / "out"))).data_hash


class TestTheRecord:
    def test_the_data_hash_is_the_hash_of_what_was_written(self, tmp_path):
        ubunye.run_task(str(_task(tmp_path)), backend="pandas", lineage=True, dt="2024-01-02")
        (record,) = _records(tmp_path)
        (out,) = record.outputs
        assert out.hash_method == "rows-v1" and out.hash_error is None
        assert out.row_count == 3
        assert out.data_hash == _written_hash(tmp_path)

    def test_it_says_which_engine_version_backend_and_variables(self, tmp_path):
        ubunye.run_task(str(_task(tmp_path)), backend="pandas", lineage=True, dt="2024-01-02")
        (record,) = _records(tmp_path)
        assert record.engine_version and record.engine_version != "unknown"
        assert record.backend == "pandas"
        assert record.variables == {"dt": "2024-01-02", "mode": "DEV"}

    def test_the_same_data_gives_the_same_hash_run_after_run(self, tmp_path):
        task = _task(tmp_path)
        ubunye.run_task(str(task), backend="pandas", lineage=True)
        ubunye.run_task(str(task), backend="pandas", lineage=True)
        first, second = _records(tmp_path)
        assert first.outputs[0].data_hash == second.outputs[0].data_hash

    def test_old_records_still_load(self):
        step = StepRecord.from_dict(
            {
                "name": "o",
                "direction": "output",
                "format": "s3",
                "location": "/x",
                "data_hash": "sha256:old",
            }
        )
        assert step.hash_method is None and step.hash_error is None


class TestOnePlace:
    def test_cli_and_api_record_the_same_task_the_same_way(self, tmp_path):
        task = _task(tmp_path)
        result = runner.invoke(
            app,
            [
                "run",
                "-d",
                str(tmp_path),
                "-u",
                "uc",
                "-p",
                "pkg",
                "-t",
                "copy",
                "--backend",
                "pandas",
                "--lineage",
            ],
        )
        assert result.exit_code == 0, result.output
        ubunye.run_task(str(task), backend="pandas", lineage=True)
        ubunye.run_pipeline(str(tmp_path), "uc", "pkg", ["copy"], backend="pandas", lineage=True)

        records = _records(tmp_path)
        assert len(records) == 3  # all three found by the CLI's own lookup
        assert {r.task_path for r in records} == {"uc/pkg/copy"}
        assert len({r.outputs[0].data_hash for r in records}) == 1

    def test_a_notebook_run_is_recorded_in_the_same_place(self, tmp_path):
        task = _task(tmp_path)
        ubunye.run_task(str(task), backend="pandas", lineage=True)
        nb = ubunye.notebook(str(task), backend="pandas", lineage=True)
        try:
            nb.run()
        finally:
            nb.close()
        api_run, notebook_run = sorted(_records(tmp_path), key=lambda r: r.started_at)
        assert notebook_run.status == "success" and notebook_run.backend == "pandas"
        assert notebook_run.outputs[0].data_hash == api_run.outputs[0].data_hash

    def test_a_step_by_step_notebook_write_is_recorded_once(self, tmp_path):
        nb = ubunye.notebook(str(_task(tmp_path)), backend="pandas", lineage=True)
        try:
            nb.write(nb.transform(nb.read()))
        finally:
            nb.close()
        (record,) = _records(tmp_path)
        assert record.outputs[0].hash_method == "rows-v1"

    def test_the_cli_lists_a_run_made_from_python(self, tmp_path):
        ubunye.run_task(str(_task(tmp_path)), backend="pandas", lineage=True)
        result = runner.invoke(
            app, ["lineage", "list", "-d", str(tmp_path), "-u", "uc", "-p", "pkg", "-t", "copy"]
        )
        assert result.exit_code == 0 and "success" in result.output


class TestHonestFailure:
    def test_a_frame_that_cannot_be_read_records_why(self, tmp_path):
        from ubunye.lineage.recorder import LineageRecorder

        class Broken:
            schema = {"id": "int64"}

            def collect(self):
                raise RuntimeError("cluster went away")

        class Ctx:
            run_id, task_name, profile, backend, variables = "r1", "uc/pkg/copy", "DEV", "x", {}

        recorder = LineageRecorder(base_dir=str(tmp_path))
        cfg = {"CONFIG": {"outputs": {"out": {"format": "s3", "path": "/x"}}}}
        recorder.task_start(context=Ctx(), config=cfg)
        recorder.task_end(
            context=Ctx(), config=cfg, outputs={"out": Broken()}, status="success", duration_sec=1
        )
        (record,) = FileSystemLineageStore(str(tmp_path)).list_runs("uc/pkg/copy")
        (out,) = record.outputs
        assert out.data_hash is None and out.row_count is None
        assert "cluster went away" in out.hash_error
        assert out.schema_hash != out.data_hash  # the old bug: schema hash posing as data


class TestCompare:
    def _compare(self, tmp_path, a, b):
        store = FileSystemLineageStore(str(tmp_path / ".ubunye" / "lineage"))
        from ubunye.lineage.context import RunContext

        for run_id, step in (("a1", a), ("b2", b)):
            ctx = RunContext(
                run_id=run_id,
                task_path="uc/pkg/copy",
                usecase="uc",
                package="pkg",
                task_name="copy",
                profile="DEV",
                model="etl",
                version="1",
                config_hash="c",
                started_at="2026-01-01T00:00:00",
                status="success",
                outputs=[step],
            )
            store.save(ctx)
        return runner.invoke(
            app,
            [
                "lineage",
                "compare",
                "-d",
                str(tmp_path),
                "-u",
                "uc",
                "-p",
                "pkg",
                "-t",
                "copy",
                "--run-id1",
                "a1",
                "--run-id2",
                "b2",
            ],
        ).output

    @staticmethod
    def _step(**kw):
        return StepRecord(name="out", direction="output", format="s3", location="/x", **kw)

    def test_two_missing_hashes_are_unknown_not_unchanged(self, tmp_path):
        out = self._compare(tmp_path, self._step(), self._step(hash_error="boom"))
        assert "data_hash: unknown (boom)" in out and "unchanged" not in out.split("data_hash")[1]

    def test_a_pre_06_hash_is_not_comparable(self, tmp_path):
        out = self._compare(
            tmp_path,
            self._step(data_hash="sha256:a"),
            self._step(data_hash="sha256:b", hash_method="rows-v1"),
        )
        assert "not comparable" in out

    def test_same_method_same_hash_is_unchanged(self, tmp_path):
        step = self._step(data_hash="sha256:a", hash_method="rows-v1")
        assert "data_hash: sha256:a  (unchanged)" in self._compare(tmp_path, step, step)
