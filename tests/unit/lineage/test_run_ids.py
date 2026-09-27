"""Every task run has its own run id, and records never mix.

Before 0.7.1 a multi-task run gave every task the same run id, and the lineage
store cached records by run id alone, so reading one task's record could hand
back another's. A registered model also never learned the id of the run that
trained it. On the pandas backend, so no Spark and no Java.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

import ubunye  # noqa: E402
from ubunye.backends.pandas_backend import PandasBackend  # noqa: E402
from ubunye.cli.main import app  # noqa: E402
from ubunye.core.runtime import Engine, EngineContext, current_run_id  # noqa: E402
from ubunye.lineage.context import RunContext  # noqa: E402
from ubunye.lineage.storage import FileSystemLineageStore  # noqa: E402
from ubunye.models.registry import ModelRegistry  # noqa: E402

runner = CliRunner()

COPY = """\
from ubunye.core.interfaces import Task


class Copy(Task):
    def transform(self, sources):
        return {"out": sources["src"]}
"""

MODEL = """\
from ubunye.models.base import UbunyeModel


class RowCount(UbunyeModel):
    def train(self, df):
        self.n = len(df)
        return {"rows": float(self.n)}

    def predict(self, df):
        return df

    def save(self, path):
        import os
        os.makedirs(path, exist_ok=True)
        with open(os.path.join(path, "n.txt"), "w") as fh:
            fh.write(str(self.n))

    @classmethod
    def load(cls, path):
        return cls()

    def metadata(self):
        return {}
"""


def _input(root: Path) -> Path:
    src = root / "in.csv"
    if not src.exists():
        src.write_text("id,city\n1,jhb\n2,cpt\n3,pta\n", encoding="utf-8")
    return src


def _copy_task(root: Path, name: str) -> Path:
    task = root / "uc" / "pkg" / name
    task.mkdir(parents=True)
    (task / "transformations.py").write_text(COPY, encoding="utf-8")
    (task / "config.yaml").write_text(
        f"""\
MODEL: etl
VERSION: "1.0.0"
CONFIG:
  inputs:
    src:
      format: s3
      path: "{_input(root).as_posix()}"
      file_format: csv
      options:
        header: "true"
  transform: {{}}
  outputs:
    out:
      format: s3
      path: "{(root / "out" / name).as_posix()}"
      file_format: parquet
      mode: overwrite
""",
        encoding="utf-8",
    )
    return task


def _run_ids(root: Path) -> dict:
    store = FileSystemLineageStore(str(root / ".ubunye" / "lineage"))
    return {name: [r.run_id for r in store.list_runs(f"uc/pkg/{name}")] for name in ("a", "b")}


class TestEachTaskHasItsOwnRunId:
    def test_run_pipeline(self, tmp_path):
        _copy_task(tmp_path, "a")
        _copy_task(tmp_path, "b")
        ubunye.run_pipeline(str(tmp_path), "uc", "pkg", ["a", "b"], backend="pandas", lineage=True)

        ids = _run_ids(tmp_path)
        assert len(ids["a"]) == 1 and len(ids["b"]) == 1
        assert ids["a"][0] != ids["b"][0]

    def test_cli_run_with_many_tasks(self, tmp_path):
        _copy_task(tmp_path, "a")
        _copy_task(tmp_path, "b")
        args = ["run", "-d", str(tmp_path), "-u", "uc", "-p", "pkg", "-t", "a", "-t", "b"]
        result = runner.invoke(app, args + ["--backend", "pandas", "--lineage"])
        assert result.exit_code == 0, result.output

        ids = _run_ids(tmp_path)
        assert len(ids["a"]) == 1 and len(ids["b"]) == 1
        assert ids["a"][0] != ids["b"][0]


class TestTheStoreNeverMixesRecords:
    def test_one_run_id_in_two_tasks_loads_each_tasks_own_record(self, tmp_path):
        # Records written by 0.7.0 multi-task runs share a run id on disk.
        store = FileSystemLineageStore(str(tmp_path))
        for name in ("a", "b"):
            store.save(
                RunContext(
                    run_id="shared",
                    task_path=f"uc/pkg/{name}",
                    usecase="uc",
                    package="pkg",
                    task_name=name,
                    profile="DEV",
                    model="etl",
                    version="1.0.0",
                    config_hash="sha256:0",
                    started_at="2026-09-27T00:00:00Z",
                )
            )

        fresh = FileSystemLineageStore(str(tmp_path))
        assert fresh.load("uc/pkg/a", "shared").task_name == "a"
        assert fresh.load("uc/pkg/b", "shared").task_name == "b"
        assert fresh.load("uc/pkg/a", "shared").task_name == "a"


TRAIN = """from ubunye.core.interfaces import Task
from ubunye.models.registry import ModelRegistry

from model import RowCount


class Train(Task):
    def transform(self, sources):
        model = RowCount()
        metrics = model.train(sources["src"])
        ModelRegistry(self.config["CONFIG"]["outputs"]["out"]["path"] + "_store").register(
            "uc", "RowCount", "1.0.0", model, metrics
        )
        return {"out": sources["src"]}
"""


def _train_task(root: Path, transform_block: str = "{}") -> Path:
    task = _copy_task(root, "train")
    (task / "model.py").write_text(MODEL, encoding="utf-8")
    (task / "transformations.py").write_text(TRAIN, encoding="utf-8")
    cfg = (task / "config.yaml").read_text(encoding="utf-8")
    (task / "config.yaml").write_text(
        cfg.replace("  transform: {}", f"  transform: {transform_block}"), encoding="utf-8"
    )
    return task


def _store(root: Path) -> ModelRegistry:
    return ModelRegistry((root / "out" / "train_store").as_posix())


class TestARegisteredModelPointsAtItsRun:
    def test_a_model_registered_in_a_task_links_to_its_run(self, tmp_path):
        ubunye.run_task(str(_train_task(tmp_path)), backend="pandas", lineage=True)

        (version,) = _store(tmp_path).list_versions("uc", "RowCount")
        records = FileSystemLineageStore(str(tmp_path / ".ubunye" / "lineage")).list_runs(
            "uc/pkg/train"
        )
        assert len(records) == 1
        assert version.lineage_run_id == records[0].run_id

    def test_a_given_run_id_wins(self, tmp_path):
        from ubunye.core.runtime import _CURRENT_CONTEXT

        token = _CURRENT_CONTEXT.set(EngineContext(run_id="the-run"))
        try:
            store = ModelRegistry(str(tmp_path))
            model = RowCountStub()
            store.register("uc", "M", "1.0.0", model, {}, lineage_run_id="given")
            store.register("uc", "M", "1.0.1", model, {})
        finally:
            _CURRENT_CONTEXT.reset(token)
        ids = {v.version: v.lineage_run_id for v in store.list_versions("uc", "M")}
        assert ids == {"1.0.0": "given", "1.0.1": "the-run"}

    def test_outside_a_run_there_is_no_run_id(self, tmp_path):
        assert current_run_id() is None
        store = ModelRegistry(str(tmp_path))
        store.register("uc", "M", "1.0.0", RowCountStub(), {})
        assert store.list_versions("uc", "M")[0].lineage_run_id is None

    def test_model_transform_reads_params_as_config_yaml_writes_them(self, tmp_path):
        task = tmp_path / "train"
        task.mkdir()
        (task / "model.py").write_text(MODEL, encoding="utf-8")
        store = tmp_path / "model_store"
        cfg = {
            "CONFIG": {
                "inputs": {
                    "src": {
                        "format": "s3",
                        "path": _input(tmp_path).as_posix(),
                        "file_format": "csv",
                        "options": {"header": "true"},
                    }
                },
                "transform": {
                    "type": "model",
                    "params": {
                        "action": "train",
                        "model_class": "model.RowCount",
                        "model_dir": task.as_posix(),
                        "registry": {
                            "store": store.as_posix(),
                            "use_case": "uc",
                            "version": "1.0.0",
                        },
                    },
                },
                "outputs": {},
            }
        }
        context = EngineContext(run_id="run-0-7-1", task_name="uc/pkg/train")
        Engine(backend=PandasBackend(), context=context).run(cfg)

        (version,) = ModelRegistry(store.as_posix()).list_versions("uc", "RowCount")
        assert version.lineage_run_id == "run-0-7-1"


MODEL_BLOCK = "{type: model, params: {action: train, model_class: model.RowCount}}"


class TestAnIgnoredTransformTypeIsNeverSilent:
    def test_the_run_warns_and_still_runs_the_task(self, tmp_path):
        task = _train_task(tmp_path, MODEL_BLOCK)
        with pytest.warns(FutureWarning, match="transform.type 'model' is not run"):
            ubunye.run_task(str(task), backend="pandas")
        assert len(_store(tmp_path).list_versions("uc", "RowCount")) == 1

    def test_no_warning_without_a_type(self, tmp_path, recwarn):
        ubunye.run_task(str(_train_task(tmp_path)), backend="pandas")
        assert not [w for w in recwarn if issubclass(w.category, FutureWarning)]

    def test_validate_says_so(self, tmp_path):
        _train_task(tmp_path, MODEL_BLOCK)
        args = ["validate", "-d", str(tmp_path), "-u", "uc", "-p", "pkg", "-t", "train"]
        result = runner.invoke(app, args + ["--json"])
        assert result.exit_code == 0, result.output
        (task,) = json.loads(result.stdout)["tasks"]
        assert task["ok"] is True
        assert "transform.type 'model' is not run" in task["warnings"][0]

    def test_plan_says_so_and_checks_transformations_py(self, tmp_path):
        task = _train_task(tmp_path, MODEL_BLOCK)
        args = ["plan", "-d", str(tmp_path), "-u", "uc", "-p", "pkg", "-t", "train"]
        result = runner.invoke(app, args + ["--backend", "pandas"])
        assert result.exit_code == 0, result.output
        assert "transform.type 'model' is not run" in result.output

        (task / "transformations.py").unlink()
        result = runner.invoke(app, args + ["--backend", "pandas"])
        assert "no transformations.py" in result.output


class RowCountStub:
    def save(self, path):
        import os

        os.makedirs(path, exist_ok=True)

    def metadata(self):
        return {}
