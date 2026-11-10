"""Model runs on the run record, and the feature contract at predict (ADR 012).

A training run says on its receipt which model version it registered, the stage
it reached and the gate result; a scoring run says which version and stage it
used, and can stamp them on the scored rows. A frame that lacks a feature the
model declared is caught before the model is called. On the pandas backend, so
no Spark and no Java.
"""

from __future__ import annotations

import logging
import uuid
from pathlib import Path

import pytest
from typer.testing import CliRunner

pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

import pandas as pd  # noqa: E402

from ubunye.backends.pandas_backend import PandasBackend  # noqa: E402
from ubunye.cli.main import app  # noqa: E402
from ubunye.core.errors import (  # noqa: E402
    FeatureContractError,
    PromotionBlockedError,
    UbunyeError,
)
from ubunye.core.runtime import Engine, EngineContext  # noqa: E402
from ubunye.lineage.context import RunContext  # noqa: E402
from ubunye.lineage.recorder import LineageRecorder  # noqa: E402
from ubunye.lineage.storage import FileSystemLineageStore  # noqa: E402
from ubunye.models.registry import ModelRegistry, ModelStage  # noqa: E402
from ubunye.plugins.transforms.model_transform import ModelTransform  # noqa: E402
from ubunye.telemetry.hooks import MonitorHook  # noqa: E402

runner = CliRunner()

MODEL = """\
import json
import os

from ubunye.models.base import UbunyeModel


class Scorer(UbunyeModel):
    features = ["a", "b"]

    def train(self, df):
        self.k = float(len(df))
        return {"auc": 0.9, "rows": float(len(df))}

    def predict(self, df):
        out = df.copy()
        out["score"] = out["a"] + out["b"]
        return out

    def save(self, path):
        os.makedirs(path, exist_ok=True)
        with open(os.path.join(path, "m.json"), "w") as fh:
            json.dump({"k": getattr(self, "k", 0.0)}, fh)

    @classmethod
    def load(cls, path):
        return cls()

    def metadata(self):
        return {"library": "none", "features": list(self.features)}


class Undeclared(Scorer):
    def metadata(self):
        return {"library": "none"}


class Engineered(Scorer):
    # Declares the features it builds inside predict() from the raw column "x".
    def predict(self, df):
        out = df.copy()
        out["a"] = out["x"]
        out["b"] = out["x"] * 2
        out["score"] = out["a"] + out["b"]
        return out
"""


def _task(root: Path, name: str, params: dict, csv: str = "a,b,c\n1,2,3\n4,5,6\n") -> dict:
    """A task's config as the engine gets it; outputs stay in memory."""
    task = root / "uc" / "pkg" / name
    task.mkdir(parents=True)
    (task / "model.py").write_text(MODEL, encoding="utf-8")
    src = task / "in.csv"
    src.write_text(csv, encoding="utf-8")
    return {
        "_name": name,
        "MODEL": "ml",
        "VERSION": "1.0.0",
        "CONFIG": {
            "inputs": {
                "src": {
                    "format": "s3",
                    "path": src.as_posix(),
                    "file_format": "csv",
                    "options": {"header": "true", "inferSchema": "true"},
                }
            },
            "transform": {
                "type": "model",
                "params": {"model_class": "model.Scorer", "model_dir": task.as_posix(), **params},
            },
            "outputs": {},
        },
    }


def _registry(root: Path, **extra) -> dict:
    return {"store": (root / "store").as_posix(), "use_case": "uc", **extra}


def _record(root: Path, task: str) -> RunContext:
    (record,) = FileSystemLineageStore(str(root / ".ubunye" / "lineage")).list_runs(
        f"uc/pkg/{task}"
    )
    return record


def _run(cfg: dict) -> dict:
    cfg = dict(cfg)
    name = cfg.pop("_name")
    root = Path(cfg["CONFIG"]["inputs"]["src"]["path"]).parents[3]
    recorder = LineageRecorder(base_dir=str(root / ".ubunye" / "lineage"))
    context = EngineContext(run_id=str(uuid.uuid4()), task_name=f"uc/pkg/{name}")
    engine = Engine(backend=PandasBackend(), context=context, extra_hooks=[MonitorHook(recorder)])
    return engine.run(cfg)


def _train(root: Path, name: str = "train", **registry) -> dict:
    task = _task(root, name, {"action": "train", "registry": _registry(root, **registry)})
    _run(task)
    return task


# ---------------------------------------------------------------------------
# Train: the receipt says what was registered and how far it got
# ---------------------------------------------------------------------------


class TestATrainingRunRecordsItsModel:
    def test_a_promoted_version_and_its_passed_gates(self, tmp_path):
        _train(tmp_path, promote_to="staging", promotion_gates={"min_auc": 0.8})

        (entry,) = _record(tmp_path, "train").model_runs
        assert entry["action"] == "train"
        assert entry["model"] == "Scorer"
        assert entry["use_case"] == "uc"
        assert entry["version"] == "1.0.0"
        assert entry["stage"] == "staging"
        assert entry["gate_result"] == "passed"
        assert entry["gates"] == [
            {
                "gate": "min_auc",
                "metric": "auc",
                "passed": True,
                "actual": 0.9,
                "threshold": 0.8,
                "message": entry["gates"][0]["message"],
            }
        ]
        assert entry["metrics"] == {"auc": 0.9, "rows": 2.0}

    def test_the_registry_points_back_at_the_same_run(self, tmp_path):
        _train(tmp_path)
        record = _record(tmp_path, "train")
        (version,) = ModelRegistry((tmp_path / "store").as_posix()).list_versions("uc", "Scorer")
        assert version.lineage_run_id == record.run_id
        assert record.model_runs[0]["version"] == version.version

    def test_a_version_not_promoted_stays_in_development(self, tmp_path):
        _train(tmp_path)
        (entry,) = _record(tmp_path, "train").model_runs
        assert entry["stage"] == "development"
        assert entry["promote_to"] is None
        assert entry["gate_result"] is None

    def test_a_promotion_with_no_gates_says_so(self, tmp_path):
        _train(tmp_path, promote_to="staging")
        (entry,) = _record(tmp_path, "train").model_runs
        assert entry["stage"] == "staging"
        assert entry["gate_result"] == "no gates"

    def test_training_without_a_registry_is_still_on_the_receipt(self, tmp_path):
        task = _task(tmp_path, "train", {"action": "train"})
        _run(task)
        (entry,) = _record(tmp_path, "train").model_runs
        assert entry["model"] == "Scorer"
        assert entry["registered"] is False
        assert entry["version"] is None


class TestABlockedPromotionIsKept:
    def test_it_still_raises_and_the_failed_run_says_which_gate(self, tmp_path):
        task = _task(
            tmp_path,
            "train",
            {
                "action": "train",
                "registry": _registry(
                    tmp_path, promote_to="production", promotion_gates={"min_auc": 0.95}
                ),
            },
        )
        with pytest.raises(PromotionBlockedError):
            _run(task)

        record = _record(tmp_path, "train")
        assert record.status == "error"
        (entry,) = record.model_runs
        assert entry["version"] == "1.0.0"
        assert entry["stage"] == "development"
        assert entry["gate_result"] == "blocked"
        assert entry["blocked_by"] == [
            {"gate": "min_auc", "metric": "auc", "actual": 0.9, "threshold": 0.95}
        ]

    def test_on_blocked_record_keeps_the_run_going(self, tmp_path):
        _train(
            tmp_path,
            promote_to="production",
            promotion_gates={"min_auc": 0.95},
            on_blocked="record",
        )
        record = _record(tmp_path, "train")
        assert record.status == "success"
        (entry,) = record.model_runs
        assert entry["gate_result"] == "blocked"
        assert entry["stage"] == "development"
        assert "auc" in entry["error"]
        versions = ModelRegistry((tmp_path / "store").as_posix()).list_versions("uc", "Scorer")
        assert [v.stage for v in versions] == [ModelStage.DEVELOPMENT]


# ---------------------------------------------------------------------------
# Predict: the receipt and (opt in) the rows say which model scored them
# ---------------------------------------------------------------------------


def _predict(root: Path, name: str = "score", csv: str = "a,b,c\n1,2,3\n4,5,6\n", **reg):
    params = {"action": "predict", "registry": _registry(root, use_stage="staging", **reg)}
    return _task(root, name, params, csv=csv)


class TestAScoringRunRecordsItsModel:
    def test_the_version_and_stage_used(self, tmp_path):
        _train(tmp_path, promote_to="staging")
        _run(_predict(tmp_path))

        (entry,) = _record(tmp_path, "score").model_runs
        assert entry["action"] == "predict"
        assert entry["model"] == "Scorer"
        assert entry["version"] == "1.0.0"
        assert entry["stage"] == "staging"
        assert entry["features"] == {"declared": ["a", "b"], "missing": [], "check": "warn"}

    def test_rows_are_not_stamped_by_default(self, tmp_path):
        _train(tmp_path, promote_to="staging")
        out = _run(_predict(tmp_path))["predictions"]
        assert list(out.columns) == ["a", "b", "c", "score"]

    def test_stamp_columns_adds_version_and_stage(self, tmp_path):
        _train(tmp_path, promote_to="staging")
        out = _run(_predict(tmp_path, stamp_columns=True))["predictions"]
        assert list(out.columns) == ["a", "b", "c", "score", "model_version", "model_stage"]
        assert set(out["model_version"]) == {"1.0.0"}
        assert set(out["model_stage"]) == {"staging"}

    def test_stamping_refuses_to_overwrite_a_column(self, tmp_path):
        _train(tmp_path, promote_to="staging")
        task = _predict(tmp_path, csv="a,b,model_version\n1,2,x\n", stamp_columns=True)
        with pytest.raises(UbunyeError, match="model_version"):
            _run(task)

    def test_a_model_path_predict_is_recorded_without_a_version(self, tmp_path):
        model_dir = tmp_path / "m"
        model_dir.mkdir()
        task = _task(tmp_path, "score", {"action": "predict", "model_path": model_dir.as_posix()})
        _run(task)
        (entry,) = _record(tmp_path, "score").model_runs
        assert entry["version"] is None
        assert entry["stage"] is None
        assert entry["model_path"] == model_dir.as_posix()


# ---------------------------------------------------------------------------
# Feature contract at predict
# ---------------------------------------------------------------------------


class TestTheFeatureContract:
    def test_check_features_refuses_a_frame_without_a_declared_feature(self, tmp_path):
        _train(tmp_path, promote_to="staging")
        task = _predict(tmp_path, csv="a,c\n1,3\n", check_features=True)
        with pytest.raises(FeatureContractError) as info:
            _run(task)
        text = str(info.value)
        assert "b" in info.value.context["Missing"]
        assert "Scorer" in text and "1.0.0" in text and "staging" in text
        assert "check_features" in text  # what to try

    def test_extra_columns_are_fine(self, tmp_path):
        _train(tmp_path, promote_to="staging")
        out = _run(_predict(tmp_path, check_features=True))["predictions"]
        assert list(out["score"]) == [3, 9]

    def test_a_model_with_no_declared_features_works_as_before(self, tmp_path):
        task = _task(
            tmp_path,
            "train",
            {
                "action": "train",
                "model_class": "model.Undeclared",
                "registry": _registry(tmp_path, promote_to="staging"),
            },
        )
        _run(task)
        score = _task(
            tmp_path,
            "score",
            {
                "action": "predict",
                "model_class": "model.Undeclared",
                "registry": _registry(tmp_path, use_stage="staging", check_features=True),
            },
            csv="a,b\n1,2\n",
        )
        out = _run(score)["predictions"]
        assert list(out["score"]) == [3]
        (entry,) = _record(tmp_path, "score").model_runs
        assert entry["features"] is None

    def test_by_default_a_model_that_builds_its_features_still_runs(self, tmp_path, caplog):
        # Nothing that works today breaks: a model may declare features it derives
        # inside predict(). The default warns and records; it does not refuse.
        _run(
            _task(
                tmp_path,
                "train",
                {
                    "action": "train",
                    "model_class": "model.Engineered",
                    "registry": _registry(tmp_path, promote_to="staging"),
                },
            )
        )
        score = _task(
            tmp_path,
            "score",
            {
                "action": "predict",
                "model_class": "model.Engineered",
                "registry": _registry(tmp_path, use_stage="staging"),
            },
            csv="x\n1\n",
        )
        with caplog.at_level(logging.WARNING):
            out = _run(score)["predictions"]
        assert list(out["score"]) == [3]
        assert "a, b" in caplog.text
        (entry,) = _record(tmp_path, "score").model_runs
        assert entry["features"]["missing"] == ["a", "b"]

    def test_by_default_a_model_that_then_fails_names_the_missing_columns(self, tmp_path):
        _train(tmp_path, promote_to="staging")
        task = _predict(tmp_path, csv="a,c\n1,3\n")
        with pytest.raises(KeyError) as info:
            _run(task)
        notes = "\n".join(getattr(info.value, "__notes__", []))
        assert "b" in notes and "Scorer" in notes

    def test_check_features_false_skips_the_check(self, tmp_path):
        _train(tmp_path, promote_to="staging")
        task = _predict(tmp_path, csv="a,c\n1,3\n", check_features=False)
        with pytest.raises(KeyError) as info:
            _run(task)
        assert not getattr(info.value, "__notes__", None)
        (entry,) = _record(tmp_path, "score").model_runs
        assert entry["features"] is None


# ---------------------------------------------------------------------------
# Outside a run, and the CLI
# ---------------------------------------------------------------------------


class TestOutsideARun:
    def test_a_direct_call_works_and_records_nothing(self, tmp_path):
        task = tmp_path / "t"
        task.mkdir()
        (task / "model.py").write_text(MODEL, encoding="utf-8")
        cfg = {"action": "train", "model_class": "model.Scorer", "model_dir": task.as_posix()}
        out = ModelTransform().apply({"src": pd.DataFrame({"a": [1], "b": [2]})}, cfg, None)
        assert out["model_metrics"]["auc"] == 0.9


class TestTheCliShowsTheModel:
    def test_show_and_trace(self, tmp_path):
        _train(tmp_path, promote_to="staging", promotion_gates={"min_auc": 0.8})
        args = ["-d", str(tmp_path), "-u", "uc", "-p", "pkg", "-t", "train"]
        args += ["--lineage-dir", str(tmp_path / ".ubunye" / "lineage")]

        shown = runner.invoke(app, ["lineage", "show", *args])
        assert shown.exit_code == 0, shown.output
        assert '"model_runs"' in shown.output

        traced = runner.invoke(app, ["lineage", "trace", *args])
        assert traced.exit_code == 0, traced.output
        assert "MODELS" in traced.output
        assert "train Scorer 1.0.0 -> staging (gates passed)" in traced.output


class TestTheRecordRoundTrips:
    def test_model_runs_survive_to_dict_and_back(self):
        ctx = RunContext(
            run_id="r",
            task_path="uc/pkg/t",
            usecase="uc",
            package="pkg",
            task_name="t",
            profile="DEV",
            model="ml",
            version="1",
            config_hash="sha256:0",
            started_at="2026-11-10T00:00:00Z",
            model_runs=[{"action": "predict", "model": "M", "version": "2", "stage": "staging"}],
        )
        assert RunContext.from_dict(ctx.to_dict()).model_runs == ctx.model_runs

    def test_an_old_record_has_no_model_runs(self):
        old = {
            "run_id": "r",
            "task_path": "uc/pkg/t",
            "usecase": "uc",
            "package": "pkg",
            "task_name": "t",
            "started_at": "2026-01-01T00:00:00Z",
        }
        assert RunContext.from_dict(old).model_runs == []


TASK_CALLS_THE_TRANSFORM = """\
from ubunye.core.interfaces import Task
from ubunye.plugins.transforms.model_transform import ModelTransform


class Score(Task):
    def transform(self, sources):
        params = self.config["CONFIG"]["transform"]["params"]
        out = ModelTransform().apply(sources, params, None)
        return {"scored": out["predictions"]}
"""


class TestATaskThatCallsTheTransform:
    def test_ubunye_run_records_the_model(self, tmp_path):
        # `ubunye run` always runs transformations.py; a task that calls
        # ModelTransform itself gets the same record and the stamped columns.
        _train(tmp_path, promote_to="staging")
        task = tmp_path / "uc" / "pkg" / "batch"
        task.mkdir(parents=True)
        (task / "model.py").write_text(MODEL, encoding="utf-8")
        (task / "transformations.py").write_text(TASK_CALLS_THE_TRANSFORM, encoding="utf-8")
        (task / "in.csv").write_text("a,b\n1,2\n", encoding="utf-8")
        import yaml

        cfg = {
            "MODEL": "ml",
            "VERSION": "1.0.0",
            "CONFIG": {
                "inputs": {
                    "src": {
                        "format": "s3",
                        "path": (task / "in.csv").as_posix(),
                        "file_format": "csv",
                        "options": {"header": "true", "inferSchema": "true"},
                    }
                },
                "transform": {
                    "params": {
                        "action": "predict",
                        "model_class": "model.Scorer",
                        "model_dir": task.as_posix(),
                        "registry": _registry(tmp_path, use_stage="staging", stamp_columns=True),
                    }
                },
                "outputs": {
                    "scored": {
                        "format": "s3",
                        "path": (tmp_path / "out" / "scored").as_posix(),
                        "file_format": "parquet",
                        "mode": "overwrite",
                    }
                },
            },
        }
        (task / "config.yaml").write_text(yaml.safe_dump(cfg), encoding="utf-8")
        import ubunye

        out = ubunye.run_task(str(task), backend="pandas", lineage=True)
        assert set(out["scored"]["model_version"]) == {"1.0.0"}
        (entry,) = _record(tmp_path, "batch").model_runs
        assert (entry["action"], entry["version"], entry["stage"]) == (
            "predict",
            "1.0.0",
            "staging",
        )
