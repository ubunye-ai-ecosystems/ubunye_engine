"""Unit tests for UbunyeModel abstract contract.

All tests are Spark-free. A MockDF is used as a duck-typed stand-in for a
PySpark DataFrame anywhere the contract calls for a DataFrame argument.
"""

import json
from pathlib import Path
from typing import Any, Dict

import pytest

from ubunye.models.base import UbunyeModel

# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


class MockDF:
    """Minimal duck-typed DataFrame stand-in — no PySpark needed."""

    def __init__(self, rows=None):
        self._rows = rows or [{"id": 1, "val": 2.0}, {"id": 2, "val": 3.5}]

    def count(self) -> int:
        return len(self._rows)

    def toPandas(self):
        return self._rows


class DummyModel(UbunyeModel):
    """Minimal UbunyeModel implementation for contract testing."""

    def __init__(self):
        self._trained = False
        self._row_count = 0

    def train(self, df: Any) -> Dict[str, Any]:
        self._trained = True
        self._row_count = df.count()
        return {"accuracy": 0.95, "f1": 0.90, "auc": 0.92}

    def predict(self, df: Any) -> Any:
        return df  # pass-through for testing

    def save(self, path: str) -> None:
        p = Path(path)
        p.mkdir(parents=True, exist_ok=True)
        (p / "model.json").write_text(
            json.dumps({"trained": self._trained, "row_count": self._row_count}),
            encoding="utf-8",
        )

    @classmethod
    def load(cls, path: str) -> "DummyModel":
        state = json.loads((Path(path) / "model.json").read_text(encoding="utf-8"))
        m = cls()
        m._trained = state["trained"]
        m._row_count = state["row_count"]
        return m

    def metadata(self) -> Dict[str, Any]:
        return {
            "library": "dummy",
            "library_version": "1.0.0",
            "features": ["id", "val"],
            "params": {"depth": 3},
        }


# ---------------------------------------------------------------------------
# Contract tests
# ---------------------------------------------------------------------------


class TestUbunyeModelContract:
    def test_is_ubunye_model_subclass(self):
        assert isinstance(DummyModel(), UbunyeModel)
        assert issubclass(DummyModel, UbunyeModel)

    def test_train_returns_dict(self):
        model = DummyModel()
        metrics = model.train(MockDF())
        assert isinstance(metrics, dict)
        assert len(metrics) > 0

    def test_train_metrics_have_expected_keys(self):
        model = DummyModel()
        metrics = model.train(MockDF())
        assert "accuracy" in metrics
        assert "f1" in metrics

    def test_predict_returns_df_like(self):
        model = DummyModel()
        df = MockDF()
        result = model.predict(df)
        assert result is df  # pass-through contract

    def test_save_creates_files(self, tmp_path):
        model = DummyModel()
        model.train(MockDF())
        model_dir = str(tmp_path / "model")
        model.save(model_dir)
        assert (tmp_path / "model" / "model.json").exists()

    def test_load_round_trips_state(self, tmp_path):
        model = DummyModel()
        df = MockDF(rows=[{"id": i} for i in range(5)])
        model.train(df)
        model.save(str(tmp_path / "model"))

        loaded = DummyModel.load(str(tmp_path / "model"))
        assert loaded._trained is True
        assert loaded._row_count == 5

    def test_train_predict_consistent_after_save_load(self, tmp_path):
        model = DummyModel()
        df = MockDF()
        model.train(df)
        pred1 = model.predict(df)

        model.save(str(tmp_path / "model"))
        loaded = DummyModel.load(str(tmp_path / "model"))
        pred2 = loaded.predict(df)

        # Both return the same MockDF object (pass-through)
        assert pred1 is pred2 or pred1 is df

    def test_metadata_has_required_keys(self):
        model = DummyModel()
        meta = model.metadata()
        assert isinstance(meta, dict)
        assert "library" in meta
        assert "library_version" in meta
        assert "features" in meta
        assert "params" in meta

    def test_validate_raises_not_implemented(self):
        model = DummyModel()
        with pytest.raises(NotImplementedError):
            model.validate(MockDF())

    def test_non_ubunye_model_not_subclass(self):
        class FakeModel:
            def train(self, df):
                pass

        assert not issubclass(FakeModel, UbunyeModel)
        assert not isinstance(FakeModel(), UbunyeModel)

    def test_abstract_base_not_instantiable(self):
        with pytest.raises(TypeError):
            UbunyeModel()  # type: ignore

    def test_partial_implementation_not_instantiable(self):
        """A class missing any abstract method must not be instantiable."""

        class Partial(UbunyeModel):
            def train(self, df):
                return {}

            # missing predict, save, load, metadata

        with pytest.raises(TypeError):
            Partial()  # type: ignore
