# Model Contract — `UbunyeModel`

`UbunyeModel` is the abstract base class every user-defined ML model must implement.
It is the **only** interface the engine ever calls — no ML library is imported by the engine itself.

---

## The contract

```python
from abc import ABC, abstractmethod
from typing import Any, Dict

class UbunyeModel(ABC):

    @abstractmethod
    def train(self, df: Any) -> Dict[str, Any]:
        """Train on df. Returns a metrics dict e.g. {"auc": 0.94, "f1": 0.87}."""

    @abstractmethod
    def predict(self, df: Any) -> Any:
        """Generate predictions. Returns df-like with prediction columns added."""

    @abstractmethod
    def save(self, path: str) -> None:
        """Serialize model to path. Pick any format — pickle, joblib, ONNX, …"""

    @classmethod
    @abstractmethod
    def load(cls, path: str) -> "UbunyeModel":
        """Deserialize from path. Returns a ready-to-predict instance."""

    @abstractmethod
    def metadata(self) -> Dict[str, Any]:
        """Return at minimum: library, library_version, features, params."""

    def validate(self, df: Any) -> Dict[str, Any]:
        """Optional holdout validation. Default raises NotImplementedError."""
        raise NotImplementedError("validate() not implemented")
```

---

## Why this contract?

The engine never imports `sklearn`, `torch`, `xgboost`, or any ML library.
It only calls `train()`, `predict()`, `save()`, and `load()` — the implementation is entirely yours.

This gives you:

- **Library freedom** — wrap any ML framework without engine changes.
- **Testability** — test your model class with unit tests and `MockDF`; no Spark needed.
- **Registry integration** — the engine calls `save()` and reads `metadata()` + `train()` metrics
  automatically when `registry` is configured in the transform params.

---

## Minimal implementation (sklearn)

```python
# model.py  (in your task directory)
import joblib
from pathlib import Path
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import roc_auc_score, f1_score
from ubunye.models.base import UbunyeModel

class FraudRiskModel(UbunyeModel):

    def __init__(self):
        self._clf = GradientBoostingClassifier(n_estimators=100, max_depth=4)
        self._features = ["amount_sum_30d", "txn_count_30d", "avg_amount_30d", "risk_encoded"]

    def train(self, df) -> dict:
        pdf = df.toPandas()
        X, y = pdf[self._features], pdf["is_fraud"]
        self._clf.fit(X, y)
        proba = self._clf.predict_proba(X)[:, 1]
        preds = self._clf.predict(X)
        return {
            "auc": float(roc_auc_score(y, proba)),
            "f1":  float(f1_score(y, preds)),
        }

    def predict(self, df):
        pdf = df.toPandas()
        pdf["fraud_score"]     = self._clf.predict_proba(pdf[self._features])[:, 1]
        pdf["fraud_predicted"] = self._clf.predict(pdf[self._features])
        return df.sparkSession.createDataFrame(pdf)

    def save(self, path: str) -> None:
        Path(path).mkdir(parents=True, exist_ok=True)
        joblib.dump(self._clf, f"{path}/model.pkl")

    @classmethod
    def load(cls, path: str) -> "FraudRiskModel":
        m = cls()
        m._clf = joblib.load(f"{path}/model.pkl")
        return m

    def metadata(self) -> dict:
        return {
            "library":         "scikit-learn",
            "library_version": "1.4.0",
            "features":        self._features,
            "params":          self._clf.get_params(),
        }
```

---

## `train()` — return value

`train()` must return a `dict` of metric name → numeric value.
These metrics are stored in the registry and used by promotion gates.

```python
return {
    "auc":      0.92,
    "f1":       0.87,
    "accuracy": 0.95,
    "loss":     0.08,
}
```

---

## `predict()` — return value

`predict()` receives the same type of object `train()` received (a PySpark DataFrame
or any duck-typed DataFrame-like) and should return a DataFrame-like with prediction
columns added. The return value is written to `CONFIG.outputs` by the engine.

---

## `metadata()` — required keys

The `metadata()` dict must contain at minimum:

| Key | Type | Description |
|---|---|---|
| `library` | string | ML library used (e.g. `"scikit-learn"`, `"xgboost"`, `"pytorch"`) |
| `library_version` | string | Version string |
| `features` | list of strings | Input feature column names |
| `params` | dict | Hyperparameters |

Additional keys are stored but not validated.

---

## Testing your model (Spark-free)

Use a duck-typed `MockDF` to test your model without PySpark:

```python
import pytest
from model import FraudRiskModel

class MockDF:
    def __init__(self, rows):
        self._rows = rows
    def toPandas(self):
        import pandas as pd
        return pd.DataFrame(self._rows)
    def count(self): return len(self._rows)

def test_train_returns_metrics(tmp_path):
    model = FraudRiskModel()
    rows = [
        {"amount_sum_30d": 100, "txn_count_30d": 5, "avg_amount_30d": 20, "risk_encoded": 0, "is_fraud": 0},
        {"amount_sum_30d": 900, "txn_count_30d": 2, "avg_amount_30d": 450, "risk_encoded": 2, "is_fraud": 1},
    ] * 50
    metrics = model.train(MockDF(rows))
    assert "auc" in metrics
    assert "f1" in metrics
    model.save(str(tmp_path / "model"))
    loaded = FraudRiskModel.load(str(tmp_path / "model"))
    assert loaded is not None
```

---

## Using with the `model` transform

Reference your class in `config.yaml`:

```yaml
CONFIG:
  transform:
    type: model
    params:
      action: train
      model_class: "model.FraudRiskModel"   # model.py is in the task directory
      registry:
        store: ".ubunye/model_store"
        use_case: fraud_detection
        auto_version: true
        promote_to: staging
        promotion_gates:
          min_auc: 0.85
          min_f1: 0.80
```

See [Model Registry](registry.md) for the full registry reference.

---

## Abstract enforcement

`UbunyeModel` is a strict ABC. Any class that does not implement all five abstract methods
(`train`, `predict`, `save`, `load`, `metadata`) will raise `TypeError` on instantiation:

```python
class IncompleteModel(UbunyeModel):
    def train(self, df): return {}
    # missing predict, save, load, metadata

IncompleteModel()   # TypeError: Can't instantiate abstract class IncompleteModel
```

This is checked by unit tests and by the `model_transform` plugin before invoking `train()`.
