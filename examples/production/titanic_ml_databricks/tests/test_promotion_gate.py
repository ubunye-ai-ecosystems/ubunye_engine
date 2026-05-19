"""Tests that promotion gate failure propagates instead of being silently swallowed.

Covers task-18: if the model's AUC is below min_auc, the task must raise
PromotionBlockedError so CI goes red.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ubunye.core.errors import PromotionBlockedError

TRAIN_DIR = Path(__file__).resolve().parent.parent / "pipelines" / "titanic" / "ml" / "train_classifier"
_spec = importlib.util.spec_from_file_location(
    "train_transformations", TRAIN_DIR / "transformations.py",
    submodule_search_locations=[str(TRAIN_DIR)],
)
_train_mod = importlib.util.module_from_spec(_spec)
sys.modules["train_transformations"] = _train_mod
if str(TRAIN_DIR) not in sys.path:
    sys.path.insert(0, str(TRAIN_DIR))
_spec.loader.exec_module(_train_mod)
TrainTitanicClassifier = _train_mod.TrainTitanicClassifier


@pytest.fixture
def titanic_df() -> pd.DataFrame:
    rows = []
    for i in range(80):
        survived = i % 3 == 0
        rows.append(
            {
                "Pclass": 1 if survived else 3,
                "Sex": "female" if survived else "male",
                "Age": 30.0 + (i % 10),
                "SibSp": i % 2,
                "Parch": 0,
                "Fare": 50.0 + (i % 20),
                "Embarked": "S" if i % 2 else "C",
                "Survived": int(survived),
            }
        )
    return pd.DataFrame(rows)


def test_promotion_gate_failure_propagates(titanic_df: pd.DataFrame, tmp_path):
    """PromotionBlockedError must NOT be caught — it should crash the task."""
    mock_spark_df = MagicMock()
    mock_spark_df.toPandas.return_value = titanic_df

    mock_spark_session = MagicMock()
    mock_spark_df.sparkSession = mock_spark_session
    mock_spark_session.createDataFrame.return_value = MagicMock()

    mock_registry = MagicMock()
    mock_version = MagicMock()
    mock_version.version = "1.0.0"
    mock_registry.register.return_value = mock_version
    mock_registry.promote.side_effect = PromotionBlockedError(
        "Promotion blocked — 1 gate(s) failed:\n  - min_auc: auc: 0.5000 < 0.8 (min threshold)",
        context={"Version": "1.0.0", "Target stage": "staging"},
        hint="Fix the failing gates or adjust thresholds.",
    )

    task = TrainTitanicClassifier(config={})

    with patch.dict("os.environ", {"TITANIC_MODEL_STORE": str(tmp_path)}):
        with patch.object(_train_mod, "ModelRegistry", return_value=mock_registry):
            with pytest.raises(PromotionBlockedError, match="gate.*failed"):
                task.transform({"titanic": mock_spark_df})
