"""ADR 012 on Spark: stamped columns and the feature contract match pandas.

A model scored through ``ModelTransform`` gets the same ``model_version`` and
``model_stage`` columns on a Spark frame as on a pandas one, and a Spark frame
without a declared feature is refused the same way.
"""

from __future__ import annotations

import json
import os

import pytest

from ubunye.core.errors import FeatureContractError
from ubunye.models.base import UbunyeModel
from ubunye.models.receipt import recording
from ubunye.models.registry import ModelRegistry, ModelStage
from ubunye.plugins.transforms.model_transform import ModelTransform

pytestmark = pytest.mark.integration


class Scorer(UbunyeModel):
    """Adds a ``score`` column on pandas and on Spark (JVM side, no Python workers)."""

    def train(self, df):
        return {"auc": 0.9}

    def predict(self, df):
        if hasattr(df, "withColumn"):
            return df.withColumn("score", df["a"] + df["b"])
        return df.assign(score=df["a"] + df["b"])

    def save(self, path):
        os.makedirs(path, exist_ok=True)
        with open(os.path.join(path, "m.json"), "w") as fh:
            json.dump({}, fh)

    @classmethod
    def load(cls, path):
        return cls()

    def metadata(self):
        return {"library": "none", "features": ["a", "b"]}


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("ubunye-model-receipts")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def store(tmp_path):
    registry = ModelRegistry(str(tmp_path))
    registry.register("uc", "Scorer", "1.0.0", Scorer(), {"auc": 0.9})
    registry.promote("uc", "Scorer", "1.0.0", ModelStage.STAGING)
    return str(tmp_path)


def _cfg(store, **registry):
    return {
        "action": "predict",
        "model_class": f"{__name__}.Scorer",
        "registry": {"store": store, "use_case": "uc", "use_stage": "staging", **registry},
    }


def test_stamped_columns_match_pandas(spark, store):
    import pandas as pd

    pdf = pd.DataFrame({"a": [1, 4], "b": [2, 5], "c": [3, 6]})
    sdf = spark.createDataFrame(pdf)
    cfg = _cfg(store, stamp_columns=True, check_features=True)

    with recording() as runs:
        on_spark = ModelTransform().apply({"src": sdf}, cfg, None)["predictions"]
        on_pandas = ModelTransform().apply({"src": pdf}, cfg, None)["predictions"]

    assert on_spark.columns == list(on_pandas.columns)
    assert on_spark.toPandas().astype(str).to_dict("records") == on_pandas.astype(str).to_dict(
        "records"
    )
    assert {r["version"] for r in runs} == {"1.0.0"}
    assert {r["stage"] for r in runs} == {"staging"}


def test_a_spark_frame_without_a_feature_is_refused(spark, store):
    sdf = spark.createDataFrame([(1, 3)], ["a", "c"])
    with pytest.raises(FeatureContractError, match="b"):
        ModelTransform().apply({"src": sdf}, _cfg(store, check_features=True), None)
