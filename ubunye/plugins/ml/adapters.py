"""
Helpers to convert inputs between Spark, pandas, numpy, and torch.
Keep this tiny and extensible.
"""
from __future__ import annotations

from typing import Tuple


def to_numpy_from_pandas(pdf, features):
    X = pdf[features].to_numpy(copy=False)
    return X

def to_numpy_from_spark(sdf, features):
    rows = sdf.select(*features).toPandas()  # simple baseline; for large data prefer vectorized UDFs
    return rows.to_numpy(copy=False)

def ensure_Xy_numpy(data, features, target=None) -> Tuple:
    if hasattr(data, "toPandas"):  # spark
        pdf = data.select(*(features + ([target] if target else []))).toPandas()
        X = pdf[features].to_numpy(copy=False)
        y = pdf[target].to_numpy(copy=False) if target else None
        return X, y
    # pandas-like
    if hasattr(data, "to_numpy"):
        X = data[features].to_numpy(copy=False) if features else data.to_numpy(copy=False)
        y = data[target].to_numpy(copy=False) if (target and target in data.columns) else None
        return X, y
    # already numpy
    return data, None
