"""Adapters: the thin shims that make non-Spark data satisfy the engine's ports.

Ports and adapters, the second half. ``ubunye.core.ports`` defines the shape;
these fill it. A Spark DataFrame needs no adapter — it satisfies ``DataFramePort``
natively, which is exactly why the port could ship without breaking anything.
"""

from ubunye.adapters.pandas_adapter import PandasDataFrameAdapter

__all__ = ["PandasDataFrameAdapter"]
