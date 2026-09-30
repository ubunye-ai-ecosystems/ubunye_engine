"""Exact column sums for ``reconcile`` (F-017), taken by each engine itself.

A reconcile compares an input's total with an output's. Summed by a DataFrame
library's defaults, an integer column wraps past 2**63 and a decimal column may
come back as a float, so a real difference can vanish. Here integers and
decimals are summed exactly, and floats leave out null and NaN (as the rules do,
F-045). The core asks through :func:`exact_sums` and never imports an engine.
"""

from __future__ import annotations

import decimal
from typing import Any, Dict, List, Optional, Tuple


def typed(value: Any, kind: str) -> Any:
    """A collected sum as an exact Python value: int, Decimal or float; none is 0."""
    if kind == "int":
        return int(value or 0)
    if kind == "decimal":
        return decimal.Decimal(0) if value is None else decimal.Decimal(value)
    return 0.0 if value is None else float(value)


def arrow_sums(frame: Any, columns: List[str]) -> Tuple[int, Dict[str, Any]]:
    """Row count and exact sums of a pandas frame or an Arrow table, with Arrow.

    Integers are summed as decimal128(38,0) and decimals as decimal256(76,s), so
    neither can overflow; floats leave out null and NaN.
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    if isinstance(frame, pa.Table):
        rows = frame.num_rows

        def column_of(c: str) -> Any:
            return frame.column(c)

    else:
        rows = len(frame)

        def column_of(c: str) -> Any:
            return pa.array(frame[c], from_pandas=True)

    sums: Dict[str, Any] = {}
    for column in columns:
        arr = column_of(column)
        t = arr.type
        if pa.types.is_integer(t):
            sums[column] = typed(pc.sum(arr.cast(pa.decimal128(38, 0))).as_py(), "int")
        elif pa.types.is_decimal(t):
            total = pc.sum(arr.cast(pa.decimal256(76, t.scale))).as_py()
            sums[column] = typed(total, "decimal")
        else:
            keep = pc.if_else(pc.is_nan(arr), pa.scalar(None, t), arr)
            sums[column] = typed(pc.sum(keep).as_py(), "float")
    return rows, sums


def exact_sums(frame: Any, columns: List[str]) -> Optional[Tuple[int, Dict[str, Any]]]:
    """Row count and exact sums of ``columns`` for a Spark, pandas or Arrow frame.

    ``None`` for any other frame: the caller sums it another way.
    """
    package = (getattr(type(frame), "__module__", "") or "").split(".")[0]
    if package == "pyspark":
        from ubunye.adapters.spark.sums import spark_sums

        return spark_sums(frame, columns)
    if package in ("pandas", "pyarrow"):
        return arrow_sums(frame, columns)
    return None
