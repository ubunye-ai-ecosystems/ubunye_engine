"""Exact column sums on Spark, for ``reconcile`` (F-017). See ``ubunye.adapters.sums``."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from ubunye.adapters.sums import typed


def spark_sums(native: Any, columns: List[str]) -> Tuple[int, Dict[str, Any]]:
    """Row count and exact sums on Spark, in one aggregation.

    Integers are summed as decimal(38,0), so a total past 2**63 neither wraps
    (non-ANSI) nor raises (ANSI). Decimals are summed as they are (Spark widens
    the precision by 10, up to 38). Floats leave out null and NaN.
    """
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    whole = (T.ByteType, T.ShortType, T.IntegerType, T.LongType)
    exprs = [F.count(F.lit(1)).alias("__rows")]
    kinds: Dict[str, str] = {}
    for i, column in enumerate(columns):
        col = native[column]
        t = native.schema[column].dataType
        if isinstance(t, whole):
            kinds[column] = "int"
            expr = F.sum(col.cast("decimal(38,0)"))
        elif isinstance(t, (T.FloatType, T.DoubleType)):
            kinds[column] = "float"
            expr = F.sum(F.when(~(col.isNull() | F.isnan(col)), col))
        else:
            kinds[column] = "decimal"
            expr = F.sum(col)
        exprs.append(expr.alias(f"s{i}"))
    row = native.agg(*exprs).collect()[0].asDict()
    sums = {c: typed(row[f"s{i}"], kinds[c]) for i, c in enumerate(columns)}
    return int(row["__rows"]), sums
