"""The ``rows-v1`` content hash computed by Spark, where the data lives (ADR 006).

One aggregation over the DataFrame: the row count and two sums of per-row
SHA-256 lanes, so nothing but three numbers ever reaches the driver. Each row's
canonical line is Spark's own ``to_json`` over the columns sorted by name, with
the options in :data:`ubunye.lineage.content_hash.JSON_OPTIONS`, which is what
the pandas side writes too. The sums are exact decimals, so no overflow and no
ANSI error for any realistic table (up to about 10**11 rows).
"""

from __future__ import annotations

from typing import Any, List, Tuple

from ubunye.lineage import content_hash as ch


def spark_kind(t: Any) -> str:
    """A Spark SQL type as a canonical type name (the same names Arrow's map to)."""
    name = type(t).__name__
    simple = {
        "ByteType": "int8",
        "ShortType": "int16",
        "IntegerType": "int32",
        "LongType": "int64",
        "FloatType": "float32",
        "DoubleType": "float64",
        "BooleanType": "bool",
        "StringType": "string",
        "VarcharType": "string",
        "CharType": "string",
        "BinaryType": "binary",
        "DateType": "date",
        "TimestampType": "timestamp",
        "TimestampNTZType": "timestamp_ntz",
        "NullType": "null",
    }
    if name in simple:
        return simple[name]
    if name == "DecimalType":
        return f"decimal({t.precision},{t.scale})"
    if name == "ArrayType":
        return f"list<{spark_kind(t.elementType)}>"
    if name == "MapType":
        return f"map<{spark_kind(t.keyType)},{spark_kind(t.valueType)}>"
    if name == "StructType":
        return "struct<" + ",".join(f"{f.name}:{spark_kind(f.dataType)}" for f in t.fields) + ">"
    return t.simpleString()


def canonical_schema(df: Any) -> List[Tuple[str, str]]:
    return [(f.name, spark_kind(f.dataType)) for f in df.schema.fields]


def _quoted(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def fingerprint_spark(df: Any) -> ch.Fingerprint:
    """Fingerprint a Spark DataFrame in one distributed pass."""
    from pyspark.sql import functions as F

    schema = canonical_schema(df)
    names = sorted(df.columns)
    line = F.to_json(F.struct(*[F.col(_quoted(n)) for n in names]), ch.JSON_OPTIONS)
    digest = F.sha2(line, 256)

    def lane(start: int) -> Any:
        return F.conv(F.substring(digest, start, 16), 16, 10).cast("decimal(20,0)")

    row = df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(lane(1)).alias("a"),
        F.sum(lane(17)).alias("b"),
    ).collect()[0]
    rows = int(row["rows"])
    sums = (int(row["a"] or 0), int(row["b"] or 0))
    return ch.Fingerprint(
        schema_hash=ch.schema_hash(schema),
        data_hash=ch.data_hash(schema, rows, sums),
        row_count=rows,
    )
