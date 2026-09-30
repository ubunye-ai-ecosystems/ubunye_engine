"""The ``rows-v1`` content hash computed by Spark, where the data lives (ADR 006).

One aggregation over the DataFrame: the row count and the sums of per-row
SHA-256 lanes, so nothing but a few numbers ever reaches the driver. Each row's
canonical line is Spark's own ``to_json`` over the columns sorted by name, with
the options in :data:`ubunye.lineage.content_hash.JSON_OPTIONS`, which is what
the pandas side writes too. The lanes are summed as 32 bit halves in ``long``
(F-041); a table past about 2.1 billion rows falls back to exact decimal sums.
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


def has_map(t: Any) -> bool:
    """Whether a Spark type holds a map anywhere inside it."""
    name = type(t).__name__
    if name == "MapType":
        return True
    if name == "ArrayType":
        return has_map(t.elementType)
    if name == "StructType":
        return any(has_map(f.dataType) for f in t.fields)
    return False


def sorted_maps(col: Any, t: Any) -> Any:
    """``col`` with every map inside it rebuilt with its entries sorted by key.

    A map has no order, and Spark keeps whatever order it was built in, so the
    canonical line sorts it (ADR 006). Built by walking the type; a column with
    no map is returned as it is, so it costs nothing. Uses only functions Spark
    3.5 and 4 both have (``map_keys``, ``array_sort``, ``transform``,
    ``element_at``, ``map_from_arrays``).
    """
    from pyspark.sql import functions as F

    if not has_map(t):
        return col
    name = type(t).__name__
    if name == "MapType":
        keys = F.array_sort(F.map_keys(col))
        values = F.transform(keys, lambda k: sorted_maps(F.element_at(col, k), t.valueType))
        return F.map_from_arrays(keys, values)
    if name == "ArrayType":
        return F.transform(col, lambda x: sorted_maps(x, t.elementType))
    fields = [sorted_maps(col.getField(f.name), f.dataType).alias(f.name) for f in t.fields]
    return F.when(col.isNotNull(), F.struct(*fields))


def fingerprint_spark(df: Any) -> ch.Fingerprint:
    """Fingerprint a Spark DataFrame in one distributed pass."""
    from pyspark.sql import functions as F

    schema = canonical_schema(df)
    names = sorted(df.columns)
    types = {f.name: f.dataType for f in df.schema.fields}
    columns = [sorted_maps(F.col(_quoted(n)), types[n]).alias(n) for n in names]
    line = F.to_json(F.struct(*columns), ch.JSON_OPTIONS)
    digest = F.sha2(line, 256)
    try:
        rows, sums = _half_lane_sums(df, digest)
    except Exception as exc:  # noqa: BLE001 (only a long overflow is retried)
        if not _long_overflow(exc):
            raise
        rows, sums = _decimal_lane_sums(df, digest)
    return ch.Fingerprint(
        schema_hash=ch.schema_hash(schema),
        data_hash=ch.data_hash(schema, rows, sums),
        row_count=rows,
    )


def _half_lane_sums(df: Any, digest: Any) -> Tuple[int, Tuple[int, int]]:
    """The two lane sums from four 32 bit half lanes summed as ``long`` (F-041).

    A lane is ``hi * 2**32 + lo``, so its sum is ``sum(hi) * 2**32 + sum(lo)``.
    ``long`` sums take about 45% less time than the ``decimal(20,0)`` sums they
    replace. They are exact up to about 2.1 billion rows; past that, Spark with
    ANSI on raises an overflow (the caller then takes the decimal path) and with
    ANSI off wraps modulo 2**64, which is all the digest keeps
    (``content_hash.data_hash`` masks each sum), so the digest is still exact.
    """
    from pyspark.sql import functions as F

    def half(start: int) -> Any:
        return F.conv(F.substring(digest, start, 8), 16, 10).cast("long")

    row = df.agg(
        F.count(F.lit(1)).alias("rows"),
        *[F.sum(half(start)).alias(f"h{start}") for start in (1, 9, 17, 25)],
    ).collect()[0]
    return int(row["rows"]), lanes_from_halves([row[f"h{s}"] for s in (1, 9, 17, 25)])


def _long_overflow(exc: BaseException) -> bool:
    """Spark's ANSI error for a ``long`` sum past its range, and nothing else.

    Any other error (a UDF that raised, a missing file, a JVM stack overflow) is the
    job failing, and hashing it again would compute it a second time.
    """
    text = str(exc)
    return "ARITHMETIC_OVERFLOW" in text or "long overflow" in text


def lanes_from_halves(halves: List[Any]) -> Tuple[int, int]:
    """Two lane sums from four half lane sums; a sum that wrapped in ``long`` is fine.

    A wrapped ``long`` is the true sum modulo 2**64 (read back signed). The digest
    masks each lane sum to 64 bits, and ``hi * 2**32 + lo`` modulo 2**64 needs
    ``hi`` only modulo 2**32, so the masked lanes are exact either way.
    """
    h = [int(x or 0) for x in halves]
    return (h[0] << 32) + h[1], (h[2] << 32) + h[3]


def _decimal_lane_sums(df: Any, digest: Any) -> Tuple[int, Tuple[int, int]]:
    """The two lane sums as exact decimals, for a table too big for ``long`` sums."""
    from pyspark.sql import functions as F

    def lane(start: int) -> Any:
        return F.conv(F.substring(digest, start, 16), 16, 10).cast("decimal(20,0)")

    row = df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(lane(1)).alias("a"),
        F.sum(lane(17)).alias("b"),
    ).collect()[0]
    return int(row["rows"]), (int(row["a"] or 0), int(row["b"] or 0))
