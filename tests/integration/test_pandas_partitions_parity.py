"""Partition folders on the pandas backend against real Spark (F-012).

The same frame is written by Spark (through the engine's own Spark write path)
and by the pandas backend, with the same ``partitionBy`` and the same mode, and
then compared:

1. the folder tree, ignoring ``.crc`` files, uuids and Spark task ids;
2. the rows and their types;
3. each engine reading the other's output, with the same schema and values.

Then Spark's partition discovery is compared with the pandas reader on folders
built by hand (type inference, widening, ignored names, errors).

Marked integration (the Spark half needs a JVM). Session zone is not UTC, so a
time zone slip cannot hide.
"""

from __future__ import annotations

import datetime as dt
import os
import re

import pytest

pd = pytest.importorskip("pandas")
pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from pyspark.sql import SparkSession  # noqa: E402

from ubunye.backends.databricks_backend import DatabricksBackend  # noqa: E402
from ubunye.backends.pandas_backend import PandasBackend  # noqa: E402
from ubunye.core import write_modes  # noqa: E402
from ubunye.core.errors import SinkWriteError, SourceReadError  # noqa: E402

from .test_pandas_backend_parity import (  # noqa: E402
    _rows,
    _same_values,
    _schema,
    assert_same,
    pandas_arrow,
    spark_arrow,
)

pytestmark = pytest.mark.integration

ZONE = "Africa/Johannesburg"
UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
TASK_ID = re.compile(r"^part-\d{5}-")


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    before = session.conf.get("spark.sql.session.timeZone")
    session.conf.set("spark.sql.session.timeZone", ZONE)
    yield session
    session.conf.set("spark.sql.session.timeZone", before)


@pytest.fixture
def pandas_backend():
    return PandasBackend(timezone=ZONE)


def _tree(root):
    """Folders and normalised file names under ``root``: no .crc, uuid or task id."""
    out = set()
    if not os.path.exists(root):
        return {"<missing>"}
    for here, dirs, names in os.walk(root):
        rel = os.path.relpath(here, root).replace(os.sep, "/")
        rel = "" if rel == "." else rel + "/"
        for d in dirs:
            if not d.startswith("."):
                out.add(rel + d + "/")
        for n in names:
            if n.endswith(".crc") or n.startswith("."):
                continue
            out.add(rel + TASK_ID.sub("part-<task>-", UUID.sub("<uuid>", n)))
    return out


def _mode(mode, partition_by, fmt="parquet"):
    cfg = {"mode": mode, "partitionBy": list(partition_by)}
    return write_modes.resolve(
        cfg, connector="s3", supported=write_modes.ALL_MODES, default="append", file_format=fmt
    )


def _write_both(spark, pandas_backend, frame, tmp_path, name, partition_by, mode, fmt, options):
    """Write ``frame`` with Spark and with pandas; return the two output folders."""
    source = str(tmp_path / f"{name}_source")
    if not os.path.exists(source):
        pandas_backend.execute_write(
            frame,
            write_modes.ResolvedWriteMode(mode="overwrite", save_mode="overwrite"),
            connector="s3",
            file_format="parquet",
            path=source,
        )
    df = spark.read.parquet(source)
    outs = []
    for engine in ("spark", "pandas"):
        out = str(tmp_path / f"{name}_{engine}")
        backend = DatabricksBackend(spark=spark) if engine == "spark" else pandas_backend
        backend.execute_write(
            df if engine == "spark" else frame,
            _mode(mode, partition_by, fmt),
            connector="s3",
            file_format=fmt,
            path=out,
            partition_by=list(partition_by),
            options=options,
        )
        outs.append(out)
    return outs


def _read_options(fmt):
    return {"header": "true"} if fmt == "csv" else {}


def assert_same_everywhere(spark, pandas_backend, spark_out, pandas_out, fmt="parquet"):
    """Same tree; and Spark and pandas read both outputs to the same frame."""
    assert _tree(pandas_out) == _tree(spark_out)
    opts = _read_options(fmt)
    reference = spark.read.format(fmt).options(**opts).load(spark_out)
    for out in (spark_out, pandas_out):
        assert_same(reference, pandas_backend.read_frame(fmt, out, options=opts))
        other = spark.read.format(fmt).options(**opts).load(out)
        left, right = spark_arrow(reference), spark_arrow(other)
        assert _schema(left) == _schema(right)
        _same_values(_rows(left), _rows(right))


# --------------------------------------------------------------------------- #
# 1. Folder layout, value text and read back types
# --------------------------------------------------------------------------- #

UTC = dt.timezone.utc
STRINGS = [
    "plain",
    "a b",
    "a/b",
    "a=b",
    "a%b",
    "a:b",
    "a#b",
    "a?b",
    "a*b",
    'a"b',
    "a'b",
    "a\\b",
    "a{b}",
    "a[b]",
    "a^b",
    "a<b",
    "a>b",
    "a|b",
    "a\tb",
    "a\nb",
    "a\x7fb",
    "a~!@$&()+,;b",
    "café",
    "日本",
    "a%2Fb",
    "-",
    "",
    None,
]


def _frame(**cols):
    n = len(next(iter(cols.values())))
    return pd.DataFrame({"id": pd.array(range(n), dtype="int64[pyarrow]"), **cols})


LAYOUTS = {
    "strings": (_frame(p=pd.array(STRINGS, dtype="string[pyarrow]")), ["p"]),
    "two-levels": (
        pd.DataFrame(
            {
                "id": pd.array([1, 2, 3], dtype="int32[pyarrow]"),
                "a": ["x", "y", "x"],
                "b": pd.array([10, 20, 20], dtype="int32[pyarrow]"),
            }
        ),
        ["b", "a"],
    ),
    "tinyint": (_frame(p=pd.array([1, -5, None], dtype="int8[pyarrow]")), ["p"]),
    "bigint": (
        _frame(p=pd.array([1, 2147483648, -9223372036854775808], dtype="int64[pyarrow]")),
        ["p"],
    ),
    "boolean": (_frame(p=pd.array([True, False, None], dtype="bool[pyarrow]")), ["p"]),
    "date": (
        _frame(
            p=pd.array(
                [dt.date(2024, 1, 31), dt.date(1, 1, 1), dt.date(9999, 12, 31), None],
                dtype=pd.ArrowDtype(pa.date32()),
            )
        ),
        ["p"],
    ),
    "timestamp-fractions": (
        _frame(
            p=pd.array(
                [
                    dt.datetime(2024, 1, 31, 10, 5, tzinfo=UTC),
                    dt.datetime(2024, 1, 31, 10, 5, 6, 123456, tzinfo=UTC),
                    dt.datetime(2024, 1, 31, 10, 5, 6, 500000, tzinfo=UTC),
                    dt.datetime(2024, 1, 30, 22, 0, tzinfo=UTC),
                ],
                dtype=pd.ArrowDtype(pa.timestamp("us", tz="UTC")),
            )
        ),
        ["p"],
    ),
    "timestamp-whole": (
        _frame(
            p=pd.array(
                [
                    dt.datetime(2024, 1, 31, 10, 5, tzinfo=UTC),
                    dt.datetime(2024, 1, 31, 10, 5, 6, 500000, tzinfo=UTC),
                    None,
                ],
                dtype=pd.ArrowDtype(pa.timestamp("us", tz="UTC")),
            )
        ),
        ["p"],
    ),
    "odd-name": (pd.DataFrame({"id": [1, 2], "k:1 y": ["x", "y"]}), ["k:1 y"]),
    "middle-column": (
        pd.DataFrame({"c1": [1, 2], "p": ["x", "y"], "c3": [3, 4], "c4": ["s", "t"]}),
        ["p"],
    ),
    "name-case": (pd.DataFrame({"id": [1, 2], "a": ["x", "y"]}), ["A"]),
    "empty-frame": (
        pd.DataFrame(
            {"id": pd.array([], dtype="int64[pyarrow]"), "a": pd.array([], dtype="string")}
        ),
        ["a"],
    ),
}


@pytest.mark.parametrize("case", sorted(LAYOUTS))
def test_partitioned_write_is_spark_s(spark, pandas_backend, tmp_path, case):
    frame, partition_by = LAYOUTS[case]
    spark_out, pandas_out = _write_both(
        spark, pandas_backend, frame, tmp_path, "w", partition_by, "overwrite", "parquet", None
    )
    if case == "empty-frame":  # _SUCCESS only; neither engine can read it back
        assert _tree(pandas_out) == _tree(spark_out) == {"_SUCCESS"}
        with pytest.raises(SourceReadError):
            pandas_backend.read_frame("parquet", pandas_out)
        return
    assert_same_everywhere(spark, pandas_backend, spark_out, pandas_out)


@pytest.mark.parametrize("fmt", ["parquet", "csv", "json"])
def test_each_format_partitions_the_same(spark, pandas_backend, tmp_path, fmt):
    frame = pd.DataFrame(
        {"id": [1, 2, 3, 4], "b": [10, 20, 30, 40], "c": [1.5, 2.5, 3.5, 4.5], "a": list("xyxy")}
    )
    options = {"header": "true"} if fmt == "csv" else None
    spark_out, pandas_out = _write_both(
        spark, pandas_backend, frame, tmp_path, fmt, ["a"], "overwrite", fmt, options
    )
    assert_same_everywhere(spark, pandas_backend, spark_out, pandas_out, fmt)


# --------------------------------------------------------------------------- #
# 2. Modes over two runs
# --------------------------------------------------------------------------- #

FIRST = pd.DataFrame({"id": [1, 2, 3, 4], "p": [1, 2, 2, 3], "q": ["a", "a", "b", "a"]})
SECOND = pd.DataFrame({"id": [10, 11], "p": [2, 4], "q": ["a", "a"]})


@pytest.mark.parametrize("mode", ["append", "overwrite", "overwrite_partitions"])
def test_two_runs_of_each_mode(spark, pandas_backend, tmp_path, mode):
    outs = {}
    for engine in ("spark", "pandas"):
        backend = DatabricksBackend(spark=spark) if engine == "spark" else pandas_backend
        out = str(tmp_path / engine)
        for n, frame in enumerate((FIRST, SECOND)):
            source = str(tmp_path / f"source{n}")
            if not os.path.exists(source):
                pandas_backend.execute_write(
                    frame,
                    write_modes.ResolvedWriteMode(mode="overwrite", save_mode="overwrite"),
                    connector="s3",
                    file_format="parquet",
                    path=source,
                )
            if n == 1:  # a stray file: only a full overwrite removes it
                with open(os.path.join(out, "stray.txt"), "w") as fh:
                    fh.write("x")
                with open(os.path.join(out, "p=1", "q=a", "stray.txt"), "w") as fh:
                    fh.write("x")
            data = spark.read.parquet(source) if engine == "spark" else frame
            backend.execute_write(
                data,
                _mode(mode, ["p", "q"]),
                connector="s3",
                file_format="parquet",
                path=out,
                partition_by=["p", "q"],
            )
        outs[engine] = out
    # Stray files: kept by append and by a dynamic overwrite, gone after a full one.
    assert _tree(outs["pandas"]) == _tree(outs["spark"])
    for out in outs.values():
        for stray in (os.path.join(out, "stray.txt"), os.path.join(out, "p=1", "q=a", "stray.txt")):
            if os.path.exists(stray):
                os.remove(stray)  # not parquet: neither engine could read the folder
    assert_same_everywhere(spark, pandas_backend, outs["spark"], outs["pandas"])
    # And the rows are what the mode means.
    got = sorted(pandas_backend.read_frame("parquet", outs["pandas"]).native["id"].tolist())
    want = {
        "append": [1, 2, 3, 4, 10, 11],
        "overwrite": [10, 11],
        "overwrite_partitions": [1, 3, 4, 10, 11],
    }[mode]
    assert got == want


def test_dynamic_overwrite_of_a_new_target_and_an_empty_frame(spark, pandas_backend, tmp_path):
    empty = SECOND.head(0)
    for frame, name in ((SECOND, "new"), (empty, "empty")):
        spark_out, pandas_out = _write_both(
            spark,
            pandas_backend,
            frame,
            tmp_path,
            name,
            ["p", "q"],
            "overwrite_partitions",
            "parquet",
            None,
        )
        assert _tree(pandas_out) == _tree(spark_out), name


# --------------------------------------------------------------------------- #
# 3. Reading folders built by hand: discovery, inference, widening
# --------------------------------------------------------------------------- #


def _build(root, leaves, fmt="parquet", data=None):
    for n, leaf in enumerate(leaves):
        folder = os.path.join(root, *leaf.split("/")) if leaf else root
        os.makedirs(folder, exist_ok=True)
        table = data or pa.table({"id": pa.array([n], pa.int64())})
        if fmt == "parquet":
            pq.write_table(table, os.path.join(folder, f"part-{n}.parquet"))
        elif fmt == "csv":
            with open(os.path.join(folder, f"part-{n}.csv"), "w") as fh:
                fh.write(f"id\n{n}\n")
        else:
            with open(os.path.join(folder, f"part-{n}.json"), "w") as fh:
                fh.write(f'{{"id":{n}}}\n')


READS = {
    "int-long": ["p=1", "p=2147483648"],
    "int-decimal": ["p=1", "p=12345678901234567890"],
    "int-double": ["p=1", "p=1.5"],
    "long-double": ["p=2147483648", "p=1.5"],
    "int-date": ["p=1", "p=2024-01-31"],
    "date-timestamp": ["p=2024-01-31", "p=2024-01-31%2010%3A05%3A00"],
    "timestamp-one-digit": ["p=2024-01-31%2010%3A05%3A00", "p=2024-01-31%2010%3A05%3A06.5"],
    "timestamp-two-digits": ["p=2024-01-31%2010%3A05%3A00", "p=2024-01-31%2010%3A05%3A06.12"],
    "leading-zero": ["p=007"],
    "decimals": ["p=1e3", "p=1E%2B3"],
    "big-decimal": ["p=9223372036854775808"],
    "39-digits": ["p=" + "9" * 39],
    "doubles": ["p=NaN", "p=Infinity", "p=1.5d", "p=-0.0", "p=.5"],
    "strings": ["p=true", "p=0x10", "p=1_000", "p=2024-1-5", "p=2024-01-31T10%3A05%3A00"],
    "escapes": ["p=a%2fb", "p=a%zzb", "p=a%2"],
    "null-and-int": ["p=__HIVE_DEFAULT_PARTITION__", "p=3"],
    "null-and-date": ["p=__HIVE_DEFAULT_PARTITION__", "p=2024-01-31"],
    "two-levels": ["z=1/a=x", "z=2/a=__HIVE_DEFAULT_PARTITION__"],
    "underscore-column": ["_p=7"],
    "root-file-dropped": ["", "p=1"],
    "plain-sub-folder": ["", "sub"],  # not partitioned: the top level files only
    "folder-under-partition": ["p=1", "p=1/sub"],  # its files belong to p=1
}


@pytest.mark.parametrize("case", sorted(READS))
def test_discovery_matches_spark(spark, pandas_backend, tmp_path, case):
    root = str(tmp_path / "t")
    _build(root, READS[case])
    assert_same(spark.read.parquet(root), pandas_backend.read_frame("parquet", root))


@pytest.mark.parametrize("fmt", ["csv", "json"])
def test_text_formats_discover_the_same(spark, pandas_backend, tmp_path, fmt):
    root = str(tmp_path / "t")
    _build(root, ["p=007/d=2024-01-31", "p=1/d=__HIVE_DEFAULT_PARTITION__"], fmt)
    opts = _read_options(fmt)
    assert_same(
        spark.read.format(fmt).options(**opts).load(root),
        pandas_backend.read_frame(fmt, root, options=opts),
    )


def test_names_that_differ_in_case_are_one_column(spark, pandas_backend, tmp_path):
    # Spark takes the name from whichever folder its file index lists first, which
    # is not a fixed order (the SPEC probe got `P`, this layout gets `p`). The
    # values and type agree; the spelling may not.
    root = str(tmp_path / "t")
    _build(root, ["p=1", "P=2"])
    left = spark_arrow(spark.read.parquet(root))
    right = pandas_arrow(pandas_backend.read_frame("parquet", root))
    lower = [(n.lower(), t) for n, t in _schema(left)]
    assert lower == [(n.lower(), t) for n, t in _schema(right)]
    _same_values(_rows(left), _rows(right))


def test_ignored_names_match_spark(spark, pandas_backend, tmp_path):
    root = str(tmp_path / "t")
    _build(root, ["p=1", "_tmpdir/p=9", ".dotdir/p=8", "p=2/_temporary/0"])
    folder = os.path.join(root, "p=1")
    for name in ("_hidden.parquet", ".hidden.parquet", "x._COPYING_"):
        pq.write_table(pa.table({"id": [99]}), os.path.join(folder, name))
    open(os.path.join(folder, "_SUCCESS"), "w").close()
    assert_same(spark.read.parquet(root), pandas_backend.read_frame("parquet", root))


def test_a_partition_column_in_the_data_file_takes_the_folder_value(
    spark, pandas_backend, tmp_path
):
    root = str(tmp_path / "t")
    _build(root, ["p=5"], data=pa.table({"p": ["from_file"], "id": pa.array([1], pa.int64())}))
    assert_same(spark.read.parquet(root), pandas_backend.read_frame("parquet", root))


def test_a_user_schema_types_the_partition_column(spark, pandas_backend, tmp_path):
    root = str(tmp_path / "t")
    _build(root, ["p=007", "p=2"])
    for schema in ("id BIGINT, p STRING", "p STRING, id BIGINT", "id BIGINT"):
        assert_same(
            spark.read.schema(schema).parquet(root),
            pandas_backend.read_frame("parquet", root, schema=schema),
        )


def test_a_sub_folder_reads_without_the_parent_column(spark, pandas_backend, tmp_path):
    root = str(tmp_path / "t")
    _build(root, ["p=1/q=a", "p=2/q=b"])
    sub = os.path.join(root, "p=1")
    assert_same(spark.read.parquet(sub), pandas_backend.read_frame("parquet", sub))


@pytest.mark.parametrize(
    "leaves",
    [["p=1", "_p=7"], ["other", "p=1"], ["a=1", "b=1"], ["a=1/b=1", "a=2"]],
    ids=["underscore-conflict", "plain-beside-partition", "other-names", "other-depth"],
)
def test_layouts_spark_refuses_are_refused(spark, pandas_backend, tmp_path, leaves):
    root = str(tmp_path / "t")
    _build(root, leaves)
    with pytest.raises(Exception):
        spark.read.parquet(root).collect()
    with pytest.raises(SourceReadError, match="Conflicting"):
        pandas_backend.read_frame("parquet", root)


# --------------------------------------------------------------------------- #
# 4. What the pandas backend refuses, Spark fails on or reads back changed
# --------------------------------------------------------------------------- #


def test_a_double_partition_column_does_not_round_trip_on_spark(spark, pandas_backend, tmp_path):
    frame = _frame(p=[1.5, 1e7])
    source = str(tmp_path / "src")
    pandas_backend.execute_write(
        frame,
        write_modes.ResolvedWriteMode(mode="overwrite", save_mode="overwrite"),
        connector="s3",
        file_format="parquet",
        path=source,
    )
    out = str(tmp_path / "spark")
    spark.read.parquet(source).write.partitionBy("p").parquet(out)
    assert dict(spark.read.parquet(out).dtypes)["p"] == "string"  # why pandas refuses
    with pytest.raises(SinkWriteError, match="Cast the column"):
        pandas_backend.execute_write(
            frame,
            _mode("overwrite", ["p"]),
            connector="s3",
            file_format="parquet",
            path=str(tmp_path / "pandas"),
            partition_by=["p"],
        )


def test_pandas_reads_a_table_spark_wrote_in_two_tasks(spark, pandas_backend, tmp_path):
    frame = pd.DataFrame({"id": [1, 2, 3, 4], "a": list("xyxy")})
    source = str(tmp_path / "src")
    pandas_backend.execute_write(
        frame,
        write_modes.ResolvedWriteMode(mode="overwrite", save_mode="overwrite"),
        connector="s3",
        file_format="parquet",
        path=source,
    )
    out = str(tmp_path / "out")
    spark.read.parquet(source).repartition(2).write.partitionBy("a").parquet(out)
    assert_same(spark.read.parquet(out), pandas_backend.read_frame("parquet", out))
    assert pandas_arrow(pandas_backend.read_frame("parquet", out)).num_rows == 4
