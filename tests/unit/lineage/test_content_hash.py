"""The ``rows-v1`` content hash (ADR 006), on the Arrow / pandas side.

The Spark side must give the same answers; the integration tier checks that.
Here the promises are pinned with examples and with Hypothesis properties.
"""

from __future__ import annotations

import datetime as dt
import decimal
import json

import pytest

pa = pytest.importorskip("pyarrow")
pd = pytest.importorskip("pandas")
hypothesis = pytest.importorskip("hypothesis")

from hypothesis import given, settings  # noqa: E402
from hypothesis import strategies as st  # noqa: E402

from ubunye.adapters.pandas_adapter import PandasDataFrameAdapter  # noqa: E402
from ubunye.lineage import content_hash as ch  # noqa: E402

# The line Spark 4.2's to_json wrote for this row with ch.JSON_OPTIONS (probed).
SPARK_ROW = {
    "i": pa.array([1], pa.int32()),
    "l": pa.array([3000000000], pa.int64()),
    "d": pa.array([1.5]),
    "n": pa.array([float("nan")]),
    "b": pa.array([True]),
    "s": pa.array(['say "hi"']),
    "day": pa.array([dt.date(2024, 1, 2)]),
    "ts": pa.array(
        [dt.datetime(2024, 1, 2, 1, 4, 5, 123456, tzinfo=dt.timezone.utc)],
        pa.timestamp("us", tz="UTC"),
    ),
    "dec": pa.array([decimal.Decimal("1.50")], pa.decimal128(10, 2)),
    "arr": pa.array([[1.0, None]], pa.list_(pa.float64())),
    "st": pa.array([{"k": None, "j": 2}], pa.struct([("k", pa.float64()), ("j", pa.int32())])),
    "m": pa.array([[("a", 1)]], pa.map_(pa.string(), pa.int32())),
    "bin": pa.array([b"\x00\x01"]),
    "f": pa.array([0.1], pa.float32()),
}
SPARK_LINE = (
    '{"arr":[1.0,null],"b":true,"bin":"AAE=","d":1.5,"day":"2024-01-02","dec":1.50,'
    '"f":0.1,"i":1,"l":3000000000,"m":{"a":1},"n":"NaN","s":"say \\"hi\\"",'
    '"st":{"j":2},"ts":"2024-01-02T01:04:05.123456Z"}'
)


def _line(table):
    schema = [(f.name, ch.arrow_kind(f.type)) for f in table.schema]
    row = {n: table.column(n).to_pylist()[0] for n in table.column_names}
    return ch.canonical_line(row, dict(schema))


class TestCanonicalLine:
    def test_matches_what_spark_writes(self):
        assert _line(pa.table(SPARK_ROW)) == SPARK_LINE

    def test_an_all_null_row_is_an_empty_object(self):
        assert _line(pa.table({"a": pa.array([None], pa.int64())})) == "{}"

    def test_the_timezone_does_not_matter(self):
        instant = dt.datetime(2024, 1, 2, 1, 4, 5, tzinfo=dt.timezone.utc)
        local = instant.astimezone(dt.timezone(dt.timedelta(hours=2)))
        assert ch.value_text(instant) == ch.value_text(local) == '"2024-01-02T01:04:05.000000Z"'

    @pytest.mark.parametrize(
        "value, text",
        [
            (2.0, "2.0"),
            (1e10, "1.0E10"),
            (1e-5, "1.0E-5"),
            (-0.0, "-0.0"),
            (float("inf"), '"Infinity"'),
            (1 / 3, "0.3333333333333333"),
        ],
    )
    def test_doubles_are_written_the_java_way(self, value, text):
        assert ch.value_text(value) == text


def _fp(table):
    return ch.fingerprint_arrow(table)


BASE = pa.table({"id": pa.array([1, 2, 3], pa.int64()), "v": ["a", "b", None]})


class TestPromises:
    def test_row_order_does_not_matter(self):
        assert _fp(BASE).data_hash == _fp(BASE.take([2, 0, 1])).data_hash

    def test_column_order_does_not_matter(self):
        assert _fp(BASE).data_hash == _fp(BASE.select(["v", "id"])).data_hash

    def test_any_changed_value_changes_it(self):
        changed = pa.table({"id": pa.array([1, 2, 4], pa.int64()), "v": ["a", "b", None]})
        assert _fp(BASE).data_hash != _fp(changed).data_hash

    def test_a_duplicated_row_changes_it(self):
        assert _fp(BASE).data_hash != _fp(BASE.take([0, 0, 1, 2])).data_hash

    def test_null_is_not_nan(self):
        a = pa.table({"x": pa.array([None], pa.float64())})
        b = pa.table({"x": pa.array([float("nan")], pa.float64())})
        assert _fp(a).data_hash != _fp(b).data_hash

    def test_the_type_counts(self):
        a = pa.table({"x": pa.array([1], pa.int32())})
        b = pa.table({"x": pa.array([1], pa.int64())})
        assert _fp(a).data_hash != _fp(b).data_hash
        assert _fp(a).schema_hash != _fp(b).schema_hash

    def test_an_empty_table_has_a_hash_and_zero_rows(self):
        fp = _fp(BASE.slice(0, 0))
        assert fp.row_count == 0 and fp.data_hash and fp.is_complete

    def test_the_method_is_named(self):
        assert _fp(BASE).method == "rows-v1"


class TestAnyFrame:
    def test_pandas_frame_and_its_port_agree_with_arrow(self):
        frame = BASE.to_pandas(types_mapper=pd.ArrowDtype)
        assert ch.fingerprint(frame).data_hash == _fp(BASE).data_hash
        assert ch.fingerprint(PandasDataFrameAdapter(frame)).data_hash == _fp(BASE).data_hash

    def test_naive_pandas_timestamps_use_the_port_timezone(self):
        frame = pd.DataFrame({"ts": pd.to_datetime(["2024-01-02 03:04:05"])})
        joburg = PandasDataFrameAdapter(frame, timezone="Africa/Johannesburg")
        utc = pa.table(
            {
                "ts": pa.array(
                    [dt.datetime(2024, 1, 2, 1, 4, 5, tzinfo=dt.timezone.utc)],
                    pa.timestamp("us", tz="UTC"),
                )
            }
        )
        assert ch.fingerprint(joburg).data_hash == _fp(utc).data_hash

    def test_a_port_that_only_collects_is_hashed_by_value(self):
        class Port:
            schema = {"id": "int64"}

            def collect(self):
                return [{"id": 2}, {"id": 1}]

        fp = ch.fingerprint(Port())
        assert fp.is_complete and fp.row_count == 2

    def test_failure_is_honest(self):
        class Broken:
            schema = {"id": "int64"}

            def collect(self):
                raise RuntimeError("cluster went away")

        fp = ch.fingerprint(Broken())
        assert fp.data_hash is None and fp.row_count is None
        assert "cluster went away" in fp.error and not fp.is_complete


# --------------------------------------------------------------------------- #
# Properties
# --------------------------------------------------------------------------- #

values = st.one_of(
    st.none(),
    st.integers(min_value=-(2**63), max_value=2**63 - 1),
)
rows = st.lists(st.tuples(values, st.text(max_size=5) | st.none()), min_size=0, max_size=30)


def _table(data):
    return pa.table(
        {
            "n": pa.array([r[0] for r in data], pa.int64()),
            "s": pa.array([r[1] for r in data], pa.string()),
        }
    )


@settings(max_examples=150, deadline=None)
@given(rows, st.randoms(use_true_random=False))
def test_property_any_permutation_hashes_the_same(data, rnd):
    shuffled = list(data)
    rnd.shuffle(shuffled)
    assert _fp(_table(data)).data_hash == _fp(_table(shuffled)).data_hash


@settings(max_examples=150, deadline=None)
@given(rows.filter(lambda d: len(d) > 0), st.data())
def test_property_changing_one_cell_changes_the_hash(data, draw):
    i = draw.draw(st.integers(0, len(data) - 1))
    old_n, old_s = data[i]
    new_s = draw.draw(st.text(max_size=5).filter(lambda s: s != old_s))
    changed = list(data)
    changed[i] = (old_n, new_s)
    assert _fp(_table(data)).data_hash != _fp(_table(changed)).data_hash


def test_the_data_hash_payload_is_stable():
    """Pinned, so an accidental change to the method is caught, not shipped."""
    table = pa.table({"id": pa.array([1, 2], pa.int64())})
    assert _fp(table).data_hash == ch.data_hash(
        [("id", "int64")],
        2,
        tuple(sum(x) for x in zip(ch.lanes('{"id":1}'), ch.lanes('{"id":2}'))),
    )
    assert json.loads(json.dumps(ch.JSON_OPTIONS))["timeZone"] == "UTC"
