"""The REST writer sends NaN, timestamps, decimals and dates safely (F-051).

requests refuses NaN (``Out of range float values are not JSON compliant``) and
cannot encode a datetime or a Decimal (``TypeError``), so the writer crashed on
the first such row, after earlier batches were already posted. Every value now
has one JSON form, the same from every backend, and a batch is encoded whole
before any of it is sent.
"""

from __future__ import annotations

import datetime as dt
import decimal
from unittest.mock import MagicMock

import pytest

pd = pytest.importorskip("pandas")
pa = pytest.importorskip("pyarrow")
pytest.importorskip("requests")

from rest_api_server import served  # noqa: E402

from ubunye.backends.pandas_backend import PandasBackend  # noqa: E402
from ubunye.core.errors import SinkWriteError  # noqa: E402
from ubunye.plugins import rest_http  # noqa: E402
from ubunye.plugins.writers.rest_api import RestApiWriter  # noqa: E402

UTC = dt.timezone.utc
INSTANT = dt.datetime(2024, 1, 2, 1, 4, 5, 123456, tzinfo=UTC)
EXPECTED = {
    "x": None,  # NaN
    "inf": None,
    "ts": "2024-01-02T01:04:05.123456Z",
    # A naive pandas timestamp is an instant in the backend's zone (UTC here), as
    # everywhere on the pandas backend, so it is sent as an instant too.
    "naive": "2024-01-02T01:04:05Z",
    "day": "2024-01-02",
    "dec": "12.50",
    "bin": "AAE=",
    "m": {"a": None, "b": 1.5},
}


def _table():
    return pa.table(
        {
            "x": pa.array([float("nan")]),
            "inf": pa.array([float("inf")]),
            "ts": pa.array([INSTANT], pa.timestamp("us", tz="UTC")),
            "naive": pa.array([dt.datetime(2024, 1, 2, 1, 4, 5)], pa.timestamp("us")),
            "day": pa.array([dt.date(2024, 1, 2)]),
            "dec": pa.array([decimal.Decimal("12.50")], pa.decimal128(10, 2)),
            "bin": pa.array([b"\x00\x01"]),
            "m": pa.array([[("a", float("nan")), ("b", 1.5)]], pa.map_(pa.string(), pa.float64())),
        }
    )


def test_every_value_is_sent_in_one_json_form():
    backend = PandasBackend()
    frame = _table().to_pandas(types_mapper=pd.ArrowDtype)
    with served() as api:
        RestApiWriter().write(frame, {"url": f"{api.base}/s"}, backend)
        posted = api.posted
    assert posted == [{"records": [EXPECTED]}]


def test_a_spark_frame_sends_the_same(monkeypatch):
    # PySpark gives a timestamp as naive local time; frame_io makes it UTC first.
    from ubunye.adapters.spark import frame_io

    types = pytest.importorskip("pyspark.sql.types")
    schema = types.StructType(
        [
            types.StructField("ts", types.TimestampType()),
            types.StructField("l", types.ArrayType(types.TimestampType())),
        ]
    )
    local = INSTANT.astimezone().replace(tzinfo=None)  # what PySpark's collect gives
    row = MagicMock()
    row.asDict.return_value = {"ts": local, "l": [local, None]}
    df = MagicMock()
    df.schema = schema
    df.toLocalIterator.return_value = iter([row])
    (record,) = list(frame_io.iter_records(df))
    assert rest_http.jsonable(record) == {
        "ts": EXPECTED["ts"],
        "l": [EXPECTED["ts"], None],
    }


def _spark_like_frame(rows):
    out = []
    for r in rows:
        row = MagicMock()
        row.asDict.return_value = r
        out.append(row)
    df = MagicMock()
    df.toLocalIterator.return_value = iter(out)
    return df


def test_a_batch_with_a_row_that_cannot_be_sent_is_not_posted_at_all():
    rows = [{"id": 1}, {"id": 2}, {"id": 3, "obj": object()}, {"id": 4}]
    with served() as api:
        with pytest.raises(SinkWriteError, match="row 0 of the batch, field 'obj'") as caught:
            RestApiWriter().write(
                _spark_like_frame(rows), {"url": f"{api.base}/s", "batch_size": 2}, None
            )
        posted, seen = api.posted, api.seen
    # The first batch went; the second, holding the bad row, sent nothing.
    assert posted == [{"records": [{"id": 1}, {"id": 2}]}]
    assert len(seen) == 1
    assert "1 earlier batch" in str(caught.value)


def test_json_body_refuses_nan_nowhere():
    body = rest_http.json_body([{"x": float("nan"), "n": [float("-inf")]}])
    assert body == b'{"records": [{"x": null, "n": [null]}]}'


def test_wall_clock_time_is_sent_without_an_offset():
    # Spark's timestamp_ntz comes back naive and is not an instant.
    assert rest_http.jsonable(dt.datetime(2024, 1, 2, 1, 4, 5)) == "2024-01-02T01:04:05"
