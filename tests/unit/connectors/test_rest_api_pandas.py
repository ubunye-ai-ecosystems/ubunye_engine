"""The rest_api connector on the pandas backend (F-015).

Pulling JSON from an HTTP API was refused on pandas ("needs spark"), though only
the last step, records to a frame, has anything to do with the engine. These
tests read and write through a local HTTP server on the pandas backend. The
same data is checked against live Spark in
``tests/integration/test_rest_api_parity.py``.

``tests/`` is on ``sys.path`` (its conftest puts it there), so the server
module imports by name.
"""

from __future__ import annotations

import textwrap
from unittest.mock import MagicMock

import pytest

pd = pytest.importorskip("pandas")
pa = pytest.importorskip("pyarrow")
pytest.importorskip("requests")

from rest_api_server import RECORDS, read_cfg, served  # noqa: E402

from ubunye.backends.pandas_backend import PandasBackend  # noqa: E402
from ubunye.core.capabilities import PATH_IO, Capabilities, check_task  # noqa: E402
from ubunye.core.errors import SinkWriteError, SourceReadError  # noqa: E402
from ubunye.plugins.readers.rest_api import RestApiReader  # noqa: E402
from ubunye.plugins.writers.rest_api import RestApiWriter  # noqa: E402

COLUMNS = ["active", "address", "code", "id", "meta", "name", "score", "tags", "late"]


def _read(cfg):
    return RestApiReader().read(cfg, PandasBackend()).native


# --------------------------------------------------------------------------- #
# Before a run: the capability check
# --------------------------------------------------------------------------- #


def test_plan_accepts_rest_api_on_pandas():
    """Failed before the fix: "the 'rest_api' connector, which needs spark"."""
    registry = MagicMock()
    registry.readers = {"rest_api": RestApiReader}
    registry.writers = {"rest_api": RestApiWriter}
    cfg = {
        "CONFIG": {
            "inputs": {"api": {"format": "rest_api", "url": "http://x"}},
            "outputs": {"sink": {"format": "rest_api", "url": "http://x"}},
        }
    }
    problems = check_task(PandasBackend.CAPABILITIES, cfg, registry, backend_name="pandas")
    assert problems == []


def test_a_backend_without_records_is_refused_before_any_request(monkeypatch):
    monkeypatch.setattr("requests.Session.request", MagicMock(side_effect=AssertionError))

    class PathsOnly:
        def capabilities(self):
            return Capabilities(features=frozenset({PATH_IO}))

    with pytest.raises(SourceReadError, match="builds frames from records"):
        RestApiReader().read({"url": "http://127.0.0.1:9/x"}, PathsOnly())
    with pytest.raises(SinkWriteError, match="builds frames from records"):
        RestApiWriter().write(MagicMock(), {"url": "http://127.0.0.1:9/x"}, PathsOnly())


# --------------------------------------------------------------------------- #
# Reading: Spark's createDataFrame rules
# --------------------------------------------------------------------------- #


def test_read_on_pandas_types_as_spark_does():
    with served() as api:
        frame = _read(read_cfg(api.base))

    # Each record's keys sorted; a key first seen on a later page goes last.
    assert list(frame.columns) == COLUMNS
    kinds = {c: frame[c].dtype.pyarrow_dtype for c in frame.columns}
    assert kinds["id"] == pa.int64()
    assert kinds["score"] == pa.float64()
    assert kinds["active"] == pa.bool_()
    assert kinds["code"] == pa.string()  # a number in some records, text in others
    assert kinds["address"] == pa.map_(pa.string(), pa.string())  # an object is a map
    assert kinds["tags"] == pa.list_(pa.string())
    assert kinds["meta"] == pa.map_(pa.string(), pa.map_(pa.string(), pa.int64()))

    assert frame["id"].tolist() == [1, 2, 3, 4, 5]
    assert frame["code"].tolist()[:3] == ["7", "A7", "12"]
    assert frame["code"].isna().tolist() == [False, False, False, True, False]
    assert frame["name"].isna().tolist() == [False, True, False, False, False]
    assert frame["late"].isna().tolist() == [True, True, True, True, False]
    # A map keeps the JSON's entry order. (Spark's is not stable: its JVM decides.)
    assert [k for k, _ in frame["address"][0]] == ["zip", "city", "country", "area"]
    assert [k for k, _ in frame["meta"][0]] == ["first", "last"]


@pytest.mark.parametrize("how", ["cursor", "next_link"])
def test_every_pagination_gives_the_same_frame(how):
    with served() as api:
        by_offset = _read(read_cfg(api.base, "offset"))
        other = _read(read_cfg(api.base, how))
    pd.testing.assert_frame_equal(by_offset, other)


def test_an_explicit_schema_selects_and_types():
    cfg_schema = [
        {"name": "id", "type": "integer"},
        {"name": "code", "type": "string"},
        {"name": "active", "type": "boolean"},
        {"name": "missing", "type": "double"},
    ]
    with served() as api:
        frame = _read({**read_cfg(api.base), "schema": cfg_schema})
    assert list(frame.columns) == ["id", "code", "active", "missing"]
    assert frame["id"].dtype.pyarrow_dtype == pa.int32()
    assert frame["code"].tolist()[:3] == ["7", "A7", "12"]
    assert frame["active"].tolist()[:2] == [True, False]
    assert frame["missing"].isna().all()


def test_whole_numbers_in_a_double_column_read_as_decimals():
    """Refused before (Spark's check takes no int for a double); now 1 is 1.0."""
    schema = [{"name": "price", "type": "double"}, {"name": "rate", "type": "float"}]
    with served([{"price": 1, "rate": 2}, {"price": 2.5, "rate": None}]) as api:
        frame = _read({"url": f"{api.base}/all", "schema": schema})
    assert frame["price"].tolist() == [1.0, 2.5]
    assert frame["rate"].tolist()[0] == 2.0


def test_a_whole_number_a_double_cannot_hold_is_still_refused():
    big = 2**53 + 1  # float(big) != big: converting would change the value
    with served([{"price": big}]) as api:
        cfg = {"url": f"{api.base}/all", "schema": [{"name": "price", "type": "double"}]}
        with pytest.raises(SourceReadError, match="can not accept object"):
            _read(cfg)


@pytest.mark.parametrize(
    "records, error",
    [
        ([{"price": 1}, {"price": 2.5}], "CANNOT_MERGE_TYPE"),
        ([{"id": 1, "note": None}, {"id": 2, "note": None}], "CANNOT_DETERMINE_TYPE"),
        ([{"flag": True}, {"flag": 1}], "CANNOT_MERGE_TYPE"),
        ([{"x": {"a": 1}}, {"x": 5}], "CANNOT_MERGE_TYPE"),
    ],
    ids=["long-and-double", "null-everywhere", "bool-and-long", "object-and-number"],
)
def test_records_spark_cannot_type_are_refused(records, error):
    with served(records) as api:
        with pytest.raises(SourceReadError, match=error) as caught:
            _read({"url": f"{api.base}/all"})
    assert "schema" in str(caught.value)  # the hint says what to do


@pytest.mark.parametrize(
    "body",
    [
        '[{"id": 1, "big": 9223372036854775808}]',
        '[{"id": 1, "big": [1, -9223372036854775809]}]',
        '[{"id": 1, "big": {"k": 99999999999999999999}}]',
    ],
    ids=["field", "in-array", "in-map"],
)
def test_a_whole_number_past_64_bits_is_refused_by_name(body):
    """Spark classic stores null; pandas raised a bare OverflowError (F-015 review)."""
    with served() as api:
        api.raw = body
        with pytest.raises(SourceReadError, match="field big") as caught:
            _read({"url": f"{api.base}/raw"})
    assert "string" in str(caught.value)


def test_a_whole_number_past_64_bits_reads_as_string():
    with served() as api:
        api.raw = '[{"big": 9223372036854775808}]'
        cfg = {"url": f"{api.base}/raw", "schema": [{"name": "big", "type": "string"}]}
        assert _read(cfg)["big"].tolist() == ["9223372036854775808"]


def test_records_with_no_fields_are_rows_as_on_spark():
    """Spark gives one struct<> row per {}; pandas gave none (F-015 review)."""
    from ubunye.lineage.content_hash import fingerprint

    backend = PandasBackend()
    with served([{}, {}, {}]) as api:
        frame = RestApiReader().read({"url": f"{api.base}/all"}, backend)
        RestApiWriter().write(frame, {"url": f"{api.base}/sink"}, backend)
        posted = api.posted
    assert frame.native.shape == (3, 0)
    assert frame.count() == 3
    assert fingerprint(frame).row_count == 3
    assert posted == [{"records": [{}, {}, {}]}]


def test_a_null_everywhere_column_reads_with_a_schema():
    with served([{"id": 1, "note": None}]) as api:
        cfg = {"url": f"{api.base}/all", "schema": [{"name": "note", "type": "string"}]}
        assert _read(cfg)["note"].isna().all()


def test_no_records_give_an_empty_frame():
    with served() as api:
        frame = _read({"url": f"{api.base}/empty", "response": {"root_key": "data"}})
    assert frame.shape == (0, 0)


def test_an_unknown_schema_type_is_refused_before_any_request(monkeypatch):
    monkeypatch.setattr("requests.Session.request", MagicMock(side_effect=AssertionError))
    cfg = {"url": "http://127.0.0.1:9/x", "schema": [{"name": "a", "type": "uuid"}]}
    with pytest.raises(SourceReadError, match="Unsupported schema type 'uuid'"):
        RestApiReader().read(cfg, PandasBackend())


# --------------------------------------------------------------------------- #
# Writing: rows back as records
# --------------------------------------------------------------------------- #


def test_write_on_pandas_posts_every_row_in_batches():
    backend = PandasBackend()
    with served() as api:
        frame = RestApiReader().read(read_cfg(api.base), backend)
        RestApiWriter().write(frame, {"url": f"{api.base}/sink", "batch_size": 2}, backend)
        posted = api.posted

    assert [len(body["records"]) for body in posted] == [2, 2, 1]
    rows = [row for body in posted for row in body["records"]]
    assert [list(r) for r in rows] == [COLUMNS] * 5
    first = rows[0]
    assert first["code"] == "7"
    assert first["address"] == RECORDS[0]["address"]  # a map is a dict again
    assert list(first["meta"]) == ["first", "last"]  # the JSON's order
    assert rows[1]["tags"] == [] and rows[2]["tags"] is None


def test_write_takes_a_plain_pandas_frame():
    backend = PandasBackend()
    frame = pd.DataFrame({"id": [1, 2], "who": ["a", None]})
    with served() as api:
        RestApiWriter().write(frame, {"url": f"{api.base}/sink"}, backend)
        posted = api.posted
    assert posted == [{"records": [{"id": 1, "who": "a"}, {"id": 2, "who": None}]}]


# --------------------------------------------------------------------------- #
# Through the engine
# --------------------------------------------------------------------------- #


def test_a_task_reads_and_writes_rest_on_pandas(tmp_path):
    from ubunye.api import run_task

    with served() as api:
        task = tmp_path / "uc" / "pkg" / "pull"
        task.mkdir(parents=True)
        (task / "transformations.py").write_text(textwrap.dedent("""
                from ubunye.core.interfaces import Task


                class Pull(Task):
                    def transform(self, sources):
                        people = sources["people"]
                        return {"sink": people[["id", "code"]]}
                """))
        (task / "config.yaml").write_text(textwrap.dedent(f"""
                MODEL: etl
                VERSION: "1.0.0"
                CONFIG:
                  inputs:
                    people:
                      format: rest_api
                      url: "{api.base}/offset"
                      pagination: {{type: offset, page_size: 3}}
                      response: {{root_key: data}}
                  transform: {{}}
                  outputs:
                    sink:
                      format: rest_api
                      url: "{api.base}/sink"
                """))
        run_task(str(task), backend="pandas")
        posted = api.posted

    rows = [r for body in posted for r in body["records"]]
    assert rows == [
        {"id": r["id"], "code": c} for r, c in zip(RECORDS, ["7", "A7", "12", None, "0012"])
    ]
