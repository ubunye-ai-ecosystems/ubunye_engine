"""Unit tests for the REST API reader and writer plugins.

Tests use unittest.mock to simulate HTTP calls — no Spark or live network needed.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from ubunye.plugins.readers.rest_api import (
    RestApiReader,
    _build_session,
    _extract_records,
    _fetch_page,
    _paginate,
)
from ubunye.plugins.writers.rest_api import RestApiWriter, _post_batch

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_response(json_data, status_code=200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(
            response=resp
        )
    return resp


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------

def test_reader_requires_url():
    reader = RestApiReader()
    backend = MagicMock()
    with pytest.raises(ValueError, match="url"):
        reader.read({}, backend)


def test_writer_requires_url():
    writer = RestApiWriter()
    df = MagicMock()
    backend = MagicMock()
    with pytest.raises(ValueError, match="url"):
        writer.write(df, {}, backend)


# ---------------------------------------------------------------------------
# Session / auth building
# ---------------------------------------------------------------------------

def test_build_session_bearer():
    cfg = {"auth": {"type": "bearer", "token": "my-secret-token"}}
    session = _build_session(cfg)
    assert session.headers["Authorization"] == "Bearer my-secret-token"


def test_build_session_api_key_header():
    cfg = {"auth": {"type": "api_key_header", "header": "X-Custom-Key", "key": "abc123"}}
    session = _build_session(cfg)
    assert session.headers["X-Custom-Key"] == "abc123"


def test_build_session_basic_auth():
    from requests.auth import HTTPBasicAuth
    cfg = {"auth": {"type": "basic", "username": "user", "password": "pass"}}
    session = _build_session(cfg)
    assert isinstance(session.auth, HTTPBasicAuth)
    assert session.auth.username == "user"
    assert session.auth.password == "pass"


def test_build_session_top_level_headers():
    cfg = {"headers": {"Authorization": "Bearer tok", "X-Trace": "123"}}
    session = _build_session(cfg)
    assert session.headers["Authorization"] == "Bearer tok"
    assert session.headers["X-Trace"] == "123"


def test_build_session_no_auth():
    session = _build_session({})
    assert "Authorization" not in session.headers


# ---------------------------------------------------------------------------
# _extract_records
# ---------------------------------------------------------------------------

def test_extract_records_root_key():
    resp = {"data": [{"id": 1}, {"id": 2}], "meta": {"total": 2}}
    records = _extract_records(resp, root_key="data")
    assert records == [{"id": 1}, {"id": 2}]


def test_extract_records_list_response():
    resp = [{"id": 1}, {"id": 2}]
    records = _extract_records(resp, root_key=None)
    assert records == resp


def test_extract_records_dict_no_root_key():
    resp = {"id": 1, "name": "Alice"}
    records = _extract_records(resp, root_key=None)
    assert records == [resp]


def test_extract_records_missing_root_key_raises():
    resp = {"items": []}
    with pytest.raises(ValueError, match="data"):
        _extract_records(resp, root_key="data")


# ---------------------------------------------------------------------------
# _fetch_page — retry behaviour
# ---------------------------------------------------------------------------

def test_fetch_page_success_on_first_attempt():
    session = MagicMock()
    session.request.return_value = _mock_response({"data": []})
    result = _fetch_page(session, "https://api.test/v1", "GET", {}, None, {}, {})
    assert result == {"data": []}
    assert session.request.call_count == 1


def test_fetch_page_retries_on_429():
    session = MagicMock()
    session.request.side_effect = [
        _mock_response({}, status_code=429),
        _mock_response([{"id": 1}]),
    ]
    with patch("ubunye.plugins.readers.rest_api.time.sleep"):
        result = _fetch_page(
            session, "https://api.test/v1", "GET", {}, None,
            {"retry_on": [429], "max_retries": 3},
            {},
        )
    assert result == [{"id": 1}]
    assert session.request.call_count == 2


def test_fetch_page_raises_after_max_retries():
    session = MagicMock()
    session.request.return_value = _mock_response({}, status_code=429)
    with patch("ubunye.plugins.readers.rest_api.time.sleep"):
        with pytest.raises(requests.HTTPError):
            _fetch_page(
                session, "https://api.test/v1", "GET", {}, None,
                {"retry_on": [429], "max_retries": 2},
                {},
            )
    assert session.request.call_count == 3  # 1 initial + 2 retries


def test_fetch_page_injects_api_key_query():
    session = MagicMock()
    session.request.return_value = _mock_response([])
    auth_cfg = {"type": "api_key_query", "param": "api_key", "key": "secret"}
    _fetch_page(session, "https://api.test/v1", "GET", {}, None, {}, auth_cfg)
    _, kwargs = session.request.call_args
    assert kwargs["params"]["api_key"] == "secret"


# ---------------------------------------------------------------------------
# _paginate — pagination strategies
# ---------------------------------------------------------------------------

def test_paginate_no_config_single_request():
    cfg = {"url": "https://api.test/v1", "method": "GET"}
    session = MagicMock()
    session.request.return_value = _mock_response([{"id": 1}, {"id": 2}])

    pages = list(_paginate(cfg, session))

    assert len(pages) == 1
    assert pages[0] == [{"id": 1}, {"id": 2}]
    assert session.request.call_count == 1


def test_paginate_offset_stops_on_empty():
    cfg = {
        "url": "https://api.test/v1",
        "method": "GET",
        "pagination": {"type": "offset", "page_size": 2},
    }
    session = MagicMock()
    session.request.side_effect = [
        _mock_response([{"id": 1}, {"id": 2}]),
        _mock_response([]),  # empty page → stop
    ]

    pages = list(_paginate(cfg, session))

    assert len(pages) == 1
    assert pages[0] == [{"id": 1}, {"id": 2}]
    assert session.request.call_count == 2


def test_paginate_offset_stops_at_max_pages():
    cfg = {
        "url": "https://api.test/v1",
        "method": "GET",
        "pagination": {"type": "offset", "page_size": 2, "max_pages": 2},
    }
    session = MagicMock()
    # Always return data — max_pages should stop iteration
    session.request.return_value = _mock_response([{"id": 1}, {"id": 2}])

    pages = list(_paginate(cfg, session))

    assert len(pages) == 2
    assert session.request.call_count == 2


def test_paginate_cursor_stops_when_no_cursor():
    cfg = {
        "url": "https://api.test/v1",
        "method": "GET",
        "pagination": {
            "type": "cursor",
            "cursor_response_key": "next_cursor",
            "cursor_param": "cursor",
        },
    }
    session = MagicMock()
    session.request.side_effect = [
        _mock_response({"items": [{"id": 1}], "next_cursor": "tok123"},
                       ),
        _mock_response({"items": [{"id": 2}], "next_cursor": None}),
    ]
    # Override to use root_key
    cfg["response"] = {"root_key": "items"}

    pages = list(_paginate(cfg, session))

    assert len(pages) == 2
    assert session.request.call_count == 2


def test_paginate_next_link_stops_when_no_next():
    cfg = {
        "url": "https://api.test/v1",
        "method": "GET",
        "pagination": {"type": "next_link", "next_key": "next"},
    }
    session = MagicMock()
    session.request.side_effect = [
        _mock_response({"data": [{"id": 1}], "next": "https://api.test/v1?page=2"}),
        _mock_response({"data": [{"id": 2}], "next": None}),
    ]
    cfg["response"] = {"root_key": "data"}

    pages = list(_paginate(cfg, session))

    assert len(pages) == 2


def test_paginate_unknown_type_raises():
    cfg = {
        "url": "https://api.test/v1",
        "pagination": {"type": "invalid_type"},
    }
    session = MagicMock()
    session.request.return_value = _mock_response([])
    with pytest.raises(ValueError, match="Unknown pagination type"):
        list(_paginate(cfg, session))


# ---------------------------------------------------------------------------
# RestApiWriter — batch posting
# ---------------------------------------------------------------------------

def test_writer_posts_in_batches():
    """250 rows with batch_size=100 should result in 3 POST calls."""
    writer = RestApiWriter()

    rows = [MagicMock() for _ in range(250)]
    for r in rows:
        r.asDict.return_value = {"id": 1}

    df = MagicMock()
    df.toLocalIterator.return_value = iter(rows)

    backend = MagicMock()

    cfg = {
        "url": "https://api.test/v1/alerts",
        "batch_size": 100,
    }

    with patch("ubunye.plugins.writers.rest_api._post_batch") as mock_post:
        writer.write(df, cfg, backend)

    assert mock_post.call_count == 3
    # First two batches = 100 rows, last = 50
    sizes = [len(c.args[2]["records"]) for c in mock_post.call_args_list]
    assert sizes == [100, 100, 50]


def test_writer_raises_on_batch_failure():
    writer = RestApiWriter()

    rows = [MagicMock()]
    rows[0].asDict.return_value = {"id": 1}
    df = MagicMock()
    df.toLocalIterator.return_value = iter(rows)

    backend = MagicMock()
    cfg = {"url": "https://api.test/v1/alerts"}

    with patch(
        "ubunye.plugins.writers.rest_api._post_batch",
        side_effect=requests.HTTPError("500"),
    ):
        with pytest.raises(RuntimeError, match="failed posting"):
            writer.write(df, cfg, backend)


def test_post_batch_retries_on_500():
    session = MagicMock()
    session.post.side_effect = [
        _mock_response({}, status_code=500),
        _mock_response({}, status_code=200),
    ]
    with patch("ubunye.plugins.writers.rest_api.time.sleep"):
        _post_batch(
            session,
            "https://api.test/v1",
            {"records": []},
            {"retry_on": [500], "max_retries": 3},
            {},
        )
    assert session.post.call_count == 2
