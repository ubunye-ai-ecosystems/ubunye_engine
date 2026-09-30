"""The REST connector reads the option names its docs print (F-049).

The docs used ``auth: {type: api_key}``, ``cursor_field`` and ``link_field``; the
code reads ``api_key_header`` / ``api_key_query``, ``cursor_response_key`` and
``next_key``. An ``api_key`` config sent no key at all, with no error, and the
pagination names were ignored (one page, then stop). The old names are now
accepted with a warning, and an unknown auth type is refused.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

pytest.importorskip("pandas")
pytest.importorskip("requests")

from rest_api_server import PAGE_SIZE, RECORDS, served  # noqa: E402

from ubunye.backends.pandas_backend import PandasBackend  # noqa: E402
from ubunye.core.errors import SinkWriteError, SourceReadError  # noqa: E402
from ubunye.plugins.readers.rest_api import RestApiReader  # noqa: E402
from ubunye.plugins.writers.rest_api import RestApiWriter  # noqa: E402


def _read(cfg):
    return RestApiReader().read(cfg, PandasBackend()).native


def test_the_documented_api_key_header_sends_the_key(caplog):
    with served([{"id": 1}]) as api:
        auth = {"type": "api_key", "header": "X-Token", "key": "k-123"}
        with caplog.at_level(logging.WARNING):
            _read({"url": f"{api.base}/all", "auth": auth})
        _, _, headers = api.seen[0]
    assert headers.get("X-Token") == "k-123"
    assert "api_key_header" in caplog.text  # says the current name


def test_the_documented_api_key_param_sends_the_key():
    with served([{"id": 1}]) as api:
        auth = {"type": "api_key", "param": "token", "key": "k-123"}
        _read({"url": f"{api.base}/all", "auth": auth})
        _, path, _ = api.seen[0]
    assert "token=k-123" in path


def test_the_writer_reads_the_old_name_too():
    backend = PandasBackend()
    frame = backend.frame_from_records([{"id": 1}])
    with served() as api:
        auth = {"type": "api_key", "header": "X-Token", "key": "k-9"}
        RestApiWriter().write(frame, {"url": f"{api.base}/s", "auth": auth}, backend)
        _, _, headers = api.seen[0]
    assert headers.get("X-Token") == "k-9"


def test_cursor_field_is_read():
    with served() as api:
        api.cursor_key = "next_page_token"
        cfg = {
            "url": f"{api.base}/cursor",
            "response": {"root_key": "data"},
            "pagination": {"type": "cursor", "cursor_field": "next_page_token"},
        }
        assert len(_read(cfg)) == len(RECORDS) > PAGE_SIZE


def test_link_field_is_read():
    with served() as api:
        api.link_key = "following"
        cfg = {
            "url": f"{api.base}/linked",
            "response": {"root_key": "data"},
            "pagination": {"type": "next_link", "link_field": "following"},
        }
        assert len(_read(cfg)) == len(RECORDS) > PAGE_SIZE


@pytest.mark.parametrize(
    "auth, problem",
    [
        ({"type": "oauth", "token": "x"}, "unknown auth type 'oauth'"),
        ({"type": "api_key", "key": "x"}, "exactly one of 'header' or 'param'"),
        ({"type": "api_key", "key": "x", "header": "H", "param": "p"}, "exactly one"),
    ],
    ids=["unknown", "api-key-neither", "api-key-both"],
)
def test_an_auth_that_would_send_nothing_is_refused_before_any_request(auth, problem, monkeypatch):
    monkeypatch.setattr("requests.Session.request", MagicMock(side_effect=AssertionError))
    cfg = {"url": "http://127.0.0.1:9/x", "auth": auth}
    assert any(problem in p for p in RestApiReader.validate_config(cfg))
    assert any(problem in p for p in RestApiWriter.validate_config(cfg))
    with pytest.raises(SourceReadError, match=problem):
        RestApiReader().read(cfg, PandasBackend())
    with pytest.raises(SinkWriteError, match=problem):
        RestApiWriter().write(MagicMock(), cfg, PandasBackend())
