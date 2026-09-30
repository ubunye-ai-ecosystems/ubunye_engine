"""No REST secret reaches a log line or an error message (F-050).

With ``api_key_query`` the key is in the URL. requests puts the full URL into
``HTTPError`` and ``ConnectionError`` messages, the writer logged that message,
and urllib3 logs every request line at DEBUG, so the key was written out in
clear. Every secret the config sends is now masked in both.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

pytest.importorskip("pandas")
requests = pytest.importorskip("requests")

from rest_api_server import served  # noqa: E402

from ubunye.backends.pandas_backend import PandasBackend  # noqa: E402
from ubunye.core.errors import SinkWriteError  # noqa: E402
from ubunye.plugins import rest_http  # noqa: E402
from ubunye.plugins.readers.rest_api import RestApiReader  # noqa: E402
from ubunye.plugins.writers.rest_api import RestApiWriter  # noqa: E402

KEY = "SUPERSECRET-1f9a"
TOKEN = "TOKEN-77ab-secret"
AUTHS = {
    "api_key_query": {"type": "api_key_query", "param": "api_key", "key": KEY},
    "api_key_header": {"type": "api_key_header", "header": "X-Api-Key", "key": KEY},
    "bearer": {"type": "bearer", "token": KEY},
    "basic": {"type": "basic", "username": "svc", "password": KEY},
}
FAST = {"max_retries": 1}


@pytest.fixture(autouse=True)
def no_waiting(monkeypatch):
    monkeypatch.setattr(rest_http, "BACKOFF_BASE", 0.0)


def _spark_frame():
    row = MagicMock()
    row.asDict.return_value = {"id": 1}
    df = MagicMock()
    df.toLocalIterator.return_value = iter([row])
    return df


def _assert_clean(text):
    assert KEY not in text and TOKEN not in text, text


@pytest.mark.parametrize("auth", list(AUTHS), ids=list(AUTHS))
@pytest.mark.parametrize("route", ["pandas", "spark"])
def test_a_failing_write_logs_and_raises_no_secret(auth, route, caplog):
    if route == "pandas":
        backend = PandasBackend()
        frame = backend.frame_from_records([{"id": 1}])
    else:  # a Spark frame, as the Spark backends hand it over
        backend, frame = None, _spark_frame()
    caplog.set_level(logging.DEBUG)
    with served() as api:
        api.statuses = [500] * 10
        cfg = {
            "url": f"{api.base}/sink",
            "auth": AUTHS[auth],
            "headers": {"Authorization": f"Bearer {TOKEN}"},
            "params": {"access_token": TOKEN},
            "rate_limit": FAST,
        }
        with pytest.raises(SinkWriteError) as caught:
            RestApiWriter().write(frame, cfg, backend)
    _assert_clean(str(caught.value))
    _assert_clean(caplog.text)
    assert "500" in caplog.text  # the failure is still reported


@pytest.mark.parametrize("auth", list(AUTHS), ids=list(AUTHS))
def test_a_failing_read_logs_and_raises_no_secret(auth, caplog):
    caplog.set_level(logging.DEBUG)
    with served() as api:
        api.statuses = [503] * 10
        cfg = {
            "url": f"{api.base}/all",
            "auth": AUTHS[auth],
            "params": {"access_token": TOKEN},
            "rate_limit": FAST,
        }
        with pytest.raises(requests.HTTPError) as caught:
            RestApiReader().read(cfg, PandasBackend())
    _assert_clean(str(caught.value))
    _assert_clean(caplog.text)
    assert "503" in str(caught.value)
    assert caught.value.response is not None  # the response is kept for callers


def test_an_unreachable_api_raises_no_secret(caplog):
    caplog.set_level(logging.DEBUG)
    cfg = {"url": "http://127.0.0.1:9/x", "auth": AUTHS["api_key_query"], "rate_limit": FAST}
    with pytest.raises(requests.ConnectionError) as caught:
        RestApiReader().read(cfg, PandasBackend())
    _assert_clean(str(caught.value))
    _assert_clean(caplog.text)


def test_the_masking_filter_is_removed_after_the_call():
    with served([{"id": 1}]) as api:
        RestApiReader().read({"url": f"{api.base}/all", "auth": AUTHS["bearer"]}, PandasBackend())
    assert not logging.getLogger("urllib3.connectionpool").filters
    assert not logging.getLogger("ubunye.plugins.rest_http").filters
