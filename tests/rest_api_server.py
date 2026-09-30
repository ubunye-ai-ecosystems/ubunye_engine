"""A local JSON API for the rest_api connector's tests (F-015). No internet.

It serves one list of records three ways (offset, cursor and next-link
pagination) and records every POST body, so the reader and the writer can be
checked on each backend against the same data.

The records are chosen to hit Spark's inference rules: a key that appears only
on the last page (added at the end), nested objects (maps, whose entry order
Spark does not keep stable), a list, nulls in every kind of field, and
a field that is a number in some records and text in others (``string``).
"""

from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import parse_qs, urlparse

PAGE_SIZE = 3

RECORDS: List[Dict[str, Any]] = [
    {
        "id": 1,
        "name": "Thandi",
        "score": 81.5,
        "code": 7,
        "active": True,
        "address": {"zip": "0152", "city": "Soshanguve", "country": "ZA", "area": "Ext 5"},
        "tags": ["new", "vip"],
        "meta": {"first": {"a": 1}, "last": {"b": 2}},
    },
    {
        "id": 2,
        "name": None,
        "score": 64.25,
        "code": "A7",
        "active": False,
        "address": {"city": "Cape Town", "zip": None},
        "tags": [],
        "meta": {"last": {}},
    },
    {
        "id": 3,
        "name": "Sipho",
        "score": None,
        "code": 12,
        "active": None,
        "address": None,
        "tags": None,
        "meta": None,
    },
    {
        "id": 4,
        "name": "Lerato é 日本",
        "score": 0.1,
        "code": None,
        "active": True,
        "address": {"country": "LS"},
        "tags": ["x", None],
        "meta": {"first": {"a": None, "b": 3}},
    },
    {
        "id": 5,
        "name": "",
        "score": -2.0,
        "code": "0012",
        "active": False,
        "address": {"street": "1 Main", "ward": "12", "town": "Tshwane", "geo": "x"},
        "tags": ["z"],
        "meta": {},
        "late": "only here",
    },
]


class _Api(BaseHTTPRequestHandler):
    records: List[Dict[str, Any]] = RECORDS
    posted: List[Any] = []
    base: str = ""
    raw: str = "[]"

    def _send(self, body: Any) -> None:
        data = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):  # noqa: N802
        url = urlparse(self.path)
        query = {k: v[0] for k, v in parse_qs(url.query).items()}
        records = type(self).records
        if url.path == "/offset":
            start = int(query.get("offset", 0))
            return self._send({"data": records[start : start + PAGE_SIZE]})
        if url.path in ("/cursor", "/linked"):
            start = int(query.get("cursor", query.get("from", 0)))
            nxt = start + PAGE_SIZE
            more = nxt < len(records)
            body: Dict[str, Any] = {"data": records[start:nxt]}
            if url.path == "/cursor":
                body["next_cursor"] = str(nxt) if more else None
            else:
                body["next"] = f"{type(self).base}/linked?from={nxt}" if more else None
            return self._send(body)
        if url.path == "/all":
            return self._send(records)
        if url.path == "/empty":
            return self._send({"data": []})
        if url.path == "/raw":  # a body given as text, as a test set it
            data = type(self).raw.encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return None
        self.send_error(404)

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length", 0))
        type(self).posted.append(json.loads(self.rfile.read(length)))
        self._send({"ok": True})

    def log_message(self, *args):
        pass


@contextmanager
def served(records: Optional[List[Dict[str, Any]]] = None) -> Iterator[Any]:
    """A running API; yields an object with ``url`` and ``posted`` (POST bodies)."""
    handler = type("Api", (_Api,), {"records": records or RECORDS, "posted": []})
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    handler.base = f"http://127.0.0.1:{server.server_address[1]}"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield handler
    finally:
        server.shutdown()
        server.server_close()


def read_cfg(base: str, how: str = "offset") -> Dict[str, Any]:
    """A rest_api input that reads every record with the given pagination."""
    cfg: Dict[str, Any] = {"format": "rest_api", "response": {"root_key": "data"}}
    if how == "offset":
        cfg.update(url=f"{base}/offset", pagination={"type": "offset", "page_size": PAGE_SIZE})
    elif how == "cursor":
        cfg.update(url=f"{base}/cursor", pagination={"type": "cursor"})
    elif how == "next_link":
        cfg.update(url=f"{base}/linked", pagination={"type": "next_link"})
    else:
        raise ValueError(how)
    return cfg
