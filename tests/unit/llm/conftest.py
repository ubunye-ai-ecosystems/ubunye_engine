"""A local HTTP server that answers in each provider's wire format, for the LLM tests."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List

import pytest


class FakeProvider:
    """Records every request; answers from a queue of (status, body, headers)."""

    def __init__(self) -> None:
        self.requests: List[Dict[str, Any]] = []
        self.answers: List[tuple] = []
        self.lock = threading.Lock()

    def answer(self, body: Dict[str, Any], status: int = 200, headers=None) -> None:
        self.answers.append((status, body, headers or {}))


def _server(provider: FakeProvider):
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802 (http.server's name)
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            with provider.lock:
                provider.requests.append(
                    {
                        "path": self.path,
                        "headers": {k.lower(): v for k, v in self.headers.items()},
                        "body": body,
                    }
                )
                status, answer, headers = (
                    provider.answers.pop(0) if provider.answers else (200, None, {})
                )
            if answer is None:  # echo: the last user message back, in the path's format
                answer = _echo(self.path, body)
            data = json.dumps(answer).encode("utf-8")
            self.send_response(status)
            for key, value in headers.items():
                self.send_header(key, value)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def _echo(path: str, body: Dict[str, Any]) -> Dict[str, Any]:
    text = "echo: " + body["messages"][-1]["content"]
    if path.endswith("/v1/messages"):
        return {
            "type": "message",
            "model": body["model"],
            "content": [{"type": "text", "text": text}],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 11, "output_tokens": 4},
        }
    return {
        "model": body.get("model", "endpoint-model"),
        "choices": [{"message": {"role": "assistant", "content": text}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 11, "completion_tokens": 4},
    }


@pytest.fixture
def provider():
    fake = FakeProvider()
    server = _server(fake)
    fake.url = f"http://127.0.0.1:{server.server_address[1]}"
    yield fake
    server.shutdown()


@pytest.fixture(autouse=True)
def _no_keys_from_the_machine(monkeypatch):
    for name in ("ANTHROPIC_API_KEY", "OPENAI_API_KEY", "DATABRICKS_TOKEN", "DATABRICKS_HOST"):
        monkeypatch.delenv(name, raising=False)
