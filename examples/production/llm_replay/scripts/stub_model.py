"""A stub OpenAI-compatible model server for recording the example's replay file.

It is not a language model: it answers "positive" or "negative" by looking for a
few words in the review. It exists so the example's committed answers can be
recorded with no key and no cost. Run ``record.py`` to use it.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

NEGATIVE = ("broke", "terrible", "worst", "poor", "not as shown")


def label(text: str) -> str:
    return "negative" if any(word in text.lower() for word in NEGATIVE) else "positive"


class _Handler(BaseHTTPRequestHandler):
    def do_POST(self):  # noqa: N802 (http.server's name)
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        prompt = body["messages"][-1]["content"]
        answer = {
            "model": body.get("model", "stub-labeller"),
            "choices": [
                {
                    "message": {"role": "assistant", "content": label(prompt)},
                    "finish_reason": "stop",
                }
            ],
            # Word counts stand in for tokens: the stub has no tokenizer.
            "usage": {"prompt_tokens": len(prompt.split()), "completion_tokens": 1},
        }
        data = json.dumps(answer).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


def serve() -> ThreadingHTTPServer:
    """Start the stub on a free local port, in a background thread."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server
