"""The LLM port: one way for a task to call a language model, seen by the engine.

Each adapter is checked against a local HTTP server that answers in the
provider's own wire format, so no test spends money or needs a network. One live
call per adapter runs in ubunye-infra, within the sandbox budgets.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List

import pytest

import ubunye
from ubunye import llm
from ubunye.core.errors import LLMError, PluginNotFoundError
from ubunye.lineage.storage import FileSystemLineageStore

# --- a local server that speaks each provider's format --------------------------------


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


# --- the three adapters -------------------------------------------------------------


def test_anthropic_sends_the_messages_api_and_reads_the_answer(provider):
    port = llm.port("anthropic", model="claude-x", api_key="k-1", base_url=provider.url)
    answer = port.complete("Say hi", system="Be brief", max_tokens=50, temperature=0)
    assert answer.text == "echo: Say hi"
    assert (answer.input_tokens, answer.output_tokens) == (11, 4)
    assert answer.stop_reason == "end_turn"
    (sent,) = provider.requests
    assert sent["path"] == "/v1/messages"
    assert sent["headers"]["x-api-key"] == "k-1"
    assert sent["headers"]["anthropic-version"] == "2023-06-01"
    assert sent["body"] == {
        "model": "claude-x",
        "messages": [{"role": "user", "content": "Say hi"}],
        "system": "Be brief",
        "max_tokens": 50,
        "temperature": 0,
    }


def test_openai_compatible_sends_chat_completions(provider):
    port = llm.port("openai_compatible", model="m", api_key="k-2", base_url=provider.url + "/v1")
    answer = port.complete("Say hi", system="Be brief", max_tokens=50)
    assert answer.text == "echo: Say hi"
    assert (answer.input_tokens, answer.output_tokens) == (11, 4)
    (sent,) = provider.requests
    assert sent["path"] == "/v1/chat/completions"
    assert sent["headers"]["authorization"] == "Bearer k-2"
    assert sent["body"]["messages"] == [
        {"role": "system", "content": "Be brief"},
        {"role": "user", "content": "Say hi"},
    ]
    assert sent["body"]["model"] == "m" and sent["body"]["max_tokens"] == 50


def test_openai_compatible_needs_no_key_for_a_local_server(provider):
    port = llm.port("openai_compatible", model="llama", base_url=provider.url + "/v1")
    port.complete("hi")
    assert "authorization" not in provider.requests[0]["headers"]


def test_databricks_serving_calls_the_endpoint_with_the_workspace_token(provider, monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", provider.url)
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-3")
    port = llm.port("databricks_serving", model="my-endpoint")
    assert port.complete("Say hi").text == "echo: Say hi"
    (sent,) = provider.requests
    assert sent["path"] == "/serving-endpoints/my-endpoint/invocations"
    assert sent["headers"]["authorization"] == "Bearer dapi-3"
    assert "model" not in sent["body"]  # the endpoint is the model


def test_the_key_can_be_a_secret_reference(provider, monkeypatch):
    monkeypatch.setenv("MY_KEY", "from-env")
    port = llm.port("anthropic", model="c", api_key="secret://env/MY_KEY", base_url=provider.url)
    port.complete("hi")
    assert provider.requests[0]["headers"]["x-api-key"] == "from-env"


def test_the_key_is_read_from_the_providers_usual_variable(provider, monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "usual")
    llm.port("anthropic", model="c", base_url=provider.url).complete("hi")
    assert provider.requests[0]["headers"]["x-api-key"] == "usual"


def test_a_missing_key_fails_before_any_call(provider):
    with pytest.raises(LLMError, match="ANTHROPIC_API_KEY"):
        llm.port("anthropic", model="c", base_url=provider.url)
    assert provider.requests == []


def test_an_unknown_backend_names_the_installed_ones():
    with pytest.raises(PluginNotFoundError, match="anthropic"):
        llm.port("nope", model="x")


# --- failures and retries -----------------------------------------------------------


def test_rate_limits_and_server_errors_are_retried(provider):
    provider.answer({"error": "slow down"}, status=429, headers={"retry-after": "0"})
    provider.answer({"error": "boom"}, status=503)
    port = llm.port("anthropic", model="c", api_key="k", base_url=provider.url, backoff_s=0)
    assert port.complete("hi").text == "echo: hi"
    assert len(provider.requests) == 3


def test_a_refused_request_is_not_retried_and_says_why(provider):
    provider.answer({"error": {"message": "invalid x-api-key"}}, status=401)
    port = llm.port("anthropic", model="c", api_key="bad", base_url=provider.url, backoff_s=0)
    with pytest.raises(LLMError, match="401") as err:
        port.complete("hi")
    assert "invalid x-api-key" in str(err.value)
    assert "bad" not in str(err.value)  # the key is never shown
    assert len(provider.requests) == 1


def test_retries_stop_after_the_limit(provider):
    for _ in range(5):
        provider.answer({"error": "busy"}, status=529)
    port = llm.port(
        "anthropic", model="c", api_key="k", base_url=provider.url, retries=2, backoff_s=0
    )
    with pytest.raises(LLMError, match="529"):
        port.complete("hi")
    assert len(provider.requests) == 3


# --- many prompts, the request key, the call log -------------------------------------


def test_complete_many_keeps_the_order_of_the_prompts(provider):
    port = llm.port("anthropic", model="c", api_key="k", base_url=provider.url)
    prompts = [f"p{i}" for i in range(20)]
    answers = port.complete_many(prompts, max_concurrency=5)
    assert [a.text for a in answers] == [f"echo: p{i}" for i in range(20)]


def test_the_request_key_is_stable_and_covers_what_changes_the_answer():
    base = llm.LLMRequest(model="m", messages=[{"role": "user", "content": "hi"}])
    same = llm.LLMRequest(model="m", messages=[{"role": "user", "content": "hi"}])
    assert base.key("anthropic") == same.key("anthropic")
    assert base.key("anthropic").startswith("sha256:")
    for other in (
        llm.LLMRequest(model="m2", messages=base.messages),
        llm.LLMRequest(model="m", messages=[{"role": "user", "content": "hi!"}]),
        llm.LLMRequest(model="m", messages=base.messages, temperature=0.5),
        llm.LLMRequest(model="m", messages=base.messages, system="s"),
    ):
        assert other.key("anthropic") != base.key("anthropic")
    assert base.key("anthropic") != base.key("openai_compatible")


def test_calls_are_logged_without_the_prompt_or_the_answer(provider):
    port = llm.port("anthropic", model="c", api_key="k", base_url=provider.url)
    with llm.recording() as calls:
        port.complete("a secret prompt")
    (call,) = calls
    assert call["backend"] == "anthropic" and call["model"] == "c"
    assert call["status"] == "ok"
    assert (call["input_tokens"], call["output_tokens"]) == (11, 4)
    assert call["request_key"].startswith("sha256:")
    assert call["seconds"] >= 0
    assert "a secret prompt" not in json.dumps(call)


def test_a_failed_call_is_logged_too(provider):
    provider.answer({"error": "no"}, status=400)
    port = llm.port("anthropic", model="c", api_key="k", base_url=provider.url)
    with llm.recording() as calls:
        with pytest.raises(LLMError):
            port.complete("hi")
    assert [c["status"] for c in calls] == ["error"]


def test_outside_a_recording_calls_still_work(provider):
    port = llm.port("anthropic", model="c", api_key="k", base_url=provider.url)
    assert port.complete("hi").text == "echo: hi"


# --- through the engine: the run record carries the calls ----------------------------

TRANSFORM = """\
from ubunye import llm
from ubunye.core.interfaces import Task


class Label(Task):
    def setup(self):
        p = self.config["CONFIG"]["transform"]["params"]
        self.model = llm.port("anthropic", model="c", api_key="k", base_url=p["url"])

    def transform(self, sources):
        df = sources["raw"].copy()
        df["label"] = [a.text for a in self.model.complete_many(list(df["text"]))]
        return {"labelled": df}
"""


def _task(root: Path, url: str) -> Path:
    task = root / "uc" / "pkg" / "label"
    task.mkdir(parents=True)
    (root / "raw.csv").write_text("id,text\n1,good\n2,bad\n", encoding="utf-8")
    (task / "transformations.py").write_text(TRANSFORM, encoding="utf-8")
    (task / "config.yaml").write_text(
        f"""\
CONFIG:
  inputs:
    raw: {{format: s3, path: "{(root / 'raw.csv').as_posix()}", file_format: csv,
           options: {{header: "true"}}}}
  transform:
    params: {{url: "{url}"}}
  outputs:
    labelled: {{format: s3, path: "{(root / 'out').as_posix()}", file_format: parquet,
                mode: overwrite}}
""",
        encoding="utf-8",
    )
    return task


def test_the_run_record_lists_every_model_call(tmp_path, provider):
    import pandas as pd

    task = _task(tmp_path, provider.url)
    lineage = tmp_path / "lineage"
    ubunye.run_task(str(task), backend="pandas", lineage=True, lineage_dir=str(lineage))
    assert list(pd.read_parquet(tmp_path / "out")["label"]) == ["echo: good", "echo: bad"]
    (record,) = FileSystemLineageStore(str(lineage)).list_runs("uc/pkg/label")
    assert len(record.llm_calls) == 2
    assert {c["status"] for c in record.llm_calls} == {"ok"}
    assert sum(c["input_tokens"] for c in record.llm_calls) == 22
    again = type(record).from_dict(json.loads(json.dumps(record.to_dict())))
    assert again.llm_calls == record.llm_calls

    from typer.testing import CliRunner

    from ubunye.cli.main import app

    shown = CliRunner().invoke(
        app,
        ["lineage", "trace", "-d", str(tmp_path), "-u", "uc", "-p", "pkg", "-t", "label",
         "--lineage-dir", str(lineage)],
    )  # fmt: skip
    assert shown.exit_code == 0, shown.output
    assert "anthropic/c: 2 calls, 0 failed, 22 tokens in, 8 out" in shown.output
