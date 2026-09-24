"""One way for a task to call a language model, seen by the engine.

A task asks for a port and calls it::

    from ubunye import llm

    class Label(Task):
        def setup(self):
            self.model = llm.port("anthropic", model="claude-haiku-4-5-20251001")

        def transform(self, sources):
            df = sources["reviews"]
            df["label"] = [a.text for a in self.model.complete_many(list(df["text"]))]
            return {"labelled": df}

Every call made during a run is written into the run record (``llm_calls``):
backend, model, tokens, seconds, status and a hash of the request, never the
prompt or the answer. Backends are plugins in the ``ubunye.llm_backends``
entry-point group; ``anthropic``, ``openai_compatible`` (OpenAI, Azure OpenAI,
vLLM, Ollama, LiteLLM and any server with ``/chat/completions``) and
``databricks_serving`` ship with the engine and need no extra packages.

Calls run where the task's Python runs: on Spark that is the driver, so collect
the column to label, or keep the frame small.

A port has a mode (``mode=`` or ``UBUNYE_LLM_MODE``): ``live`` calls the provider;
``record`` calls it and keeps each answer in a replay file; ``replay`` answers from
that file, needs no key and no network, and fails on a request it has no answer
for. See :mod:`ubunye.llm.replay`.
"""

from __future__ import annotations

import contextvars
import hashlib
import json
import logging
import os
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple, Union

from ubunye.core.errors import LLMError

log = logging.getLogger(__name__)

__all__ = [
    "LLMBackend",
    "LLMPort",
    "LLMRequest",
    "LLMResponse",
    "port",
    "recording",
]

Messages = List[Dict[str, Any]]

#: HTTP statuses worth another try: rate limits, overload, and server errors.
RETRY_STATUSES = frozenset({408, 409, 429, 500, 502, 503, 504, 529})
#: live: call the provider; record: call it and keep the answer; replay: answer
#: from the replay file only.
MODES = ("live", "record", "replay")
#: The longest wait a ``retry-after`` header can ask for.
MAX_RETRY_AFTER_S = 60.0


@dataclass(frozen=True)
class LLMRequest:
    """What is sent: everything that can change the answer, and nothing else."""

    model: str
    messages: Messages
    system: Optional[str] = None
    max_tokens: int = 1024
    temperature: Optional[float] = None
    stop: Optional[Tuple[str, ...]] = None

    def key(self, backend: str) -> str:
        """``sha256:`` of the request and the backend: the same key means the same call."""
        canonical = json.dumps(
            {
                "backend": backend,
                "model": self.model,
                "system": self.system,
                "messages": self.messages,
                "max_tokens": self.max_tokens,
                "temperature": self.temperature,
                "stop": list(self.stop) if self.stop else None,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass
class LLMResponse:
    """What came back."""

    text: str
    model: str
    input_tokens: int = 0
    output_tokens: int = 0
    stop_reason: Optional[str] = None
    request_key: str = ""
    raw: Dict[str, Any] = field(default_factory=dict, repr=False)


class LLMBackend:
    """A provider's wire format. The port does the HTTP, retries and logging.

    Subclasses set ``KEY_ENV`` (where the key is read from when none is given)
    and implement :meth:`build` and :meth:`parse`.
    """

    #: The environment variable a key is read from when none is given.
    KEY_ENV: Optional[str] = None
    #: False when a key is optional (a local server).
    KEY_REQUIRED: bool = True
    DEFAULT_BASE_URL: str = ""

    def __init__(
        self, *, model: str, api_key: Optional[str] = None, base_url: Optional[str] = None
    ) -> None:
        self.model = model
        self.api_key = api_key
        self.base_url = (base_url or self.DEFAULT_BASE_URL).rstrip("/")

    def build(self, request: LLMRequest) -> Tuple[str, Dict[str, str], Dict[str, Any]]:
        """The URL, headers and JSON body for one request."""
        raise NotImplementedError

    def parse(self, answer: Dict[str, Any], request: LLMRequest) -> LLMResponse:
        """The provider's JSON answer as an :class:`LLMResponse`."""
        raise NotImplementedError


# --- the call log a run carries ------------------------------------------------------


class _CallLog:
    def __init__(self, into: List[Dict[str, Any]], task_dir: Optional[str]) -> None:
        self.calls = into
        self.task_dir = task_dir
        self.lock = threading.Lock()

    def add(self, call: Dict[str, Any]) -> None:
        with self.lock:
            self.calls.append(call)


_ACTIVE: contextvars.ContextVar[Optional[_CallLog]] = contextvars.ContextVar(
    "ubunye_llm_calls", default=None
)


@contextmanager
def recording(
    into: Optional[List[Dict[str, Any]]] = None, *, task_dir: Optional[str] = None
) -> Iterator[List[Dict[str, Any]]]:
    """Collect every call made inside the block; the engine opens one per run.

    ``task_dir`` is where a replay file lives by default (``.ubunye/llm-replay.jsonl``).
    """
    calls: List[Dict[str, Any]] = into if into is not None else []
    token = _ACTIVE.set(_CallLog(calls, task_dir))
    try:
        yield calls
    finally:
        _ACTIVE.reset(token)


# --- the port ------------------------------------------------------------------------


class LLMPort:
    """Sends requests through a backend, with retries, and logs every call."""

    def __init__(
        self,
        backend: LLMBackend,
        *,
        name: str,
        timeout_s: float = 60.0,
        retries: int = 3,
        backoff_s: float = 1.0,
        mode: str = "live",
        store: Optional[str] = None,
    ) -> None:
        self.backend = backend
        self.name = name
        self.timeout_s = timeout_s
        self.retries = retries
        self.backoff_s = backoff_s
        self.mode = mode
        self.store = store

    @property
    def model(self) -> str:
        return self.backend.model

    def request(
        self,
        prompt: Union[str, Messages],
        *,
        system: Optional[str] = None,
        max_tokens: int = 1024,
        temperature: Optional[float] = None,
        stop: Optional[Sequence[str]] = None,
    ) -> LLMRequest:
        messages = [{"role": "user", "content": prompt}] if isinstance(prompt, str) else prompt
        return LLMRequest(
            model=self.backend.model,
            messages=list(messages),
            system=system,
            max_tokens=max_tokens,
            temperature=temperature,
            stop=tuple(stop) if stop else None,
        )

    def complete(self, prompt: Union[str, Messages], **options: Any) -> LLMResponse:
        """One call. ``prompt`` is a user message, or a list of chat messages."""
        return self.send(self.request(prompt, **options))

    def complete_many(
        self,
        prompts: Sequence[Union[str, Messages]],
        *,
        max_concurrency: int = 4,
        **options: Any,
    ) -> List[LLMResponse]:
        """One call per prompt, ``max_concurrency`` at a time; answers in prompt order."""
        requests = [self.request(p, **options) for p in prompts]
        if not requests:
            return []
        with ThreadPoolExecutor(max_workers=max(1, min(max_concurrency, len(requests)))) as pool:
            # Each worker runs in a copy of this context, so the run's call log sees it.
            futures = [pool.submit(contextvars.copy_context().run, self.send, r) for r in requests]
            return [f.result() for f in futures]

    def send(self, request: LLMRequest) -> LLMResponse:
        key = request.key(self.name)
        started = time.perf_counter()
        call: Dict[str, Any] = {
            "backend": self.name,
            "model": request.model,
            "request_key": key,
            "status": "error",
            "input_tokens": 0,
            "output_tokens": 0,
            "attempts": 0,
            "source": self.mode,
        }
        active = _ACTIVE.get()
        try:
            if self.mode == "replay":
                response = self._replay(request, key, active)
            else:
                answer, call["attempts"] = self._post(request)
                response = self.backend.parse(answer, request)
                response.request_key = key
                if self.mode == "record":
                    self._store(active).put(
                        key, backend=self.name, model=request.model, response=_saved(response)
                    )
            call.update(
                status="ok",
                input_tokens=response.input_tokens,
                output_tokens=response.output_tokens,
                stop_reason=response.stop_reason,
            )
            return response
        except LLMError as exc:
            call["attempts"] = exc.context.get("Attempts", call["attempts"])
            call["error"] = str(exc.args[0])[:300]
            raise
        finally:
            call["seconds"] = round(time.perf_counter() - started, 6)
            if active is not None:
                active.add(call)

    def _store(self, active: Optional[_CallLog]) -> Any:
        from ubunye.llm.replay import store_for

        return store_for(self.store, active.task_dir if active else None)

    def _replay(self, request: LLMRequest, key: str, active: Optional[_CallLog]) -> LLMResponse:
        store = self._store(active)
        entry = store.get(key)
        if entry is None:
            raise LLMError(
                f"No recorded answer for {key} ({self.name}/{request.model})",
                context={"Replay file": str(store.path), "Attempts": 0},
                hint="Record it first: run once with UBUNYE_LLM_MODE=record, which calls "
                "the model and keeps the answer. Replay never calls out.",
            )
        saved = entry["response"]
        return LLMResponse(
            text=saved.get("text", ""),
            model=saved.get("model") or request.model,
            input_tokens=int(saved.get("input_tokens") or 0),
            output_tokens=int(saved.get("output_tokens") or 0),
            stop_reason=saved.get("stop_reason"),
            request_key=key,
        )

    def _post(self, request: LLMRequest) -> Tuple[Dict[str, Any], int]:
        url, headers, body = self.backend.build(request)
        data = json.dumps(body).encode("utf-8")
        headers = {"Content-Type": "application/json", **headers}
        attempt = 0
        while True:
            attempt += 1
            http = urllib.request.Request(url, data=data, headers=headers, method="POST")
            try:
                with urllib.request.urlopen(http, timeout=self.timeout_s) as answer:
                    return json.loads(answer.read().decode("utf-8")), attempt
            except urllib.error.HTTPError as exc:
                detail = _error_detail(exc)
                if exc.code not in RETRY_STATUSES or attempt > self.retries:
                    raise LLMError(
                        f"{self.name} refused the call: HTTP {exc.code}: {detail}",
                        context={"Backend": self.name, "Model": request.model, "Attempts": attempt},
                        hint=_hint(exc.code, self.backend),
                    ) from None
                asked = _retry_after(exc)
                wait = asked if asked is not None else self._backoff(attempt)
            except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
                if attempt > self.retries:
                    raise LLMError(
                        f"{self.name} could not be reached: {getattr(exc, 'reason', exc)}",
                        context={"Backend": self.name, "URL": url, "Attempts": attempt},
                    ) from None
                wait = self._backoff(attempt)
            log.info("%s: retrying in %.1fs (attempt %d)", self.name, wait, attempt + 1)
            time.sleep(wait)

    def _backoff(self, attempt: int) -> float:
        return float(self.backoff_s) * (2 ** (attempt - 1))


def _error_detail(exc: urllib.error.HTTPError) -> str:
    try:
        body = json.loads(exc.read().decode("utf-8", "replace"))
    except Exception:
        return exc.reason or ""
    error = body.get("error", body) if isinstance(body, dict) else body
    if isinstance(error, dict):
        error = error.get("message") or json.dumps(error)
    return str(error)[:300]


def _retry_after(exc: urllib.error.HTTPError) -> Optional[float]:
    value = exc.headers.get("retry-after") if exc.headers else None
    try:
        return min(float(value), MAX_RETRY_AFTER_S) if value is not None else None
    except ValueError:
        return None


def _hint(status: int, backend: LLMBackend) -> Optional[str]:
    if status in (401, 403):
        where = f"the {backend.KEY_ENV} variable, or " if backend.KEY_ENV else ""
        return f"Check the key: {where}api_key= (a secret:// reference works)."
    if status == 404:
        return "Check the model name and the base URL."
    return None


def _saved(response: LLMResponse) -> Dict[str, Any]:
    """What a replay file keeps of an answer: enough to give it back, no raw payload."""
    return {
        "text": response.text,
        "model": response.model,
        "input_tokens": response.input_tokens,
        "output_tokens": response.output_tokens,
        "stop_reason": response.stop_reason,
    }


def _resolve_key(value: Optional[str]) -> Optional[str]:
    from ubunye.core import secrets

    if value and secrets.is_reference(value):
        return secrets.SecretResolver().get(value)
    return value


def port(
    backend: str,
    *,
    model: str,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    timeout_s: float = 60.0,
    retries: int = 3,
    backoff_s: float = 1.0,
    mode: Optional[str] = None,
    store: Optional[str] = None,
) -> LLMPort:
    """A port to ``model`` on ``backend``.

    ``api_key`` may be a ``secret://`` reference; without one the backend's usual
    environment variable is read (``ANTHROPIC_API_KEY``, ``OPENAI_API_KEY``,
    ``DATABRICKS_TOKEN``). A missing key fails here, before any call, except in
    replay, which needs none.

    ``mode`` is ``live``, ``record`` or ``replay`` (default: ``UBUNYE_LLM_MODE``, else
    ``live``). ``store`` is the replay file (default: ``UBUNYE_LLM_STORE``, else
    ``.ubunye/llm-replay.jsonl`` in the task's folder).
    """
    from ubunye._internal.discovery import _get

    chosen = (mode or os.environ.get("UBUNYE_LLM_MODE") or "live").strip().lower()
    if chosen not in MODES:
        raise LLMError(
            f"Unknown LLM mode '{chosen}'",
            context={"Modes": ", ".join(MODES)},
            hint="Set UBUNYE_LLM_MODE (or mode=) to live, record or replay.",
        )
    cls = _get("ubunye.llm_backends", backend)
    key = _resolve_key(api_key) or (os.environ.get(cls.KEY_ENV) if cls.KEY_ENV else None)
    if cls.KEY_REQUIRED and not key and chosen != "replay":
        raise LLMError(
            f"No key for {backend}",
            context={"Backend": backend, "Model": model},
            hint=f"Set {cls.KEY_ENV}, or pass api_key= (a secret:// reference works).",
        )
    return LLMPort(
        cls(model=model, api_key=key, base_url=base_url),
        name=backend,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        mode=chosen,
        store=store,
    )
