"""Recorded model answers, keyed by the request: replay a run for nothing, anywhere.

One JSON line per answer::

    {"key": "sha256:...", "backend": "anthropic", "model": "...",
     "recorded_at": "2026-09-25T10:00:00Z", "response": {"text": "...", ...}}

The key is :meth:`ubunye.llm.LLMRequest.key`, a hash of everything that can change
the answer, so a changed prompt, model or setting is a miss. Prompts are never
stored; answers are, since replay needs them. A file can be committed next to the
task, so CI and any machine replay the same answers with no key and no network.
Recording the same request again appends; the latest line wins.
"""

from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

#: The file's name inside a task folder's ``.ubunye`` directory.
DEFAULT_NAME = "llm-replay.jsonl"

_stores: Dict[str, "ReplayStore"] = {}
_stores_lock = threading.Lock()


class ReplayStore:
    """One replay file; loaded once, appended under a lock."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._answers: Optional[Dict[str, Dict[str, Any]]] = None

    def _load(self) -> Dict[str, Dict[str, Any]]:
        if self._answers is None:
            answers: Dict[str, Dict[str, Any]] = {}
            if self.path.exists():
                with open(self.path, encoding="utf-8") as fh:
                    for line in fh:
                        if line.strip():
                            entry = json.loads(line)
                            answers[entry["key"]] = entry
            self._answers = answers
        return self._answers

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._load().get(key)

    def put(self, key: str, *, backend: str, model: str, response: Dict[str, Any]) -> None:
        entry = {
            "key": key,
            "backend": backend,
            "model": model,
            "recorded_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "response": response,
        }
        with self._lock:
            answers = self._load()
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(entry, ensure_ascii=False, sort_keys=True) + "\n")
            answers[key] = entry


def store_for(explicit: Optional[str], task_dir: Optional[str]) -> ReplayStore:
    """The store a call uses: ``store=``, else ``UBUNYE_LLM_STORE``, else the task's own."""
    chosen = explicit or os.environ.get("UBUNYE_LLM_STORE")
    if chosen:
        path = Path(chosen)
    else:
        path = Path(task_dir or ".") / ".ubunye" / DEFAULT_NAME
    # Lexical, not Path.resolve(): on Windows resolve() spells a folder differently
    # (8.3 short name or not) before and after it exists, which split one file
    # into two stores, each holding half the answers.
    resolved = os.path.normcase(os.path.abspath(path))
    with _stores_lock:
        store = _stores.get(resolved)
        if store is None:
            store = _stores[resolved] = ReplayStore(Path(resolved))
        return store


def forget() -> None:
    """Drop every loaded store (tests; a file changed outside this process)."""
    with _stores_lock:
        _stores.clear()
