"""Recorded model answers, keyed by the request: replay a run for nothing, anywhere.

One JSON line per answer::

    {"key": "sha256:...", "occurrence": 0, "session": "...", "backend": "anthropic",
     "model": "...", "recorded_at": "2026-09-25T10:00:00Z", "response": {"text": "..."}}

The key is :meth:`ubunye.llm.LLMRequest.key`, a hash of everything that can change
the answer, so a changed prompt, model or setting is a miss. Prompts are never
stored; answers are, since replay needs them. A file can be committed next to the
task, so CI and any machine replay the same answers with no key and no network.

A run can ask the same request more than once (two identical reviews), and a model
may answer each differently. So each answer is kept with its ``occurrence``, the
count of earlier identical requests in the same run, and replay gives the nth
request the nth answer: the replayed run is the recorded run, call for call.
Recording a request again in a later run replaces that request's answers as a set
(``session`` tells runs apart). Lines without an occurrence (files written before
0.7.2) hold one answer per request, given to every occurrence, as before.
"""

from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

#: The file's name in the task folder, next to ``config.yaml``.
DEFAULT_NAME = "llm-replay.jsonl"

_stores: Dict[str, "ReplayStore"] = {}
_stores_lock = threading.Lock()

# key -> (session that recorded it last, {occurrence: entry})
_Answers = Dict[str, Tuple[Optional[str], Dict[int, Dict[str, Any]]]]


class ReplayStore:
    """One replay file; loaded once, appended under a lock."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._answers: Optional[_Answers] = None

    @staticmethod
    def _keep(answers: _Answers, entry: Dict[str, Any]) -> None:
        session = entry.get("session")
        held = answers.get(entry["key"])
        if held is None or held[0] != session or session is None:
            # A new recording of this request (or an old-format line): its
            # answers replace what an earlier run recorded.
            held = (session, {} if held is None or held[0] != session else held[1])
            answers[entry["key"]] = held
        held[1][int(entry.get("occurrence") or 0)] = entry

    def _load(self) -> _Answers:
        if self._answers is None:
            answers: _Answers = {}
            if self.path.exists():
                with open(self.path, encoding="utf-8") as fh:
                    for line in fh:
                        if line.strip():
                            self._keep(answers, json.loads(line))
            self._answers = answers
        return self._answers

    def get(self, key: str, occurrence: int = 0) -> Optional[Dict[str, Any]]:
        """The answer the ``occurrence``-th identical request of a run was given."""
        with self._lock:
            held = self._load().get(key)
        if held is None:
            return None
        session, by_occurrence = held
        if session is None:
            # Recorded before occurrences were kept: one answer for them all.
            return by_occurrence.get(max(by_occurrence))
        return by_occurrence.get(occurrence)

    def recorded(self, key: str) -> int:
        """How many answers the latest recording of ``key`` holds."""
        with self._lock:
            held = self._load().get(key)
        return len(held[1]) if held else 0

    def put(
        self,
        key: str,
        *,
        backend: str,
        model: str,
        response: Dict[str, Any],
        occurrence: int = 0,
        session: Optional[str] = None,
    ) -> None:
        entry = {
            "key": key,
            "occurrence": occurrence,
            "session": session,
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
            self._keep(answers, entry)


def path_for(explicit: Optional[str], task_dir: Optional[str]) -> Path:
    """The replay file: ``store=``, else ``UBUNYE_LLM_STORE``, else the task's own."""
    chosen = explicit or os.environ.get("UBUNYE_LLM_STORE")
    if chosen:
        return Path(chosen)
    # In the task folder itself, next to config.yaml: the file is meant to be
    # committed, and projects commonly ignore .ubunye/ (lineage records live there).
    return Path(task_dir or ".") / DEFAULT_NAME


def store_for(explicit: Optional[str], task_dir: Optional[str]) -> ReplayStore:
    """The store a call uses (see :func:`path_for`); one per file per process."""
    path = path_for(explicit, task_dir)
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
