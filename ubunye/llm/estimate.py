"""The bill before the run: what ``ubunye plan`` says about a task's model calls.

The best guide to what a run will call is what it called before, so the plan
prices the task's replay file (its recorded answers, one per distinct request) at
today's prices and sets that against the run's limits. It reads no data and calls
no model. A task that has never been recorded gets its limits and mode checked,
and no estimate.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ubunye.core.errors import LLMError
from ubunye.llm import MODES, prices

_USES = ("from ubunye import llm", "import ubunye.llm", "from ubunye.llm")


def uses_llm(source: str) -> bool:
    return any(marker in source for marker in _USES)


def _recorded(path: Path) -> List[Dict[str, Any]]:
    """The replay file's answers, latest per key, grouped by backend and model."""
    latest: Dict[str, Dict[str, Any]] = {}
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                entry = json.loads(line)
                latest[entry["key"]] = entry
    groups: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for entry in latest.values():
        backend, model = entry.get("backend", ""), entry.get("model", "")
        g = groups.setdefault(
            (backend, model),
            {"backend": backend, "model": model, "calls": 0, "input_tokens": 0, "output_tokens": 0},
        )
        answer = entry.get("response") or {}
        g["calls"] += 1
        g["input_tokens"] += int(answer.get("input_tokens") or 0)
        g["output_tokens"] += int(answer.get("output_tokens") or 0)
    for g in groups.values():
        price = prices.lookup(g["backend"], g["model"])
        g["cost_usd"] = (
            None if price is None else prices.cost(price, g["input_tokens"], g["output_tokens"])
        )
    return sorted(groups.values(), key=lambda g: (g["backend"], g["model"]))


def plan_section(
    task_dir: Optional[Path], source: str
) -> Tuple[Optional[Dict[str, Any]], List[str], List[str]]:
    """The plan's ``llm`` section, and the problems and warnings it finds."""
    from ubunye.llm.budget import Budget
    from ubunye.llm.replay import path_for

    problems: List[str] = []
    warnings: List[str] = []
    replay_file = path_for(None, str(task_dir) if task_dir else None)
    if not uses_llm(source) and not replay_file.exists():
        return None, problems, warnings

    mode = (os.environ.get("UBUNYE_LLM_MODE") or "live").strip().lower()
    if mode not in MODES:
        problems.append(f"llm: UBUNYE_LLM_MODE={mode!r} is not one of {', '.join(MODES)}")
    try:
        limits = Budget.from_env().summary()
    except LLMError as exc:
        problems.append(f"llm: {exc.args[0]}")
        limits = {}
    max_usd = limits.get("max_usd")

    recorded = _recorded(replay_file) if replay_file.exists() else []
    estimate: Optional[float] = None
    if mode == "replay":
        estimate = 0.0
        if not recorded:
            problems.append(
                f"llm: replay mode, but nothing recorded at {replay_file}; "
                "run once with UBUNYE_LLM_MODE=record"
            )
    elif recorded:
        costs = [g["cost_usd"] for g in recorded]
        estimate = sum(costs) if all(c is not None for c in costs) else None
        if max_usd is not None:
            for g in recorded:
                if g["cost_usd"] is None:
                    problems.append(
                        f"llm: UBUNYE_LLM_MAX_USD is set but {g['backend']}/{g['model']} has "
                        "no price, so its calls will be refused; give it price= or a "
                        "UBUNYE_LLM_PRICES file"
                    )
            if estimate is not None and estimate > max_usd:
                warnings.append(
                    f"llm: a run like the recorded one costs ${estimate:.6f}, over "
                    f"UBUNYE_LLM_MAX_USD=${max_usd:g}; calls past the ceiling will be refused"
                )
    if mode != "replay" and max_usd is None:
        warnings.append("llm: live model calls with no dollar ceiling (UBUNYE_LLM_MAX_USD)")

    section = {
        "uses_llm": uses_llm(source),
        "mode": mode,
        "replay_file": str(replay_file),
        "recorded": recorded,
        "estimated_usd": estimate,
        "limits": {k: v for k, v in limits.items() if k.startswith("max_")},
        "prices_as_of": prices.AS_OF,
    }
    return section, problems, warnings
