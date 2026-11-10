"""Job health is a query over run records (ADR 013).

For each task: how its last run ended, when it last succeeded, how many runs in a
row failed since, and which runs still say ``running`` long after they should have
finished (their process most likely died). Only the four
:class:`~ubunye.interfaces.lineage.RunRecordStore` methods are used, so it works on
every store: the filesystem one, Delta, or a plugin. Jobs that share no disk but
share a store see each other's health.

    from ubunye.lineage import health, stores

    store = stores.open_for("pipelines")
    for h in health.check(store, ["pay/monthly/calc"], max_age="36h"):
        print(h.task, h.state, h.problems)
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Union

#: A run that says ``running`` for longer than this most likely died.
DEFAULT_STUCK_AFTER = 6 * 3600
#: How many recent runs are read per task (the last success is looked up apart).
DEFAULT_SCAN = 50

Duration = Union[int, float, str]

_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
_DURATION = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([smhd]?)\s*$", re.IGNORECASE)


def parse_duration(value: Duration) -> float:
    """Seconds in ``value``: a number of seconds, or ``"90s"``, ``"15m"``, ``"36h"``, ``"2d"``."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    m = _DURATION.match(str(value))
    if not m:
        raise ValueError(
            f"'{value}' is not a duration. Write a number of seconds or a number with "
            "s, m, h or d after it, for example 90s, 15m, 36h or 2d."
        )
    return float(m.group(1)) * _UNITS[(m.group(2) or "s").lower()]


def format_duration(seconds: float) -> str:
    """``seconds`` as a person reads it: 45s, 12m, 36h, 3d (a unit once there are two)."""
    for unit, size in (("d", 86400), ("h", 3600), ("m", 60)):
        if seconds >= 2 * size:
            n = seconds / size
            return f"{n:.0f}{unit}" if n >= 10 or n == int(n) else f"{n:.1f}{unit}"
    return f"{seconds:.0f}s"


def parse_time(stamp: Optional[str]) -> Optional[datetime]:
    """An ISO 8601 time from a run record, in UTC (a time with no zone is taken as UTC)."""
    if not stamp:
        return None
    try:
        t = datetime.fromisoformat(stamp.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return t if t.tzinfo else t.replace(tzinfo=timezone.utc)


@dataclass
class TaskHealth:
    """One task's health, from its run records."""

    task: str
    #: ok, running, failing, stuck, stale or never_run.
    state: str
    healthy: bool
    last_run_id: Optional[str] = None
    last_status: Optional[str] = None
    last_started_at: Optional[str] = None
    last_error: Optional[str] = None
    #: When the newest successful run ended (or started, when it has no end).
    last_success_at: Optional[str] = None
    #: Seconds since then.
    success_age_sec: Optional[float] = None
    #: Runs since the last success that did not succeed (a stuck run counts).
    consecutive_failures: int = 0
    #: Runs that still say ``running`` after the stuck threshold: likely dead.
    stuck_runs: List[Dict[str, Any]] = field(default_factory=list)
    #: One plain sentence per problem; empty when healthy.
    problems: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _same_task(records: Iterable[Any], task: str) -> List[Any]:
    # search() may match a task path's first parts; keep the task itself only.
    return [r for r in records if r.task_path == task]


def task_health(
    store: Any,
    task: str,
    *,
    max_age: Optional[Duration] = None,
    stuck_after: Duration = DEFAULT_STUCK_AFTER,
    now: Optional[datetime] = None,
    scan: int = DEFAULT_SCAN,
) -> TaskHealth:
    """The health of one task (``usecase/package/task``) from the records in ``store``.

    ``max_age``: the task is stale when it has not succeeded for longer than this.
    ``stuck_after``: a run still ``running`` after this long is likely dead.
    """
    now = now or datetime.now(timezone.utc)
    stuck_limit = parse_duration(stuck_after)
    age_limit = parse_duration(max_age) if max_age is not None else None
    records = _same_task(store.list_runs(task, n=scan), task)

    def age(r: Any) -> Optional[float]:
        t = parse_time(r.started_at)
        return (now - t).total_seconds() if t else None

    def is_stuck(r: Any) -> bool:
        a = age(r)
        return r.status == "running" and a is not None and a > stuck_limit

    stuck = [r for r in records if is_stuck(r)]
    failures = 0
    last_success = None
    for r in records:
        if r.status == "success":
            last_success = r
            break
        if r.status == "running" and not is_stuck(r):
            continue  # alive, not yet a verdict
        failures += 1
    if last_success is None and records:
        found = _same_task(store.search(task_path=task, status="success", limit=1), task)
        if not found:  # a store whose search matches by prefix may need more rows
            found = _same_task(store.search(task_path=task, status="success"), task)
        last_success = found[0] if found else None

    h = TaskHealth(task=task, state="ok", healthy=True, consecutive_failures=failures)
    if records:
        last = records[0]
        h.last_run_id, h.last_status = last.run_id, last.status
        h.last_started_at = last.started_at
        errors = [r.error for r in records if r.status not in ("success", "running") and r.error]
        h.last_error = errors[0] if failures and errors else None
    if last_success is not None:
        h.last_success_at = last_success.ended_at or last_success.started_at
        t = parse_time(h.last_success_at)
        h.success_age_sec = (now - t).total_seconds() if t else None
    h.stuck_runs = [
        {"run_id": r.run_id, "started_at": r.started_at, "running_for_sec": age(r)} for r in stuck
    ]

    for s in h.stuck_runs:
        h.problems.append(
            f"Run {s['run_id']} has said running for "
            f"{format_duration(s['running_for_sec'])} (more than "
            f"{format_duration(stuck_limit)}): its process likely died. Rerun the batch; "
            "the next run takes over its lease."
        )
    if failures:
        what = "The last run failed" if failures == 1 else f"{failures} runs in a row failed"
        since = f"; last success {h.last_success_at}" if h.last_success_at else "; never succeeded"
        h.problems.append(
            f"{what}{since}." + (f" Last error: {h.last_error}" if h.last_error else "")
        )
    stale = False
    if age_limit is not None:
        if h.success_age_sec is None:
            stale = True
            h.problems.append(
                f"No successful run on record, and --max-age is {format_duration(age_limit)}."
            )
        elif h.success_age_sec > age_limit:
            stale = True
            h.problems.append(
                f"Last success was {format_duration(h.success_age_sec)} ago, more than "
                f"{format_duration(age_limit)}."
            )

    if records and is_stuck(records[0]):
        h.state = "stuck"
    elif failures:
        h.state = "failing"
    elif stuck:
        h.state = "stuck"
    elif stale:
        h.state = "stale"
    elif not records:
        h.state = "never_run"
    elif records[0].status == "running":
        h.state = "running"
    h.healthy = not h.problems
    return h


def check(
    store: Any,
    tasks: Iterable[str],
    *,
    max_age: Optional[Duration] = None,
    stuck_after: Duration = DEFAULT_STUCK_AFTER,
    now: Optional[datetime] = None,
    scan: int = DEFAULT_SCAN,
) -> List[TaskHealth]:
    """:func:`task_health` for each task, in the order given."""
    return [
        task_health(store, t, max_age=max_age, stuck_after=stuck_after, now=now, scan=scan)
        for t in tasks
    ]


def tasks_for(
    store: Any,
    usecase_dir: Union[str, Path],
    usecase: str,
    package: Optional[str] = None,
) -> List[str]:
    """The tasks of a use case (or one package): those with a folder under
    ``usecase_dir`` and those with run records in ``store``, sorted.

    The store matters when the jobs ran elsewhere and only their records are shared.
    """
    root = Path(usecase_dir)
    prefix = f"{usecase}/{package}" if package else usecase
    found = {
        "/".join(cfg.parent.relative_to(root).parts)
        for cfg in root.glob(f"{usecase}/{package or '*'}/*/config.yaml")
    }
    for r in store.search(task_path=prefix):
        if r.task_path.startswith(prefix + "/") and r.task_path.count("/") == 2:
            found.add(r.task_path)
    return sorted(found)
