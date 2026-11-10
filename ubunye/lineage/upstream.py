"""Upstream checks: do not start until task X succeeded for this batch (ADR 013).

A task names what must have finished first in its config. It is platform
invariant (the same on a laptop, Databricks or Airflow), so it belongs there::

    CONFIG:
      requires:
        - task: pay/monthly/calc     # usecase/package/task
          variables: [month]         # the same month as this run
          wait: 2h                   # optional: wait up to 2 hours, then refuse

``variables`` says which batch of the upstream counts:

* a list of names: the upstream run had the same values as this run;
* a mapping of name to value: the upstream run had exactly these values
  (``{month: "{{ prev_month }}"}``; values are rendered like the rest of the config);
* left out: every variable that names this run's batch (all but ``mode`` and
  ``dtf``) must match. ``variables: []`` means any successful run counts.

A run is ready when the store holds a record of the upstream task with status
``success`` and those values. Without ``wait`` a run that is not ready is refused at
once, before the task is loaded, with what is missing and what to try. A task with
no ``requires`` runs exactly as before. Only the four RunRecordStore methods are
used, so the upstream may have run on another machine, as long as both write to
the same store (ADR 011).
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Union

from ubunye.core.errors import UbunyeError
from ubunye.lineage.health import Duration, format_duration, parse_duration

#: How often a waiting run looks again, unless the requirement says.
DEFAULT_POLL = 60.0
#: Variables that never name a batch (the same rule as rerun safety, ADR 008).
NOT_A_BATCH = ("mode", "dtf")


class UpstreamNotReady(UbunyeError, RuntimeError):
    """A task this run requires has not succeeded for the batch."""


@dataclass
class Requirement:
    """One upstream a run waits for (one entry of ``CONFIG.requires``)."""

    task: str
    variables: Optional[Union[Sequence[str], Mapping[str, Any]]] = None
    wait: Optional[Duration] = None
    poll: Optional[Duration] = None

    @staticmethod
    def of(item: Any) -> "Requirement":
        """A Requirement from a config entry (a dict or a pydantic model)."""
        if isinstance(item, Requirement):
            return item
        if hasattr(item, "model_dump"):
            item = item.model_dump()
        return Requirement(
            task=item["task"],
            variables=item.get("variables"),
            wait=item.get("wait"),
            poll=item.get("poll"),
        )


def batch_of(req: Requirement, run_variables: Mapping[str, Any]) -> Dict[str, str]:
    """The upstream batch this run needs: variable name to value, as strings.

    Raises :class:`UpstreamNotReady` when a named variable is not set on this run.
    """
    run_variables = dict(run_variables or {})
    if req.variables is None:
        return {
            k: str(v)
            for k, v in run_variables.items()
            if k not in NOT_A_BATCH and v not in (None, "")
        }
    if isinstance(req.variables, Mapping):
        return {str(k): str(v) for k, v in req.variables.items()}
    missing = [n for n in req.variables if run_variables.get(n) in (None, "")]
    if missing:
        raise UpstreamNotReady(
            f"This run requires '{req.task}' for the same {', '.join(missing)}, "
            f"but the run has no {', '.join(missing)}.",
            context={"Requires": req.task, "Run variables": _fmt(run_variables) or "(none)"},
            hint=f"Pass {' '.join(f'--var {n}=...' for n in missing)} (or -dt for dt), "
            "or give the values in CONFIG.requires (variables: {name: value}).",
        )
    return {n: str(run_variables[n]) for n in req.variables}


def _fmt(values: Mapping[str, Any]) -> str:
    return ", ".join(f"{k}={v}" for k, v in sorted(values.items()))


def _matches(record: Any, batch: Mapping[str, str]) -> bool:
    got = record.variables or {}
    return all(k in got and str(got[k]) == v for k, v in batch.items())


def _runs_of_batch(store: Any, task: str, batch: Mapping[str, str], status: Optional[str]) -> List:
    return [
        r
        for r in store.search(task_path=task, status=status)
        if r.task_path == task and _matches(r, batch)
    ]


def find(store: Any, req: Requirement, run_variables: Mapping[str, Any]) -> Optional[Any]:
    """The newest successful run of the upstream for this batch, or None."""
    found = _runs_of_batch(store, req.task, batch_of(req, run_variables), "success")
    return found[0] if found else None


def _not_ready(
    store: Any, req: Requirement, batch: Mapping[str, str], task: Optional[str], waited: float
) -> UpstreamNotReady:
    latest = _runs_of_batch(store, req.task, batch, None)
    if latest:
        r = latest[0]
        seen = f"its latest run of this batch is {r.status} ({r.run_id}, started {r.started_at})"
        if r.error:
            seen += f": {r.error}"
    else:
        seen = "no run of this batch is on record"
    who = f"'{task}'" if task else "This run"
    waited_txt = f" after waiting {format_duration(waited)}" if waited else ""
    return UpstreamNotReady(
        f"{who} requires '{req.task}' to have succeeded for this batch first, "
        f"and it has not{waited_txt}: {seen}.",
        context={
            "Requires": req.task,
            "Batch": _fmt(batch) or "(any run)",
        },
        hint=f"Run '{req.task}' for this batch first. To wait for it, set wait: (e.g. 2h) "
        "on the requirement, or run `ubunye runs wait-for "
        f"{req.task} -d <usecase_dir> {' '.join(f'--var {k}={v}' for k, v in batch.items())}"
        " --timeout 2h` before this task. Both read the same run record store "
        "(--lineage-backend, --lineage-dir).",
    )


def wait_for(
    store: Any,
    req: Requirement,
    run_variables: Mapping[str, Any],
    *,
    task: Optional[str] = None,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> Any:
    """The upstream's successful run for this batch, waiting up to ``req.wait``.

    Raises :class:`UpstreamNotReady` when it has not succeeded in time.
    """
    batch = batch_of(req, run_variables)
    timeout = parse_duration(req.wait) if req.wait is not None else 0.0
    poll = parse_duration(req.poll) if req.poll is not None else DEFAULT_POLL
    start = clock()
    while True:
        found = _runs_of_batch(store, req.task, batch, "success")
        if found:
            return found[0]
        waited = clock() - start
        if waited >= timeout:
            err = _not_ready(store, req, batch, task, timeout if timeout else 0.0)
            if timeout:
                err.context["Waited"] = format_duration(timeout)
            raise err
        sleep(max(0.0, min(poll, timeout - waited)))


def check(
    store: Any,
    requires: Iterable[Any],
    run_variables: Mapping[str, Any],
    *,
    task: Optional[str] = None,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> List[Any]:
    """Every requirement met, or :class:`UpstreamNotReady` for the first that is not.

    Returns the upstream runs found, one per requirement.
    """
    return [
        wait_for(store, Requirement.of(r), run_variables, task=task, sleep=sleep, clock=clock)
        for r in requires
    ]
