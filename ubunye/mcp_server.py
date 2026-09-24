"""``ubunye mcp``: the engine as an MCP server, so an agent can plan, run and check tasks.

`MCP <https://modelcontextprotocol.io>`_ is how agents (Claude, IDE assistants and
others) call tools. This server exposes the commands a person uses, over one
pipelines folder:

========================  =========================================================
``tasks``                 The tasks under the folder (``usecase/package/task``).
``doctor``                What will fail on this machine, and why.
``plan``                  What a task will read and write, the bill before the run,
                          and what will stop it. Reads no data.
``runs``, ``record``      Recorded runs and a full run record.
``gate``                  A run against a baseline, rule by rule.
``focus``                 A run's model calls as FOCUS 1.4 cost rows.
``run``                   Runs a task. Only with ``--allow-run``.
========================  =========================================================

Safe by default. Every tool but ``run`` only reads. Tasks are named, never given as
paths, and must sit under the folder. ``run`` exists only when the server was
started with ``--allow-run``; its model calls replay (``UBUNYE_LLM_MODE=replay``:
no key, no spend) unless the server was also started with ``--allow-live-llm``, and
the run's limits (``UBUNYE_LLM_MAX_*``) apply as always. A task's own ``print``
goes to stderr, so it cannot break the protocol on stdout.

Needs the ``mcp`` extra: ``pip install "ubunye-engine[mcp]"``.
"""

from __future__ import annotations

import contextlib
import os
import re
import sys
import threading
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

_NAME = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.-]*$")

INSTRUCTIONS = (
    "Ubunye Engine runs config-first data pipelines. A task is named "
    "usecase/package/task. Call `tasks` to see them, `plan` before `run` (it shows "
    "what will be read and written, what will stop the run, and the model bill), and "
    "`gate` to compare a run with the previous one. Tools read only, except `run`."
)


class _Tools:
    """The tools' logic, over one pipelines folder; plain methods, testable alone."""

    def __init__(
        self,
        root: Path,
        *,
        lineage_dir: str = ".ubunye/lineage",
        allow_live_llm: bool = False,
    ) -> None:
        self.root = Path(root).resolve()
        self.lineage = self.root / lineage_dir
        self.allow_live_llm = allow_live_llm
        self._run_lock = threading.Lock()

    # --- names ------------------------------------------------------------------

    def task_dir(self, task: str) -> Path:
        parts = task.strip("/").split("/")
        if len(parts) != 3 or not all(_NAME.match(p) and p not in (".", "..") for p in parts):
            raise ValueError(f"'{task}' is not a task name: use usecase/package/task")
        path = self.root.joinpath(*parts)
        if not (path / "config.yaml").is_file():
            raise ValueError(f"no task '{task}' under {self.root} (no config.yaml)")
        return path

    # --- tools --------------------------------------------------------------------

    def tasks(self) -> Dict[str, Any]:
        found = sorted(
            "/".join(cfg.parent.relative_to(self.root).parts)
            for cfg in self.root.glob("*/*/*/config.yaml")
        )
        return {"root": str(self.root), "tasks": found}

    def doctor(self, tasks: Optional[List[str]] = None) -> Dict[str, Any]:
        from ubunye.cli.doctor import (
            FAIL,
            backend_checks,
            environment_checks,
            plugin_checks,
            task_checks,
        )
        from ubunye.config.variables import build_variables

        checks = environment_checks() + backend_checks() + plugin_checks()
        variables = build_variables(dt=None, dtf=None, mode="DEV", extra={})
        for task in tasks or []:
            checks += task_checks(self.task_dir(task), task.split("/")[-1], variables)
        return {
            "ok": not any(c.status == FAIL for c in checks),
            "checks": [asdict(c) for c in checks],
        }

    def plan(
        self,
        task: str,
        dt: Optional[str] = None,
        mode: str = "DEV",
        variables: Optional[Dict[str, str]] = None,
        backend: Optional[str] = None,
    ) -> Dict[str, Any]:
        from ubunye.config import load_config
        from ubunye.config.variables import build_variables
        from ubunye.core.planning import build_plans

        path = self.task_dir(task)
        values = build_variables(dt=dt, dtf=None, mode=mode, extra=dict(variables or {}))
        with _stdout_to_stderr():
            cfg = load_config(str(path), values)
            (plan,) = build_plans(
                [(task, cfg, path)],
                backend=backend,
                variables=values,
                raw_yaml={task: (path / "config.yaml").read_text(encoding="utf-8")},
            )
        return plan

    def run(
        self,
        task: str,
        dt: Optional[str] = None,
        mode: str = "DEV",
        variables: Optional[Dict[str, str]] = None,
        backend: Optional[str] = None,
    ) -> Dict[str, Any]:
        import ubunye

        path = self.task_dir(task)
        # One run at a time: the model mode is process-wide.
        with self._run_lock, _llm_mode(None if self.allow_live_llm else "replay"):
            error = None
            with _stdout_to_stderr():
                try:
                    ubunye.run_task(
                        str(path),
                        mode=mode,
                        dt=dt,
                        variables=dict(variables or {}),
                        backend=backend,
                        lineage=True,
                        lineage_dir=str(self.lineage),
                    )
                except Exception as exc:  # the run's own failure: reported, not raised
                    error = f"{type(exc).__name__}: {exc}"
        runs = self._store().list_runs(task, n=1)
        summary = _summary(runs[0]) if runs else None
        return {"ok": error is None, "error": error, "run": summary}

    def runs(self, task: str, n: int = 10) -> Dict[str, Any]:
        self.task_dir(task)
        return {"task": task, "runs": [_summary(r) for r in self._store().list_runs(task, n=n)]}

    def record(self, task: str, run_id: str = "latest") -> Dict[str, Any]:
        return self._load(task, run_id).to_dict()

    def gate(
        self,
        task: str,
        baseline: str = "previous",
        candidate: str = "latest",
        allow_data_change: bool = False,
        max_slowdown: Optional[float] = None,
        max_row_change: Optional[float] = None,
        require_replay: bool = False,
        max_llm_cost_increase: Optional[float] = None,
    ) -> Dict[str, Any]:
        from ubunye.core import gate as rules

        base, cand = self._load(task, baseline), self._load(task, candidate)
        findings = rules.evaluate(
            base,
            cand,
            rules.Policy(
                allow_data_change=allow_data_change,
                max_slowdown=max_slowdown,
                max_row_change=max_row_change,
                require_replay=require_replay,
                max_llm_cost_increase=max_llm_cost_increase,
            ),
        )
        return {
            "ok": rules.passed(findings),
            "baseline": base.run_id,
            "candidate": cand.run_id,
            "findings": [f.as_dict() for f in findings],
        }

    def focus(self, task: str, run_id: str = "latest") -> Dict[str, Any]:
        from ubunye.lineage import focus

        record = self._load(task, run_id)
        return {"rows": focus.rows(record), "left_out": focus.left_out(record)}

    # --- helpers ----------------------------------------------------------------

    def _store(self) -> Any:
        from ubunye.lineage.storage import FileSystemLineageStore

        return FileSystemLineageStore(str(self.lineage))

    def _load(self, task: str, which: str) -> Any:
        from ubunye.cli.gate import _from_store

        self.task_dir(task)
        return _from_store(task, self.lineage, which)


def _summary(record: Any) -> Dict[str, Any]:
    calls = [c for c in record.llm_calls or [] if c.get("status") == "ok"]
    return {
        "run_id": record.run_id,
        "status": record.status,
        "started_at": record.started_at,
        "duration_sec": record.duration_sec,
        "error": record.error,
        "outputs": [
            {"name": s.name, "rows": s.row_count, "data_hash": s.data_hash} for s in record.outputs
        ],
        "llm": {
            "calls": len(calls),
            "replayed": sum(1 for c in calls if c.get("source") == "replay"),
            "budget": record.llm_budget or None,
        },
    }


@contextlib.contextmanager
def _stdout_to_stderr() -> Iterator[None]:
    """Keep stdout for the protocol: anything a task prints goes to stderr."""
    with contextlib.redirect_stdout(sys.stderr):
        yield


@contextlib.contextmanager
def _llm_mode(mode: Optional[str]) -> Iterator[None]:
    if mode is None:
        yield
        return
    before = os.environ.get("UBUNYE_LLM_MODE")
    os.environ["UBUNYE_LLM_MODE"] = mode
    try:
        yield
    finally:
        if before is None:
            os.environ.pop("UBUNYE_LLM_MODE", None)
        else:
            os.environ["UBUNYE_LLM_MODE"] = before


def build_server(
    root: Path,
    *,
    allow_run: bool = False,
    allow_live_llm: bool = False,
    lineage_dir: str = ".ubunye/lineage",
) -> Any:
    """The MCP server over the pipelines folder ``root``."""
    try:
        from mcp.server.mcpserver import MCPServer
        from mcp.server.mcpserver.exceptions import ToolError
        from mcp.types import ToolAnnotations
    except ImportError as exc:
        from ubunye.core.errors import UbunyeError

        raise UbunyeError(
            "`ubunye mcp` needs the MCP SDK",
            hint='pip install "ubunye-engine[mcp]"',
        ) from exc
    from ubunye import __version__

    tools = _Tools(root, lineage_dir=lineage_dir, allow_live_llm=allow_live_llm)
    server = MCPServer("ubunye", version=__version__, instructions=INSTRUCTIONS)
    read_only = ToolAnnotations(read_only_hint=True, open_world_hint=False)

    def guarded(fn: Any) -> Any:
        """Engine errors reach the agent as their message, not a bare failure."""

        def call(*args: Any, **kwargs: Any) -> Any:
            try:
                return fn(*args, **kwargs)
            except (ValueError, FileNotFoundError, KeyError) as exc:
                raise ToolError(str(exc).strip("'\"")) from None
            except Exception as exc:
                raise ToolError(f"{type(exc).__name__}: {exc}") from None

        return call

    @server.tool(annotations=read_only)
    def tasks() -> Dict[str, Any]:
        """The tasks under the pipelines folder, as usecase/package/task."""
        return guarded(tools.tasks)()

    @server.tool(annotations=read_only)
    def doctor(tasks: Optional[List[str]] = None) -> Dict[str, Any]:
        """What will fail on this machine, and why; with tasks, also their configs and env vars."""
        return guarded(tools.doctor)(tasks)

    @server.tool(annotations=read_only)
    def plan(
        task: str,
        dt: Optional[str] = None,
        mode: str = "DEV",
        variables: Optional[Dict[str, str]] = None,
        backend: Optional[str] = None,
    ) -> Dict[str, Any]:
        """What the task will read and write, the model bill, and what will stop it. Reads no data."""
        return guarded(tools.plan)(task, dt, mode, variables, backend)

    @server.tool(annotations=read_only)
    def runs(task: str, n: int = 10) -> Dict[str, Any]:
        """The task's recorded runs, newest first."""
        return guarded(tools.runs)(task, n)

    @server.tool(annotations=read_only)
    def record(task: str, run_id: str = "latest") -> Dict[str, Any]:
        """A full run record: outputs and hashes, code, environment, timings, model calls."""
        return guarded(tools.record)(task, run_id)

    @server.tool(annotations=read_only)
    def gate(
        task: str,
        baseline: str = "previous",
        candidate: str = "latest",
        allow_data_change: bool = False,
        max_slowdown: Optional[float] = None,
        max_row_change: Optional[float] = None,
        require_replay: bool = False,
        max_llm_cost_increase: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Compare a run with a baseline (default: latest against previous), rule by rule."""
        return guarded(tools.gate)(
            task,
            baseline,
            candidate,
            allow_data_change,
            max_slowdown,
            max_row_change,
            require_replay,
            max_llm_cost_increase,
        )

    @server.tool(annotations=read_only)
    def focus(task: str, run_id: str = "latest") -> Dict[str, Any]:
        """A run's model calls as FinOps FOCUS 1.4 cost rows."""
        return guarded(tools.focus)(task, run_id)

    if allow_run:
        live = "Model calls go live and can spend money, within UBUNYE_LLM_MAX_* limits."
        replay = "Model calls replay from the task's committed answers: no key, no spend."

        @server.tool(
            description=(
                "Run a task: read its inputs, transform, write its outputs, and record the "
                "run. Call `plan` first. " + (live if allow_live_llm else replay)
            ),
            annotations=ToolAnnotations(
                read_only_hint=False, destructive_hint=False, open_world_hint=allow_live_llm
            ),
        )
        def run(
            task: str,
            dt: Optional[str] = None,
            mode: str = "DEV",
            variables: Optional[Dict[str, str]] = None,
            backend: Optional[str] = None,
        ) -> Dict[str, Any]:
            return guarded(tools.run)(task, dt, mode, variables, backend)

    return server
