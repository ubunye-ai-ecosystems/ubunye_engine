"""Job health and upstream checks are queries over run records (ADR 013).

Every test runs on the filesystem store and on a duck typed store from another
package, through the four RunRecordStore methods only.
"""

from __future__ import annotations

import json
import re
import uuid
import warnings
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import pytest

from tests.unit.lineage.test_lineage_stores import MemoryStore, memory_registered  # noqa: F401
from ubunye.lineage import health, stores, upstream
from ubunye.lineage.context import RunContext
from ubunye.lineage.storage import FileSystemLineageStore

NOW = datetime(2026, 11, 10, 12, 0, tzinfo=timezone.utc)
CALC = "pay/monthly/calc"
SEND = "pay/monthly/send"


def _at(hours_ago: float) -> str:
    return (NOW - timedelta(hours=hours_ago)).strftime("%Y-%m-%dT%H:%M:%SZ")


def rec(
    task: str,
    status: str,
    hours_ago: float,
    variables: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
) -> RunContext:
    uc, pkg, name = task.split("/")
    return RunContext(
        run_id=str(uuid.uuid4()),
        task_path=task,
        usecase=uc,
        package=pkg,
        task_name=name,
        profile="PROD",
        model="etl",
        version="1.0.0",
        config_hash="sha256:x",
        started_at=_at(hours_ago),
        ended_at=None if status == "running" else _at(hours_ago - 0.1),
        status=status,
        error=error,
        variables=dict(variables or {"dt": "2026-11-01", "mode": "PROD"}),
    )


@pytest.fixture(params=["filesystem", "memory"])
def store(request, tmp_path):
    if request.param == "filesystem":
        yield FileSystemLineageStore(str(tmp_path / ".ubunye" / "lineage"))
        return
    MemoryStore.shelves.clear()
    yield MemoryStore(f"mem-{uuid.uuid4().hex}")
    MemoryStore.shelves.clear()


def _save(store, *records):
    # Oldest first, so "newest first" by write order and by start time agree.
    for r in sorted(records, key=lambda r: r.started_at):
        store.save(r)


# ---------------------------------------------------------------------------
# Durations
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text,seconds",
    [("90", 90), ("90s", 90), ("15m", 900), ("6h", 21600), ("2d", 172800), (3600, 3600)],
)
def test_durations_read_as_people_write_them(text, seconds):
    assert health.parse_duration(text) == seconds


def test_a_bad_duration_says_what_to_write():
    with pytest.raises(ValueError, match="36h"):
        health.parse_duration("soon")


# ---------------------------------------------------------------------------
# Job health
# ---------------------------------------------------------------------------


def test_a_task_whose_last_run_succeeded_is_healthy(store):
    _save(store, rec(CALC, "error", 30), rec(CALC, "success", 5))
    h = health.task_health(store, CALC, now=NOW)
    assert h.healthy and h.state == "ok"
    assert h.last_status == "success"
    assert h.last_success_at == _at(4.9)
    assert h.consecutive_failures == 0


def test_consecutive_failures_count_back_to_the_last_success(store):
    _save(
        store,
        rec(CALC, "success", 50),
        rec(CALC, "error", 30),
        rec(CALC, "interrupted", 20),
        rec(CALC, "error", 2, error="ValueError: bad month"),
    )
    h = health.task_health(store, CALC, now=NOW)
    assert not h.healthy and h.state == "failing"
    assert h.consecutive_failures == 3
    assert h.last_error == "ValueError: bad month"
    assert h.last_success_at == _at(49.9)
    assert any("3 runs in a row failed" in p for p in h.problems)


def test_the_last_success_is_found_beyond_the_recent_runs(store):
    _save(store, rec(CALC, "success", 100), *[rec(CALC, "error", h) for h in range(1, 6)])
    h = health.task_health(store, CALC, now=NOW, scan=3)
    assert h.last_success_at == _at(99.9)
    assert h.consecutive_failures >= 3


def test_a_run_left_running_too_long_is_likely_dead(store):
    _save(store, rec(CALC, "success", 30), rec(CALC, "running", 10))
    h = health.task_health(store, CALC, now=NOW, stuck_after=6 * 3600)
    assert not h.healthy and h.state == "stuck"
    assert len(h.stuck_runs) == 1
    assert any("likely died" in p for p in h.problems)


def test_a_run_still_running_within_the_threshold_is_fine(store):
    _save(store, rec(CALC, "success", 30), rec(CALC, "running", 1))
    h = health.task_health(store, CALC, now=NOW, stuck_after=6 * 3600)
    assert h.healthy and h.state == "running"
    assert h.consecutive_failures == 0


def test_max_age_makes_an_old_success_stale(store):
    _save(store, rec(CALC, "success", 50))
    assert health.task_health(store, CALC, now=NOW).healthy
    h = health.task_health(store, CALC, now=NOW, max_age=36 * 3600)
    assert not h.healthy and h.state == "stale"
    assert any("36h" in p for p in h.problems)


def test_a_task_that_never_ran(store):
    h = health.task_health(store, CALC, now=NOW)
    assert h.state == "never_run" and h.healthy
    h = health.task_health(store, CALC, now=NOW, max_age=3600)
    assert not h.healthy and h.state == "stale"


def test_another_tasks_records_are_not_mixed_in(store):
    _save(store, rec(CALC, "success", 5), rec(CALC + "2", "error", 1))
    assert health.task_health(store, CALC, now=NOW).healthy


def test_health_report_as_json(store):
    _save(store, rec(CALC, "success", 5))
    (doc,) = [h.as_dict() for h in health.check(store, [CALC], now=NOW)]
    json.dumps(doc)
    assert doc["task"] == CALC and doc["healthy"] is True


def test_tasks_of_a_use_case_come_from_disk_and_from_the_store(store, tmp_path):
    (tmp_path / "pay" / "monthly" / "calc").mkdir(parents=True)
    (tmp_path / "pay" / "monthly" / "calc" / "config.yaml").write_text("x: 1\n")
    _save(store, rec(SEND, "success", 5), rec("other/a/b", "success", 5))
    assert health.tasks_for(store, tmp_path, "pay") == [CALC, SEND]
    assert health.tasks_for(store, tmp_path, "pay", "monthly") == [CALC, SEND]


# ---------------------------------------------------------------------------
# Upstream checks
# ---------------------------------------------------------------------------


def test_the_upstream_succeeded_for_this_batch(store):
    _save(store, rec(CALC, "success", 200, {"dt": "2026-10-31", "month": "2026-10"}))
    req = upstream.Requirement(task=CALC, variables=["month"])
    found = upstream.find(store, req, {"month": "2026-10", "dt": "2026-11-09"})
    assert found is not None and found.status == "success"


def test_another_batch_does_not_count(store):
    _save(store, rec(CALC, "success", 200, {"month": "2026-09"}))
    req = upstream.Requirement(task=CALC, variables=["month"])
    assert upstream.find(store, req, {"month": "2026-10"}) is None


def test_a_failed_or_running_upstream_does_not_count(store):
    _save(
        store,
        rec(CALC, "error", 3, {"month": "2026-10"}),
        rec(CALC, "running", 1, {"month": "2026-10"}),
    )
    req = upstream.Requirement(task=CALC, variables=["month"])
    assert upstream.find(store, req, {"month": "2026-10"}) is None


def test_by_default_every_batch_variable_must_match(store):
    _save(store, rec(CALC, "success", 3, {"dt": "2026-11-01", "region": "gp", "mode": "DEV"}))
    req = upstream.Requirement(task=CALC)
    # mode and dtf do not name a batch.
    assert upstream.find(store, req, {"dt": "2026-11-01", "region": "gp", "mode": "PROD"})
    assert upstream.find(store, req, {"dt": "2026-11-01", "region": "wc"}) is None


def test_explicit_values_name_the_upstream_batch(store):
    _save(store, rec(CALC, "success", 200, {"dt": "2026-10-31"}))
    req = upstream.Requirement(task=CALC, variables={"dt": "2026-10-31"})
    assert upstream.find(store, req, {"dt": "2026-11-09"}) is not None


def test_a_missing_upstream_refuses_with_what_which_batch_and_what_to_try(store):
    _save(store, rec(CALC, "error", 3, {"month": "2026-10"}, error="boom"))
    reqs = [upstream.Requirement(task=CALC, variables=["month"])]
    with pytest.raises(upstream.UpstreamNotReady) as exc:
        upstream.check(store, reqs, {"month": "2026-10"}, task=SEND)
    text = str(exc.value)
    assert CALC in text and SEND in text and "month=2026-10" in text
    assert "error" in text  # the latest run of that batch is named
    assert "ubunye runs wait-for" in text


def test_a_variable_the_run_does_not_have_is_a_clear_error(store):
    reqs = [upstream.Requirement(task=CALC, variables=["month"])]
    with pytest.raises(upstream.UpstreamNotReady, match="month"):
        upstream.check(store, reqs, {"dt": "2026-11-01"}, task=SEND)


def test_waiting_polls_until_the_upstream_succeeds(store):
    reqs = [upstream.Requirement(task=CALC, variables=["month"], wait="10m", poll="1m")]
    slept = []

    def sleep(seconds):
        slept.append(seconds)
        if len(slept) == 2:
            store.save(rec(CALC, "success", 0, {"month": "2026-10"}))

    clock = iter(range(0, 10_000, 60))
    upstream.check(
        store, reqs, {"month": "2026-10"}, task=SEND, sleep=sleep, clock=lambda: next(clock)
    )
    assert slept == [60, 60]


def test_waiting_gives_up_after_the_timeout(store):
    reqs = [upstream.Requirement(task=CALC, variables=["month"], wait="3m", poll="1m")]
    clock = iter(range(0, 10_000, 60))
    with pytest.raises(upstream.UpstreamNotReady, match="3m"):
        upstream.check(
            store,
            reqs,
            {"month": "2026-10"},
            task=SEND,
            sleep=lambda s: None,
            clock=lambda: next(clock),
        )


# ---------------------------------------------------------------------------
# CONFIG.requires
# ---------------------------------------------------------------------------


def _config(requires: str = "") -> str:
    return (
        "VERSION: 0.1.0\nCONFIG:\n  inputs:\n    src: {format: s3, path: in.csv}\n"
        "  outputs:\n    out: {format: s3, path: out}\n" + requires
    )


def test_config_requires_is_validated(tmp_path):
    import yaml

    from ubunye.config.schema import UbunyeConfig

    good = yaml.safe_load(
        _config("  requires:\n    - {task: pay/monthly/calc, variables: [month], wait: 2h}\n")
    )
    cfg = UbunyeConfig.model_validate(good)
    (req,) = cfg.CONFIG.requires
    assert req.task == CALC and req.variables == ["month"] and req.wait == "2h"
    bad = yaml.safe_load(_config("  requires:\n    - {task: calc}\n"))
    with pytest.raises(ValueError, match="usecase/package/task"):
        UbunyeConfig.model_validate(bad)
    typo = yaml.safe_load(_config("  requires:\n    - {task: a/b/c, varables: [dt]}\n"))
    with pytest.raises(ValueError):
        UbunyeConfig.model_validate(typo)


def test_a_config_without_requires_hashes_as_before():
    import yaml

    from ubunye.config.hashing import config_hash
    from ubunye.config.schema import UbunyeConfig

    cfg = UbunyeConfig.model_validate(yaml.safe_load(_config()))
    dumped = cfg.model_dump(mode="json")
    assert "requires" not in dumped["CONFIG"]
    assert config_hash(dumped).startswith("sha256:")


def _task(tmp_path, requires: str = ""):
    pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    task = tmp_path / "pay" / "monthly" / "send"
    task.mkdir(parents=True)
    (tmp_path / "in.csv").write_text("id\n1\n2\n", encoding="utf-8")
    (task / "config.yaml").write_text(
        "VERSION: 0.1.0\nCONFIG:\n  inputs:\n"
        f"    src: {{format: s3, path: '{(tmp_path / 'in.csv').as_posix()}', file_format: csv,"
        " options: {header: 'true'}}\n  outputs:\n"
        f"    out: {{format: s3, path: '{(tmp_path / 'out').as_posix()}', file_format: parquet,"
        " mode: overwrite}\n" + requires,
        encoding="utf-8",
    )
    (task / "transformations.py").write_text(
        "from ubunye.core.interfaces import Task\n"
        "class T(Task):\n"
        "    def transform(self, sources):\n"
        "        return {'out': sources['src']}\n",
        encoding="utf-8",
    )
    return task


_REQ = "  requires:\n    - {task: pay/monthly/calc, variables: [month]}\n"


def _run(task, **kw):
    from ubunye.api import run_task

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return run_task(str(task), backend="pandas", **kw)


def test_a_task_refuses_to_start_before_its_upstream_succeeded(tmp_path, monkeypatch):
    monkeypatch.delenv(stores.ENV_VAR, raising=False)
    task = _task(tmp_path, _REQ)
    with pytest.raises(upstream.UpstreamNotReady, match="pay/monthly/calc"):
        _run(task, variables={"month": "2026-10"})
    assert not (tmp_path / "out").exists()  # nothing ran


def test_a_task_starts_once_its_upstream_succeeded(tmp_path, monkeypatch):
    monkeypatch.delenv(stores.ENV_VAR, raising=False)
    task = _task(tmp_path, _REQ)
    FileSystemLineageStore(str(tmp_path / ".ubunye" / "lineage")).save(
        rec(CALC, "success", 200, {"month": "2026-10"})
    )
    _run(task, variables={"month": "2026-10"})
    assert (tmp_path / "out").exists()


def test_the_upstream_is_read_from_the_named_store(memory_registered, tmp_path):  # noqa: F811
    task = _task(tmp_path, _REQ)
    MemoryStore("ops.runs").save(rec(CALC, "success", 200, {"month": "2026-10"}))
    _run(
        task,
        variables={"month": "2026-10"},
        lineage=True,
        lineage_dir="ops.runs",
        lineage_backend="memory",
    )
    assert (tmp_path / "out").exists()


def test_a_task_without_requires_runs_as_today(tmp_path, monkeypatch):
    monkeypatch.delenv(stores.ENV_VAR, raising=False)
    _run(_task(tmp_path), variables={"month": "2026-10"})
    assert (tmp_path / "out").exists()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _strip(text: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


def _fs(tmp_path):
    return FileSystemLineageStore(str(tmp_path / ".ubunye" / "lineage"))


def _cli(*args):
    from typer.testing import CliRunner

    from ubunye.cli.main import app

    return CliRunner().invoke(app, ["runs", *args])


def test_cli_health_is_zero_when_all_is_well(tmp_path, monkeypatch):
    monkeypatch.delenv(stores.ENV_VAR, raising=False)
    _fs(tmp_path).save(rec(CALC, "success", 0.5))
    res = _cli("health", "-d", str(tmp_path), "--task", CALC)
    assert res.exit_code == 0, res.output
    assert CALC in res.output and "ok" in res.output


def test_cli_health_fails_a_failing_task_and_says_why(tmp_path, monkeypatch):
    monkeypatch.delenv(stores.ENV_VAR, raising=False)
    _fs(tmp_path).save(rec(CALC, "success", 0.5))
    _fs(tmp_path).save(rec(SEND, "error", 0.2))
    res = _cli("health", "-d", str(tmp_path), "-u", "pay", "--json")
    assert res.exit_code == 1, res.output
    doc = json.loads(res.output)
    assert doc["ok"] is False
    by = {t["task"]: t for t in doc["tasks"]}
    assert by[CALC]["healthy"] and not by[SEND]["healthy"]


def test_cli_health_max_age(tmp_path, monkeypatch):
    monkeypatch.delenv(stores.ENV_VAR, raising=False)
    old = (datetime.now(timezone.utc) - timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
    r = rec(CALC, "success", 0)
    r.started_at = r.ended_at = old
    _fs(tmp_path).save(r)
    assert _cli("health", "-d", str(tmp_path), "--task", CALC).exit_code == 0
    res = _cli("health", "-d", str(tmp_path), "--task", CALC, "--max-age", "36h")
    assert res.exit_code == 1
    assert "stale" in _strip(res.output)


def test_cli_health_reads_the_named_store(memory_registered, tmp_path):  # noqa: F811
    MemoryStore("ops.runs").save(rec(CALC, "error", 1))
    res = _cli(
        "health",
        "-d",
        str(tmp_path),
        "-u",
        "pay",
        "--lineage-backend",
        "memory",
        "--lineage-dir",
        "ops.runs",
        "--json",
    )
    assert res.exit_code == 1, res.output
    assert json.loads(res.output)["tasks"][0]["task"] == CALC


def test_cli_health_needs_tasks(tmp_path):
    res = _cli("health", "-d", str(tmp_path))
    assert res.exit_code != 0
    assert "--task" in _strip(res.output)


def test_cli_wait_for_finds_the_batch(tmp_path, monkeypatch):
    monkeypatch.delenv(stores.ENV_VAR, raising=False)
    _fs(tmp_path).save(rec(CALC, "success", 200, {"month": "2026-10"}))
    res = _cli("wait-for", CALC, "-d", str(tmp_path), "--var", "month=2026-10", "--json")
    assert res.exit_code == 0, res.output
    assert json.loads(res.output)["ok"] is True
    res = _cli("wait-for", CALC, "-d", str(tmp_path), "--var", "month=2026-09")
    assert res.exit_code == 1
    assert "month=2026-09" in _strip(res.output)


def test_cli_run_refuses_a_task_whose_upstream_has_not_succeeded(tmp_path, monkeypatch):
    from typer.testing import CliRunner

    from ubunye.cli.main import app

    monkeypatch.delenv(stores.ENV_VAR, raising=False)
    _task(tmp_path, _REQ)
    res = CliRunner().invoke(
        app,
        ["run", "-d", str(tmp_path), "-u", "pay", "-p", "monthly", "-t", "send"]
        + ["--backend", "pandas", "--var", "month=2026-10"],
    )
    assert res.exit_code == 1
    out = _strip(res.output)
    assert "Run refused for send" in out and "pay/monthly/calc" in out
    assert "Traceback" not in out


# ---------------------------------------------------------------------------
# MCP
# ---------------------------------------------------------------------------


def test_mcp_health_and_upstream_tools(tmp_path, monkeypatch):
    monkeypatch.delenv(stores.ENV_VAR, raising=False)
    from ubunye.mcp_server import _Tools

    _fs(tmp_path).save(rec(CALC, "success", 0.5, {"month": "2026-10"}))
    tools = _Tools(tmp_path)
    doc = tools.health(tasks=[CALC])
    assert doc["ok"] is True and doc["tasks"][0]["task"] == CALC
    assert tools.upstream(CALC, {"month": "2026-10"})["ok"] is True
    assert tools.upstream(CALC, {"month": "2026-09"})["ok"] is False


# ---------------------------------------------------------------------------
# Python API
# ---------------------------------------------------------------------------


def test_python_api_health_and_wait(tmp_path, monkeypatch):
    monkeypatch.delenv(stores.ENV_VAR, raising=False)
    from ubunye.api import job_health, wait_for_upstream

    _fs(tmp_path).save(rec(CALC, "success", 0.5, {"month": "2026-10"}))
    _fs(tmp_path).save(rec(SEND, "error", 0.2, {"month": "2026-10"}))
    report = {h.task: h for h in job_health(tmp_path, usecase="pay")}
    assert report[CALC].healthy and not report[SEND].healthy
    found = wait_for_upstream(tmp_path, CALC, variables={"month": "2026-10"})
    assert found.task_path == CALC
    with pytest.raises(upstream.UpstreamNotReady):
        wait_for_upstream(tmp_path, CALC, variables={"month": "2026-09"})


def test_health_orders_runs_by_start_time_not_the_stores_order(store):
    # Coarse file clocks (Windows) can tie, so a store may list runs out of order.
    _save(store, rec(CALC, "error", 30), rec(CALC, "success", 5))

    class Shuffled:
        def __getattr__(self, name):
            return getattr(store, name)

        def list_runs(self, task_path, n=20):
            return list(reversed(store.list_runs(task_path, n=n)))

    h = health.task_health(Shuffled(), CALC, now=NOW)
    assert h.last_status == "success" and h.healthy
