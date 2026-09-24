"""``ubunye mcp``: an agent can list, plan, run and gate tasks, and nothing more.

Most tests talk to the server in process (the MCP SDK's own client); one starts
the ``ubunye mcp`` command and talks to it over stdio, as an agent would, with a task
that prints, which must not break the protocol on stdout.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
from pathlib import Path

import pytest

try:  # the mcp extra needs pydantic 2.12, so the minimum-versions job has none
    import anyio
    from mcp import Client
    from mcp.client.stdio import StdioServerParameters
except Exception:  # pragma: no cover
    pytest.skip("the mcp extra is not installed", allow_module_level=True)

pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

from ubunye.mcp_server import build_server  # noqa: E402

READ_ONLY = {"tasks", "doctor", "plan", "runs", "record", "gate", "focus"}
EXAMPLE = Path(__file__).resolve().parents[2] / "examples" / "production" / "llm_replay"

TRANSFORM = """\
from ubunye.core.interfaces import Task


class Copy(Task):
    def transform(self, sources):
        print("a task that prints to stdout")
        return {"out": sources["src"]}
"""


@pytest.fixture
def root(tmp_path, monkeypatch):
    (tmp_path / "in.csv").write_text("id,v\n1,a\n2,b\n", encoding="utf-8")
    task = tmp_path / "shop" / "orders" / "copy"
    task.mkdir(parents=True)
    (task / "transformations.py").write_text(TRANSFORM, encoding="utf-8")
    (task / "config.yaml").write_text(
        f"""\
CONFIG:
  inputs:
    src: {{format: s3, path: "{(tmp_path / 'in.csv').as_posix()}", file_format: csv,
           options: {{header: "true"}}}}
  outputs:
    out: {{format: s3, path: "{(tmp_path / 'out').as_posix()}", file_format: parquet,
           mode: overwrite}}
""",
        encoding="utf-8",
    )
    monkeypatch.delenv("UBUNYE_LLM_MODE", raising=False)
    return tmp_path


def _call(server, *calls):
    """Run tool calls in one session; each result's structured content, or the error."""

    async def go():
        out = []
        async with Client(server) as client:
            for name, args in calls:
                result = await client.call_tool(name, args)
                if result.is_error:
                    out.append({"error": result.content[0].text})
                else:
                    out.append((result.structured_content or {}).get("result"))
        return out

    return anyio.run(go)


def _tools(server):
    async def go():
        async with Client(server) as client:
            return (await client.list_tools()).tools

    return anyio.run(go)


def test_every_tool_but_run_only_reads(root):
    tools = {t.name: t for t in _tools(build_server(root))}
    assert set(tools) == READ_ONLY
    assert all(t.annotations.read_only_hint for t in tools.values())


def test_run_exists_only_when_allowed(root):
    tools = {t.name: t for t in _tools(build_server(root, allow_run=True))}
    assert set(tools) == READ_ONLY | {"run"}
    assert tools["run"].annotations.read_only_hint is False
    assert "replay" in tools["run"].description


def test_plan_run_and_gate(root):
    server = build_server(root, allow_run=True)
    listed, plan, first, second, runs, gate = _call(
        server,
        ("tasks", {}),
        ("plan", {"task": "shop/orders/copy", "backend": "pandas"}),
        ("run", {"task": "shop/orders/copy", "backend": "pandas"}),
        ("run", {"task": "shop/orders/copy", "backend": "pandas"}),
        ("runs", {"task": "shop/orders/copy"}),
        ("gate", {"task": "shop/orders/copy"}),
    )
    assert listed["tasks"] == ["shop/orders/copy"]
    assert plan["ok"] is True and [o["name"] for o in plan["outputs"]] == ["out"]
    assert first["ok"] is True and first["run"]["status"] == "success"
    assert first["run"]["outputs"][0]["rows"] == 2
    assert len(runs["runs"]) == 2
    assert gate["ok"] is True
    assert {f["rule"]: f["detail"] for f in gate["findings"]}["data"] == "identical"
    assert (root / "out").exists()


def test_a_name_that_is_a_path_is_refused(root):
    (result,) = _call(build_server(root), ("plan", {"task": "../../etc"}))
    assert "is not a task name" in result["error"]
    (result,) = _call(build_server(root), ("plan", {"task": "shop/orders/missing"}))
    assert "no task 'shop/orders/missing'" in result["error"]


def test_a_failed_run_is_reported_not_raised(root):
    (root / "in.csv").unlink()
    (result,) = _call(
        build_server(root, allow_run=True),
        ("run", {"task": "shop/orders/copy", "backend": "pandas"}),
    )
    assert result["ok"] is False and result["error"]


def test_an_agents_run_replays_model_calls_by_default(tmp_path, monkeypatch):
    shutil.copytree(EXAMPLE / "pipelines", tmp_path / "pipelines")
    monkeypatch.setenv("REVIEWS_INPUT_PATH", (EXAMPLE / "data" / "reviews.csv").as_posix())
    monkeypatch.setenv("REVIEWS_OUTPUT_PATH", (tmp_path / "out").as_posix())
    monkeypatch.setenv("UBUNYE_LLM_MODE", "live")  # the server's run overrides it
    server = build_server(tmp_path / "pipelines", allow_run=True)
    (result,) = _call(server, ("run", {"task": "shop/reviews/label", "backend": "pandas"}))
    assert result["ok"] is True, result
    assert result["run"]["llm"] == {"calls": 6, "replayed": 6, "budget": None}
    assert os.environ["UBUNYE_LLM_MODE"] == "live"  # put back afterwards


def test_over_stdio_a_printing_task_does_not_break_the_protocol(root):
    params = StdioServerParameters(
        command=sys.executable,
        # The `ubunye` command, from this interpreter (`python -m ubunye` runs one task).
        args=[
            "-c",
            "from ubunye.cli.main import app; app()",
            "mcp",
            "-d",
            str(root),
            "--allow-run",
        ],
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
    )

    async def go():
        async with Client(params, read_timeout_seconds=120) as client:
            run = await client.call_tool("run", {"task": "shop/orders/copy", "backend": "pandas"})
            record = await client.call_tool("record", {"task": "shop/orders/copy"})
            return run, record

    run, record = anyio.run(go)
    assert not run.is_error, run.content
    assert run.structured_content["result"]["ok"] is True
    doc = record.structured_content["result"]
    assert doc["status"] == "success" and json.dumps(doc)
