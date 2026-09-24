"""OpenLineage events: valid by the spec, carrying the receipt, never failing a run.

Every event built here is validated offline against the OpenLineage 2-0-2 spec and
the facet schemas (vendored in tests/data/openlineage), and the engine's own two
facets against docs/schemas.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

from ubunye.lineage.context import RunContext, StepRecord
from ubunye.lineage.openlineage import Emitter, dataset_id, events, run_event

jsonschema = pytest.importorskip("jsonschema")
referencing = pytest.importorskip("referencing")

ROOT = Path(__file__).resolve().parents[3]
SPEC = ROOT / "tests" / "data" / "openlineage"
OURS = ROOT / "docs" / "schemas"


def _registry():
    from referencing import Registry, Resource

    spec = json.loads((SPEC / "OpenLineage.json").read_text(encoding="utf-8"))
    resources = []
    # The facet schemas point at 1-0-2 of the core spec; its $defs are the same.
    for version in ("2-0-2", "1-0-2"):
        resources.append((f"https://openlineage.io/spec/{version}/OpenLineage.json", spec))
    for path in list(SPEC.glob("*Facet.json")) + list(OURS.glob("*.json")):
        doc = json.loads(path.read_text(encoding="utf-8"))
        resources.append((doc["$id"], doc))
    return Registry().with_resources(
        (uri, Resource.from_contents(doc, default_specification=referencing.jsonschema.DRAFT202012))
        for uri, doc in resources
    )


REGISTRY = _registry()


def validate(event):
    """The event against RunEvent, and every facet against its own schema."""
    jsonschema.Draft202012Validator(
        {"$ref": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"},
        registry=REGISTRY,
    ).validate(event)
    facets = list(event["run"].get("facets", {}).values())
    for ds in event["inputs"] + event["outputs"]:
        for group in ("facets", "inputFacets", "outputFacets"):
            facets += list(ds.get(group, {}).values())
    for facet in facets:
        jsonschema.Draft202012Validator({"$ref": facet["_schemaURL"]}, registry=REGISTRY).validate(
            facet
        )
    return event


def _record(status="success", expectations=()):
    ctx = RunContext(
        run_id="6f1a2b3c-4d5e-4f60-8a1b-2c3d4e5f6a7b",
        task_path="shop/orders/clean",
        usecase="shop",
        package="orders",
        task_name="clean",
        profile="default",
        model="etl",
        version="1.0.0",
        config_hash="sha256:c",
        started_at="2026-09-24T10:00:00Z",
        ended_at="2026-09-24T10:00:05Z",
        duration_sec=5.0,
        status=status,
        engine_version="0.7.0",
        backend="pandas",
        code_hash="sha256:code",
        environment={"python": "3.12.0", "packages": {"pandas": "3.0.6"}},
        environment_hash="sha256:env",
        timings=[{"step": "Reader:s3", "input": "raw", "seconds": 0.1}],
        expectations=list(expectations),
        error="Expectations failed" if status == "error" else None,
    )
    ctx.inputs = [
        StepRecord(
            "raw", "input", "s3", "s3a://lake/raw/orders", 3, "sha256:s", "sha256:d", "rows-v1"
        )
    ]
    ctx.outputs = [
        StepRecord(
            "clean",
            "output",
            "s3",
            "gs://lake/clean/orders",
            2,
            "sha256:s2",
            "sha256:d2",
            "rows-v1",
        )
    ]
    return ctx


# --- the events -------------------------------------------------------------------


def test_a_finished_run_is_a_valid_start_and_complete():
    start, complete = [validate(e) for e in events(_record())]
    assert (start["eventType"], complete["eventType"]) == ("START", "COMPLETE")
    assert start["run"]["runId"] == complete["run"]["runId"]
    assert complete["job"] == {"namespace": "ubunye", "name": "shop.orders.clean"}
    assert complete["producer"].endswith("/tree/v0.7.0")


def test_the_complete_event_carries_the_receipt():
    event = run_event(_record(), "COMPLETE")
    evidence = event["run"]["facets"]["ubunye_evidence"]
    assert evidence["configHash"] == "sha256:c" and evidence["codeHash"] == "sha256:code"
    assert evidence["environmentHash"] == "sha256:env" and evidence["timings"]
    [inp] = event["inputs"]
    [out] = event["outputs"]
    assert inp["inputFacets"]["dataQualityMetrics"]["rowCount"] == 3
    assert out["outputFacets"]["outputStatistics"]["rowCount"] == 2
    assert out["facets"]["ubunye_hash"]["dataHash"] == "sha256:d2"


def test_the_start_event_names_the_datasets_without_results():
    event = validate(run_event(_record(), "START"))
    assert "facets" not in event["outputs"][0]
    assert "timings" not in event["run"]["facets"]["ubunye_evidence"]


def test_a_failed_run_says_why_and_which_expectation_broke():
    broken = {
        "output": "clean",
        "rule": "qty_between",
        "kind": "between",
        "severity": "fail",
        "column": "qty",
        "failed": 1,
        "total": 3,
        "passed": False,
    }
    start, fail = [validate(e) for e in events(_record("error", [broken]))]
    assert fail["eventType"] == "FAIL"
    assert fail["run"]["facets"]["errorMessage"]["message"] == "Expectations failed"
    assertions = fail["outputs"][0]["facets"]["dataQualityAssertions"]["assertions"]
    assert assertions == [{"assertion": "qty_between", "success": False, "column": "qty"}]


# --- dataset names ----------------------------------------------------------------


@pytest.mark.parametrize(
    "fmt, location, expected",
    [
        ("s3", "s3a://lake/raw/orders", ("s3://lake", "raw/orders")),
        ("s3", "s3://lake/raw", ("s3://lake", "raw")),
        ("s3", "gs://bucket/a/b", ("gs://bucket", "a/b")),
        (
            "s3",
            "abfss://c@acct.dfs.core.windows.net/p",
            ("abfss://c@acct.dfs.core.windows.net", "p"),
        ),
        ("s3", "/data/in.csv", ("file", "/data/in.csv")),
        ("s3", "file:///data/in.csv", ("file", "/data/in.csv")),
        ("s3", "C:/data/in.csv", ("file", "C:/data/in.csv")),
        ("hive", "sales.orders", ("hive", "sales.orders")),
        (
            "jdbc",
            "jdbc:postgresql://u:pw@db:5432/shop/public.orders",
            ("postgresql://db:5432", "shop.public.orders"),
        ),
        (
            "rest_api",
            "https://token@api.example.com/v1/items",
            ("https://api.example.com", "/v1/items"),
        ),
        ("s3", "secret://env/PATH", ("secret", "secret://env/PATH")),
    ],
)
def test_datasets_are_named_by_the_openlineage_conventions(fmt, location, expected):
    got = dataset_id(StepRecord("x", "input", fmt, location))
    assert (got["namespace"], got["name"]) == expected


def test_credentials_never_reach_an_event():
    step = StepRecord("x", "input", "jdbc", "jdbc:postgresql://reporter:hunter2@db:5432/shop/t")
    assert "hunter2" not in json.dumps(dataset_id(step))


def test_unity_uses_the_workspace_host(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "https://dbc-1.cloud.databricks.com/")
    got = dataset_id(StepRecord("x", "output", "unity", "main.sales.orders"))
    assert got == {
        "namespace": "databricks://dbc-1.cloud.databricks.com",
        "name": "main.sales.orders",
    }


# --- sending ------------------------------------------------------------------------


def test_no_configuration_no_emitter(monkeypatch):
    for var in ("OPENLINEAGE_URL", "UBUNYE_OPENLINEAGE_FILE"):
        monkeypatch.delenv(var, raising=False)
    assert Emitter.from_env() is None
    monkeypatch.setenv("OPENLINEAGE_URL", "http://x")
    monkeypatch.setenv("OPENLINEAGE_DISABLED", "true")
    assert Emitter.from_env() is None


def test_http_posts_each_event_with_the_key():
    received = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            length = int(self.headers["Content-Length"])
            received.append(
                (self.path, self.headers.get("Authorization"), json.loads(self.rfile.read(length)))
            )
            self.send_response(201)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        emitter = Emitter(url=f"http://127.0.0.1:{server.server_port}", api_key="k")
        emitter.emit(_record(), "START")
        emitter.emit(_record(), "COMPLETE")
    finally:
        server.shutdown()
    assert [(p, a, e["eventType"]) for p, a, e in received] == [
        ("/api/v1/lineage", "Bearer k", "START"),
        ("/api/v1/lineage", "Bearer k", "COMPLETE"),
    ]


def test_a_server_that_is_down_does_not_raise(caplog):
    Emitter(url="http://127.0.0.1:9", timeout=0.5).emit(_record(), "START")
    assert "refused the START event" in caplog.text


# --- end to end: a real run, events to a file ---------------------------------------

pd = pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

from typer.testing import CliRunner  # noqa: E402

import ubunye  # noqa: E402
from ubunye.cli.main import app  # noqa: E402

TRANSFORM = """\
from ubunye.core.interfaces import Task


class Copy(Task):
    def transform(self, sources):
        return {"out": sources["src"]}
"""


def _task(root: Path) -> Path:
    task = root / "uc" / "pkg" / "t"
    task.mkdir(parents=True)
    (root / "in.csv").write_text("id\n1\n2\n3\n", encoding="utf-8")
    (task / "transformations.py").write_text(TRANSFORM, encoding="utf-8")
    (task / "config.yaml").write_text(
        f"""\
CONFIG:
  inputs:
    src: {{format: s3, path: "{(root / 'in.csv').as_posix()}", file_format: csv, options: {{header: "true"}}}}
  outputs:
    out: {{format: s3, path: "{(root / 'out').as_posix()}", file_format: parquet, mode: overwrite}}
""",
        encoding="utf-8",
    )
    return task


def test_a_recorded_run_emits_valid_events(tmp_path, monkeypatch):
    sink = tmp_path / "events.jsonl"
    monkeypatch.delenv("OPENLINEAGE_URL", raising=False)
    monkeypatch.setenv("UBUNYE_OPENLINEAGE_FILE", str(sink))
    task = _task(tmp_path)
    ubunye.run_task(str(task), backend="pandas", lineage=True, lineage_dir=str(tmp_path / "lin"))

    start, complete = [validate(json.loads(line)) for line in sink.read_text().splitlines()]
    assert (start["eventType"], complete["eventType"]) == ("START", "COMPLETE")
    assert complete["job"]["name"] == "uc.pkg.t"
    assert complete["outputs"][0]["outputFacets"]["outputStatistics"]["rowCount"] == 3
    # A copy: the input and the output hash the same.
    assert (
        complete["inputs"][0]["facets"]["ubunye_hash"]["dataHash"]
        == complete["outputs"][0]["facets"]["ubunye_hash"]["dataHash"]
    )


def test_stored_runs_export_as_events(tmp_path, monkeypatch):
    monkeypatch.delenv("UBUNYE_OPENLINEAGE_FILE", raising=False)
    monkeypatch.delenv("OPENLINEAGE_URL", raising=False)
    task = _task(tmp_path)
    for _ in range(2):
        ubunye.run_task(str(task), backend="pandas", lineage=True)
    result = CliRunner().invoke(
        app, ["lineage", "openlineage", "-d", str(tmp_path), "-u", "uc", "-p", "pkg", "-t", "t"]
    )
    assert result.exit_code == 0, result.output
    lines = [validate(json.loads(line)) for line in result.output.splitlines()]
    assert [e["eventType"] for e in lines] == ["START", "COMPLETE", "START", "COMPLETE"]
