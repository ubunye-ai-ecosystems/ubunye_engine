"""Run identity and job settings (ADR 015).

The Databricks plugin is tested with fakes of ``dbutils``: its widgets and its
notebook context. Nothing here needs Databricks, Spark or a network.
"""

from __future__ import annotations

import builtins
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

import pytest

from ubunye.core import identity as identity_mod
from ubunye.core import settings as settings_mod
from ubunye.core.identity import IDENTITY_FIELDS, RunIdentity, collect_identity
from ubunye.core.settings import JobSettings, job_settings
from ubunye.plugins import run_identity as plugins

# --------------------------------------------------------------------------
# Fakes
# --------------------------------------------------------------------------


class _WidgetMissing(Exception):
    """What dbutils.widgets.get raises for a widget the job did not pass."""


class FakeWidgets:
    def __init__(self, values: Mapping[str, str], *, get_all: bool = True) -> None:
        self._values = dict(values)
        if not get_all:
            self.getAll = None  # type: ignore[assignment]

    def getAll(self) -> Dict[str, str]:  # noqa: N802 - the dbutils name
        return dict(self._values)

    def get(self, name: str) -> str:
        if name not in self._values:
            raise _WidgetMissing(name)
        return self._values[name]


class FakeContext:
    def __init__(self, doc: Optional[Dict[str, Any]], *, safe: bool = True) -> None:
        self._doc = doc
        self._safe = safe

    def safeToJson(self) -> str:  # noqa: N802
        if not self._safe:
            raise RuntimeError("py4j.security.Py4JSecurityException: not whitelisted")
        return json.dumps({"attributes": self._doc})

    def toJson(self) -> str:  # noqa: N802
        if self._doc is None:
            raise RuntimeError("no context")
        return json.dumps({"tags": self._doc, "extraContext": {}})


class FakeDbutils:
    """Just enough of dbutils: widgets and the notebook context."""

    def __init__(self, widgets: FakeWidgets, context: FakeContext) -> None:
        self.widgets = widgets
        ctx = context

        class _Notebook:
            def getContext(self) -> FakeContext:  # noqa: N802
                return ctx

        class _Inner:
            def notebook(self) -> _Notebook:
                return _Notebook()

        class _EntryPoint:
            def getDbutils(self) -> _Inner:  # noqa: N802
                return _Inner()

        class _NotebookUtils:
            entry_point = _EntryPoint()

        self.notebook = _NotebookUtils()


JOB_CONTEXT = {
    "jobId": "123",
    "multitaskParentRunId": "456",
    "runId": "789",
    "taskKey": "claims_etl",
    "user": "svc-ubunye@example.com",
    "browserHostName": "adb-1.2.azuredatabricks.net",
}


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    """No CI, no Databricks, no ubunye settings in the environment."""
    for name in list(__import__("os").environ):
        if (
            name.startswith(("UBUNYE_", "GITHUB_", "GITLAB_", "CI_", "BUILD_", "SYSTEM_"))
            or name in {"CI", "TF_BUILD", "DATABRICKS_RUNTIME_VERSION", "GIT_COMMIT"}
            or name == "GIT_BRANCH"
        ):
            monkeypatch.delenv(name, raising=False)
    monkeypatch.delattr(builtins, "dbutils", raising=False)
    return monkeypatch


@pytest.fixture
def on_databricks(clean_env: pytest.MonkeyPatch):
    """Install a fake dbutils where a notebook finds it; return a setter."""

    def install(params: Mapping[str, str], context: Optional[Dict[str, Any]] = JOB_CONTEXT, **kw):
        fake = FakeDbutils(
            FakeWidgets(params, get_all=kw.get("get_all", True)),
            FakeContext(context, safe=kw.get("safe", True)),
        )
        clean_env.setattr(builtins, "dbutils", fake, raising=False)
        clean_env.setenv("DATABRICKS_RUNTIME_VERSION", "client.4.9")
        return fake

    return install


# --------------------------------------------------------------------------
# Core contract
# --------------------------------------------------------------------------


def test_core_identity_and_settings_import_no_engine() -> None:
    code = (
        "import sys; import ubunye.core.identity, ubunye.core.settings; "
        "bad = [m for m in ('pyspark', 'pandas', 'pyarrow') if m in sys.modules]; "
        "print(','.join(bad))"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == ""


def test_the_databricks_plugin_imports_nothing_from_databricks() -> None:
    code = (
        "import sys, builtins; import ubunye.plugins.run_identity; "
        "bad = [m for m in ('pyspark', 'pyspark.dbutils', 'IPython') if m in sys.modules]; "
        "print(','.join(bad))"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == ""


def test_the_shipped_providers_are_entry_points() -> None:
    names = {p.name for p in identity_mod.providers()}
    assert {"databricks", "ci", "local"} <= names
    order = [p.name for p in identity_mod.providers()]
    assert order.index("databricks") < order.index("ci") < order.index("local")


def test_as_dict_omits_what_was_not_found() -> None:
    ident = RunIdentity(platform="local", git_commit="abc")
    assert ident.as_dict() == {"platform": "local", "git_commit": "abc"}
    assert set(IDENTITY_FIELDS) == set(RunIdentity.__dataclass_fields__)


class _Boom:
    name = "boom"
    priority = 1

    def parameters(self) -> Mapping[str, str]:
        raise RuntimeError("broken plugin")

    def identity(self, parameters: Mapping[str, str], task_dir: Optional[str]) -> Mapping[str, str]:
        raise RuntimeError("broken plugin")


class _Platform:
    def __init__(self, name: str, priority: int, found: Dict[str, Optional[str]]) -> None:
        self.name, self.priority, self._found = name, priority, found

    def parameters(self) -> Mapping[str, str]:
        return {}

    def identity(self, parameters: Mapping[str, str], task_dir: Optional[str]) -> Mapping[str, Any]:
        return dict(self._found)


def test_a_broken_provider_never_fails_the_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        identity_mod, "providers", lambda: [_Boom(), _Platform("x", 5, {"platform": "x"})]
    )
    assert collect_identity(JobSettings()).platform == "x"
    assert job_settings().run_records is None


def test_the_first_platform_owns_the_run_fields_only_git_falls_through(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _Platform("plat", 10, {"platform": "plat", "job_id": "1", "run_by": None})
    later = _Platform(
        "local",
        90,
        {"platform": "local", "run_by": "root", "git_commit": "c0ffee", "git_branch": "b"},
    )
    monkeypatch.setattr(identity_mod, "providers", lambda: [first, later])
    ident = collect_identity(JobSettings())
    assert ident.platform == "plat"
    assert ident.job_id == "1"
    assert ident.run_by is None  # not borrowed from the laptop provider
    assert ident.git_commit == "c0ffee" and ident.git_branch == "b"


def test_unknown_fields_from_a_provider_are_dropped(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        identity_mod,
        "providers",
        lambda: [_Platform("p", 1, {"platform": "p", "password": "x", "job_id": 7})],
    )
    assert collect_identity(JobSettings()).as_dict() == {"platform": "p", "job_id": "7"}


# --------------------------------------------------------------------------
# Settings reader
# --------------------------------------------------------------------------


def test_settings_off_databricks_read_the_environment(clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("UBUNYE_RUN_RECORDS", "main.ops.ubunye_runs")
    clean_env.setenv("UBUNYE_DT", "2026-11-01")
    s = job_settings()
    assert s.run_records == "main.ops.ubunye_runs"
    assert s.dt == "2026-11-01"
    assert s.mode is None and s.run_records_fallback is None
    assert s.source == {"run_records": "env", "dt": "env"}
    assert s.parameters == {}


def test_settings_with_nothing_set_are_empty(clean_env: pytest.MonkeyPatch) -> None:
    s = job_settings()
    assert (s.run_records, s.run_records_fallback, s.dt, s.mode) == (None, None, None, None)
    assert s.source == {}


def test_explicit_beats_job_parameter_beats_env(on_databricks, clean_env) -> None:
    on_databricks(
        {"ubunye_run_records": "main.ops.from_job", "dt": "2026-11-02", "ubunye_mode": ""}
    )
    clean_env.setenv("UBUNYE_RUN_RECORDS", "main.ops.from_env")
    clean_env.setenv("UBUNYE_DT", "2026-01-01")
    clean_env.setenv("UBUNYE_MODE", "PROD")
    s = job_settings({"dt": "2026-12-31", "mode": None})
    assert s.run_records == "main.ops.from_job"
    assert s.dt == "2026-12-31"
    assert s.mode == "PROD"  # an empty widget and a None explicit are both unset
    assert s.source == {"run_records": "job_param", "dt": "explicit", "mode": "env"}


def test_an_unknown_explicit_setting_is_an_error(clean_env) -> None:
    with pytest.raises(ValueError, match="run_record"):
        job_settings({"run_record": "x"})


def test_settings_on_serverless_without_get_all(on_databricks) -> None:
    """Older runtimes have no widgets.getAll; each known name is asked for."""
    on_databricks(
        {"ubunye_run_records": "/Volumes/main/ops/runs", "git_commit": "abc123"}, get_all=False
    )
    s = job_settings()
    assert s.run_records == "/Volumes/main/ops/runs"
    assert s.parameters["git_commit"] == "abc123"


# --------------------------------------------------------------------------
# Databricks plugin
# --------------------------------------------------------------------------


def test_databricks_identity_from_the_job_context_and_parameters(on_databricks) -> None:
    on_databricks({"git_commit": "a1b2c3", "git_branch": "main", "deploy_target": "prod"})
    ident = collect_identity()
    assert ident.as_dict() == {
        "platform": "databricks",
        "workspace": "adb-1.2.azuredatabricks.net",
        "job_id": "123",
        "job_run_id": "456",
        "task_key": "claims_etl",
        "run_by": "svc-ubunye@example.com",
        "git_commit": "a1b2c3",
        "git_branch": "main",
        "deploy_target": "prod",
    }


def test_job_parameters_beat_the_context(on_databricks) -> None:
    on_databricks({"job_run_id": "999", "task_key": "override"})
    ident = collect_identity()
    assert ident.job_run_id == "999" and ident.task_key == "override"
    assert ident.job_id == "123"


def test_serverless_context_through_to_json_when_safe_is_refused(on_databricks) -> None:
    on_databricks({}, safe=False)
    assert collect_identity().job_id == "123"


def test_an_interactive_notebook_has_no_job_ids(on_databricks) -> None:
    on_databricks({}, context={"user": "me@example.com", "browserHostName": "adb-1"})
    ident = collect_identity()
    assert ident.platform == "databricks"
    assert ident.job_id is None and ident.job_run_id is None
    assert ident.run_by == "me@example.com"


def test_a_single_task_run_id_is_used_only_with_a_job(on_databricks) -> None:
    on_databricks({}, context={"jobId": "5", "runId": "77"})
    assert collect_identity().job_run_id == "77"


def test_databricks_without_dbutils_is_still_databricks(clean_env) -> None:
    clean_env.setenv("DATABRICKS_RUNTIME_VERSION", "17.3")
    provider = plugins.DatabricksIdentity()
    assert provider.parameters() == {}
    assert provider.identity({}, None) == {"platform": "databricks"}


def test_databricks_off_platform_is_nothing(clean_env) -> None:
    provider = plugins.DatabricksIdentity()
    assert provider.parameters() == {}
    assert provider.identity({"git_commit": "x"}, None) == {}


def test_a_dbutils_that_raises_everywhere_degrades_to_nothing(clean_env) -> None:
    class Hostile:
        def __getattr__(self, name: str) -> Any:
            raise RuntimeError("not available on this compute")

    clean_env.setattr(builtins, "dbutils", Hostile(), raising=False)
    clean_env.setenv("DATABRICKS_RUNTIME_VERSION", "client.1")
    assert job_settings().parameters == {}
    assert collect_identity().platform == "databricks"


# --------------------------------------------------------------------------
# CI and local plugins
# --------------------------------------------------------------------------


def test_github_actions(clean_env) -> None:
    for k, v in {
        "GITHUB_ACTIONS": "true",
        "GITHUB_REPOSITORY": "org/repo",
        "GITHUB_WORKFLOW": "ci",
        "GITHUB_RUN_ID": "42",
        "GITHUB_JOB": "unit",
        "GITHUB_ACTOR": "dev",
        "GITHUB_SHA": "deadbeef",
        "GITHUB_REF_NAME": "main",
    }.items():
        clean_env.setenv(k, v)
    assert collect_identity().as_dict() == {
        "platform": "github-actions",
        "workspace": "org/repo",
        "job_id": "ci",
        "job_run_id": "42",
        "task_key": "unit",
        "run_by": "dev",
        "git_commit": "deadbeef",
        "git_branch": "main",
    }


def test_a_pull_request_branch_is_the_head_branch(clean_env) -> None:
    clean_env.setenv("GITHUB_ACTIONS", "true")
    clean_env.setenv("GITHUB_REF_NAME", "12/merge")
    clean_env.setenv("GITHUB_HEAD_REF", "feat/x")
    assert collect_identity().git_branch == "feat/x"


def test_gitlab(clean_env) -> None:
    clean_env.setenv("GITLAB_CI", "true")
    clean_env.setenv("CI_COMMIT_SHA", "f00")
    clean_env.setenv("CI_ENVIRONMENT_NAME", "prod")
    ident = collect_identity()
    assert (ident.platform, ident.git_commit, ident.deploy_target) == ("gitlab-ci", "f00", "prod")


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True)


@pytest.mark.skipif(__import__("shutil").which("git") is None, reason="needs git")
def test_local_reads_the_task_folders_repo(clean_env, tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    task = repo / "uc" / "pkg" / "task"
    task.mkdir(parents=True)
    _git(repo, "init", "-q", "-b", "trunk")
    (task / "config.yaml").write_text("x: 1\n")
    _git(repo, "add", ".")
    _git(repo, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "c")
    sha = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True, check=True
    ).stdout.strip()
    ident = collect_identity(task_dir=str(task))
    assert ident.platform == "local"
    assert ident.git_commit == sha and ident.git_branch == "trunk"
    assert ident.run_by and ident.workspace


def test_local_outside_a_repo_has_no_git(clean_env, tmp_path: Path) -> None:
    clean_env.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    ident = collect_identity(task_dir=str(tmp_path))
    assert ident.platform == "local"
    assert ident.git_commit is None and ident.git_branch is None


def test_settings_module_has_no_platform_code() -> None:
    src = Path(settings_mod.__file__).read_text() + Path(identity_mod.__file__).read_text()
    assert "dbutils" not in src.replace("``dbutils``", "")
