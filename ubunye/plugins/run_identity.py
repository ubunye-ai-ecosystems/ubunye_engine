"""The shipped run identity plugins: ``databricks``, ``ci`` and ``local`` (ADR 015).

Registered in the ``ubunye.run_identity`` entry point group; the contract is in
:mod:`ubunye.core.identity`. Each one returns ``{}`` when it is not on its
platform and never raises out of a method that can fail (a missing widget, a
refused notebook context, no git): the run goes on without that field.

Importing this module imports nothing from Databricks, Spark or IPython; they
are looked up when a method is called, and only when already loaded or when the
Databricks runtime says it is there.
"""

from __future__ import annotations

import builtins
import json
import logging
import os
import shutil
import subprocess
import sys
from typing import Any, Dict, Mapping, Optional

from ubunye.core.settings import KNOWN_PARAMETERS

logger = logging.getLogger(__name__)


def _str(value: Any) -> Optional[str]:
    if value is None or isinstance(value, (dict, list, tuple)):
        return None
    text = str(value).strip()
    return text or None


def _first(*values: Any) -> Optional[str]:
    for value in values:
        text = _str(value)
        if text is not None:
            return text
    return None


# ---------------------------------------------------------------------------
# Databricks
# ---------------------------------------------------------------------------


def _dbutils() -> Any:
    """The notebook's ``dbutils``, or ``None``. Never imports it off Databricks."""
    ipython = sys.modules.get("IPython")
    if ipython is not None:
        try:
            shell = ipython.get_ipython()
            found = shell.user_ns.get("dbutils") if shell is not None else None
            if found is not None:
                return found
        except Exception:
            pass
    found = getattr(builtins, "dbutils", None)
    if found is not None:
        return found
    if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
        return None
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        return DBUtils(spark) if spark is not None else None
    except Exception:
        return None


def _widgets(dbutils: Any) -> Dict[str, str]:
    """Every widget (job parameter) the notebook was given."""
    try:
        widgets = dbutils.widgets
    except Exception:
        return {}
    try:
        get_all = getattr(widgets, "getAll", None)
        if callable(get_all):
            return {str(k): str(v) for k, v in dict(get_all()).items() if v is not None}
    except Exception as exc:
        logger.debug("dbutils.widgets.getAll failed, asking by name: %s", exc)
    found: Dict[str, str] = {}
    for name in KNOWN_PARAMETERS:
        try:
            value = widgets.get(name)
        except Exception:  # the job did not pass it
            continue
        if value is not None:
            found[name] = str(value)
    return found


def _norm(key: str) -> str:
    return key.replace("_", "").replace("-", "").lower()


def _notebook_context(dbutils: Any) -> Dict[str, Any]:
    """The notebook context as one flat dict with normalised keys, or ``{}``.

    ``safeToJson`` first (it is what serverless and shared compute allow), then
    ``toJson``. Only scalar values are kept; a field is read from it by name.
    """
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    except Exception:
        return {}
    for method in ("safeToJson", "toJson"):
        try:
            doc = json.loads(getattr(ctx, method)())
        except Exception:
            continue
        if not isinstance(doc, dict):
            continue
        flat: Dict[str, Any] = {}
        for part in (doc, doc.get("extraContext"), doc.get("tags"), doc.get("attributes")):
            if isinstance(part, dict):
                for key, value in part.items():
                    if _str(value) is not None:
                        flat[_norm(str(key))] = value
        if flat:
            return flat
    return {}


class DatabricksIdentity:
    """Databricks: job parameters (widgets) and the notebook's job context.

    A job parameter named like a field (``git_commit``, ``job_run_id``, ...)
    beats the context, so a bundle can always say what it deployed.
    """

    name = "databricks"
    priority = 10

    def parameters(self) -> Mapping[str, str]:
        dbutils = _dbutils()
        return _widgets(dbutils) if dbutils is not None else {}

    def identity(self, parameters: Mapping[str, str], task_dir: Optional[str]) -> Mapping[str, Any]:
        dbutils = _dbutils()
        if dbutils is None and "DATABRICKS_RUNTIME_VERSION" not in os.environ:
            return {}
        ctx = _notebook_context(dbutils) if dbutils is not None else {}
        p = parameters
        job_id = _first(p.get("job_id"), ctx.get("jobid"))
        found = {
            "platform": "databricks",
            "workspace": _first(
                p.get("workspace"),
                ctx.get("browserhostname"),
                os.environ.get("DATABRICKS_HOST"),
                ctx.get("orgid"),
            ),
            "job_id": job_id,
            # In a multi-task job runId is the task's run; the job's run is the parent.
            "job_run_id": _first(
                p.get("job_run_id"),
                ctx.get("multitaskparentrunid"),
                ctx.get("jobrunid"),
                ctx.get("runid") if job_id else None,
            ),
            "task_key": _first(p.get("task_key"), ctx.get("taskkey")),
            "run_by": _first(p.get("run_by"), ctx.get("user")),
            "git_commit": _str(p.get("git_commit")),
            "git_branch": _str(p.get("git_branch")),
            "deploy_target": _str(p.get("deploy_target")),
        }
        return {k: v for k, v in found.items() if v is not None}


# ---------------------------------------------------------------------------
# CI
# ---------------------------------------------------------------------------

# platform -> (marker variable, field -> variables tried in order)
_CI: Dict[str, tuple] = {
    "github-actions": (
        "GITHUB_ACTIONS",
        {
            "workspace": ("GITHUB_REPOSITORY",),
            "job_id": ("GITHUB_WORKFLOW",),
            "job_run_id": ("GITHUB_RUN_ID",),
            "task_key": ("GITHUB_JOB",),
            "run_by": ("GITHUB_ACTOR",),
            "git_commit": ("GITHUB_SHA",),
            # A pull request's REF_NAME is "12/merge"; HEAD_REF is its branch.
            "git_branch": ("GITHUB_HEAD_REF", "GITHUB_REF_NAME"),
        },
    ),
    "gitlab-ci": (
        "GITLAB_CI",
        {
            "workspace": ("CI_PROJECT_PATH",),
            "job_run_id": ("CI_PIPELINE_ID",),
            "task_key": ("CI_JOB_NAME",),
            "run_by": ("GITLAB_USER_LOGIN",),
            "git_commit": ("CI_COMMIT_SHA",),
            "git_branch": ("CI_COMMIT_REF_NAME",),
            "deploy_target": ("CI_ENVIRONMENT_NAME",),
        },
    ),
    "azure-pipelines": (
        "TF_BUILD",
        {
            "workspace": ("SYSTEM_TEAMPROJECT",),
            "job_id": ("BUILD_DEFINITIONNAME",),
            "job_run_id": ("BUILD_BUILDID",),
            "task_key": ("SYSTEM_JOBDISPLAYNAME",),
            "run_by": ("BUILD_REQUESTEDFOR",),
            "git_commit": ("BUILD_SOURCEVERSION",),
            "git_branch": ("BUILD_SOURCEBRANCHNAME",),
        },
    ),
    "ci": (
        "CI",
        {
            "job_run_id": ("BUILD_ID", "BUILD_NUMBER"),
            "task_key": ("JOB_NAME",),
            "git_commit": ("GIT_COMMIT",),
            "git_branch": ("GIT_BRANCH",),
        },
    ),
}


class CiIdentity:
    """CI services, from the variables they set (GitHub Actions, GitLab, Azure, generic CI)."""

    name = "ci"
    priority = 20

    def parameters(self) -> Mapping[str, str]:
        return {}

    def identity(self, parameters: Mapping[str, str], task_dir: Optional[str]) -> Mapping[str, Any]:
        env = os.environ
        for platform, (marker, fields) in _CI.items():
            if not _str(env.get(marker)) or env.get(marker, "").lower() in {"false", "0"}:
                continue
            found = {"platform": platform}
            for field, names in fields.items():
                value = _first(*(env.get(n) for n in names))
                if value is not None:
                    found[field] = value
            return found
        return {}


# ---------------------------------------------------------------------------
# Local
# ---------------------------------------------------------------------------


def _git(where: Optional[str], *args: str) -> Optional[str]:
    if shutil.which("git") is None:
        return None
    cwd = where if where and os.path.isdir(where) else None
    try:
        out = subprocess.run(
            ["git", *args],
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=3,
            env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
        )
    except Exception:
        return None
    return _str(out.stdout) if out.returncode == 0 else None


class LocalIdentity:
    """Any machine: the user, the host, and the git checkout the task is in."""

    name = "local"
    priority = 90

    def parameters(self) -> Mapping[str, str]:
        return {}

    def identity(self, parameters: Mapping[str, str], task_dir: Optional[str]) -> Mapping[str, Any]:
        import getpass
        import socket

        try:
            user: Optional[str] = getpass.getuser()
        except Exception:
            user = None
        try:
            host: Optional[str] = socket.gethostname()
        except Exception:
            host = None
        commit = _git(task_dir, "rev-parse", "HEAD")
        branch = _git(task_dir, "rev-parse", "--abbrev-ref", "HEAD") if commit else None
        found = {
            "platform": "local",
            "workspace": _str(host),
            "run_by": _str(user),
            "git_commit": commit,
            "git_branch": None if branch == "HEAD" else branch,  # detached
        }
        return {k: v for k, v in found.items() if v is not None}
