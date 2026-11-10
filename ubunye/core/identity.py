"""Who and where ran this: the run identity (ADR 015).

A run record says what ran (config, code, inputs). This module adds who and
where: the platform, its workspace, the job and its run, the task, the person or
service that ran it, and the code it ran from (git commit and branch, and the
deploy target).

Every platform is a plugin in the ``ubunye.run_identity`` entry point group. The
shipped ones are ``databricks``, ``ci`` and ``local``. A provider is built with no
arguments and has:

* ``name`` (str) and ``priority`` (int, lower is asked first);
* ``parameters() -> Mapping[str, str]``: the job parameters its platform passed
  (on Databricks, the notebook widgets); ``{}`` when it is not on its platform;
* ``identity(parameters, task_dir) -> Mapping[str, str | None]``: what it found,
  with ``platform`` set; ``{}`` when it is not on its platform.

The first provider that finds its platform owns the run fields (platform,
workspace, job, run, task, run_by, deploy target). Only ``git_commit`` and
``git_branch`` may come from a later one: a job that does not pass its commit
can still get it from the git checkout it runs in.

Nothing here imports an engine or a platform SDK, and nothing here raises: a
provider that fails is logged and skipped, and a run is never stopped because
its identity could not be read.
"""

from __future__ import annotations

import importlib.metadata as md
import logging
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Protocol, runtime_checkable

if TYPE_CHECKING:  # pragma: no cover
    from ubunye.core.settings import JobSettings

logger = logging.getLogger(__name__)

GROUP = "ubunye.run_identity"

#: The fields of a run identity, in the order they are shown.
IDENTITY_FIELDS = (
    "platform",
    "workspace",
    "job_id",
    "job_run_id",
    "task_key",
    "run_by",
    "git_commit",
    "git_branch",
    "deploy_target",
)

#: The fields a later provider may fill when the platform's provider did not.
_FALL_THROUGH = ("git_commit", "git_branch")


@dataclass(frozen=True)
class RunIdentity:
    """Who and where ran this. A field that was not found is ``None``."""

    platform: Optional[str] = None
    workspace: Optional[str] = None
    job_id: Optional[str] = None
    job_run_id: Optional[str] = None
    task_key: Optional[str] = None
    run_by: Optional[str] = None
    git_commit: Optional[str] = None
    git_branch: Optional[str] = None
    deploy_target: Optional[str] = None

    def as_dict(self) -> Dict[str, str]:
        """The fields that were found; a missing one is left out."""
        return {k: v for k, v in asdict(self).items() if v is not None}


@runtime_checkable
class RunIdentityProvider(Protocol):
    """What a ``ubunye.run_identity`` plugin provides (see the module docstring)."""

    name: str
    priority: int

    def parameters(self) -> Mapping[str, str]: ...

    def identity(
        self, parameters: Mapping[str, str], task_dir: Optional[str]
    ) -> Mapping[str, Any]: ...


def providers() -> List[Any]:
    """Every installed provider, built, in the order they are asked."""
    found: List[Any] = []
    for ep in md.entry_points(group=GROUP):
        try:
            obj = ep.load()()
        except Exception as exc:  # a broken plugin must not stop a run
            logger.warning("run identity plugin %r could not be loaded: %s", ep.name, exc)
            continue
        if not getattr(obj, "name", None):
            try:
                obj.name = ep.name
            except Exception:
                pass
        found.append(obj)
    return sorted(found, key=lambda p: (int(getattr(p, "priority", 100)), str(p.name)))


def _clean(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def collect_identity(
    settings: Optional["JobSettings"] = None, *, task_dir: Optional[str] = None
) -> RunIdentity:
    """Who and where ran this, from every installed provider. Never raises.

    ``settings`` is the job's settings (:func:`ubunye.core.settings.job_settings`);
    its ``parameters`` are handed to the providers. When it is not given it is
    read here. ``task_dir`` is the task folder, where the local provider looks
    for a git checkout (the working directory when not given).
    """
    try:
        if settings is None:
            from ubunye.core.settings import job_settings

            settings = job_settings()
        params: Mapping[str, str] = dict(settings.parameters)
    except Exception as exc:  # pragma: no cover - job_settings does not raise for a platform
        logger.debug("run identity: settings could not be read: %s", exc)
        params = {}

    found: Dict[str, str] = {}
    owner: Optional[str] = None
    for provider in providers():
        try:
            got = provider.identity(params, task_dir) or {}
        except Exception as exc:
            logger.debug("run identity: %r failed: %s", getattr(provider, "name", "?"), exc)
            continue
        values = {k: _clean(got.get(k)) for k in IDENTITY_FIELDS}
        if owner is None:
            if not values.get("platform"):
                continue
            owner = provider.name
            found = {k: v for k, v in values.items() if v is not None}
            continue
        for key in _FALL_THROUGH:
            if key not in found and values.get(key):
                found[key] = values[key]  # type: ignore[assignment]
    return RunIdentity(**found)
