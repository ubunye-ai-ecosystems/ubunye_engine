"""The job's Ubunye settings, read in one place (ADR 015).

A notebook or ``run_task`` needs a few settings from the job that runs it: where
run records go, the batch date, the mode. They come from three places, first
found wins:

1. **explicit**: what the code passed (``job_settings({"dt": ...})``);
2. **job parameters**: what the job passed to the task. On Databricks these are
   the notebook's widgets, and on serverless compute they are the only channel,
   because a job cannot set environment variables there. The platform plugins
   (``ubunye.run_identity`` entry points) read them; this module never touches
   a platform.
3. **environment variables**, for clusters, laptops and CI.

An empty job parameter counts as not set, so a widget with no value falls
through to the environment. Off Databricks there are no job parameters, and the
reader still works. It never raises for a platform reason; it raises
``ValueError`` only for an explicit setting it does not know (a typo).

=========================  ===============================  ===============================
setting                    job parameter                    environment variable
=========================  ===============================  ===============================
``run_records``            ``ubunye_run_records``           ``UBUNYE_RUN_RECORDS``
``run_records_fallback``   ``ubunye_run_records_fallback``  ``UBUNYE_RUN_RECORDS_FALLBACK``
``dt``                     ``dt``                           ``UBUNYE_DT``
``mode``                   ``ubunye_mode``                  ``UBUNYE_MODE``
=========================  ===============================  ===============================
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional

from ubunye.core import identity as _identity

logger = logging.getLogger(__name__)

#: setting -> (job parameter, environment variable)
SETTINGS: Dict[str, tuple] = {
    "run_records": ("ubunye_run_records", "UBUNYE_RUN_RECORDS"),
    "run_records_fallback": ("ubunye_run_records_fallback", "UBUNYE_RUN_RECORDS_FALLBACK"),
    "dt": ("dt", "UBUNYE_DT"),
    "mode": ("ubunye_mode", "UBUNYE_MODE"),
}

#: Job parameters a run identity reads (ADR 015), named like its fields.
IDENTITY_PARAMETERS = (
    "git_commit",
    "git_branch",
    "deploy_target",
    "job_id",
    "job_run_id",
    "task_key",
    "workspace",
    "run_by",
)

#: Every job parameter Ubunye reads. A platform that cannot list its parameters
#: asks for these by name.
KNOWN_PARAMETERS = tuple(p for p, _ in SETTINGS.values()) + IDENTITY_PARAMETERS


@dataclass(frozen=True)
class JobSettings:
    """The job's Ubunye settings. A setting that was not found is ``None``."""

    run_records: Optional[str] = None
    run_records_fallback: Optional[str] = None
    dt: Optional[str] = None
    mode: Optional[str] = None
    #: Every job parameter found, as passed (for the identity providers).
    parameters: Mapping[str, str] = field(default_factory=dict)
    #: Where each setting that was found came from: "explicit", "job_param" or "env".
    source: Mapping[str, str] = field(default_factory=dict)


def job_parameters() -> Dict[str, str]:
    """The job parameters every platform plugin found; the first plugin wins a name."""
    found: Dict[str, str] = {}
    for provider in _identity.providers():
        try:
            params = provider.parameters() or {}
        except Exception as exc:
            logger.debug("job parameters: %r failed: %s", getattr(provider, "name", "?"), exc)
            continue
        for name, value in dict(params).items():
            if value is not None:
                found.setdefault(str(name), str(value))
    return found


def _set(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def job_settings(
    explicit: Optional[Mapping[str, Any]] = None,
    *,
    parameters: Optional[Mapping[str, str]] = None,
    environ: Optional[Mapping[str, str]] = None,
) -> JobSettings:
    """The job's settings: explicit, then job parameters, then environment variables.

    ``parameters`` and ``environ`` replace the job parameters and the environment
    (for tests and callers that already hold them).
    """
    explicit = dict(explicit or {})
    unknown = sorted(set(explicit) - set(SETTINGS))
    if unknown:
        raise ValueError(
            f"unknown Ubunye setting(s) {', '.join(unknown)}; "
            f"the settings are {', '.join(sorted(SETTINGS))}"
        )
    params = dict(job_parameters() if parameters is None else parameters)
    env = os.environ if environ is None else environ

    values: Dict[str, Optional[str]] = {}
    source: Dict[str, str] = {}
    for name, (param, var) in SETTINGS.items():
        for where, value in (
            ("explicit", explicit.get(name)),
            ("job_param", params.get(param)),
            ("env", env.get(var)),
        ):
            text = _set(value)
            if text is not None:
                values[name] = text
                source[name] = where
                break
    return JobSettings(parameters=params, source=source, **values)
