"""Run records switched on by the job's settings (ADR 016).

A job turns run records on by saying where they go, in one of three places, the
first one set wins:

1. an explicit argument (``run_records=`` on ``ubunye.notebook`` / ``run_task``);
2. the job parameter ``ubunye_run_records`` (a Databricks job notebook parameter,
   read through the job settings reader; serverless has no environment variables);
3. the environment variable ``UBUNYE_RUN_RECORDS``.

The location picks the store: a table name (``main.ops.ubunye_runs``) goes to the
``delta`` store, a path (``/Volumes/...``, ``./runs``, ``s3://...``) to the
``filesystem`` store. A store named with ``lineage_backend=`` or
``$UBUNYE_LINEAGE_BACKEND`` is kept. The older ``lineage=True`` with
``lineage_dir=`` still works exactly as before; ``lineage=False`` switches records
off even when a location is configured.

A record a store cannot save is kept in a fallback manifest only where one is
configured (``ubunye_run_records_fallback`` / ``$UBUNYE_RUN_RECORDS_FALLBACK``,
usually a Unity Catalog Volume); with none, a warning says why and the job goes on.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Union

logger = logging.getLogger(__name__)

#: The job parameter and environment variable for each setting this module reads.
SETTINGS = {
    "run_records": ("ubunye_run_records", "UBUNYE_RUN_RECORDS"),
    "run_records_fallback": ("ubunye_run_records_fallback", "UBUNYE_RUN_RECORDS_FALLBACK"),
    "dt": ("dt", "UBUNYE_DT"),
    "mode": ("ubunye_mode", "UBUNYE_MODE"),
}

#: The stage of a run that failed outside any stage the engine knows: code in the
#: notebook's ``with`` block between ``read``, ``transform`` and ``write``.
NOTEBOOK_STAGE = "notebook"
SETUP_STAGE = "setup"


def blank_as_none(value: Optional[str]) -> Optional[str]:
    """``""`` (a job parameter left empty) counts as not given."""
    if value is None:
        return None
    return value if str(value).strip() else None


def _job_parameters() -> Mapping[str, str]:
    """Every job parameter the platform hands the run, raw.

    Read through the job settings reader (``ubunye.core.settings``) when it is
    installed; before it is, from the Databricks widgets directly, and empty
    anywhere else. Never raises.
    """
    try:
        from ubunye.core.settings import job_settings

        return dict(job_settings().parameters or {})
    except ImportError:
        pass
    except Exception as exc:  # noqa: BLE001  never fail a job over its settings
        logger.debug("The job settings could not be read: %s", exc)
        return {}
    return _widgets()


def _widgets() -> Dict[str, str]:
    """The ubunye job parameters as Databricks widgets, when there are any."""
    from ubunye.notebook import _get_dbutils

    dbutils = _get_dbutils()
    if dbutils is None:
        return {}
    found: Dict[str, str] = {}
    for param, _ in SETTINGS.values():
        try:
            value = dbutils.widgets.get(param)
        except Exception:  # noqa: BLE001  a widget the job does not define
            continue
        if isinstance(value, str) and blank_as_none(value) is not None:
            found[param] = value
    return found


@dataclass(frozen=True)
class Settings:
    """The ubunye settings a run reads, and where each came from."""

    run_records: Optional[str] = None
    run_records_fallback: Optional[str] = None
    dt: Optional[str] = None
    mode: Optional[str] = None
    source: Mapping[str, str] = field(default_factory=dict)


def settings(explicit: Optional[Mapping[str, Any]] = None) -> Settings:
    """The run's settings: explicit, else the job parameter, else the environment."""
    explicit = explicit or {}
    params = _job_parameters()
    values: Dict[str, Optional[str]] = {}
    source: Dict[str, str] = {}
    for name, (param, env) in SETTINGS.items():
        for where, value in (
            ("explicit", explicit.get(name)),
            ("job_param", params.get(param)),
            ("env", os.environ.get(env)),
        ):
            value = blank_as_none(None if value is None else str(value))
            if value is not None:
                values[name], source[name] = value, where
                break
    return Settings(**values, source=source)


def identity(found: Settings, task_dir: Optional[Union[str, Path]] = None) -> Dict[str, str]:
    """Who and where ran this (``ubunye.core.identity``), or empty before it exists."""
    try:
        from ubunye.core.identity import collect_identity
        from ubunye.core.settings import job_settings
    except ImportError:
        return {}
    try:
        explicit = {
            k: getattr(found, k) for k in ("run_records", "run_records_fallback", "dt", "mode")
        }
        who = collect_identity(job_settings(explicit), task_dir=task_dir)
        return dict(who.as_dict())
    except Exception as exc:  # noqa: BLE001  identity is evidence, never a failure
        logger.debug("The run identity could not be collected: %s", exc)
        return {}


def with_identity(fields: Dict[str, Any], who: Mapping[str, str]) -> Dict[str, Any]:
    """``fields`` for an EngineContext, plus ``identity`` where the context has it."""
    import dataclasses

    from ubunye.core.runtime import EngineContext

    if who and "identity" in {f.name for f in dataclasses.fields(EngineContext)}:
        return {**fields, "identity": dict(who)}
    return fields


@dataclass(frozen=True)
class Choice:
    """Where this run's records go."""

    backend: str
    #: The store's location, as its constructor takes it.
    location: str
    #: Where a record the store could not save is kept: a folder, or None to warn only.
    #: :data:`HOME` keeps the old place, ``~/.ubunye/fallback``.
    fallback: Optional[str]
    #: Where the location came from: "explicit", "job_param", "env" or "lineage".
    source: str

    def describe(self) -> str:
        return f"{self.backend} {self.location}"

    def lineage_dir(self, usecase_dir: Union[str, Path]) -> str:
        """``EngineContext.lineage_dir``: the record files, for rerun safety (ADR 008)."""
        if self.backend == "filesystem":
            return self.location
        return str(Path(usecase_dir) / self.location)


#: The fallback a run that did not configure one keeps: the old home folder.
HOME = "~"


def choose(
    usecase_dir: Union[str, Path],
    *,
    lineage: Optional[bool] = None,
    lineage_dir: Optional[str] = None,
    lineage_backend: Optional[str] = None,
    run_records: Optional[str] = None,
    found: Optional[Settings] = None,
) -> Optional[Choice]:
    """Where this run's records go, or None when they are off.

    ``lineage=False`` is off. A run-record location (explicit, job parameter,
    environment) turns them on. Without one, ``lineage=True`` is the old switch:
    ``lineage_dir`` under the use case folder, fallback in the home folder.
    """
    from ubunye.lineage import stores

    if lineage is False:
        return None
    found = found or settings({"run_records": run_records})
    explicit = blank_as_none(run_records)
    if explicit is None and lineage and lineage_dir is not None:
        return _old_switch(usecase_dir, lineage_dir, lineage_backend, found)
    location = explicit or found.run_records
    if location is not None:
        named = lineage_backend or os.environ.get(stores.ENV_VAR)
        backend = stores.backend_name(named) if named else _backend_for(location)
        where = str(Path(usecase_dir) / location) if backend == stores.DEFAULT_BACKEND else location
        source = "explicit" if explicit else (found.source or {}).get("run_records", "env")
        return Choice(backend, where, found.run_records_fallback, source)
    if lineage:
        return _old_switch(usecase_dir, lineage_dir, lineage_backend, found)
    return None


def _old_switch(
    usecase_dir: Union[str, Path],
    lineage_dir: Optional[str],
    lineage_backend: Optional[str],
    found: Settings,
) -> Choice:
    """``lineage=True``: ``lineage_dir`` under the use case folder, as before 0.9.2."""
    from ubunye.lineage import stores

    directory = lineage_dir or stores.DEFAULT_DIR
    return Choice(
        stores.backend_name(lineage_backend),
        stores.location(usecase_dir, directory, lineage_backend),
        found.run_records_fallback or HOME,
        "lineage",
    )


def _backend_for(location: str) -> str:
    from ubunye.lineage.delta_store import is_path

    return "filesystem" if is_path(location) else "delta"


def recorder_for(choice: Choice) -> Any:
    """The LineageRecorder for a choice."""
    from ubunye.lineage.recorder import LineageRecorder

    return LineageRecorder(
        backend=choice.backend, base_dir=choice.location, fallback=choice.fallback
    )


def record_failure(
    recorder: Any, context: Any, config: Optional[Dict[str, Any]], exc: BaseException, stage: str
) -> None:
    """A run record for a run that failed before the engine had it (``setup``).

    Never raises: the failure the caller is about to raise is the one that counts.
    """
    from ubunye.telemetry.hooks.monitors import _error_text

    cfg = dict(config or {})
    try:
        recorder.task_start(context=context, config=cfg)
        recorder.task_end(
            context=context,
            config=cfg,
            outputs=None,
            status="error",
            duration_sec=0.0,
            error=_error_text(exc, context, cfg),
            failed_stage=stage,
        )
    except Exception as err:  # noqa: BLE001
        logger.warning("The %s failure of %s was not recorded: %s", stage, context, err)
