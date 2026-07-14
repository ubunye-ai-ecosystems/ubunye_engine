"""Typed config schema using Pydantic v2.

All models use strict validation with ``extra="forbid"`` so that typos in
config keys are caught at load time rather than silently ignored. The one
exception is :class:`IOConfig`, which keeps ``extra="allow"`` because
connector plugins read arbitrary keys from the config dict. Plugin-specific
fields should go in the ``options`` sub-dict; top-level extras on IOConfig
are a conscious tradeoff documented in the PR that introduced strict mode.
"""

from __future__ import annotations

import re
from enum import Enum
from functools import lru_cache
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class JobType(str, Enum):
    """Supported pipeline job types."""

    ETL = "etl"
    ML = "ml"


class WriteMode(str, Enum):
    """Valid write modes for output connectors.

    The first four are Spark's native ``SaveMode`` values (``ERROR`` is the
    alias Spark also accepts for ``errorifexists``). ``MERGE`` and
    ``OVERWRITE_PARTITIONS`` are Ubunye's lakehouse modes — see
    ``ubunye.core.write_modes``. Not every connector supports every mode; the
    writer raises ``SinkWriteError`` at write time if it cannot honour one.
    """

    OVERWRITE = "overwrite"
    APPEND = "append"
    ERRORIFEXISTS = "errorifexists"
    ERROR = "error"
    IGNORE = "ignore"
    MERGE = "merge"
    OVERWRITE_PARTITIONS = "overwrite_partitions"


# NOTE: `FormatType` used to live here — a closed enum of hive/jdbc/s3/delta/binary/
# unity/rest_api. It is gone.
#
# It was the hardcoded list at the heart of the problem: a correctly-registered
# third-party connector was rejected by config validation *before the plugin registry
# was ever consulted*, so "adding a connector = write the class, register the entry
# point" was simply not true. The engine now asks the registry what exists, and asks the
# plugin what it needs. It keeps no list of implementations at all.


class OrchestrationType(str, Enum):
    """Supported orchestration export targets."""

    AIRFLOW = "airflow"
    DATABRICKS = "databricks"
    PREFECT = "prefect"
    DAGSTER = "dagster"


# ---------------------------------------------------------------------------
# ENGINE sub-models
# ---------------------------------------------------------------------------


class EngineProfile(BaseModel):
    """Profile-specific Spark configuration overrides."""

    model_config = ConfigDict(extra="forbid")

    spark_conf: Dict[str, str] = Field(default_factory=dict)
    catalog: Optional[str] = None
    schema_name: Optional[str] = None


class EngineConfig(BaseModel):
    """Spark/compute settings with optional per-profile overrides."""

    model_config = ConfigDict(extra="forbid")

    spark_conf: Dict[str, str] = Field(default_factory=dict)
    profiles: Dict[str, EngineProfile] = Field(default_factory=dict)
    catalog: Optional[str] = None
    schema_name: Optional[str] = None


# ---------------------------------------------------------------------------
# CONFIG sub-models
# ---------------------------------------------------------------------------


@lru_cache(maxsize=2)
def _connectors(group: str) -> Dict[str, Any]:
    """Map connector name -> plugin class, for one entry-point group.

    The engine holds NO list of implementations. It asks the registry which plugin the
    config named, and then asks that plugin what it needs and what it can do.

    Before this, ``format`` was a closed enum and this module carried an ``if/elif``
    chain spelling out the requirements of hive, jdbc, s3, delta, unity and rest_api.
    A third-party connector could not declare its own required fields, so
    "adding a connector = write the class, register the entry point" was false: you had
    to edit the engine too. Now you do not.
    """
    import importlib.metadata as md

    found: Dict[str, Any] = {}
    try:
        eps: Any = md.entry_points()
        group_eps = eps.get(group, []) if hasattr(eps, "get") else eps.select(group=group)
        for ep in group_eps:
            try:
                found[ep.name] = ep.load()
            except Exception:  # noqa: BLE001 — one broken plugin must not break validation
                found[ep.name] = None
    except Exception:  # noqa: BLE001
        pass
    return found


def _registered_formats() -> frozenset:
    """Every connector name the plugin system can actually load."""
    return frozenset(_connectors("ubunye.readers")) | frozenset(_connectors("ubunye.writers"))


class IOConfig(BaseModel):
    """Input or output connector configuration.

    Extra fields are allowed so plugin-specific keys (e.g. rest_api's
    ``auth``, ``pagination``, ``headers``) pass through to the plugin
    unchanged via ``model_dump()``.
    """

    model_config = ConfigDict(extra="allow")

    format: str
    # Common fields shared across connectors
    db_name: Optional[str] = None
    tbl_name: Optional[str] = None
    sql: Optional[str] = None
    path: Optional[str] = None
    mode: Optional[WriteMode] = None
    options: Dict[str, Any] = Field(default_factory=dict)
    # JDBC / REST shared fields
    url: Optional[str] = None
    table: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None

    @model_validator(mode="after")
    def _check_format_is_registered(self) -> "IOConfig":
        """The format must name a connector the plugin registry can actually load."""
        known = _registered_formats()
        if self.format not in known:
            raise ValueError(
                f"Unknown format '{self.format}'. "
                f"Available connectors: {', '.join(sorted(known))}. "
                "A third-party connector must be installed and registered under the "
                "'ubunye.readers' or 'ubunye.writers' entry-point group."
            )
        return self

    # NOTE: what each connector REQUIRES is no longer decided here.
    #
    # This class used to carry an if/elif chain naming hive, jdbc, s3, binary, delta,
    # unity and rest_api, and spelling out the fields each one needed. That meant a
    # third-party connector could not state its own requirements, and the engine had to
    # be edited to add one.
    #
    # It also could not tell an INPUT from an OUTPUT — `unity` as a source may be given
    # `sql`, but as a sink it must have a `table`, and one shared rule could express
    # neither. TaskConfig now validates each side against the right plugin: readers for
    # inputs, writers for outputs. See TaskConfig._check_connector_requirements.


class TransformConfig(BaseModel):
    """Transform plugin configuration.

    ``type`` is optional.  When omitted the engine assumes that a
    ``transformations.py`` Task class supplies the logic — no explicit
    ``type: noop`` is needed.  Existing configs that still declare
    ``type: noop`` continue to work but emit a deprecation warning.
    """

    model_config = ConfigDict(extra="forbid")

    type: Optional[str] = None
    params: Dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Model registry sub-models (informational — used by ModelTransform and CLI)
# ---------------------------------------------------------------------------


class RegistryConfig(BaseModel):
    """Configuration for model registry integration within a transform."""

    model_config = ConfigDict(extra="forbid")

    store: str
    use_case: Optional[str] = "default"
    version: Optional[str] = None
    auto_version: bool = True
    promote_to: Optional[Literal["development", "staging", "production"]] = None
    use_stage: Literal["development", "staging", "production"] = "production"
    promotion_gates: Optional[Dict[str, Any]] = None


class ModelTransformParams(BaseModel):
    """Typed params for ``transform.type: model`` — for documentation and validation."""

    model_config = ConfigDict(extra="forbid")

    action: Literal["train", "predict"]
    model_class: str
    model_dir: Optional[str] = None
    model_path: Optional[str] = None
    input_name: Optional[str] = None
    registry: Optional[RegistryConfig] = None


class TaskConfig(BaseModel):
    """The ``CONFIG`` section of a task: inputs, transform, outputs."""

    model_config = ConfigDict(extra="forbid")

    inputs: Dict[str, IOConfig]
    transform: TransformConfig = Field(default_factory=TransformConfig)
    outputs: Dict[str, IOConfig]

    @model_validator(mode="after")
    def _check_non_empty(self) -> "TaskConfig":
        if not self.inputs:
            raise ValueError("CONFIG.inputs must define at least one input")
        if not self.outputs:
            raise ValueError("CONFIG.outputs must define at least one output")
        return self

    @model_validator(mode="after")
    def _check_connector_requirements(self) -> "TaskConfig":
        """Ask each connector what it needs. Do not presume to know.

        Inputs are validated against the READER registered under that name; outputs
        against the WRITER. That distinction was impossible before: one shared rule
        validated both roles, so `unity` had to accept `sql` everywhere — including as an
        output, where it is meaningless.

        The engine holds no table of requirements. If a plugin is installed and does not
        declare any, it gets none: `Connector.validate_config` returns `[]` by default,
        so a simple connector stays simple.
        """
        errors: List[str] = []

        for role, group, blocks in (
            ("inputs", "ubunye.readers", self.inputs),
            ("outputs", "ubunye.writers", self.outputs),
        ):
            plugins = _connectors(group)
            for name, io in blocks.items():
                plugin = plugins.get(io.format)
                if plugin is None:
                    # Registered for the other role only — e.g. `binary` can be read but
                    # not written. _check_writable_outputs says so more precisely.
                    continue

                # STRUCTURAL, not nominal. The design philosophy (docs/interfaces.md,
                # blog.md) is ports and adapters: "any class that implements the
                # required methods satisfies the protocol — no inheritance needed."
                # The first version of this called plugin.validate_config() directly,
                # which made a duck-typed connector — one that implements read() and
                # inherits nothing, exactly what the docs promise will work — crash
                # with AttributeError. Asking is fine; DEMANDING an answer is a new
                # inheritance requirement smuggled in through a method call.
                declare = getattr(plugin, "validate_config", None)
                if not callable(declare):
                    continue  # declares nothing -> requires nothing. Its choice.

                cfg = io.model_dump(exclude_none=True)
                cfg.update(io.model_extra or {})
                for problem in declare(cfg) or []:
                    errors.append(f"{role}.{name}: {problem}")

        if errors:
            raise ValueError("; ".join(errors))
        return self

    @model_validator(mode="after")
    def _check_writable_outputs(self) -> "TaskConfig":
        """Spark's ``binaryFile`` source can read but not write.

        Caught here rather than at write time, so the pipeline fails in
        ``ubunye validate`` instead of after the transform has already run.
        """
        writers = _connectors("ubunye.writers")
        read_only = sorted(name for name, out in self.outputs.items() if out.format not in writers)
        if read_only:
            names = {n: self.outputs[n].format for n in read_only}
            raise ValueError(
                "these outputs name a connector with no registered WRITER: "
                + ", ".join(f"{n} (format '{f}')" for n, f in names.items())
                + ". Some connectors are read-only — Spark's binaryFile source, for one. "
                "The engine does not keep a list of which; it simply asks whether a "
                "writer exists."
            )
        return self


# ---------------------------------------------------------------------------
# ORCHESTRATION sub-model
# ---------------------------------------------------------------------------


class OrchestrationConfig(BaseModel):
    """Orchestration export metadata."""

    model_config = ConfigDict(extra="forbid")

    type: OrchestrationType
    schedule: Optional[str] = None
    retries: int = 2
    owner: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    databricks: Optional[Dict[str, Any]] = None


# ---------------------------------------------------------------------------
# Top-level model
# ---------------------------------------------------------------------------

_SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+(-[\w.]+)?$")

DEFAULT_VERSION = "0.0.0-dev"


class UbunyeConfig(BaseModel):
    """Top-level Ubunye task config (the full contents of a ``config.yaml``).

    ``MODEL`` and ``VERSION`` are optional; they default to ``etl`` and
    ``"0.0.0-dev"`` respectively. Set them explicitly in production pipelines
    where job type or version is load-bearing (lineage records, model
    registry, orchestrator metadata).
    """

    model_config = ConfigDict(extra="forbid")

    MODEL: JobType = JobType.ETL
    VERSION: str = DEFAULT_VERSION
    ENGINE: EngineConfig = Field(default_factory=EngineConfig)
    CONFIG: TaskConfig
    ORCHESTRATION: Optional[OrchestrationConfig] = None

    @field_validator("VERSION")
    @classmethod
    def _validate_semver(cls, v: str) -> str:
        if not _SEMVER_RE.match(v):
            raise ValueError(
                f"VERSION must be a valid semver string (e.g. '1.0.0' or '1.0.0-rc1'), got: '{v}'"
            )
        return v

    def merged_spark_conf(self, profile: str | None = None) -> Dict[str, str]:
        """Return base spark_conf merged with the named profile's overrides."""
        conf = dict(self.ENGINE.spark_conf)
        if profile and self.ENGINE.profiles:
            if profile not in self.ENGINE.profiles:
                from ubunye.core.errors import ConfigProfileError

                available = sorted(self.ENGINE.profiles.keys())
                raise ConfigProfileError(
                    f"Profile '{profile}' not found.",
                    context={"Profile": profile, "Available": available},
                    hint=f"Valid profiles: {', '.join(available)}",
                )
            conf.update(self.ENGINE.profiles[profile].spark_conf)
        return conf

    def resolved_catalog(self, profile: str | None = None) -> Optional[str]:
        """Return the catalog name, with profile override if available."""
        if profile and self.ENGINE.profiles:
            if profile not in self.ENGINE.profiles:
                from ubunye.core.errors import ConfigProfileError

                available = sorted(self.ENGINE.profiles.keys())
                raise ConfigProfileError(
                    f"Profile '{profile}' not found.",
                    context={"Profile": profile, "Available": available},
                    hint=f"Valid profiles: {', '.join(available)}",
                )
            override = self.ENGINE.profiles[profile].catalog
            if override is not None:
                return override
        return self.ENGINE.catalog

    def resolved_schema(self, profile: str | None = None) -> Optional[str]:
        """Return the schema name, with profile override if available."""
        if profile and self.ENGINE.profiles:
            if profile not in self.ENGINE.profiles:
                from ubunye.core.errors import ConfigProfileError

                available = sorted(self.ENGINE.profiles.keys())
                raise ConfigProfileError(
                    f"Profile '{profile}' not found.",
                    context={"Profile": profile, "Available": available},
                    hint=f"Valid profiles: {', '.join(available)}",
                )
            override = self.ENGINE.profiles[profile].schema_name
            if override is not None:
                return override
        return self.ENGINE.schema_name
