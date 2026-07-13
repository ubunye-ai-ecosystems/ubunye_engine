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


class FormatType(str, Enum):
    """Registered connector format names."""

    HIVE = "hive"
    JDBC = "jdbc"
    UNITY = "unity"
    S3 = "s3"
    DELTA = "delta"
    BINARY = "binary"
    REST_API = "rest_api"


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



@lru_cache(maxsize=1)
def _registered_formats() -> frozenset:
    """Every connector name the plugin system can actually load.

    Cached: entry-point discovery walks the installed distributions, and this is called
    once per input and once per output. Uncached, a config with five IO blocks did five
    full scans, and a property test caught it by blowing a 200ms deadline.

    The engine advertises that adding a connector is "write the class, register the
    entry point". That was not true: ``format`` was a closed enum, so a perfectly
    well-registered plugin was rejected by config validation **before the registry was
    ever consulted**. The documented extension story did not work.

    Now the enum is only the list of connectors that ship in the box, and anything the
    entry points can load is equally valid.
    """
    import importlib.metadata as md

    names = {e.value for e in FormatType}
    for group in ("ubunye.readers", "ubunye.writers"):
        try:
            eps = md.entry_points()
            found = eps.get(group, []) if hasattr(eps, "get") else eps.select(group=group)
            names.update(ep.name for ep in found)
        except Exception:  # noqa: BLE001 — a broken third-party plugin must not break validation
            pass
    return frozenset(names)


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

    @model_validator(mode="after")
    def _check_format_requirements(self) -> "IOConfig":
        """Enforce format-specific required fields."""
        fmt = self.format
        errors: List[str] = []

        if fmt == "hive":
            has_table = self.db_name and self.tbl_name
            if not has_table and not self.sql:
                errors.append("format 'hive' requires either ('db_name' + 'tbl_name') or 'sql'")

        elif fmt == "jdbc":
            if not self.url:
                errors.append("format 'jdbc' requires 'url'")
            if not self.table and not self.sql:
                errors.append("format 'jdbc' requires 'table' or 'sql'")

        elif fmt in ("s3", "binary"):
            if not self.path:
                errors.append(f"format '{fmt}' requires 'path'")

        elif fmt == "delta":
            if not self.path and not self.table:
                errors.append("format 'delta' requires 'path' or 'table'")

        elif fmt == "unity":
            has_table_parts = self.db_name and self.tbl_name
            if not has_table_parts and not self.table and not self.sql:
                errors.append("format 'unity' requires 'table', ('db_name' + 'tbl_name'), or 'sql'")

        elif fmt == "rest_api":
            # url may come from the declared field or from model_extra (plugin-specific)
            url = self.url or (self.model_extra or {}).get("url")
            if not url:
                errors.append("format 'rest_api' requires 'url'")

        if errors:
            raise ValueError("; ".join(errors))

        return self


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
    def _check_writable_outputs(self) -> "TaskConfig":
        """Spark's ``binaryFile`` source can read but not write.

        Caught here rather than at write time, so the pipeline fails in
        ``ubunye validate`` instead of after the transform has already run.
        """
        read_only = [name for name, out in self.outputs.items() if out.format == "binary"]
        if read_only:
            raise ValueError(
                f"format 'binary' is read-only and cannot be used as an output "
                f"(CONFIG.outputs: {', '.join(sorted(read_only))}). "
                f"Write files with format 's3' instead."
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
