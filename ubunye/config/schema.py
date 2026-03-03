"""Typed config schema using Pydantic v2.

All models use strict validation. Enums inherit from `str` so they can be used
directly as dict keys and compared to plain strings throughout the codebase.
"""
from __future__ import annotations

import re
from enum import Enum
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
    """Valid write modes for output connectors."""
    OVERWRITE = "overwrite"
    APPEND = "append"
    MERGE = "merge"


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
    spark_conf: Dict[str, str] = Field(default_factory=dict)


class EngineConfig(BaseModel):
    """Spark/compute settings with optional per-profile overrides."""
    spark_conf: Dict[str, str] = Field(default_factory=dict)
    profiles: Dict[str, EngineProfile] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# CONFIG sub-models
# ---------------------------------------------------------------------------

class IOConfig(BaseModel):
    """Input or output connector configuration.

    Extra fields are allowed so plugin-specific keys (e.g. rest_api's
    ``auth``, ``pagination``, ``headers``) pass through to the plugin
    unchanged via ``model_dump()``.
    """
    model_config = ConfigDict(extra="allow")

    format: FormatType
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
    def _check_format_requirements(self) -> "IOConfig":
        """Enforce format-specific required fields."""
        fmt = self.format
        errors: List[str] = []

        if fmt == FormatType.HIVE:
            has_table = self.db_name and self.tbl_name
            if not has_table and not self.sql:
                errors.append(
                    "format 'hive' requires either ('db_name' + 'tbl_name') or 'sql'"
                )

        elif fmt == FormatType.JDBC:
            if not self.url:
                errors.append("format 'jdbc' requires 'url'")
            if not self.table and not self.sql:
                errors.append("format 'jdbc' requires 'table' or 'sql'")

        elif fmt in (FormatType.S3, FormatType.BINARY):
            if not self.path:
                errors.append(f"format '{fmt.value}' requires 'path'")

        elif fmt == FormatType.DELTA:
            if not self.path and not self.table:
                errors.append("format 'delta' requires 'path' or 'table'")

        elif fmt == FormatType.UNITY:
            has_table_parts = self.db_name and self.tbl_name
            if not has_table_parts and not self.table and not self.sql:
                errors.append(
                    "format 'unity' requires 'table', ('db_name' + 'tbl_name'), or 'sql'"
                )

        elif fmt == FormatType.REST_API:
            # url may come from the declared field or from model_extra (plugin-specific)
            url = self.url or (self.model_extra or {}).get("url")
            if not url:
                errors.append("format 'rest_api' requires 'url'")

        if errors:
            raise ValueError("; ".join(errors))

        return self


class TransformConfig(BaseModel):
    """Transform plugin configuration."""
    type: str = "noop"
    params: Dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Model registry sub-models (informational — used by ModelTransform and CLI)
# ---------------------------------------------------------------------------

class RegistryConfig(BaseModel):
    """Configuration for model registry integration within a transform."""
    model_config = ConfigDict(extra="allow")

    store: str
    use_case: Optional[str] = "default"
    version: Optional[str] = None
    auto_version: bool = True
    promote_to: Optional[Literal["development", "staging", "production"]] = None
    use_stage: Literal["development", "staging", "production"] = "production"
    promotion_gates: Optional[Dict[str, Any]] = None


class ModelTransformParams(BaseModel):
    """Typed params for ``transform.type: model`` — for documentation and validation."""
    model_config = ConfigDict(extra="allow")

    action: Literal["train", "predict"]
    model_class: str
    model_dir: Optional[str] = None
    model_path: Optional[str] = None
    input_name: Optional[str] = None
    registry: Optional[RegistryConfig] = None


class TaskConfig(BaseModel):
    """The ``CONFIG`` section of a task: inputs, transform, outputs."""
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


# ---------------------------------------------------------------------------
# ORCHESTRATION sub-model
# ---------------------------------------------------------------------------

class OrchestrationConfig(BaseModel):
    """Orchestration export metadata."""
    model_config = ConfigDict(extra="allow")

    type: OrchestrationType
    schedule: Optional[str] = None
    retries: int = 2
    owner: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    databricks: Optional[Dict[str, Any]] = None


# ---------------------------------------------------------------------------
# Top-level model
# ---------------------------------------------------------------------------

_SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+$")


class UbunyeConfig(BaseModel):
    """Top-level Ubunye task config (the full contents of a ``config.yaml``)."""

    MODEL: JobType
    VERSION: str
    ENGINE: EngineConfig = Field(default_factory=EngineConfig)
    CONFIG: TaskConfig
    ORCHESTRATION: Optional[OrchestrationConfig] = None

    @field_validator("VERSION")
    @classmethod
    def _validate_semver(cls, v: str) -> str:
        if not _SEMVER_RE.match(v):
            raise ValueError(
                f"VERSION must be a valid semver string (e.g. '1.0.0'), got: '{v}'"
            )
        return v

    def merged_spark_conf(self, profile: str | None = None) -> Dict[str, str]:
        """Return base spark_conf merged with the named profile's overrides."""
        conf = dict(self.ENGINE.spark_conf)
        if profile and profile in self.ENGINE.profiles:
            conf.update(self.ENGINE.profiles[profile].spark_conf)
        return conf
