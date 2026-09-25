"""Typed config schema using Pydantic v2.

All models use strict validation with ``extra="forbid"`` so that typos in
config keys are caught at load time rather than silently ignored. The one
exception is :class:`IOConfig`, which keeps ``extra="allow"`` because
connector plugins read arbitrary keys from the config dict. Plugin-specific
fields should go in the ``options`` sub-dict; top-level extras on IOConfig
are a conscious tradeoff documented in the PR that introduced strict mode.
"""

from __future__ import annotations

import difflib
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
        for ep in md.entry_points(group=group):
            try:
                found[ep.name] = ep.load()
            except Exception:  # noqa: BLE001 — one broken plugin must not break validation
                found[ep.name] = None
    except Exception:  # noqa: BLE001
        pass
    return found


#: Keys any input or output block may carry, whatever its connector.
COMMON_IO_KEYS = frozenset({"format", "options"})

#: Keys any output block may carry: the engine's own write-mode settings, read by
#: ``core/write_modes.py`` for every writer.
OUTPUT_IO_KEYS = frozenset({"mode", "merge_keys", "replace_where"})


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

    # ``model_class`` is the config's own name, so pydantic's reserved ``model_``
    # prefix is switched off here (pydantic 2.0 refused the field; later ones warn).
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    action: Literal["train", "predict"]
    model_class: str
    model_dir: Optional[str] = None
    model_path: Optional[str] = None
    input_name: Optional[str] = None
    registry: Optional[RegistryConfig] = None


class BetweenSpec(BaseModel):
    """``between``: a column's values lie within [min, max] (either may be left out)."""

    model_config = ConfigDict(extra="forbid")

    column: str
    min: Optional[float] = None
    max: Optional[float] = None

    @model_validator(mode="after")
    def _one_bound(self) -> "BetweenSpec":
        if self.min is None and self.max is None:
            raise ValueError("'between' needs 'min', 'max' or both")
        return self


class OneOfSpec(BaseModel):
    """``one_of``: a column's values come from a fixed list."""

    model_config = ConfigDict(extra="forbid")

    column: str
    values: List[Any]


class MatchesSpec(BaseModel):
    """``matches``: a string column matches a regular expression."""

    model_config = ConfigDict(extra="forbid")

    column: str
    pattern: str

    @field_validator("pattern")
    @classmethod
    def _compiles(cls, v: str) -> str:
        try:
            re.compile(v)
        except re.error as exc:
            raise ValueError(f"'matches' pattern {v!r} is not a valid regular expression: {exc}")
        return v


class RowCountSpec(BaseModel):
    """``row_count``: the number of rows lies within [min, max]."""

    model_config = ConfigDict(extra="forbid")

    min: Optional[int] = None
    max: Optional[int] = None


EXPECTATION_KINDS = ("not_null", "unique", "between", "one_of", "matches", "row_count")
#: Kinds judged per row; only these can quarantine rows.
ROW_KINDS = ("not_null", "between", "one_of", "matches")


class ExpectationRule(BaseModel):
    """One check on an output, before it is written.

    Exactly one kind is set. ``severity`` decides what a breach does: ``fail``
    stops the run before anything is written, ``quarantine`` moves the breaking
    rows to the set's quarantine output, ``warn`` only reports. A null passes
    every kind except ``not_null``, as in SQL: pair a rule with ``not_null``
    when a value must be present.
    """

    model_config = ConfigDict(extra="forbid")

    #: Defaults to "<column>_<kind>" (or the kind alone) when left empty.
    name: str = ""
    severity: Literal["fail", "quarantine", "warn"] = "fail"
    description: Optional[str] = None
    not_null: Optional[str] = None
    unique: Optional[List[str]] = None
    between: Optional[BetweenSpec] = None
    one_of: Optional[OneOfSpec] = None
    matches: Optional[MatchesSpec] = None
    row_count: Optional[RowCountSpec] = None

    @field_validator("unique", mode="before")
    @classmethod
    def _unique_as_list(cls, v: Any) -> Any:
        return [v] if isinstance(v, str) else v

    @property
    def kind(self) -> str:
        return next(k for k in EXPECTATION_KINDS if getattr(self, k) is not None)

    @property
    def column(self) -> Optional[str]:
        spec = getattr(self, self.kind)
        if isinstance(spec, str):
            return spec
        if isinstance(spec, list):
            return ", ".join(spec)
        return getattr(spec, "column", None)

    @model_validator(mode="after")
    def _exactly_one_kind(self) -> "ExpectationRule":
        kinds = [k for k in EXPECTATION_KINDS if getattr(self, k) is not None]
        if len(kinds) != 1:
            raise ValueError(
                f"an expectation needs exactly one of {', '.join(EXPECTATION_KINDS)}; "
                f"got {', '.join(kinds) or 'none'}"
            )
        if self.severity == "quarantine" and kinds[0] not in ROW_KINDS:
            raise ValueError(
                f"'{kinds[0]}' is about the whole output, not a row, so it cannot "
                "quarantine rows; use severity fail or warn"
            )
        if not self.name:
            column = (self.column or "").replace(", ", "_")
            self.name = f"{column}_{kinds[0]}" if column else kinds[0]
        return self


class ExpectationSet(BaseModel):
    """The expectations on one output, and where its quarantined rows go."""

    model_config = ConfigDict(extra="forbid")

    rules: List[ExpectationRule]
    #: The output that receives rows breaking a ``quarantine`` rule, with a
    #: ``_ubunye_failed_rules`` column naming the rules each row broke.
    quarantine: Optional[str] = None
    #: More than this share of rows quarantined fails the run: a source that has
    #: changed under you is not fixed by quietly setting aside a third of the data.
    max_quarantine_rate: Optional[float] = Field(default=None, ge=0, le=1)

    @model_validator(mode="after")
    def _quarantine_has_a_target(self) -> "ExpectationSet":
        names = [r.name for r in self.rules]
        dupes = sorted({n for n in names if names.count(n) > 1})
        if dupes:
            raise ValueError(f"expectation names must be unique; repeated: {', '.join(dupes)}")
        wants = any(r.severity == "quarantine" for r in self.rules)
        if wants and not self.quarantine:
            raise ValueError("a rule with severity quarantine needs 'quarantine: <output name>'")
        if self.max_quarantine_rate is not None and not wants:
            raise ValueError("'max_quarantine_rate' needs at least one quarantine rule")
        return self


class TaskConfig(BaseModel):
    """The ``CONFIG`` section of a task: inputs, transform, outputs."""

    model_config = ConfigDict(extra="forbid")

    inputs: Dict[str, IOConfig]
    transform: TransformConfig = Field(default_factory=TransformConfig)
    outputs: Dict[str, IOConfig]
    #: Checks on outputs, by output name, run after the transform and before any
    #: output is written. See ``ubunye.core.expectations``.
    expectations: Dict[str, ExpectationSet] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _check_secret_references(self) -> "TaskConfig":
        """Every ``secret://`` reference names an installed provider. Nothing is fetched."""
        from ubunye.core import secrets

        errors: List[str] = []
        for role, blocks in (("inputs", self.inputs), ("outputs", self.outputs)):
            for name, io in blocks.items():
                errors += secrets.problems(io.model_dump(exclude_none=True), f"{role}.{name}")
        if errors:
            raise ValueError("; ".join(errors))
        return self

    @model_validator(mode="after")
    def _check_expectations_name_outputs(self) -> "TaskConfig":
        errors: List[str] = []
        quarantines = [s.quarantine for s in self.expectations.values() if s.quarantine]
        targets = set(quarantines)
        shared = sorted({q for q in quarantines if quarantines.count(q) > 1})
        if shared:
            errors.append(
                f"expectations: two outputs quarantine into '{', '.join(shared)}'; "
                "give each its own quarantine output"
            )
        for name, spec in self.expectations.items():
            if name not in self.outputs:
                errors.append(f"expectations.{name}: there is no output named '{name}'")
            if spec.quarantine and spec.quarantine not in self.outputs:
                errors.append(
                    f"expectations.{name}.quarantine: there is no output named "
                    f"'{spec.quarantine}'"
                )
            if spec.quarantine == name:
                errors.append(f"expectations.{name}: an output cannot quarantine into itself")
            if name in targets:
                errors.append(
                    f"expectations.{name}: '{name}' receives quarantined rows, so it "
                    "cannot have expectations of its own"
                )
        if errors:
            raise ValueError("; ".join(errors))
        return self

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
        # A misspelt key explains a "requires" error below it, so it is reported first.
        errors: List[str] = self._unknown_key_errors()

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

    def _unknown_key_errors(self) -> List[str]:
        """A key the connector does not read is a typo until proven otherwise.

        ``IOConfig`` accepts any key, because connectors read their own settings, so
        ``paht:`` used to validate and be ignored. A connector that declares
        ``CONFIG_KEYS`` gets every other key reported, with the closest real one. A
        connector that declares nothing keeps accepting anything: the check is the
        plugin's to opt into, like ``validate_config``.
        """
        errors: List[str] = []
        for role, group, blocks in (
            ("inputs", "ubunye.readers", self.inputs),
            ("outputs", "ubunye.writers", self.outputs),
        ):
            plugins = _connectors(group)
            for name, io in blocks.items():
                declared = getattr(plugins.get(io.format), "CONFIG_KEYS", None)
                if declared is None:
                    continue
                allowed = set(declared) | COMMON_IO_KEYS
                if role == "outputs":
                    allowed |= OUTPUT_IO_KEYS
                # An explicit null is "not set": a parsed config dumped and read back
                # carries every common field, most of them None.
                given = {k for k in io.model_fields_set if getattr(io, k) is not None}
                given |= {k for k, v in (io.model_extra or {}).items() if v is not None}
                for key in sorted(given - allowed):
                    near = difflib.get_close_matches(key, sorted(allowed), n=1, cutoff=0.6)
                    if near:
                        errors.append(
                            f"{role}.{name}: '{key}' is not a setting of '{io.format}'; "
                            f"did you mean '{near[0]}'?"
                        )
                    else:
                        errors.append(
                            f"{role}.{name}: '{key}' is not a setting of '{io.format}', "
                            f"which reads: {', '.join(sorted(allowed))}. Options for the "
                            "underlying engine go under 'options'."
                        )
        return errors

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
