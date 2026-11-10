"""Write-mode grammar, shared by every writer plugin.

This is the *decision* layer, and it is backend-agnostic: it validates a
config's ``mode`` against what a connector says it can do, and produces a
:class:`ResolvedWriteMode` describing what should happen. It contains no Spark,
because the same validation serves a pandas writer just as well.

The *mechanism* — turning a resolved mode into a Spark ``df.write`` chain, a
``MERGE INTO``, or a dynamic-partition overwrite — used to live here too. Under
issue #37 it moved to ``ubunye.adapters.spark.write_exec`` so the engine core
stops naming Spark. For backward compatibility, the moved names
(``apply``, ``merge_into``, ``target_exists``, ``dynamic_partition_overwrite``)
are still reachable through this module with a ``DeprecationWarning`` — see the
module ``__getattr__`` at the bottom. New code should import them from the
adapter directly.

The write modes:

``append`` / ``overwrite`` / ``errorifexists`` / ``ignore``
    The four save modes Spark's ``DataFrameWriter`` accepts directly.

``merge``
    Delta MERGE (upsert) keyed on ``merge_keys``.

``insert_new``
    Insert only the rows whose ``merge_keys`` are not in the target yet; rows
    already there are never changed, so the first write of a key wins and a rerun
    adds nothing (ADR 018). Delta (or any format a writer lists as mergeable).

``overwrite_partitions``
    Dynamic partition overwrite — replace only the partitions present in the
    incoming DataFrame, leave the rest of the table alone.

Connectors declare which modes they can honour via ``supported=``; asking for
one they cannot do is a config error, raised before a single row is written.
"""

from __future__ import annotations

import importlib
import warnings
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from ubunye.core.errors import SinkWriteError

# The four modes Spark's DataFrameWriter accepts directly.
NATIVE_SAVE_MODES = frozenset({"append", "overwrite", "errorifexists", "ignore"})

# The ones Ubunye adds on top; each needs real code, not a save-mode string.
LAKEHOUSE_MODES = frozenset({"merge", "insert_new", "overwrite_partitions"})

# The modes that match rows on merge_keys, and so need a mergeable (Delta) target.
KEYED_MODES = frozenset({"merge", "insert_new"})

ALL_MODES = NATIVE_SAVE_MODES | LAKEHOUSE_MODES

# Spellings Spark itself tolerates, normalised to the canonical name.
_ALIASES = {"error": "errorifexists", "errorifexist": "errorifexists"}

# The DEFAULT table formats that can do a MERGE / replaceWhere.
#
# This is a fallback, not a verdict. It used to be the only authority in the engine, so
# a connector that could genuinely upsert -- Iceberg, Hudi -- could not say so without
# someone patching the core. Writers now pass their own `merge_formats=`, declared on
# the class (Writer.MERGE_FILE_FORMATS), and the core simply believes them.
_DEFAULT_MERGE_FORMATS = frozenset({"delta"})


@dataclass(frozen=True)
class ResolvedWriteMode:
    """A validated write mode plus everything a writer needs to honour it."""

    mode: str
    """Canonical Ubunye mode (aliases already resolved)."""

    save_mode: str
    """What to hand to ``df.write.mode(...)``. For the lakehouse modes this is
    the save mode used when the target does not exist yet."""

    options: Dict[str, str] = field(default_factory=dict)
    """Writer options the mode implies (e.g. ``replaceWhere``)."""

    merge_keys: Sequence[str] = ()

    batch_where: Optional[str] = None
    """For an ``append``: the predicate that selects one batch's rows
    (``replace_where`` on an append output). A Delta or JDBC append with one replaces
    those rows in one transaction instead of adding them again (ADR 008, F-071,
    F-072). Writers that cannot honour it ignore it, as before."""

    @property
    def is_merge(self) -> bool:
        return self.mode == "merge"

    @property
    def is_insert_new(self) -> bool:
        """Insert rows whose keys the target lacks; leave every existing row alone."""
        return self.mode == "insert_new"

    @property
    def is_overwrite_partitions(self) -> bool:
        return self.mode == "overwrite_partitions"


def parse_merge_keys(cfg: Dict[str, Any]) -> List[str]:
    """Pull merge keys out of a writer config.

    Accepts a list or a comma-separated string, at the top level (``merge_keys``)
    or nested under ``options`` — the shape the connector docs use.
    """
    raw = cfg.get("merge_keys")
    if raw is None:
        raw = (cfg.get("options") or {}).get("merge_keys")
    if raw is None:
        return []
    if isinstance(raw, str):
        return [k.strip() for k in raw.split(",") if k.strip()]
    if isinstance(raw, Iterable):
        return [str(k).strip() for k in raw if str(k).strip()]
    return []


def resolve(
    cfg: Dict[str, Any],
    *,
    connector: str,
    supported: Iterable[str],
    default: str,
    file_format: Optional[str] = None,
    merge_formats: Optional[Iterable[str]] = None,
) -> ResolvedWriteMode:
    """Validate ``cfg['mode']`` against what this connector can actually do.

    Parameters
    ----------
    cfg:
        The writer's output config.
    connector:
        Plugin name, used in error messages (``s3``, ``unity``, ...).
    supported:
        Modes this connector can honour.
    default:
        Mode to use when the config does not set one.
    file_format:
        Underlying Spark format (``delta``, ``parquet``, ...). Needed because
        ``merge`` and ``replaceWhere`` are Delta-only.

    Raises
    ------
    SinkWriteError
        If the mode is unknown, unsupported by this connector, or missing the
        config it needs (merge keys, a Delta format, ...).
    """
    supported_set = set(supported)
    raw = cfg.get("mode") or default
    # The engine dumps config with mode="json", so this is normally a plain str.
    # Engine.write_outputs() is public though, and a caller passing a bare
    # model_dump() hands us a WriteMode enum — whose str() is "WriteMode.MERGE",
    # not "merge". Unwrap it before comparing.
    raw = getattr(raw, "value", raw)
    normalised = str(raw).strip().lower()
    mode = _ALIASES.get(normalised, normalised)

    if mode not in ALL_MODES:
        raise SinkWriteError(
            f"Unknown write mode '{raw}'.",
            context={"Format": connector, "Mode": raw, "Valid": sorted(ALL_MODES)},
            hint=f"Set mode to one of: {', '.join(sorted(ALL_MODES))}.",
        )

    if mode not in supported_set:
        raise SinkWriteError(
            f"Write mode '{mode}' is not supported by the '{connector}' connector.",
            context={
                "Format": connector,
                "Mode": mode,
                "Supported": sorted(supported_set),
            },
            hint=f"The '{connector}' connector supports: {', '.join(sorted(supported_set))}.",
        )

    mergeable = frozenset(merge_formats) if merge_formats is not None else _DEFAULT_MERGE_FORMATS

    if mode in KEYED_MODES:
        return _resolve_merge(
            cfg, connector=connector, file_format=file_format, mergeable=mergeable, mode=mode
        )

    if mode == "overwrite_partitions":
        return _resolve_overwrite_partitions(
            cfg, connector=connector, file_format=file_format, mergeable=mergeable
        )

    if mode == "append":
        where = cfg.get("replace_where") or (cfg.get("options") or {}).get("replaceWhere")
        return ResolvedWriteMode(
            mode=mode, save_mode=mode, batch_where=str(where) if where else None
        )

    return ResolvedWriteMode(mode=mode, save_mode=mode)


def writer_modes(writer_cls: Any) -> Tuple[Optional[Iterable[str]], str]:
    """The modes a writer declares (``None`` when it declares none) and its default.

    Asked the way writers declare them: ``SUPPORTED_MODES`` / ``DEFAULT_MODE`` in the
    writer's module, else ``SUPPORTED_MODES`` on the class.
    """
    supported: Any = None
    default = "append"
    module_name = getattr(writer_cls, "__module__", None)
    if module_name:
        try:
            module = importlib.import_module(module_name)
            supported = getattr(module, "SUPPORTED_MODES", None)
            default = getattr(module, "DEFAULT_MODE", default)
        except Exception:  # a plugin that will not import is reported elsewhere
            pass
    if supported is None:
        supported = getattr(writer_cls, "SUPPORTED_MODES", None)
    return supported, default


def resolve_for(
    writer_cls: Any, cfg: Dict[str, Any], supported: Optional[Iterable[str]] = None
) -> ResolvedWriteMode:
    """:func:`resolve` for one output, with what its writer declares.

    ``ubunye plan`` and config validation both ask this, so they give one answer.
    ``supported`` is used when the writer declares no modes.
    """
    declared, default = writer_modes(writer_cls)
    return resolve(
        cfg,
        connector=str(cfg.get("format", "")),
        supported=declared if declared is not None else (supported or NATIVE_SAVE_MODES),
        default=default,
        file_format=(cfg.get("file_format") or "").lower() or None,
        merge_formats=getattr(writer_cls, "MERGE_FILE_FORMATS", None) or None,
    )


def _resolve_merge(
    cfg: Dict[str, Any],
    *,
    connector: str,
    file_format: Optional[str],
    mergeable: frozenset = _DEFAULT_MERGE_FORMATS,
    mode: str = "merge",
) -> ResolvedWriteMode:
    fmt = (file_format or "").lower()
    if fmt and fmt not in mergeable:
        raise SinkWriteError(
            f"Write mode '{mode}' requires a Delta target, got file_format '{fmt}'.",
            context={"Format": connector, "file_format": fmt},
            hint="Set file_format: delta on this output, or use mode: append/overwrite.",
        )

    keys = parse_merge_keys(cfg)
    if not keys:
        raise SinkWriteError(
            f"Write mode '{mode}' requires 'merge_keys'.",
            context={"Format": connector},
            hint="Add merge_keys: [id, dt] (or options.merge_keys: 'id,dt') to the output config.",
        )

    if mode == "insert_new":
        # First run has nothing to match against: the target is created. errorifexists,
        # not overwrite: a target that exists but is not Delta is refused, never
        # replaced, because this mode promises to change nothing that is there.
        return ResolvedWriteMode(
            mode="insert_new", save_mode="errorifexists", merge_keys=tuple(keys)
        )

    # First run has nothing to merge into — the target is created by an overwrite.
    return ResolvedWriteMode(mode="merge", save_mode="overwrite", merge_keys=tuple(keys))


def _resolve_overwrite_partitions(
    cfg: Dict[str, Any],
    *,
    connector: str,
    file_format: Optional[str],
    mergeable: frozenset = _DEFAULT_MERGE_FORMATS,
) -> ResolvedWriteMode:
    fmt = (file_format or "").lower()
    replace_where = cfg.get("replace_where") or (cfg.get("options") or {}).get("replaceWhere")

    if replace_where:
        if fmt and fmt not in mergeable:
            raise SinkWriteError(
                f"'replace_where' requires a Delta target, got file_format '{fmt}'.",
                context={"Format": connector, "file_format": fmt},
                hint="Set file_format: delta, or drop replace_where to use dynamic "
                "partition overwrite instead.",
            )
        return ResolvedWriteMode(
            mode="overwrite_partitions",
            save_mode="overwrite",
            options={"replaceWhere": str(replace_where)},
        )

    # No predicate — fall back to Spark's dynamic partition overwrite, which is
    # only meaningful on a partitioned target. Without partitions it silently
    # degrades to a full overwrite, which is exactly the accident this mode exists
    # to prevent, so refuse it.
    if not (cfg.get("partitionBy") or []):
        raise SinkWriteError(
            "Write mode 'overwrite_partitions' requires 'partitionBy' or 'replace_where'.",
            context={"Format": connector},
            hint="Add partitionBy: [dt] to the output config (dynamic partition overwrite), "
            "or set replace_where: \"dt = '{{ dt }}'\" for a Delta predicate overwrite.",
        )

    return ResolvedWriteMode(mode="overwrite_partitions", save_mode="overwrite")


# --------------------------------------------------------------------------- #
# Backward-compatibility shim (issue #37).
#
# apply / merge_into / target_exists / dynamic_partition_overwrite moved to
# ubunye.adapters.spark.write_exec. In-repo writers import them from there. This
# lazy re-export keeps any third-party connector that still calls
# ``write_modes.apply(...)`` working, with a DeprecationWarning, until a later
# major removes it. The import is lazy (PEP 562) so the core never imports the
# Spark adapter at module load — the dependency only exists if someone reaches
# for a moved name.
# --------------------------------------------------------------------------- #
_MOVED_TO_SPARK_ADAPTER = frozenset(
    {"apply", "merge_into", "target_exists", "dynamic_partition_overwrite"}
)


def __getattr__(name: str) -> Any:
    if name in _MOVED_TO_SPARK_ADAPTER:
        warnings.warn(
            f"ubunye.core.write_modes.{name} has moved to "
            f"ubunye.adapters.spark.write_exec.{name}. Import it from there; "
            "the shim in core will be removed in a future major.",
            DeprecationWarning,
            stacklevel=2,
        )
        from ubunye.adapters.spark import write_exec

        return getattr(write_exec, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
