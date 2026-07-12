"""Write-mode semantics, shared by every writer plugin.

Spark's ``DataFrameWriter`` understands exactly four save modes — ``append``,
``overwrite``, ``errorifexists`` and ``ignore``. Ubunye layers two lakehouse
modes on top:

``merge``
    Delta MERGE (upsert) keyed on ``merge_keys``. Spark cannot express this as
    a save mode, so it is executed as a ``MERGE INTO`` statement here.

``overwrite_partitions``
    Dynamic partition overwrite — replace only the partitions present in the
    incoming DataFrame, leave the rest of the table alone. Either a Delta
    ``replaceWhere`` predicate or Spark's ``partitionOverwriteMode=DYNAMIC``.

Connectors declare which modes they can honour via ``supported=``; asking for
one they cannot do is a config error, raised before a single row is written.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence

from ubunye.core.errors import SinkWriteError

# The four modes Spark's DataFrameWriter accepts directly.
NATIVE_SAVE_MODES = frozenset({"append", "overwrite", "errorifexists", "ignore"})

# The two Ubunye adds on top; each needs real code, not a save-mode string.
LAKEHOUSE_MODES = frozenset({"merge", "overwrite_partitions"})

ALL_MODES = NATIVE_SAVE_MODES | LAKEHOUSE_MODES

# Spellings Spark itself tolerates, normalised to the canonical name.
_ALIASES = {"error": "errorifexists", "errorifexist": "errorifexists"}

# Formats that can do a MERGE / replaceWhere.
_MERGE_FORMATS = frozenset({"delta"})


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

    @property
    def is_merge(self) -> bool:
        return self.mode == "merge"

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

    if mode == "merge":
        return _resolve_merge(cfg, connector=connector, file_format=file_format)

    if mode == "overwrite_partitions":
        return _resolve_overwrite_partitions(cfg, connector=connector, file_format=file_format)

    return ResolvedWriteMode(mode=mode, save_mode=mode)


def _resolve_merge(
    cfg: Dict[str, Any], *, connector: str, file_format: Optional[str]
) -> ResolvedWriteMode:
    fmt = (file_format or "").lower()
    if fmt and fmt not in _MERGE_FORMATS:
        raise SinkWriteError(
            f"Write mode 'merge' requires a Delta target, got file_format '{fmt}'.",
            context={"Format": connector, "file_format": fmt},
            hint="Set file_format: delta on this output, or use mode: append/overwrite.",
        )

    keys = parse_merge_keys(cfg)
    if not keys:
        raise SinkWriteError(
            "Write mode 'merge' requires 'merge_keys'.",
            context={"Format": connector},
            hint="Add merge_keys: [id, dt] (or options.merge_keys: 'id,dt') to the output config.",
        )

    # First run has nothing to merge into — the target is created by an overwrite.
    return ResolvedWriteMode(mode="merge", save_mode="overwrite", merge_keys=tuple(keys))


def _resolve_overwrite_partitions(
    cfg: Dict[str, Any], *, connector: str, file_format: Optional[str]
) -> ResolvedWriteMode:
    fmt = (file_format or "").lower()
    replace_where = cfg.get("replace_where") or (cfg.get("options") or {}).get("replaceWhere")

    if replace_where:
        if fmt and fmt not in _MERGE_FORMATS:
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


def apply(
    df: Any,
    spark: Any,
    resolved: ResolvedWriteMode,
    *,
    connector: str,
    file_format: str,
    table: Optional[str] = None,
    path: Optional[str] = None,
    partition_by: Optional[Sequence[str]] = None,
    options: Optional[Dict[str, Any]] = None,
) -> None:
    """Write ``df`` to ``table`` or ``path``, honouring ``resolved``.

    The one place the mode actually happens: a MERGE into an existing target, a
    dynamic-partition-scoped overwrite, or a plain save. Writers call this
    instead of rebuilding the ``df.write`` chain themselves.
    """
    if not (table or path):
        raise SinkWriteError(
            "Nothing to write to — no table or path.",
            context={"Format": connector},
            hint="Set a table or a path on this output.",
        )

    if resolved.is_merge and target_exists(spark, table=table, path=path):
        merge_into(
            df,
            spark,
            connector=connector,
            merge_keys=resolved.merge_keys,
            table=table,
            path=path,
        )
        return

    opts: Dict[str, Any] = dict(options or {})
    opts.pop("merge_keys", None)  # consumed by the resolver; not a Spark option
    opts.update(resolved.options)
    parts = list(partition_by or [])

    def _save() -> None:
        writer = df.write.mode(resolved.save_mode).format(file_format)
        if parts:
            writer = writer.partitionBy(*parts)
        for key, value in opts.items():
            writer = writer.option(key, str(value))
        if table:
            writer.saveAsTable(table)
        else:
            writer.save(path)

    if resolved.is_overwrite_partitions:
        _overwrite_partitions(
            df,
            spark,
            connector=connector,
            file_format=file_format,
            table=table,
            path=path,
            opts=opts,
            save=_save,
        )
        return

    _save()


def _overwrite_partitions(
    df: Any,
    spark: Any,
    *,
    connector: str,
    file_format: str,
    table: Optional[str],
    path: Optional[str],
    opts: Dict[str, Any],
    save,
) -> None:
    """Replace only the partitions present in ``df``.

    Spark has no single mechanism for this, and the obvious one is a trap:
    ``spark.sql.sources.partitionOverwriteMode=DYNAMIC`` is **not** honoured on a
    path-based ``save()`` — verified against Spark 4.1, where it silently wipes
    every other partition (tests/integration/test_write_modes_integration.py).
    It only applies to ``INSERT OVERWRITE`` into a table.

    So there are three real routes, and one dead end:

    * ``replaceWhere`` — Delta only; the predicate is already in ``opts``.
    * Delta without a predicate — Delta's own ``partitionOverwriteMode`` write
      option, which Delta (unlike Spark's file sources) does honour.
    * A non-Delta **table** — ``INSERT OVERWRITE`` with the session conf set,
      which is the one place Spark's conf actually bites.
    * A non-Delta **path** — Spark cannot do it. Refuse, rather than wipe.
    """
    is_delta = file_format == "delta"

    if "replaceWhere" in opts:  # resolve() has already checked this is Delta
        save()
        return

    if is_delta:
        # Delta honours this as a write option; Spark's file sources do not.
        opts["partitionOverwriteMode"] = "dynamic"
        save()
        return

    if not table:
        raise SinkWriteError(
            f"Write mode 'overwrite_partitions' cannot be done safely on a non-Delta "
            f"path write (file_format '{file_format}').",
            context={"Format": connector, "file_format": file_format, "Path": path},
            hint="Spark ignores partitionOverwriteMode on a path write and would "
            "overwrite the whole dataset. Use file_format: delta, set "
            "replace_where, or write to a table instead.",
        )

    if not target_exists(spark, table=table):
        save()  # nothing to preserve on the first run
        return

    # INSERT OVERWRITE is positional, not by name — line the columns up with the
    # target's schema or rows land in the wrong columns.
    target_columns = [f.name for f in spark.table(table).schema.fields]
    aligned = df.select(*target_columns) if target_columns else df

    with dynamic_partition_overwrite(spark):
        aligned.write.mode("overwrite").insertInto(table)


@contextmanager
def dynamic_partition_overwrite(spark: Any) -> Iterator[None]:
    """Flip Spark into dynamic partition overwrite for the duration of a write.

    Restores the previous value afterwards — the conf is session-wide, and
    leaking it would quietly change the behaviour of every later write in the
    same Spark session.
    """
    key = "spark.sql.sources.partitionOverwriteMode"
    try:
        previous = spark.conf.get(key)
    except Exception:  # unset, or a backend without conf access
        previous = None

    spark.conf.set(key, "DYNAMIC")
    try:
        yield
    finally:
        if previous is None:
            try:
                spark.conf.unset(key)
            except Exception:
                spark.conf.set(key, "STATIC")
        else:
            spark.conf.set(key, previous)


def target_exists(spark: Any, *, table: Optional[str] = None, path: Optional[str] = None) -> bool:
    """Whether a merge target already exists.

    A missing target is not an error — it is the first run, and the caller
    creates it with a plain write instead of a MERGE.
    """
    if table:
        try:
            return bool(spark.catalog.tableExists(table))
        except Exception:
            return False
    if path:
        try:
            spark.sql(f"DESCRIBE DETAIL delta.`{path}`")
            return True
        except Exception:
            return False
    return False


def merge_into(
    df: Any,
    spark: Any,
    *,
    connector: str,
    merge_keys: Sequence[str],
    table: Optional[str] = None,
    path: Optional[str] = None,
) -> None:
    """Upsert ``df`` into an existing Delta target with ``MERGE INTO``.

    Matches on ``merge_keys``, updating matched rows and inserting the rest.
    The target must already exist — callers use :func:`target_exists` to decide
    between this and a create-on-first-run write.
    """
    if not (table or path):
        raise SinkWriteError(
            "Write mode 'merge' needs a target table or path.",
            context={"Format": connector},
            hint="Set table: 'catalog.schema.table' or path: 's3a://bucket/prefix/'.",
        )

    columns = set(getattr(df, "columns", []) or [])
    missing = [k for k in merge_keys if columns and k not in columns]
    if missing:
        raise SinkWriteError(
            f"merge_keys not present in the DataFrame: {', '.join(missing)}.",
            context={
                "Format": connector,
                "merge_keys": list(merge_keys),
                "Columns": sorted(columns),
            },
            hint="Merge keys must be columns of the DataFrame being written.",
        )

    target_ref = table if table else f"delta.`{path}`"
    view = f"ubunye_merge_src_{uuid.uuid4().hex}"
    df.createOrReplaceTempView(view)

    condition = " AND ".join(f"t.{k} = s.{k}" for k in merge_keys)
    try:
        spark.sql(
            f"MERGE INTO {target_ref} AS t "
            f"USING {view} AS s "
            f"ON {condition} "
            f"WHEN MATCHED THEN UPDATE SET * "
            f"WHEN NOT MATCHED THEN INSERT *"
        )
    finally:
        try:
            spark.catalog.dropTempView(view)
        except Exception:
            pass
