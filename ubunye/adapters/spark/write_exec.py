"""Spark execution of a resolved write mode.

This is the *mechanism* half of the old ``ubunye.core.write_modes``. The
*decision* half — validating a config's ``mode`` against what a connector can
do, and producing a :class:`~ubunye.core.write_modes.ResolvedWriteMode` — stays
in the core, because it is backend-agnostic and a pandas writer needs it too.

What lives here is everything that speaks Spark: turning a resolved mode into a
``df.write`` chain, a Delta ``MERGE INTO``, or a dynamic-partition overwrite.
It was moved out of ``core/`` under issue #37 so the engine core stops naming
Spark. The functions are unchanged; only their home is.

All four overwrite routes are exercised against a real Spark session in
``tests/integration/test_write_modes_integration.py`` — a mocked writer cannot
tell "Spark accepted this" from "Spark did what we meant".
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from typing import Any, Dict, Iterator, Optional, Sequence

from ubunye.core.errors import SinkWriteError
from ubunye.core.write_modes import ResolvedWriteMode


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

    def _save_as(mode: str, with_opts: Dict[str, Any]) -> None:
        if _replace_rows_of_table(df, spark, table, file_format, mode, with_opts):
            return
        writer = df.write.mode(mode).format(file_format)
        if parts:
            writer = writer.partitionBy(*parts)
        for key, value in with_opts.items():
            writer = writer.option(key, str(value))
        if table:
            writer.saveAsTable(table)
        else:
            writer.save(path)

    def _save() -> None:
        _save_as(resolved.save_mode, opts)

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

    if resolved.save_mode == "append" and str(file_format).lower() == "delta":
        # A named batch lands once: a Delta idempotent write, or a replace of the
        # batch's rows when the output sets replace_where (ADR 008, F-071).
        from ubunye.adapters.spark import delta_batch

        plain = {k: v for k, v in opts.items() if k != "replaceWhere"}
        if delta_batch.append(
            spark=spark,
            table=table,
            path=path,
            opts=plain,
            batch_where=resolved.batch_where,
            save=_save_as,
        ):
            return

    if resolved.save_mode == "append" and path and not table:
        # A path append under a run lease: Spark writes into a staging folder of this
        # run, and each file is claimed before it is moved in (ADR 008).
        from ubunye.adapters.spark import claimed_append

        if claimed_append.append(
            df, spark, path=path, file_format=file_format, partition_by=parts, opts=opts
        ):
            return

    _save()


def _replace_rows_of_table(
    df: Any,
    spark: Any,
    table: Optional[str],
    file_format: str,
    mode: str,
    opts: Dict[str, Any],
) -> bool:
    """Replace the rows ``replaceWhere`` selects in an existing Delta table.

    ``saveAsTable`` with ``replaceWhere`` fails on Spark 3.5 + Delta 3 ("does not
    support truncate in batch mode"); ``writeTo(table).overwrite(condition)`` does
    the same replace in one commit on Spark 3.5 and 4. A table that does not exist
    yet is created by ``saveAsTable`` as before. Returns False when not this case.
    """
    where = opts.get("replaceWhere")
    if not (table and where and mode == "overwrite" and str(file_format).lower() == "delta"):
        return False
    if not spark.catalog.tableExists(table):
        return False
    from pyspark.sql import functions as F

    writer = df.writeTo(table)
    for key, value in opts.items():
        if key != "replaceWhere":
            writer = writer.option(key, str(value))
    writer.overwrite(F.expr(str(where)))
    return True


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

    Each kind of target needs a different mechanism — there is no single switch:

    * ``replaceWhere`` — Delta only; the predicate is already in ``opts``.
    * Delta without a predicate — Delta's own ``partitionOverwriteMode`` write
      option.
    * An existing non-Delta **table** — ``INSERT OVERWRITE``, which is positional,
      so the columns are aligned to the target schema first.
    * A non-Delta **path** (or a table that doesn't exist yet) — a partitioned
      overwrite with ``spark.sql.sources.partitionOverwriteMode=DYNAMIC`` set for
      the duration of the write.
    """
    is_delta = file_format == "delta"

    if "replaceWhere" in opts:  # resolve() has already checked this is Delta
        save()
        return

    if is_delta:
        opts["partitionOverwriteMode"] = "dynamic"
        save()
        return

    if table and target_exists(spark, table=table):
        # INSERT OVERWRITE is positional, not by name — line the columns up with
        # the target's schema or rows land in the wrong columns.
        target_columns = [f.name for f in spark.table(table).schema.fields]
        aligned = df.select(*target_columns) if target_columns else df

        with dynamic_partition_overwrite(spark):
            aligned.write.mode("overwrite").insertInto(table)
        return

    # A path, or a table that does not exist yet (nothing to preserve on the
    # first run — but the conf costs nothing and keeps one code path).
    with dynamic_partition_overwrite(spark):
        save()


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
