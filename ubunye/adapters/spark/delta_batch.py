"""A Delta append of a named batch that lands once (ADR 008, F-071).

A Delta append is one commit, so ADR 008 cannot claim its files: moving files behind
Delta's log would corrupt the table. Delta has its own answer, idempotent writes: a
write that carries ``txnAppId`` and ``txnVersion`` is skipped when the table already
holds a commit of that app id at that version or higher (``SetTransaction`` in the
log). Probed on Spark 4.2 and delta-spark 4.4: a path, a new path, a catalog table
and a new catalog table all skip the second write and commit nothing.

So, under a run lease with a named batch:

* ``replace_where`` set on the append: the write replaces the rows it selects, in
  one commit (``replaceWhere``), every run. A retry or a ``--rerun`` writes the batch
  again from the current data, so a bad batch can be corrected. Delta refuses data
  outside the predicate, so a wrong predicate fails loudly instead of deleting
  another batch. No ``txnAppId``: a replace is idempotent by itself, and keying it
  would skip a deliberate correction.
* otherwise: the append carries ``txnAppId`` = task, output and batch, and
  ``txnVersion`` 0. A run killed or failed after its commit leaves the rows; the
  rerun commits nothing. If the table version did not move, the run says the batch
  was already there, so rows removed by hand are not lost in silence. Only a run
  that follows such a run checks that (two ``DESCRIBE HISTORY`` calls): the lease
  knows (it took over a dead run, or a failed run left a note). A first run makes
  no extra call.
* ``--rerun`` of a finished batch with no ``replace_where``: a plain append, as
  before; the run warned at its start that the batch is added again.

With no lease or no named batch, an append is a plain append, as before, unless the
output sets ``replace_where``, which always replaces its rows.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Optional

from ubunye.core import runs

logger = logging.getLogger(__name__)

#: Writer options that make a Delta write idempotent; a config that sets them itself
#: is left alone.
TXN_OPTIONS = ("txnAppId", "txnVersion")


def _version(spark: Any, table: Optional[str], path: Optional[str]) -> Optional[int]:
    """The table's latest commit version, or None when there is no table yet."""
    ref = table if table else "delta.`" + str(path).replace("`", "``") + "`"
    try:
        rows = spark.sql(f"DESCRIBE HISTORY {ref} LIMIT 1").collect()
    except Exception:  # noqa: BLE001 (no table yet, or not Delta)
        return None
    return int(rows[0]["version"]) if rows else None


def append(
    *,
    spark: Any,
    table: Optional[str],
    path: Optional[str],
    opts: Dict[str, Any],
    batch_where: Optional[str],
    save: Callable[[str, Dict[str, Any]], None],
) -> bool:
    """Append to a Delta target so the batch lands once. ``save(mode, opts)`` does
    the Spark write. Returns False, having written nothing, when this is a plain
    append (the caller then writes it as before)."""
    target = table or path
    if any(k in opts for k in TXN_OPTIONS):
        logger.info("Delta append to %s sets its own txnAppId/txnVersion; left as is.", target)
        return False
    if batch_where:
        runs.batch_write(replaces=True)  # notes it in the lease, when one is held
        save("overwrite", {**opts, "replaceWhere": batch_where})
        logger.info("Replaced the batch's rows in %s (replace_where %s).", target, batch_where)
        return True

    batch = runs.batch_write(replaces=False)
    if batch is None or batch.how != "once":
        return False
    before = _version(spark, table, path) if batch.retry else None
    save("append", {**opts, "txnAppId": batch.app_id, "txnVersion": batch.version})
    if before is not None and _version(spark, table, path) == before:
        logger.warning(
            "Delta output %s already holds this batch: a run of it committed before (a "
            "run that was killed or failed), so this run did not add it again. If those "
            "rows were removed or are wrong, set replace_where on the output (for "
            "example \"dt = '{{ dt }}'\") and run again: it replaces the batch's rows.",
            target,
        )
    return True
