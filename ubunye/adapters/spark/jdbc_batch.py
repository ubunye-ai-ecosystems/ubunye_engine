"""A JDBC append whose batch lands once: staged, then moved in by one transaction
(ADR 008, F-072).

Spark's JDBC writer commits each partition in a transaction of its own, so a JDBC
append that dies part way leaves part of the batch, and nothing records which part.
There are no files to claim. What a database does have is a transaction.

So an append output that names its batch's rows with ``replace_where`` is written in
two steps:

1. Spark writes the batch into a staging table beside the target
   (``ubunye_stage_<12 hex>``, in the target's schema), replacing it if a dead run
   left one of the same batch. The target is not touched.
2. One database transaction deletes the rows ``replace_where`` selects from the
   target and inserts the staged rows, by column name. It commits, or nothing
   changed. The staging table is then dropped.

A run killed before the commit leaves the target as it was; one killed after it left
the batch in place, and the rerun replaces it with the same rows. ``--rerun``
replaces a finished batch. The SQL is plain (``DELETE``, ``INSERT INTO ... SELECT``,
``DROP TABLE``), with identifiers quoted by Spark's own dialect for the URL.

Without ``replace_where`` a JDBC append is Spark's, as before: it cannot be taken
back, and the run says so after a crash.
"""

from __future__ import annotations

import hashlib
import logging
import uuid
from typing import Any, Dict, List, Optional

from ubunye.core import runs
from ubunye.core.errors import SinkWriteError

logger = logging.getLogger(__name__)

STAGE_PREFIX = "ubunye_stage_"


def _stage_name(table: str, batch: Optional[runs.BatchWrite]) -> str:
    """Beside the target, in its schema. Named by the batch when a lease holds one, so a
    retry of the batch replaces a staging table a killed run left behind."""
    tag = hashlib.sha256(batch.app_id.encode()).hexdigest()[:12] if batch else uuid.uuid4().hex[:12]
    schema = table.rsplit(".", 1)[0] + "." if "." in table else ""
    return f"{schema}{STAGE_PREFIX}{tag}"


def _connect(spark: Any, url: str, driver: Optional[str], props: Dict[str, str]) -> Any:
    try:
        jvm = spark._jvm
    except Exception:  # noqa: BLE001
        jvm = None
    if jvm is None:
        raise SinkWriteError(
            "A JDBC append with replace_where needs a classic Spark session (with a JVM).",
            context={"Format": "jdbc", "URL": url},
            hint="Run it on classic Spark, or drop replace_where (Spark's own append).",
        )
    if driver:
        # Spark's registry makes a driver from spark.jars visible to DriverManager.
        jvm.org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry.register(driver)
    info = jvm.java.util.Properties()
    for key, value in props.items():
        info.setProperty(key, value)
    return jvm, jvm.java.sql.DriverManager.getConnection(url, info)


def _exists(conn: Any, table: str) -> bool:
    stmt = conn.createStatement()
    try:
        stmt.executeQuery(f"SELECT 1 FROM {table} WHERE 1 = 0").close()
        return True
    except Exception:  # noqa: BLE001 (SQLException: no such table)
        return False
    finally:
        stmt.close()


def _move_in(conn: Any, *, table: str, stage: str, where: str, columns: List[str]) -> int:
    """Delete the batch's rows and insert the staged ones, in one transaction.
    Returns the number of rows deleted."""
    cols = ", ".join(columns)
    conn.setAutoCommit(False)
    stmt = conn.createStatement()
    try:
        deleted = stmt.executeUpdate(f"DELETE FROM {table} WHERE {where}")
        stmt.executeUpdate(f"INSERT INTO {table} ({cols}) SELECT {cols} FROM {stage}")
        conn.commit()
        return int(deleted)
    except BaseException:
        conn.rollback()
        raise
    finally:
        stmt.close()
        conn.setAutoCommit(True)


def _drop(conn: Any, stage: str) -> None:
    stmt = conn.createStatement()
    try:
        stmt.executeUpdate(f"DROP TABLE {stage}")
    except Exception as exc:  # noqa: BLE001 (left behind: named, harmless)
        logger.warning(
            "Could not drop the staging table %s (%s). It holds no target data; drop it "
            "by hand.",
            stage,
            str(exc).splitlines()[0][:120],
        )
    finally:
        stmt.close()


def append(
    df: Any,
    *,
    url: str,
    table: str,
    where: str,
    driver: Optional[str],
    props: Dict[str, str],
    options: Dict[str, Any],
) -> None:
    """Write ``df`` as the batch of ``table`` that ``where`` selects, replacing it.

    ``props``: user and password. ``options``: the output's extra Spark JDBC options,
    used for the staging write."""
    spark = df.sparkSession
    batch = runs.batch_write(replaces=True)  # noted in the lease, when one is held
    stage = _stage_name(table, batch)

    def write(target: str, mode: str, frame: Any) -> None:
        writer = frame.write.format("jdbc").mode(mode).option("url", url)
        writer = writer.option("dbtable", target)
        if driver:
            writer = writer.option("driver", driver)
        for key, value in {**props, **options}.items():
            writer = writer.option(key, str(value))
        writer.save()

    jvm, conn = _connect(spark, url, driver, props)
    try:
        if not _exists(conn, table):
            write(table, "append", df.limit(0))  # created by Spark, as a plain append would
        write(stage, "overwrite", df)
        dialect = jvm.org.apache.spark.sql.jdbc.JdbcDialects.get(url)
        columns = [str(dialect.quoteIdentifier(c)) for c in df.columns]
        try:
            deleted = _move_in(conn, table=table, stage=stage, where=where, columns=columns)
        finally:
            _drop(conn, stage)
        logger.info(
            "Replaced the batch in %s (%s): %d row(s) removed, the new batch inserted in "
            "the same transaction.",
            table,
            where,
            deleted,
        )
    finally:
        conn.close()
