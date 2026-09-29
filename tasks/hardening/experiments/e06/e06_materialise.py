"""F-040: persist or localCheckpoint? The evidence for the choice in ADR 009.

One output with a column that differs every time it is computed
(``current_timestamp()``, ``rand()`` and a Python UDF that answers differently
each call, as a service or a model can), materialised once with each mechanism,
then written and hashed. Four questions:

1. ``normal``: does the hash of the materialised frame equal the hash of the
   written files?
2. ``block_loss``: after the materialised blocks are dropped (what executor loss
   does on a cluster), does the next action recompute quietly, or fail?
3. ``overwrite_input``: the output overwrites the path it was read from.
4. ``cost``: seconds to materialise, write and hash at ROWS rows, and the size
   the materialised rows take in storage.

    python e06_materialise.py WORK_DIR [ROWS]

Prints one JSON line per mechanism and question.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import time
from pathlib import Path

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

from pyspark import StorageLevel  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from ubunye.adapters.spark.content_hash import fingerprint_spark  # noqa: E402

root = Path(sys.argv[1]).resolve()
rows = int(sys.argv[2]) if len(sys.argv) > 2 else 100_000
shutil.rmtree(root, ignore_errors=True)
root.mkdir(parents=True)

spark = (
    SparkSession.builder.config("spark.sql.session.timeZone", "UTC")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
sc = spark.sparkContext
src = str(root / "src")
spark.range(rows).withColumn("qty", (F.col("id") % 97) + 1).write.parquet(src)


def build(path: str):
    return (
        spark.read.parquet(path)
        .withColumn("loaded_at", F.current_timestamp())
        .withColumn("noise", F.rand())
        .withColumn("call", _service())
    )


def _service():
    """A Python UDF that answers differently each call, like a service or a model."""
    import uuid

    return F.udf(lambda: str(uuid.uuid4()), "string").asNondeterministic()()


def persist(df):
    df = df.persist(StorageLevel.MEMORY_AND_DISK)
    df.count()
    return df


def checkpoint(df):
    return df.localCheckpoint(eager=True)


def release(df, how: str) -> None:
    if how == "persist":
        df.unpersist(blocking=True)
    else:
        df._jdf.logicalPlan().rdd().unpersist(True)


def storage_bytes() -> int:
    total = 0
    for info in sc._jsc.sc().getRDDStorageInfo():
        total += info.memSize() + info.diskSize()
    return total


def drop_blocks() -> int:
    """Remove every stored RDD block, as losing the executors that held them does."""
    master = sc._jvm.org.apache.spark.SparkEnv.get().blockManager().master()
    ids = [info.id() for info in sc._jsc.sc().getRDDStorageInfo()]
    for rdd_id in ids:
        master.removeRdd(rdd_id, True)
    return len(ids)


def why(exc: Exception) -> str:
    """The line of a Spark error that names the cause."""
    lines = [ln.strip() for ln in str(exc).splitlines() if ln.strip()]
    for ln in lines:
        if "Checkpoint block" in ln or "Exception:" in ln:
            return ln[:240]
    return lines[0][:240] if lines else type(exc).__name__


MECHANISMS = {"persist": persist, "localCheckpoint": checkpoint}


def report(**kv) -> None:
    print(json.dumps(kv), flush=True)


for how, materialise in MECHANISMS.items():
    # 1. normal
    out = str(root / f"out_{how}")
    t0 = time.perf_counter()
    df = materialise(build(src))
    t_mat = time.perf_counter() - t0
    size = storage_bytes()
    t0 = time.perf_counter()
    df.write.mode("overwrite").parquet(out)
    t_write = time.perf_counter() - t0
    t0 = time.perf_counter()
    recorded = fingerprint_spark(df)
    t_hash = time.perf_counter() - t0
    written = fingerprint_spark(spark.read.parquet(out))
    report(
        mechanism=how,
        question="normal",
        rows=rows,
        same=recorded.data_hash == written.data_hash,
        materialise_s=round(t_mat, 3),
        write_s=round(t_write, 3),
        hash_s=round(t_hash, 3),
        stored_bytes=size,
    )
    # 5. a frame derived from the materialised one, used after release
    derived = df.filter(F.col("qty") > 1)
    release(df, how)
    try:
        n = derived.count()
        report(mechanism=how, question="derived_after_release", ok=True, rows=n)
    except Exception as exc:  # noqa: BLE001
        report(
            mechanism=how,
            question="derived_after_release",
            ok=False,
            error=why(exc),
        )
    report(mechanism=how, question="left_in_storage_after_release", bytes=storage_bytes())

    # 2. block loss between materialise and write
    out = str(root / f"lost_{how}")
    df = materialise(build(src))
    checked = fingerprint_spark(df)  # what the expectations would have seen
    dropped = drop_blocks()
    try:
        df.write.mode("overwrite").parquet(out)
        written = fingerprint_spark(spark.read.parquet(out))
        report(
            mechanism=how,
            question="block_loss",
            rdds_dropped=dropped,
            failed=False,
            checked_rows_are_written_rows=checked.data_hash == written.data_hash,
        )
    except Exception as exc:  # noqa: BLE001
        report(
            mechanism=how,
            question="block_loss",
            rdds_dropped=dropped,
            failed=True,
            error=why(exc),
        )
    try:
        release(df, how)
    except Exception:  # noqa: BLE001
        pass

    # 3. the output overwrites its own input
    own = str(root / f"own_{how}")
    shutil.copytree(src, own)
    df = materialise(build(own))
    try:
        df.write.mode("overwrite").parquet(own)
        recorded = fingerprint_spark(df)
        written = fingerprint_spark(spark.read.parquet(own))
        report(
            mechanism=how,
            question="overwrite_input",
            failed=False,
            same=recorded.data_hash == written.data_hash,
            written_rows=written.row_count,
        )
    except Exception as exc:  # noqa: BLE001
        report(
            mechanism=how,
            question="overwrite_input",
            failed=True,
            error=why(exc),
        )
    try:
        release(df, how)
    except Exception:  # noqa: BLE001
        pass

# 3b. the same overwrite with nothing materialised: what plain Spark does
own = str(root / "own_plain")
shutil.copytree(src, own)
try:
    build(own).write.mode("overwrite").parquet(own)
    report(mechanism="none", question="overwrite_input", failed=False)
except Exception as exc:  # noqa: BLE001
    report(mechanism="none", question="overwrite_input", failed=True, error=why(exc))

spark.stop()
