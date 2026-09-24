"""Run one task on a given Spark master and write what its run record says, as JSON.

Used by test_distributed.py in a fresh process: a local-cluster needs its own
JVM, and one process can hold only one SparkContext.

    python distributed_run.py <task_dir> <master> <max_result_size> <result.json>
"""

from __future__ import annotations

import json
import sys


def main() -> None:
    task_dir, master, max_result, result_path = sys.argv[1:5]

    import ubunye
    from ubunye.backends.spark_backend import SparkBackend

    backend = SparkBackend(
        app_name="distributed-check",
        conf={
            "spark.master": master,
            "spark.executor.memory": "512m",
            "spark.ui.enabled": "false",
            # Many small input splits and an odd number of shuffle partitions,
            # so the work really is spread out.
            "spark.sql.files.maxPartitionBytes": "262144",
            "spark.sql.shuffle.partitions": "7",
            # Far less than the data: anything that pulls rows to the driver
            # (a collect, a toPandas) fails the run instead of passing quietly.
            "spark.driver.maxResultSize": max_result,
            "spark.sql.session.timeZone": "UTC",
        },
    )
    import time

    backend.start()
    wanted = 2 if master.startswith("local-cluster") else 0
    status = backend.spark.sparkContext._jsc.sc().getExecutorMemoryStatus
    deadline = time.monotonic() + 120
    # Executors register in the background; block managers are the driver plus
    # one per executor.
    while status().size() - 1 < wanted and time.monotonic() < deadline:
        time.sleep(1)
    executors = status().size() - 1
    # run_task stops the backend (and so the session it started) when done.
    ubunye.run_task(task_dir, backend=backend, lineage=True)

    from pathlib import Path

    from ubunye.lineage.storage import FileSystemLineageStore

    root = Path(task_dir).parents[2]
    (record,) = FileSystemLineageStore(str(root / ".ubunye" / "lineage")).list_runs(
        "uc/pkg/spread"
    )[:1]
    # A file, not stdout: Spark's shutdown prints to stdout on some systems.
    Path(result_path).write_text(
        json.dumps(
            {
                "executors": executors,
                "status": record.status,
                "outputs": {
                    o.name: {"rows": o.row_count, "data_hash": o.data_hash, "error": o.hash_error}
                    for o in record.outputs
                },
            }
        ),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
