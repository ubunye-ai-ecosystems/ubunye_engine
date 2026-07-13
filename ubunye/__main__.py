"""``python -m ubunye`` — the entry point a cloud can actually run.

The engine could only be reached through its console script, ``ubunye``. That is fine on
a laptop and fine in a notebook, and it is **useless in a cloud**.

AWS EMR Serverless and GCP Dataproc Serverless do not give you a shell. They take a
**Python file**, hand it to ``spark-submit``, and run it. There is no ``PATH``, no
console script, no ``-t`` flags. An engine reachable only through its CLI cannot run on
either of them — and you would discover that after wiring up IAM, a bucket and a billing
account, which is the most expensive possible moment to find out.

So:

    spark-submit --py-files deps.zip \\
        -m ubunye \\
        --task-dir s3a://bucket/code/pipelines/sales/etl/daily \\
        --mode PROD --dt 2026-07-13

or, equivalently, as the entry point file itself::

    spark-submit /path/to/ubunye/__main__.py --task-dir ... --mode PROD

It deliberately does **not** create a SparkSession. ``spark-submit`` has already made
one, with the platform's master, its executors and its conf; the engine attaches to it.
Building a second one here would either fail or — far worse — quietly ignore the
cluster and run everything in the driver.
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import List, Optional


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m ubunye",
        description="Run an Ubunye task under spark-submit (EMR, Dataproc, YARN, k8s).",
    )
    parser.add_argument("--task-dir", required=True, help="directory holding config.yaml")
    parser.add_argument("--mode", default="PROD", help="ENGINE profile to use")
    parser.add_argument("--dt", default=None, help="the {{ dt }} passed to the config")
    parser.add_argument("--lineage", action="store_true", help="record lineage")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )

    import ubunye

    ubunye.run_task(
        task_dir=args.task_dir,
        dt=args.dt,
        mode=args.mode,
        lineage=args.lineage,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
