"""What every cloud deploy ships: the task as a zip, and the script that runs it.

The zip holds the task folder (config, transformations, helpers) at the same
``usecase/package/task`` path it has locally, so relative imports and
``{{ task_dir }}`` behave the same. The entry script (:data:`ENTRY_SCRIPT`) is
what the cloud runs: it unpacks the zip if it was not unpacked for it, runs the
task through :func:`ubunye.run_task` with a run record, and prints that record
between two markers, so the run's receipt can be read back from the job's log
and gated against any other run (``ubunye gate``).
"""

from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path
from typing import Any, Dict, Optional

#: Printed around the run record at the end of a cloud run.
RECORD_BEGIN = "=====UBUNYE-RUN-RECORD-BEGIN====="
RECORD_END = "=====UBUNYE-RUN-RECORD-END====="

ENTRY_SCRIPT = '''\
"""Run one Ubunye task in a cloud job, and print its run record.

Written by `ubunye deploy`. Arguments: --task PATH (usecase/package/task inside
the bundle), --mode, --dt, --bundle (the zip, when the platform did not unpack it
next to this script: a local path or an s3:// URI), --backend, and any number of
--var KEY=VALUE (template variables) and --env KEY=VALUE (environment).
"""

import argparse
import json
import os
import sys
import tempfile
import zipfile


def _fetch(bundle, into):
    local = bundle
    if bundle.startswith("s3://"):
        import boto3

        bucket, key = bundle[len("s3://"):].split("/", 1)
        local = os.path.join(into, "bundle.zip")
        boto3.client("s3").download_file(bucket, key, local)
    with zipfile.ZipFile(local) as z:
        z.extractall(into)
    return into


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", required=True)
    parser.add_argument("--mode", default="PROD")
    parser.add_argument("--dt", default=None)
    parser.add_argument("--bundle", default=None)
    parser.add_argument("--backend", default=None)
    parser.add_argument("--var", action="append", default=[])
    parser.add_argument("--env", action="append", default=[])
    # Glue takes each argument once, so it passes these two as JSON objects.
    parser.add_argument("--env_json", default="{}")
    parser.add_argument("--var_json", default="{}")
    # A platform whose CLI cannot pass arguments that start with "--" (Azure
    # Container Apps) passes them as a JSON list in UBUNYE_ENTRY_ARGS instead.
    argv = json.loads(os.environ.get("UBUNYE_ENTRY_ARGS", "[]")) + sys.argv[1:]
    args, _platform_args = parser.parse_known_args(argv)
    env = dict(pair.partition("=")[::2] for pair in args.env)
    env.update(json.loads(args.env_json))
    os.environ.update({k: str(v) for k, v in env.items()})

    root = os.getcwd()
    # Unpacked next to the script (Dataproc), the working folder (a container image
    # sets it), or where the images bake the pipelines.
    for candidate in ("bundle", ".", "/app/pipelines"):
        if os.path.isdir(os.path.join(candidate, args.task)):
            root = os.path.abspath(candidate)
            break
    else:
        if not args.bundle:
            raise SystemExit(f"task {args.task} not found and no --bundle given")
        root = _fetch(args.bundle, tempfile.mkdtemp(prefix="ubunye-"))

    import ubunye

    lineage = tempfile.mkdtemp(prefix="ubunye-lineage-")
    variables = dict(v.split("=", 1) for v in args.var)
    variables.update(json.loads(args.var_json))
    error = None
    try:
        ubunye.run_task(
            os.path.join(root, args.task),
            mode=args.mode,
            dt=args.dt,
            backend=args.backend,
            variables=variables or None,
            lineage=True,
            lineage_dir=lineage,
        )
    except Exception as exc:  # the record says what failed; the job still fails below
        error = exc
    records = []
    for dirpath, _dirs, files in os.walk(lineage):
        records += [os.path.join(dirpath, f) for f in files if f.endswith(".json")]
    if records:
        newest = max(records, key=os.path.getmtime)
        with open(newest, encoding="utf-8") as fh:
            record = json.load(fh)
        print("RECORD_BEGIN", flush=True)
        print(json.dumps(record, default=str), flush=True)
        print("RECORD_END", flush=True)
    if error is not None:
        raise error


if __name__ == "__main__":
    main()
'''.replace('"RECORD_BEGIN"', repr(RECORD_BEGIN)).replace('"RECORD_END"', repr(RECORD_END))


def task_path(usecase: str, package: str, task: str) -> str:
    return f"{usecase}/{package}/{task}"


def bundle(usecase_dir: Path, usecase: str, package: str, task: str) -> bytes:
    """The task folder as a zip, at ``usecase/package/task`` (caches left out)."""
    root = Path(usecase_dir)
    folder = root / usecase / package / task
    if not (folder / "config.yaml").is_file():
        raise FileNotFoundError(f"no config.yaml in {folder}")
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as z:
        for path in sorted(folder.rglob("*")):
            rel = path.relative_to(root)
            if path.is_dir() or "__pycache__" in rel.parts or path.suffix == ".pyc":
                continue
            z.write(path, rel.as_posix())
    return buffer.getvalue()


def read_record(log: str) -> Optional[Dict[str, Any]]:
    """The run record a cloud run printed, from its log text; None if absent."""
    if RECORD_BEGIN not in log:
        return None
    body = log.split(RECORD_BEGIN, 1)[1].split(RECORD_END, 1)[0].strip()
    for line in body.splitlines():  # log prefixes can wrap the JSON line
        line = line.strip()
        start = line.find("{")
        if start >= 0:
            return json.loads(line[start:])
    # Nothing between the markers: a log store (Azure's Log Analytics) can return
    # lines printed in the same instant in any order. Take the record from anywhere.
    for line in log.splitlines():
        start = line.find("{")
        if start >= 0 and '"run_id"' in line:
            try:
                return json.loads(line[start:])
            except ValueError:
                continue
    return None


DOCKERFILE = """\
# Written by `ubunye deploy dockerfile`. One image for a Spark job on
# {platform}: the engine, Delta, and the task, baked in (the job downloads nothing).
FROM {base}

{prologue}
ARG UBUNYE_ENGINE="{engine}"
RUN {python} -m venv /opt/ubunye \\
 && /opt/ubunye/bin/pip install --no-cache-dir --upgrade pip \\
 && /opt/ubunye/bin/pip install --no-cache-dir "${{UBUNYE_ENGINE}}" \\
 && /opt/ubunye/bin/pip install --no-cache-dir --no-deps "delta-spark=={delta}" \\
 && (/opt/ubunye/bin/pip uninstall -y pyspark || true)
ENV PYSPARK_PYTHON=/opt/ubunye/bin/python

RUN mkdir -p /opt/jars \\
 && curl -fsSL -o /opt/jars/delta-spark_{scala}-{delta}.jar \\
      https://repo1.maven.org/maven2/io/delta/delta-spark_{scala}/{delta}/delta-spark_{scala}-{delta}.jar \\
 && curl -fsSL -o /opt/jars/delta-storage-{delta}.jar \\
      https://repo1.maven.org/maven2/io/delta/delta-storage/{delta}/delta-storage-{delta}.jar

COPY {pipelines} /app/pipelines
COPY ubunye_entry.py /app/ubunye_entry.py
{epilogue}
"""

#: Per platform: the base image, the Scala build of Spark it runs, and its rules.
IMAGE_PLATFORMS = {
    "dataproc": {
        # Dataproc Serverless mounts Spark and Java itself; the image must not have
        # them, must have a spark user 1099:1099, procps and tini.
        "base": "debian:12-slim",
        "scala": "2.13",
        "python": "python3",
        "prologue": (
            "ENV DEBIAN_FRONTEND=noninteractive\n"
            "RUN apt-get update \\\n"
            " && apt-get install -y --no-install-recommends procps tini python3 python3-venv "
            "curl ca-certificates \\\n"
            " && rm -rf /var/lib/apt/lists/*"
        ),
        "epilogue": (
            "RUN groupadd -g 1099 spark && useradd -u 1099 -g 1099 -d /home/spark -m spark \\\n"
            " && chown -R spark:spark /app\n"
            "USER spark"
        ),
    },
    "emr-serverless": {
        "base": "public.ecr.aws/emr-serverless/spark/emr-7.2.0:latest",
        "scala": "2.12",
        "python": "python3.11",
        "prologue": "USER root\nRUN dnf install -y python3.11 python3.11-pip && dnf clean all",
        "epilogue": "RUN chown -R hadoop:hadoop /app\nUSER hadoop:hadoop",
    },
}


CONTAINER_DOCKERFILE = """\
# Written by `ubunye deploy dockerfile container`. A self-contained Spark job for a
# plain container runtime (Kubernetes, Azure Container Apps, Docker): Java, Spark in
# local mode, Delta, the engine and the pipelines, all baked in.
FROM python:3.11-slim-bookworm

RUN apt-get update \\
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl \\
 && rm -rf /var/lib/apt/lists/*

ARG UBUNYE_ENGINE="{engine}"
RUN pip install --no-cache-dir "pyspark==3.5.*" "delta-spark=={delta}" "${{UBUNYE_ENGINE}}"

# Delta for Spark 3.5 (Scala 2.12), baked in: the job downloads nothing.
RUN mkdir -p /opt/jars \\
 && curl -fsSL -o /opt/jars/delta-spark_2.12-{delta}.jar \\
      https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/{delta}/delta-spark_2.12-{delta}.jar \\
 && curl -fsSL -o /opt/jars/delta-storage-{delta}.jar \\
      https://repo1.maven.org/maven2/io/delta/delta-storage/{delta}/delta-storage-{delta}.jar
ENV PYSPARK_SUBMIT_ARGS="--jars /opt/jars/delta-spark_2.12-{delta}.jar,/opt/jars/delta-storage-{delta}.jar \\
--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \\
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog pyspark-shell"

COPY {pipelines} /app/pipelines
COPY ubunye_entry.py /app/ubunye_entry.py
WORKDIR /app/pipelines
RUN useradd -u 1001 -m ubunye && chown -R ubunye /app
USER ubunye
ENTRYPOINT ["python", "/app/ubunye_entry.py"]
"""

#: The platforms `ubunye deploy dockerfile` knows: the managed Spark ones, and a
#: plain container.
IMAGE_KINDS = (*IMAGE_PLATFORMS, "container")


def dockerfile(
    platform: str, pipelines: str = "pipelines", engine: str = "ubunye-engine", delta: str = "3.2.0"
) -> str:
    """A Dockerfile for a Spark job on ``platform`` (see :data:`IMAGE_KINDS`)."""
    if platform == "container":
        return CONTAINER_DOCKERFILE.format(pipelines=pipelines, engine=engine, delta=delta)
    spec = IMAGE_PLATFORMS[platform]
    return DOCKERFILE.format(
        platform=platform, pipelines=pipelines, engine=engine, delta=delta, **spec
    )
