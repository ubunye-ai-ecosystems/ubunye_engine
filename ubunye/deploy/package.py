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

import base64
import hashlib
import io
import json
import re
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

#: Printed around the run record at the end of a cloud run.
RECORD_BEGIN = "=====UBUNYE-RUN-RECORD-BEGIN====="
RECORD_END = "=====UBUNYE-RUN-RECORD-END====="
#: Each part of the record: ``UBUNYE-RECORD-PART <n>/<total> <base64>``.
RECORD_PART = "UBUNYE-RECORD-PART"
#: The whole record's SHA-256, printed before its parts.
RECORD_DIGEST = "UBUNYE-RECORD-SHA256"
#: Characters of base64 per part. Log stores cut lines both at a width (CloudWatch at
#: about 1,000) and wherever their buffer flushes, so each part says its own length.
PART_SIZE = 600
_PART = re.compile(re.escape(RECORD_PART) + r" (\d+)/(\d+) (\d+) ([A-Za-z0-9+/=]*)")
_DIGEST = re.compile(re.escape(RECORD_DIGEST) + r" ([0-9a-f]{64})")
_B64 = re.compile(r"[A-Za-z0-9+/=]+")
#: A whole marker line, to its end: what a line starting one must hold to be read alone.
_WHOLE = {
    RECORD_PART: re.compile(re.escape(RECORD_PART) + r" \d+/\d+ \d+ [A-Za-z0-9+/=]*\s*"),
    RECORD_DIGEST: re.compile(re.escape(RECORD_DIGEST) + r" [0-9a-f]{64}\s*"),
}


def _marker_start(line: str) -> Tuple[Optional[str], str]:
    """The marker a line starts to print but does not finish, and the piece printed."""
    for marker, whole in _WHOLE.items():
        at = line.find(marker)
        if at >= 0:
            piece = line[at:]
            return (None, "") if whole.fullmatch(piece) else (marker, piece)
    tokens = line.split()
    last = tokens[-1] if tokens else ""
    for marker in _WHOLE:
        if last and marker.startswith(last):
            return marker, last
    return None, ""


def _mend(lines: List[str]) -> List[str]:
    """Join a marker a log store cut in two (F-035) back into one line.

    Glue's CloudWatch flushed its buffer inside a marker and sent ``UB`` and
    ``UNYE-RECORD-PART 9/14 600 ...`` as two lines, so part 9 had no header. A line
    that starts a marker without finishing it is joined with the next line, with or
    without a space, skipping any prefix the store put on that line, but only when
    the join makes a whole marker and the next line holds no marker of its own (so a
    part's last character that looks like the start of a marker is never taken). A
    wrong join cannot pass: each part says its length and the record its SHA-256.
    """
    out: List[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        marker, piece = _marker_start(line)
        if marker and i + 1 < len(lines) and not any(m in lines[i + 1] for m in _WHOLE):
            tokens = lines[i + 1].split()
            joined = None
            for k in range(len(tokens)):
                rest = " ".join(tokens[k:])
                for glue in ("", " "):
                    if _WHOLE[marker].fullmatch(piece + glue + rest):
                        joined = piece + glue + rest
                        break
                if joined:
                    break
            if joined:
                out += [line[: len(line) - len(piece)], joined]
                i += 2
                continue
        out.append(line)
        i += 1
    return out


ENTRY_SCRIPT = (
    '''\
"""Run Ubunye task(s) in a cloud job, and print the run record(s).

Written by `ubunye deploy`. Arguments: --task PATH (usecase/package/task inside
the bundle; several, comma-separated, run in that order), --mode, --dt, --bundle (the zip, when the platform did not unpack it
next to this script: a local path or an s3:// URI), --backend, and any number of
--var KEY=VALUE (template variables) and --env KEY=VALUE (environment).
"""

import argparse
import base64
import hashlib
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
    first = args.task.split(",")[0]
    for candidate in ("bundle", ".", "/app/pipelines"):
        if os.path.isdir(os.path.join(candidate, first)):
            root = os.path.abspath(candidate)
            break
    else:
        if not args.bundle:
            raise SystemExit(f"task {first} not found and no --bundle given")
        root = _fetch(args.bundle, tempfile.mkdtemp(prefix="ubunye-"))

    import ubunye

    variables = dict(v.split("=", 1) for v in args.var)
    variables.update(json.loads(args.var_json))
    error = None
    done = []
    # Several tasks (comma-separated) run in order in this one job, in one session, so
    # a task can read what the one before it wrote to the job's own disk. The first
    # that fails stops the rest.
    for task in [t for t in args.task.split(",") if t]:
        lineage = tempfile.mkdtemp(prefix="ubunye-lineage-")
        try:
            ubunye.run_task(
                os.path.join(root, task),
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
            with open(max(records, key=os.path.getmtime), encoding="utf-8") as fh:
                done.append(json.load(fh))
        if error is not None:
            break
    if done:
        # One task prints its record; several print them in one document, in order.
        record = done[0] if "," not in args.task else {"ubunye_records": done}
        # In short numbered base64 parts: a log store splits a long line (CloudWatch
        # cut a record at about 1,000 characters) and can reorder lines printed in the
        # same instant (Log Analytics); the reader puts the parts back by number.
        # Each part says its length, and the whole record its SHA-256, because a log
        # store also cuts a line wherever its buffer flushes (Glue's CloudWatch cut
        # part 7 of 8 at 295 of 600 characters) and sends the rest as the next line.
        raw = json.dumps(record, default=str).encode("utf-8")
        text = base64.b64encode(raw).decode()
        parts = [text[i:i + PART_SIZE] for i in range(0, len(text), PART_SIZE)] or [""]
        print("RECORD_BEGIN", flush=True)
        print("RECORD_DIGEST", hashlib.sha256(raw).hexdigest(), flush=True)
        for n, part in enumerate(parts, 1):
            print("RECORD_PART", "%d/%d" % (n, len(parts)), len(part), part, flush=True)
        print("RECORD_END", flush=True)
    if error is not None:
        raise error


if __name__ == "__main__":
    main()
'''.replace('"RECORD_BEGIN"', repr(RECORD_BEGIN))
    .replace('"RECORD_END"', repr(RECORD_END))
    .replace('"RECORD_PART"', repr(RECORD_PART))
    .replace('"RECORD_DIGEST"', repr(RECORD_DIGEST))
    .replace("PART_SIZE", str(PART_SIZE))
)


def _tasks(task: Union[str, Sequence[str]]) -> List[str]:
    tasks = [task] if isinstance(task, str) else list(task)
    if not tasks or any(not t or "," in t for t in tasks):
        raise ValueError(f"give one or more task names without commas, not {task!r}")
    return tasks


def task_path(usecase: str, package: str, task: Union[str, Sequence[str]]) -> str:
    """``usecase/package/task``; several tasks give their paths comma-separated, in order."""
    return ",".join(f"{usecase}/{package}/{t}" for t in _tasks(task))


def unpack_records(record: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """The run record(s) a job printed: one per task, in the order they ran."""
    if record is None:
        return []
    return list(record.get("ubunye_records") or [record])


def bundle(usecase_dir: Path, usecase: str, package: str, task: Union[str, Sequence[str]]) -> bytes:
    """The task folder(s) as a zip, at ``usecase/package/task`` (caches left out)."""
    root = Path(usecase_dir)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as z:
        for name in _tasks(task):
            folder = root / usecase / package / name
            if not (folder / "config.yaml").is_file():
                raise FileNotFoundError(f"no config.yaml in {folder}")
            for path in sorted(folder.rglob("*")):
                rel = path.relative_to(root)
                if path.is_dir() or "__pycache__" in rel.parts or path.suffix == ".pyc":
                    continue
                z.write(path, rel.as_posix())
    return buffer.getvalue()


class RecordIncomplete(ValueError):
    """The log does not hold the whole record, provably intact: never a partial record."""


def _parts_from(log: str) -> Tuple[Dict[int, str], int, Optional[str]]:
    """The parts in ``log`` by number, their count, and the record's SHA-256.

    A part a log store cut short (it says its own length) is completed from the lines
    that follow it, which is where the store puts the rest: the last token of each,
    while it is plain base64 and not the start of another part or a marker.
    """
    lines = _mend(log.splitlines())
    digest = _DIGEST.search("\n".join(lines))
    parts: Dict[int, str] = {}
    damaged: Dict[int, str] = {}
    totals = set()
    for i, line in enumerate(lines):
        m = _PART.search(line)
        if not m:
            continue
        n, total, length, payload = int(m[1]), int(m[2]), int(m[3]), m[4]
        totals.add(total)
        j = i + 1
        while len(payload) < length and j < len(lines):
            nxt = lines[j].strip()
            j += 1
            if not nxt:
                continue
            if _PART.search(nxt) or _DIGEST.search(nxt) or RECORD_END in nxt:
                break
            token = nxt.split()[-1]
            if not _B64.fullmatch(token):
                break
            payload += token
        if len(payload) == length:
            parts.setdefault(n, payload)
        else:
            damaged[n] = f"part {n} arrived with {len(payload)} of {length} characters"
    if len(totals) > 1:
        raise RecordIncomplete(f"record parts disagree on their count: {sorted(totals)}")
    total = totals.pop() if totals else 0
    for n, why in damaged.items():
        if n not in parts:
            raise RecordIncomplete(why + " and could not be completed from the log")
    return parts, total, digest[1] if digest else None


def read_record(log: str) -> Optional[Dict[str, Any]]:
    """The run record a cloud run printed, from its log text; None if absent.

    Reads the numbered parts the entry script prints, from anywhere in the log and in
    any order, completing a part a log store cut short, then checks the whole record
    against its SHA-256. A record that is not provably whole raises
    :class:`RecordIncomplete`. Logs from engines before the parts existed hold the
    record as one JSON line.
    """
    parts, total, digest = _parts_from(log)
    if total:
        missing = [n for n in range(1, total + 1) if n not in parts]
        if missing:
            raise RecordIncomplete(
                f"the log holds {total - len(missing)} of the record's {total} parts; "
                f"missing {missing[:10]}"
            )
        raw = base64.b64decode("".join(parts[n] for n in range(1, total + 1)))
        if digest is not None and hashlib.sha256(raw).hexdigest() != digest:
            raise RecordIncomplete("the record read from the log does not match its SHA-256")
        return json.loads(raw.decode("utf-8"))
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
