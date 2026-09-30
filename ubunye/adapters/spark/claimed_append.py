"""A Spark path append whose files are claimed before they land (ADR 008).

Rerun safety takes back a failed or dead run's appends only if the writer named each
file in the run's lease *before* the file appeared in the output. Spark cannot be
asked for its file names in advance: it names each file ``part-NNNNN-<job uuid>...``
with a job UUID it makes itself (``InsertIntoHadoopFsRelationCommand``, Spark 3.5 and
4), and when files appear depends on the output committer (at job commit for
``FileOutputCommitter`` v1, at each task commit for v2, and a failed v2 job leaves
files behind).

So Spark writes the batch into a new folder *inside* the output, named by this run
(``_ubunye-<uuid>``, hidden from readers by its leading ``_`` as Spark's own
``_temporary`` is) and recorded in the lease before it exists. Once Spark has
finished, the data files in that folder are this run's by construction: nothing else
writes there. Their names are claimed in the lease, all of them, and only then is
each moved up into the output in one rename (the same folder tree, so the same disk,
whatever is mounted where). The output folder itself is never listed to decide what
is this run's.

Used only when the claim can be made good: a run lease is held, the format is plain
files (not Delta or another format with a log), and Spark resolves the path to the
local file system (a laptop, a shared disk). Anything else is written by Spark
directly, as before, and named after a crash, never taken back; the log says why.
"""

from __future__ import annotations

import glob
import logging
import os
import re
import shutil
import uuid
from typing import Any, Dict, List, Sequence

from ubunye.core import runs
from ubunye.core.errors import SinkWriteError

logger = logging.getLogger(__name__)

#: File formats whose output is a folder of part files and nothing else.
CLAIMABLE_FORMATS = frozenset({"parquet", "csv", "json", "orc", "avro", "text"})

#: The staging folder's name inside the output: ``_ubunye-<12 hex>``.
STAGING_PREFIX = "_ubunye-"


def _hidden(rel: str) -> bool:
    """Whether this is not a data file: ``_SUCCESS``, ``.crc`` files, and the rest of
    Spark's ``HadoopFSUtils.shouldFilterOutPathName`` (a name with ``=`` is a
    partition folder even when it starts with ``_``). Parquet summary files
    (``_metadata``, ``_common_metadata``) describe the staging folder only, so they
    are not moved either."""
    return any(
        part.startswith(".")
        or (part.startswith("_") and "=" not in part)
        or part.endswith("._COPYING_")
        for part in rel.split(os.sep)
    )


def _local(spark: Any, path: str) -> tuple:
    """(output folder on this machine, the staging folder's URI for Spark, its local
    path). Raises ``_NotLocal`` (with the reason) when Spark does not resolve ``path``
    to the local file system."""
    try:
        jvm = spark._jvm
        conf = spark._jsc.hadoopConfiguration()
    except Exception:  # noqa: BLE001 (Spark Connect)
        raise _NotLocal("no JVM (Spark Connect)") from None
    try:
        hpath = jvm.org.apache.hadoop.fs.Path(path)
        scheme = hpath.toUri().getScheme()
        if scheme and str(scheme).lower() != "file" and len(str(scheme)) > 1:
            raise _NotLocal(f"{scheme}:// is not the local file system")
        fs = hpath.getFileSystem(conf)
        if str(fs.getScheme()).lower() != "file":
            raise _NotLocal(f"the path resolves to {fs.getScheme()}://, not the local disk")
        qualified = fs.makeQualified(hpath)
        local = str(qualified.toUri().getPath())
        if os.name == "nt" and re.match(r"^/[A-Za-z]:", local):
            local = local[1:]
        local = os.path.normpath(local)
        name = f"{STAGING_PREFIX}{uuid.uuid4().hex[:12]}"
        staging_uri = str(jvm.org.apache.hadoop.fs.Path(qualified, name).toString())
        return local, staging_uri, os.path.join(local, name)
    except _NotLocal:
        raise
    except Exception as exc:  # noqa: BLE001 (an unknown scheme, a bad path)
        raise _NotLocal(f"Spark could not resolve the path ({type(exc).__name__})") from None


class _NotLocal(Exception):
    pass


def _data_files(folder: str) -> List[str]:
    """The data files Spark wrote into this run's own staging folder, relative to it."""
    found = []
    for root, _, names in os.walk(folder):
        for name in names:
            rel = os.path.relpath(os.path.join(root, name), folder)
            if not _hidden(rel):
                found.append(rel)
    return sorted(found)


def _not_claimed(path: str, why: str) -> bool:
    lease = runs.current()
    output = getattr(lease, "_current_output", None) if lease is not None else None
    logger.info(
        "Spark append to %s%s is not claimed (%s): a crash leaves it named, not taken back.",
        path,
        f" (output {output})" if output else "",
        why,
    )
    return False


def _cancel_on_interrupt(spark: Any):
    """Run the Spark write in a job group of its own, so an interrupt (Ctrl+C) can
    cancel it before the staging folder is removed: a job left running would keep
    writing into a folder that is gone. Returns (group, restore)."""
    group = f"ubunye-append-{uuid.uuid4().hex[:12]}"
    try:
        sc = spark.sparkContext
        before = {
            k: sc.getLocalProperty(k)
            for k in ("spark.jobGroup.id", "spark.job.description", "spark.job.interruptOnCancel")
        }
        sc.setJobGroup(group, "ubunye claimed append", interruptOnCancel=True)
    except Exception:  # noqa: BLE001 (no SparkContext here: nothing to cancel)
        return None, lambda: None

    def restore() -> None:
        try:
            for k, v in before.items():
                sc.setLocalProperty(k, v)
        except Exception:  # noqa: BLE001
            pass

    return group, restore


def append(
    df: Any,
    spark: Any,
    *,
    path: str,
    file_format: str,
    partition_by: Sequence[str],
    opts: Dict[str, Any],
) -> bool:
    """Append ``df`` to the folder at ``path``, claiming each file before it lands.

    Returns False, having written nothing, when the append cannot be claimed here;
    the caller then lets Spark append directly, as before, and the log says why."""
    if runs.current() is None:
        return _not_claimed(path, "no run lease")
    if str(file_format).lower() not in CLAIMABLE_FORMATS:
        return _not_claimed(path, f"format {file_format} is not a folder of plain files")
    try:
        local, staging_uri, staging = _local(spark, path)
    except _NotLocal as exc:
        return _not_claimed(path, str(exc))
    if os.path.exists(local) and not os.path.isdir(local):
        return _not_claimed(path, "the path is a file")  # Spark's own error says more
    left = glob.glob(os.path.join(glob.escape(local), STAGING_PREFIX + "*"))
    if left:
        # Never deleted here: one may be a live run's (another batch of this output).
        logger.warning(
            "Found staging folders in %s, left by a run that was interrupted or still "
            "being written by another: %s. Readers skip them (leading '_'). Remove them "
            "once no run is writing.",
            local,
            sorted(os.path.basename(p) for p in left),
        )
    runs.staging(staging)  # recorded before it exists: a take back removes it
    os.makedirs(local, exist_ok=True)  # as Spark's append makes it
    group, restore = _cancel_on_interrupt(spark)
    try:
        writer = df.write.mode("errorifexists").format(file_format)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        for key, value in opts.items():
            writer = writer.option(key, str(value))
        try:
            writer.save(staging_uri)
        except BaseException:
            if group is not None:
                try:
                    spark.sparkContext.cancelJobGroup(group)
                except Exception:  # noqa: BLE001
                    pass
            raise
        finally:
            restore()

        files = _data_files(staging)
        clash = [f for f in files if os.path.lexists(os.path.join(local, f))]
        if clash:
            # Spark's names carry a fresh job UUID, so this never happens by chance.
            # A rename would replace a file that is not this run's: refuse.
            raise SinkWriteError(
                f"Cannot append to {local}: a file named like this run's is already there "
                f"({clash[0]}).",
                context={"Path": local},
                hint="Nothing was added. Check what wrote that file.",
            )
        targets = [os.path.join(local, f) for f in files]
        runs.claim_all(targets)
        for rel, target in zip(files, targets):
            os.makedirs(os.path.dirname(target), exist_ok=True)
            os.replace(os.path.join(staging, rel), target)
            runs.landed(target)  # one stat: was this run taken over?
        runs.all_landed(targets)  # and once, the whole lease
        if os.path.exists(os.path.join(staging, "_SUCCESS")):
            with open(os.path.join(local, "_SUCCESS"), "w", encoding="utf-8"):
                pass  # as Spark's own append leaves it
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return True
