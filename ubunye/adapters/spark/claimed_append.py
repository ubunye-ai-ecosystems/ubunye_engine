"""A Spark path append whose files are claimed before they land (ADR 008).

Rerun safety takes back a failed or dead run's appends only if the writer named each
file in the run's lease *before* the file appeared in the output. Spark cannot be
asked for its file names in advance: it names each file ``part-NNNNN-<job uuid>...``
with a job UUID it makes itself (``InsertIntoHadoopFsRelationCommand``, Spark 3.5 and
4), and when files appear depends on the output committer (at job commit for
``FileOutputCommitter`` v1, at each task commit for v2, and a failed v2 job leaves
files behind).

So Spark writes the batch into a new folder beside the output, named by this run
(``.<output>.ubunye-<uuid>``) and recorded in the lease before it exists. Once Spark
has finished, the data files in that folder are this run's by construction: nothing
else writes there. Their names are claimed in the lease, all of them, and only then
is each moved into the output in one rename (same disk). The output folder itself is
never listed to decide what is this run's.

Used only when the claim can be made good: a run lease is held, the format is plain
files (not Delta or another format with a log), and Spark resolves the path to the
local file system (a laptop, a shared disk). Anything else is written by Spark
directly, as before, and named after a crash, never taken back.
"""

from __future__ import annotations

import os
import re
import shutil
import uuid
from typing import Any, Dict, List, Optional, Sequence

from ubunye.core import runs
from ubunye.core.errors import SinkWriteError

#: File formats whose output is a folder of part files and nothing else.
CLAIMABLE_FORMATS = frozenset({"parquet", "csv", "json", "orc", "avro", "text"})


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


def _local(spark: Any, path: str) -> Optional[tuple]:
    """(output folder on this machine, the staging folder's URI for Spark, its local
    path), or None when Spark does not resolve ``path`` to the local file system."""
    try:
        jvm = spark._jvm
        conf = spark._jsc.hadoopConfiguration()
        hpath = jvm.org.apache.hadoop.fs.Path(path)
        scheme = hpath.toUri().getScheme()
        if scheme and str(scheme).lower() != "file" and len(str(scheme)) > 1:
            return None  # s3a, abfss, gs, dbfs, hdfs: not claimed here
        fs = hpath.getFileSystem(conf)
        if str(fs.getScheme()).lower() != "file":
            return None  # a scheme-less path on a cluster whose default is HDFS
        qualified = fs.makeQualified(hpath)
        parent = qualified.getParent()
        if parent is None:
            return None
        local = str(qualified.toUri().getPath())
        if os.name == "nt" and re.match(r"^/[A-Za-z]:", local):
            local = local[1:]
        local = os.path.normpath(local)
        name = f".{os.path.basename(local)}.ubunye-{uuid.uuid4().hex[:12]}"
        staging_uri = str(jvm.org.apache.hadoop.fs.Path(parent, name).toString())
        return local, staging_uri, os.path.join(os.path.dirname(local), name)
    except Exception:  # noqa: BLE001 (Spark Connect has no JVM; an unknown scheme)
        return None


def _data_files(folder: str) -> List[str]:
    """The data files Spark wrote into this run's own staging folder, relative to it."""
    found = []
    for root, _, names in os.walk(folder):
        for name in names:
            rel = os.path.relpath(os.path.join(root, name), folder)
            if not _hidden(rel):
                found.append(rel)
    return sorted(found)


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
    the caller then lets Spark append directly, as before."""
    if runs.current() is None or str(file_format).lower() not in CLAIMABLE_FORMATS:
        return False
    where = _local(spark, path)
    if where is None:
        return False
    local, staging_uri, staging = where
    if os.path.exists(local) and not os.path.isdir(local):
        return False  # Spark's own error says what is wrong
    runs.staging(staging)  # recorded before it exists: a take back removes it
    try:
        writer = df.write.mode("errorifexists").format(file_format)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        for key, value in opts.items():
            writer = writer.option(key, str(value))
        writer.save(staging_uri)

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
        runs.claim_all([os.path.join(local, f) for f in files])
        os.makedirs(local, exist_ok=True)
        for rel in files:
            target = os.path.join(local, rel)
            os.makedirs(os.path.dirname(target), exist_ok=True)
            os.replace(os.path.join(staging, rel), target)
            runs.landed(target)
        if os.path.exists(os.path.join(staging, "_SUCCESS")):
            with open(os.path.join(local, "_SUCCESS"), "w", encoding="utf-8"):
                pass  # as Spark's own append leaves it
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return True
