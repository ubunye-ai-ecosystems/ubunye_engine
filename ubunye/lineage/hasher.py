"""Deterministic hashing utilities for DataFrames, schemas, and files.

All functions produce ``"sha256:<hex>"`` strings. PySpark is imported lazily so
the module can be imported in environments without Spark installed (tests,
validation-only runs, lineage CLI commands).

Usage
-----
    from ubunye.lineage.hasher import hash_dataframe, hash_schema, hash_file

    schema_h = hash_schema(df)   # "sha256:abc..."
    data_h   = hash_dataframe(df, sample_fraction=0.01, seed=42)
    file_h   = hash_file("/tmp/output.parquet")
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from typing import Any

# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------


def _sha256(data: bytes) -> str:
    """Return ``"sha256:<hex>"`` for the given bytes."""
    return "sha256:" + hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def hash_schema(df: Any) -> str:
    """Return a deterministic hash of a Spark DataFrame's schema.

    The schema is serialised as a sorted JSON string so column order does not
    affect the hash. Works with any object that has a ``.schema.jsonValue()``
    method (PySpark StructType) or a ``.schema`` attribute that is itself
    JSON-serialisable.

    Parameters
    ----------
    df:
        A PySpark DataFrame (or a mock with a compatible ``.schema``).

    Returns
    -------
    str
        ``"sha256:<hex>"`` of the JSON schema.
    """
    try:
        schema = df.schema
        # PySpark StructType has .jsonValue() → dict
        if hasattr(schema, "jsonValue"):
            schema_dict = schema.jsonValue()
        elif hasattr(schema, "json"):
            schema_dict = json.loads(schema.json())
        else:
            # Fallback: use str representation
            schema_dict = str(schema)
        payload = json.dumps(schema_dict, sort_keys=True, ensure_ascii=True)
    except Exception:
        payload = str(getattr(df, "schema", "unknown"))
    return _sha256(payload.encode())


# The hard ceiling on how many rows lineage may ever pull into the driver.
#
# A fraction alone is not a bound: 1% of a billion rows is ten million rows landing on
# one JVM, and the old code had a fallback that collected the ENTIRE DataFrame when the
# sample came back empty. A fingerprint is a receipt, not a copy. It never needs more
# than a bounded handful of rows to be useful.
MAX_SAMPLE_ROWS = int(os.environ.get("UBUNYE_LINEAGE_SAMPLE_ROWS", "1000"))


@dataclass(frozen=True)
class DataFrameFingerprint:
    """Everything lineage wants to know about an output, computed in ONE place."""

    schema_hash: str
    data_hash: str
    row_count: int


def fingerprint_dataframe(
    df: Any,
    sample_fraction: float = 0.01,
    seed: int = 42,
    max_rows: int = MAX_SAMPLE_ROWS,
) -> DataFrameFingerprint:
    """Fingerprint a DataFrame with one materialisation and bounded driver memory.

    The old path was three separate full executions of the output's entire plan:
    ``hash_dataframe`` counted, then sampled and collected with no upper bound (and
    collected the WHOLE table if the sample was empty), and the recorder then counted
    the same uncached DataFrame a second time. Lineage cost two to three complete
    recomputations of the pipeline it was describing, and could OOM the driver.

    Now:

    * the DataFrame is persisted for the duration, so the plan runs once and the
      second action reads from cache;
    * the count happens exactly once, and the row count and data hash share it;
    * every ``collect`` is capped at ``max_rows``, whatever the table size — the
      empty-sample fallback collects ``df.limit(max_rows)``, never ``df``.
    """
    schema_hash = hash_schema(df)

    persisted = False
    try:
        try:
            df.persist()
            persisted = True
        except Exception:
            pass  # not persistable (mock, pandas, already-persisted): still correct, just slower

        count = int(df.count())
        if count == 0:
            return DataFrameFingerprint(schema_hash, schema_hash, 0)

        fraction = min(max(sample_fraction, 0.0001), 1.0)
        rows = df.sample(fraction=fraction, seed=seed).limit(max_rows).collect()
        if not rows:
            # A small DataFrame can sample to nothing. Take its head, BOUNDED — the old
            # code collected the whole thing here, which on a big table with a tiny
            # fraction was the exact OOM this function exists to avoid.
            rows = df.limit(max_rows).collect()

        parts = [str(count)]
        for row in rows:
            if hasattr(row, "asDict"):
                parts.append(json.dumps(row.asDict(recursive=True), sort_keys=True, default=str))
            else:
                parts.append(str(row))

        payload = "\n".join(parts)
        return DataFrameFingerprint(schema_hash, _sha256(payload.encode()), count)
    except Exception:
        # Best effort, like everything in lineage: a fingerprint failure must not
        # fail a pipeline that already succeeded.
        return DataFrameFingerprint(schema_hash, schema_hash, -1)
    finally:
        if persisted:
            try:
                df.unpersist()
            except Exception:
                pass


def hash_dataframe(df: Any, sample_fraction: float = 0.01, seed: int = 42) -> str:
    """Content hash of a DataFrame. Kept for backward compatibility.

    Delegates to :func:`fingerprint_dataframe`, which is what new code should call —
    it returns the row count from the same single pass instead of leaving callers to
    count again. This wrapper inherits its bounds: no collect can exceed
    ``MAX_SAMPLE_ROWS``, and the whole-table fallback is gone.
    """
    return fingerprint_dataframe(df, sample_fraction=sample_fraction, seed=seed).data_hash


def hash_file(path: str) -> str:
    """Return a sha256 hash of a local file's contents.

    Parameters
    ----------
    path:
        Absolute or relative path to the file.

    Returns
    -------
    str
        ``"sha256:<hex>"`` hash of the file bytes.

    Raises
    ------
    FileNotFoundError
        If the path does not exist.
    """
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return "sha256:" + h.hexdigest()
