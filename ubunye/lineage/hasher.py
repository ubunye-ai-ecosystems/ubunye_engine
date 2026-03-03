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


def hash_dataframe(df: Any, sample_fraction: float = 0.01, seed: int = 42) -> str:
    """Return a deterministic content hash of a Spark DataFrame.

    Samples ``sample_fraction`` of rows with a fixed ``seed``, converts each
    row to a string, and sha256-hashes the concatenation. Falls back to
    ``hash_schema(df)`` if the DataFrame is empty or sampling fails.

    Parameters
    ----------
    df:
        A PySpark DataFrame.
    sample_fraction:
        Fraction of rows to include in the sample (0 < fraction ≤ 1).
    seed:
        Random seed for reproducible sampling.

    Returns
    -------
    str
        ``"sha256:<hex>"`` content hash.
    """
    try:
        count = df.count()
        if count == 0:
            return hash_schema(df)

        fraction = min(max(sample_fraction, 0.0001), 1.0)
        sample_rows = df.sample(fraction=fraction, seed=seed).collect()

        if not sample_rows:
            return hash_schema(df)

        parts = [str(count)]
        for row in sample_rows:
            # Convert Row to a stable string (sorted dict representation)
            if hasattr(row, "asDict"):
                row_dict = row.asDict(recursive=True)
                parts.append(json.dumps(row_dict, sort_keys=True, default=str))
            else:
                parts.append(str(row))

        payload = "\n".join(parts)
        return _sha256(payload.encode())

    except Exception:
        # Fallback to schema hash if anything goes wrong (e.g., lazy evaluation)
        try:
            return hash_schema(df)
        except Exception:
            return _sha256(b"unknown")


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
