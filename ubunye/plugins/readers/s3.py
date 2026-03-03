"""S3 reader plugin (generic path-based reader).

Reads a Spark DataFrame from any filesystem path Spark understands
(S3, ADLS, GCS, local) using the configured file format.

Supported config keys:
    path        (str, required)  – e.g. ``s3a://bucket/prefix/``
    file_format (str, default "parquet") – parquet | delta | csv | json | orc | avro
    options     (dict, optional) – passed directly to ``spark.read.options(**options)``
    schema      (str, optional)  – DDL schema string; skips schema inference when set

Note: ``format`` in the IOConfig is the plugin selector (``s3``); use ``file_format``
to specify the actual Spark file format.
"""

from __future__ import annotations

from typing import Any

from ubunye.core.interfaces import Reader


class S3Reader(Reader):
    """Read a Spark DataFrame from S3 (or any filesystem path Spark understands)."""

    def read(self, cfg: dict, backend) -> Any:
        path = cfg.get("path")
        if not path:
            raise ValueError("'path' is required for S3Reader")

        fmt = cfg.get("file_format", "parquet")
        options: dict = cfg.get("options") or {}
        schema: str | None = cfg.get("schema")

        reader = backend.spark.read.format(fmt)

        if options:
            reader = reader.options(**options)

        if schema:
            reader = reader.schema(schema)

        return reader.load(path)
