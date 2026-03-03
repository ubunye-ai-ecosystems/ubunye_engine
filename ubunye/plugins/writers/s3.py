"""S3 writer plugin (generic writer).

Writes a DataFrame to a given path using the configured format/mode.
"""

from __future__ import annotations

from typing import Any

from ubunye.core.interfaces import Writer


class S3Writer(Writer):
    """Write a Spark DataFrame to S3 (or any filesystem path Spark understands)."""

    def write(self, df: Any, cfg: dict, backend) -> None:
        path = cfg.get("path")
        if not path:
            raise ValueError("'path' is required for S3Writer")
        mode = cfg.get("mode", "overwrite")
        fmt = cfg.get("format", "parquet")
        df.write.mode(mode).format(fmt).save(path)
