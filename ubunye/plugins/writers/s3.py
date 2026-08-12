"""S3 writer plugin (generic writer).

Writes a DataFrame to a given path using the configured format/mode.

Config keys:
  - path: "s3a://bucket/prefix/"           # required
  - file_format: parquet|delta|csv|json|orc|avro (default: parquet)
  - mode: append|overwrite|errorifexists|ignore|merge|overwrite_partitions
          (default: append)
  - partitionBy: ["dt", "region"]          # optional
  - merge_keys: ["id", "dt"]               # required for mode: merge (Delta only)
  - replace_where: "dt = '2026-01-01'"     # optional Delta predicate for
                                           # mode: overwrite_partitions
  - options:                               # Spark writer options
      mergeSchema: "true"
"""

from __future__ import annotations

from typing import Any

from ubunye.core import write_modes
from ubunye.core.errors import SinkWriteError
from ubunye.core.interfaces import Writer

SUPPORTED_MODES = write_modes.ALL_MODES
DEFAULT_MODE = "append"


class S3Writer(Writer):
    """Write a Spark DataFrame to S3 (or any filesystem path Spark understands)."""

    SUPPORTS_MERGE = True
    MERGE_FILE_FORMATS = frozenset({"delta"})

    @classmethod
    def validate_config(cls, cfg):
        return [] if cfg.get("path") else ["format 's3' requires 'path'"]

    def write(self, df: Any, cfg: dict, backend) -> None:
        path = cfg.get("path")
        if not path:
            raise SinkWriteError(
                "'path' is required for S3Writer.",
                context={"Format": "s3"},
                hint="Set path: 's3a://bucket/prefix/' in your output config.",
            )

        fmt = (cfg.get("file_format") or "parquet").lower()
        resolved = write_modes.resolve(
            cfg,
            connector="s3",
            supported=SUPPORTED_MODES,
            merge_formats=self.MERGE_FILE_FORMATS,
            default=DEFAULT_MODE,
            file_format=fmt,
        )

        # The data-plane write seam (#38): hand the resolved mode to the backend
        # to execute. SparkBackend runs it through Spark (MERGE, dynamic overwrite,
        # save); PandasBackend writes the file with pandas.
        backend.execute_write(
            df,
            resolved,
            connector="s3",
            file_format=fmt,
            path=path,
            partition_by=cfg.get("partitionBy"),
            options=cfg.get("options"),
        )
