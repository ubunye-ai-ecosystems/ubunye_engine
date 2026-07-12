"""Binary file reader plugin.

Reads raw files (PDFs, images, audio) through Spark's ``binaryFile`` source,
yielding one row per file with columns ``path``, ``modificationTime``,
``length`` and ``content``. The usual source for document/RAG pipelines.

``format: binary`` was declared in the schema and documented, but no reader was
registered — those configs failed with ``ReaderNotFoundError``.

Spark's ``binaryFile`` source is **read-only**: there is no binary writer, and
``format: binary`` in ``CONFIG.outputs`` is rejected at config-load time.

Config keys:
  - path: "/mnt/raw/documents/"    # required
  - path_glob_filter: "*.pdf"      # optional; only read matching files
  - recursive: true                # optional; descend into sub-directories
  - options: {}                    # Spark reader options
"""

from __future__ import annotations

from typing import Any

from ubunye.core.errors import SourceReadError
from ubunye.core.interfaces import Reader

FORMAT = "binaryFile"


class BinaryReader(Reader):
    """Read raw files into a DataFrame, one row per file."""

    def read(self, cfg: dict, backend) -> Any:
        path = cfg.get("path")
        if not path:
            raise SourceReadError(
                "'path' is required for BinaryReader.",
                context={"Format": "binary"},
                hint="Set path: '/mnt/raw/documents/' in your input config.",
            )

        options: dict = dict(cfg.get("options") or {})
        if cfg.get("path_glob_filter"):
            options["pathGlobFilter"] = cfg["path_glob_filter"]
        if cfg.get("recursive") is not None:
            options["recursiveFileLookup"] = str(bool(cfg["recursive"])).lower()

        reader = backend.spark.read.format(FORMAT)
        for key, value in options.items():
            reader = reader.option(key, str(value))

        return reader.load(path)
