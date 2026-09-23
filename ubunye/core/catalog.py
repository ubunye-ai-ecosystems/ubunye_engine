"""Deprecated shim — ``set_catalog_and_schema`` moved to the Spark adapter.

Under issue #37, ``USE CATALOG`` / ``USE SCHEMA`` (Spark and Unity statements)
moved out of the engine core to ``ubunye.adapters.spark.catalog``. In-repo
callers import it from there. This module keeps the old import path working for
third-party code, with a ``DeprecationWarning``, until a later major removes it.
"""

from __future__ import annotations

import warnings
from typing import Any


def __getattr__(name: str) -> Any:
    if name == "set_catalog_and_schema":
        warnings.warn(
            "ubunye.core.catalog.set_catalog_and_schema has moved to "
            "ubunye.adapters.spark.catalog.set_catalog_and_schema. Import it from "
            "there; the shim in core will be removed in a future major.",
            DeprecationWarning,
            stacklevel=2,
        )
        from ubunye.adapters.spark.catalog import set_catalog_and_schema

        return set_catalog_and_schema
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
