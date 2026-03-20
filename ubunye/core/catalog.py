"""Utility to set Unity Catalog and schema on a Spark backend."""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from ubunye.core.interfaces import Backend


def set_catalog_and_schema(
    backend: "Backend",
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
) -> None:
    """Run ``USE CATALOG`` and/or ``USE SCHEMA`` on the backend's SparkSession.

    No-op if both *catalog* and *schema* are ``None``, or if the backend
    does not expose a ``spark`` attribute (e.g. a future non-Spark backend).

    Catalog names containing hyphens are automatically backtick-quoted.
    """
    if not catalog and not schema:
        return

    spark = getattr(backend, "spark", None)
    if spark is None:
        return

    if catalog:
        # Backtick-quote if the name contains characters that need escaping
        safe = f"`{catalog}`" if not catalog.startswith("`") else catalog
        spark.sql(f"USE CATALOG {safe}")

    if schema:
        safe = f"`{schema}`" if not schema.startswith("`") else schema
        spark.sql(f"USE SCHEMA {safe}")
