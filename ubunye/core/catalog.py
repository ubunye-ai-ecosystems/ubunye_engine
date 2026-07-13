"""Utility to set Unity Catalog and schema on a Spark backend."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from ubunye.core.interfaces import Backend


log = logging.getLogger(__name__)


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
        # `USE CATALOG` is a Unity Catalog statement. Open-source Spark does not have it
        # and rejects the whole thing with PARSE_SYNTAX_ERROR — so a config that set
        # ENGINE.catalog could not run anywhere except Databricks, and the failure named
        # a syntax error rather than the real problem.
        #
        # A catalog you cannot switch to is not fatal: the tables in the config are
        # three-part names anyway, and Spark resolves those without a USE. So say what
        # happened and carry on, rather than taking the pipeline down over a nicety.
        safe = f"`{catalog}`" if not catalog.startswith("`") else catalog
        try:
            spark.sql(f"USE CATALOG {safe}")
        except Exception as exc:  # noqa: BLE001 — AnalysisException on non-UC Spark
            log.warning(
                "ENGINE.catalog is set to %s, but this Spark does not support "
                "'USE CATALOG' (%s). Continuing — use fully-qualified "
                "catalog.schema.table names in the config instead.",
                catalog,
                str(exc).splitlines()[0][:90],
            )

    if schema:
        safe = f"`{schema}`" if not schema.startswith("`") else schema
        spark.sql(f"USE SCHEMA {safe}")
