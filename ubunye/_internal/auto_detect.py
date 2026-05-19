"""Auto-detect the right backend for the current environment.

Each function probes environment signals (env vars, runtime context)
and returns the entry-point name of the appropriate backend.

Detection follows the same pattern as
``ubunye.api._detect_backend`` for SparkSession.
"""

from __future__ import annotations

import os


def detect_registry_backend() -> str:
    """Choose a registry backend based on environment.

    - Databricks runtime with MLflow available → ``"mlflow"``
    - Otherwise → ``"filesystem"``
    """
    if _on_databricks() and _has_package("mlflow"):
        return "mlflow"
    return "filesystem"


def detect_lineage_backend() -> str:
    """Choose a lineage backend based on environment.

    - ``UBUNYE_LINEAGE_TABLE`` env var set → ``"delta"``
    - Otherwise → ``"filesystem"``
    """
    if os.environ.get("UBUNYE_LINEAGE_TABLE"):
        return "delta"
    return "filesystem"


def detect_auth_backend() -> str:
    """Choose an auth backend based on environment.

    - ``DATABRICKS_CLIENT_ID`` + ``DATABRICKS_CLIENT_SECRET`` → ``"service_principal"``
    - ``DATABRICKS_TOKEN`` → ``"token"``
    - Otherwise → raises ``AuthNotFoundError``
    """
    if os.environ.get("DATABRICKS_CLIENT_ID") and os.environ.get("DATABRICKS_CLIENT_SECRET"):
        return "service_principal"
    if os.environ.get("DATABRICKS_TOKEN"):
        return "token"
    from ubunye.core.errors import AuthNotFoundError

    raise AuthNotFoundError(
        "No authentication credentials found in environment.",
        context={
            "Checked": [
                "DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET",
                "DATABRICKS_TOKEN",
            ],
        },
        hint="Set DATABRICKS_TOKEN for token auth or "
        "DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET for service principal auth.",
    )


def _on_databricks() -> bool:
    """Heuristic: are we running on Databricks?"""
    return bool(os.environ.get("DATABRICKS_RUNTIME_VERSION"))


def _has_package(name: str) -> bool:
    """Check if a package is importable without importing it."""
    import importlib.util

    return importlib.util.find_spec(name) is not None
