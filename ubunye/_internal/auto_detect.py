"""Auto-detect the right backend for the current environment.

Each function probes environment signals (env vars, runtime context)
and returns the entry-point name of the appropriate backend.

Detection follows the same pattern as
``ubunye.api._detect_backend`` for SparkSession.
"""

from __future__ import annotations

import logging
import os

log = logging.getLogger(__name__)


def detect_registry_backend() -> str:
    """Deprecated: use :func:`ubunye.models.backends.choose`.

    Since 0.9 the registry backend is chosen in one order everywhere (ADR 010):
    the name asked for, else ``UBUNYE_REGISTRY_BACKEND``, else ``filesystem``.
    This function used to pick ``mlflow`` on its own when it saw Databricks; it no
    longer sniffs the host, so the same pipeline means the same registry anywhere.
    Nothing in Ubunye called it; it stays one release for anyone who did.
    """
    import warnings

    from ubunye.models.backends import choose

    warnings.warn(
        "detect_registry_backend() is deprecated; use ubunye.models.backends.choose(). "
        "It no longer picks 'mlflow' on Databricks: set UBUNYE_REGISTRY_BACKEND=mlflow.",
        DeprecationWarning,
        stacklevel=2,
    )
    return choose()


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
