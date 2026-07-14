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
    """Choose a registry backend. An explicit choice always wins.

    - ``UBUNYE_REGISTRY_BACKEND`` set → that, whatever it says
    - Databricks runtime with MLflow available → ``"mlflow"``
    - Otherwise → ``"filesystem"``

    The env var is new, and it matters. This function used to sniff the host
    (``DATABRICKS_RUNTIME_VERSION``) and pick a backend on your behalf **with no way to
    say otherwise**. The same pipeline, unchanged, registered models to MLflow on
    Databricks and to a directory anywhere else — and nothing told you which. Two runs of
    the same code would disagree about where the model went, and you would find out when
    something tried to load it.

    Sniffing the host is still the *fallback*, because on Databricks MLflow really is
    already there and really is the natural registry. But it is now a default you can
    override, and it says out loud what it picked, rather than a decision made behind
    your back.
    """
    explicit = os.environ.get("UBUNYE_REGISTRY_BACKEND")
    if explicit:
        return explicit.strip().lower()

    if _on_databricks() and _has_package("mlflow"):
        log.info(
            "registry backend: 'mlflow' (auto-detected — DATABRICKS_RUNTIME_VERSION is "
            "set and mlflow is installed). Set UBUNYE_REGISTRY_BACKEND to override."
        )
        return "mlflow"

    log.info("registry backend: 'filesystem' (default). Set UBUNYE_REGISTRY_BACKEND to override.")
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
