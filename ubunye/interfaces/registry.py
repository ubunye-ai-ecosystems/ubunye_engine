"""RegistryBackend protocol — contract for model registries.

Schema evolution (Decision 3)
-----------------------------
``ModelVersionInfo`` separates strict core fields from a flexible
``metadata: Dict[str, str]`` payload:

* Core fields (``name``, ``version``, ``stage``, ``created_at``,
  ``engine_version``) never change without a major version bump.
* New data goes into ``metadata`` — old clients ignore unknown keys,
  new clients write new keys without touching the schema.
* Reads explicitly project the columns they need; never ``SELECT *``.

Non-blocking writes (Decision 1)
---------------------------------
``register()`` and ``promote()`` queue their metadata updates via a
background worker thread (see ``ubunye._internal.metadata_worker``).
Artifact persistence (model files) remains synchronous — the method
returns only after the model is safely stored.

Graceful degradation (Decision 2)
----------------------------------
When a metadata write fails, the record is appended to a local
fallback manifest at ``~/.ubunye/fallback/{run_id}/registry.jsonl``.
Pipeline execution continues.  ``ubunye sync registry`` replays
the manifest with idempotent deduplication.

Implementations are discovered via the ``ubunye.registry_backends``
entry-point group.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol, Tuple, runtime_checkable


@dataclass
class ModelVersionInfo:
    """Cross-boundary model version record (Decision 3 compliant).

    Core fields are strict; everything else goes into ``metadata``.
    Every record stamps ``engine_version`` so downstream consumers can
    filter by writer version.
    """

    name: str
    version: str
    stage: str
    created_at: str
    engine_version: str
    artifact_path: Optional[str] = None
    metrics: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, str] = field(default_factory=dict)


@runtime_checkable
class RegistryBackend(Protocol):
    """Structural interface for model registries.

    Concrete implementations: ``FilesystemRegistryBackend`` (built-in),
    ``MLflowRegistryBackend`` (Phase 2).
    """

    def register(
        self,
        *,
        use_case: str,
        model_name: str,
        version: Optional[str],
        model: Any,
        metrics: Dict[str, Any],
        metadata: Optional[Dict[str, str]] = None,
        lineage_run_id: Optional[str] = None,
    ) -> ModelVersionInfo:
        """Register a trained model as a new version.

        Artifact writes are synchronous — the method returns only after
        the model is safely persisted.  Index/metadata updates are
        non-blocking per Decision 1.

        Parameters
        ----------
        use_case:
            Logical namespace (e.g. ``"fraud_detection"``).
        model_name:
            Model identifier within the use case.
        version:
            Explicit semver string, or ``None`` for auto-increment.
        model:
            A trained model instance whose ``save()``/``metadata()``
            methods will be called by the backend.
        metrics:
            Training metrics dict.
        metadata:
            Optional flexible key-value pairs (Decision 3).
        lineage_run_id:
            Optional lineage run ID for cross-referencing.
        """
        ...

    def get(
        self,
        *,
        use_case: str,
        model_name: str,
        version: Optional[str] = None,
        stage: Optional[str] = None,
    ) -> Tuple[str, ModelVersionInfo]:
        """Retrieve a model version by exact version or active stage.

        Returns
        -------
        tuple of (artifact_path, ModelVersionInfo)
            ``artifact_path`` is an opaque string — it may be a local
            path, an MLflow artifact URI, or a cloud storage URL.
            Callers pass it to ``ModelClass.load(artifact_path)``.

        Raises
        ------
        ubunye.core.errors.VersionNotFoundError
            When no matching version exists.
        """
        ...

    def list_versions(
        self,
        *,
        use_case: str,
        model_name: str,
    ) -> List[ModelVersionInfo]:
        """List all registered versions, newest first."""
        ...

    def promote(
        self,
        *,
        use_case: str,
        model_name: str,
        version: str,
        to_stage: str,
        gates: Optional[Dict[str, Any]] = None,
        promoted_by: Optional[str] = None,
    ) -> ModelVersionInfo:
        """Promote a version to a higher lifecycle stage.

        If ``gates`` are provided, all checks must pass before the
        transition.  On gate failure, raises
        :class:`~ubunye.core.errors.PromotionBlockedError` — this is
        NOT caught by the graceful degradation layer (auth and gate
        failures must propagate).

        Metadata update is non-blocking per Decision 1.
        """
        ...

    def demote(
        self,
        *,
        use_case: str,
        model_name: str,
        version: str,
        to_stage: str,
    ) -> ModelVersionInfo:
        """Demote a version to a lower stage."""
        ...

    def delete(
        self,
        *,
        use_case: str,
        model_name: str,
        version: str,
    ) -> None:
        """Remove a version and its artifacts."""
        ...
