"""AuthBackend protocol — contract for authentication backends.

Auth failures are EXCLUDED from graceful degradation (Decision 2).
A failed ``resolve()`` must raise immediately — pipelines must not
execute with invalid or missing credentials.

Implementations are discovered via the ``ubunye.auth_backends``
entry-point group.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional, Protocol, runtime_checkable


@dataclass
class Credentials:
    """Resolved authentication credentials.

    The ``auth_type`` field tells callers which fields are populated.
    ``metadata`` carries backend-specific extras (e.g. token expiry,
    scope, tenant ID) without polluting the core fields.
    """

    host: str
    auth_type: str
    token: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    metadata: Dict[str, str] = field(default_factory=dict)


@runtime_checkable
class AuthBackend(Protocol):
    """Structural interface for authentication backends.

    Concrete implementations: ``TokenAuthBackend`` (built-in),
    ``ServicePrincipalAuthBackend`` (Phase 2).
    """

    def resolve(self, workspace_url: str, **kwargs: str) -> Credentials:
        """Resolve credentials for the given workspace.

        Parameters
        ----------
        workspace_url:
            The Databricks (or other) workspace host URL.
        **kwargs:
            Backend-specific hints (e.g. ``token_env="MY_TOKEN"``).

        Returns
        -------
        Credentials
            Populated credentials ready for client construction.

        Raises
        ------
        ubunye.core.errors.AuthNotFoundError
            When credentials cannot be located (missing env var, etc.).
        """
        ...

    def validate(self, credentials: Credentials) -> bool:
        """Validate that credentials are accepted by the workspace.

        Returns ``True`` on success.

        Raises
        ------
        ubunye.core.errors.AuthInvalidError
            When the workspace rejects the credentials.
        """
        ...
