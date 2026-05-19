"""Databricks authentication — resolve credentials and return a WorkspaceClient.

``TokenAuthBackend`` implements the :class:`~ubunye.interfaces.AuthBackend`
protocol using personal access tokens read from environment variables.

Auth failures always propagate — they are excluded from the graceful
degradation policy (Decision 2).
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from ubunye.core.errors import AuthInvalidError, AuthNotFoundError
from ubunye.deploy.databricks.targets import DatabricksTargetConfig
from ubunye.interfaces.auth import Credentials

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


class TokenAuthBackend:
    """AuthBackend implementation using Databricks personal access tokens.

    Reads the token from an environment variable (default
    ``DATABRICKS_TOKEN``, configurable via ``token_env`` in
    ``targets.yaml``).
    """

    def resolve(self, workspace_url: str, **kwargs: str) -> Credentials:
        """Resolve token credentials from the environment.

        Parameters
        ----------
        workspace_url:
            The Databricks host URL.
        **kwargs:
            ``token_env`` — name of the env var holding the token
            (default ``"DATABRICKS_TOKEN"``).
        """
        token_env = kwargs.get("token_env", "DATABRICKS_TOKEN")
        token = os.environ.get(token_env)
        if not token:
            raise AuthNotFoundError(
                f"Databricks token not found in environment variable '{token_env}'.",
                context={"Host": workspace_url, "Token env var": token_env},
                hint=f"Set the {token_env} environment variable, or use a "
                f"different env var name via token_env in targets.yaml.",
            )
        return Credentials(
            host=workspace_url,
            auth_type="token",
            token=token,
            metadata={"token_env": token_env},
        )

    def validate(self, credentials: Credentials) -> bool:
        """Validate credentials by calling the Databricks current-user API."""
        try:
            from databricks.sdk import WorkspaceClient
        except ImportError:
            raise AuthNotFoundError(
                "databricks-sdk is not installed.",
                context={"Host": credentials.host},
                hint="Install it with: pip install ubunye-engine[databricks]",
            )

        client = WorkspaceClient(host=credentials.host, token=credentials.token)
        try:
            client.current_user.me()
        except Exception as exc:
            raise AuthInvalidError(
                f"Databricks rejected the credentials for {credentials.host}.",
                context={"Host": credentials.host},
                hint="Check that the token is valid and has workspace access.",
            ) from exc
        return True


class ServicePrincipalAuthBackend:
    """AuthBackend implementation using Databricks OAuth (M2M).

    Reads ``DATABRICKS_CLIENT_ID`` and ``DATABRICKS_CLIENT_SECRET``
    from environment variables.  Service principal auth takes priority
    over token auth when both are available (see ``auto_detect``).
    """

    def resolve(self, workspace_url: str, **kwargs: str) -> Credentials:
        """Resolve service principal credentials from the environment."""
        client_id_env = kwargs.get("client_id_env", "DATABRICKS_CLIENT_ID")
        client_secret_env = kwargs.get("client_secret_env", "DATABRICKS_CLIENT_SECRET")
        client_id = os.environ.get(client_id_env)
        client_secret = os.environ.get(client_secret_env)
        if not client_id or not client_secret:
            missing = []
            if not client_id:
                missing.append(client_id_env)
            if not client_secret:
                missing.append(client_secret_env)
            raise AuthNotFoundError(
                f"Service principal credentials not found: {', '.join(missing)}.",
                context={"Host": workspace_url, "Missing": missing},
                hint=f"Set {client_id_env} and {client_secret_env} environment variables.",
            )
        return Credentials(
            host=workspace_url,
            auth_type="service_principal",
            client_id=client_id,
            client_secret=client_secret,
            metadata={
                "client_id_env": client_id_env,
                "client_secret_env": client_secret_env,
            },
        )

    def validate(self, credentials: Credentials) -> bool:
        """Validate credentials by calling the Databricks current-user API."""
        try:
            from databricks.sdk import WorkspaceClient
        except ImportError:
            raise AuthNotFoundError(
                "databricks-sdk is not installed.",
                context={"Host": credentials.host},
                hint="Install it with: pip install ubunye-engine[databricks]",
            )

        client = WorkspaceClient(
            host=credentials.host,
            client_id=credentials.client_id,
            client_secret=credentials.client_secret,
        )
        try:
            client.current_user.me()
        except Exception as exc:
            raise AuthInvalidError(
                f"Databricks rejected the service principal credentials for {credentials.host}.",
                context={"Host": credentials.host, "Client ID": credentials.client_id},
                hint="Check that the client ID/secret are valid and the service principal has workspace access.",
            ) from exc
        return True


def resolve_auth(target: DatabricksTargetConfig) -> "WorkspaceClient":
    """Create an authenticated ``WorkspaceClient`` from a target config.

    This is the backward-compatible entry point used by
    ``deploy_task()``.  It delegates to :class:`TokenAuthBackend`
    internally.
    """
    backend = TokenAuthBackend()
    creds = backend.resolve(target.host, token_env=target.token_env)
    backend.validate(creds)

    from databricks.sdk import WorkspaceClient

    return WorkspaceClient(host=creds.host, token=creds.token)
