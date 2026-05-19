"""Databricks authentication — resolve credentials and return a WorkspaceClient."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from ubunye.core.errors import AuthInvalidError, AuthNotFoundError
from ubunye.deploy.databricks.targets import DatabricksTargetConfig

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


def resolve_auth(target: DatabricksTargetConfig) -> "WorkspaceClient":
    """Create an authenticated ``WorkspaceClient`` from a target config.

    Reads the personal access token from the environment variable named in
    ``target.token_env``.  Raises :class:`AuthNotFoundError` when the env var
    is missing and :class:`AuthInvalidError` when the Databricks API rejects
    the credentials.
    """
    token = os.environ.get(target.token_env)
    if not token:
        raise AuthNotFoundError(
            f"Databricks token not found in environment variable '{target.token_env}'.",
            context={"Host": target.host, "Token env var": target.token_env},
            hint=f"Set the {target.token_env} environment variable, or use a "
            f"different env var name via token_env in targets.yaml.",
        )

    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        raise AuthNotFoundError(
            "databricks-sdk is not installed.",
            context={"Host": target.host},
            hint="Install it with: pip install ubunye-engine[databricks]",
        )

    client = WorkspaceClient(host=target.host, token=token)

    try:
        client.current_user.me()
    except Exception as exc:
        raise AuthInvalidError(
            f"Databricks rejected the credentials for {target.host}.",
            context={"Host": target.host, "Token env var": target.token_env},
            hint="Check that the token is valid and has workspace access.",
        ) from exc

    return client
