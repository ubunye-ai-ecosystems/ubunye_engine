"""The built-in secret providers, for ``secret://<provider>/<reference>``.

==============  =====================================================  ====================
provider        reference                                              needs
==============  =====================================================  ====================
``env``         ``NAME``                                               nothing
``file``        a path (``secret://file//run/secrets/db``)             nothing
``databricks``  ``scope/key``                                          a Databricks runtime,
                                                                       or databricks-sdk
``aws-sm``      ``name`` or ``name#field`` (JSON secret)               ``[aws]`` extra
``gcp-sm``      ``project/secret[/version]``, optional ``#field``      ``[gcp]`` extra
``azure-kv``    ``vault/secret[/version]``, optional ``#field``        ``[azure]`` extra
==============  =====================================================  ====================

Cloud providers log in the way the cloud's own SDK does (environment, profile,
workload identity, managed identity): the engine never takes a credential.
"""

from __future__ import annotations

import base64
import json
import os
from pathlib import Path
from typing import Any, Tuple

from ubunye.core.errors import SecretError


def _field(ref: str) -> Tuple[str, str]:
    """``name#field`` -> (``name``, ``field``); no ``#`` -> (``name``, ``""``)."""
    name, _, field = ref.partition("#")
    return name, field


def _pick(raw: str, field: str, where: str) -> str:
    """The whole secret, or one field of a JSON secret."""
    if not field:
        return raw
    try:
        doc = json.loads(raw)
    except ValueError as exc:
        raise SecretError(f"{where} is not JSON, so it has no field '{field}'.") from exc
    if not isinstance(doc, dict) or field not in doc:
        keys = ", ".join(sorted(doc)) if isinstance(doc, dict) else "none"
        raise SecretError(f"{where} has no field '{field}' (fields: {keys}).")
    return str(doc[field])


class EnvSecrets:
    """``secret://env/NAME``: an environment variable."""

    REQUIRES_MODULES: Tuple[Tuple[str, str], ...] = ()

    def get(self, ref: str) -> str:
        if ref not in os.environ:
            raise SecretError(
                f"Environment variable {ref} is not set.", hint=f"Set {ref} before the run."
            )
        return os.environ[ref]


class FileSecrets:
    """``secret://file/<path>``: a file's contents, without a final newline.

    For mounted secrets (Kubernetes, Docker ``/run/secrets``).
    """

    REQUIRES_MODULES: Tuple[Tuple[str, str], ...] = ()

    def get(self, ref: str) -> str:
        path = Path(ref)
        if not path.is_file():
            raise SecretError(f"No secret file at {ref}.")
        return path.read_text(encoding="utf-8").rstrip("\r\n")


class DatabricksSecrets:
    """``secret://databricks/<scope>/<key>``: a Databricks secret scope.

    On a Databricks cluster it uses ``dbutils``; elsewhere, the Databricks SDK with
    its usual login (``DATABRICKS_HOST`` and a token or a service principal).
    """

    REQUIRES_MODULES: Tuple[Tuple[str, str], ...] = ()
    HINT = "Check the scope and key with: databricks secrets list-secrets <scope>"

    def get(self, ref: str) -> str:
        scope, _, key = ref.partition("/")
        if not scope or not key:
            raise SecretError(f"'{ref}' should be <scope>/<key>.")
        dbutils = _dbutils()
        if dbutils is not None:
            return dbutils.secrets.get(scope=scope, key=key)
        try:
            from databricks.sdk import WorkspaceClient
        except ImportError as exc:
            raise SecretError(
                "Outside Databricks, the databricks provider needs databricks-sdk.",
                hint="pip install 'ubunye-engine[databricks]'",
            ) from exc
        value = WorkspaceClient().secrets.get_secret(scope=scope, key=key).value
        return base64.b64decode(value).decode("utf-8")


def _dbutils() -> Any:
    try:
        from pyspark.dbutils import DBUtils  # only exists on a Databricks runtime
        from pyspark.sql import SparkSession
    except ImportError:
        return None
    spark = SparkSession.getActiveSession()
    return DBUtils(spark) if spark is not None else None


class AwsSecretsManager:
    """``secret://aws-sm/<name>[#field]``: AWS Secrets Manager (region from the SDK)."""

    REQUIRES_MODULES = (("boto3", "boto3"),)
    HINT = "pip install 'ubunye-engine[aws]'; the login is the AWS SDK's (profile, role, env)."

    def get(self, ref: str) -> str:
        import boto3

        name, field = _field(ref)
        answer = boto3.client("secretsmanager").get_secret_value(SecretId=name)
        raw = answer.get("SecretString")
        if raw is None:
            raw = base64.b64decode(answer["SecretBinary"]).decode("utf-8")
        return _pick(raw, field, f"AWS secret {name}")


class GcpSecretManager:
    """``secret://gcp-sm/<project>/<secret>[/<version>][#field]``: Google Secret Manager."""

    REQUIRES_MODULES = (("google.cloud.secretmanager", "google-cloud-secret-manager"),)
    HINT = "pip install 'ubunye-engine[gcp]'; the login is Google's (ADC, workload identity)."

    def get(self, ref: str) -> str:
        from google.cloud import secretmanager

        path, field = _field(ref)
        parts = path.split("/")
        if len(parts) not in (2, 3):
            raise SecretError(f"'{path}' should be <project>/<secret>[/<version>].")
        project, secret = parts[0], parts[1]
        version = parts[2] if len(parts) == 3 else "latest"
        name = f"projects/{project}/secrets/{secret}/versions/{version}"
        client = secretmanager.SecretManagerServiceClient()
        raw = client.access_secret_version(name=name).payload.data.decode("utf-8")
        return _pick(raw, field, f"GCP secret {project}/{secret}")


class AzureKeyVault:
    """``secret://azure-kv/<vault>/<secret>[/<version>][#field]``: Azure Key Vault."""

    REQUIRES_MODULES = (
        ("azure.identity", "azure-identity"),
        ("azure.keyvault.secrets", "azure-keyvault-secrets"),
    )
    HINT = "pip install 'ubunye-engine[azure]'; the login is Azure's (DefaultAzureCredential)."

    def get(self, ref: str) -> str:
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient

        path, field = _field(ref)
        parts = path.split("/")
        if len(parts) not in (2, 3):
            raise SecretError(f"'{path}' should be <vault>/<secret>[/<version>].")
        vault, secret = parts[0], parts[1]
        version = parts[2] if len(parts) == 3 else None
        client = SecretClient(
            vault_url=f"https://{vault}.vault.azure.net", credential=DefaultAzureCredential()
        )
        raw = client.get_secret(secret, version).value or ""
        return _pick(raw, field, f"Azure secret {vault}/{secret}")
