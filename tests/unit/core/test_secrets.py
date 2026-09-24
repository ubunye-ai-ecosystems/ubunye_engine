"""``secret://<provider>/<ref>``: named in config, fetched only for the connector.

The reference must survive everywhere the config is shown or remembered (the
config hash, plan, run records) and become the value only in the copy a
connector gets. The cloud providers are tested here against stand-ins for their
SDKs; the live calls run in ubunye-infra against the sandboxes.
"""

from __future__ import annotations

import base64
import json
import sys
import types
from pathlib import Path

import pytest
from pydantic import ValidationError

from ubunye.config.schema import UbunyeConfig
from ubunye.core import secrets
from ubunye.core.errors import SecretError
from ubunye.plugins.secrets import (
    AwsSecretsManager,
    AzureKeyVault,
    EnvSecrets,
    FileSecrets,
    GcpSecretManager,
)

# --- references -----------------------------------------------------------------


def test_a_reference_splits_into_provider_and_ref():
    assert secrets.parse("secret://aws-sm/prod/db#password") == ("aws-sm", "prod/db#password")
    assert secrets.is_reference("secret://env/X")
    assert not secrets.is_reference("s3://bucket/x")


@pytest.mark.parametrize("bad", ["secret://", "secret://env", "secret:///X"])
def test_a_malformed_reference_is_refused(bad):
    with pytest.raises(SecretError, match="not a secret reference"):
        secrets.parse(bad)


def test_references_are_found_anywhere_in_a_config_tree():
    tree = {"url": "x", "auth": {"token": "secret://env/T"}, "headers": ["a", "secret://file/f"]}
    assert secrets.references(tree, "inputs.api") == [
        ("inputs.api.auth.token", "secret://env/T"),
        ("inputs.api.headers[1]", "secret://file/f"),
    ]


def test_every_built_in_provider_is_registered():
    assert set(secrets.providers()) >= {"env", "file", "databricks", "aws-sm", "gcp-sm", "azure-kv"}


# --- the simple providers --------------------------------------------------------


def test_env(monkeypatch):
    monkeypatch.setenv("UBUNYE_TEST_SECRET", "s3cr3t")
    assert EnvSecrets().get("UBUNYE_TEST_SECRET") == "s3cr3t"
    monkeypatch.delenv("UBUNYE_TEST_SECRET")
    with pytest.raises(SecretError, match="UBUNYE_TEST_SECRET is not set"):
        EnvSecrets().get("UBUNYE_TEST_SECRET")


def test_file_drops_only_the_final_newline(tmp_path):
    path = tmp_path / "db"
    path.write_text("pa ss\n", encoding="utf-8")
    assert FileSecrets().get(str(path)) == "pa ss"
    with pytest.raises(SecretError, match="No secret file"):
        FileSecrets().get(str(tmp_path / "nope"))


# --- the cloud providers, against stand-ins for their SDKs --------------------------


def _module(monkeypatch, name, **attrs):
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    monkeypatch.setitem(sys.modules, name, mod)
    parent, _, child = name.rpartition(".")
    if parent:
        if parent not in sys.modules:
            _module(monkeypatch, parent)
        monkeypatch.setattr(sys.modules[parent], child, mod, raising=False)
    return mod


def test_aws_reads_a_string_or_one_json_field(monkeypatch):
    asked = []

    class Client:
        def get_secret_value(self, SecretId):
            asked.append(SecretId)
            return {"SecretString": json.dumps({"user": "u", "password": "p"})}

    _module(monkeypatch, "boto3", client=lambda service: Client())
    assert AwsSecretsManager().get("prod/db#password") == "p"
    assert json.loads(AwsSecretsManager().get("prod/db")) == {"user": "u", "password": "p"}
    assert asked == ["prod/db", "prod/db"]
    with pytest.raises(SecretError, match="has no field 'pwd'"):
        AwsSecretsManager().get("prod/db#pwd")


def test_aws_reads_a_binary_secret(monkeypatch):
    class Client:
        def get_secret_value(self, SecretId):
            return {"SecretBinary": base64.b64encode(b"raw")}

    _module(monkeypatch, "boto3", client=lambda service: Client())
    assert AwsSecretsManager().get("blob") == "raw"


def test_gcp_names_the_version_and_defaults_to_latest(monkeypatch):
    asked = []

    class Client:
        def access_secret_version(self, name):
            asked.append(name)
            payload = types.SimpleNamespace(data=b'{"key": "v"}')
            return types.SimpleNamespace(payload=payload)

    _module(
        monkeypatch,
        "google.cloud.secretmanager",
        SecretManagerServiceClient=lambda: Client(),
    )
    assert GcpSecretManager().get("proj/api#key") == "v"
    GcpSecretManager().get("proj/api/3")
    assert asked == [
        "projects/proj/secrets/api/versions/latest",
        "projects/proj/secrets/api/versions/3",
    ]
    with pytest.raises(SecretError, match="should be <project>/<secret>"):
        GcpSecretManager().get("just-a-name")


def test_azure_uses_the_vault_url_and_the_default_credential(monkeypatch):
    made = {}

    class SecretClient:
        def __init__(self, vault_url, credential):
            made["url"] = vault_url
            made["credential"] = credential

        def get_secret(self, name, version):
            made["asked"] = (name, version)
            return types.SimpleNamespace(value="az")

    _module(monkeypatch, "azure.identity", DefaultAzureCredential=lambda: "default-credential")
    _module(monkeypatch, "azure.keyvault.secrets", SecretClient=SecretClient)
    assert AzureKeyVault().get("my-vault/db-password") == "az"
    assert made == {
        "url": "https://my-vault.vault.azure.net",
        "credential": "default-credential",
        "asked": ("db-password", None),
    }


def test_a_missing_sdk_is_named_with_the_extra_to_install():
    names = {n: secrets.missing_packages(n) for n in ("aws-sm", "gcp-sm", "azure-kv", "env")}
    assert names["env"] == []
    for name, package in (("aws-sm", "boto3"), ("gcp-sm", "google-cloud-secret-manager")):
        try:
            __import__({"aws-sm": "boto3", "gcp-sm": "google.cloud.secretmanager"}[name])
        except ImportError:
            assert package in names[name]


# --- the resolver ------------------------------------------------------------------


def test_the_resolver_fetches_each_secret_once_and_leaves_the_input_alone(monkeypatch):
    calls = []

    class Counting:
        def get(self, ref):
            calls.append(ref)
            return f"value-of-{ref}"

    monkeypatch.setattr(secrets, "_load", lambda name: Counting)
    resolver = secrets.SecretResolver()
    cfg = {"user": "u", "password": "secret://x/pw", "headers": {"t": "secret://x/pw"}}
    out = resolver.resolve(cfg)
    assert out == {"user": "u", "password": "value-of-pw", "headers": {"t": "value-of-pw"}}
    assert cfg["password"] == "secret://x/pw"
    assert calls == ["pw"]


def test_a_provider_failure_names_the_reference_not_the_value(monkeypatch):
    class Broken:
        HINT = "log in first"

        def get(self, ref):
            raise RuntimeError("AccessDenied")

    monkeypatch.setattr(secrets, "_load", lambda name: Broken)
    with pytest.raises(SecretError) as err:
        secrets.resolve({"p": "secret://x/prod/db"})
    assert "secret://x/prod/db" in str(err.value) and "AccessDenied" in str(err.value)
    assert "log in first" in str(err.value)


# --- validation: checked at load, nothing fetched ------------------------------------


def _cfg(password):
    return {
        "CONFIG": {
            "inputs": {"src": {"format": "s3", "path": "in.csv", "file_format": "csv"}},
            "outputs": {
                "db": {
                    "format": "jdbc",
                    "url": "jdbc:postgresql://h/d",
                    "table": "t",
                    "password": password,
                }
            },
        }
    }


def test_an_unknown_provider_fails_validation_with_a_suggestion():
    with pytest.raises(ValidationError, match="(?i)did you mean 'aws-sm'"):
        UbunyeConfig(**_cfg("secret://aws-smm/prod/db"))


def test_a_known_provider_validates_without_fetching(monkeypatch):
    monkeypatch.delenv("NOT_SET_ANYWHERE", raising=False)
    UbunyeConfig(**_cfg("secret://env/NOT_SET_ANYWHERE"))  # read only at run time


# --- end to end: the value reaches the connector and nothing else ---------------------

pd = pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

TRANSFORM = """\
from ubunye.core.interfaces import Task


class Copy(Task):
    def transform(self, sources):
        return {"out": sources["src"]}
"""


def _task(root: Path) -> Path:
    task = root / "uc" / "pkg" / "t"
    task.mkdir(parents=True)
    (task / "transformations.py").write_text(TRANSFORM, encoding="utf-8")
    (task / "config.yaml").write_text(
        f"""\
CONFIG:
  inputs:
    src:
      format: s3
      path: secret://env/UBUNYE_TEST_INPUT
      file_format: csv
      options:
        header: "true"
        inferSchema: "true"
  outputs:
    out:
      format: s3
      path: "{(root / 'out').as_posix()}"
      file_format: parquet
      mode: overwrite
""",
        encoding="utf-8",
    )
    return task


def test_the_connector_gets_the_value_and_the_run_record_keeps_the_reference(tmp_path, monkeypatch):
    import ubunye

    hidden = tmp_path / "hidden-location"
    hidden.mkdir()
    for name in ("a", "b"):
        (hidden / f"{name}.csv").write_text("id\n1\n2\n", encoding="utf-8")

    task = _task(tmp_path)
    lineage = tmp_path / "lineage"
    hashes = []
    for name in ("a", "b"):  # the secret changes between runs, as when it is rotated
        monkeypatch.setenv("UBUNYE_TEST_INPUT", str(hidden / f"{name}.csv"))
        ubunye.run_task(str(task), backend="pandas", lineage=True, lineage_dir=str(lineage))
        assert sorted(pd.read_parquet(tmp_path / "out")["id"]) == [1, 2]

    records = [p.read_text(encoding="utf-8") for p in lineage.rglob("*.json")]
    assert records, "the runs left no record"
    for text in records:
        assert "hidden-location" not in text
        doc = json.loads(text)
        if "config_hash" in doc:
            hashes.append(doc["config_hash"])
    assert "secret://env/UBUNYE_TEST_INPUT" in "".join(records)
    assert len(set(hashes)) == 1, "rotating a secret must not change the config hash"
