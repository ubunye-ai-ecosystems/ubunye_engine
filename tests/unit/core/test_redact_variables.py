"""A variable whose name says it is a secret is masked wherever it is recorded.

Found by experiment E-04 (finding F-016): a token passed as `--var token=...` was
written to the lineage record, sent in every OpenLineage event and printed by
`ubunye plan`. The run still uses the real value; only what is kept or shown is masked.
"""

from __future__ import annotations

import json

import pytest

from ubunye.core.secrets import REDACTED, looks_secret, redact_variables

SECRET = [
    "token",
    "db_password",
    "apiKey",
    "API_KEY",
    "GITHUB_TOKEN",
    "aws_secret_access_key",
    "client-secret",
    "authToken",
    "sas_token",
    "PWD",
    "x-auth",
    "passphrase",
    "SECRETS",
    "passwords",
    "tokens",
    "privatekey",
    "secretkey",
    "accesskey",
    "bearer",
    "jwt",
    "signing_key",
    "encryption_key",
    "ssh_key",
    "connection_string",
]
PLAIN = [
    "dt",
    "dtf",
    "mode",
    "tokenizer",
    "author",
    "partition_key",
    "keys",
    "region",
    "passthrough",
    "compass",
    "max_tokens",
    "secretariat",
    "keyword",
    # About a secret, not holding one (counts and limits matter with model calls).
    "max_token",
    "token_limit",
    "token_count",
    "auth_mode",
    "auth_type",
    "secret_name",
    "secretScope",
    "signature_version",
    "cookie_domain",
    "sas_expiry",
    "password_min_length",
    "password_file",
    "secret_path",
]


@pytest.mark.parametrize("name", SECRET)
def test_secret_names_are_masked(name):
    assert looks_secret(name)


@pytest.mark.parametrize("name", PLAIN)
def test_ordinary_names_are_kept(name):
    assert not looks_secret(name)


def test_redact_masks_values_keeps_the_rest_and_drops_none():
    got = redact_variables({"dt": "2024-01-02", "token": "abc", "empty_token": "", "n": None})
    assert got == {"dt": "2024-01-02", "token": REDACTED, "empty_token": ""}


def test_the_input_is_not_changed():
    given = {"token": "abc"}
    redact_variables(given)
    assert given == {"token": "abc"}


def test_a_run_uses_the_real_value_and_records_the_mask(tmp_path):
    pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    import ubunye
    from ubunye.lineage.storage import FileSystemLineageStore

    task = tmp_path / "uc" / "pkg" / "t"
    task.mkdir(parents=True)
    (tmp_path / "in.csv").write_text("id\n1\n", encoding="utf-8")
    (task / "transformations.py").write_text(
        "from ubunye.core.interfaces import Task\n\n\n"
        "class T(Task):\n"
        "    def transform(self, sources):\n"
        "        df = sources['src'].copy()\n"
        "        df['seen'] = self.config['CONFIG']['transform']['params']['seen']\n"
        "        return {'out': df}\n",
        encoding="utf-8",
    )
    (task / "config.yaml").write_text(
        "MODEL: etl\n"
        'VERSION: "1.0.0"\n'
        "CONFIG:\n"
        "  inputs:\n"
        "    src:\n"
        "      format: s3\n"
        f'      path: "{(tmp_path / "in.csv").as_posix()}"\n'
        "      file_format: csv\n"
        "      options: {header: 'true'}\n"
        "  transform:\n"
        "    params:\n"
        '      seen: "{{ api_token }}"\n'
        "  outputs:\n"
        "    out:\n"
        "      format: s3\n"
        f'      path: "{(tmp_path / "out").as_posix()}"\n'
        "      file_format: parquet\n"
        "      mode: overwrite\n",
        encoding="utf-8",
    )
    ubunye.run_task(
        str(task), backend="pandas", lineage=True, variables={"api_token": "s3cr3t-value"}
    )
    import pandas as pd

    assert pd.read_parquet(tmp_path / "out")["seen"].tolist() == ["s3cr3t-value"]
    (record,) = FileSystemLineageStore(str(tmp_path / ".ubunye" / "lineage")).list_runs("uc/pkg/t")
    assert record.variables["api_token"] == REDACTED
    lineage_files = list((tmp_path / ".ubunye").rglob("*.json"))
    assert lineage_files
    assert not any("s3cr3t-value" in p.read_text(encoding="utf-8") for p in lineage_files)
    assert "s3cr3t-value" not in json.dumps(record.to_dict())


# --- a secret inside a URL never reaches a location (Spline's original leak) -------


@pytest.mark.parametrize(
    "url, masked",
    [
        ("jdbc:postgresql://bob:PW123@h:5432/db", "jdbc:postgresql://bob:***@h:5432/db"),
        ("https://u:p4ss@h/x?api_key=K9&page=2", "https://u:***@h/x?api_key=***&page=2"),
        (
            "jdbc:sqlserver://h;user=sa;password=PW2;db=x",
            "jdbc:sqlserver://h;user=sa;password=***;db=x",
        ),
        ("https://h/hook?token=TK1&page=2", "https://h/hook?token=***&page=2"),
        ("s3://bucket/path/file.parquet", "s3://bucket/path/file.parquet"),
        ("https://h/v1/items?limit=10", "https://h/v1/items?limit=10"),
    ],
)
def test_redact_url(url, masked):
    from ubunye.core.secrets import redact_url

    assert redact_url(url) == masked


def test_a_step_location_never_holds_a_url_password():
    from ubunye.lineage.context import StepRecord

    jdbc = StepRecord.from_io_cfg(
        "db", "input", {"format": "jdbc", "url": "jdbc:postgresql://bob:PW123@h/db", "table": "t"}
    )
    rest = StepRecord.from_io_cfg(
        "api", "output", {"format": "rest_api", "url": "https://h/hook?token=TK1"}
    )
    assert "PW123" not in jdbc.location and "TK1" not in rest.location


def test_plan_masks_a_secret_variable_templated_into_urls(tmp_path):
    from typer.testing import CliRunner

    from ubunye.cli.main import app

    task = tmp_path / "uc" / "pkg" / "t"
    task.mkdir(parents=True)
    (task / "transformations.py").write_text(
        "from ubunye.core.interfaces import Task\n\n\n"
        "class T(Task):\n"
        "    def transform(self, sources):\n"
        "        return {'out': sources['db']}\n",
        encoding="utf-8",
    )
    (task / "config.yaml").write_text(
        "MODEL: etl\n"
        'VERSION: "1.0.0"\n'
        "CONFIG:\n"
        "  inputs:\n"
        "    db:\n"
        "      format: jdbc\n"
        '      url: "jdbc:postgresql://h/db?user=bob&pw={{ db_password }}"\n'
        "      table: orders\n"
        "  transform: {}\n"
        "  outputs:\n"
        "    out:\n"
        "      format: rest_api\n"
        '      url: "https://h/hook?t={{ api_token }}"\n',
        encoding="utf-8",
    )
    args = ["plan", "-d", str(tmp_path), "-u", "uc", "-p", "pkg", "-t", "t", "--json"]
    args += ["--var", "db_password=PWLEAK1", "--var", "api_token=TKLEAK2"]
    result = CliRunner().invoke(app, args)
    assert "PWLEAK1" not in result.output and "TKLEAK2" not in result.output
    assert "***" in result.output
