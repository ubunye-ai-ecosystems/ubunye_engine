"""No secret in a failed run's error: one masking function for every place (F-054 review).

The skeptic's leak table (p1_error_leaks.py): before this fix a failed run's record,
`lineage trace`, OpenLineage's errorMessage and `ubunye prove`'s reason kept all of
these. Each row is a test here, on `mask_text` and through a real run.
"""

from __future__ import annotations

import base64
import os
import urllib.parse

import pytest

from ubunye.core import secrets
from ubunye.core.secrets import SecretResolver, error_text, known_secrets, mask_text

VAR_PW = "VarSecret-123!"


@pytest.mark.parametrize(
    "text, secret",
    [
        # A --var secret, word for word.
        ("bad pw " + VAR_PW, VAR_PW),
        # URL-encoded.
        ("bad " + urllib.parse.quote(VAR_PW), urllib.parse.quote(VAR_PW)),
        ("bad " + urllib.parse.quote_plus("a b " + VAR_PW), VAR_PW[:6]),
        # base64, alone and inside user:password (Basic auth), in both alphabets.
        ("bad " + base64.b64encode(VAR_PW.encode()).decode(), "VmFyU2VjcmV0"),
        ("bad " + base64.b64encode(("u:" + VAR_PW).encode()).decode(), "WYXJTZWNyZXQtMTIz"),
        ("bad " + base64.urlsafe_b64encode(("ab:" + VAR_PW).encode()).decode(), "VmFyU2VjcmV0"),
        # Split across a line.
        ("bad " + VAR_PW[:6] + "\n" + VAR_PW[6:], "ret-123!"),
    ],
)
def test_known_values_are_masked_in_every_form(text, secret):
    out = mask_text(text, [VAR_PW])
    assert secret not in out and "***" in out


@pytest.mark.parametrize(
    "text, secret",
    [
        ("jdbc:sqlserver://h;user=u;password=EnvSecret-456;db=x", "EnvSecret-456"),
        ("could not connect to postgresql://admin:LiteralPw99@db:5432/x", "LiteralPw99"),
        ("404 for https://api.x/v1?api_key=LiteralKey77&q=1", "LiteralKey77"),
        ("GET https://x/y?key=K3y-value&sig=S1gnat&q=1", "K3y-value"),
        ("GET https://x/y?key=K3y-value&sig=S1gnat&q=1", "S1gnat"),
        ("https://x/y?pwd=p4ss&passwd=p5ss&token=t0k", "p4ss"),
        ("https://x/y?pwd=p4ss&passwd=p5ss&token=t0k", "p5ss"),
        ("https://x/y?access_key=AK1234&secret=SK1234", "SK1234"),
        ("rejected headers {'Authorization': 'Bearer LiteralBearer55'}", "LiteralBearer55"),
        ("Proxy-Authorization: Basic dXNlcjpwYXNz", "dXNlcjpwYXNz"),
        ('{"x-api-key": "RawKey999"}', "RawKey999"),
        ("authorization=RawToken42", "RawToken42"),
    ],
)
def test_the_shapes_of_a_secret_are_masked_without_knowing_it(text, secret):
    out = mask_text(text, [])
    assert secret not in out and "***" in out


def test_plain_text_is_left_alone():
    text = "KeyError: 'price' in https://x/y?q=1&page=2 (user=u;db=x)"
    assert mask_text(text, ["unrelated-value"]) == text


def test_a_value_shorter_than_4_characters_is_not_masked_word_for_word():
    assert mask_text("bad pw abc", ["abc"]) == "bad pw abc"


def test_resolved_secret_references_are_known(monkeypatch):
    monkeypatch.setenv("UBUNYE_TEST_REF_VALUE", "RefSecret-789")
    assert SecretResolver().get("secret://env/UBUNYE_TEST_REF_VALUE") == "RefSecret-789"
    assert "RefSecret-789" in known_secrets()


def test_config_literals_and_secret_env_vars_are_known(monkeypatch):
    monkeypatch.setenv("UBUNYE_TEST_DB_PASSWORD", "EnvOnly-4242")
    config = {
        "CONFIG": {
            "inputs": {
                "api": {
                    "format": "rest_api",
                    "auth": {"type": "api_key", "key": "CfgKey-1111"},
                    "headers": {"Authorization": "Bearer CfgBearer-2222"},
                    "params": {"api_token": "CfgToken-3333", "page": "1"},
                }
            }
        }
    }
    found = known_secrets({"db_password": VAR_PW, "user": "u"}, config)
    for value in (VAR_PW, "CfgKey-1111", "CfgBearer-2222", "CfgToken-3333", "EnvOnly-4242"):
        assert value in found
    assert "u" not in found and "1" not in found


def test_error_text_masks_before_anything_sees_it(monkeypatch):
    exc = ValueError("jdbc:x://h;password=" + VAR_PW + " and again " + VAR_PW)
    text = error_text(exc, {"db_password": VAR_PW})
    assert text.startswith("ValueError: ") and VAR_PW not in text


def test_the_rest_connector_uses_the_same_function():
    from ubunye.plugins import rest_http

    text = "GET https://x/y?sig=S1gnat Authorization: Bearer abcdef " + VAR_PW
    assert rest_http.redact(text, [VAR_PW]) == mask_text(text, [VAR_PW])


# --- through a real run: the record file keeps none of them ---------------------

CONFIG = """\
MODEL: etl
VERSION: "1.0.0"
CONFIG:
  inputs:
    rows: {format: s3, path: "{{ task_dir }}/rows.csv", file_format: csv, options: {header: "true"}}
  transform:
    type: noop
    params:
      env_pw: "{{ env.UBUNYE_TEST_DB_PASSWORD }}"
  outputs:
    out: {format: s3, path: "{{ task_dir }}/out", file_format: parquet, mode: overwrite}
"""

CODE = """\
import base64, os, urllib.parse
from ubunye.core.interfaces import Task
from ubunye.core.secrets import SecretResolver


class Boom(Task):
    def transform(self, sources):
        pw = os.environ["UBUNYE_TEST_VARPW"]
        env_pw = self.config["CONFIG"]["transform"]["params"]["env_pw"]
        raise ValueError(" | ".join([
            "bad pw " + pw,
            "jdbc:sqlserver://h;user=u;password=" + env_pw + ";db=x",
            "postgresql://admin:LiteralPw99@db:5432/x",
            "https://api.x/v1?api_key=LiteralKey77&q=1",
            str({"Authorization": "Bearer LiteralBearer55"}),
            SecretResolver().get("secret://env/UBUNYE_TEST_REF_SECRET"),
            urllib.parse.quote(pw),
            base64.b64encode(("u:" + pw).encode()).decode(),
            pw[:6] + chr(10) + pw[6:],
        ]))
"""


def test_a_failed_run_record_keeps_no_secret(tmp_path, monkeypatch):
    pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    import ubunye

    monkeypatch.setenv("UBUNYE_TEST_VARPW", VAR_PW)
    monkeypatch.setenv("UBUNYE_TEST_DB_PASSWORD", "EnvSecret-456")
    monkeypatch.setenv("UBUNYE_TEST_REF_SECRET", "RefSecret-789")
    task = tmp_path / "uc" / "pkg" / "t"
    task.mkdir(parents=True)
    (task / "config.yaml").write_text(CONFIG, encoding="utf-8")
    (task / "transformations.py").write_text(CODE, encoding="utf-8")
    (task / "rows.csv").write_text("a\n1\n", encoding="utf-8")

    with pytest.raises(ValueError):
        ubunye.run_task(
            str(task), backend="pandas", lineage=True, variables={"db_password": VAR_PW}
        )

    (path,) = (tmp_path / ".ubunye" / "lineage").rglob("*.json")
    record = path.read_text(encoding="utf-8")
    assert "ValueError: bad pw ***" in record
    for secret in (
        VAR_PW,
        "EnvSecret-456",
        "LiteralPw99",
        "LiteralKey77",
        "LiteralBearer55",
        "RefSecret-789",
        urllib.parse.quote(VAR_PW),
        "WYXJTZWNyZXQtMTIz",
        "VarSec\\nret",
    ):
        assert secret not in record, secret
    assert os.environ["UBUNYE_TEST_VARPW"] == VAR_PW  # the run itself used the real value


def test_the_seen_values_do_not_include_short_ones():
    secrets._remember("abc")
    assert "abc" not in secrets._SEEN
