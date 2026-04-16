"""Unit tests for the Jinja2 config resolver."""

import pytest

from ubunye.config.resolver import resolve_config


class TestJinjaResolver:
    def test_env_var_resolution(self, monkeypatch):
        monkeypatch.setenv("DB_USER", "admin")
        raw = {"user": "{{ env.DB_USER }}"}
        resolved = resolve_config(raw, cli_vars={})
        assert resolved["user"] == "admin"

    def test_cli_var_resolution(self):
        raw = {"path": "s3a://bucket/{{ ds }}/"}
        resolved = resolve_config(raw, cli_vars={"ds": "2025-01-01"})
        assert resolved["path"] == "s3a://bucket/2025-01-01/"

    def test_default_filter(self):
        raw = {"path": "s3a://bucket/{{ ds | default('1970-01-01') }}/"}
        resolved = resolve_config(raw, cli_vars={})
        assert resolved["path"] == "s3a://bucket/1970-01-01/"

    def test_missing_env_var_raises(self, monkeypatch):
        monkeypatch.delenv("MISSING_VAR", raising=False)
        raw = {"password": "{{ env.MISSING_VAR }}"}
        with pytest.raises(ValueError, match="MISSING_VAR"):
            resolve_config(raw, cli_vars={})

    def test_missing_env_var_with_default_does_not_raise(self, monkeypatch):
        monkeypatch.delenv("OPTIONAL_VAR", raising=False)
        raw = {"val": "{{ env.OPTIONAL_VAR | default('fallback') }}"}
        resolved = resolve_config(raw, cli_vars={})
        assert resolved["val"] == "fallback"

    def test_nested_resolution(self, monkeypatch):
        monkeypatch.setenv("API_TOKEN", "secret123")
        raw = {"inputs": {"api": {"headers": {"Authorization": "Bearer {{ env.API_TOKEN }}"}}}}
        resolved = resolve_config(raw, cli_vars={})
        assert resolved["inputs"]["api"]["headers"]["Authorization"] == "Bearer secret123"

    def test_list_resolution(self, monkeypatch):
        monkeypatch.setenv("TEAM", "fraud")
        raw = {"tags": ["{{ env.TEAM }}", "etl"]}
        resolved = resolve_config(raw, cli_vars={})
        assert resolved["tags"][0] == "fraud"
        assert resolved["tags"][1] == "etl"

    def test_non_string_values_unchanged(self):
        raw = {"retries": 3, "enabled": True, "extra": None}
        resolved = resolve_config(raw, cli_vars={})
        assert resolved["retries"] == 3
        assert resolved["enabled"] is True
        assert resolved["extra"] is None

    def test_multiple_templates_in_one_string(self, monkeypatch):
        monkeypatch.setenv("HOST", "db.example.com")
        raw = {"url": "jdbc:postgresql://{{ env.HOST }}:5432/{{ db_name }}"}
        resolved = resolve_config(raw, cli_vars={"db_name": "fraud"})
        assert resolved["url"] == "jdbc:postgresql://db.example.com:5432/fraud"

    def test_no_template_string_unchanged(self):
        raw = {"plain": "just a string", "num": 42}
        resolved = resolve_config(raw, cli_vars={})
        assert resolved["plain"] == "just a string"
        assert resolved["num"] == 42

    def test_empty_dict(self):
        assert resolve_config({}) == {}

    def test_env_override_for_testing(self):
        """_env parameter allows injecting a custom env dict in tests."""
        raw = {"token": "{{ env.MY_TOKEN }}"}
        resolved = resolve_config(raw, cli_vars={}, _env={"MY_TOKEN": "test-value"})
        assert resolved["token"] == "test-value"

    def test_deeply_nested_list_of_dicts(self, monkeypatch):
        monkeypatch.setenv("REGION", "us-east-1")
        raw = {"steps": [{"region": "{{ env.REGION }}"}, {"region": "eu-west-1"}]}
        resolved = resolve_config(raw, cli_vars={})
        assert resolved["steps"][0]["region"] == "us-east-1"
        assert resolved["steps"][1]["region"] == "eu-west-1"

    # ------------------------------------------------------------------
    # Undefined CLI-var detection — prevents silent pass-through
    # ------------------------------------------------------------------

    def test_missing_cli_var_raises(self):
        """A referenced cli_var that's not provided must fail loudly,
        not render as a literal ``{{ ds }}`` in the resolved output."""
        raw = {"path": "s3a://bucket/{{ ds }}/data"}
        with pytest.raises(ValueError, match="ds"):
            resolve_config(raw, cli_vars={})

    def test_missing_cli_var_with_default_does_not_raise(self):
        """The Jinja ``| default(...)`` filter must still bypass the check."""
        raw = {"path": "s3a://bucket/{{ ds | default('1970-01-01') }}/data"}
        resolved = resolve_config(raw, cli_vars={})
        assert resolved["path"] == "s3a://bucket/1970-01-01/data"

    def test_missing_cli_var_in_nested_structure_raises(self):
        raw = {"inputs": {"src": {"path": "file:///{{ missing }}/data"}}}
        with pytest.raises(ValueError, match="missing"):
            resolve_config(raw, cli_vars={})
