"""Unit tests for ubunye.notebook — interactive notebook API."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from ubunye.config.resolver import extract_env_references
from ubunye.notebook import NotebookContext, _auto_resolve_env

# ---------------------------------------------------------------------------
# extract_env_references
# ---------------------------------------------------------------------------


class TestExtractEnvReferences:
    def test_single_reference(self):
        raw = 'table: "{{ env.CATALOG }}.{{ env.SCHEMA }}.claims"'
        assert extract_env_references(raw) == {"CATALOG", "SCHEMA"}

    def test_with_default_filter(self):
        raw = "{{ env.CATALOG | default('workspace') }}"
        assert extract_env_references(raw) == {"CATALOG"}

    def test_no_references(self):
        raw = "table: raw_db.claims"
        assert extract_env_references(raw) == set()

    def test_duplicate_refs_deduplicated(self):
        raw = "{{ env.FOO }} and {{ env.FOO }}"
        assert extract_env_references(raw) == {"FOO"}

    def test_multiline_yaml(self):
        raw = (
            "inputs:\n"
            '  src:\n    table: "{{ env.CATALOG }}.{{ env.SCHEMA }}.raw"\n'
            "outputs:\n"
            '  sink:\n    table: "{{ env.CATALOG }}.{{ env.SCHEMA }}.out"\n'
        )
        assert extract_env_references(raw) == {"CATALOG", "SCHEMA"}

    def test_spacing_variations(self):
        raw = "{{env.A}} and {{  env.B  }} and {{ env.C}}"
        assert extract_env_references(raw) == {"A", "B", "C"}


# ---------------------------------------------------------------------------
# _auto_resolve_env
# ---------------------------------------------------------------------------


class TestAutoResolveEnv:
    def test_explicit_env_wins(self):
        result = _auto_resolve_env(
            env_refs={"CATALOG"},
            explicit_env={"CATALOG": "my_catalog"},
            secrets_scope=None,
            secrets_map=None,
        )
        assert result == {"CATALOG": "my_catalog"}

    def test_falls_back_to_os_environ(self, monkeypatch):
        monkeypatch.setenv("CATALOG", "env_catalog")
        result = _auto_resolve_env(
            env_refs={"CATALOG"},
            explicit_env={},
            secrets_scope=None,
            secrets_map=None,
        )
        assert result == {"CATALOG": "env_catalog"}

    def test_explicit_overrides_os_environ(self, monkeypatch):
        monkeypatch.setenv("CATALOG", "env_val")
        result = _auto_resolve_env(
            env_refs={"CATALOG"},
            explicit_env={"CATALOG": "explicit_val"},
            secrets_scope=None,
            secrets_map=None,
        )
        assert result == {"CATALOG": "explicit_val"}

    def test_missing_var_not_in_result(self):
        result = _auto_resolve_env(
            env_refs={"MISSING_VAR"},
            explicit_env={},
            secrets_scope=None,
            secrets_map=None,
        )
        assert result == {}

    def test_multiple_refs_partial_resolution(self, monkeypatch):
        monkeypatch.setenv("FOUND", "yes")
        result = _auto_resolve_env(
            env_refs={"FOUND", "NOT_FOUND"},
            explicit_env={},
            secrets_scope=None,
            secrets_map=None,
        )
        assert result == {"FOUND": "yes"}


# ---------------------------------------------------------------------------
# NotebookContext
# ---------------------------------------------------------------------------

_VALID_CONFIG = {
    "MODEL": "etl",
    "VERSION": "0.1.0",
    "CONFIG": {
        "inputs": {
            "source": {"format": "hive", "db_name": "raw_db", "tbl_name": "claims"},
        },
        "transform": {},
        "outputs": {
            "sink": {"format": "s3", "path": "s3a://bucket/out/", "mode": "overwrite"},
        },
    },
}

_TRANSFORMATIONS_PY = """\
from ubunye.core.interfaces import Task

class NoOpTask(Task):
    def transform(self, sources):
        return sources
"""

_ENV_CONFIG = {
    "MODEL": "etl",
    "VERSION": "0.1.0",
    "CONFIG": {
        "inputs": {
            "source": {
                "format": "unity",
                "table": "{{ env.TEST_CATALOG }}.{{ env.TEST_SCHEMA }}.claims",
            },
        },
        "transform": {},
        "outputs": {
            "sink": {"format": "s3", "path": "s3a://bucket/out/", "mode": "overwrite"},
        },
    },
}


@pytest.fixture
def notebook_task_dir(tmp_path: Path) -> Path:
    """Temp task directory with a valid config and transformations.py."""
    task = tmp_path / "uc" / "pkg" / "task1"
    task.mkdir(parents=True)
    (task / "config.yaml").write_text(yaml.dump(_VALID_CONFIG), encoding="utf-8")
    (task / "transformations.py").write_text(_TRANSFORMATIONS_PY, encoding="utf-8")
    return task


@pytest.fixture
def env_task_dir(tmp_path: Path) -> Path:
    """Temp task directory with env-var references in config."""
    task = tmp_path / "uc" / "pkg" / "env_task"
    task.mkdir(parents=True)
    (task / "config.yaml").write_text(yaml.dump(_ENV_CONFIG), encoding="utf-8")
    (task / "transformations.py").write_text(_TRANSFORMATIONS_PY, encoding="utf-8")
    return task


@patch("ubunye.notebook._detect_backend")
class TestNotebookContext:
    def test_construction_loads_config(self, mock_detect, notebook_task_dir):
        mock_detect.return_value = MagicMock()
        ctx = NotebookContext(str(notebook_task_dir), auto_env=False)
        assert ctx.task_name == "task1"
        assert ctx.config is not None
        assert ctx.config.VERSION == "0.1.0"

    def test_config_dict_returns_dict(self, mock_detect, notebook_task_dir):
        mock_detect.return_value = MagicMock()
        ctx = NotebookContext(str(notebook_task_dir), auto_env=False)
        d = ctx.config_dict
        assert isinstance(d, dict)
        assert "CONFIG" in d

    def test_env_vars_property(self, mock_detect, env_task_dir, monkeypatch):
        monkeypatch.setenv("TEST_CATALOG", "cat1")
        monkeypatch.setenv("TEST_SCHEMA", "sch1")
        mock_detect.return_value = MagicMock()
        ctx = NotebookContext(str(env_task_dir))
        assert ctx.env_vars == {"TEST_CATALOG": "cat1", "TEST_SCHEMA": "sch1"}

    def test_auto_env_injects_to_os_environ(self, mock_detect, env_task_dir):
        mock_detect.return_value = MagicMock()
        ctx = NotebookContext(
            str(env_task_dir),
            env={"TEST_CATALOG": "injected_cat", "TEST_SCHEMA": "injected_sch"},
        )
        assert os.environ.get("TEST_CATALOG") == "injected_cat"
        assert os.environ.get("TEST_SCHEMA") == "injected_sch"
        ctx.close()
        assert os.environ.get("TEST_CATALOG") is None
        assert os.environ.get("TEST_SCHEMA") is None

    def test_close_restores_original_env(self, mock_detect, env_task_dir, monkeypatch):
        monkeypatch.setenv("TEST_CATALOG", "original")
        monkeypatch.setenv("TEST_SCHEMA", "original_sch")
        mock_detect.return_value = MagicMock()
        ctx = NotebookContext(
            str(env_task_dir),
            env={"TEST_CATALOG": "overridden", "TEST_SCHEMA": "overridden_sch"},
        )
        assert os.environ["TEST_CATALOG"] == "overridden"
        ctx.close()
        assert os.environ["TEST_CATALOG"] == "original"

    def test_read_returns_dict(self, mock_detect, notebook_task_dir):
        mock_backend = MagicMock()
        mock_detect.return_value = mock_backend
        mock_reader_cls = MagicMock()
        mock_df = MagicMock()
        mock_reader_cls.return_value.read.return_value = mock_df

        with patch("ubunye.core.runtime.Registry.from_entrypoints") as mock_reg:
            reg = MagicMock()
            reg.readers = {"hive": mock_reader_cls}
            reg.writers = {"s3": MagicMock()}
            reg.transforms = {}
            mock_reg.return_value = reg

            ctx = NotebookContext(str(notebook_task_dir), auto_env=False)
            sources = ctx.read()

        assert "source" in sources
        assert sources["source"] is mock_df

    def test_transform_uses_cached_sources(self, mock_detect, notebook_task_dir):
        mock_backend = MagicMock()
        mock_detect.return_value = mock_backend

        mock_reader_cls = MagicMock()
        mock_df = MagicMock()
        mock_reader_cls.return_value.read.return_value = mock_df

        mock_transform_cls = MagicMock()
        mock_transform_cls.return_value.apply.return_value = {"sink": mock_df}

        with patch("ubunye.core.runtime.Registry.from_entrypoints") as mock_reg:
            reg = MagicMock()
            reg.readers = {"hive": mock_reader_cls}
            reg.writers = {"s3": MagicMock()}
            reg.transforms = {"_ubunye_user_task": mock_transform_cls}
            mock_reg.return_value = reg

            ctx = NotebookContext(str(notebook_task_dir), auto_env=False)
            ctx.read()
            outputs = ctx.transform()

        assert "sink" in outputs

    def test_write_without_transform_raises(self, mock_detect, notebook_task_dir):
        mock_detect.return_value = MagicMock()
        ctx = NotebookContext(str(notebook_task_dir), auto_env=False)
        with pytest.raises(ValueError, match="No outputs to write"):
            ctx.write()

    def test_write_with_explicit_outputs(self, mock_detect, notebook_task_dir):
        mock_backend = MagicMock()
        mock_detect.return_value = mock_backend

        mock_writer_cls = MagicMock()

        with patch("ubunye.core.runtime.Registry.from_entrypoints") as mock_reg:
            reg = MagicMock()
            reg.readers = {"hive": MagicMock()}
            reg.writers = {"s3": mock_writer_cls}
            reg.transforms = {}
            mock_reg.return_value = reg

            ctx = NotebookContext(str(notebook_task_dir), auto_env=False)
            ctx.write({"sink": MagicMock()})

        mock_writer_cls.return_value.write.assert_called_once()

    def test_spark_property(self, mock_detect, notebook_task_dir):
        mock_backend = MagicMock()
        mock_backend.spark = "fake_spark"
        mock_detect.return_value = mock_backend
        ctx = NotebookContext(str(notebook_task_dir), auto_env=False)
        assert ctx.spark == "fake_spark"


# ---------------------------------------------------------------------------
# notebook() factory
# ---------------------------------------------------------------------------


@patch("ubunye.notebook._detect_backend")
class TestNotebookFactory:
    def test_returns_context(self, mock_detect, notebook_task_dir):
        mock_detect.return_value = MagicMock()
        from ubunye.notebook import notebook

        ctx = notebook(str(notebook_task_dir), auto_env=False)
        assert isinstance(ctx, NotebookContext)

    def test_accessible_from_package(self, mock_detect, notebook_task_dir):
        mock_detect.return_value = MagicMock()
        import ubunye

        ctx = ubunye.notebook(str(notebook_task_dir), auto_env=False)
        assert isinstance(ctx, NotebookContext)
