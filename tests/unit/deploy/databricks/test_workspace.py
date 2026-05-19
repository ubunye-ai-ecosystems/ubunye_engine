"""Tests for workspace file upload."""

from unittest.mock import MagicMock

import pytest

from ubunye.core.errors import WorkspaceUploadError
from ubunye.deploy.databricks.workspace import _collect_files, upload_task_files


class TestCollectFiles:
    def test_collects_yaml_and_py(self, tmp_path):
        (tmp_path / "config.yaml").write_text("MODEL: etl", encoding="utf-8")
        (tmp_path / "transformations.py").write_text("x=1", encoding="utf-8")
        files = _collect_files(tmp_path)
        names = [name for _, name in files]
        assert "config.yaml" in names
        assert "transformations.py" in names

    def test_skips_pycache(self, tmp_path):
        (tmp_path / "config.yaml").write_text("x", encoding="utf-8")
        cache = tmp_path / "__pycache__"
        cache.mkdir()
        (cache / "mod.cpython-311.pyc").write_text("x", encoding="utf-8")
        files = _collect_files(tmp_path)
        names = [name for _, name in files]
        assert not any("__pycache__" in n for n in names)

    def test_skips_notebooks_dir(self, tmp_path):
        (tmp_path / "config.yaml").write_text("x", encoding="utf-8")
        nb = tmp_path / "notebooks"
        nb.mkdir()
        (nb / "dev.ipynb").write_text("{}", encoding="utf-8")
        files = _collect_files(tmp_path)
        names = [name for _, name in files]
        assert not any("notebooks" in n for n in names)

    def test_collects_helper_files(self, tmp_path):
        (tmp_path / "config.yaml").write_text("x", encoding="utf-8")
        (tmp_path / "transformations.py").write_text("x", encoding="utf-8")
        (tmp_path / "helpers.py").write_text("x", encoding="utf-8")
        files = _collect_files(tmp_path)
        names = [name for _, name in files]
        assert "helpers.py" in names


class TestUploadTaskFiles:
    def test_uploads_all_files(self, tmp_path):
        (tmp_path / "config.yaml").write_text("MODEL: etl", encoding="utf-8")
        (tmp_path / "transformations.py").write_text("x=1", encoding="utf-8")

        client = MagicMock()
        result = upload_task_files(
            client=client,
            task_dir=tmp_path,
            workspace_root="/Workspace/ubunye",
            usecase="fraud",
            pipeline="ingestion",
            task="claim_etl",
        )
        assert result == "/Workspace/ubunye/fraud/ingestion/claim_etl"
        assert client.workspace.upload.call_count >= 2

    def test_uploads_notebook_when_provided(self, tmp_path):
        (tmp_path / "config.yaml").write_text("x", encoding="utf-8")

        client = MagicMock()
        # Make import_ available but raise ImportError to test fallback
        client.workspace.import_ = MagicMock(side_effect=ImportError)

        upload_task_files(
            client=client,
            task_dir=tmp_path,
            workspace_root="/Workspace/ubunye",
            usecase="fraud",
            pipeline="ingestion",
            task="claim_etl",
            notebook_source="# notebook content",
            notebook_name="run_claim_etl",
        )
        # Should fall back to upload with .py extension
        upload_calls = client.workspace.upload.call_args_list
        nb_calls = [c for c in upload_calls if "run_claim_etl" in str(c)]
        assert len(nb_calls) >= 1

    def test_upload_failure_raises(self, tmp_path):
        (tmp_path / "config.yaml").write_text("x", encoding="utf-8")

        client = MagicMock()
        client.workspace.upload.side_effect = Exception("403 Forbidden")

        with pytest.raises(WorkspaceUploadError, match="Failed to upload"):
            upload_task_files(
                client=client,
                task_dir=tmp_path,
                workspace_root="/Workspace/ubunye",
                usecase="fraud",
                pipeline="ingestion",
                task="claim_etl",
            )
