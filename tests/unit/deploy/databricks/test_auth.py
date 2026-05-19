"""Tests for Databricks authentication resolution."""

from unittest.mock import MagicMock, patch

import pytest

from ubunye.core.errors import AuthInvalidError, AuthNotFoundError
from ubunye.deploy.databricks.auth import resolve_auth
from ubunye.deploy.databricks.targets import DatabricksTargetConfig


@pytest.fixture
def target():
    return DatabricksTargetConfig(host="https://adb-123.azuredatabricks.net")


class TestResolveAuth:
    def test_missing_token_raises(self, target):
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(AuthNotFoundError, match="DATABRICKS_TOKEN") as exc_info:
                resolve_auth(target)
            assert exc_info.value.hint

    def test_missing_sdk_raises(self, target):
        with patch.dict("os.environ", {"DATABRICKS_TOKEN": "dapi123"}):
            with patch.dict("sys.modules", {"databricks": None, "databricks.sdk": None}):
                with pytest.raises(AuthNotFoundError, match="databricks-sdk"):
                    resolve_auth(target)

    def test_successful_auth(self, target):
        mock_client = MagicMock()
        mock_client.current_user.me.return_value = MagicMock()

        with patch.dict("os.environ", {"DATABRICKS_TOKEN": "dapi123"}):
            with patch("databricks.sdk.WorkspaceClient", return_value=mock_client):
                client = resolve_auth(target)
                assert client is mock_client
                mock_client.current_user.me.assert_called_once()

    def test_invalid_token_raises(self, target):
        mock_client = MagicMock()
        mock_client.current_user.me.side_effect = Exception("401 Unauthorized")

        with patch.dict("os.environ", {"DATABRICKS_TOKEN": "dapi_bad"}):
            with patch("databricks.sdk.WorkspaceClient", return_value=mock_client):
                with pytest.raises(AuthInvalidError, match="rejected"):
                    resolve_auth(target)

    def test_custom_token_env(self):
        target = DatabricksTargetConfig(
            host="https://adb-123.azuredatabricks.net",
            token_env="MY_CUSTOM_TOKEN",
        )
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(AuthNotFoundError, match="MY_CUSTOM_TOKEN"):
                resolve_auth(target)
