"""Unit tests for ubunye.api — pure-function helpers that don't need Spark."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from ubunye.api import _build_extra_hooks, _make_app_name


class TestMakeAppName:
    def test_all_parts(self):
        assert (
            _make_app_name("fraud", "ingestion", "claim_etl")
            == "ubunye:fraud.ingestion.claim_etl"
        )

    def test_usecase_only(self):
        assert _make_app_name("fraud") == "ubunye:fraud"

    def test_usecase_and_package(self):
        assert _make_app_name("fraud", "ingestion") == "ubunye:fraud.ingestion"

    def test_no_parts(self):
        assert _make_app_name() == "ubunye"

    def test_none_parts_skipped(self):
        assert _make_app_name(None, "ingestion", None) == "ubunye:ingestion"

    def test_empty_string_parts_skipped(self):
        assert _make_app_name("", "ingestion", "") == "ubunye:ingestion"


class TestBuildExtraHooks:
    def test_none_recorder_returns_empty(self):
        assert _build_extra_hooks(None) == []

    @patch("ubunye.api.MonitorHook")
    def test_with_recorder_returns_monitor_hook(self, mock_hook_cls):
        recorder = MagicMock()
        hooks = _build_extra_hooks(recorder)
        assert len(hooks) == 1
        mock_hook_cls.assert_called_once_with(recorder)
