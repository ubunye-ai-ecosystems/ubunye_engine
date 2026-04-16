"""Unit tests for ubunye.core.runtime — Engine validation helpers."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ubunye.core.runtime import Engine, EngineContext, Registry


@pytest.fixture
def engine():
    """Engine with a mock backend and empty registry."""
    reg = Registry()
    reg.register_reader("hive", MagicMock)
    reg.register_writer("hive", MagicMock)
    reg.register_transform("noop", MagicMock)
    return Engine(
        backend=MagicMock(),
        registry=reg,
        context=EngineContext(run_id="test-run"),
        manage_backend=False,
    )


class TestEngineValidation:
    def test_missing_input_format_raises(self, engine):
        with pytest.raises(ValueError, match="Inputs missing 'format'"):
            engine._validate_io_configs(
                {"src": {"path": "/tmp"}},
                {"out": {"format": "hive"}},
            )

    def test_missing_output_format_raises(self, engine):
        with pytest.raises(ValueError, match="Outputs missing 'format'"):
            engine._validate_io_configs(
                {"src": {"format": "hive"}},
                {"out": {"path": "/tmp"}},
            )

    def test_valid_io_does_not_raise(self, engine):
        engine._validate_io_configs(
            {"src": {"format": "hive"}},
            {"out": {"format": "hive"}},
        )

    def test_normalize_single_transform(self, engine):
        result = engine._normalize_transforms({"type": "noop"})
        assert result == [{"type": "noop"}]

    def test_normalize_list_of_transforms(self, engine):
        result = engine._normalize_transforms([{"type": "noop"}, {"type": "noop"}])
        assert len(result) == 2

    def test_normalize_invalid_type_raises(self, engine):
        with pytest.raises(TypeError, match="must be a dict or a list"):
            engine._normalize_transforms("noop")

    def test_missing_transform_plugin_raises(self, engine):
        with pytest.raises(KeyError, match="not found"):
            engine._validate_transforms_exist([{"type": "nonexistent"}])

    def test_registered_transform_does_not_raise(self, engine):
        engine._validate_transforms_exist([{"type": "noop"}])


class TestEngineContext:
    def test_frozen(self):
        ctx = EngineContext(run_id="abc")
        with pytest.raises(AttributeError):
            ctx.run_id = "xyz"  # type: ignore[misc]

    def test_defaults(self):
        ctx = EngineContext(run_id="abc")
        assert ctx.profile is None
        assert ctx.task_name is None


class TestRegistry:
    def test_from_entrypoints_returns_registry(self):
        reg = Registry.from_entrypoints()
        assert isinstance(reg.readers, dict)
        assert isinstance(reg.writers, dict)
        assert isinstance(reg.transforms, dict)

    def test_register_and_retrieve(self):
        reg = Registry()
        mock_cls = MagicMock
        reg.register_reader("test", mock_cls)
        assert reg.readers["test"] is mock_cls

    def test_from_entrypoints_discovers_hive(self):
        reg = Registry.from_entrypoints()
        assert "hive" in reg.readers
