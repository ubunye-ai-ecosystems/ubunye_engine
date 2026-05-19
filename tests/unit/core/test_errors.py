"""Unit tests for ubunye.core.errors — hierarchy, formatting, dual inheritance."""

import pytest

from ubunye.core.errors import (
    AuthInvalidError,
    AuthNotFoundError,
    BundleDeployError,
    ConfigError,
    ConfigFieldError,
    ConfigProfileError,
    ConfigTemplateError,
    DeployError,
    LineageRecordNotFoundError,
    ModelLoadError,
    ModelNotFittedError,
    MonitorNotFoundError,
    PluginNotFoundError,
    PromotionBlockedError,
    ReaderNotFoundError,
    RegistryError,
    RegistryNotFoundError,
    SinkWriteError,
    SourceReadError,
    SparkSessionError,
    TargetNotFoundError,
    TaskClassMissingError,
    TaskNotFoundError,
    TransformNotFoundError,
    TransformOutputError,
    UbunyeError,
    VersionExistsError,
    VersionNotFoundError,
    WorkspaceUploadError,
    WriterNotFoundError,
)


class TestUbunyeErrorFormatting:
    def test_message_only(self):
        e = UbunyeError("plain error")
        assert str(e) == "plain error"

    def test_message_with_context(self):
        e = UbunyeError("broke", context={"Task": "etl", "File": "x.yaml"})
        s = str(e)
        assert "Task:" in s
        assert "etl" in s
        assert "File:" in s
        assert "x.yaml" in s

    def test_message_with_hint_only(self):
        e = UbunyeError("broke", hint="try this")
        assert "Hint: try this" in str(e)

    def test_message_with_context_and_hint(self):
        e = UbunyeError("broke", context={"X": 1}, hint="fix it")
        s = str(e)
        assert "X:" in s
        assert "Hint: fix it" in s

    def test_context_and_hint_accessible(self):
        e = UbunyeError("msg", context={"a": 1}, hint="h")
        assert e.context == {"a": 1}
        assert e.hint == "h"

    def test_empty_context_defaults(self):
        e = UbunyeError("msg")
        assert e.context == {}
        assert e.hint is None


class TestDualInheritance:
    """Every exception must be catchable by both UbunyeError and its stdlib base."""

    @pytest.mark.parametrize(
        "cls,stdlib_base",
        [
            (ConfigFieldError, ValueError),
            (ConfigTemplateError, ValueError),
            (ConfigProfileError, ValueError),
            (TaskNotFoundError, FileNotFoundError),
            (TaskClassMissingError, RuntimeError),
            (ReaderNotFoundError, KeyError),
            (WriterNotFoundError, KeyError),
            (TransformNotFoundError, KeyError),
            (MonitorNotFoundError, KeyError),
            (SourceReadError, IOError),
            (SinkWriteError, IOError),
            (TransformOutputError, TypeError),
            (SparkSessionError, RuntimeError),
            (ModelLoadError, ImportError),
            (ModelNotFittedError, RuntimeError),
            (VersionExistsError, ValueError),
            (VersionNotFoundError, ValueError),
            (PromotionBlockedError, ValueError),
            (RegistryNotFoundError, FileNotFoundError),
            (LineageRecordNotFoundError, FileNotFoundError),
            (DeployError, RuntimeError),
            (AuthNotFoundError, RuntimeError),
            (AuthInvalidError, RuntimeError),
            (TargetNotFoundError, RuntimeError),
            (WorkspaceUploadError, RuntimeError),
            (BundleDeployError, RuntimeError),
        ],
    )
    def test_isinstance_stdlib(self, cls, stdlib_base):
        e = cls("test")
        assert isinstance(e, UbunyeError)
        assert isinstance(e, stdlib_base)

    def test_plugin_not_found_str_no_extra_quotes(self):
        """KeyError normally wraps the message in quotes; our override prevents that."""
        e = ReaderNotFoundError("Reader 'hve' not found.")
        assert str(e) == "Reader 'hve' not found."


class TestSubclassChains:
    def test_config_hierarchy(self):
        assert issubclass(ConfigFieldError, ConfigError)
        assert issubclass(ConfigError, UbunyeError)
        assert issubclass(ConfigError, ValueError)

    def test_plugin_hierarchy(self):
        assert issubclass(ReaderNotFoundError, PluginNotFoundError)
        assert issubclass(PluginNotFoundError, KeyError)

    def test_registry_hierarchy(self):
        assert issubclass(VersionExistsError, RegistryError)
        assert issubclass(RegistryError, ValueError)

    def test_deploy_hierarchy(self):
        assert issubclass(AuthNotFoundError, DeployError)
        assert issubclass(AuthInvalidError, DeployError)
        assert issubclass(TargetNotFoundError, DeployError)
        assert issubclass(WorkspaceUploadError, DeployError)
        assert issubclass(BundleDeployError, DeployError)
        assert issubclass(DeployError, UbunyeError)
        assert issubclass(DeployError, RuntimeError)
