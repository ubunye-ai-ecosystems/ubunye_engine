"""Structured exception hierarchy for the Ubunye Engine.

Every user-facing exception inherits from :class:`UbunyeError` **and** from
the stdlib type it replaces (dual inheritance), so existing ``except ValueError``
or ``except KeyError`` catches continue to work. Each exception carries optional
``context`` (dict of relevant facts) and ``hint`` (what to try next), rendered
as a multi-line diagnostic when printed.
"""

from __future__ import annotations

from typing import Any, Dict, Optional


class UbunyeError(Exception):
    """Base class for all Ubunye Engine exceptions."""

    def __init__(
        self,
        message: str,
        *,
        context: Optional[Dict[str, Any]] = None,
        hint: Optional[str] = None,
    ) -> None:
        self.context: Dict[str, Any] = context or {}
        self.hint: Optional[str] = hint
        super().__init__(message)

    def __str__(self) -> str:
        parts = [str(self.args[0])]
        if self.context:
            parts.append("")
            key_width = max(len(str(k)) for k in self.context) + 1
            for key, val in self.context.items():
                label = f"{key}:".ljust(key_width)
                parts.append(f"  {label} {val}")
        if self.hint:
            parts.append(f"  Hint: {self.hint}")
        return "\n".join(parts)


# ---------------------------------------------------------------------------
# Config errors
# ---------------------------------------------------------------------------


class ConfigError(UbunyeError, ValueError):
    """Base for config-related errors."""


class ConfigFieldError(ConfigError):
    """Raised when a config contains unknown fields, with typo suggestions."""


class ConfigTemplateError(ConfigError):
    """Raised when a Jinja template variable is undefined."""


class ConfigProfileError(ConfigError):
    """Raised when a requested profile does not exist in ENGINE.profiles."""


# ---------------------------------------------------------------------------
# Task discovery errors
# ---------------------------------------------------------------------------


class TaskNotFoundError(UbunyeError, FileNotFoundError):
    """Raised when transformations.py is missing from the task directory."""


class TaskClassMissingError(UbunyeError, RuntimeError):
    """Raised when transformations.py has no Task subclass."""


# ---------------------------------------------------------------------------
# Plugin discovery errors
# ---------------------------------------------------------------------------


class PluginNotFoundError(UbunyeError, KeyError):
    """Base for missing plugin errors."""

    def __str__(self) -> str:
        return UbunyeError.__str__(self)


class ReaderNotFoundError(PluginNotFoundError):
    """Raised when a reader plugin format is not registered."""


class WriterNotFoundError(PluginNotFoundError):
    """Raised when a writer plugin format is not registered."""


class TransformNotFoundError(PluginNotFoundError):
    """Raised when a transform plugin type is not registered."""


class MonitorNotFoundError(PluginNotFoundError):
    """Raised when a monitor plugin type is not registered."""


# ---------------------------------------------------------------------------
# I/O runtime errors
# ---------------------------------------------------------------------------


class SourceReadError(UbunyeError, IOError):
    """Raised when reading a data source fails."""


class SinkWriteError(UbunyeError, IOError):
    """Raised when writing to a data sink fails."""


# ---------------------------------------------------------------------------
# Transform errors
# ---------------------------------------------------------------------------


class TransformOutputError(UbunyeError, TypeError):
    """Raised when a transform returns an unexpected type or missing output."""


# ---------------------------------------------------------------------------
# Backend errors
# ---------------------------------------------------------------------------


class SparkSessionError(UbunyeError, RuntimeError):
    """Raised when a SparkSession is not available."""


# ---------------------------------------------------------------------------
# Model / ML errors
# ---------------------------------------------------------------------------


class ModelLoadError(UbunyeError, ImportError):
    """Raised when a user model class cannot be loaded."""


class ModelNotFittedError(UbunyeError, RuntimeError):
    """Raised when predict/save is called on an unfitted model."""


# ---------------------------------------------------------------------------
# Registry errors
# ---------------------------------------------------------------------------


class RegistryError(UbunyeError, ValueError):
    """Base for model registry errors."""


class VersionExistsError(RegistryError):
    """Raised when a version string is already registered."""


class VersionNotFoundError(RegistryError):
    """Raised when a requested version does not exist."""


class PromotionBlockedError(RegistryError):
    """Raised when promotion gates fail."""


class RegistryNotFoundError(UbunyeError, FileNotFoundError):
    """Raised when the registry JSON file does not exist."""


# ---------------------------------------------------------------------------
# Lineage errors
# ---------------------------------------------------------------------------


class LineageRecordNotFoundError(UbunyeError, FileNotFoundError):
    """Raised when a lineage record file does not exist."""


# ---------------------------------------------------------------------------
# Deploy errors
# ---------------------------------------------------------------------------


class DeployError(UbunyeError, RuntimeError):
    """Base for deployment errors."""


class AuthNotFoundError(DeployError):
    """Raised when Databricks auth credentials are missing."""


class AuthInvalidError(DeployError):
    """Raised when Databricks auth credentials are rejected."""


class TargetNotFoundError(DeployError):
    """Raised when a deploy target (dev/prod) is not configured."""


class WorkspaceUploadError(DeployError):
    """Raised when uploading files to Databricks workspace fails."""


class BundleDeployError(DeployError):
    """Raised when job creation/update on Databricks fails."""
