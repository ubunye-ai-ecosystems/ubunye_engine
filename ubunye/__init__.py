"""Ubunye Engine package."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("ubunye-engine")
except PackageNotFoundError:
    __version__ = "unknown"

from ubunye.api import run_pipeline, run_task

__all__ = ["core", "config", "cli", "plugins", "backends", "run_task", "run_pipeline"]
