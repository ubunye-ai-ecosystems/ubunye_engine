"""Ubunye Engine package."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("ubunye-engine")
except PackageNotFoundError:
    __version__ = "unknown"

__all__ = ["core", "config", "cli", "plugins", "backends"]
