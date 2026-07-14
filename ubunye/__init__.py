"""Ubunye Engine package."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("ubunye-engine")
except PackageNotFoundError:
    __version__ = "unknown"

from ubunye.api import run_pipeline, run_task
from ubunye.core.errors import UbunyeError
from ubunye.notebook import notebook

# Only names this module actually imports. The old list also carried the strings
# "core", "config", "cli", "plugins" and "backends", which were never imported here,
# so `from ubunye import *` NameError'd on its own advertised surface.
__all__ = [
    "run_task",
    "run_pipeline",
    "notebook",
    "UbunyeError",
]
