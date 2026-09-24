"""Plugins are found by entry point group, the way Python 3.10 onwards asks for them.

Every discovery site called ``entry_points()`` and then ``.get(group)``, the
dict interface kept for Python 3.9. On 3.10 and 3.11 that interface is
deprecated, so every plugin lookup (readers, writers, backends, hooks, stores)
raised a DeprecationWarning there, and it is gone in later Pythons' plans. With
3.9 dropped, each site asks ``entry_points(group=...)``.

Checked in a child process, so no discovery cache hides a lookup.
"""

from __future__ import annotations

import subprocess
import sys

LOOKUPS = """
from ubunye._internal import discovery
from ubunye.config import schema
from ubunye.core import backends, runtime
from ubunye.models import artifact_store

runtime.Registry.from_entrypoints()
runtime._discover_hooks()
backends.available()
schema._connectors("ubunye.readers")
artifact_store._registered_stores()
discovery._load_group("ubunye.deploy_adapters")
"""


def test_plugin_discovery_raises_no_deprecation_warning():
    result = subprocess.run(
        [sys.executable, "-W", "error::DeprecationWarning", "-c", LOOKUPS],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
