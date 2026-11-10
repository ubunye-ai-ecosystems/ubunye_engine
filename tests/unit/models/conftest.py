"""Fixtures for the model registry tests."""

from __future__ import annotations

from pathlib import Path

import pytest

THIRDPARTY = "unit.models.thirdparty_registry:JsonRegistryBackend"


@pytest.fixture
def thirdparty_installed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    """Install the test's third party registry the way pip would: a dist-info on sys.path.

    Nothing in Ubunye is edited or patched; the backend is found through the
    ``ubunye.registry_backends`` entry point group like any installed package.
    Returns the name it is registered under.
    """
    site = tmp_path / "site"
    dist = site / "thirdparty_registry-1.0.dist-info"
    dist.mkdir(parents=True)
    (dist / "METADATA").write_text(
        "Metadata-Version: 2.1\nName: thirdparty-registry\nVersion: 1.0\n", encoding="utf-8"
    )
    (dist / "entry_points.txt").write_text(
        f"[ubunye.registry_backends]\njsonreg = {THIRDPARTY}\n", encoding="utf-8"
    )
    monkeypatch.syspath_prepend(str(site))
    return "jsonreg"
