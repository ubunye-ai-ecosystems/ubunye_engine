"""Paths and fonts shared by the docs diagram scripts in this folder."""

from __future__ import annotations

from pathlib import Path

OUT = Path(__file__).resolve().parents[2] / "docs" / "assets" / "diagrams"

SANS = "Roboto, 'Segoe UI', Helvetica, Arial, sans-serif"
MONO = "'Roboto Mono', Menlo, Consolas, 'DejaVu Sans Mono', monospace"
