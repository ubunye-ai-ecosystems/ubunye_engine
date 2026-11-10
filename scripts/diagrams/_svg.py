"""Small helpers shared by the docs diagram scripts in this folder.

Each script draws one diagram twice, with a light and a dark palette, and writes
docs/assets/diagrams/<name>-light.svg and <name>-dark.svg. The docs page shows the
right one with mkdocs-material's #only-light / #only-dark image suffixes.
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable, Dict
from xml.sax.saxutils import escape

OUT = Path(__file__).resolve().parents[2] / "docs" / "assets" / "diagrams"

SANS = "Roboto, 'Segoe UI', Helvetica, Arial, sans-serif"
MONO = "'Roboto Mono', Menlo, Consolas, 'DejaVu Sans Mono', monospace"

# Colours every diagram uses. Each script adds its own accents on top.
BASE = {
    "light": {
        "ink": "#1f2937",
        "muted": "#5b6472",
        "card": "#ffffff",
        "on_badge": "#ffffff",
        "tint": 0.07,
        "line": "#c5cad3",
        "accent": "#3f51b5",  # the site's indigo
        "ok": "#15803d",
    },
    "dark": {
        "ink": "#e5e7eb",
        "muted": "#a3abb8",
        "card": "#1e2129",
        "on_badge": "#111318",
        "tint": 0.12,
        "line": "#4a5060",
        "accent": "#8c9eff",
        "ok": "#4ade80",
    },
}


def text(x, y, s, *, size=16, fill, weight=400, font=SANS, anchor="start", extra=""):
    return (
        f'<text x="{x}" y="{y}" font-family="{font}" font-size="{size}" '
        f'font-weight="{weight}" fill="{fill}" text-anchor="{anchor}"{extra}>{escape(s)}</text>'
    )


def spans(x, y, parts, *, size, font=MONO, anchor="start"):
    """One line of text in several colours: parts = [(text, colour, weight), ...]."""
    inner = "".join(f'<tspan fill="{c}" font-weight="{w}">{escape(t)}</tspan>' for t, c, w in parts)
    return (
        f'<text x="{x}" y="{y}" font-family="{font}" font-size="{size}" '
        f'text-anchor="{anchor}" xml:space="preserve">{inner}</text>'
    )


def rect(x, y, w, h, *, fill="none", stroke="none", rx=12, sw=1.5, extra=""):
    return (
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" fill="{fill}" '
        f'stroke="{stroke}" stroke-width="{sw}"{extra}/>'
    )


def arrow_down(x, y1, y2, colour, sw=2.5):
    return (
        f'<line x1="{x}" y1="{y1}" x2="{x}" y2="{y2 - 10}" stroke="{colour}" stroke-width="{sw}"/>'
        f'<polygon points="{x},{y2} {x - 7},{y2 - 12} {x + 7},{y2 - 12}" fill="{colour}"/>'
    )


def svg(width, height, label, body):
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" '
        f'width="{width}" height="{height}" role="img" aria-label="{escape(label)}">\n'
        f"{body}\n</svg>\n"
    )


def write(name: str, build: Callable[[Dict], str], accents: Dict[str, Dict] | None = None) -> None:
    """Draw `name` with both palettes. `accents` adds per-palette colours to BASE."""
    OUT.mkdir(parents=True, exist_ok=True)
    for scheme, base in BASE.items():
        p = {**base, **((accents or {}).get(scheme, {}))}
        path = OUT / f"{name}-{scheme}.svg"
        path.write_text(build(p), encoding="utf-8")
        print(path.relative_to(OUT.parents[2]))
