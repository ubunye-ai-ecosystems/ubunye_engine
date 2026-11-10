"""Building blocks for the docs diagrams: a dark stage, stacked cards, logo tiles.

The look follows the original slides (dark stage, light-blue stacked cards, red
YAML and green PY blocks, white logo cards); the labels are the real names.

Logos in icons/ come from Simple Icons (https://simpleicons.org, CC0). Brand
marks belong to their owners. The Apache Airflow, Hive and Spark marks are
Apache-2.0; the OpenTelemetry mark is CC BY 4.0 (the OpenTelemetry Authors).
AWS and Azure are not in Simple Icons, so they are drawn as plain badges.
"""

from __future__ import annotations

import re
from pathlib import Path
from xml.sax.saxutils import escape

from _svg import MONO, OUT, SANS

ICONS = Path(__file__).resolve().parent / "icons"

C = {
    "bg": "#1c1d22",
    "outline": "#e6e6e6",
    "ink": "#f3f4f6",
    "muted": "#a1a7b3",
    "front": "#cdeefb",
    "mid": "#a6c3cf",
    "back": "#6c838d",
    "card_ink": "#0f172a",
    "yaml": "#e85d5d",
    "yaml_band": "#d93434",
    "py": "#5b8f6c",
    "py_band": "#4caf50",
    "engine": "#2a2c33",
    "step": "#3b3e47",
    "tile": "#ffffff",
    "tile_ink": "#111827",
    "tile_muted": "#5b6472",
    "uc": "#b49cff",
    "pl": "#3dd6c3",
    "tk": "#ff9d5c",
    "accent": "#ff5a4f",
    "ok": "#4ade80",
}

BRAND = {
    "pandas": "#150458",
    "apachespark": "#E25A1C",
    "databricks": "#FF3621",
    "googlecloud": "#4285F4",
    "kubernetes": "#326CE5",
    "docker": "#2496ED",
    "apacheairflow": "#017CEE",
    "prometheus": "#E6522C",
    "opentelemetry": "#425CC7",
    "mlflow": "#0194E2",
    "postgresql": "#4169E1",
    "mysql": "#4479A1",
    "apachehive": "#D4A900",
    "json": "#0f172a",
    "python": "#3776AB",
    "scikitlearn": "#F7931E",
    "pytorch": "#EE4C2C",
}

# Glyphs for marks Simple Icons does not carry, in a 24x24 box.
GLYPHS = {
    "s3": '<path d="M3 5c0-1.1 4-2 9-2s9 .9 9 2l-2 14c0 1.1-3.1 2-7 2s-7-.9-7-2z" fill="#569A31"/>'
    '<ellipse cx="12" cy="5" rx="9" ry="2" fill="#7cc35a"/>',
    "delta": '<path d="M12 3 22 21H2z" fill="none" stroke="#00ADD4" stroke-width="2.6" '
    'stroke-linejoin="round"/><path d="M12 10 16.5 18h-9z" fill="#00ADD4"/>',
    "file": '<path d="M6 2h8l5 5v15H6z" fill="#64748b"/><path d="M14 2v5h5" fill="#94a3b8"/>'
    '<text x="12.5" y="18" font-family="monospace" font-size="6" font-weight="700" '
    'fill="#fff" text-anchor="middle">010</text>',
    "aws": '<rect width="24" height="24" rx="5" fill="#232F3E"/><text x="12" y="14" '
    f'font-family="{SANS}" font-size="9" font-weight="700" fill="#FF9900" '
    'text-anchor="middle">aws</text><path d="M6 16.5q6 3 12 0" fill="none" stroke="#FF9900" '
    'stroke-width="1.6" stroke-linecap="round"/>',
    "azure": '<rect width="24" height="24" rx="5" fill="#0078D4"/><path d="M6 18 11 5h3l-5 13z'
    'M12 18l3.5-7 3.5 7z" fill="#fff"/>',
    "openlineage": '<circle cx="5" cy="12" r="3" fill="#1e88e5"/><circle cx="19" cy="6" r="3" '
    'fill="#1e88e5"/><circle cx="19" cy="18" r="3" fill="#1e88e5"/><path d="M5 12 19 6M5 12 19 18" '
    'stroke="#1e88e5" stroke-width="2"/>',
    "table": '<rect x="2" y="3" width="20" height="18" rx="2.5" fill="#0f766e"/>'
    '<path d="M2 9h20M2 15h20M9 9v12M15.5 9v12" stroke="#fff" stroke-width="1.6"/>',
    "bin": '<path d="M4 7h16l-1.6 14H5.6z" fill="#d97706"/><rect x="2.5" y="3.5" width="19" height="3.5" '
    'rx="1.2" fill="#f59e0b"/>',
    "log": '<path d="M5 2h10l4 4v16H5z" fill="#64748b"/><path d="M8 10h8M8 14h8M8 18h5" stroke="#fff" '
    'stroke-width="1.6"/>',
    "stop": '<path d="M7.6 2h8.8L22 7.6v8.8L16.4 22H7.6L2 16.4V7.6z" fill="#dc2626"/>'
    '<path d="M7.5 12h9" stroke="#fff" stroke-width="3" stroke-linecap="round"/>',
    "shield": '<path d="M12 2 21 5.5v6c0 5.4-3.8 9.3-9 10.5C6.8 20.8 3 16.9 3 11.5v-6z" fill="#6366f1"/>'
    '<path d="M8 12l3 3 5-6" fill="none" stroke="#fff" stroke-width="2.2" stroke-linecap="round" '
    'stroke-linejoin="round"/>',
    "laptop": '<rect x="4" y="5" width="16" height="11" rx="1.5" fill="none" stroke="#475569" '
    'stroke-width="2"/><path d="M2 19h20" stroke="#475569" stroke-width="2.4" stroke-linecap="round"/>',
}


def t(x, y, s, *, size=16, fill=None, weight=400, font=SANS, anchor="start", extra=""):
    fill = fill or C["ink"]
    return (
        f'<text x="{x}" y="{y}" font-family="{font}" font-size="{size}" font-weight="{weight}" '
        f'fill="{fill}" text-anchor="{anchor}"{extra}>{escape(s)}</text>'
    )


def spans(x, y, parts, *, size, font=MONO, anchor="start"):
    inner = "".join(f'<tspan fill="{c}" font-weight="{w}">{escape(s)}</tspan>' for s, c, w in parts)
    return (
        f'<text x="{x}" y="{y}" font-family="{font}" font-size="{size}" text-anchor="{anchor}" '
        f'xml:space="preserve">{inner}</text>'
    )


def icon(name, x, y, size):
    """A logo (Simple Icons slug) or one of GLYPHS, drawn in a size x size box."""
    if name in GLYPHS:
        inner = GLYPHS[name]
    else:
        src = (ICONS / f"{name}.svg").read_text(encoding="utf-8")
        d = re.search(r'<path d="([^"]+)"', src).group(1)
        inner = f'<path d="{d}" fill="{BRAND[name]}"/>'
    return f'<svg x="{x}" y="{y}" width="{size}" height="{size}" viewBox="0 0 24 24">{inner}</svg>'


def chip_icon(name, x, y, size, pad=6):
    """A logo on a small white chip, so dark marks still show on the dark stage."""
    return (
        f'<rect x="{x - pad}" y="{y - pad}" width="{size + 2 * pad}" height="{size + 2 * pad}" rx="8" '
        f'fill="{C["tile"]}"/>' + icon(name, x, y, size)
    )


def defs():
    return (
        "<defs>"
        '<marker id="head" viewBox="0 0 10 10" refX="7" refY="5" markerWidth="7" markerHeight="7" '
        f'orient="auto-start-reverse"><path d="M0 0 10 5 0 10z" fill="{C["outline"]}"/></marker>'
        "</defs>"
    )


def stage(w, h):
    return f'<rect width="{w}" height="{h}" rx="22" fill="{C["bg"]}"/>'


def box(x, y, w, h, *, rx=22, stroke=None, fill="none", sw=2, dash=False):
    d = ' stroke-dasharray="7 6"' if dash else ""
    return (
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" fill="{fill}" '
        f'stroke="{stroke or C["outline"]}" stroke-width="{sw}"{d}/>'
    )


def badge(x, y, flag, colour):
    """The CLI flag as a small coloured pill; returns (svg, width)."""
    w = 40
    return (
        f'<rect x="{x}" y="{y}" width="{w}" height="26" rx="13" fill="{colour}"/>'
        + t(x + w / 2, y + 19, flag, size=16, weight=700, font=MONO, fill=C["bg"], anchor="middle"),
        w,
    )


def heading(x, y, flag, label, colour, sub=None):
    out = []
    if flag:
        b, w = badge(x, y - 19, flag, colour)
        out.append(b)
        x += w + 10
    out.append(
        t(
            x,
            y,
            label,
            size=20,
            weight=700,
            fill=colour if flag else C["ink"],
            extra=' letter-spacing="1"',
        )
    )
    if sub:
        out.append(t(x - (50 if flag else 0), y + 26, sub, size=15, fill=C["muted"], font=MONO))
    return "\n".join(out)


def stack(x, y, w, h, labels, *, step=26, colour=None):
    """Cards stacked like the slides: labels[0] in front, the rest behind, up and right."""
    fills = [C["front"], C["mid"], C["back"]]
    out = []
    for i in reversed(range(len(labels))):
        cx, cy = x + i * step, y - i * step
        last = labels[i] == "…"
        stroke = C["outline"]
        out.append(
            box(cx, cy, w, h, rx=14, fill=fills[min(i, 2)], stroke=stroke, sw=1.5, dash=last)
        )
        ink = C["card_ink"] if i < 2 else C["ink"]
        out.append(t(cx + 14, cy + 22, labels[i], size=17, weight=700, font=MONO, fill=ink))
    if colour:  # the front card is the one we look inside: its level colour and a folder
        out.append(box(x, y, w, h, rx=14, stroke=colour, sw=3))
        out.append(folder(x + w / 2 - 22, y + h / 2 - 8, 44, colour))
    return "\n".join(out)


def folder(x, y, w, colour):
    h = w * 0.72
    return (
        f'<path d="M{x} {y + 6}a4 4 0 0 1 4-4h{w * 0.3}l5 6h{w * 0.7 - 13}a4 4 0 0 1 4 4v{h - 12}'
        f'a4 4 0 0 1-4 4H{x + 4}a4 4 0 0 1-4-4z" fill="{colour}" fill-opacity="0.85"/>'
    )


def file_block(x, y, w, h, ext, top, band, caption):
    out = [
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="12" fill="{top}"/>',
        f'<path d="M{x} {y + h - 40}h{w}v28a12 12 0 0 1-12 12h-{w - 24}a12 12 0 0 1-12-12z" fill="{band}"/>',
        t(
            x + w / 2,
            y + (h - 40) / 2 + 9,
            "</>",
            size=24,
            weight=700,
            fill="#fff",
            anchor="middle",
            font=MONO,
        ),
        t(x + w / 2, y + h - 13, ext, size=19, weight=700, fill="#fff", anchor="middle", font=MONO),
    ]
    if caption:
        out.append(
            t(
                x + w / 2,
                y + h + 22,
                caption,
                size=15,
                weight=700,
                fill=C["card_ink"],
                anchor="middle",
            )
        )
    return "\n".join(out)


def task_files(x, y, w, h, *, captions=("read & write", "your logic")):
    """The red YAML and green PY blocks side by side inside a task card."""
    bw = (w - 14) / 2
    return file_block(x, y, bw, h, ".yaml", C["yaml"], C["yaml_band"], captions[0]) + file_block(
        x + bw + 14, y, bw, h, ".py", C["py"], C["py_band"], captions[1]
    )


def loop_arrow(x1, y1, x2, y2, depth=90):
    """The dashed swoop from the slides: down out of a card, round and into the next box."""
    return (
        f'<path d="M{x1} {y1} C{x1} {y1 + depth} {x2 - 40} {y1 + depth} {x2} {y2}" fill="none" '
        f'stroke="{C["outline"]}" stroke-width="2.5" stroke-dasharray="9 7" marker-end="url(#head)"/>'
    )


def arrow(x1, x2, y, colour, *, width=16):
    """A fat arrow like the slides' white and red ones."""
    hw = width / 2
    return (
        f'<path d="M{x1} {y - hw}H{x2 - 22}V{y - hw - 9}L{x2} {y}L{x2 - 22} {y + hw + 9}V{y + hw}H{x1}z" '
        f'fill="{colour}"/>'
    )


def tile(x, y, w, h, icons, label, *, sub=None, mark=None, size=18):
    """A white logo card: icon(s), a name, an optional grey hint and a corner mark."""
    out = [f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="{C["tile"]}"/>']
    isz = min(28, h - 18)
    ix = x + 12
    for name in icons:
        out.append(icon(name, ix, y + (h - isz) / 2, isz))
        ix += isz + 6
    ty = y + h / 2 + (size * 0.35 if not sub else -2)
    out.append(t(ix + 4, ty, label, size=size, weight=700, fill=C["tile_ink"]))
    if sub:
        out.append(t(ix + 4, ty + 17, sub, size=13, fill=C["tile_muted"]))
    if mark:
        out.append(icon(mark, x + w - 30, y + (h - 20) / 2, 20))
    return "\n".join(out)


def receipt(x, y, w, h, label, value):
    zig = "".join(f"l{6} {5 if i % 2 == 0 else -5}" for i in range(int(w // 6)))
    return "\n".join(
        [
            f'<path d="M{x} {y}h{w}v{h}H{x}z" fill="{C["tile"]}"/>',
            f'<path d="M{x} {y + h}{zig}" fill="none" stroke="{C["bg"]}" stroke-width="3"/>',
            t(x + 12, y + 20, label, size=13, weight=700, fill=C["tile_muted"]),
            t(x + 12, y + 44, value, size=17, weight=700, font=MONO, fill=C["tile_ink"]),
        ]
    )


def write_one(name, w, h, label, body):
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / f"{name}.svg"
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" width="{w}" height="{h}" '
        f'role="img" aria-label="{escape(label)}">\n<desc>Logos: Simple Icons (CC0); marks belong '
        "to their owners; OpenTelemetry logo CC BY 4.0, the OpenTelemetry Authors.</desc>\n"
        f"{defs()}\n{stage(w, h)}\n{body}\n</svg>\n",
        encoding="utf-8",
    )
    print(path.relative_to(OUT.parents[2]))
