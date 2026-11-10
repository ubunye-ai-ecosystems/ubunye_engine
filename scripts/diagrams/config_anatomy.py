"""Draw the config.yaml anatomy diagram (docs/assets/diagrams/config-anatomy.svg).

The file on the left in four coloured bands; each band points to what it controls.

    python scripts/diagrams/config_anatomy.py
"""

from __future__ import annotations

from _svg import MONO
from stage import C, chip_icon, t, write_one

W, H = 1100, 664

BANDS = [
    # colour, yaml lines, title, icons, pills, required
    (
        "#94a3b8",
        ["MODEL: etl", 'VERSION: "1.0.0"'],
        "What it is",
        [],
        ["etl", "ml", "1.0.0"],
        False,
    ),
    (
        "#3b82f6",
        ["ENGINE:", "  profiles:", "    DEV:  ...", "    PROD: ..."],
        "How it runs",
        ["apachespark", "databricks", "pandas"],
        ["DEV", "PROD"],
        False,
    ),
    (
        "#f97316",
        ["CONFIG:", "  inputs: ...", "  transform: ...", "  outputs: ...", "  expectations: ..."],
        "What it does",
        ["s3", "python", "table", "shield"],
        [],
        True,
    ),
    (
        "#a855f7",
        ["ORCHESTRATION:", "  type: airflow", '  schedule: "0 2 * * *"'],
        "When it runs",
        ["apacheairflow", "databricks"],
        ["ubunye export"],
        False,
    ),
]
LINE, PAD, GAP = 24, 16, 12


def pill(x, y, label, colour):
    w = len(label) * 9.6 + 22
    return (
        f'<rect x="{x}" y="{y}" width="{w}" height="30" rx="15" fill="none" stroke="{colour}" stroke-width="1.6"/>'
        + t(x + w / 2, y + 20, label, size=15, weight=700, font=MONO, fill=colour, anchor="middle"),
        w,
    )


def build() -> str:
    parts = []
    fx, fy, fw = 30, 40, 470
    heights = [max(len(lines) * LINE + 2 * PAD, 96) for _, lines, *_ in BANDS]
    fh = 50 + sum(heights) + GAP * (len(BANDS) - 1) + 18

    # the file
    parts.append(f'<rect x="{fx}" y="{fy}" width="{fw}" height="{fh}" rx="14" fill="{C["tile"]}"/>')
    parts.append(
        f'<path d="M{fx} {fy + 14}a14 14 0 0 1 14-14h{fw - 28}a14 14 0 0 1 14 14v22H{fx}z" fill="#e5e7eb"/>'
    )
    parts.append(
        t(fx + 18, fy + 25, "config.yaml", size=16, weight=700, font=MONO, fill=C["tile_ink"])
    )

    y = fy + 50
    for (colour, lines, title, icons, pills, required), h in zip(BANDS, heights):
        # the band in the file
        bx, bw = fx + 14, fw - 28
        parts.append(
            f'<rect x="{bx}" y="{y}" width="{bw}" height="{h}" rx="10" fill="{colour}" fill-opacity="0.16"/>'
        )
        parts.append(f'<rect x="{bx}" y="{y}" width="6" height="{h}" rx="3" fill="{colour}"/>')
        ly = y + (h - len(lines) * LINE) / 2 + 17
        for i, line in enumerate(lines):
            parts.append(
                t(
                    bx + 20,
                    ly + i * LINE,
                    line,
                    size=17,
                    font=MONO,
                    fill=C["tile_ink"],
                    weight=700 if i == 0 else 400,
                    extra=' xml:space="preserve"',
                )
            )

        # connector and the meaning card
        cy = y + h / 2
        cx = 580
        parts.append(f'<path d="M{bx + bw} {cy}H{cx}" stroke="{colour}" stroke-width="3"/>')
        parts.append(f'<circle cx="{bx + bw}" cy="{cy}" r="5" fill="{colour}"/>')
        parts.append(
            f'<rect x="{cx}" y="{y}" width="{1070 - cx}" height="{h}" rx="14" fill="{C["engine"]}" '
            f'stroke="{colour}" stroke-width="2"/>'
        )
        parts.append(t(cx + 22, y + 34, title, size=21, weight=700))
        tag, tag_c = ("required", colour) if required else ("optional", C["muted"])
        tw = len(tag) * 8.4 + 20
        parts.append(
            f'<rect x="{1070 - 16 - tw}" y="{y + 14}" width="{tw}" height="26" rx="13" fill="{tag_c}" '
            f'fill-opacity="{0.25 if required else 0.15}"/>'
        )
        parts.append(
            t(
                1070 - 16 - tw / 2,
                y + 32,
                tag,
                size=14,
                weight=700,
                fill=tag_c if required else C["muted"],
                anchor="middle",
            )
        )
        x = cx + 28
        row = y + 56
        for i, ic in enumerate(icons):
            parts.append(chip_icon(ic, x, row, 26))
            if title == "What it does":
                word = ["read", "change", "write", "check"][i]
                parts.append(t(x + 13, row + 58, word, size=14, fill=C["muted"], anchor="middle"))
            x += 46
            if title == "What it does" and i < len(icons) - 2:
                parts.append(t(x - 6, row + 19, "→", size=18, fill=C["muted"]))
                x += 18
        for label in pills:
            svg, w = pill(x, row - 2, label, colour if colour != "#94a3b8" else C["ink"])
            parts.append(svg)
            x += w + 10
        y += h + GAP
    return "\n".join(parts)


if __name__ == "__main__":
    write_one(
        "config-anatomy",
        W,
        H,
        "config.yaml in four parts: MODEL and VERSION say what the job is; ENGINE says how it runs, "
        "with DEV and PROD profiles; CONFIG, the only required part, says what it does: inputs, "
        "transform, outputs and expectations; ORCHESTRATION says when it runs, for ubunye export.",
        build(),
    )
