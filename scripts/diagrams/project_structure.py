"""Draw the Project Structure diagram (docs/assets/diagrams/project-structure-*.svg).

One template, two palettes (light and dark). Re-run after changing a label:

    python scripts/diagrams/project_structure.py
"""

from __future__ import annotations

from pathlib import Path
from xml.sax.saxutils import escape

OUT = Path(__file__).resolve().parents[2] / "docs" / "assets" / "diagrams"

SANS = "Roboto, 'Segoe UI', Helvetica, Arial, sans-serif"
MONO = "'Roboto Mono', Menlo, Consolas, 'DejaVu Sans Mono', monospace"

PALETTES = {
    "light": {
        "ink": "#1f2937",
        "muted": "#5b6472",
        "card": "#ffffff",
        "on_badge": "#ffffff",
        "tint": 0.07,
        "root": "#5b6472",
        "usecase": "#6d28d9",
        "pipeline": "#0f766e",
        "task": "#c2410c",
    },
    "dark": {
        "ink": "#e5e7eb",
        "muted": "#a3abb8",
        "card": "#1e2129",
        "on_badge": "#111318",
        "tint": 0.12,
        "root": "#a3abb8",
        "usecase": "#b49cff",
        "pipeline": "#3dd6c3",
        "task": "#ff9d5c",
    },
}

W = 720
X0, X1 = 20, W - 20  # panel edges
PAD = 24  # inner padding


def text(x, y, s, *, size=16, fill, weight=400, font=SANS, anchor="start", extra=""):
    return (
        f'<text x="{x}" y="{y}" font-family="{font}" font-size="{size}" '
        f'font-weight="{weight}" fill="{fill}" text-anchor="{anchor}"{extra}>{escape(s)}</text>'
    )


def spans(x, y, parts, *, size, font=MONO):
    """One line of text in several colours: parts = [(text, colour, weight), ...]."""
    inner = "".join(f'<tspan fill="{c}" font-weight="{w}">{escape(t)}</tspan>' for t, c, w in parts)
    return (
        f'<text x="{x}" y="{y}" font-family="{font}" font-size="{size}" '
        f'xml:space="preserve">{inner}</text>'
    )


def panel(y, h, colour, p):
    return (
        f'<rect x="{X0}" y="{y}" width="{X1 - X0}" height="{h}" rx="16" '
        f'fill="{colour}" fill-opacity="{p["tint"]}" stroke="{colour}" stroke-width="2"/>'
    )


def header(y, flag, kind, name, blurb, colour, p):
    """Badge with the CLI flag, the level name, the folder name and one line of meaning."""
    x = X0 + PAD
    out = [
        f'<rect x="{x}" y="{y}" width="44" height="28" rx="14" fill="{colour}"/>',
        text(
            x + 22,
            y + 20,
            flag,
            size=16,
            weight=700,
            font=MONO,
            fill=p["on_badge"],
            anchor="middle",
        ),
        text(x + 58, y + 20, kind, size=14, weight=700, fill=colour, extra=' letter-spacing="1.5"'),
        text(x, y + 60, name, size=26, weight=700, font=MONO, fill=p["ink"]),
        text(x, y + 88, blurb, size=17, fill=p["muted"]),
    ]
    return "\n".join(out)


def chip(x, y, w, label, colour, p, *, focus):
    h = 44
    if focus:
        box = (
            f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="{p["card"]}" '
            f'stroke="{colour}" stroke-width="2.5"/>'
        )
        label_fill, weight = p["ink"], 700
    else:
        box = (
            f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="{p["card"]}" '
            f'stroke="{colour}" stroke-opacity="0.55" stroke-width="1.5" stroke-dasharray="6 5"/>'
        )
        label_fill, weight = p["muted"], 400
    return box + text(
        x + w / 2,
        y + 29,
        label,
        size=18,
        weight=weight,
        font=MONO,
        fill=label_fill,
        anchor="middle",
    )


def zoom(x_a, x_b, y_top, y_bot, colour):
    """Two lines from a chip down to the edges of the next panel: 'look inside this one'."""
    poly = f"{x_a},{y_top} {x_b},{y_top} {X1},{y_bot} {X0},{y_bot}"
    return (
        f'<polygon points="{poly}" fill="{colour}" fill-opacity="0.08"/>'
        f'<line x1="{x_a}" y1="{y_top}" x2="{X0}" y2="{y_bot}" stroke="{colour}" '
        f'stroke-opacity="0.5" stroke-width="1.5" stroke-dasharray="4 4"/>'
        f'<line x1="{x_b}" y1="{y_top}" x2="{X1}" y2="{y_bot}" stroke="{colour}" '
        f'stroke-opacity="0.5" stroke-width="1.5" stroke-dasharray="4 4"/>'
    )


def arrow(x1, x2, y, colour):
    return (
        f'<line x1="{x1}" y1="{y}" x2="{x2 - 9}" y2="{y}" stroke="{colour}" stroke-width="2.5"/>'
        f'<polygon points="{x2},{y} {x2 - 11},{y - 6} {x2 - 11},{y + 6}" fill="{colour}"/>'
    )


def file_card(x, y, w, h, name, code, what, how, colour, p):
    out = [
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="12" fill="{p["card"]}" '
        f'stroke="{colour}" stroke-opacity="0.6" stroke-width="1.5"/>',
        text(x + 18, y + 32, name, size=19, weight=700, font=MONO, fill=p["ink"]),
    ]
    for i, line in enumerate(code):
        out.append(
            text(
                x + 18,
                y + 62 + i * 22,
                line,
                size=15,
                font=MONO,
                fill=p["muted"],
                extra=' xml:space="preserve"',
            )
        )
    cy = y + 62 + len(code) * 22 + 14
    out.append(
        f'<line x1="{x + 18}" y1="{cy - 14}" x2="{x + w - 18}" y2="{cy - 14}" '
        f'stroke="{colour}" stroke-opacity="0.25"/>'
    )
    out.append(
        spans(
            x + 18, cy + 10, [(what, colour, 700), ("  " + how, p["ink"], 400)], size=17, font=SANS
        )
    )
    return "\n".join(out)


def build(p) -> str:
    uc, pl, tk, root = p["usecase"], p["pipeline"], p["task"], p["root"]
    parts = []

    # 1. Use case
    y1, h1 = 20, 196
    parts.append(panel(y1, h1, uc, p))
    parts.append(
        header(
            y1 + 20,
            "-u",
            "USE CASE",
            "fraud_detection/",
            "A business problem. It holds one or more pipelines.",
            uc,
            p,
        )
    )
    chip_y = y1 + 132
    parts.append(chip(X0 + PAD, chip_y, 200, "ingestion", pl, p, focus=True))
    parts.append(chip(X0 + PAD + 220, chip_y, 200, "scoring", pl, p, focus=False))

    # zoom into "ingestion"
    y2 = y1 + h1 + 44
    parts.append(zoom(X0 + PAD, X0 + PAD + 200, chip_y + 44, y2, pl))

    # 2. Pipeline
    h2 = 214
    parts.append(panel(y2, h2, pl, p))
    parts.append(
        header(
            y2 + 20,
            "-p",
            "PIPELINE",
            "ingestion/",
            "Runs its tasks in order, one after another.",
            pl,
            p,
        )
    )
    ty = y2 + 150
    cw, gap = 180, 54
    xs = [X0 + PAD + i * (cw + gap) for i in range(3)]
    for i, (x, name) in enumerate(zip(xs, ["claim_etl", "policy_etl", "claims_joined"])):
        parts.append(chip(x, ty, cw, name, tk, p, focus=(i == 0)))
        parts.append(text(x + 4, ty - 8, f"step {i + 1}", size=13, weight=700, fill=p["muted"]))
        if i < 2:
            parts.append(arrow(x + cw + 6, x + cw + gap - 6, ty + 22, pl))

    # zoom into "claim_etl"
    y3 = y2 + h2 + 44
    parts.append(zoom(xs[0], xs[0] + cw, ty + 44, y3, tk))

    # 3. Task
    h3 = 300
    parts.append(panel(y3, h3, tk, p))
    parts.append(
        header(
            y3 + 20,
            "-t",
            "TASK",
            "claim_etl/",
            "One folder, one job: read, transform, write.",
            tk,
            p,
        )
    )
    cy, ch = y3 + 128, 150
    half = (X1 - X0 - 2 * PAD - 16) / 2
    parts.append(
        file_card(
            X0 + PAD,
            cy,
            half,
            ch,
            "config.yaml",
            ["inputs:   where to read", "outputs:  where to write"],
            "WHAT",
            "to read and write",
            tk,
            p,
        )
    )
    parts.append(
        file_card(
            X0 + PAD + half + 16,
            cy,
            half,
            ch,
            "transformations.py",
            ["class ClaimEtl(Task):", "    def transform(...)"],
            "HOW",
            "to change the data",
            tk,
            p,
        )
    )

    # 4. The same address, as a folder and as a command
    y4 = y3 + h3 + 28
    h4 = 206
    parts.append(
        f'<rect x="{X0}" y="{y4}" width="{X1 - X0}" height="{h4}" rx="16" fill="{p["card"]}" '
        f'stroke="{root}" stroke-opacity="0.45" stroke-width="1.5"/>'
    )
    x = X0 + PAD
    parts.append(
        text(
            x,
            y4 + 32,
            "ONE ADDRESS, TWO WAYS TO WRITE IT",
            size=14,
            weight=700,
            fill=p["muted"],
            extra=' letter-spacing="1.5"',
        )
    )
    parts.append(text(x, y4 + 64, "the folder", size=15, fill=p["muted"]))
    parts.append(
        spans(
            x,
            y4 + 88,
            [
                ("pipelines/", root, 400),
                ("fraud_detection/", uc, 700),
                ("ingestion/", pl, 700),
                ("claim_etl/", tk, 700),
            ],
            size=18,
        )
    )
    parts.append(text(x, y4 + 122, "the command", size=15, fill=p["muted"]))
    parts.append(
        spans(
            x,
            y4 + 146,
            [
                ("ubunye run ", p["ink"], 400),
                ("-d pipelines", root, 400),
            ],
            size=18,
        )
    )
    parts.append(
        spans(
            x + 44,
            y4 + 174,
            [
                ("-u fraud_detection ", uc, 700),
                ("-p ingestion ", pl, 700),
                ("-t claim_etl", tk, 700),
            ],
            size=18,
        )
    )

    height = y4 + h4 + 20
    body = "\n".join(parts)
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {height}" '
        f'width="{W}" height="{height}" role="img" '
        f'aria-label="A use case folder holds pipelines; a pipeline runs its tasks in order; '
        f"a task is a folder with config.yaml and transformations.py. The folder path and the "
        f'ubunye run flags name the same task.">\n{body}\n</svg>\n'
    )


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for name, p in PALETTES.items():
        path = OUT / f"project-structure-{name}.svg"
        path.write_text(build(p), encoding="utf-8")
        print(path.relative_to(OUT.parents[2]))


if __name__ == "__main__":
    main()
