"""Draw the connectors diagram (docs/assets/diagrams/connectors-*.svg).

inputs: -> your transform -> outputs:, labelled with the names you write in
config.yaml. Re-run after a change:

    python scripts/diagrams/connectors.py
"""

from __future__ import annotations

from _svg import MONO, arrow_down, rect, spans, svg, text, write

W = 720
X0, X1 = 20, W - 20
PAD = 22

# (format, what it reaches, also runs on pandas)
READERS = [
    ("hive", "Hive tables", False),
    ("jdbc", "Postgres, MySQL, Oracle, SQL Server", False),
    ("unity", "Databricks Unity Catalog", False),
    ("delta", "Delta Lake, by path or table", False),
    ("s3", "S3, ADLS, GCS, local files", True),
    ("rest_api", "any HTTP API", True),
    ("binary", "PDFs, images (read only)", False),
]
WRITERS = [r for r in READERS if r[0] != "binary"]

ACCENTS = {
    "light": {"in": "#0369a1", "out": "#15803d"},
    "dark": {"in": "#38bdf8", "out": "#4ade80"},
}


def io_panel(y, key, blurb, items, colour, p):
    rows = (len(items) + 1) // 2
    h = 96 + rows * 66
    out = [
        rect(
            X0,
            y,
            X1 - X0,
            h,
            fill=colour,
            stroke=colour,
            sw=2,
            rx=16,
            extra=f' fill-opacity="{p["tint"]}"',
        ),
        text(X0 + PAD, y + 38, key, size=24, weight=700, font=MONO, fill=colour),
        text(X0 + PAD, y + 66, blurb, size=16, fill=p["muted"]),
    ]
    cw = (X1 - X0 - 2 * PAD - 12) / 2
    for i, (name, what, pandas) in enumerate(items):
        x = X0 + PAD + (i % 2) * (cw + 12)
        iy = y + 84 + (i // 2) * 66
        out.append(
            rect(
                x,
                iy,
                cw,
                56,
                fill=p["card"],
                stroke=colour,
                sw=1.2,
                rx=10,
                extra=' stroke-opacity="0.45"',
            )
        )
        out.append(text(x + 14, iy + 24, name, size=17, weight=700, font=MONO, fill=p["ink"]))
        out.append(text(x + 14, iy + 45, what, size=14, fill=p["muted"]))
        if pandas:
            out.append(
                rect(
                    x + cw - 72,
                    iy + 9,
                    60,
                    22,
                    fill=p["accent"],
                    stroke="none",
                    rx=11,
                    extra=f' fill-opacity="{p["tint"] * 2}"',
                )
            )
            out.append(
                text(
                    x + cw - 42,
                    iy + 24,
                    "pandas",
                    size=12,
                    weight=700,
                    fill=p["accent"],
                    anchor="middle",
                )
            )
    return "\n".join(out), h


def build(p) -> str:
    parts = []

    y = 20
    panel, h = io_panel(
        y, "inputs:", "Read. The format: key picks the connector.", READERS, p["in"], p
    )
    parts.append(panel)

    a = y + h
    parts.append(arrow_down(W / 2, a + 6, a + 52, p["accent"]))
    parts.append(spans(W / 2 + 16, a + 34, [('sources["claims"]', p["muted"], 400)], size=15))

    # transform
    ty = a + 58
    th = 238
    parts.append(
        rect(
            X0,
            ty,
            X1 - X0,
            th,
            fill=p["accent"],
            stroke=p["accent"],
            sw=2,
            rx=16,
            extra=f' fill-opacity="{p["tint"]}"',
        )
    )
    parts.append(
        text(X0 + PAD, ty + 38, "transform", size=24, weight=700, font=MONO, fill=p["accent"])
    )
    parts.append(
        text(
            X0 + PAD,
            ty + 66,
            "Change the data. Your Python, in transformations.py.",
            size=16,
            fill=p["muted"],
        )
    )
    code = [
        "class ClaimEtl(Task):",
        "    def transform(self, sources):",
        '        claims = sources["claims"]',
        '        return {"clean": claims[claims.amount > 0]}',
    ]
    parts.append(
        rect(X0 + PAD, ty + 82, X1 - X0 - 2 * PAD, 100, fill=p["card"], stroke=p["line"], rx=10)
    )
    for i, line in enumerate(code):
        parts.append(
            text(
                X0 + PAD + 14,
                ty + 106 + i * 21,
                line,
                size=14,
                font=MONO,
                fill=p["ink"],
                extra=' xml:space="preserve"',
            )
        )
    parts.append(text(X0 + PAD, ty + 220, "runs on", size=15, fill=p["muted"]))
    x = X0 + PAD + 66
    for b in ["spark", "databricks", "pandas"]:
        w = len(b) * 9.2 + 22
        parts.append(
            rect(
                x,
                ty + 202,
                w,
                26,
                fill=p["card"],
                stroke=p["accent"],
                rx=13,
                extra=' stroke-opacity="0.6"',
            )
        )
        parts.append(
            text(
                x + w / 2,
                ty + 220,
                b,
                size=14,
                weight=700,
                font=MONO,
                fill=p["accent"],
                anchor="middle",
            )
        )
        x += w + 8

    a = ty + th
    parts.append(arrow_down(W / 2, a + 6, a + 52, p["accent"]))
    parts.append(spans(W / 2 + 16, a + 34, [('{"clean": df}', p["muted"], 400)], size=15))

    oy = a + 58
    panel, h = io_panel(oy, "outputs:", "Write. Same formats, same key.", WRITERS, p["out"], p)
    parts.append(panel)

    ly = oy + h + 34
    parts.append(
        rect(
            X0,
            ly - 17,
            60,
            22,
            fill=p["accent"],
            stroke="none",
            rx=11,
            extra=f' fill-opacity="{p["tint"] * 2}"',
        )
    )
    parts.append(
        text(X0 + 30, ly - 2, "pandas", size=12, weight=700, fill=p["accent"], anchor="middle")
    )
    parts.append(
        text(
            X0 + 72,
            ly - 1,
            "also works on the pandas backend (no Java). The rest need Spark.",
            size=15,
            fill=p["muted"],
        )
    )

    return svg(
        W,
        ly + 20,
        "Inputs are read by the connector named in format:, your transform in transformations.py "
        "changes the data on Spark, Databricks or pandas, and outputs are written the same way.",
        "\n".join(parts),
    )


if __name__ == "__main__":
    write("connectors", build, ACCENTS)
