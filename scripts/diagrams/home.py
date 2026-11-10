"""Draw the home page diagram (docs/assets/diagrams/home-*.svg).

The story in four steps: write a task once, run it anywhere unchanged, every run
leaves a record, `ubunye prove` checks the records match. Re-run after a change:

    python scripts/diagrams/home.py
"""

from __future__ import annotations

from _svg import MONO, arrow_down, rect, spans, svg, text, write

W = 720
X0, X1 = 20, W - 20
GAP = 14
COL_W = (X1 - X0 - 2 * GAP) / 3
COLS = [X0 + i * (COL_W + GAP) for i in range(3)]

TARGETS = [
    ("Your laptop", ["pandas (no Java)", "local Spark"]),
    (
        "Your cloud",
        ["AWS Glue", "EMR Serverless", "GCP Dataproc", "Azure Container Apps", "Kubernetes"],
    ),
    ("Databricks", ["jobs", "notebooks", "serverless"]),
]
SCHEDULE = ["Airflow", "Databricks jobs", "Spark Declarative Pipelines"]
WATCH = ["Prometheus", "OpenTelemetry", "MLflow", "OpenLineage"]
HASH = "9f3a…c21"


def step(y, n, title, sub, p):
    x = X0
    return "\n".join(
        [
            f'<circle cx="{x + 15}" cy="{y + 15}" r="15" fill="{p["accent"]}"/>',
            text(x + 15, y + 21, str(n), size=17, weight=700, fill=p["on_badge"], anchor="middle"),
            text(x + 42, y + 22, title, size=22, weight=700, fill=p["ink"]),
            text(x + 42, y + 48, sub, size=16, fill=p["muted"]),
        ]
    )


def pills(x, y, items, p, *, size=15):
    """A row of small labels; returns (svg, x after the last one)."""
    out = []
    for s in items:
        w = len(s) * size * 0.53 + 24
        out.append(rect(x, y, w, 30, fill=p["card"], stroke=p["line"], rx=15))
        out.append(text(x + w / 2, y + 20, s, size=size, fill=p["ink"], anchor="middle"))
        x += w + 8
    return "\n".join(out), x


def build(p) -> str:
    parts = []

    # 1. Write a task once
    y = 20
    parts.append(
        step(y, 1, "Write a task once", "A folder with a config.yaml and a transformations.py.", p)
    )
    cx, cw = W / 2, 340
    cy = y + 70
    parts.append(rect(cx - cw / 2, cy, cw, 70, fill=p["card"], stroke=p["accent"], sw=2))
    parts.append(
        text(
            cx,
            cy + 30,
            "claim_etl/",
            size=21,
            weight=700,
            font=MONO,
            fill=p["ink"],
            anchor="middle",
        )
    )
    parts.append(
        text(
            cx,
            cy + 54,
            "config.yaml  +  transformations.py",
            size=15,
            font=MONO,
            fill=p["muted"],
            anchor="middle",
        )
    )
    a1 = cy + 70
    parts.append(arrow_down(cx, a1 + 6, a1 + 58, p["accent"]))
    parts.append(
        spans(
            cx + 16,
            a1 + 38,
            [
                ("ubunye run", p["accent"], 700),
                ("  or  ", p["muted"], 400),
                ("ubunye deploy", p["accent"], 700),
            ],
            size=15,
        )
    )

    # 2. Run it anywhere
    y = a1 + 72
    parts.append(
        step(
            y, 2, "Run it anywhere, unchanged", "Same files, same rows. Only the place changes.", p
        )
    )
    ty = y + 70
    col_h = 52 + 5 * 42 + 6
    for (title, items), x in zip(TARGETS, COLS):
        parts.append(
            rect(
                x,
                ty,
                COL_W,
                col_h,
                fill=p["accent"],
                stroke=p["accent"],
                sw=1.5,
                rx=14,
                extra=f' fill-opacity="{p["tint"]}" stroke-opacity="0.5"',
            )
        )
        parts.append(
            text(
                x + COL_W / 2,
                ty + 32,
                title,
                size=18,
                weight=700,
                fill=p["accent"],
                anchor="middle",
            )
        )
        for i, item in enumerate(items):
            iy = ty + 50 + i * 42
            parts.append(rect(x + 12, iy, COL_W - 24, 34, fill=p["card"], stroke=p["line"], rx=8))
            parts.append(
                text(x + COL_W / 2, iy + 23, item, size=15, fill=p["ink"], anchor="middle")
            )

    # 3. Every run leaves a record
    r_top = ty + col_h
    for x in COLS:
        parts.append(arrow_down(x + COL_W / 2, r_top + 6, r_top + 46, p["muted"], sw=2))
    y = r_top + 56
    parts.append(
        step(y, 3, "Every run leaves a record", "It counts the rows and hashes every one.", p)
    )
    ry = y + 70
    rh = 84
    for where, x in zip(["laptop", "cloud", "Databricks"], COLS):
        rx_ = x + 18
        rw = COL_W - 36
        parts.append(
            rect(
                rx_,
                ry,
                rw,
                rh,
                fill=p["card"],
                stroke=p["line"],
                rx=6,
                extra=' stroke-dasharray="0"',
            )
        )
        parts.append(
            f'<line x1="{rx_}" y1="{ry + 26}" x2="{rx_ + rw}" y2="{ry + 26}" '
            f'stroke="{p["line"]}" stroke-dasharray="4 4"/>'
        )
        parts.append(
            text(
                rx_ + 12,
                ry + 18,
                f"run record: {where}",
                size=13,
                weight=700,
                fill=p["muted"],
            )
        )
        parts.append(text(rx_ + 12, ry + 48, "row hash", size=14, fill=p["muted"]))
        parts.append(text(rx_ + 12, ry + 72, HASH, size=18, weight=700, font=MONO, fill=p["ink"]))

    # 4. Prove they match
    b = ry + rh
    mid_y = b + 26
    xs = [x + COL_W / 2 for x in COLS]
    for x in xs:
        parts.append(
            f'<line x1="{x}" y1="{b}" x2="{x}" y2="{mid_y}" stroke="{p["ok"]}" stroke-width="2"/>'
        )
    parts.append(
        f'<line x1="{xs[0]}" y1="{mid_y}" x2="{xs[2]}" y2="{mid_y}" stroke="{p["ok"]}" stroke-width="2"/>'
    )
    parts.append(arrow_down(W / 2, mid_y, mid_y + 30, p["ok"], sw=2))
    py_ = mid_y + 34
    pw = 470
    parts.append(
        rect(
            W / 2 - pw / 2,
            py_,
            pw,
            48,
            fill=p["ok"],
            stroke=p["ok"],
            rx=24,
            extra=f' fill-opacity="{p["tint"] * 1.6}"',
        )
    )
    parts.append(
        spans(
            W / 2,
            py_ + 31,
            [
                ("✓ ", p["ok"], 700),
                ("ubunye prove", p["ok"], 700),
                ("  same rows on every target", p["ink"], 400),
            ],
            size=17,
            anchor="middle",
        )
    )

    # Plugs into what you already use
    sy = py_ + 48 + 30
    sh = 156
    parts.append(rect(X0, sy, X1 - X0, sh, fill=p["card"], stroke=p["line"], rx=14))
    parts.append(
        text(
            X0 + 20,
            sy + 30,
            "PLUGS INTO WHAT YOU ALREADY USE",
            size=14,
            weight=700,
            fill=p["muted"],
            extra=' letter-spacing="1.5"',
        )
    )
    parts.append(
        spans(
            X0 + 20,
            sy + 62,
            [("schedule  ", p["ink"], 700), ("ubunye export", p["muted"], 400)],
            size=15,
        )
    )
    row, _ = pills(X0 + 20, sy + 72, SCHEDULE, p)
    parts.append(row)
    parts.append(text(X0 + 20, sy + 128, "watch", size=15, weight=700, font=MONO, fill=p["ink"]))
    row, _ = pills(X0 + 90, sy + 108, WATCH, p)
    parts.append(row)

    height = sy + sh + 20
    return svg(
        W,
        height,
        "Write a task once, run it unchanged on your laptop, your cloud or Databricks; every run "
        "leaves a record with a hash of every row, and ubunye prove checks the rows match.",
        "\n".join(parts),
    )


if __name__ == "__main__":
    write("home", build)
