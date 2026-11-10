"""Draw the home page diagram (docs/assets/diagrams/home.svg).

Your task -> the engine (read, transform, write, record) -> runs on any of these.
Underneath: the records from every place match (ubunye prove), and what it plugs into.

    python scripts/diagrams/home.py
"""

from __future__ import annotations

from _svg import MONO
from stage import C, arrow, box, chip_icon, heading, receipt, spans, t, task_files, tile, write_one

W, H = 1100, 700

TARGETS = [
    (["pandas"], "pandas"),
    (["apachespark"], "Spark"),
    (["databricks"], "Databricks"),
    (["kubernetes"], "Kubernetes"),
    (["aws"], "AWS"),
    (["googlecloud"], "GCP"),
    (["azure"], "Azure"),
    (["docker"], "Docker"),
]
PLUGS = [
    ("apacheairflow", "Airflow"),
    ("prometheus", "Prometheus"),
    ("opentelemetry", "OpenTelemetry"),
    ("mlflow", "MLflow"),
    ("openlineage", "OpenLineage"),
    ("databricks", "Databricks jobs"),
]


def step_card(x, y, w, h, label, glyph):
    return "\n".join(
        [
            f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="12" fill="{C["step"]}"/>',
            t(x + 18, y + h / 2 + 7, glyph, size=18, weight=700, font=MONO, fill=C["muted"]),
            t(x + 64, y + h / 2 + 7, label, size=19, weight=700),
        ]
    )


def build() -> str:
    parts = []

    # your task
    parts.append(heading(30, 52, None, "YOUR TASK", C["ink"]))
    tx, ty, tw, th = 30, 80, 250, 300
    parts.append(box(tx, ty, tw, th, rx=16, fill=C["front"], sw=1.5))
    parts.append(
        t(tx + 18, ty + 32, "claim_etl/", size=20, weight=700, font=MONO, fill=C["card_ink"])
    )
    parts.append(task_files(tx + 18, ty + 56, tw - 36, 190))

    parts.append(arrow(292, 382, 230, C["outline"]))
    parts.append(t(337, 272, "ubunye run", size=14, font=MONO, fill=C["muted"], anchor="middle"))

    # the engine
    parts.append(heading(392, 52, None, "UBUNYE ENGINE", C["ink"]))
    ex, ey, ew, eh = 392, 80, 290, 300
    parts.append(box(ex, ey, ew, eh, rx=28, fill=C["engine"], stroke="#4a4d57"))
    sx, sw, sh = ex + 30, ew - 60, 50
    for i, (label, glyph) in enumerate([("read", "in"), ("transform", "</>"), ("write", "out")]):
        sy = ey + 26 + i * 68
        parts.append(step_card(sx, sy, sw, sh, label, glyph))
        if i < 2:
            parts.append(
                t(ex + ew / 2, sy + sh + 15, "↓", size=16, fill=C["muted"], anchor="middle")
            )
    parts.append(receipt(sx, ey + 228, sw, 56, "run record", "rows + row hash"))

    parts.append(arrow(694, 784, 230, C["accent"]))
    parts.append(t(739, 272, "deploy", size=14, font=MONO, fill=C["muted"], anchor="middle"))

    # where it runs
    parts.append(heading(796, 52, None, "RUNS ON", C["ink"]))
    gx, gy, cw, ch = 796, 80, 132, 63
    for i, (icons, name) in enumerate(TARGETS):
        x = gx + (i % 2) * (cw + 10)
        y = gy + (i // 2) * (ch + 16)
        parts.append(tile(x, y, cw, ch, icons, name, size=14))

    # same rows everywhere
    by = 420
    parts.append(box(30, by, 520, 250, rx=18, stroke="#4a4d57"))
    parts.append(heading(52, by + 40, None, "SAME ROWS, EVERYWHERE", C["ink"]))
    places = [("pandas", "laptop"), ("googlecloud", "cloud"), ("databricks", "Databricks")]
    for i, (ic, where) in enumerate(places):
        x = 52 + i * 160
        parts.append(chip_icon(ic, x + 60, by + 62, 24))
        parts.append(receipt(x, by + 98, 146, 58, where, "9f3a…c21"))
    parts.append(
        f'<path d="M125 {by + 166}v18H445v-18M285 {by + 184}v12" fill="none" stroke="{C["ok"]}" stroke-width="2.5"/>'
    )
    pw = 300
    parts.append(
        f'<rect x="{285 - pw / 2}" y="{by + 198}" width="{pw}" height="40" rx="20" fill="{C["ok"]}" '
        f'fill-opacity="0.16" stroke-width="2" stroke="{C["ok"]}"/>'
    )
    parts.append(spans(285, by + 225, [("✓ ubunye prove", C["ok"], 700)], size=19, anchor="middle"))

    # plugs into
    parts.append(box(570, by, 500, 250, rx=18, stroke="#4a4d57"))
    parts.append(heading(592, by + 40, None, "PLUGS INTO", C["ink"]))
    for i, (ic, name) in enumerate(PLUGS):
        x = 592 + (i % 2) * 234
        y = by + 62 + (i // 2) * 60
        parts.append(tile(x, y, 222, 48, [ic], name, size=16))
    return "\n".join(parts)


if __name__ == "__main__":
    write_one(
        "home",
        W,
        H,
        "Your task, a config.yaml and a transformations.py, goes into the Ubunye engine, which reads, "
        "transforms, writes and keeps a run record. It runs on pandas, Spark, Databricks, Kubernetes, "
        "AWS, GCP, Azure or Docker. The run records from every place match, checked by ubunye prove. "
        "It plugs into Airflow, Prometheus, OpenTelemetry, MLflow, OpenLineage and Databricks jobs.",
        build(),
    )
