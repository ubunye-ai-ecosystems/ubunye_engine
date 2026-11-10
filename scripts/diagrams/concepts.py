"""Draw the export, backend choice and profiles diagrams
(docs/assets/diagrams/export.svg, backends.svg, profiles.svg).

    python scripts/diagrams/concepts.py
"""

from __future__ import annotations

from _svg import MONO
from stage import C, box, flow_arrow, mini_task, t, tile, write_one

W = 1100
YES, NO = "#22c55e", "#a1a7b3"
PURPLE = "#a855f7"


def yaml_card(x, y, w, title, lines, colour, *, h=None, title_fill="#e5e7eb"):
    h = h or 52 + len(lines) * 26
    out = [
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="12" fill="{C["tile"]}"/>',
        f'<path d="M{x} {y + 12}a12 12 0 0 1 12-12h{w - 24}a12 12 0 0 1 12 12v22H{x}z" fill="{title_fill}"/>',
        t(x + 14, y + 23, title, size=14, weight=700, font=MONO, fill=C["tile_ink"]),
        f'<rect x="{x + 10}" y="{y + 42}" width="5" height="{h - 52}" rx="2.5" fill="{colour}"/>',
    ]
    for i, (key, value, hot) in enumerate(lines):
        yy = y + 64 + i * 26
        out.append(t(x + 26, yy, key, size=15, font=MONO, fill=C["tile_ink"]))
        out.append(
            t(
                x + w - 16,
                yy,
                value,
                size=15,
                weight=700,
                font=MONO,
                fill=hot or C["tile_ink"],
                anchor="end",
            )
        )
    return "\n".join(out)


def export() -> str:
    parts = []
    parts.append(
        yaml_card(
            36,
            70,
            300,
            "config.yaml",
            [
                ("ORCHESTRATION:", "", None),
                ("  type", "airflow", None),
                ("  schedule", "0 2 * * *", None),
                ("  retries", "3", None),
            ],
            PURPLE,
        )
    )
    parts.append(
        f'<rect x="420" y="146" width="200" height="56" rx="28" fill="{C["engine"]}" '
        f'stroke="{C["outline"]}" stroke-width="2"/>'
    )
    parts.append(t(520, 181, "ubunye export", size=18, weight=700, font=MONO, anchor="middle"))
    parts.append(flow_arrow(340, 174, 414, 174))
    outs = [
        (["apacheairflow"], "airflow", "an Airflow DAG"),
        (["databricks"], "databricks", "a job.json spec"),
        (["apachespark"], "spark-pipeline", "a Declarative Pipeline"),
    ]
    for i, (ic, name, sub) in enumerate(outs):
        y = 56 + i * 90
        parts.append(
            f'<path d="M620 174C670 174 680 {y + 34} 724 {y + 34}" fill="none" stroke="{C["outline"]}" '
            'stroke-width="2.5" marker-end="url(#head)"/>'
        )
        parts.append(tile(730, y, 330, 68, ic, name, sub=sub, size=18))
    parts.append(
        t(36, 330, "run on any scheduler; ubunye run ignores this block", size=15, fill=C["muted"])
    )
    return "\n".join(parts)


def diamond(cx, cy, lines, colour):
    r = 82
    out = [
        f'<path d="M{cx} {cy - r}L{cx + r * 1.35} {cy}L{cx} {cy + r}L{cx - r * 1.35} {cy}z" fill="{C["engine"]}" '
        f'stroke="{colour}" stroke-width="2.5"/>'
    ]
    for i, line in enumerate(lines):
        out.append(
            t(
                cx,
                cy - 4 + (i - (len(lines) - 1) / 2) * 22,
                line,
                size=15,
                weight=700,
                anchor="middle",
                font=(
                    MONO
                    if line.startswith("--")
                    else None or "Roboto, 'Segoe UI', Helvetica, Arial, sans-serif"
                ),
            )
        )
    return "\n".join(out)


def yes_no(x, y, word, colour, anchor="start"):
    return t(x, y, word, size=14, weight=700, fill=colour, anchor=anchor)


def backends() -> str:
    parts = []
    parts.append(mini_task(36, 60, "claim_etl", w=200))
    parts.append(t(36, 186, "config.yaml never", size=15, fill=C["muted"]))
    parts.append(t(36, 208, "names an engine", size=15, fill=C["muted"]))
    parts.append(flow_arrow(240, 106, 300, 106))
    d1 = (420, 106)
    parts.append(diamond(*d1, ["--backend", "given?"], C["outline"]))
    parts.append(flow_arrow(d1[0] + 112, d1[1], 640, d1[1], YES))
    parts.append(yes_no(560, d1[1] - 10, "yes", YES))
    for i, (ic, name) in enumerate(
        [("pandas", "pandas"), ("apachespark", "spark"), ("databricks", "databricks")]
    ):
        parts.append(tile(650 + i * 140, d1[1] - 27, 132, 54, [ic], name, size=14))
    parts.append(t(650, d1[1] + 52, "the one you named", size=15, fill=C["muted"]))

    d2 = (420, 300)
    parts.append(flow_arrow(d1[0], d1[1] + 84, d2[0], d2[1] - 88, NO))
    parts.append(yes_no(d1[0] + 10, d1[1] + 108, "no", NO))
    parts.append(diamond(*d2, ["inside a", "Spark session?"], C["outline"]))
    parts.append(flow_arrow(d2[0] + 112, d2[1], 640, d2[1], YES))
    parts.append(yes_no(560, d2[1] - 10, "yes", YES))
    parts.append(
        tile(
            650,
            d2[1] - 34,
            260,
            68,
            ["databricks"],
            "attach to it",
            sub="a notebook's session",
            size=16,
        )
    )
    parts.append(t(650, d2[1] + 60, "never created, never stopped", size=15, fill=C["muted"]))

    parts.append(flow_arrow(d2[0], d2[1] + 84, d2[0], 446, NO))
    parts.append(yes_no(d2[0] + 10, d2[1] + 106, "no", NO))
    parts.append(
        tile(
            d2[0] - 130,
            452,
            260,
            64,
            ["apachespark"],
            "spark",
            sub="a new session: the default",
            size=16,
        )
    )
    return "\n".join(parts)


def profiles() -> str:
    parts = []
    base = [("shuffle.partitions", "200", None), ("executor.memory", "4g", None)]
    parts.append(
        t(36, 52, "BASE", size=15, weight=700, fill=C["muted"], extra=' letter-spacing="1"')
    )
    parts.append(yaml_card(36, 70, 290, "ENGINE.spark_conf", base, C["muted"]))
    parts.append(t(366, 140, "+", size=40, weight=700, fill=C["muted"], anchor="middle"))

    parts.append(
        t(410, 52, "PROFILES", size=15, weight=700, fill=C["muted"], extra=' letter-spacing="1"')
    )
    sheets = [
        (
            "PROD",
            [("executor.memory", "32g", "#c2410c"), ("executor.cores", "8", "#c2410c")],
            "#f97316",
        ),
        ("STAGING", [("executor.memory", "8g", None)], "#3b82f6"),
        ("DEV", [("shuffle.partitions", "4", None), ("executor.memory", "512m", None)], "#94a3b8"),
    ]
    for i in reversed(range(3)):
        name, lines, colour = sheets[i]
        x, y = 410 + i * 26, 92 - i * 26 + 52
        parts.append(
            yaml_card(
                x,
                y,
                290,
                name,
                lines if i == 0 else [],
                colour,
                h=130,
                title_fill=colour if i == 0 else "#d1d5db",
            )
        )
    parts.append(
        f'<rect x="608" y="38" width="96" height="30" rx="15" fill="{C["engine"]}" stroke="#f97316" '
        'stroke-width="2"/>'
    )
    parts.append(
        t(656, 59, "-m PROD", size=15, weight=700, font=MONO, fill="#f97316", anchor="middle")
    )

    parts.append(flow_arrow(762, 210, 812, 210))
    parts.append(
        t(820, 52, "WHAT RUNS", size=15, weight=700, fill=C["muted"], extra=' letter-spacing="1"')
    )
    merged = [
        ("shuffle.partitions", "200", None),
        ("executor.memory", "32g", "#c2410c"),
        ("executor.cores", "8", "#c2410c"),
    ]
    parts.append(yaml_card(820, 140, 250, "merged", merged, "#f97316"))
    parts.append(box(36, 330, 1034, 56, rx=14, stroke="#4a4d57"))
    parts.append(t(60, 365, "-m prod", size=16, weight=700, font=MONO, fill="#ef4444"))
    parts.append(
        t(
            150,
            365,
            "✗ refused: names match exactly, and the error lists the profiles there are",
            size=16,
            fill=C["muted"],
        )
    )
    return "\n".join(parts)


if __name__ == "__main__":
    write_one(
        "export",
        W,
        330,
        "ubunye export reads the ORCHESTRATION block of config.yaml and writes an Airflow DAG, a "
        "Databricks job.json or a Spark Declarative Pipeline. ubunye run ignores the block.",
        export(),
    )
    write_one(
        "backends",
        W,
        540,
        "How a backend is chosen. config.yaml never names one. If --backend is given, that one runs. "
        "Otherwise, inside a Spark session such as a Databricks notebook, Ubunye attaches to it and "
        "never stops it. Otherwise a new Spark session, the default.",
        backends(),
    )
    write_one(
        "profiles",
        W,
        410,
        "Profiles: the base ENGINE.spark_conf plus the profile named by -m. With -m PROD, executor "
        "memory becomes 32g and executor cores 8, and shuffle partitions stay 200 from the base. "
        "-m prod is refused, because names match exactly.",
        profiles(),
    )
