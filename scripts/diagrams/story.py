"""DRAFT story frames, keynote style: one idea, one hero picture, one technical line.

Writes drafts/story/NN-name.svg and drafts/story/README.md (all frames on one page).
Not used by the docs until approved.

    python scripts/diagrams/story.py
"""

from __future__ import annotations

from pathlib import Path
from xml.sax.saxutils import escape

from _svg import MONO
from stage import C, chip_icon, defs, icon, stage, t, task_files

OUT = Path(__file__).resolve().parents[2] / "drafts" / "story"
W, H = 1100, 640
OK, AMBER, RED = "#22c55e", "#f59e0b", "#ef4444"


def card(x, y, w, h, name="claim_etl/", captions=("what", "how")):
    return "\n".join(
        [
            f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="22" fill="{C["front"]}"/>',
            t(
                x + 24,
                y + 40,
                name,
                size=max(16, w // 12),
                weight=700,
                font=MONO,
                fill=C["card_ink"],
            ),
            task_files(x + 24, y + 62, w - 48, h - 120, captions=captions),
        ]
    )


def disc(cx, cy, r, ic, label, sub=None):
    out = [
        f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="{C["tile"]}"/>',
        icon(ic, cx - r * 0.55, cy - r * 0.55, r * 1.1),
        t(cx + r + 22, cy + (0 if sub else 8), label, size=23, weight=700),
    ]
    if sub:
        out.append(t(cx + r + 22, cy + 28, sub, size=16, font=MONO, fill=C["muted"]))
    return "\n".join(out)


def fan(spots):
    out = []
    for i, (ic, label, sub) in enumerate(spots):
        cy = 222 + i * 118
        out.append(curve(300, 330, 586, cy))
        out.append(disc(650, cy, 52, ic, label, sub))
    return "\n".join(out)


def curve(x1, y1, x2, y2, colour=None, sw=3):
    mx = (x1 + x2) / 2
    return (
        f'<path d="M{x1} {y1}C{mx} {y1} {mx} {y2} {x2} {y2}" fill="none" stroke="{colour or C["outline"]}" '
        f'stroke-width="{sw}" marker-end="url(#head)"/>'
    )


def paper(x, y, w, h):
    zig = "".join(f"l8 {7 if i % 2 == 0 else -7}" for i in range(int(w // 8)))
    return (
        f'<path d="M{x} {y}h{w}v{h}H{x}z" fill="{C["tile"]}"/><path d="M{x} {y + h}{zig}" fill="none" '
        f'stroke="{C["bg"]}" stroke-width="5"/>'
    )


def frame(title, sub, tech, body):
    tech_svg = "".join(
        f'<tspan fill="{C["ink"] if i % 2 == 0 else C["muted"]}">{escape(p)}</tspan>'
        for i, p in enumerate(tech)
    )
    return "\n".join(
        [
            f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" role="img" '
            f'aria-label="{escape(title + " " + sub)}">',
            "<desc>Logos: Simple Icons (CC0); marks belong to their owners.</desc>",
            defs(),
            stage(W, H),
            t(W / 2, 92, title, size=52, weight=700, anchor="middle"),
            t(W / 2, 134, sub, size=21, fill=C["muted"], anchor="middle"),
            body,
            f'<rect x="40" y="{H - 82}" width="{W - 80}" height="50" rx="25" fill="#26282f"/>',
            t(72, H - 51, "HOW", size=13, weight=700, fill=C["tk"], extra=' letter-spacing="2"'),
            f'<text x="124" y="{H - 51}" font-family="{MONO}" font-size="16" xml:space="preserve">{tech_svg}</text>',
            "</svg>",
        ]
    )


# ---------------------------------------------------------------- frames


def f_write():
    x, y, w, h = 400, 180, 300, 320
    body = [card(x, y, w, h)]
    body.append(f'<path d="M{x - 12} {y + 160}H{x - 70}" stroke="{C["yaml"]}" stroke-width="2"/>')
    body.append(t(x - 80, y + 154, "config.yaml", size=19, weight=700, font=MONO, anchor="end"))
    body.append(
        t(x - 80, y + 180, "what to read and write", size=17, fill=C["muted"], anchor="end")
    )
    body.append(
        f'<path d="M{x + w + 12} {y + 160}H{x + w + 70}" stroke="{C["py_band"]}" stroke-width="2"/>'
    )
    body.append(t(x + w + 80, y + 154, "transformations.py", size=19, weight=700, font=MONO))
    body.append(t(x + w + 80, y + 180, "how to change the data", size=17, fill=C["muted"]))
    return frame(
        "Write it once.",
        "A task is one folder. Settings in YAML, logic in Python.",
        [
            "use case / pipeline / task",
            "   ·   ",
            "ubunye run -u fraud_detection -p ingestion -t claim_etl",
        ],
        "\n".join(body),
    )


def f_anywhere():
    body = [card(90, 220, 210, 220)]
    body.append(
        fan(
            [
                ("pandas", "your laptop", "--backend pandas"),
                ("apachespark", "a Spark cluster", "spark"),
                ("databricks", "Databricks", "attaches to the notebook"),
            ]
        )
    )
    return frame(
        "Run it anywhere.",
        "The same folder, unchanged. Only the place changes.",
        [
            "--backend pandas | spark | databricks",
            "   ·   ",
            "ubunye deploy glue | dataproc | k8s | emr-serverless",
        ],
        "\n".join(body),
    )


def f_prove():
    body = []
    for i, where in enumerate(["laptop", "cloud", "Databricks"]):
        x = 160 + i * 290
        body.append(paper(x, 190, 210, 170))
        body.append(t(x + 22, 226, where, size=17, weight=700, fill=C["tile_muted"]))
        body.append(t(x + 22, 264, "940 rows", size=17, font=MONO, fill=C["tile_ink"]))
        body.append(t(x + 22, 318, "9f3a…c21", size=27, weight=700, font=MONO, fill="#0f766e"))
        body.append(
            f'<path d="M{x + 105} 372V408H550" fill="none" stroke="{OK}" stroke-width="3"/>'
        )
    body.append(f'<path d="M550 408V428" stroke="{OK}" stroke-width="3"/>')
    body.append(f'<circle cx="550" cy="468" r="38" fill="{OK}"/>')
    body.append(t(550, 484, "✓", size=42, weight=700, fill=C["bg"], anchor="middle"))
    body.append(t(606, 476, "same rows, everywhere", size=22, weight=700, fill=OK))
    return frame(
        "Prove it's the same.",
        "Every run leaves a receipt with a hash of every row.",
        [
            "ubunye run --lineage",
            "   ·   ",
            "rows hashed in any order",
            "   ·   ",
            "ubunye prove",
            "  ",
            "ubunye gate",
        ],
        "\n".join(body),
    )


def f_bad_data():
    body = []
    bad = {(1, 1), (3, 0), (0, 3), (4, 4)}
    for r in range(5):
        for c in range(5):
            fill = AMBER if (c, r) in bad else C["front"]
            body.append(
                f'<rect x="{110 + c * 40}" y="{210 + r * 44}" width="30" height="30" rx="6" fill="{fill}"/>'
            )
    body.append(curve(320, 320, 460, 320))
    body.append(icon("shield", 470, 240, 160))
    body.append(curve(640, 300, 760, 250, OK))
    body.append(curve(640, 340, 760, 420, AMBER))
    body.append(chip_icon("table", 780, 222, 56, pad=10))
    for i in range(4):
        body.append(
            f'<rect x="{866 + i * 36}" y="{236}" width="26" height="26" rx="5" fill="{C["front"]}"/>'
        )
    body.append(t(866, 300, "written", size=20, weight=700, fill=OK))
    body.append(chip_icon("bin", 780, 392, 56, pad=10))
    for i in range(2):
        body.append(
            f'<rect x="{866 + i * 36}" y="{406}" width="26" height="26" rx="5" fill="{AMBER}"/>'
        )
    body.append(t(866, 470, "quarantined", size=20, weight=700, fill=AMBER))
    return frame(
        "Bad data never lands.",
        "Every row is checked before anything is written.",
        [
            "CONFIG.expectations",
            "   ·   ",
            "not_null  unique  between  one_of",
            "   ·   ",
            "fail | quarantine | warn",
        ],
        "\n".join(body),
    )


def f_rerun():
    body = []
    # the table, written once
    tx, ty = 600, 200
    body.append(f'<rect x="{tx}" y="{ty}" width="320" height="260" rx="18" fill="{C["tile"]}"/>')
    body.append(
        t(tx + 24, ty + 40, "dt = 2026-11-10", size=20, weight=700, font=MONO, fill=C["tile_ink"])
    )
    for i in range(5):
        body.append(
            f'<rect x="{tx + 24}" y="{ty + 64 + i * 36}" width="272" height="26" rx="6" fill="{C["front"]}"/>'
        )
    # run 1 lands, run 2 is stopped
    for i, (label, colour) in enumerate([("run 1", OK), ("run 2", RED)]):
        y = 250 + i * 160
        body.append(
            f'<rect x="110" y="{y - 30}" width="150" height="60" rx="30" fill="{colour}" fill-opacity="0.18" '
            f'stroke="{colour}" stroke-width="2.5"/>'
        )
        body.append(
            t(185, y + 8, label, size=22, weight=700, font=MONO, fill=colour, anchor="middle")
        )
    body.append(curve(266, 250, tx - 10, 280, OK))
    body.append(f'<path d="M266 410H420" stroke="{RED}" stroke-width="3" stroke-dasharray="8 6"/>')
    body.append(chip_icon("lock", 432, 384, 52, pad=10))
    body.append(t(458, 480, "refused", size=19, weight=700, fill=RED, anchor="middle"))
    body.append(
        t(tx + 160, ty + 300, "the batch lands once", size=20, weight=700, fill=OK, anchor="middle")
    )
    return frame(
        "Safe to run again.",
        "A batch lands once, even after a crash.",
        [
            "one lease per batch (task + dt)",
            "   ·   ",
            "--rerun replaces",
            "   ·   ",
            "--resume finishes",
        ],
        "\n".join(body),
    )


def f_promote():
    body = []
    steps = [
        ("development", 170, 80, "#94a3b8"),
        ("staging", 450, 150, "#3b82f6"),
        ("production", 730, 220, OK),
    ]
    base = 500
    for name, x, h, colour in steps:
        body.append(
            f'<rect x="{x}" y="{base - h}" width="220" height="{h}" rx="14" fill="{colour}" fill-opacity="0.22" '
            f'stroke="{colour}" stroke-width="2.5"/>'
        )
        body.append(
            t(
                x + 110,
                base - 18,
                name,
                size=19,
                weight=700,
                font=MONO,
                fill=colour,
                anchor="middle",
            )
        )
    # the model card on each step
    for (name, x, h, _), (v, m) in zip(
        steps, [("1.3.0", "auc 0.91"), ("1.2.0", "auc 0.89"), ("1.1.0", "auc 0.88")]
    ):
        y = base - h - 96
        body.append(
            f'<rect x="{x + 30}" y="{y}" width="160" height="84" rx="14" fill="{C["front"]}"/>'
        )
        body.append(t(x + 50, y + 36, v, size=24, weight=700, font=MONO, fill=C["card_ink"]))
        body.append(t(x + 50, y + 66, m, size=16, font=MONO, fill=C["tile_muted"]))
    for x1, y1, x2, y2 in [(364, 362, 476, 300), (644, 292, 756, 230)]:
        body.append(curve(x1, y1, x2, y2, OK))
        body.append(icon("shield", (x1 + x2) / 2 - 22, min(y1, y2) - 30, 44))
    body.append(f'<circle cx="930" cy="{base - 220 - 70}" r="9" fill="{OK}"/>')
    return frame(
        "Promote with proof.",
        "A model moves up only when it passes its gates.",
        [
            "ubunye models promote --to production",
            "   ·   ",
            "gates: min_auc 0.85",
            "   ·   ",
            "rollback  archive",
        ],
        "\n".join(body),
    )


def f_schedule():
    body = [card(90, 220, 210, 220)]
    body.append(
        fan(
            [
                ("apacheairflow", "Airflow", "a DAG"),
                ("databricks", "Databricks", "a job.json"),
                ("apachespark", "Spark", "a Declarative Pipeline"),
            ]
        )
    )
    return frame(
        "Schedule it anywhere.",
        "One command writes the job for the scheduler you use.",
        [
            "ORCHESTRATION in config.yaml",
            "   ·   ",
            "ubunye export airflow | databricks | spark-pipeline",
        ],
        "\n".join(body),
    )


def f_agents():
    body = [f'<circle cx="250" cy="330" r="110" fill="{C["tile"]}"/>', icon("agent", 190, 270, 120)]
    body.append(t(250, 478, "an AI agent", size=21, weight=700, anchor="middle"))
    body.append(card(700, 200, 260, 260))
    body.append(f'<path d="M370 300H690" stroke="{OK}" stroke-width="3" marker-end="url(#head)"/>')
    body.append(t(530, 288, "reads: tasks, plans, runs, gates", size=17, fill=OK, anchor="middle"))
    body.append(
        f'<path d="M370 380H580" stroke="{AMBER}" stroke-width="3" stroke-dasharray="8 6"/>'
    )
    body.append(chip_icon("lock", 594, 358, 40, pad=8))
    body.append(t(530, 430, "runs: only if you allow it", size=17, fill=AMBER, anchor="middle"))
    return frame(
        "Let agents help, safely.",
        "Read-only by default. Running is opt in.",
        [
            "ubunye mcp -d pipelines",
            "   ·   ",
            "9 read-only tools",
            "   ·   ",
            "run only with --allow-run",
        ],
        "\n".join(body),
    )


FRAMES = [
    ("01-write-it-once", f_write),
    ("02-run-it-anywhere", f_anywhere),
    ("03-prove-its-the-same", f_prove),
    ("04-bad-data-never-lands", f_bad_data),
    ("05-safe-to-run-again", f_rerun),
    ("06-promote-with-proof", f_promote),
    ("07-schedule-it-anywhere", f_schedule),
    ("08-agents-safely", f_agents),
]


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Story frames (draft, for approval)",
        "",
        "One idea per frame, one hero picture, and the technical line underneath.",
        "",
    ]
    for name, fn in FRAMES:
        (OUT / f"{name}.svg").write_text(fn() + "\n", encoding="utf-8")
        lines += [f"![{name}]({name}.svg)", ""]
        print(f"drafts/story/{name}.svg")
    (OUT / "README.md").write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    main()
