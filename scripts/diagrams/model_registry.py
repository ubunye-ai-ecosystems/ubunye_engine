"""Draw the model registry diagram (docs/assets/diagrams/model-registry.svg).

Versions move development -> staging -> production through promotion gates;
promoting to production archives the old one; rollback brings one back.

    python scripts/diagrams/model_registry.py
"""

from __future__ import annotations

from _svg import MONO
from stage import C, arrow, box, chip_icon, icon, t, write_one

W, H = 1100, 640
DEV, STAGE, PROD, ARCH = "#94a3b8", "#3b82f6", "#22c55e", "#6c838d"
ROLL = "#ff9d5c"


def version_card(x, y, version, metric, *, fill=None, ink=None, live=False):
    out = [
        f'<rect x="{x}" y="{y}" width="190" height="96" rx="14" fill="{fill or C["front"]}" '
        f'stroke="{C["outline"]}" stroke-width="1.5"/>',
        t(x + 18, y + 38, version, size=24, weight=700, font=MONO, fill=ink or C["card_ink"]),
        t(x + 18, y + 70, metric, size=16, font=MONO, fill=ink or C["tile_muted"]),
    ]
    if live:
        out.append(f'<circle cx="{x + 162}" cy="{y + 30}" r="8" fill="{PROD}"/>')
        out.append(
            f'<circle cx="{x + 162}" cy="{y + 30}" r="13" fill="none" stroke="{PROD}" stroke-opacity="0.5" '
            'stroke-width="2"/>'
        )
    return "\n".join(out)


def stage_col(x, name, colour, cards):
    out = [
        f'<circle cx="{x + 10}" cy="{46}" r="8" fill="{colour}"/>',
        t(x + 28, 53, name, size=21, weight=700, fill=colour, font=MONO),
        box(x, 74, 260, 190, rx=20, stroke=colour),
    ]
    for i in reversed(range(len(cards))):
        v, m, live = cards[i]
        fill = C["front"] if i == 0 else C["mid"]
        out.append(version_card(x + 26 + i * 24, 140 - i * 24, v, m, fill=fill, live=live))
    return "\n".join(out)


def gate(x, y):
    return "\n".join(
        [
            icon("shield", x - 16, y - 58, 32),
            t(
                x,
                y + 46,
                "auc ≥ 0.85",
                size=14,
                weight=700,
                font=MONO,
                fill=C["muted"],
                anchor="middle",
            ),
            t(x, y + 28, "promote", size=15, weight=700, font=MONO, fill=PROD, anchor="middle"),
        ]
    )


def build() -> str:
    parts = []
    parts.append(stage_col(30, "development", DEV, [("1.3.0", "auc 0.91", False)]))
    parts.append(stage_col(420, "staging", STAGE, [("1.2.0", "auc 0.89", False)]))
    parts.append(stage_col(810, "production", PROD, [("1.1.0", "auc 0.88", True)]))

    # promote, through the gates
    for x1, x2 in [(300, 410), (690, 800)]:
        parts.append(arrow(x1, x2, 168, PROD, width=14))
        parts.append(gate((x1 + x2) / 2, 168))

    # demote, back down
    parts.append(
        f'<path d="M430 250C380 300 330 300 290 262" fill="none" stroke="{C["muted"]}" stroke-width="2.5" '
        'stroke-dasharray="7 6" marker-end="url(#head)"/>'
    )
    parts.append(
        t(360, 312, "demote", size=15, weight=700, font=MONO, fill=C["muted"], anchor="middle")
    )

    # archived, with replaced and rollback
    ax, ay = 590, 380
    parts.append(f'<circle cx="{ax + 10}" cy="{ay - 22}" r="8" fill="{ARCH}"/>')
    parts.append(t(ax + 28, ay - 15, "archived", size=21, weight=700, fill=C["muted"], font=MONO))
    parts.append(box(ax, ay, 480, 220, rx=20, stroke=ARCH, dash=True))
    for i, (v, m) in enumerate([("1.0.0", "auc 0.86"), ("0.9.0", "auc 0.84")]):
        parts.append(version_card(ax + 30 + i * 220, ay + 62, v, m, fill=C["back"], ink=C["ink"]))
    parts.append(t(ax + 30, ay + 196, "kept for audit", size=15, fill=C["muted"]))
    # production -> archived when replaced
    parts.append(
        f'<path d="M960 264V{ay - 6}" stroke="{C["muted"]}" stroke-width="2.5" marker-end="url(#head)"/>'
    )
    parts.append(t(972, 330, "replaced", size=15, weight=700, font=MONO, fill=C["muted"]))
    # rollback: an archived version back to production
    parts.append(
        f'<path d="M{ax + 125} {ay + 60}C{ax + 125} 300 830 330 860 270" fill="none" stroke="{ROLL}" '
        'stroke-width="3" marker-end="url(#head)"/>'
    )
    parts.append(t(ax + 150, 318, "rollback", size=15, weight=700, font=MONO, fill=ROLL))

    # any library
    parts.append(box(30, ay, 520, 220, rx=20, stroke="#4a4d57"))
    parts.append(t(52, ay + 40, "ANY LIBRARY", size=20, weight=700, extra=' letter-spacing="1"'))
    for i, ic in enumerate(["scikitlearn", "pytorch", "apachespark", "python"]):
        parts.append(chip_icon(ic, 66 + i * 70, ay + 72, 36))
    parts.append(t(52, ay + 160, "UbunyeModel", size=20, weight=700, font=MONO, fill=C["front"]))
    parts.append(t(52, ay + 188, "your model class, saved in any format", size=15, fill=C["muted"]))
    parts.append(chip_icon("mlflow", 470, ay + 26, 26, pad=5))
    parts.append(t(483, ay + 80, "or MLflow", size=13, fill=C["muted"], anchor="middle"))
    return "\n".join(parts)


if __name__ == "__main__":
    write_one(
        "model-registry",
        W,
        H,
        "A model version moves from development to staging to production by promote, through "
        "promotion gates such as auc at least 0.85. Demote moves it back. Promoting to production "
        "archives the version it replaces, and rollback brings an archived version back. Models "
        "from any library fit, through the UbunyeModel contract, stored by Ubunye or MLflow.",
        build(),
    )
