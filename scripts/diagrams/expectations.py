"""Draw the expectations diagram (docs/assets/diagrams/expectations.svg).

Rows leave the transform, pass the rules, and go one of four ways by severity.
Underneath: reconcile (nothing lost) and the quarantine rate that stops a run.

    python scripts/diagrams/expectations.py
"""

from __future__ import annotations

from _svg import MONO
from stage import C, arrow, box, chip_icon, heading, icon, t, write_one

W, H = 1100, 660

GOOD, BAD, WARN, FAIL = "#cdeefb", "#f59e0b", "#eab308", "#ef4444"
OK = "#22c55e"
# which rows are bad (quarantine) or doubtful (warn) in the 6 x 7 grid
BAD_AT = {(1, 2), (4, 0), (2, 5), (5, 4)}
WARN_AT = {(0, 6), (3, 3)}
RULES = ["not_null", "unique", "between", "one_of", "matches", "row_count"]
LANES = [
    (OK, "pass", "table", "clean_transactions", "good rows are written", GOOD, 3),
    (
        BAD,
        "quarantine",
        "bin",
        "quarantined_transactions",
        "bad rows + the rules they broke",
        BAD,
        2,
    ),
    (WARN, "warn", "log", "kept", "the row stays, the breach is logged", WARN, 1),
    (FAIL, "fail", "stop", "run stops", "nothing is written, for any output", None, 0),
]


def squares(x, y, colour, n, size=16, gap=6):
    return "".join(
        f'<rect x="{x + i * (size + gap)}" y="{y}" width="{size}" height="{size}" rx="3" fill="{colour}"/>'
        for i in range(n)
    )


def build() -> str:
    parts = []

    # rows coming out of the transform
    parts.append(heading(30, 52, None, "ROWS OUT OF", C["ink"]))
    parts.append(t(30, 78, "the transform", size=16, fill=C["muted"]))
    gx, gy = 40, 110
    for r in range(7):
        for c in range(6):
            fill = BAD if (c, r) in BAD_AT else WARN if (c, r) in WARN_AT else GOOD
            parts.append(
                f'<rect x="{gx + c * 32}" y="{gy + r * 34}" width="24" height="24" rx="4" fill="{fill}"/>'
            )
    parts.append(arrow(246, 300, 222, C["outline"], width=12))

    # the rules
    parts.append(heading(310, 52, None, "EXPECTATIONS", C["ink"]))
    parts.append(t(310, 78, "before anything is written", size=16, fill=C["muted"]))
    bx, by, bw, bh = 310, 96, 200, 262
    parts.append(box(bx, by, bw, bh, rx=22, fill=C["engine"], stroke="#4a4d57"))
    parts.append(icon("shield", bx + bw / 2 - 18, by + 14, 36))
    for i, rule in enumerate(RULES):
        ry = by + 62 + i * 32
        parts.append(
            f'<rect x="{bx + 22}" y="{ry}" width="{bw - 44}" height="26" rx="8" fill="{C["step"]}"/>'
        )
        parts.append(t(bx + bw / 2, ry + 18, rule, size=15, weight=700, font=MONO, anchor="middle"))

    # four ways out
    lx, lw, lh = 620, 450, 70
    for i, (colour, sev, ic, name, sub, sq, n) in enumerate(LANES):
        ly = 62 + i * 82
        cy = ly + lh / 2
        sy = by + 60 + i * 50
        parts.append(
            f'<path d="M{bx + bw} {sy} C{bx + bw + 50} {sy} {lx - 70} {cy} {lx - 12} {cy}" fill="none" '
            f'stroke="{colour}" stroke-width="3" marker-end="url(#head)"/>'
        )
        parts.append(
            f'<rect x="{lx}" y="{ly}" width="{lw}" height="{lh}" rx="12" fill="{C["tile"]}"/>'
        )
        parts.append(f'<rect x="{lx}" y="{ly}" width="7" height="{lh}" rx="3" fill="{colour}"/>')
        parts.append(icon(ic, lx + 20, ly + 19, 32))
        parts.append(t(lx + 66, ly + 30, name, size=16, weight=700, font=MONO, fill=C["tile_ink"]))
        parts.append(t(lx + 66, ly + 53, sub, size=14, fill=C["tile_muted"]))
        pw = len(sev) * 9 + 20
        parts.append(
            f'<rect x="{lx + lw - 14 - pw}" y="{ly + 10}" width="{pw}" height="24" rx="12" fill="{colour}"/>'
        )
        parts.append(
            t(
                lx + lw - 14 - pw / 2,
                ly + 27,
                sev,
                size=14,
                weight=700,
                font=MONO,
                fill=C["bg"],
                anchor="middle",
            )
        )
        if sq:
            parts.append(squares(lx + lw - 14 - n * 22 + 6, ly + 42, sq, n))

    # reconcile
    py = 420
    parts.append(box(30, py, 520, 210, rx=18, stroke="#4a4d57"))
    parts.append(heading(52, py + 38, None, "RECONCILE: NOTHING LOST", C["ink"]))
    bar_x, full = 160, 360
    parts.append(chip_icon("table", 60, py + 72, 22, pad=5))
    parts.append(t(98, py + 90, "in", size=16, weight=700, font=MONO, fill=C["muted"]))
    parts.append(
        f'<rect x="{bar_x}" y="{py + 70}" width="{full}" height="28" rx="6" fill="{GOOD}"/>'
    )
    parts.append(
        t(bar_x + 14, py + 90, "1000 rows", size=15, weight=700, font=MONO, fill=C["card_ink"])
    )
    parts.append(chip_icon("table", 60, py + 122, 22, pad=5))
    parts.append(t(98, py + 140, "out", size=16, weight=700, font=MONO, fill=C["muted"]))
    out = full * 0.9
    parts.append(
        f'<rect x="{bar_x}" y="{py + 120}" width="{out}" height="28" rx="6" fill="{GOOD}"/>'
    )
    parts.append(
        t(bar_x + 14, py + 140, "900 rows", size=15, weight=700, font=MONO, fill=C["card_ink"])
    )
    parts.append(
        f'<rect x="{bar_x + out + 4}" y="{py + 120}" width="{full - out - 4}" height="28" rx="6" fill="none" '
        f'stroke="{FAIL}" stroke-width="2" stroke-dasharray="5 4"/>'
    )
    parts.append(
        t(
            bar_x + full,
            py + 182,
            "✗ 100 lost: the run fails",
            size=16,
            weight=700,
            fill=FAIL,
            anchor="end",
        )
    )

    # quarantine rate
    parts.append(box(570, py, 500, 210, rx=18, stroke="#4a4d57"))
    parts.append(heading(592, py + 38, None, "TOO MANY BAD ROWS", C["ink"]))
    mx, mw, my = 600, 440, py + 90
    parts.append(f'<rect x="{mx}" y="{my}" width="{mw}" height="30" rx="15" fill="{C["step"]}"/>')
    parts.append(f'<rect x="{mx}" y="{my}" width="{mw * 0.6}" height="30" rx="15" fill="{BAD}"/>')
    mark = mx + mw * 0.5
    parts.append(f'<path d="M{mark} {my - 14}V{my + 44}" stroke="{FAIL}" stroke-width="3"/>')
    parts.append(
        t(
            mark,
            my - 22,
            "max_quarantine_rate",
            size=14,
            weight=700,
            font=MONO,
            fill=FAIL,
            anchor="middle",
        )
    )
    parts.append(
        t(mx, my + 72, "past the line: the run stops, nothing is written", size=16, fill=C["muted"])
    )
    return "\n".join(parts)


if __name__ == "__main__":
    write_one(
        "expectations",
        W,
        H,
        "Rows from the transform pass expectation rules before anything is written. Good rows are "
        "written; quarantine rows go to a quarantine output with the rules they broke; warn rows "
        "stay and the breach is logged; a fail rule stops the run and nothing is written. "
        "Reconcile fails a run that loses rows between input and output, and too high a quarantine "
        "rate stops the run.",
        build(),
    )
