"""Draw the run record diagram (docs/assets/diagrams/run-record.svg).

Left: what one receipt holds. Right: one hash over every row (order free), and
`ubunye gate` comparing two receipts.

    python scripts/diagrams/run_record.py
"""

from __future__ import annotations

from _svg import MONO
from stage import C, box, folder, heading, icon, t, write_one

W, H = 1100, 650
GOOD, OK, FAIL = "#cdeefb", "#22c55e", "#ef4444"


def paper(x, y, w, h):
    zig = "".join(f"l7 {6 if i % 2 == 0 else -6}" for i in range(int(w // 7)))
    return f'<path d="M{x} {y}h{w}v{h}H{x}z" fill="{C["tile"]}"/>' + (
        f'<path d="M{x} {y + h}{zig}" fill="none" stroke="{C["bg"]}" stroke-width="4"/>'
    )


def build() -> str:
    parts = []

    # the receipt
    parts.append(heading(30, 52, None, "EVERY RUN LEAVES A RECEIPT", C["ink"]))
    rx, ry, rw, rh = 50, 80, 440, 460
    parts.append(paper(rx, ry, rw, rh))
    parts.append(
        t(rx + 24, ry + 40, "claim_etl", size=22, weight=700, font=MONO, fill=C["tile_ink"])
    )
    parts.append(t(rx + 24, ry + 64, "run 7c2e41", size=15, font=MONO, fill=C["tile_muted"]))
    parts.append(
        f'<path d="M{rx + 20} {ry + 84}H{rx + rw - 20}" stroke="#cbd5e1" stroke-dasharray="5 5"/>'
    )
    rows = [
        ("s3", "read", "claims", "1000 rows"),
        ("table", "wrote", "clean", "940 rows"),
        (None, "", "row hash", "9f3a…c21"),
        ("shield", "checks", "expectations", "✓ passed"),
        ("pandas", "where", "pandas, DEV", "ubunye 0.9.0"),
        (None, "time", "", "4.2 s"),
    ]
    y = ry + 120
    for ic, label, name, value in rows:
        if ic:
            parts.append(icon(ic, rx + 24, y - 20, 26))
        if label:
            parts.append(
                t(
                    rx + 62,
                    y,
                    label,
                    size=14,
                    weight=700,
                    fill=C["tile_muted"],
                    extra=' letter-spacing="1"',
                )
            )
        strong = name == "row hash"
        parts.append(
            t(
                rx + 150,
                y,
                name,
                size=17,
                font=MONO,
                fill=C["tile_ink"],
                weight=700 if strong else 400,
            )
        )
        parts.append(
            t(
                rx + rw - 24,
                y,
                value,
                size=19 if strong else 17,
                font=MONO,
                fill="#0f766e" if strong else C["tile_ink"],
                weight=700,
                anchor="end",
            )
        )
        y += 58 if label or not strong else 50
    parts.append(folder(rx, ry + rh + 32, 30, C["muted"]))
    parts.append(
        t(
            rx + 42,
            ry + rh + 52,
            ".ubunye/lineage/  (--lineage)",
            size=16,
            font=MONO,
            fill=C["muted"],
        )
    )

    # one hash for every row
    parts.append(box(560, 30, 510, 270, rx=18, stroke="#4a4d57"))
    parts.append(heading(582, 68, None, "ONE HASH FOR EVERY ROW", C["ink"]))
    sx, sy = 590, 92
    hashes = ["a1", "7f", "3c", "e9", "52"]
    for i, h in enumerate(hashes):
        yy = sy + i * 34
        for c in range(4):
            parts.append(
                f'<rect x="{sx + c * 26}" y="{yy}" width="22" height="22" rx="4" fill="{GOOD}"/>'
            )
        parts.append(
            f'<path d="M{sx + 108} {yy + 11}H{sx + 140}" stroke="{C["muted"]}" stroke-width="2"/>'
        )
        parts.append(
            f'<rect x="{sx + 144}" y="{yy}" width="46" height="22" rx="11" fill="{C["step"]}"/>'
        )
        parts.append(t(sx + 167, yy + 16, "#" + h, size=13, weight=700, font=MONO, anchor="middle"))
        parts.append(
            f'<path d="M{sx + 190} {yy + 11}L{sx + 240} {sy + 79}" stroke="{C["muted"]}" stroke-width="1.5"/>'
        )
    parts.append(
        f'<circle cx="{sx + 262}" cy="{sy + 79}" r="22" fill="{C["step"]}" stroke="{C["outline"]}"/>'
    )
    parts.append(t(sx + 262, sy + 88, "+", size=26, weight=700, anchor="middle"))
    parts.append(
        f'<path d="M{sx + 284} {sy + 79}H{sx + 318}" stroke="{C["outline"]}" stroke-width="2.5" '
        'marker-end="url(#head)"/>'
    )
    parts.append(
        f'<rect x="{sx + 322}" y="{sy + 61}" width="136" height="36" rx="8" fill="{C["tile"]}"/>'
    )
    parts.append(
        t(
            sx + 390,
            sy + 85,
            "9f3a…c21",
            size=17,
            weight=700,
            font=MONO,
            fill="#0f766e",
            anchor="middle",
        )
    )
    parts.append(t(sx + 2, 282, "↻ rows in any order: same hash", size=15, fill=OK))
    parts.append(t(sx + 262, 282, "✎ one cell changed: new hash", size=15, fill="#fbbf24"))

    # compare two runs
    parts.append(box(560, 320, 510, 300, rx=18, stroke="#4a4d57"))
    parts.append(heading(582, 358, None, "COMPARE TWO RUNS", C["ink"]))
    for i, (who, hsh, colour) in enumerate(
        [("base", "9f3a…c21", "#0f766e"), ("this PR", "b71d…04e", "#b45309")]
    ):
        y = 386 + i * 104
        parts.append(paper(584, y, 170, 80))
        parts.append(t(598, y + 26, who, size=14, weight=700, fill=C["tile_muted"]))
        parts.append(t(598, y + 56, hsh, size=17, weight=700, font=MONO, fill=colour))
        parts.append(
            f'<path d="M760 {y + 40}C790 {y + 40} 790 466 812 466" fill="none" stroke="{C["outline"]}" '
            'stroke-width="2.5" marker-end="url(#head)"/>'
        )
    parts.append(
        f'<rect x="816" y="436" width="110" height="60" rx="30" fill="{C["engine"]}" '
        f'stroke="{C["outline"]}" stroke-width="2"/>'
    )
    parts.append(
        t(871, 461, "ubunye", size=14, weight=700, font=MONO, anchor="middle", fill=C["muted"])
    )
    parts.append(t(871, 482, "gate", size=18, weight=700, font=MONO, anchor="middle"))
    for i, (mark, label, colour, sub) in enumerate(
        [("✓", "pass", OK, "same data"), ("✗", "fail", FAIL, "changed, same VERSION")]
    ):
        y = 396 + i * 110
        parts.append(
            f'<path d="M926 466C944 466 940 {y + 30} 948 {y + 30}" fill="none" stroke="{colour}" '
            'stroke-width="2.5" marker-end="url(#head)"/>'
        )
        parts.append(
            f'<rect x="954" y="{y}" width="104" height="60" rx="12" fill="{colour}" fill-opacity="0.18" '
            f'stroke="{colour}" stroke-width="2"/>'
        )
        parts.append(
            t(1006, y + 26, f"{mark} {label}", size=18, weight=700, fill=colour, anchor="middle")
        )
        parts.append(t(1006, y + 46, sub, size=10.5, fill=C["ink"], anchor="middle"))
    parts.append(t(584, 606, "checks: run · data · schema · rows · time", size=14, fill=C["muted"]))
    return "\n".join(parts)


if __name__ == "__main__":
    write_one(
        "run-record",
        W,
        H,
        "Every run leaves a receipt: what it read, what it wrote with a row count and a hash of "
        "every row, the expectations, where it ran and how long it took, stored in .ubunye/lineage. "
        "The hash adds up a hash of each row, so row order does not matter and one changed cell "
        "changes it. ubunye gate compares two receipts and fails when the data changed but the "
        "task VERSION did not.",
        build(),
    )
