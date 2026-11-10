"""Draw the Project Structure diagram (docs/assets/diagrams/project-structure.svg).

Use cases hold pipelines, a pipeline holds tasks that run in order, a task is a
.yaml and a .py. The flag colours match the command along the bottom.

    python scripts/diagrams/project_structure.py
"""

from __future__ import annotations

from _svg import MONO
from stage import C, box, heading, loop_arrow, spans, stack, t, task_files, write_one

W, H = 1100, 590
CARD_W, CARD_H = 184, 110


def build() -> str:
    parts = []

    # 1. use cases
    b1 = (30, 190, 300, 230)
    parts.append(heading(b1[0], b1[1] - 52, "-u", "USE CASES", C["uc"], sub="pipelines/"))
    parts.append(box(*b1))
    fx, fy = b1[0] + 24, b1[1] + b1[3] - 24 - CARD_H
    parts.append(stack(fx, fy, CARD_W, CARD_H, ["fraud_detection", "churn", "…"], colour=C["uc"]))

    # 2. pipelines inside fraud_detection
    b2 = (400, 190, 300, 230)
    parts.append(heading(b2[0], b2[1] - 52, "-p", "PIPELINES", C["pl"], sub="fraud_detection/"))
    parts.append(box(*b2))
    gx, gy = b2[0] + 24, b2[1] + b2[3] - 24 - CARD_H
    parts.append(stack(gx, gy, CARD_W, CARD_H, ["ingestion", "scoring", "…"], colour=C["pl"]))
    parts.append(loop_arrow(fx + CARD_W / 2, fy + CARD_H, b2[0] - 4, b2[1] + 70))

    # 3. tasks inside ingestion, run in order
    b3 = (770, 110, 300, 400)
    parts.append(heading(b3[0], b3[1] - 52, "-t", "TASKS", C["tk"], sub="ingestion/"))
    parts.append(box(*b3))
    tw, th, step = 228, 262, 24
    tx, ty = b3[0] + 20, b3[1] + b3[3] - 20 - th
    labels = ["claim_etl", "policy_etl", "claims_joined"]
    fills = [C["front"], C["mid"], C["back"]]
    for i in reversed(range(3)):
        cx, cy = tx + i * step, ty - i * step
        parts.append(box(cx, cy, tw, th, rx=14, fill=fills[i], sw=1.5))
        ink = C["card_ink"] if i < 2 else C["ink"]
        # order number in a circle, then the task name
        parts.append(f'<circle cx="{cx + 16}" cy="{cy + 14}" r="9" fill="{C["tk"]}"/>')
        parts.append(
            t(cx + 16, cy + 19, str(i + 1), size=13, weight=700, fill=C["bg"], anchor="middle")
        )
        parts.append(t(cx + 32, cy + 19, labels[i], size=16, weight=700, font=MONO, fill=ink))
    parts.append(task_files(tx + 14, ty + 40, tw - 28, 166))
    parts.append(loop_arrow(gx + CARD_W / 2, gy + CARD_H, b3[0] - 4, b3[1] + 150))
    parts.append(
        t(
            b3[0] + b3[2] - 12,
            b3[1] + 26,
            "run in order 1 → 2 → 3",
            size=14,
            fill=C["muted"],
            anchor="end",
        )
    )

    # the command that names this task, coloured like the flags
    parts.append(
        spans(
            W / 2,
            H - 32,
            [
                ("ubunye run ", C["ink"], 400),
                ("-d pipelines ", C["muted"], 400),
                ("-u fraud_detection ", C["uc"], 700),
                ("-p ingestion ", C["pl"], 700),
                ("-t claim_etl", C["tk"], 700),
            ],
            size=24,
            anchor="middle",
        )
    )
    return "\n".join(parts)


if __name__ == "__main__":
    write_one(
        "project-structure",
        W,
        H,
        "Use cases hold pipelines; a pipeline holds tasks that run in order; a task is a "
        "config.yaml and a transformations.py. ubunye run -d pipelines -u fraud_detection "
        "-p ingestion -t claim_etl runs the first task.",
        build(),
    )
