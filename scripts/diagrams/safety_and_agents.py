"""Draw the rerun safety and MCP diagrams (docs/assets/diagrams/rerun.svg, mcp.svg).

python scripts/diagrams/safety_and_agents.py
"""

from __future__ import annotations

from _svg import MONO
from stage import C, box, chip_icon, flow_arrow, folder, icon, mini_task, t, tile, write_one

W = 1100
OK, FAIL = "#22c55e", "#ef4444"

ROWS = [
    (
        "run again",
        "#a1a7b3",
        [("table", "run 1 appended"), ("stop", "run 2 refused")],
        "no double rows",
    ),
    (
        "--rerun",
        "#3dd6c3",
        [("table", "run 1 appended"), ("table", "run 2 written"), ("bin", "old files removed")],
        "replaced once",
    ),
    (
        "crash",
        "#ef4444",
        [("bolt", "run 1 dies"), ("lock", "lease taken over"), ("bin", "its files removed")],
        "safe to retry",
    ),
    (
        "--resume",
        "#b49cff",
        [("table", "tasks 1, 2 done"), ("bolt", "task 3 failed"), ("table", "skip 1, 2; run 3")],
        "pipeline done",
    ),
]


def rerun() -> str:
    parts = []
    parts.append(mini_task(36, 30, "claim_etl", w=180))
    parts.append(
        f'<rect x="232" y="56" width="190" height="36" rx="18" fill="{C["engine"]}" stroke="{C["outline"]}"/>'
    )
    parts.append(t(327, 80, "dt=2026-11-10", size=16, weight=700, font=MONO, anchor="middle"))
    parts.append(t(440, 70, "= one batch", size=17, weight=700))
    parts.append(chip_icon("lock", 560, 52, 26))
    parts.append(t(600, 72, "one live run per batch, with a lease", size=16, fill=C["muted"]))

    for r, (label, colour, steps, result) in enumerate(ROWS):
        y = 150 + r * 96
        parts.append(box(30, y, 1040, 82, rx=16, stroke="#3a3d46", fill=C["engine"]))
        parts.append(t(52, y + 48, label, size=18, weight=700, font=MONO, fill=colour))
        x = 196
        for i, (ic, text) in enumerate(steps):
            parts.append(tile(x, y + 14, 200, 54, [ic], text, size=15))
            if i < len(steps) - 1:
                parts.append(flow_arrow(x + 204, y + 41, x + 228, y + 41))
            x += 234
        parts.append(flow_arrow(x - 30, y + 41, 900, y + 41, OK))
        parts.append(
            f'<rect x="906" y="{y + 22}" width="150" height="38" rx="19" fill="{OK}" fill-opacity="0.16" '
            f'stroke="{OK}" stroke-width="2"/>'
        )
        parts.append(t(981, y + 47, "✓ " + result, size=15, weight=700, fill=OK, anchor="middle"))
    parts.append(
        t(
            36,
            548,
            "Appends Ubunye cannot take back (JDBC, a catalog table, s3a://) are named in the "
            "log, never deleted.",
            size=15,
            fill=C["muted"],
        )
    )
    return "\n".join(parts)


TOOLS = ["tasks", "doctor", "plan", "runs", "record", "gate", "focus", "health", "upstream"]


def mcp() -> str:
    parts = []
    # the agent
    parts.append(
        t(36, 52, "AN AGENT", size=15, weight=700, fill=C["muted"], extra=' letter-spacing="1"')
    )
    parts.append(f'<rect x="36" y="80" width="230" height="200" rx="18" fill="{C["tile"]}"/>')
    parts.append(icon("agent", 111, 104, 80))
    parts.append(
        t(151, 222, "any MCP client", size=17, weight=700, fill=C["tile_ink"], anchor="middle")
    )
    parts.append(
        t(151, 248, "Claude, IDE assistants", size=14, fill=C["tile_muted"], anchor="middle")
    )
    parts.append(
        f'<path d="M276 180H330" stroke="{C["outline"]}" stroke-width="3" marker-start="url(#head)" '
        'marker-end="url(#head)"/>'
    )

    # the server and its tools
    parts.append(t(346, 52, "ubunye mcp -d pipelines", size=17, weight=700, font=MONO))
    parts.append(box(340, 72, 470, 300, rx=22, fill=C["engine"], stroke="#4a4d57"))
    parts.append(t(364, 106, "read only, always on", size=15, weight=700, fill=OK))
    for i, name in enumerate(TOOLS):
        x = 364 + (i % 3) * 144
        y = 120 + (i // 3) * 48
        parts.append(
            f'<rect x="{x}" y="{y}" width="132" height="38" rx="10" fill="{C["step"]}" stroke="{OK}" '
            'stroke-opacity="0.6" stroke-width="1.5"/>'
        )
        parts.append(t(x + 66, y + 25, name, size=15, weight=700, font=MONO, anchor="middle"))
    parts.append(
        '<rect x="364" y="276" width="420" height="72" rx="12" fill="none" stroke="#f59e0b" '
        'stroke-width="2" stroke-dasharray="7 5"/>'
    )
    parts.append(chip_icon("lock", 384, 297, 26, pad=5))
    parts.append(t(430, 308, "run", size=18, weight=700, font=MONO, fill="#f59e0b"))
    parts.append(t(430, 332, "only with --allow-run", size=15, font=MONO, fill=C["muted"]))

    # what it reaches
    parts.append(
        f'<path d="M820 180H874" stroke="{C["outline"]}" stroke-width="3" marker-end="url(#head)"/>'
    )
    parts.append(
        t(
            886,
            52,
            "YOUR PROJECT",
            size=15,
            weight=700,
            fill=C["muted"],
            extra=' letter-spacing="1"',
        )
    )
    parts.append(f'<rect x="886" y="80" width="184" height="200" rx="18" fill="{C["tile"]}"/>')
    parts.append(folder(904, 102, 34, "#0f766e"))
    parts.append(t(948, 124, "pipelines/", size=16, weight=700, font=MONO, fill=C["tile_ink"]))
    parts.append(t(904, 164, "tasks, by name", size=14, fill=C["tile_muted"]))
    parts.append(folder(904, 192, 34, "#64748b"))
    parts.append(t(948, 214, ".ubunye/", size=16, weight=700, font=MONO, fill=C["tile_ink"]))
    parts.append(t(904, 254, "run records", size=14, fill=C["tile_muted"]))

    parts.append(box(886, 300, 184, 72, rx=14, stroke=FAIL))
    parts.append(
        t(978, 330, "../../etc", size=16, weight=700, font=MONO, fill=FAIL, anchor="middle")
    )
    parts.append(t(978, 356, "✗ refused", size=15, weight=700, fill=FAIL, anchor="middle"))
    return "\n".join(parts)


if __name__ == "__main__":
    write_one(
        "rerun",
        W,
        580,
        "Rerun safety. A batch is a task with its dt; one run per batch holds a lease. Running a "
        "finished append again is refused. --rerun writes the batch again and then removes the first "
        "run's files. After a crash the next run takes over the lease and removes the dead run's files. "
        "--resume skips the tasks of a pipeline that already finished and runs the rest.",
        rerun(),
    )
    write_one(
        "mcp",
        W,
        400,
        "ubunye mcp serves the engine to any MCP client. Tasks, doctor, plan, runs, record, gate, focus, "
        "health and upstream only read. The run tool exists only with --allow-run. Tasks are named, "
        "never given as paths, so ../../etc is refused.",
        mcp(),
    )
