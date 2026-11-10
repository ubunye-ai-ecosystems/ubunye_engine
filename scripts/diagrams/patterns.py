"""Draw the RAG and feature store pipeline diagrams (docs/assets/diagrams/rag.svg, features.svg).

python scripts/diagrams/patterns.py
"""

from __future__ import annotations

from stage import C, chip_icon, flow_arrow, mini_task, t, tile, write_one

W = 1100


def label(x, y, s, w=180):
    return t(
        x + w / 2,
        y,
        s,
        size=14,
        weight=700,
        fill=C["muted"],
        anchor="middle",
        extra=' letter-spacing="1"',
    )


def caption(x, y, s, w=180):
    return t(x + w / 2, y, s, size=15, fill=C["ink"], anchor="middle")


def rag() -> str:
    parts = []
    xs = [36, 248, 460, 672, 884]
    top = 90
    parts.append(label(xs[0], 60, "RAW DOCS"))
    parts.append(tile(xs[0], top, 180, 54, ["json"], "REST API", size=16))
    parts.append(tile(xs[0], top + 66, 180, 54, ["s3"], "S3", size=16))
    parts.append(label(xs[1], 60, "UBUNYE TASK"))
    parts.append(mini_task(xs[1], top + 14, "documents", w=180))
    parts.append(caption(xs[1], top + 136, "clean + chunk"))
    parts.append(label(xs[2], 60, "DELTA TABLE"))
    parts.append(tile(xs[2], top + 33, 180, 54, ["delta"], "chunks", size=16))
    parts.append(label(xs[3], 60, "UBUNYE TASK"))
    parts.append(mini_task(xs[3], top + 14, "embed", w=180))
    parts.append(caption(xs[3], top + 136, "chunk → vector"))
    parts.append(label(xs[4], 60, "VECTOR STORE"))
    parts.append(
        tile(xs[4], top + 26, 180, 68, ["vectors"], "vectors", sub="pgvector, Pinecone…", size=16)
    )
    for a, b in zip(xs, xs[1:]):
        parts.append(flow_arrow(a + 184, top + 60, b - 6, top + 60))
    # the question at query time
    qy = 290
    parts.append(
        f'<path d="M{xs[4] + 90} {top + 98}V{qy - 6}" stroke="{C["accent"]}" stroke-width="2.5" '
        'stroke-dasharray="7 6" marker-end="url(#head)"/>'
    )
    parts.append(chip_icon("agent", xs[4] + 76, qy + 8, 28))
    parts.append(
        t(
            xs[4] + 60,
            qy + 30,
            "an LLM finds the closest chunks",
            size=15,
            fill=C["muted"],
            anchor="end",
        )
    )
    return "\n".join(parts)


def features() -> str:
    parts = []
    top = 120
    x0, x1, x2, x3, x4 = 36, 260, 484, 708, 900
    parts.append(label(x0, 60, "RAW EVENTS"))
    parts.append(tile(x0, top, 180, 54, ["apachehive"], "hive", size=16))
    parts.append(tile(x0, top + 66, 180, 54, ["s3"], "S3", size=16))
    parts.append(label(x1, 60, "UBUNYE TASK"))
    parts.append(mini_task(x1, top + 14, "transaction", w=180))
    parts.append(caption(x1, top + 136, "aggregate + encode"))
    parts.append(label(x2, 60, "FEATURE TABLE"))
    parts.append(tile(x2, top + 26, 180, 68, ["delta"], "features", sub="point in time", size=16))
    parts.append(flow_arrow(x0 + 184, top + 60, x1 - 6, top + 60))
    parts.append(flow_arrow(x1 + 184, top + 60, x2 - 6, top + 60))
    parts.append(label(x3, 60, "UBUNYE TASKS", w=170))
    parts.append(mini_task(x3, 70, "train", w=170))
    parts.append(mini_task(x3, 220, "score", w=170))
    parts.append(
        f'<path d="M{x2 + 184} {top + 60}C{x2 + 205} {top + 60} {x3 - 30} 116 {x3 - 6} 116" fill="none" '
        f'stroke="{C["outline"]}" stroke-width="2.5" marker-end="url(#head)"/>'
    )
    parts.append(
        f'<path d="M{x2 + 184} {top + 60}C{x2 + 205} {top + 60} {x3 - 30} 266 {x3 - 6} 266" fill="none" '
        f'stroke="{C["outline"]}" stroke-width="2.5" marker-end="url(#head)"/>'
    )
    parts.append(label(x4, 60, "RESULT", w=170))
    parts.append(tile(x4, 89, 170, 54, ["scikitlearn"], "model", size=16))
    parts.append(tile(x4, 239, 170, 54, ["table"], "scores", size=16))
    parts.append(flow_arrow(x3 + 174, 116, x4 - 6, 116))
    parts.append(flow_arrow(x3 + 174, 266, x4 - 6, 266))
    parts.append(
        t(
            W / 2,
            360,
            "training and scoring read the same features",
            size=16,
            fill=C["muted"],
            anchor="middle",
        )
    )
    return "\n".join(parts)


if __name__ == "__main__":
    write_one(
        "rag",
        W,
        340,
        "RAG pipeline: raw documents from a REST API or S3, a Ubunye task cleans and chunks them into a "
        "Delta chunks table, a second task embeds each chunk into a vector store, and an LLM finds the "
        "closest chunks at query time.",
        rag(),
    )
    write_one(
        "features",
        W,
        390,
        "Feature store pipeline: raw events from Hive or S3, a Ubunye task aggregates and encodes them "
        "into a point-in-time Delta feature table, and training and scoring tasks both read it.",
        features(),
    )
