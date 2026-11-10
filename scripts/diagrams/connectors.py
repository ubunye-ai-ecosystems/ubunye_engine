"""Draw the connectors diagram (docs/assets/diagrams/connectors.svg).

inputs: -> your transform -> outputs:, each connector shown with its logo and the
name you write after format: in config.yaml. A small pandas mark means the
connector also works on the pandas backend.

    python scripts/diagrams/connectors.py
"""

from __future__ import annotations

from _svg import MONO
from stage import C, arrow, box, chip_icon, heading, icon, t, tile, write_one

W, H = 1100, 620

# (icons, format name, hint, also on pandas)
READERS = [
    (["apachehive"], "hive", "Hive tables", False),
    (["postgresql", "mysql"], "jdbc", "Postgres, MySQL, ...", False),
    (["databricks"], "unity", "Unity Catalog", False),
    (["delta"], "delta", "Delta Lake", False),
    (["s3"], "s3", "S3, ADLS, GCS, files", True),
    (["json"], "rest_api", "any HTTP API", True),
    (["file"], "binary", "PDFs, images", False),
]
WRITERS = [r for r in READERS if r[1] != "binary"]


def column(x, y, items):
    out = []
    for i, (icons, name, hint, pandas) in enumerate(items):
        out.append(
            tile(
                x,
                y + i * 64,
                300,
                54,
                icons,
                name,
                sub=hint,
                mark="pandas" if pandas else None,
                size=18,
            )
        )
    return "\n".join(out)


def build() -> str:
    parts = []

    parts.append(heading(30, 52, None, "INPUTS", C["ink"]))
    parts.append(column(30, 76, READERS))

    parts.append(arrow(344, 414, 300, C["outline"]))

    # the transform
    parts.append(heading(426, 52, None, "YOUR TRANSFORM", C["ink"]))
    ex, ey, ew, eh = 426, 76, 248, 438
    parts.append(box(ex, ey, ew, eh, rx=28, fill=C["engine"], stroke="#4a4d57"))
    parts.append(icon("python", ex + ew / 2 - 34, ey + 30, 68))
    parts.append(
        t(
            ex + ew / 2,
            ey + 130,
            "transformations.py",
            size=16,
            weight=700,
            font=MONO,
            anchor="middle",
        )
    )
    code = ["def transform(", "    self, sources):", "  ...", "  return outputs"]
    parts.append(
        f'<rect x="{ex + 20}" y="{ey + 148}" width="{ew - 40}" height="104" rx="10" fill="{C["step"]}"/>'
    )
    for i, line in enumerate(code):
        parts.append(
            t(
                ex + 34,
                ey + 174 + i * 22,
                line,
                size=14,
                font=MONO,
                fill=C["muted"],
                extra=' xml:space="preserve"',
            )
        )
    parts.append(t(ex + ew / 2, ey + 288, "runs on", size=15, fill=C["muted"], anchor="middle"))
    for i, (ic, name) in enumerate(
        [("apachespark", "Spark"), ("databricks", "Databricks"), ("pandas", "pandas")]
    ):
        parts.append(tile(ex + 34, ey + 302 + i * 44, ew - 68, 38, [ic], name, size=15))

    parts.append(arrow(686, 756, 300, C["accent"]))

    parts.append(heading(770, 52, None, "OUTPUTS", C["ink"]))
    parts.append(column(770, 76, WRITERS))

    # legend
    parts.append(chip_icon("pandas", 776, H - 54, 22, pad=5))
    parts.append(t(812, H - 37, "also works on pandas, no Java", size=15, fill=C["muted"]))
    parts.append(t(30, H - 37, "format: picks the connector", size=15, fill=C["muted"], font=MONO))
    return "\n".join(parts)


if __name__ == "__main__":
    write_one(
        "connectors",
        W,
        H,
        "Inputs: hive, jdbc, unity, delta, s3, rest_api and binary, picked by format: in config.yaml. "
        "Your transform in transformations.py runs on Spark, Databricks or pandas. Outputs: the same "
        "connectors except binary. s3 and rest_api also work on pandas.",
        build(),
    )
