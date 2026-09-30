"""The real-world example `examples/real-world/food_prices_africa` runs, and finds what it says.

Runs both steps on the committed WFP sample (Chad and Mozambique, staple cereals, 2025
and 2026) on the pandas backend, exactly as the README tells a newcomer to, and checks
the alerts the README promises and the golden data hashes (the same on Spark: see
tests/integration/test_real_world_food_prices.py).
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

pytest.importorskip("pandas")
pytest.importorskip("pyarrow")
nw = pytest.importorskip("narwhals")
if not hasattr(nw.col("x"), "floor"):
    pytest.skip("the example needs narwhals>=2.9 (Expr.floor)", allow_module_level=True)

import pandas as pd  # noqa: E402

import ubunye  # noqa: E402
from ubunye.lineage.storage import FileSystemLineageStore  # noqa: E402

EXAMPLE = Path(__file__).resolve().parents[3] / "examples" / "real-world" / "food_prices_africa"
GOLDEN = {"clean": "f61e0f0544f5", "monitor": "021cc19ca2b6"}


def _digest(record) -> str:
    import hashlib

    joined = ";".join(
        f"{o.name}={o.data_hash}" for o in sorted(record.outputs, key=lambda o: o.name)
    )
    return hashlib.sha256(joined.encode()).hexdigest()[:12]


@pytest.fixture
def example(tmp_path):
    root = tmp_path / "food"
    shutil.copytree(
        EXAMPLE, root, ignore=shutil.ignore_patterns("output*", "data", "evidence", ".ubunye")
    )
    return root


def test_the_readme_run_finds_the_chad_alerts(example):
    for step in ("clean", "monitor"):
        ubunye.run_task(
            str(example / "pipelines" / "food" / "prices" / step), backend="pandas", lineage=True
        )

    alerts = pd.read_parquet(example / "output" / "alerts").sort_values("commodity")
    assert list(alerts["countryiso3"]) == ["TCD", "TCD"]
    assert list(alerts["commodity"]) == ["Rice (imported)", "Wheat flour"]
    assert list(alerts["change_on_year"]) == [0.594, 0.7792]
    assert list(alerts["markets"]) == [54, 54]

    store = FileSystemLineageStore(str(example / "pipelines" / ".ubunye" / "lineage"))
    for step, golden in GOLDEN.items():
        (record,) = store.list_runs(f"food/prices/{step}")
        assert record.status == "success"
        assert _digest(record) == golden, step
