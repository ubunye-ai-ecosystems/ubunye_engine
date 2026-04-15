"""Validate Ubunye Titanic pipeline output against the committed golden parquet.

Intended to be run after ``ubunye run`` as the final CI gate. Compares the
pipeline's output parquet with ``expected_output/survival_by_class.parquet``
by loading both with pandas — parquet files are not byte-deterministic, so
byte-level comparison would be meaningless. Schema and row values must match
exactly; ordering is canonicalised by sorting on ``Pclass``.

Exit codes:
    0  — outputs match
    1  — mismatch or missing output; prints a diff to stderr
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd


def _load_parquet_dir(path: Path) -> pd.DataFrame:
    """Read a parquet directory (Spark's default) or a single file."""
    if path.is_dir():
        parts = sorted(path.glob("*.parquet"))
        if not parts:
            raise FileNotFoundError(f"No parquet part files under {path}")
        return pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)
    return pd.read_parquet(path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--actual", required=True, help="Path to the pipeline output")
    parser.add_argument("--expected", required=True, help="Path to the golden parquet")
    args = parser.parse_args()

    actual_path = Path(args.actual)
    expected_path = Path(args.expected)

    if not actual_path.exists():
        print(f"FAIL: actual output missing at {actual_path}", file=sys.stderr)
        return 1

    actual = (
        _load_parquet_dir(actual_path)
        .sort_values("Pclass")
        .reset_index(drop=True)
    )
    expected = (
        _load_parquet_dir(expected_path)
        .sort_values("Pclass")
        .reset_index(drop=True)
    )

    try:
        pd.testing.assert_frame_equal(actual, expected, check_dtype=False)
    except AssertionError as exc:
        print("FAIL: output does not match golden.", file=sys.stderr)
        print("\n-- actual --", file=sys.stderr)
        print(actual.to_string(), file=sys.stderr)
        print("\n-- expected --", file=sys.stderr)
        print(expected.to_string(), file=sys.stderr)
        print(f"\n{exc}", file=sys.stderr)
        return 1

    print(f"OK: {len(actual)} rows match golden.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
