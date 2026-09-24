"""Check the latest run replayed every model call and wrote the golden rows.

python check_golden.py --lineage-dir DIR
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

EXAMPLE = Path(__file__).resolve().parent.parent


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lineage-dir", required=True)
    args = parser.parse_args()

    from ubunye.lineage.storage import FileSystemLineageStore

    golden = json.loads((EXAMPLE / "expected_output" / "golden.json").read_text("utf-8"))
    (record,) = FileSystemLineageStore(args.lineage_dir).list_runs("shop/reviews/label", n=1)
    problems = []
    if record.status != "success":
        problems.append(f"the run ended {record.status}: {record.error}")
    sources = {c.get("source") for c in record.llm_calls}
    if sources != {"replay"}:
        problems.append(f"model calls were not all replayed: {sorted(map(str, sources))}")
    (out,) = record.outputs
    want = golden["labelled"]
    if (out.row_count, out.data_hash) != (want["row_count"], want["data_hash"]):
        problems.append(
            f"labelled: {out.row_count} rows {out.data_hash}, "
            f"golden {want['row_count']} rows {want['data_hash']}"
        )
    for p in problems:
        print(f"FAIL: {p}")
    if not problems:
        print(f"OK: {len(record.llm_calls)} calls replayed, {out.row_count} rows match golden")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
