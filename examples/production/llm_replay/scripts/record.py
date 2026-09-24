"""Record the example's model answers from the stub, and write the golden hash.

    python examples/production/llm_replay/scripts/record.py

Starts the stub model, runs the task once with UBUNYE_LLM_MODE=record (answers
go to pipelines/shop/reviews/label/llm-replay.jsonl), then writes the output's row
hash to expected_output/golden.json. Commit both. Re-run it only when the prompt,
the model settings or the data change on purpose.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
EXAMPLE = HERE.parent
TASK = EXAMPLE / "pipelines" / "shop" / "reviews" / "label"


def main() -> int:
    sys.path.insert(0, str(HERE))
    from stub_model import serve

    import ubunye
    from ubunye.lineage.storage import FileSystemLineageStore

    replay = TASK / "llm-replay.jsonl"
    replay.unlink(missing_ok=True)
    server = serve()
    work = Path(tempfile.mkdtemp())
    os.environ.update(
        UBUNYE_LLM_MODE="record",
        LLM_BASE_URL=f"http://127.0.0.1:{server.server_address[1]}/v1",
        REVIEWS_INPUT_PATH=(EXAMPLE / "data" / "reviews.csv").as_posix(),
        REVIEWS_OUTPUT_PATH=(work / "out").as_posix(),
    )
    try:
        ubunye.run_task(str(TASK), backend="pandas", lineage=True, lineage_dir=str(work / "lin"))
    finally:
        server.shutdown()
    (record,) = FileSystemLineageStore(str(work / "lin")).list_runs("shop/reviews/label")
    (out,) = record.outputs
    golden = {"labelled": {"row_count": out.row_count, "data_hash": out.data_hash}}
    (EXAMPLE / "expected_output" / "golden.json").write_text(
        json.dumps(golden, indent=2) + "\n", encoding="utf-8"
    )
    print(f"recorded {len(record.llm_calls)} answers to {replay}")
    print(f"golden: {golden}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
