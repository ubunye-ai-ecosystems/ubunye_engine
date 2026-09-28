"""A cloud run's record survives the log store: split lines, reordered lines, prefixes.

Found by the proving ground (finding F-023): C01 ran to success on AWS Glue, but its
record, printed as one JSON line of a few thousand characters, came back from
CloudWatch cut at about 1,000 characters, and `ubunye deploy glue` failed with
`JSONDecodeError: Unterminated string`. The record is now printed in short numbered
base64 parts that the reader puts back by number from anywhere in the log.
"""

from __future__ import annotations

import base64
import json
import random
import subprocess
import sys

import pytest

from ubunye.deploy import package


def _entry_log(tmp_path, record: dict) -> str:
    """What the real entry script prints for ``record`` (its printing code, run)."""
    src = package.ENTRY_SCRIPT
    start = src.index("        text = base64.b64encode")
    end = src.index("        print(" + repr(package.RECORD_END))
    body = src[start:end] + "        print(" + repr(package.RECORD_END) + ", flush=True)\n"
    script = tmp_path / "printer.py"
    script.write_text(
        "import base64, json, sys\n" "record = json.loads(sys.stdin.read())\n" "if True:\n" + body,
        encoding="utf-8",
    )
    done = subprocess.run(
        [sys.executable, str(script)], input=json.dumps(record), capture_output=True, text=True
    )
    assert done.returncode == 0, done.stderr
    return done.stdout


def _big_record() -> dict:
    return {
        "run_id": "0d06bd33-f821-445b-adf4-1894f349ae83",
        "status": "success",
        "outputs": [
            {"name": f"out{i}", "data_hash": "sha256:" + "ab" * 32, "row_count": i}
            for i in range(40)
        ],
        "note": 'José Álvarez; quotes " and back\\slashes',
    }


def _cloudwatch(log: str, width: int = 1000) -> str:
    """Every line cut at ``width`` characters, each piece its own prefixed event."""
    events = []
    for line in log.splitlines():
        pieces = [line[i : i + width] for i in range(0, len(line), width)] or [""]
        events += [f"2026-09-28T16:35:02Z INFO {p}" for p in pieces]
    return "\n".join(events)


def test_a_long_record_survives_lines_cut_at_1000_characters(tmp_path):
    record = _big_record()
    assert len(json.dumps(record)) > 3000
    log = _entry_log(tmp_path, record)
    assert all(len(line) < 700 for line in log.splitlines())
    assert package.read_record(_cloudwatch(log)) == record


def test_parts_reordered_by_the_log_store_are_put_back(tmp_path):
    record = _big_record()
    lines = _entry_log(tmp_path, record).splitlines()
    random.Random(3).shuffle(lines)
    assert package.read_record("\n".join(lines)) == record


def test_a_missing_part_is_an_error_never_a_partial_record(tmp_path):
    lines = _entry_log(tmp_path, _big_record()).splitlines()
    kept = [ln for ln in lines if f"{package.RECORD_PART} 2/" not in ln]
    with pytest.raises(package.RecordIncomplete, match="missing \\[2\\]"):
        package.read_record("\n".join(kept))


def test_a_record_printed_by_an_older_engine_still_reads():
    old = f'{package.RECORD_BEGIN}\nINFO {{"run_id": "r1", "status": "success"}}\n{package.RECORD_END}\n'
    assert package.read_record(old) == {"run_id": "r1", "status": "success"}


def test_the_parts_are_plain_base64():
    text = base64.b64encode(json.dumps({"a": 1}).encode()).decode()
    log = f"x {package.RECORD_PART} 1/1 {text}\n"
    assert package.read_record(log) == {"a": 1}
