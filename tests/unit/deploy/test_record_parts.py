"""A cloud run's record survives the log store: cut lines, reordered lines, prefixes.

Found by the proving ground (finding F-023): C01 ran to success on AWS Glue, but its
record, printed as one JSON line of a few thousand characters, came back from
CloudWatch cut at about 1,000 characters, and `ubunye deploy glue` failed with
`JSONDecodeError: Unterminated string`. Printed next as numbered base64 parts, one part
still came back cut (295 of 600 characters) where Glue's output buffer flushed, the
rest as the next line. So each part says its length, a cut part is completed from the
lines after it, and the whole record is checked against its SHA-256: a record is read
whole and provably intact, or refused.
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
    start = src.index("        raw = json.dumps(record")
    end = src.index("        print(" + repr(package.RECORD_END))
    body = src[start:end] + "        print(" + repr(package.RECORD_END) + ", flush=True)\n"
    script = tmp_path / "printer.py"
    script.write_text(
        "import base64, hashlib, json, sys\n"
        "record = json.loads(sys.stdin.read())\n"
        "if True:\n" + body,
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


def test_a_long_record_survives_lines_cut_at_a_width(tmp_path):
    record = _big_record()
    assert len(json.dumps(record)) > 3000
    log = _entry_log(tmp_path, record)
    assert all(len(line) < 700 for line in log.splitlines())
    assert package.read_record(_cloudwatch(log)) == record


def test_a_part_cut_where_the_buffer_flushed_is_completed(tmp_path):
    # What Glue's CloudWatch did: a part arrived short, the rest as the next line,
    # with no prefix.
    record = _big_record()
    out = []
    for line in _entry_log(tmp_path, record).splitlines():
        if f"{package.RECORD_PART} 3/" in line:
            out += ["INFO " + line[:120], line[120:]]
        else:
            out.append("INFO " + line)
    assert package.read_record("\n".join(out)) == record


def test_parts_reordered_by_the_log_store_are_put_back(tmp_path):
    record = _big_record()
    lines = _entry_log(tmp_path, record).splitlines()
    random.Random(3).shuffle(lines)
    assert package.read_record("\n".join(lines)) == record


def test_a_cut_part_whose_rest_is_elsewhere_is_refused(tmp_path):
    lines = _entry_log(tmp_path, _big_record()).splitlines()
    i = next(k for k, ln in enumerate(lines) if f"{package.RECORD_PART} 3/" in ln)
    head, rest = lines[i][:120], lines[i][120:]
    lines[i] = head
    lines.insert(0, rest)  # a log store that reordered lines
    with pytest.raises(package.RecordIncomplete, match="part 3 arrived with"):
        package.read_record("\n".join(lines))


def test_a_missing_part_is_an_error_never_a_partial_record(tmp_path):
    lines = _entry_log(tmp_path, _big_record()).splitlines()
    kept = [ln for ln in lines if f"{package.RECORD_PART} 2/" not in ln]
    with pytest.raises(package.RecordIncomplete, match="missing \\[2\\]"):
        package.read_record("\n".join(kept))


def test_a_record_that_does_not_match_its_digest_is_refused(tmp_path):
    lines = _entry_log(tmp_path, _big_record()).splitlines()
    k = next(i for i, ln in enumerate(lines) if package.RECORD_DIGEST in ln)
    lines[k] = f"{package.RECORD_DIGEST} {'0' * 64}"
    with pytest.raises(package.RecordIncomplete, match="SHA-256"):
        package.read_record("\n".join(lines))


def test_a_record_printed_by_an_older_engine_still_reads():
    old = f'{package.RECORD_BEGIN}\nINFO {{"run_id": "r1", "status": "success"}}\n{package.RECORD_END}\n'
    assert package.read_record(old) == {"run_id": "r1", "status": "success"}


def test_a_small_record_is_one_part():
    text = base64.b64encode(json.dumps({"a": 1}).encode()).decode()
    log = f"x {package.RECORD_PART} 1/1 {len(text)} {text}\n"
    assert package.read_record(log) == {"a": 1}


@pytest.mark.parametrize("prefix", ["", "2026-09-29T13:43:54Z INFO "])
def test_a_line_cut_inside_a_part_header_is_mended(tmp_path, prefix):
    """What Glue's CloudWatch did (F-035): "...ODky" / "UB" / "UNYE-RECORD-PART 9/14 600 ...".

    The buffer flushed inside the marker itself, so the part had no header. Every cut
    point in the header, in the part number and in the digest line must read back.
    """
    record = _big_record()
    log = _entry_log(tmp_path, record)
    header = next(ln for ln in log.splitlines() if f"{package.RECORD_PART} 3/" in ln)
    header = header[: header.index(" ", len(package.RECORD_PART) + 5) + 1]  # "...PART 3/N 600 "
    for at in range(1, len(header)):
        cut = "\n".join(
            piece
            for ln in log.splitlines()
            for piece in (
                [prefix + ln[:at], prefix + ln[at:]]
                if f"{package.RECORD_PART} 3/" in ln
                else [prefix + ln]
            )
        )
        assert package.read_record(cut) == record, (at, header[:at])
    digest_line = f"{package.RECORD_DIGEST} "
    for at in range(1, len(digest_line) + 64):
        cut = "\n".join(
            piece
            for ln in log.splitlines()
            for piece in (
                [prefix + ln[:at], prefix + ln[at:]]
                if package.RECORD_DIGEST in ln
                else [prefix + ln]
            )
        )
        assert package.read_record(cut) == record, ("digest", at)


def test_a_mended_header_never_steals_from_the_part_before(tmp_path):
    """A continuation of one character that looks like the start of a marker ("U")."""
    record = {"k": "U" * 1500}
    log = _entry_log(tmp_path, record)
    lines = log.splitlines()
    i = next(k for k, ln in enumerate(lines) if f"{package.RECORD_PART} 1/" in ln)
    lines[i : i + 1] = [lines[i][:-1], lines[i][-1:]]  # the part's last character alone
    assert package.read_record("\n".join(lines)) == record
