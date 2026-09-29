# F-035: a cloud run's record was lost when the log store cut a line inside a marker

**Status:** fixed on feat/prove-r1 (2026-09-29), not yet merged
**Severity:** major
**Source:** proving R1 on AWS Glue (ubunye-infra prove-r1, run 36576881079)
**Promise:** a run that succeeded on a cloud gives back its run record (F-023)

## What happens
R1's two records are 14 parts of base64 (C01's one record is fewer). Glue's CloudWatch
flushes its buffer wherever it likes. F-023 taught the reader to complete a part whose
payload was cut. This time the cut fell inside the marker itself:

```
...ZGI2MGVm...ODky
UB
UNYE-RECORD-PART 9/14 600 ZGI2MGVmMGEyZDAzZSIs...
```

Part 9 had no header, so the reader refused the record ("the log holds 13 of the
record's 14 parts; missing [9]"), although both tasks had succeeded on Glue.

## Repro
`tests/unit/deploy/test_record_parts.py::test_a_line_cut_inside_a_part_header_is_mended`
cuts the header of part 3 at every position (and the digest line at every position),
with and without a log prefix on each line. On the old code: RecordIncomplete.

## Fix
Before reading parts, `package._mend` joins a line that starts a marker without
finishing it (the marker, or a last token that is the start of one) with the next
line, skipping that line's prefix, with or without a space, and only when the join
makes a whole marker line and the next line holds no marker of its own. A wrong join
cannot give a wrong record: each part says its length and the whole record its
SHA-256; every rejected join still ends in RecordIncomplete, never a partial record.
A guard test checks a part's last character that looks like a marker start ("U") is
never taken from it.

## Follow-up (skeptic review)
The reader skipped the SHA-256 check when the digest line itself could not be read, so
a digest line cut by a foreign line plus reordered continuation lines returned a
scrambled record as good; and a log holding an earlier run's record first returned
that one. Now parts without a whole digest are refused (every engine that prints parts
prints the digest), more than one digest in a log is refused, and a piece such as
`UBUNY` that starts both markers is tried as each (a cut inside the digest marker was
never mended, which the old unchecked path hid). Tests:
`test_parts_without_a_readable_digest_are_refused`,
`test_two_run_records_in_one_log_are_refused` (both fail on the old code).
