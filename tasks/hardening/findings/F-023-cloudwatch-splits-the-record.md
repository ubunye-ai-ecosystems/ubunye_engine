# F-023: a cloud run's record was lost when the log store cut its line

**Status:** fixed (see commit fixing F-023)
**Severity:** major
**Source:** proving ground, C01 on AWS Glue 5.0, infra run 36451744539 (2026-09-28)
**Environment:** AWS Glue 5.0 (Spark 3.5), 2 x G.1X; engine hardening/real-world
**Promise:** the run record reaches whoever asked for it (`--record-out`)

## What happens
C01 ran to success on Glue (the record in the log says `status: success`, and its
`order_lines` hash equals local pandas'). `ubunye deploy glue --record-out` then failed:
`JSONDecodeError: Unterminated string starting at: line 1 column 1019`. The record was
printed as one JSON line of a few thousand characters; CloudWatch returned it cut at
about 1,000 characters. The run-anywhere example's record was short enough to fit.
The proving report showed aws-glue FAIL (no record), correctly: no record, no pass.

## Root cause
One long log line assumed to survive the log store. Log Analytics had already shown the
other failure mode (lines reordered, #90).

## Fix
The entry script prints the record as numbered base64 parts of 600 characters
(`UBUNYE-RECORD-PART n/total ...`); `read_record` collects them by number from anywhere
in the log, refuses a record with a missing part (`RecordIncomplete`), and still reads
the old one-line form. The Container Apps reader waits for every part.

## Evidence
tests/unit/deploy/test_record_parts.py: a 3,000+ character record through lines cut at
1,000 characters, shuffled lines, a missing part (4 fail before the fix). Glue rerun:
see the proving report.
