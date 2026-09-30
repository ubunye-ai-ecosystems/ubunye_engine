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
The entry script prints the record's SHA-256, then the record as numbered base64 parts
that say their own length (`UBUNYE-RECORD-PART n/total length payload`). `read_record`
collects the parts by number from anywhere in the log, completes a part the store cut
short from the lines that follow it, and checks the whole against the SHA-256; a record
with a missing, uncompletable or mismatched part is refused (`RecordIncomplete`), never
read in part. The old one-line form still reads; the Container Apps reader waits for
every part.

A first fix (parts without lengths) failed on the Glue rerun, infra run 36452872273:
part 7 of 8 arrived with 295 of its 600 characters. CloudWatch also cuts where Glue's
output buffer flushes, not only at a width.

## Follow-up
Where the platform has a bucket (Glue, Dataproc), write the record to it as well; logs
are a lossy transport.

## Evidence
tests/unit/deploy/test_record_parts.py: a 3,000+ character record through lines cut at
a width and cut mid-line where a buffer flushed, shuffled lines, a missing part, a cut
part whose rest is elsewhere, a wrong digest (7 of 8 fail on the first fix).
