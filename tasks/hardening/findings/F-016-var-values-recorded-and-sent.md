# F-016: a `--var` value is stored in the run record and sent to lineage servers

**Status:** open
**Severity:** major
**Source:** experiment E-04 (2026-09-28)
**Promise:** 4 (a run record never holds a secret)

## What happens
Variables are recorded by design (the record says what the run was given). So a user
who passes a token as `--var token=...` (a natural thing to try) has it written to the
lineage record, sent in every OpenLineage event, and printed by `plan --json`. Spline
(AbsaOSS) hit this class of leak with JDBC URLs and added a name-based redaction
filter.

## Repro
tests/experiments/e04_secrets.py <ubunye> spark: marker `cli_var`.

## Expected
Values whose names look secret (token, password, passwd, secret, key, credential,
auth) are recorded as `***` in records, events and output, as Spline's default filter
does; the docs say `--var` is not for secrets and point to `secret://`.

## Evidence
The marker was found in the lineage record JSON, openlineage.jsonl and `plan --json`
output; no `secret://` or `{{ env.X }}` value was.
