---
description: Check a pandas-backend behaviour against live Spark and pin it as a parity case
argument-hint: <the behaviour, e.g. "CSV with a BOM and a quoted header">
---

Use the `parity-checker` agent to establish, on live Spark, this behaviour: $ARGUMENTS

It returns a SAME/DIFF table across the relevant option grid and adds the cases to the
integration parity tests. If any case is DIFF, file a finding (`/finding`) with the
table as evidence. If Spark itself behaves surprisingly, the fix is parity plus a
warning, never a changed default.
