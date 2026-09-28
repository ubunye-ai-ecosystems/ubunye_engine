# F-012: the pandas backend has no rerun-safe way to write incremental data

**Status:** open
**Severity:** major
**Source:** experiment E-01 (2026-09-28)
**Promise:** 1 (same task anywhere) and 5

## What happens
The rerun-safe pattern for daily data is to replace the day's partition
(`mode: overwrite_partitions`, `partitionBy: [dt]`). On the pandas backend it is
refused: "the pandas backend can do append, errorifexists, ignore, overwrite." So the
backend meant for laptops and small machines, where power cuts are most likely, can
only append (duplicates on a rerun, F-011) or overwrite the whole table.

## Repro
Any task with an output `mode: overwrite_partitions`, `partitionBy: [batch]`, run with
`--backend pandas`.

## Expected
Spark's dynamic partition overwrite on the pandas backend: hive-style folders
(`batch=2/part-...`), replacing only the partitions present in the new data, the same
layout Spark writes (checked with the parity-checker), committed per partition through
the existing staging and rename.

## Evidence
The refusal above (a clear message, which is good). The E-01 rerun with this mode
could not start.
