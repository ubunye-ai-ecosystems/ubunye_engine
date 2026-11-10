# F-102: a unity read with `db_name` + `tbl_name` passed validate and failed at run time

**Status:** fixed (0.9.1)
**Severity:** major (a config that validates fails only on the cluster)
**Source:** docs audit (0.9.1)
**Promise:** `ubunye validate` catches a config the connector cannot run

## What happened
The unity reader's `validate_config` accepted `db_name` + `tbl_name`, but `read()`
reads only `table`, `sql` or `catalog` + `schema` + `tbl_name`, so the run raised
`SourceReadError`. The reverse was wrong too: `catalog` + `schema` + `tbl_name`,
which the reader does read, failed validation. The unity writer never took `db_name`,
so there is no alias to keep; mapping it would make the reader and writer disagree.

## Fix
The reader's `validate_config` now accepts what `read()` reads, the same names the
writer takes. `db_name` is refused with a message naming the keys to use. The two
sample pipelines under `pipelines/fraud_detection/risk_scoring` used `db_name` and now
name the table in full.

## Evidence
`tests/unit/config/test_unity_read_keys.py`: the `db_name` refusal, the
catalog/schema/tbl_name pass and the `tbl_name`-alone refusal fail before the fix and
pass after.

## Open
`ubunye init` (Databricks template, `ubunye/cli/scaffold.py`) still writes a unity
read with `db_name`; that file belongs to the CLI work in flight and needs the same
change (`table: main.raw_db.<task>_input`).
