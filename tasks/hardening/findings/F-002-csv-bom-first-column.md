# F-002: csv bom first column

**Status:** fixed in PR #98
**Severity:** major
**Source:** stranger (Olist), round 1 (2026-09-28)
**Promise:** 1

## What happens
product_category_name_translation.csv starts with a UTF-8 byte order mark; the pandas backend kept it in the first column's name, so a join raised KeyError. Spark names the column product_category_name.

## Repro
Read any CSV that starts with the bytes EF BB BF, header on.

## Expected
Spark drops a BOM at the start of each file and keeps one anywhere else.

## Evidence
16-case grid on live Spark 4.2: 8 DIFF before, 16 SAME after. Parity cases added to tests/integration/test_pandas_backend_parity.py.
