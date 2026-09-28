# E-05: A source adds, drops, renames or retypes a column: does the run fail loudly, or quietly write something wrong?

**Status:** answered (2026-09-28): loud or caught by gate, except a retype at run time
**Why:** Enceladus and ABRiS (AbsaOSS) handle schema evolution explicitly after drift in production.

## Method
Run a task, then change the input's schema in each way and run again. Read outputs and records.

## Pass means
Each change is handled as documented or fails with a message naming the column; the run record shows the schema changed.

## Result
Harness: tests/experiments/e03_e05_rows_and_schema.py. A good run, then the orders source changes, then a run and a
`gate` against the good run.

| Change | Run | Record | Gate |
|---|---|---|---|
| column added | writes | schema hash changes | FAIL: schema changed, names input orders |
| column dropped (used by the task) | fails: KeyError 'qty' | error | FAIL: run ended error |
| column renamed | fails: KeyError 'price' | error | FAIL: run ended error |
| price int to text | writes wrong totals ("11.011.0") | schema hash changes | FAIL: schema changed |
| qty int to float | writes | schema hash changes | FAIL: schema changed |

Loud failures name the column only through the user's own KeyError. The gate catches
every change and names the input; nothing catches a retype at run time (F-018).
