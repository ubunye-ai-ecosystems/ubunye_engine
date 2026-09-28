# E-05: A source adds, drops, renames or retypes a column: does the run fail loudly, or quietly write something wrong?

**Status:** planned
**Why:** Enceladus and ABRiS (AbsaOSS) handle schema evolution explicitly after drift in production.

## Method
Run a task, then change the input's schema in each way and run again. Read outputs and records.

## Pass means
Each change is handled as documented or fails with a message naming the column; the run record shows the schema changed.

## Result
Not run yet.
