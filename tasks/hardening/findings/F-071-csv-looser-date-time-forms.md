# F-071: Spark infers dates and times from looser CSV text than pandas does

**Status:** open (not fixed; needs a port of Spark's date and time parsers)
**Severity:** minor (silent type difference, uncommon text)
**Source:** experiment E-08 (awkward data), shapes 4 (messy CSV) and 5 (time zones)
**Promise:** 1 (same result anywhere)

## What happens
With `inferSchema`, the pandas backend (F-067) takes dates and timestamps in the
strict ISO forms Arrow reads. Spark takes more, from its own parser
(`DateTimeUtils.stringToTimestamp`, used when no `timestampFormat` is set):

| Column text | pandas | Spark (by the source; CI will confirm) |
|---|---|---|
| `2024-1-2` | text | `timestamp` (the date pattern `yyyy-MM-dd` fails, the timestamp parser takes one digit months) |
| `2024-01` | text | `timestamp` |
| `2024-01-02 3:04:05` | text | `timestamp` |
| `2024-01-02 03:04:05.123456789` | text | `timestamp` (the extra digits dropped) |
| `03:04:05` | text | `timestamp` on today's date |

## Why not fixed here
Spark's timestamp grammar has many forms (time zone names, `T` or space, a trailing
space, time only), and the reading side (`UnivocityParser` with its legacy fallback)
has to match the inference exactly. It needs its own port and fuzz against live
Spark, like `spark_csv`. The time-only form also makes Spark's result depend on the
day it runs, which is a question in itself.

## Evidence
`tests/integration/test_awkward_data_parity.py::test_csv_looser_date_and_time_forms`,
`xfail(strict=False)`: CI shows which forms Spark really takes.
