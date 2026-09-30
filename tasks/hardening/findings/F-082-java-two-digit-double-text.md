# F-082: the smallest doubles and floats hash differently on pandas and Spark

**Status:** fixed on hardening/awkward-data (2026-09-30)
**Severity:** minor (a digest mismatch; values the same)
**Source:** experiment E-09 (awkward data), CI run on live Spark 3.5 and 4
**Promise:** 1 (same result anywhere)

## What happens
`test_parquet_special_numbers` and the E-09 task `special-numbers`: same rows and
types on both engines, different rows-v1 digest, on Spark 3.5 (Java 11) and Spark 4
(Java 21). Spark: `sha256:65d216a0...5e189b`; pandas: `sha256:5c372072...f3314ef47`.

## Cause
The table holds `5e-324` (the smallest double) and `1e-45` as a float. Python's
shortest text is `5e-324` and `1e-45`; Java's `Double.toString` and `Float.toString`,
when the shortest text has one digit, write the closest decimal of two digits:
`4.9E-324`, `1.4E-45` (JDK 19 spec; the older algorithm gives the same for these).
The hash wrote `5.0E-324` and `1.0E-45`.

## Fix
`content_hash.java_double` applies Java's rule; `java_float` is the same for 32 bit
floats. The pandas json and csv writers use the same functions, so the files match
Spark's text too.

## Evidence
Unit: `TestJavaTwoDigitRule` (the pandas digest of the CI table now equals Spark's
recorded digest; failed before) and `TestJavaNumberTextInFiles` (json and csv; failed
before). Integration: `test_parquet_special_numbers`, e2e `special-numbers` (CI).
