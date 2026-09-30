# F-086: broken UTF-8 gets more U+FFFD on pandas than on Spark

**Status:** fixed on hardening/awkward-data (2026-09-30)
**Severity:** minor (text differs in the replacement characters)
**Source:** experiment E-09 (awkward data), CI run on live Spark 3.5 and 4; skeptic
review (item 5)
**Promise:** 1 (same result anywhere)

## What happens
`test_csv_invalid_utf8_fuzz` failed on Spark 3.5 and 4: row 101 (bytes
`ed a0 80 ff ff 80 c0 80 e2 82`) read as 7 U+FFFD on Spark, 9 on pandas.

## Cause
F-061 decoded with Python's `errors="replace"`. Python and Java agree on every
malformed sequence but one: an encoded surrogate (`ED` then `A0`..`BF` then a
continuation byte). Java's UTF-8 decoder reads the three bytes as one malformed
character (`isMalformed3` passes, then `Character.isSurrogate`), one U+FFFD; Python
stops at the second byte, one U+FFFD per byte. `ED A0` followed by a byte that is not a
continuation is also one U+FFFD in Java (two bytes), two in Python.

## Fix
A codec error handler (`ubunye-java-replace`) that takes Python's answer except for
that sequence. `pandas_io.java_decode` uses it; the CSV reader decodes with it when
strict decoding fails.

## Evidence
Unit: `TestJavaDecodingOfBrokenUtf8` (the CI row gives 7, as Spark; 8 cases failed
before). Fuzz against Java 21's `new String(bytes, charset)` (the skeptic's `Dec`
oracle, `scratchpad/awkward/java_decode_fuzz.py`): 40,010 random byte strings, 0
differences for UTF-8, UTF-16LE, UTF-16BE and US-ASCII. `UTF-16` and `UTF-32` (with no
byte order mark) differ, filed as F-087.
