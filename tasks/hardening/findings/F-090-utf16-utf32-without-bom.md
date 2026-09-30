# F-090: UTF-16 and UTF-32 without a byte order mark decode differently

**Status:** open (found by fuzz; not fixed)
**Severity:** minor (rare files; wrong text when it happens)
**Source:** E-09 follow-up, Java decoder fuzz (`scratchpad/awkward/java_decode_fuzz.py`)
**Promise:** 1 (same result anywhere)

## What happens
Spark 4 accepts the encodings `UTF-16` and `UTF-32` (F-085). Java's decoders for
those names read a byte order mark and, without one, take big endian. Python's
`utf-16` and `utf-32` codecs read a byte order mark and, without one, take the
machine's order (little endian on x86 and ARM). Over 40,010 random byte strings with
no mark, 34,743 decoded differently for `UTF-16` and 85 for `UTF-32` (the 85 are
invalid code points, which Java and Python also replace differently). `UTF-16LE`,
`UTF-16BE`, `UTF-8` and `US-ASCII` matched exactly.

## Why not fixed here
The pandas CSV reader hands such a file to pyarrow's transcoder, which follows
Python; and how Spark splits the lines of a UTF-16 file with multiLine off (on the
byte 0x0A) needs its own check against a live Spark. A fix is small (read `UTF-16`
with no mark as `UTF-16BE`) but should come with a live parity case for the line
splitting too.

## Evidence
The fuzz above (Java 21 `new String(bytes, charset)` against Python).
