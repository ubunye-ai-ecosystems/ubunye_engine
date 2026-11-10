# F-112: Spark with no Java ends in a raw traceback, and doctor calls it usable

**Status:** fixed (0.9.1)
**Severity:** major (the first run on a fresh laptop)
**Source:** review (0.9.1 CLI pass)
**Promise:** errors say what, where and what to try; `ubunye doctor` says what will fail

## What happens
- `ubunye run` with pyspark installed and no Java prints pyspark's
  `PySparkRuntimeError: [JAVA_GATEWAY_EXITED]` traceback.
- `ubunye doctor` reports `backend: spark (default): usable` with no Java (its java
  line warns, the backend line does not).
- The doctor's fix text said "Java 17 (Spark 4) or 11 (Spark 3.5)", narrower than what
  works (Spark 4: 17 or 21; Spark 3.5: 8, 11 or 17). pyproject's integration marker
  said Java 11.

## Repro
```bash
env -u JAVA_HOME PATH=/nowhere .venv/bin/ubunye run -d pipelines -u uc -p pkg -t t
env -u JAVA_HOME PATH=/nowhere .venv/bin/ubunye doctor
```

## Fix
One function, `ubunye.backends.spark_backend.java_problem()`: no Java, or a Java older
than the installed Spark needs. `SparkBackend.start()` calls it before it creates a
session (not when a session is already running, not for Spark Connect) and raises
`SparkSessionError` with the versions that work; `ubunye run` prints it as `[ERROR]`.
`describe_all()` (doctor, `ubunye backends`) asks the same function through
`SparkBackend.environment_problem()`. The Java helpers moved from the doctor to the
backend, so there is one copy. A Java newer than Spark lists is left for Spark to judge.

## Evidence
`tests/unit/backends/test_spark_java_check.py`: 9 tests. Before: all failed or errored
(no check, doctor called spark usable, the run test got no `[ERROR]` line). After: pass.
Live: with no Java, `ubunye run` prints one `[ERROR] Spark needs Java, ...` with the
versions, exit 1.
