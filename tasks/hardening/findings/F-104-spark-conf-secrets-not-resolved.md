# F-104: `secret://` in ENGINE.spark_conf reached Spark as literal text

**Status:** fixed (0.9.1)
**Severity:** major (a storage key set as a reference never worked, and nothing said so)
**Source:** docs audit (0.9.1)
**Promise:** a config names a secret, never holds one (docs/config/secrets.md)

## What happened
Secret references were checked (`ubunye validate`) and resolved (the engine, just
before a connector reads or writes) only inside `CONFIG.inputs` and `CONFIG.outputs`.
`ENGINE.spark_conf` was neither: a misspelt provider validated, and
`fs.azure.account.key...: secret://azure-kv/...` reached Spark as that text.

## Fix
- `EngineConfig` checks the references in `spark_conf` and in each profile's
  `spark_conf` with the same `secrets.problems` the connectors use. Nothing is fetched.
- `core/backends.spark_conf_for(cls, conf)` resolves them with the same
  `secrets.resolve`, for a backend that offers Spark, where every backend is built
  (`create`, the platform attach, and `run_task(spark=...)`). Each value is
  remembered for masking, so it never shows in logs or run records. The config, its
  hash and the run record keep the reference.
- A backend without Spark (pandas) only reads the time zone key, so it is not made
  to fetch secrets it never uses: a task that runs on pandas today still does.

## Evidence
`tests/unit/config/test_spark_conf_secrets.py`: the base and profile validation, and
the Spark backend getting the value (masked), fail before and pass after; the pandas
case passes both ways.
