# Troubleshooting

Something went wrong and you want it fixed. Each entry starts with what you see, then
says why it happens and what to do. For what each exception class means, see the
[Error Reference](errors.md).

## First step: ask `ubunye doctor`

`ubunye doctor` checks this machine (Python, Java, the backends, the plugins) and, given
a task, the environment variables and secret providers its config needs. It says what
would fail and how to fix it:

```bash
ubunye doctor -d pipelines -u demo -p starter -t filter_adults
```

```
[OK] python: Python 3.12.16 on linux
[OK] ubunye-engine: 0.9.0
[WARN] java: pyspark is installed but no Java was found (JAVA_HOME or java on PATH)
       fix: Install Java 17 (Spark 4) or 11 (Spark 3.5), or run with --backend pandas.
[OK] delta-spark: delta-spark 4.4.1 with Spark 4.2.0
[OK] backend: databricks: usable, needs Java, distributed
[OK] backend: pandas: usable, single machine
[OK] backend: spark (default): usable, needs Java, distributed
[OK] plugins: 18 load
[OK] task filter_adults: environment: every variable it needs is set
[FAIL] task filter_adults: secret provider aws-sm: needs boto3
       fix: pip install boto3
[OK] task filter_adults: config: loads and validates

2 to look at; a run would fail.
```

This machine has no Java, and the task reads a password from AWS Secrets Manager
without the AWS client installed. It exits `1` when a run would fail.

`ubunye plan` then checks the task itself without moving any data. Most problems below
show up in one of these two first.

---

## `Profile 'DEV' not found`

**What you see** when you run a task:

```
ConfigProfileError: Profile 'DEV' not found.

  Profile:   DEV
  Available: ['dev']
  Hint: Valid profiles: dev
```

**Why.** `ubunye run` uses the profile `DEV` when you give no `-m`. Profile names are
matched exactly, case included, and this config's profile is spelled `dev`. (A long
traceback may print above the message; the last lines are the part that matters.)

**Fix.** Either rename the profile in `config.yaml` to `DEV`, or pass the name as
written: `ubunye run ... -m dev`. Use upper case names (`DEV`, `PROD`) to match the
default. A config with no profiles at all runs under any `-m`. Note that
`ubunye export airflow` and `ubunye export databricks` default to `--profile prod`, so
pass `--profile PROD` if your profile is upper case. See
[Engine and Profiles](config/engine.md).

---

## Spark cannot find Java

**What you see** when you run on Spark (no `--backend pandas`):

```
JAVA_HOME is not set
...
PySparkRuntimeError: [JAVA_GATEWAY_EXITED] Java gateway process exited before
sending its port number.
```

**Why.** Spark runs on Java, and none was found on `PATH` or through `JAVA_HOME`.

**Fix.** Install a Java version your Spark supports (the versions are on
[Installation](getting_started/install.md#system-requirements)) and check it with
`java -version`. Or run without Java on pandas: add `--backend pandas`.

---

## The pandas backend cannot serve hive, jdbc, unity or delta

**What you see** from `ubunye plan --backend pandas`:

```
  problem: input 'people' uses the 'hive' connector, which needs spark; the pandas backend does not provide it.

Plan would fail: 1 problem(s) in 1 task(s).
```

and from `ubunye run --backend pandas`:

```
  - input 'people' uses the 'hive' connector, which needs spark; the pandas
backend does not provide it.

  Backend: pandas
  Hint: Run it on a backend that can (--backend spark), or change the inputs and
outputs listed above.
```

**Why.** On pandas, only the `s3` (files on any path, local or cloud) and `rest_api`
connectors work. `hive`, `jdbc`, `unity` and `delta` need Spark. The same check refuses
a file option pandas does not support.

**Fix.** Run on Spark (leave out `--backend pandas`), or read files with `format: s3`.
See [Execution Backends](backends.md).

---

## `Warning: --package is renamed --pipeline`

**What you see:**

```
Warning: --package is renamed --pipeline; it will be removed in 1.0.
```

**Why.** In 0.9 the middle folder level is called a pipeline, so `-p` is now short for
`--pipeline`. The old `--package` still works until 1.0. Giving both is an error:

```
Invalid value for '--pipeline': give the pipeline once: use -p/--pipeline
and drop --package (its old name).
```

**Fix.** Write `-p` or `--pipeline` in your scripts and jobs. In Python, use
`run_pipeline(pipeline=...)`. See [Upgrading](upgrading.md).

---

## `Run refused`: the batch already landed

**What you see** when you run an appending task a second time with the same `-dt`:

```
[ERROR] Run refused for filter_adults: demo/starter/filter_adults already finished this batch (run b88e7ba8 (finished 2026-11-10T10:36:38Z)) and appends to adults: running it again would add the batch twice.

  Finished: .../pipelines/.ubunye/leases/demo/starter/filter_adults/ecdf848508a4535d.finished.json
  Hint: To replace the batch, run again with --rerun (rerun=True in Python). If each run adds new data to the same batch, give each run its own variable, for example --var hour=13.
```

**Why.** A batch is the task with its `-dt` and `--var` values. This one was already
appended by a run that finished, and running it again would write the same rows twice.

**Fix.** Choose one:

- To replace the batch: add `--rerun`. You will see
  `Replaced the batch: removed 1 file(s) of the earlier run.`
- If each run really adds new data: give each run its own variable, for example
  `--var hour=13`.
- To finish a pipeline that stopped half way: add `--resume`. Tasks that finished the
  batch are skipped.

See [Safe Reruns](guides/safe-reruns.md).

---

## `Unknown field 'ENGNE'`

**What you see:**

```
[ERROR] Config validation failed for 'filter_adults':
Unknown fields in pipelines/demo/starter/filter_adults/config.yaml:

  (top level):
    Unknown field 'ENGNE'
    Did you mean 'ENGINE'?
```

**Why.** A key in `config.yaml` is misspelled. Ubunye refuses keys it does not know,
so a typo cannot be silently ignored.

**Fix.** Use the spelling it suggests. `ubunye validate` finds these without running
anything. See [Config Overview](config/overview.md).

---

## A secret provider package is missing

**What you see** when a config uses, for example, `secret://aws-sm/...`:

```
SecretError: Could not read secret secret://aws-sm/prod/shop-db#password:
ModuleNotFoundError: No module named 'boto3'

  Provider:  aws-sm
  Reference: prod/shop-db#password
  Hint: pip install 'ubunye-engine[aws]'; the login is the AWS SDK's (profile,
role, env).
```

**Why.** Each cloud secret store needs its own client library, and it is not installed.

**Fix.** Install the extra the hint names: `[aws]` for `aws-sm`, `[gcp]` for `gcp-sm`,
`[azure]` for `azure-kv`, `[databricks]` for `databricks` outside a cluster.
`ubunye doctor` with a task finds this before a run. See [Secrets](config/secrets.md).

---

## `PySpark does not yet fully support pandas >= 3.0.0`

**What you see** when Spark starts:

```
FutureWarning: PySpark does not yet fully support pandas >= 3.0.0. Some features may not work correctly. It is recommended to use pandas < 3.0.0 for now.
```

**Why.** You installed the `pandas` and `spark` extras together and pip chose pandas 3.
PySpark prints this warning for any pandas 3.

**Fix.** Nothing to fix: it is expected and the run carries on. To silence it, install
`"pandas<3"`. See [Installation](getting_started/install.md).

---

## Still stuck?

- Every exception class, with what it means: [Error Reference](errors.md).
- Every command and flag: [CLI Reference](cli.md).
- Ask, or report a bug, on
  [GitHub](https://github.com/ubunye-ai-ecosystems/ubunye_engine/issues).
