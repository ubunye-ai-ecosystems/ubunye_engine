# Quickstart

Your first task, running on your laptop in a few minutes, with no Java and no
cloud account. For anyone new to Ubunye. The commands in step 2 are run by the
test suite exactly as written, on every change, so they work as shown.

New words (use case, pipeline, task, run record) are explained in
[Concepts](concepts.md).

---

## 1. Install

```bash
pip install "ubunye-engine[pandas]"
```

The `pandas` extra is what lets a task run with no Spark and no Java. (Add
`[spark]` later to run the same task on Spark.)

---

## 2. Make a task, check it, run it

<!-- quickstart:begin -->
```bash
ubunye init -d pipelines -u demo -p starter -t filter_adults
ubunye plan -d pipelines -u demo -p starter -t filter_adults --backend pandas
ubunye run -d pipelines -u demo -p starter -t filter_adults --backend pandas --lineage
ubunye lineage list -d pipelines -u demo -p starter -t filter_adults
```
<!-- quickstart:end -->

What each one did:

1. **`init`** made a task folder, and the folder is the whole task. `-d` is the
   pipelines folder, `-u` the use case, `-p` the pipeline, `-t` the task:

    ```
    pipelines/demo/starter/filter_adults/
        config.yaml          reads data/people.csv, writes output/adults as Parquet
        transformations.py   keeps the people aged 18 and over
        data/people.csv      eight people, some of them children
        notebooks/           a notebook for trying things step by step
    ```

    In `config.yaml`, input and output both say `format: s3`. That is the file
    connector: `s3` means any file path, on your disk or in a cloud bucket, not only
    Amazon S3.

2. **`plan`** checked everything it could without moving any data: the input
   file is there, the transform loads, the write mode makes sense, and the
   pandas backend can do all of it. It exits `1` if anything would stop the run.

3. **`run`** read the CSV, kept the adults, and wrote
   `pipelines/demo/starter/filter_adults/output/adults/` as Parquet. With
   `--backend pandas` that happens in plain Python, no Java involved.

4. **`lineage list`** shows the run record the run left (`--lineage` wrote it):
   when it ran, how it went, how many rows came out. Each record also keeps a
   hash of every row read and written, so two runs can be compared exactly.

    ```
    RUN_ID     STARTED              STATUS        DUR  ROWS
    ------------------------------------------------------------
    dede6d1c  2026-11-10T10:34:34  success      0.3s  in:8  out:5
    ```

    Your run ID and time will differ.

If a command fails because of the machine (no Java, an old pyarrow, a missing
environment variable), `ubunye doctor` says which, and how to fix it. See
[Troubleshooting](../troubleshooting.md).

---

## 3. Change it

Open `transformations.py`:

```python
class FilterAdults(Task):
    def transform(self, sources):
        people = sources["people"]
        return {"adults": people[people["age"] >= 18]}
```

Change `18` to `21` and run it again, with `--lineage` so it leaves a record:

```bash
ubunye run -d pipelines -u demo -p starter -t filter_adults --backend pandas --lineage
ubunye lineage list -d pipelines -u demo -p starter -t filter_adults
```

The list now shows two runs, newest first:

```
RUN_ID     STARTED              STATUS        DUR  ROWS
------------------------------------------------------------
9913a5f1  2026-11-10T10:34:39  success      0.3s  in:8  out:4
dede6d1c  2026-11-10T10:34:34  success      0.3s  in:8  out:5
```

Compare them. Put your own two run IDs in place of these:

```bash
ubunye lineage compare -d pipelines -u demo -p starter -t filter_adults \
  --run-id1 dede6d1c --run-id2 9913a5f1
```

```
  code_hash: sha256:b6e66360... -> sha256:49d550bd...  CHANGED
  Input 'people':
      row_count: 8  (unchanged)
      data_hash: sha256:00d7526f...  (unchanged)
  Output 'adults':
      row_count: 5 -> 4  CHANGED
      data_hash: sha256:622fcbc6... -> sha256:7420fc72...  CHANGED
```

(Shortened here: the real output prints full hashes, plus the config, schema and
environment hashes.) The code changed, the input stayed the same, and the output
lost one row.

The line `people[people["age"] >= 18]` means the same thing in pandas and in
Spark, which is why this task can run on either.

---

## 4. Run the same folder on Spark

You need Java ([which version](install.md#system-requirements)) and
`pip install "ubunye-engine[spark]"`. Change `21` back to `18` in
`transformations.py`, then run without `--backend`:

```bash
ubunye run -d pipelines -u demo -p starter -t filter_adults --lineage
```

Now check that Spark wrote exactly what pandas wrote. List the runs, then compare
the Spark run (the newest) with your first pandas run (the oldest, the same code):

```bash
ubunye lineage list -d pipelines -u demo -p starter -t filter_adults
ubunye lineage compare -d pipelines -u demo -p starter -t filter_adults \
  --run-id1 <pandas run id> --run-id2 <spark run id>
```

When the two backends agree, every hash says `(unchanged)` and only the time
differs:

```
  status: success  (unchanged)
  duration_sec: 0.27769826199801173 -> 2.659492216000217  CHANGED
  config_hash: sha256:8bd62c96...  (unchanged)
  code_hash: sha256:b6e66360...  (unchanged)
  Input 'people':
      row_count: 8  (unchanged)
      data_hash: sha256:00d7526f...  (unchanged)
  Output 'adults':
      row_count: 5  (unchanged)
      schema_hash: sha256:9c8ec3b8...  (unchanged)
      data_hash: sha256:622fcbc6...  (unchanged)
```

Same folder, same config, same rows. On Databricks the notebook's session is used
automatically.

---

## 5. From Python

```python
import ubunye

outputs = ubunye.run_task("pipelines/demo/starter/filter_adults", backend="pandas")
print(len(outputs["adults"]))   # a pandas DataFrame
```

---

## Common errors

Every error says what went wrong, where, and what to try.

More symptoms and fixes: [Troubleshooting](../troubleshooting.md).

**A typo in a field name**

```
Unknown fields in pipelines/demo/starter/filter_adults/config.yaml:

  (top level):
    Unknown field 'ENGNE'
    Did you mean 'ENGINE'?
```

**A template variable with no value**

```
Template resolution failed for .../config.yaml:
  Undefined variable 'region' in config value '.../{{ region }}'. ...
```

Pass it with `--var region=gauteng`, or give it a default:
`{{ region | default('gauteng') }}`.

**Something the backend cannot do**

```
[ERROR] Run refused for copy: This task cannot run on the pandas backend:
  - input 'orders' uses the 'hive' connector, which needs spark; the pandas backend does not provide it.
```

`ubunye plan --backend pandas` finds these before anything runs.

See the full [Error Reference](../errors.md).

---

## What's next?

| Topic | Link |
|---|---|
| The words, in one page | [Concepts](concepts.md) |
| How a config is laid out | [Config Reference](../config/overview.md) |
| Reading and writing data | [Connectors](../connectors/overview.md) |
| Choosing Spark or pandas | [Execution Backends](../backends.md) |
| Every command and flag | [CLI Reference](../cli.md) |
| Running from Python and notebooks | [API Reference](../api.md) |
| Deploying to Databricks | [Deployment](../deployment/databricks.md) |
| Training and versioning models | [Model Contract](../ml/model_contract.md) |
