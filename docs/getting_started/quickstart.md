# Quickstart

Your first pipeline, running on your laptop in a few minutes, with no Java and
no cloud account. The commands in step 2 are run by the test suite exactly as
written, on every change, so they work as shown.

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

1. **`init`** made a folder, and the folder is the whole task:

    ```
    pipelines/demo/starter/filter_adults/
        config.yaml          reads data/people.csv, writes output/adults as Parquet
        transformations.py   keeps the people aged 18 and over
        data/people.csv      eight people, some of them children
        notebooks/           a notebook for trying things step by step
    ```

2. **`plan`** checked everything it could without moving any data: the input
   file is there, the transform loads, the write mode makes sense, and the
   pandas backend can do all of it. It exits `1` if anything would stop the run.

3. **`run`** read the CSV, kept the adults, and wrote
   `pipelines/demo/starter/filter_adults/output/adults/` as Parquet. With
   `--backend pandas` that happens in plain Python, no Java involved.

4. **`lineage list`** shows the record the run left: when it ran, how it went,
   how many rows came out. Each record also keeps a hash of every row written,
   so two runs can be compared exactly (`ubunye lineage compare`).

---

## 3. Change it

Open `transformations.py`:

```python
class FilterAdults(Task):
    def transform(self, sources):
        people = sources["people"]
        return {"adults": people[people["age"] >= 18]}
```

Change `18` to `21` and run it again. `ubunye lineage list` now shows two runs;
`ubunye lineage compare ... --run-id1 <first> --run-id2 <second>` shows that
the row count and the data hash changed.

The line `people[people["age"] >= 18]` means the same thing in pandas and in
Spark, which is why this task can run on either.

---

## 4. Run the same folder on Spark

With Java installed and `pip install "ubunye-engine[spark]"`, leave out
`--backend`:

```bash
ubunye run -d pipelines -u demo -p starter -t filter_adults --lineage
```

Same folder, same config, same result: the run record carries the same data
hash as the pandas run. On Databricks the notebook's session is used
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
This task cannot run on the pandas backend:
  - input 'orders' uses the 'hive' connector, which needs spark; the pandas backend does not provide it.
```

`ubunye plan --backend pandas` finds these before anything runs.

See the full [Error Reference](../errors.md).

---

## What's next?

| Topic | Link |
|---|---|
| How a config is laid out | [Config Reference](../config/overview.md) |
| Reading and writing data | [Connectors](../connectors/overview.md) |
| Choosing Spark or pandas | [Execution Backends](../backends.md) |
| Every command and flag | [CLI Reference](../cli.md) |
| Running from Python and notebooks | [API Reference](../api.md) |
| Deploying to Databricks | [Deployment](../deployment/databricks.md) |
| Training and versioning models | [Model Contract](../ml/model_contract.md) |
