# Transform

`CONFIG.transform` is the part of `config.yaml` that sits between reading inputs and
writing outputs. This page is for anyone writing a task: it shows where your code
goes, how to give it settings, and how to train a model inside a task.

---

## Structure

```yaml
CONFIG:
  transform: {}
```

A task folder always runs the `Task` subclass in its `transformations.py`. That holds for
`ubunye run`, `run_task`, `run_pipeline` and notebooks. Leave `transform` as `{}`, or
give it `params` (below).

---

## The Task class

```yaml
transform: {}
```

**`transformations.py`:**

```python
from ubunye.core.interfaces import Task

class MyTask(Task):
    def transform(self, sources: dict) -> dict:
        df = sources["input_name"]
        cleaned = df.filter("value IS NOT NULL").dropDuplicates(["id"])
        return {"output_name": cleaned}
```

The logical names in `sources` and the returned dict must match the keys declared
under `CONFIG.inputs` and `CONFIG.outputs`.

### Settings for your Task: `params`

Put your own settings under `transform.params` and read them from `self.config`, the
whole config as a dict. Jinja variables work here too, so a setting can come from
`--var` or the data timestamp.

A value that comes from `--var` arrives as text: `--var min_amount=30` gives the
string `'30'`, not the number 30. Convert it in your code (`int(params["min_amount"])`).
A number written straight into the YAML (`min_amount: 10`) stays a number.

```yaml
CONFIG:
  transform:
    params:
      only_states: ["SP", "RJ"]
      min_amount: 10
```

```python
class MyTask(Task):
    def transform(self, sources: dict) -> dict:
        params = self.config["CONFIG"]["transform"]["params"]
        orders = sources["orders"]
        orders = orders[orders["customer_state"].isin(params["only_states"])]
        return {"orders_out": orders}
```

`params` is the only key besides `type` (see the note at the end of this page); any
other key under `transform` is refused by `ubunye validate` with the list of valid
fields.

---

## Training a model

Train and register the model in `transformations.py`. A model registered inside a run
records that run's id (`lineage_run_id`), so `ubunye lineage show` finds the data it was
trained on. See [Model Contract](../ml/model_contract.md) and
[Model Registry](../ml/registry.md).

```python
# transformations.py
from ubunye.core.interfaces import Task
from ubunye.models.registry import ModelRegistry, ModelStage

from model import FraudRiskModel  # model.py in the task folder


class TrainFraudModel(Task):
    def transform(self, sources: dict) -> dict:
        features = sources["features"]
        model = FraudRiskModel()
        metrics = model.train(features)

        registry = ModelRegistry(".ubunye/model_store")
        mv = registry.register("fraud_detection", "FraudRiskModel", None, model, metrics)
        registry.promote(
            "fraud_detection",
            "FraudRiskModel",
            mv.version,
            ModelStage.STAGING,
            gates={"min_auc": 0.85, "min_f1": 0.80},
        )
        # Write what the model was trained on, so the run record hashes it.
        return {"training_set": features}
```

A gate that fails raises `PromotionBlockedError`, which fails the run. The version stays
registered in `development`. Catch the error if the run should still succeed.

To have the run record name the model, its version, stage and gate result, train
with `ModelTransform` from `transformations.py` instead (see
[Model Registry: the run record names the model](../ml/registry.md#the-run-record-names-the-model-09)).
There, `registry.on_blocked: record` keeps a blocked run going.

---

## `ModelTransform` from Python

`ModelTransform` trains or scores a model from a settings dict. Call it yourself,
for example from a notebook or from your `Task`. It takes the settings flat or under
`params:`. The third argument is the run's backend; pass `None` when you call it
outside a run.

```python
from ubunye.plugins.transforms.model_transform import ModelTransform

ModelTransform().apply(
    {"features": df},
    {
        "action": "train",
        "model_class": "model.FraudRiskModel",
        "registry": {"store": ".ubunye/model_store", "use_case": "fraud_detection"},
    },
    None,  # the backend: None when you call it yourself
)
```

| Field | Type | Default | Description |
|---|---|---|---|
| `action` | `train` \| `predict` | required | Whether to train or score |
| `model_class` | string | required | `module.ClassName` of the `UbunyeModel` subclass |
| `model_dir` | string | `null` | Folder holding the model file; defaults to `sys.path` |
| `model_path` | string | `null` | Path to a saved artifact (predict without a registry) |
| `input_name` | string | `null` | Key in `inputs` to train or score on |
| `registry` | [registry settings](#registry-settings) | `null` | Model registry settings |

### Registry settings

| Field | Type | Default | Description |
|---|---|---|---|
| `store` | string | required | Path or URI of the model store |
| `backend` | string | `null` | Where the registry keeps models. Not set: the `UBUNYE_REGISTRY_BACKEND` environment variable, else `filesystem`. See [Model Registry](../ml/registry.md) |
| `use_case` | string | `"default"` | Logical grouping for the model |
| `version` | string | `null` | Explicit version; generated for you if `null` |
| `auto_version` | bool | `true` | Accepted, but has no effect today: a version is generated whenever `version` is `null` |
| `promote_to` | `development` \| `staging` \| `production` | `null` | Promote after registration |
| `use_stage` | `development` \| `staging` \| `production` | `"production"` | Stage to load from (predict only) |
| `promotion_gates` | dict | `null` | Metric thresholds that must pass before promotion |
| `on_blocked` | `raise` \| `record` | `raise` | What a promotion the gates block does. `raise` fails the run. `record` keeps the run going; the version stays in `development` and the run record says which gate blocked it |
| `stamp_columns` | bool | `false` | Predict only: add `model_version` and `model_stage` columns to the scored rows |
| `check_features` | bool | `null` | Predict only: compare the frame with the model's declared features. Not set: warn and call the model. `true`: refuse the frame before the model is called. `false`: no check |

---

## Transform plugins

Classes registered under the `ubunye.transforms` entry point run when you build an
`Engine` yourself and pass it a config; a task folder does not run them. See
[Writing a Plugin](../connectors/plugin_guide.md).

---

## `type:` in a task's config.yaml is not run

!!! note "A `type:` line only gets a warning"
    A task's config may name a transform plugin (`transform: {type: model}`, or your
    own). In 0.9.0 the type is still **not run**: `transformations.py` runs instead.
    `ubunye validate`, `ubunye plan` and `ubunye run` print a warning about it, and
    the run carries on. Move the logic into `transformations.py` (see
    [Training a model](#training-a-model)) and remove the `type` line.
    `type: noop` is accepted and does nothing.
