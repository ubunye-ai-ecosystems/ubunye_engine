# Transform

`CONFIG.transform` declares what happens between reading inputs and writing outputs.

---

## Structure

```yaml
CONFIG:
  transform:
    type: noop      # required — transform type name
    params: {}      # optional — passed to the transform plugin
```

---

## Built-in transform types

### `noop` — pass-through

The default transform. Passes all inputs straight to outputs with no modification.
Useful for pure connector moves (e.g. Hive → Delta).

```yaml
transform:
  type: noop
```

---

### `task` — Python Task class

Loads `transformations.py` from the task directory and calls the `Task` subclass.

```yaml
transform:
  type: task
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

---

### `model` — ML model train/predict

Runs a `UbunyeModel` subclass for training or inference.
See [Model Contract](../ml/model_contract.md) and [Model Registry](../ml/registry.md).

```yaml
transform:
  type: model
  params:
    action: train                          # train | predict
    model_class: "model.FraudRiskModel"    # module.ClassName (model.py in task dir)
    registry:
      store: ".ubunye/model_store"
      use_case: fraud_detection
      auto_version: true
      promote_to: staging
      promotion_gates:
        min_auc: 0.85
        min_f1: 0.80
```

#### `ModelTransformParams` fields

| Field | Type | Default | Description |
|---|---|---|---|
| `action` | `train` \| `predict` | required | Whether to train or score |
| `model_class` | string | required | `module.ClassName` of the `UbunyeModel` subclass |
| `model_dir` | string | `null` | Directory containing the model file; defaults to task dir |
| `model_path` | string | `null` | Path to saved artifact (used for predict without registry) |
| `input_name` | string | `null` | Key in `inputs` dict to use as training/scoring data |
| `registry` | [RegistryConfig](#registryconfig) | `null` | Model registry settings |

#### `RegistryConfig` fields

| Field | Type | Default | Description |
|---|---|---|---|
| `store` | string | required | Filesystem path for the model store |
| `use_case` | string | `"default"` | Logical grouping for the model |
| `version` | string | `null` | Explicit version; auto-generated if `null` |
| `auto_version` | bool | `true` | Bump patch version automatically |
| `promote_to` | `development` \| `staging` \| `production` | `null` | Promote after registration |
| `use_stage` | `development` \| `staging` \| `production` | `"production"` | Stage to load from (predict only) |
| `promotion_gates` | dict | `null` | Metric thresholds that must pass before promotion |

---

### Custom transforms

Any class registered under the `ubunye.transforms` entry point can be used as a `type`.
See [Writing a Plugin](../connectors/plugin_guide.md).

---

## Transform params and extra fields

`TransformConfig.params` is a `Dict[str, Any]`, so you can pass arbitrary keys:

```yaml
transform:
  type: my_custom_transform
  params:
    window_days: 30
    feature_cols: [f1, f2, f3]
    threshold: 0.5
```

These are passed directly to your plugin's `apply(inputs, cfg, backend)` call as `cfg`.
