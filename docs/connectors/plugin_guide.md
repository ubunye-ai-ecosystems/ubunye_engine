# Writing a Plugin

Ubunye's connector and transform system is fully extensible via Python entry points.
Any installed package that declares the right entry points is discovered automatically.

---

## Plugin types

| Type | Interface | Entry point group |
|---|---|---|
| Reader | `ubunye.core.interfaces.Reader` | `ubunye.readers` |
| Writer | `ubunye.core.interfaces.Writer` | `ubunye.writers` |
| Transform | `ubunye.core.interfaces.Transform` | `ubunye.transforms` |
| Monitor | `ubunye.telemetry.monitors.Monitor` (a protocol) | `ubunye.monitors` |
| Secret provider | a class with `get(ref) -> str` (see [Secrets](../config/secrets.md)) | `ubunye.secrets` |

---

## Writing a Reader

```python
# my_package/readers/my_api.py
from ubunye.core.interfaces import Reader

class MyApiReader(Reader):
    CONFIG_KEYS = frozenset({"url", "token"})   # see "Declare your settings" below

    @classmethod
    def validate_config(cls, cfg):
        # Problems found here are reported by `ubunye validate`, before any run.
        return [] if cfg.get("url") else ["format 'my_api' requires 'url'"]

    def read(self, cfg: dict, backend):
        rows = _fetch_all(cfg["url"], cfg.get("token", ""))   # a list of dicts
        # The backend builds its own frame: Spark on spark, pandas on pandas.
        return backend.frame_from_records(rows)
```

`cfg` is this input's block from the config as a dict: `format`, `options` and
every other key in the YAML. `secret://` references in it are already resolved.
`validate_config` and `CONFIG_KEYS` are optional; leave them out and the
connector accepts any block.

---

## Writing a Writer

```python
# my_package/writers/my_sink.py
from ubunye.core.interfaces import Writer

class MySinkWriter(Writer):
    def write(self, df: "pyspark.sql.DataFrame", cfg: dict, backend) -> None:
        url = cfg["url"]
        mode = cfg.get("mode") or "append"
        # ... write df to your sink
        _send_to_sink(df.toPandas(), url, mode)
```

---

## Writing a Transform

```python
# my_package/transforms/my_transform.py
from ubunye.core.interfaces import Transform

class MyTransform(Transform):
    def apply(
        self,
        inputs: dict,           # {logical_name: DataFrame}
        cfg: dict,              # TransformConfig.params dict
        backend,                # the run's backend (see Backend reference)
    ) -> dict:                  # {logical_name: DataFrame}
        df = inputs["source"]
        threshold = cfg.get("threshold", 0.5)
        return {"output": df.filter(f"score > {threshold}")}
```

A task folder does not run transform plugins: `ubunye run` always runs the
task's `transformations.py`, and `transform.type` in its `config.yaml` only gets a
warning. A transform plugin runs when you build an `Engine`
(`ubunye.core.runtime.Engine`) yourself and pass it a config, or when your
`Task` calls it. See [Transform](../config/transform.md).

---

## Registering via entry points

In your package's `pyproject.toml`:

```toml
[project.entry-points."ubunye.readers"]
my_api = "my_package.readers.my_api:MyApiReader"

[project.entry-points."ubunye.writers"]
my_sink = "my_package.writers.my_sink:MySinkWriter"

[project.entry-points."ubunye.transforms"]
my_transform = "my_package.transforms.my_transform:MyTransform"
```

After `pip install -e .` (or publishing to PyPI):

```bash
ubunye plugins
# Should list: my_api, my_sink, my_transform
```

---

## Using your plugin in a config

```yaml
CONFIG:
  inputs:
    data:
      format: my_api          # matches the entry point key
      url: "https://..."
      token: "secret://env/MY_API_TOKEN"

  transform:
    params:
      threshold: 0.7          # read in transformations.py from self.config

  outputs:
    result:
      format: my_sink
      url: "https://..."
```

`ubunye validate` checks that `format` names an installed reader (for an input)
or writer (for an output). An unknown name is refused with the list of
installed ones.

### Declare your settings, so typos fail early

Input and output blocks accept keys the engine does not know, because your
connector reads its own settings. That also means a typo (`tokne:`) is silently
ignored, unless you say which keys you read:

```python
class MyApiReader(Reader):
    CONFIG_KEYS = frozenset({"url", "token"})
```

With `CONFIG_KEYS` declared, `ubunye validate` rejects any other key in a block
that uses your connector and names the closest real one
(`inputs.data: 'tokne' is not a setting of 'my_api'; did you mean 'token'?`).
You do not list `format` or `options` (every block may have them), nor, for a
writer, `mode`, `merge_keys` and `replace_where` (the engine reads those for
every output). Leave `CONFIG_KEYS` out and any key is accepted, as before. Every
built-in connector declares its keys.

---

## Backend reference

The `backend` argument passed to `read`, `write`, and `apply` is the run's
backend: `SparkBackend`, `DatabricksBackend` or `PandasBackend` (`ubunye backends`
lists them). What every backend offers:

```python
backend.is_spark                                  # True on spark and databricks
backend.frame_from_records(rows, schema=None)     # list of dicts -> frame (schema: Spark DDL)
backend.iter_records(frame)                       # frame -> each row as a dict
backend.read_frame(file_format, path, options=..., schema=...)
```

A Spark backend also has `backend.spark` (the `SparkSession`). A connector that
needs Spark should get it with `ubunye.adapters.spark.session.spark_of`, which
raises a clear error on the pandas backend, and declare it so the check happens
before a run:

```python
from ubunye.core.capabilities import SPARK

class MySparkReader(Reader):
    REQUIRES = frozenset({SPARK})   # also: PATH_IO, RECORDS
```

---

## Testing your plugin

Write Spark-free unit tests using mock DataFrames:

```python
import pytest
from my_package.transforms.my_transform import MyTransform

class MockBackend:
    pass

class MockDF:
    def filter(self, expr):
        return self  # stub

def test_apply_passes_threshold():
    t = MyTransform()
    out = t.apply({"source": MockDF()}, {"threshold": 0.5}, MockBackend())
    assert "output" in out
```

For integration tests with real Spark:

```python
from ubunye.backends.spark_backend import SparkBackend

@pytest.fixture(scope="session")
def spark():
    with SparkBackend(app_name="plugin_test") as be:
        yield be.spark
```

---

## Publishing

Package your plugin as a normal Python package and publish to PyPI.
Users install it alongside `ubunye-engine`:

```bash
pip install ubunye-engine my-ubunye-plugin
```

Entry points are discovered automatically: no code changes required in Ubunye.
