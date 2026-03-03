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
| Monitor | `ubunye.telemetry.monitors.Monitor` | `ubunye.monitors` |

---

## Writing a Reader

```python
# my_package/readers/my_api.py
from ubunye.core.interfaces import Reader

class MyApiReader(Reader):
    def read(self, cfg: dict, backend) -> "pyspark.sql.DataFrame":
        spark = backend.spark
        url = cfg["url"]
        token = cfg.get("token", "")
        # ... fetch data, convert to Spark DataFrame
        rows = _fetch_all(url, token)
        return spark.createDataFrame(rows)
```

`cfg` is the full `IOConfig.model_dump()` for this input — includes `format`, `options`,
and any extra fields declared in the YAML.

---

## Writing a Writer

```python
# my_package/writers/my_sink.py
from ubunye.core.interfaces import Writer

class MySinkWriter(Writer):
    def write(self, df: "pyspark.sql.DataFrame", cfg: dict, backend) -> None:
        url = cfg["url"]
        mode = cfg.get("mode", "append")
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
        backend,                # SparkBackend (has .spark)
    ) -> dict:                  # {logical_name: DataFrame}
        df = inputs["source"]
        threshold = cfg.get("threshold", 0.5)
        return {"output": df.filter(f"score > {threshold}")}
```

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
      token: "{{ env.TOKEN }}"

  transform:
    type: my_transform
    params:
      threshold: 0.7

  outputs:
    result:
      format: my_sink
      url: "https://..."
```

---

## Backend reference

The `backend` argument passed to `read`, `write`, and `apply` is a `SparkBackend` instance:

```python
backend.spark          # pyspark.sql.SparkSession
backend.spark_conf     # dict of active Spark config
```

For Spark-free plugins (e.g. pure REST connectors), you can ignore `backend`.

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

Entry points are discovered automatically — no code changes required in Ubunye.
