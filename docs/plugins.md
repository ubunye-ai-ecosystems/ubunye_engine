
---

# 📄 `docs/plugins.md`
```markdown
# Plugins

Ubunye plugins extend the engine by adding new readers, writers, transforms, and ML backends.

---

## 🔌 How plugins work
- Plugins are Python classes implementing `Reader`, `Writer`, `Transform`, or `BaseModel`.
- Registered via [entry points](https://setuptools.pypa.io/en/latest/userguide/entry_point.html).
- Discovered automatically at runtime by the Registry.

---

## 📚 Built-in plugins

### Readers
- **hive** – read from Hive tables
- **jdbc** – read from JDBC sources (Postgres, MySQL, …)
- **delta** – read Delta tables by path or name
- **unity** – read Unity Catalog tables

### Writers
- **s3** – write to S3 path (Parquet/CSV/JSON)
- **jdbc** – write to JDBC tables
- **delta** – write Delta tables
- **unity** – write Unity Catalog tables

### Transforms
- **noop** – pass-through (identity)
- Custom: user-defined `feature_class.py`

### ML
- **sklearn** – wrap scikit-learn estimators
- **torch** – wrap PyTorch models
- **sparkml** – wrap Spark ML pipelines

---

## 🛠 Creating a new plugin

1. Write a class implementing the right interface:
```python
from ubunye.core.interfaces import Reader

class MongoReader(Reader):
    def read(self, cfg: dict, backend) -> Any:
        # connect + load into DataFrame
        ...
```
2. Register it in pyproject.toml:
```bash
[project.entry-points."ubunye.readers"]
mongo = "my_pkg.mongo_reader:MongoReader"
```
3. Install the package:
```bash
   pip install -e .
```
4. verify:
```bash
ubunye plugins
```

