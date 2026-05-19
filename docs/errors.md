# Error Reference

Ubunye Engine uses a structured exception hierarchy. Every user-facing error
inherits from `UbunyeError` **and** the stdlib type it replaces, so existing
`except ValueError` or `except KeyError` catches continue to work.

---

## Catching errors

```python
from ubunye import UbunyeError

try:
    ubunye.run_task(task_dir="pipelines/fraud/ingestion/claim_etl")
except UbunyeError as e:
    print(e)           # formatted message with context + hint
    print(e.context)   # dict of relevant facts
    print(e.hint)      # suggested fix (or None)
```

Narrow catches still work via dual inheritance:

```python
except FileNotFoundError:   # catches TaskNotFoundError, RegistryNotFoundError
except KeyError:            # catches ReaderNotFoundError, WriterNotFoundError
except ValueError:          # catches ConfigError, RegistryError subtypes
except RuntimeError:        # catches SparkSessionError, ModelNotFittedError
```

---

## Exception hierarchy

```
UbunyeError(Exception)
├── ConfigError(UbunyeError, ValueError)
│   ├── ConfigFieldError          — unknown config fields with typo suggestions
│   ├── ConfigTemplateError       — undefined Jinja template variables
│   └── ConfigProfileError        — invalid profile name
├── TaskNotFoundError(UbunyeError, FileNotFoundError)
├── TaskClassMissingError(UbunyeError, RuntimeError)
├── PluginNotFoundError(UbunyeError, KeyError)
│   ├── ReaderNotFoundError
│   ├── WriterNotFoundError
│   ├── TransformNotFoundError
│   └── MonitorNotFoundError
├── SourceReadError(UbunyeError, IOError)
├── SinkWriteError(UbunyeError, IOError)
├── TransformOutputError(UbunyeError, TypeError)
├── SparkSessionError(UbunyeError, RuntimeError)
├── ModelLoadError(UbunyeError, ImportError)
├── ModelNotFittedError(UbunyeError, RuntimeError)
├── RegistryError(UbunyeError, ValueError)
│   ├── VersionExistsError
│   ├── VersionNotFoundError
│   └── PromotionBlockedError
├── RegistryNotFoundError(UbunyeError, FileNotFoundError)
└── LineageRecordNotFoundError(UbunyeError, FileNotFoundError)
```

---

## Example error output

**Missing transformations.py**

```
TaskNotFoundError: Missing transformations.py at pipelines/demo/etl/hello_world/transformations.py.

  Task dir:      pipelines/demo/etl/hello_world
  Expected file: pipelines/demo/etl/hello_world/transformations.py
  Hint: Run 'ubunye init' to scaffold a new task, or check the directory path.
```

**Unknown reader format**

```
ReaderNotFoundError: Reader plugin 'hve' not found.

  Format:    hve
  Input:     source
  Installed: ['hive', 'jdbc', 'rest_api', 's3', 'unity']
  Hint: Check the 'format' field in CONFIG.inputs.source. Installed reader plugins: hive, jdbc, rest_api, s3, unity
```

**Invalid profile**

```
ConfigProfileError: Profile 'staging' not found.

  Profile:   staging
  Available: ['dev', 'prod']
  Hint: Valid profiles: dev, prod
```

**Missing env variable (Jinja)**

```
ConfigTemplateError: Template resolution failed for config.yaml:
  Undefined variable 'DB_PASS' in config value '{{ env.DB_PASS }}'.
  Available variables: ['env']. Use '| default(...)' for optional values.
```

---

## Importing exceptions

```python
# Broadest catch
from ubunye import UbunyeError

# By category
from ubunye.core.errors import (
    ConfigError,
    SourceReadError,
    SinkWriteError,
    TaskNotFoundError,
    ReaderNotFoundError,
)

# Also available from ubunye.core
from ubunye.core import TaskNotFoundError
```
