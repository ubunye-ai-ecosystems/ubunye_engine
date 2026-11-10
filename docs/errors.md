# Error Reference

Every exception Ubunye raises, for people who catch errors in Python. If you
have an error message and want the fix, start at
[Troubleshooting](troubleshooting.md) instead.

Every user-facing error inherits from `UbunyeError` **and** the standard Python
type it replaces, so existing `except ValueError` or `except KeyError` catches
keep working.

---

## Catching errors

```python
import ubunye
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
except KeyError:            # catches ReaderNotFoundError, WriterNotFoundError, SecretError, FeatureContractError
except ValueError:          # catches ConfigError, RegistryError subtypes, ExpectationError
except RuntimeError:        # catches SparkSessionError, ModelNotFittedError, DeployError, LLMError, RunLeaseHeld
```

---

## Exception hierarchy

```
UbunyeError(Exception)
├── ConfigError(UbunyeError, ValueError)
│   ├── ConfigFieldError          unknown config fields, with typo suggestions
│   ├── ConfigTemplateError       undefined Jinja variables, unset env variables
│   └── ConfigProfileError        a profile name the config does not define
├── TaskNotFoundError(UbunyeError, FileNotFoundError)
├── TaskClassMissingError(UbunyeError, RuntimeError)
├── PluginNotFoundError(UbunyeError, KeyError)
│   ├── ReaderNotFoundError
│   ├── WriterNotFoundError
│   ├── TransformNotFoundError
│   ├── MonitorNotFoundError
│   ├── BackendNotFoundError      no backend under that name, or it failed to load
│   └── RegistryBackendNotFoundError  no registry backend under that name, or it failed to load
├── SourceReadError(UbunyeError, IOError)
├── SinkWriteError(UbunyeError, IOError)
├── TransformOutputError(UbunyeError, TypeError)
├── SecretError(UbunyeError, KeyError)          a secret:// reference cannot be read
├── LLMError(UbunyeError, RuntimeError)         a model call fails or cannot be made
│   └── LLMBudgetError                          a call refused before sending: it could pass a limit
├── ExpectationError(UbunyeError, ValueError)   an output breaks a `fail` expectation; nothing written
├── BackendCapabilityError(UbunyeError, ValueError)  the task needs something the backend cannot do
├── SparkSessionError(UbunyeError, RuntimeError)
├── ModelLoadError(UbunyeError, ImportError)
├── ModelNotFittedError(UbunyeError, RuntimeError)
├── FeatureContractError(UbunyeError, KeyError)  at predict, the frame lacks a feature the model declared
├── RegistryError(UbunyeError, ValueError)
│   ├── VersionExistsError
│   ├── VersionNotFoundError
│   └── PromotionBlockedError
├── RegistryNotFoundError(UbunyeError, FileNotFoundError)
├── LineageRecordNotFoundError(UbunyeError, FileNotFoundError)
├── RunLeaseHeld(UbunyeError, RuntimeError)     another live run holds this task and batch
│   └── BatchFinished                           a finished run already appended this batch; --rerun replaces it
├── RunLeaseLost(UbunyeError, RuntimeError)     this run's lease was taken over; it stops
├── UpstreamNotReady(UbunyeError, RuntimeError) a task in CONFIG.requires has not succeeded for this batch
└── DeployError(UbunyeError, RuntimeError)
    ├── AuthNotFoundError         Databricks credentials are missing
    ├── AuthInvalidError          Databricks credentials are rejected
    ├── TargetNotFoundError       the deploy target is not in targets.yaml
    ├── WorkspaceUploadError      uploading files to the workspace failed
    └── BundleDeployError         creating or updating the job failed
```

`SecretError` and `LLMError` never carry the secret or the key.
`RunLeaseHeld`, `BatchFinished` and `RunLeaseLost` are explained in
[Safe reruns](guides/safe-reruns.md); `FeatureContractError` in
[ADR 012](architecture/adr-012-model-runs-and-feature-contract.md).

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
  Installed: ['binary', 'delta', 'hive', 'jdbc', 'rest_api', 's3', 'unity']
  Hint: Check the 'format' field in CONFIG.inputs.source. Installed reader plugins: binary, delta, hive, jdbc, rest_api, s3, unity
```

**Invalid profile**

```
ConfigProfileError: Profile 'staging' not found.

  Profile:   staging
  Available: ['DEV', 'PROD']
  Hint: This config's profiles are: DEV, PROD. Names match exactly, case included. With no -m, run, validate, plan and doctor use DEV.
```

**Missing env variable (Jinja)**

```
ConfigTemplateError: Template resolution failed for pipelines/demo/etl/hello_world/config.yaml:
  Environment variable 'DB_PASS' is not set (referenced in '{{ env.DB_PASS }}'). Set it in your environment or use {{ env.DB_PASS | default('fallback') }}.
```

An undefined variable that is not under `env` gives `Undefined variable 'name' in config value ...`,
with the list of variables that are available.

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

# Also available from ubunye.core (all but SecretError, LLMError,
# LLMBudgetError and ExpectationError, which only ubunye.core.errors has)
from ubunye.core import TaskNotFoundError

# Elsewhere
from ubunye.core.runs import RunLeaseHeld, BatchFinished, RunLeaseLost
from ubunye.lineage.upstream import UpstreamNotReady
from ubunye.models.backends import RegistryBackendNotFoundError
```
