"""A task as a Spark Declarative Pipeline (Spark 4.1+): ``spark-pipeline.yml`` and code.

Spark 4.1 ships its own pipeline format: a ``spark-pipeline.yml`` naming a storage
location and the source files, and Python functions decorated as datasets, run with
``spark-pipelines run``. This writes a task in that format so it can run where only
Spark is installed, or be handed to a team that standardised on it:

- every **input** becomes a temporary view that reads what the config says;
- the task's own ``transform()`` runs, unchanged (its code is copied next to the
  pipeline, outside the folder Spark scans for definitions);
- every **output** becomes a materialized view of the same name.

What does not carry over, and is reported instead of dropped: write modes other
than a full rewrite (a materialized view is recomputed; ``merge`` is not), output
paths (the pipeline stores its views in its catalog), ``CONFIG.expectations`` and
``secret://`` references. Only readers Spark can express directly are supported:
path formats (``s3``, ``delta``, ``binary``) and tables (``hive``, ``unity``).
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any, Dict, List, Mapping

import yaml

from ubunye.core.errors import ConfigError

_MODULE = '''\
"""Written by `ubunye export spark-pipeline`: {task_path} as Spark Declarative Pipeline datasets.

Inputs are temporary views; the task's transform() runs unchanged; each output is a
materialized view. Regenerate this file rather than editing it.
"""

import importlib.util
import sys
from pathlib import Path

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

_TASK_DIR = Path(__file__).resolve().parent.parent / "task"
_INPUTS = {inputs!r}
_OUTPUTS = {outputs!r}
_CONFIG = {config!r}


def _task():
    sys.path.insert(0, str(_TASK_DIR))
    spec = importlib.util.spec_from_file_location("ubunye_task_{slug}", _TASK_DIR / "transformations.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    from ubunye.core.interfaces import Task

    classes = [
        v for v in vars(module).values()
        if isinstance(v, type) and issubclass(v, Task) and v is not Task
    ]
    task = classes[0](config=_CONFIG)
    task.setup()
    return task


def _spark():
    """The pipeline's session: the active one, or the `spark` Spark puts in scope."""
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    import builtins

    return globals().get("spark") or getattr(builtins, "spark")


def _read(name):
    spec = _INPUTS[name]
    spark = _spark()
    if spec["kind"] == "table":
        return spark.read.table(spec["table"])
    reader = spark.read.format(spec["format"])
    for key, value in spec["options"].items():
        reader = reader.option(key, value)
    return reader.load(spec["path"])


def _outputs():
    sources = {{name: spark_read_view(name) for name in _INPUTS}}
    return _task().transform(sources)


def spark_read_view(name):
    return _spark().read.table("ubunye_input_" + name)


for _name in _INPUTS:
    dp.temporary_view(name="ubunye_input_" + _name)(lambda _n=_name: _read(_n))

for _name in _OUTPUTS:
    dp.materialized_view(name=_name, comment="Ubunye output {task_path}:" + _name)(
        lambda _n=_name: _outputs()[_n]
    )
'''


def _input_spec(name: str, io: Mapping[str, Any]) -> Dict[str, Any]:
    fmt = io.get("format")
    if fmt in ("hive", "unity"):
        table = io.get("table") or ".".join(p for p in (io.get("db_name"), io.get("tbl_name")) if p)
        if not table:
            raise ConfigError(f"inputs.{name}: a {fmt} input needs table or db_name/tbl_name")
        return {"kind": "table", "table": table}
    options = {str(k): str(v) for k, v in (io.get("options") or {}).items()}
    if fmt == "binary":
        if io.get("path_glob_filter"):
            options["pathGlobFilter"] = str(io["path_glob_filter"])
        if io.get("recursive"):
            options["recursiveFileLookup"] = "true"
        return {"kind": "path", "format": "binaryFile", "path": io["path"], "options": options}
    if fmt in ("s3", "delta") and io.get("path"):
        file_format = "delta" if fmt == "delta" else str(io.get("file_format") or "parquet")
        return {"kind": "path", "format": file_format, "path": io["path"], "options": options}
    raise ConfigError(
        f"inputs.{name}: format '{fmt}' has no Spark Declarative Pipelines equivalent here",
        hint="Supported: s3 and delta paths, binary files, hive and unity tables.",
    )


class SparkPipelineExporter:
    """Writes ``spark-pipeline.yml``, ``transformations/`` and ``task/`` into a folder."""

    def export(
        self, config_path: Path, *, output_path: Path, options: Mapping[str, Any] | None = None
    ) -> Dict[str, Any]:
        opts = dict(options or {})
        cfg: Dict[str, Any] = opts["config"]  # the resolved config, as a dict
        task_dir = Path(config_path).resolve().parent
        task_path = "/".join(task_dir.parts[-3:])
        slug = "_".join(task_dir.parts[-3:])
        section = cfg.get("CONFIG", {})

        notes: List[str] = []
        inputs = {name: _input_spec(name, io) for name, io in section.get("inputs", {}).items()}
        outputs = sorted(section.get("outputs", {}))
        for name, io in section.get("outputs", {}).items():
            if io.get("mode") not in (None, "overwrite"):
                notes.append(
                    f"outputs.{name}: mode '{io['mode']}' becomes a full recompute "
                    "(a materialized view)"
                )
            if io.get("path"):
                notes.append(f"outputs.{name}: written to the pipeline catalog, not {io['path']}")
        if section.get("expectations"):
            notes.append("CONFIG.expectations are not carried over")
        if "secret://" in yaml.safe_dump(section):
            raise ConfigError(
                "This task uses secret:// references, which a generated pipeline would have to "
                "hold in plain text.",
                hint="Export a task without secrets, or read them in the task's transform().",
            )

        out = Path(output_path)
        (out / "transformations").mkdir(parents=True, exist_ok=True)
        if (out / "task").exists():
            shutil.rmtree(out / "task")
        shutil.copytree(
            task_dir, out / "task", ignore=shutil.ignore_patterns("__pycache__", "*.pyc", ".*")
        )
        storage = opts.get("storage") or (out / "pipeline-storage").resolve().as_uri()
        spec = {
            "name": opts.get("name") or f"ubunye_{slug}",
            "storage": storage,
            "libraries": [{"glob": {"include": "transformations/**"}}],
        }
        if opts.get("catalog"):
            spec["catalog"] = opts["catalog"]
        if opts.get("database"):
            spec["database"] = opts["database"]
        conf = (cfg.get("ENGINE") or {}).get("spark_conf") or {}
        if conf:
            spec["configuration"] = {str(k): str(v) for k, v in conf.items()}
        (out / "spark-pipeline.yml").write_text(
            yaml.safe_dump(spec, sort_keys=False), encoding="utf-8"
        )
        module = _MODULE.format(
            task_path=task_path, slug=slug, inputs=inputs, outputs=outputs, config=cfg
        )
        compile(module, "ubunye_pipeline.py", "exec")
        (out / "transformations" / f"ubunye_{slug}.py").write_text(module, encoding="utf-8")
        return {"path": str(out), "datasets": outputs, "notes": notes}
