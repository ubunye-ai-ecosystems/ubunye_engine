"""``ubunye doctor``: what is installed, what will fail, and why, before a run.

It answers the first bad hour's questions in one command: which backends can run
here and what each is missing, whether Java suits the installed Spark, whether
Delta pairs with that Spark, which plugins fail to load, and, for a task you
name, which environment variables it needs that are not set and whether its
config is valid.

Two levels, on purpose:

- **warn**: a problem with the environment that matters only for the backend or
  plugin you use (Spark without Java is fine if you run on pandas).
- **fail**: a problem that makes a run fail: no usable backend at all, or a task
  you named whose config cannot load. Any failure makes the exit code 1.
"""

from __future__ import annotations

import importlib.metadata as md
import os
import re
import shutil
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import List, Optional

import typer

from ubunye.cli import backend_choice as backends_view
from ubunye.cli.output import emit, json_option
from ubunye.cli.variables import cli_variables, var_option

OK, WARN, FAIL = "ok", "warn", "fail"

# Tested Pythons (CI runs 3.10 to 3.13).
SUPPORTED_PYTHON = ((3, 10), (3, 13))

# The plugin groups a task can pull code from. Backends have their own check.
PLUGIN_GROUPS = ("ubunye.readers", "ubunye.writers", "ubunye.transforms", "ubunye.hooks")


@dataclass
class Check:
    """One line of the report."""

    name: str
    status: str
    detail: str
    fix: str = ""


# --- the pieces, each testable on its own ------------------------------------


def java_major(banner: str) -> Optional[int]:
    """The Java major version from ``java -version`` output (``1.8`` means 8)."""
    found = re.search(r'version "(\d+)(?:\.(\d+))?', banner)
    if not found:
        return None
    first = int(found.group(1))
    if first == 1 and found.group(2):
        return int(found.group(2))
    return first


def java_suits_spark(spark_version: str, java: int) -> bool:
    """Spark 4 runs on Java 17 and 21; Spark 3.5 on Java 8, 11 and 17."""
    major = int(spark_version.split(".")[0])
    if major >= 4:
        return java in (17, 21)
    return java in (8, 11, 17)


def delta_pairs_with_spark(spark_version: str, delta_version: str) -> bool:
    """delta-spark 3.x is built for Spark 3, 4.x for Spark 4.

    A mismatch installs cleanly and then fails at run time with
    ``ClassNotFoundException: delta.DefaultSource``.
    """
    return int(spark_version.split(".")[0]) == int(delta_version.split(".")[0])


def _version(package: str) -> Optional[str]:
    try:
        return md.version(package)
    except md.PackageNotFoundError:
        return None


def _find_java() -> Optional[str]:
    home = os.environ.get("JAVA_HOME")
    if home:
        exe = Path(home) / "bin" / ("java.exe" if os.name == "nt" else "java")
        if exe.exists():
            return str(exe)
    return shutil.which("java")


def _java_banner(java: str) -> str:
    try:
        done = subprocess.run(
            [java, "-version"], capture_output=True, text=True, timeout=20, check=False
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return f"could not run: {exc}"
    return (done.stderr or done.stdout).strip()


# --- the checks ----------------------------------------------------------------


def environment_checks() -> List[Check]:
    """Python, the engine, and what Spark needs around it (Java, Hadoop, Delta)."""
    checks: List[Check] = []
    here = sys.version_info[:2]
    low, high = SUPPORTED_PYTHON
    python = f"Python {sys.version.split()[0]} on {sys.platform}"
    if low <= here <= high:
        checks.append(Check("python", OK, python))
    else:
        checks.append(
            Check(
                "python",
                WARN,
                f"{python}; tested on {low[0]}.{low[1]} to {high[0]}.{high[1]}",
                "Use a tested Python.",
            )
        )
    checks.append(Check("ubunye-engine", OK, _version("ubunye-engine") or "not installed"))

    spark = _version("pyspark")
    if not spark:
        return checks

    java = _find_java()
    if not java:
        checks.append(
            Check(
                "java",
                WARN,
                "pyspark is installed but no Java was found (JAVA_HOME or java on PATH)",
                "Install Java 17 (Spark 4) or 11 (Spark 3.5), or run with --backend pandas.",
            )
        )
    else:
        major = java_major(_java_banner(java))
        if major is None:
            checks.append(Check("java", WARN, f"{java}: could not read its version"))
        elif java_suits_spark(spark, major):
            checks.append(Check("java", OK, f"Java {major} for Spark {spark} ({java})"))
        else:
            checks.append(
                Check(
                    "java",
                    WARN,
                    f"Java {major} does not suit Spark {spark} ({java})",
                    "Spark 4 needs Java 17 or 21; Spark 3.5 needs 8, 11 or 17. Set JAVA_HOME.",
                )
            )

    if os.name == "nt":
        hadoop = os.environ.get("HADOOP_HOME")
        winutils = Path(hadoop) / "bin" / "winutils.exe" if hadoop else None
        if winutils and winutils.exists():
            checks.append(Check("hadoop (Windows)", OK, str(winutils)))
        else:
            checks.append(
                Check(
                    "hadoop (Windows)",
                    WARN,
                    "Spark on Windows writes files through winutils.exe, and HADOOP_HOME "
                    "does not point at one",
                    "Download winutils for your Hadoop version and set HADOOP_HOME.",
                )
            )

    delta = _version("delta-spark")
    if delta:
        if delta_pairs_with_spark(spark, delta):
            checks.append(Check("delta-spark", OK, f"delta-spark {delta} with Spark {spark}"))
        else:
            checks.append(
                Check(
                    "delta-spark",
                    WARN,
                    f"delta-spark {delta} is built for a different Spark than {spark}; "
                    "Delta reads and writes will fail at run time",
                    f"pip install 'delta-spark=={spark.split('.')[0]}.*'",
                )
            )
    return checks


def _backend_checks() -> List[Check]:
    checks: List[Check] = []
    rows = backends_view.describe_all()
    for row in rows:
        name = f"backend: {row['name']}" + (" (default)" if row.get("default") else "")
        if row.get("loaded"):
            caps = row.get("capabilities", {})
            jvm = " needs Java," if caps.get("needs_jvm") else ""
            dist = "distributed" if caps.get("distributed") else "single machine"
            checks.append(Check(name, OK, f"usable,{jvm} {dist}"))
        else:
            error = row.get("error", "not usable")
            fix = ""
            if "(pip" in error:  # "needs x (pip install ...)": say it once, as the fix
                error, fix = error[: error.find(" (pip")], error[error.find("(pip") + 1 : -1]
            checks.append(Check(name, WARN, error, fix))
    usable = [row["name"] for row in rows if row.get("loaded")]
    default = next((row for row in rows if row.get("default")), None)
    if usable and default is not None and not default.get("loaded"):
        checks.append(
            Check(
                "default backend",
                WARN,
                f"a run without --backend uses '{default['name']}', which cannot run here "
                "(on Databricks the platform's own backend is used instead)",
                f"Pass --backend {usable[0]}, or install what '{default['name']}' needs.",
            )
        )
    if not usable:
        checks.append(
            Check(
                "backends",
                FAIL,
                "no backend can run here, so every run will fail",
                "pip install 'ubunye-engine[pandas]' (no Java needed) or 'ubunye-engine[spark]'",
            )
        )
    return checks


def backend_checks() -> List[Check]:
    """One check per registered backend; failure only if none is usable."""
    return _backend_checks()


def plugin_checks() -> List[Check]:
    """Every reader, writer, transform and hook plugin must load."""
    checks: List[Check] = []
    loaded = 0
    for group in PLUGIN_GROUPS:
        for ep in md.entry_points(group=group):
            try:
                ep.load()
                loaded += 1
            except Exception as exc:  # a broken third party plugin, reported not raised
                checks.append(
                    Check(
                        f"plugin {group.split('.')[-1][:-1]} '{ep.name}'",
                        WARN,
                        f"{ep.value} does not load: {type(exc).__name__}: {exc}",
                        "Reinstall or remove the package that provides it.",
                    )
                )
    checks.insert(0, Check("plugins", OK, f"{loaded} load"))
    return checks


def task_checks(task_dir: Path, task: str, variables: dict) -> List[Check]:
    """The environment variables a task needs, and whether its config loads."""
    from ubunye.config import load_config
    from ubunye.config.resolver import required_env_references

    config_path = task_dir / "config.yaml"
    if not config_path.is_file():
        return [Check(f"task {task}", FAIL, f"no config.yaml at {config_path}")]

    checks: List[Check] = []
    raw = config_path.read_text(encoding="utf-8")
    missing = sorted(v for v in required_env_references(raw) if v not in os.environ)
    if missing:
        checks.append(
            Check(
                f"task {task}: environment",
                FAIL,
                "not set, and used without a default: " + ", ".join(missing),
                "Set them, or give each a default: {{ env.NAME | default('value') }}.",
            )
        )
    else:
        checks.append(Check(f"task {task}: environment", OK, "every variable it needs is set"))

    checks += _secret_checks(task, raw)

    try:
        load_config(str(config_path), variables)
    except Exception as exc:
        first = str(exc).strip().splitlines()[0] if str(exc).strip() else type(exc).__name__
        checks.append(Check(f"task {task}: config", FAIL, first))
    else:
        checks.append(Check(f"task {task}: config", OK, "loads and validates"))
    return checks


def _secret_checks(task: str, raw: str) -> List[Check]:
    """Each ``secret://`` provider a task uses: installed, with what it needs.

    Nothing is fetched: doctor must not read secrets, and a login may only exist
    where the task really runs.
    """
    from ubunye.core import secrets

    names = sorted(set(re.findall(r"secret://([A-Za-z0-9_.-]+)/", raw)))
    checks: List[Check] = []
    for name in names:
        label = f"task {task}: secret provider {name}"
        if name not in secrets.providers():
            checks.append(Check(label, FAIL, "no such provider is installed"))
            continue
        missing = secrets.missing_packages(name)
        if missing:
            checks.append(
                Check(
                    label, FAIL, "needs " + ", ".join(missing), "pip install " + " ".join(missing)
                )
            )
        else:
            checks.append(Check(label, OK, "installed (the secret itself is read at run time)"))
    return checks


# --- the command ---------------------------------------------------------------

_MARK = {
    OK: ("[OK]", typer.colors.GREEN),
    WARN: ("[WARN]", typer.colors.YELLOW),
    FAIL: ("[FAIL]", typer.colors.RED),
}


def doctor_command(
    usecase_dir: Optional[Path] = typer.Option(
        None, "-d", "--usecase-dir", help="Also check tasks under this pipelines folder."
    ),
    usecase: Optional[str] = typer.Option(None, "-u", "--usecase"),
    package: Optional[str] = typer.Option(None, "-p", "--package"),
    task_list: Optional[List[str]] = typer.Option(None, "-t", "--task-list"),
    data_timestamp: Optional[str] = typer.Option(None, "-dt", "--data-timestamp"),
    mode: str = typer.Option("DEV", "-m", "--mode"),
    var: Optional[List[str]] = var_option(),
    as_json: bool = json_option(),
) -> None:
    """Check this machine (and optionally tasks) before a run: what will fail, and why."""
    checks = environment_checks() + backend_checks() + plugin_checks()

    if task_list:
        if not (usecase_dir and usecase and package):
            checks.append(Check("tasks", FAIL, "-t needs -d, -u and -p too"))
        else:
            variables = cli_variables(dt=data_timestamp, dtf=None, mode=mode, var=var)
            for task in task_list:
                checks += task_checks(usecase_dir / usecase / package / task, task, variables)

    ok = not any(c.status == FAIL for c in checks)
    if as_json:
        emit({"ok": ok, "checks": [asdict(c) for c in checks]})
    else:
        for c in checks:
            mark, colour = _MARK[c.status]
            typer.secho(f"{mark} {c.name}", fg=colour, nl=False)
            typer.echo(f": {c.detail}")
            if c.fix and c.status != OK:
                typer.echo(f"       fix: {c.fix}")
        problems = sum(c.status != OK for c in checks)
        summary = "nothing to fix" if not problems else f"{problems} to look at"
        typer.echo(f"\n{summary}; " + ("a run can start." if ok else "a run would fail."))
    if not ok:
        raise typer.Exit(code=1)
