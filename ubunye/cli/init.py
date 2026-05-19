"""``ubunye init`` — scaffold pipelines and CI workflows.

Subcommands
-----------
    ubunye init pipeline        Scaffold task folders with config.yaml, transformations.py, and a dev notebook.
    ubunye init github-actions  Generate a GitHub Actions workflow for CI/CD.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import List, Optional

import typer

init_app = typer.Typer(
    name="init",
    help="Scaffold pipelines and CI workflows.",
    add_completion=False,
)


# ── helpers (notebook builder) ───────────────────────────────────────


def _md_cell(source: str) -> dict:
    return {"cell_type": "markdown", "metadata": {}, "source": [source]}


def _code_cell(source: str) -> dict:
    return {
        "cell_type": "code",
        "metadata": {},
        "source": [source],
        "execution_count": None,
        "outputs": [],
    }


def _build_dev_notebook(task: str, usecase: str, package: str) -> dict:
    """Build a Jupyter notebook dict for interactive Databricks development."""
    class_name = "".join(s.capitalize() for s in task.replace("-", "_").split("_"))
    cells = [
        _md_cell("## Parameters"),
        _code_cell(
            'dbutils.widgets.text("effective_year_month", "202501")\n'
            'dbutils.widgets.dropdown("mode", "nonprod", ["nonprod", "prod"])\n'
            "\n"
            'dt = dbutils.widgets.get("effective_year_month")\n'
            'mode = dbutils.widgets.get("mode")'
        ),
        _md_cell("## Setup"),
        _code_cell("%pip install ubunye-engine -q"),
        _code_cell(
            "from ubunye.config import load_config\n"
            "from ubunye.core.runtime import Registry\n"
            "from ubunye.backends.databricks_backend import DatabricksBackend\n"
            "\n"
            f'task_dir = "{usecase}/{package}/{task}"\n'
            'cfg = load_config(task_dir, variables={"dt": dt, "mode": mode})\n'
            "\n"
            "backend = DatabricksBackend()\n"
            "backend.start()\n"
            "reg = Registry.from_entrypoints()\n"
            "\n"
            'print(f"Config loaded: {len(cfg.CONFIG.inputs)} inputs, {len(cfg.CONFIG.outputs)} outputs")'
        ),
        _md_cell("## Extract\nRead all inputs defined in config.yaml"),
        _code_cell(
            "sources = {}\n"
            "for name, icfg in cfg.CONFIG.inputs.items():\n"
            "    reader_cls = reg.readers[icfg.format]\n"
            '    sources[name] = reader_cls().read(icfg.model_dump(mode="json"), backend)\n'
            '    print(f"{name}: {sources[name].count()} rows")'
        ),
        _md_cell("## Inspect Sources"),
        _code_cell(
            "for name, df in sources.items():\n"
            '    print(f"--- {name} ---")\n'
            "    display(df.limit(20))"
        ),
        _md_cell("## Transform"),
        _code_cell(
            "import sys, importlib.util\n"
            "\n"
            f'spec = importlib.util.spec_from_file_location("transformations", f"{{task_dir}}/transformations.py")\n'
            "mod = importlib.util.module_from_spec(spec)\n"
            "spec.loader.exec_module(mod)\n"
            "\n"
            f'task_obj = mod.{class_name}(config=cfg.model_dump(mode="json"))\n'
            "task_obj.setup()\n"
            "outputs = task_obj.transform(sources)\n"
            "\n"
            'print(f"Transform returned: {{list(outputs.keys())}}")'
        ),
        _md_cell("## Inspect Outputs"),
        _code_cell(
            "for name, df in outputs.items():\n"
            '    print(f"--- {name}: {df.count()} rows ---")\n'
            "    display(df.limit(20))"
        ),
        _md_cell(
            "## Load (disabled by default)\nUncomment to write outputs. **Review carefully before running in prod.**"
        ),
        _code_cell(
            "# WARNING: Uncomment to write outputs to the configured destinations.\n"
            "# for name, ocfg in cfg.CONFIG.outputs.items():\n"
            "#     writer_cls = reg.writers[ocfg.format]\n"
            '#     writer_cls().write(outputs[name], ocfg.model_dump(mode="json"), backend)\n'
            '#     print(f"Written: {name}")'
        ),
        _md_cell("## Sandbox\nSpark session is available for free exploration."),
        _code_cell(
            "spark = backend.spark\n"
            "\n"
            "# Example:\n"
            "# spark.sql('SELECT 1').show()\n"
            "# display(spark.catalog.listTables())"
        ),
    ]
    return {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python", "version": "3.9.0"},
        },
        "cells": cells,
    }


# ── init pipeline ────────────────────────────────────────────────────


@init_app.command("pipeline")
def init_pipeline(
    usecase_dir: Path = typer.Option(
        ..., "-d", "--usecase-dir", help="Specifies the directory path for the use case."
    ),
    usecase: str = typer.Option(..., "-u", "--usecase", help="Selects the desired use case."),
    package: str = typer.Option(
        ..., "-p", "--package", help="Selects a package from the specified use case."
    ),
    task_list: List[str] = typer.Option(
        ..., "-t", "--task-list", help="Specifies the task(s) to execute from the chosen package."
    ),
    overwrite: bool = typer.Option(False, help="Overwrite existing files"),
):
    """Scaffold task folders with config.yaml and transformations.py."""
    for task in task_list:
        target = usecase_dir / usecase / package / task
        cfg_file = target / "config.yaml"
        feat_file = target / "transformations.py"
        target.mkdir(parents=True, exist_ok=True)

        if cfg_file.exists() and not overwrite:
            typer.echo(f"exists: {cfg_file}")
        else:
            cfg = f"""MODEL: "etl"
VERSION: "0.1.0"
ENGINE:
  spark_conf:
    spark.sql.shuffle.partitions: "50"

CONFIG:
  inputs:
    tx_data:
      format: unity
      db_name: raw_db
      tbl_name: {task}_input
  transform: {{}}
  outputs:
    output_features:
      format: s3
      path: "s3a://your-bucket/{usecase}/{package}/{task}/{{{{ dt | default('1970-01-01') }}}}"
      mode: overwrite
"""
            cfg_file.write_text(cfg, encoding="utf-8")
            typer.echo(f"created: {cfg_file}")

        if feat_file.exists() and not overwrite:
            typer.echo(f"exists: {feat_file}")
        else:
            class_name = "".join(s.capitalize() for s in task.replace("-", "_").split("_"))
            feat = f"""from typing import Dict, Any
from ubunye.core.interfaces import Task

class {class_name}(Task):
    \"\"\"User-defined Spark transformation task.\"\"\"
    def setup(self) -> None:
        pass

    def transform(self, sources: Dict[str, Any]) -> Dict[str, Any]:
        # Replace with your pure DataFrame transformations.
        df = sources.get("tx_data")
        return {{"output_features": df}}
"""
            feat_file.write_text(feat, encoding="utf-8")
            typer.echo(f"created: {feat_file}")

        # --- Dev notebook ---
        nb_dir = target / "notebooks"
        nb_dir.mkdir(parents=True, exist_ok=True)
        nb_file = nb_dir / f"{task}_dev.ipynb"
        if nb_file.exists() and not overwrite:
            typer.echo(f"exists: {nb_file}")
        else:
            nb_file.write_text(
                json.dumps(_build_dev_notebook(task, usecase, package), indent=1),
                encoding="utf-8",
            )
            typer.echo(f"created: {nb_file}")

    typer.secho("[OK] Scaffold complete", fg=typer.colors.GREEN)


# ── init github-actions ──────────────────────────────────────────────


def _build_workflow(
    *,
    usecase_dir: str,
    usecase: str,
    package: str,
    task_list: list[str],
    extras: str,
    target: str,
    include_deploy: bool,
    workflow_filename: str,
) -> str:
    """Return the full GitHub Actions workflow YAML as a string."""
    needs_java = "spark" in extras
    path_prefix = f"{usecase_dir}/{usecase}/{package}"
    concurrency_group = f"{usecase}-{package}"

    lines: list[str] = []

    # ── header ──
    lines.append(f"name: {usecase}/{package}")
    lines.append("")
    lines.append("# Generated by: ubunye init github-actions")
    lines.append(f"# Pipeline: {path_prefix}")
    if include_deploy:
        lines.append("#")
        lines.append("# Required secrets (add in repo Settings > Secrets and variables > Actions):")
        lines.append(
            "#   DATABRICKS_HOST   - workspace URL (e.g. https://adb-1234.azuredatabricks.net)"
        )
        lines.append("#   DATABRICKS_TOKEN  - personal access token")
    lines.append("")

    # ── triggers ──
    lines.append("on:")
    lines.append("  push:")
    lines.append("    branches: [main]")
    lines.append("    paths:")
    lines.append(f'      - "{path_prefix}/**"')
    lines.append(f'      - ".github/workflows/{workflow_filename}"')
    lines.append("  pull_request:")
    lines.append("    branches: [main]")
    lines.append("    paths:")
    lines.append(f'      - "{path_prefix}/**"')
    lines.append(f'      - ".github/workflows/{workflow_filename}"')
    if include_deploy:
        lines.append("  workflow_dispatch:")
        lines.append("    inputs:")
        lines.append("      run_after_deploy:")
        lines.append('        description: "Trigger the job after deploying"')
        lines.append("        type: boolean")
        lines.append("        default: false")
    lines.append("")

    # ── concurrency ──
    lines.append("concurrency:")
    lines.append(f"  group: {concurrency_group}-${{{{ github.ref }}}}")
    lines.append("  cancel-in-progress: true")
    lines.append("")

    # ── jobs ──
    lines.append("jobs:")
    lines.append("  test-and-deploy:")
    lines.append("    runs-on: ubuntu-latest")
    lines.append("    timeout-minutes: 20")
    lines.append("")
    lines.append("    env:")
    lines.append(f"      PIPELINE_DIR: {path_prefix}")
    if include_deploy:
        lines.append("      DATABRICKS_HOST:  ${{ secrets.DATABRICKS_HOST }}")
        lines.append("      DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}")
    lines.append("")
    lines.append("    steps:")

    # ── checkout ──
    lines.append("      - name: Checkout")
    lines.append("        uses: actions/checkout@v6")
    lines.append("")

    # ── python ──
    lines.append("      - name: Set up Python 3.11")
    lines.append("        uses: actions/setup-python@v6")
    lines.append("        with:")
    lines.append('          python-version: "3.11"')
    lines.append("          cache: pip")
    lines.append("          cache-dependency-path: pyproject.toml")
    lines.append("")

    # ── java (only if spark extras) ──
    if needs_java:
        lines.append("      - name: Set up Java 17 (required by PySpark)")
        lines.append("        uses: actions/setup-java@v4")
        lines.append("        with:")
        lines.append("          distribution: temurin")
        lines.append('          java-version: "17"')
        lines.append("")

    # ── install ──
    lines.append("      - name: Install dependencies")
    lines.append("        run: |")
    lines.append("          python -m pip install --upgrade pip")
    lines.append(f'          pip install -e ".[{extras}]"')
    lines.append("")

    # ── validate ──
    lines.append("      - name: Validate config")
    lines.append(f"        run: ubunye validate -d {usecase_dir} -u {usecase} -p {package} --all")
    lines.append("")

    # ── tests ──
    lines.append("      - name: Unit tests")
    lines.append("        run: |")
    lines.append('          if [ -d "${PIPELINE_DIR}/tests" ]; then')
    lines.append('            pytest "${PIPELINE_DIR}/tests" -v')
    lines.append("          else")
    lines.append('            echo "No tests/ directory found - skipping."')
    lines.append("          fi")

    # ── deploy steps ──
    if include_deploy:
        lines.append("")
        lines.append("      - name: Check for Databricks secrets")
        lines.append("        id: check_secrets")
        lines.append("        run: |")
        lines.append('          if [[ -z "${DATABRICKS_HOST}" || -z "${DATABRICKS_TOKEN}" ]]; then')
        lines.append('            echo "has_secrets=false" >> "$GITHUB_OUTPUT"')
        lines.append(
            '            echo "::warning::DATABRICKS_HOST/DATABRICKS_TOKEN not set - skipping deploy."'
        )
        lines.append("          else")
        lines.append('            echo "has_secrets=true" >> "$GITHUB_OUTPUT"')
        lines.append("          fi")

        for task in task_list:
            lines.append("")
            deploy_label = f"Deploy {task}" if len(task_list) > 1 else "Deploy to Databricks"
            lines.append(f"      - name: {deploy_label}")
            lines.append(
                "        if: "
                "${{ steps.check_secrets.outputs.has_secrets == 'true' "
                "&& github.event_name == 'push' "
                "&& github.ref == 'refs/heads/main' }}"
            )
            lines.append("        run: |")
            lines.append(
                f"          ubunye deploy databricks \\\n"
                f"            -d {usecase_dir} -u {usecase} -p {package} -t {task} \\\n"
                f"            --target {target}"
            )

        for task in task_list:
            lines.append("")
            run_label = (
                f"Run {task} (manual trigger)" if len(task_list) > 1 else "Run job (manual trigger)"
            )
            lines.append(f"      - name: {run_label}")
            lines.append(
                "        if: "
                "${{ steps.check_secrets.outputs.has_secrets == 'true' "
                "&& github.event_name == 'workflow_dispatch' "
                "&& inputs.run_after_deploy }}"
            )
            lines.append("        run: |")
            lines.append(
                f"          ubunye deploy databricks \\\n"
                f"            -d {usecase_dir} -u {usecase} -p {package} -t {task} \\\n"
                f"            --target {target}"
            )

        lines.append("")
        lines.append("      - name: Notebook output link")
        lines.append(
            "        if: "
            "${{ steps.check_secrets.outputs.has_secrets == 'true' "
            "&& github.event_name == 'workflow_dispatch' "
            "&& inputs.run_after_deploy }}"
        )
        lines.append(
            '        run: echo "::notice::Notebook cell output is only visible in the '
            'Databricks job UI. Visit ${DATABRICKS_HOST}/#job/list to find the run."'
        )

    lines.append("")
    return "\n".join(lines)


@init_app.command("github-actions")
def init_github_actions(
    usecase_dir: Path = typer.Option(
        ..., "-d", "--usecase-dir", help="Root directory of pipelines."
    ),
    usecase: str = typer.Option(..., "-u", "--usecase", help="Usecase name."),
    package: str = typer.Option(..., "-p", "--package", help="Pipeline/package name."),
    task_list: List[str] = typer.Option(
        ..., "-t", "--task-list", help="Task(s) to include in the deploy steps."
    ),
    extras: str = typer.Option(
        "spark,dev",
        "--extras",
        help="pip install extras, comma-separated (e.g. spark,dev or ml,dev).",
    ),
    target: str = typer.Option("nonprod", "--target", help="Databricks deploy target."),
    no_deploy: bool = typer.Option(
        False, "--no-deploy", help="Omit Databricks deploy steps (CI-only workflow)."
    ),
    output: Optional[Path] = typer.Option(
        None,
        "-o",
        "--output",
        help="Output file path (default: .github/workflows/<usecase>_<package>.yml).",
    ),
    overwrite: bool = typer.Option(False, "--overwrite", help="Overwrite existing workflow file."),
):
    """Generate a GitHub Actions workflow for CI/CD.

    Creates a workflow file that validates config and runs tests on PRs,
    then deploys to Databricks on merge to main.

    Examples
    --------
    ubunye init github-actions -d pipelines -u fraud -p ingestion -t claim_etl

    ubunye init github-actions -d pipelines -u fraud -p ingestion -t claim_etl --no-deploy

    ubunye init github-actions -d pipelines -u fraud -p ingestion -t task_a -t task_b --extras ml,dev
    """
    workflow_filename = f"{usecase}_{package}.yml"
    if output is None:
        output = Path(".github") / "workflows" / workflow_filename
    else:
        workflow_filename = output.name

    if output.exists() and not overwrite:
        typer.secho(
            f"[SKIP] {output} already exists (use --overwrite to replace)", fg=typer.colors.YELLOW
        )
        raise typer.Exit(code=1)

    content = _build_workflow(
        usecase_dir=str(usecase_dir).replace("\\", "/"),
        usecase=usecase,
        package=package,
        task_list=list(task_list),
        extras=extras,
        target=target,
        include_deploy=not no_deploy,
        workflow_filename=workflow_filename,
    )

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(content, encoding="utf-8")
    typer.secho(f"[OK] Workflow written to {output}", fg=typer.colors.GREEN)

    if not no_deploy:
        typer.echo("")
        typer.echo("Next steps:")
        typer.echo("  1. Add DATABRICKS_HOST and DATABRICKS_TOKEN as repository secrets")
        typer.echo("     (Settings > Secrets and variables > Actions)")
        typer.echo("  2. Commit and push the workflow file")
        typer.echo(f"  3. The workflow triggers on changes to {usecase_dir}/{usecase}/{package}/**")
