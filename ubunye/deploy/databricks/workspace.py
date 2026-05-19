"""Upload task files to the Databricks workspace."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Optional

from ubunye.core.errors import WorkspaceUploadError

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

SKIP_PATTERNS = {"__pycache__", ".pyc", ".ipynb_checkpoints"}


def upload_task_files(
    client: "WorkspaceClient",
    task_dir: Path,
    workspace_root: str,
    usecase: str,
    pipeline: str,
    task: str,
    notebook_source: Optional[str] = None,
    notebook_name: Optional[str] = None,
) -> str:
    """Upload ``config.yaml``, ``transformations.py``, and siblings to the workspace.

    Returns the workspace directory path where files were uploaded.
    Raises :class:`WorkspaceUploadError` if an upload fails.
    """
    workspace_dir = f"{workspace_root}/{usecase}/{pipeline}/{task}"

    files_to_upload = _collect_files(task_dir)

    for local_path, relative_name in files_to_upload:
        remote_path = f"{workspace_dir}/{relative_name}"
        _upload_one(client, local_path, remote_path)

    if notebook_source and notebook_name:
        nb_path = f"{workspace_dir}/{notebook_name}"
        _upload_notebook(client, notebook_source, nb_path)

    return workspace_dir


def _collect_files(task_dir: Path) -> list[tuple[Path, str]]:
    """Collect uploadable files from the task directory."""
    result = []
    for path in sorted(task_dir.iterdir()):
        if path.is_dir():
            if path.name in SKIP_PATTERNS:
                continue
            if path.name == "notebooks":
                continue
            for child in sorted(path.rglob("*")):
                if child.is_file() and not _should_skip(child):
                    result.append((child, str(child.relative_to(task_dir))))
        elif path.is_file() and not _should_skip(path):
            result.append((path, path.name))
    return result


def _should_skip(path: Path) -> bool:
    return (
        path.suffix == ".pyc"
        or "__pycache__" in path.parts
        or ".ipynb_checkpoints" in path.parts
    )


def _upload_one(client: "WorkspaceClient", local_path: Path, remote_path: str) -> None:
    """Upload a single file to the workspace."""
    try:
        content = local_path.read_bytes()
        client.workspace.upload(
            remote_path,
            content,
            overwrite=True,
        )
    except Exception as exc:
        raise WorkspaceUploadError(
            f"Failed to upload {local_path.name} to {remote_path}.",
            context={
                "Local file": str(local_path),
                "Remote path": remote_path,
            },
            hint="Check workspace permissions and that the path is valid.",
        ) from exc


def _upload_notebook(
    client: "WorkspaceClient", source: str, remote_path: str
) -> None:
    """Upload notebook source as a Python notebook."""
    try:
        import base64

        from databricks.sdk.service.workspace import ImportFormat, Language

        client.workspace.import_(
            remote_path,
            content=base64.b64encode(source.encode("utf-8")).decode("ascii"),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
        )
    except ImportError:
        client.workspace.upload(
            remote_path + ".py",
            source.encode("utf-8"),
            overwrite=True,
        )
    except Exception as exc:
        raise WorkspaceUploadError(
            f"Failed to upload notebook to {remote_path}.",
            context={"Remote path": remote_path},
            hint="Check workspace permissions and that the path is valid.",
        ) from exc
