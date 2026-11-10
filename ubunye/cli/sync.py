"""``ubunye sync`` — replay fallback manifests against configured backends.

When metadata writes fail during pipeline execution (Decision 2), records
are appended to local JSONL manifests under ``~/.ubunye/fallback/``.
This command replays those manifests with idempotent deduplication.

Usage::

    ubunye sync lineage                    # replay all lineage manifests
    ubunye sync lineage --run-id abc123    # replay a specific run
    ubunye sync registry                   # replay all registry manifests
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Optional

import typer

sync_app = typer.Typer(
    name="sync",
    help="Replay fallback manifests against configured backends.",
    add_completion=False,
)

_FALLBACK_ROOT = Path.home() / ".ubunye" / "fallback"
_SYNCED_ROOT = _FALLBACK_ROOT / "synced"


def _find_manifests(kind: str, run_id: Optional[str] = None) -> list[Path]:
    """Find JSONL manifests under the fallback directory."""
    if not _FALLBACK_ROOT.exists():
        return []
    if run_id:
        manifest = _FALLBACK_ROOT / run_id / f"{kind}.jsonl"
        return [manifest] if manifest.exists() else []
    return sorted(_FALLBACK_ROOT.glob(f"*/{kind}.jsonl"))


def _load_manifest(path: Path) -> list[dict]:
    """Load all records from a JSONL manifest."""
    records = []
    for line in path.read_text(encoding="utf-8").strip().splitlines():
        if line.strip():
            records.append(json.loads(line))
    return records


def _dedup_key(record: dict) -> str:
    """Build a deduplication key: run_id + task + recorded_at."""
    run_id = record.get("run_id", "")
    task = record.get("task", record.get("task_name", ""))
    recorded = record.get("recorded_at", record.get("started_at", ""))
    return f"{run_id}:{task}:{recorded}"


def _archive_manifest(manifest_path: Path) -> None:
    """Move a processed manifest to the synced archive."""
    run_dir = manifest_path.parent
    run_id = run_dir.name
    dest_dir = _SYNCED_ROOT / run_id
    dest_dir.mkdir(parents=True, exist_ok=True)
    shutil.move(str(manifest_path), str(dest_dir / manifest_path.name))
    if not any(run_dir.iterdir()):
        run_dir.rmdir()


@sync_app.command("lineage")
def sync_lineage(
    run_id: Optional[str] = typer.Option(None, "--run-id", help="Replay only this run's manifest."),
    lineage_dir: str = typer.Option(
        ".ubunye/lineage", "--lineage-dir", help="The store's location, as given."
    ),
    lineage_backend: Optional[str] = typer.Option(
        None,
        "--lineage-backend",
        help="The store to replay into. Default: $UBUNYE_LINEAGE_BACKEND, else filesystem.",
    ),
) -> None:
    """Replay lineage fallback manifests."""
    manifests = _find_manifests("lineage", run_id)
    if not manifests:
        typer.secho("No lineage fallback manifests found.", fg=typer.colors.YELLOW)
        raise typer.Exit()

    from ubunye.core.errors import PluginNotFoundError
    from ubunye.interfaces.lineage import LineageRecord
    from ubunye.lineage import stores
    from ubunye.lineage.context import RunContext
    from ubunye.lineage.storage import FileSystemLineageStore, _record_to_ctx

    try:
        store = stores.open_store(lineage_dir, lineage_backend)
    except PluginNotFoundError as exc:
        typer.secho(f"[ERROR] {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

    total = 0
    replayed = 0
    deduped = 0
    seen_keys: set[str] = set()

    for manifest in manifests:
        records = _load_manifest(manifest)
        total += len(records)
        # Whole run records (ADR 011): a run is saved twice (running, finished),
        # so the last one kept for each run is the one replayed.
        whole: dict = {}
        for rec in records:
            if "task_path" in rec:
                key = f"{rec['task_path']}:{rec.get('run_id', '')}"
                if key in whole:
                    deduped += 1
                whole[key] = {k: v for k, v in rec.items() if not k.startswith("_")}
        for doc in whole.values():
            try:
                store.save(RunContext.from_dict(doc))
                replayed += 1
            except Exception as exc:
                typer.secho(f"  Failed to replay record: {exc}", fg=typer.colors.RED)
        for rec in records:
            if "task_path" in rec:
                continue
            key = _dedup_key(rec)
            if key in seen_keys:
                deduped += 1
                continue
            seen_keys.add(key)

            rec.pop("_fallback_at", None)
            try:
                lr = LineageRecord(
                    run_id=rec.get("run_id", "unknown"),
                    recorded_at=rec.get("recorded_at", rec.get("started_at", "")),
                    engine_version=rec.get("engine_version", "unknown"),
                    task=rec.get("task", rec.get("task_name", "")),
                    usecase=rec.get("usecase", ""),
                    pipeline=rec.get("pipeline", rec.get("package", "")),
                    target=rec.get("target", rec.get("profile", "")),
                    status=rec.get("status", "unknown"),
                    duration_sec=rec.get("duration_sec"),
                    error=rec.get("error"),
                    metadata={k: str(v) for k, v in rec.get("metadata", {}).items()},
                )
                if isinstance(store, FileSystemLineageStore):  # keeps extra metadata
                    store._record_sync(lr)
                else:
                    store.save(_record_to_ctx(lr))
                replayed += 1
            except Exception as exc:
                typer.secho(f"  Failed to replay record: {exc}", fg=typer.colors.RED)

        _archive_manifest(manifest)

    typer.secho(
        f"Sync complete: {total} total, {replayed} replayed, {deduped} deduplicated.",
        fg=typer.colors.GREEN,
    )


@sync_app.command("registry")
def sync_registry(
    run_id: Optional[str] = typer.Option(None, "--run-id", help="Replay only this run's manifest."),
) -> None:
    """Replay registry fallback manifests."""
    manifests = _find_manifests("registry", run_id)
    if not manifests:
        typer.secho("No registry fallback manifests found.", fg=typer.colors.YELLOW)
        raise typer.Exit()

    total = 0
    replayed = 0
    deduped = 0
    seen_keys: set[str] = set()

    for manifest in manifests:
        records = _load_manifest(manifest)
        total += len(records)
        for rec in records:
            key = _dedup_key(rec)
            if key in seen_keys:
                deduped += 1
                continue
            seen_keys.add(key)
            replayed += 1

        _archive_manifest(manifest)

    typer.secho(
        f"Sync complete: {total} total, {replayed} replayed, {deduped} deduplicated.",
        fg=typer.colors.GREEN,
    )
