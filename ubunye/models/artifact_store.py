"""Where model artifacts live. The registry asks; it never assumes a disk.

The model registry used to be built directly on ``pathlib.Path``. That worked on a
laptop and on Databricks (whose volumes are mounted like folders), and worked nowhere
else. It could not write to ``s3://`` or ``gs://``, which is the one thing AWS and GCP
serverless offer. The registry knew too much about one kind of storage, which is the
same mistake the connectors had before 0.5.0.

Now storage is a port, like everything else in this engine:

* the registry talks to a small :class:`ArtifactStore` contract,
* the SCHEME of the store path picks the implementation (``/models`` or ``file://`` is
  local, ``s3://`` and friends go to fsspec, ``mything://`` goes to whatever someone
  registered),
* and anyone can add a store the same way they add a connector: write a class,
  register it under the ``ubunye.artifact_stores`` entry-point group, done. Nothing in
  the engine needs an edit, and the class does not need to inherit anything. The
  contract is structural, matching the rest of the framework.

Two methods deserve a word, because they exist for a physical reason. Models write
real files: ``model.save(dir)`` is joblib, or a Keras zip, and those need an actual
local directory (we measured Keras failing outright on a network mount, because
writing a zip needs seeks). So:

* :meth:`ArtifactStore.staging_dir` hands the caller a REAL local directory to write
  into, and publishes it to the target when the block ends. The local store simply
  yields the target itself; a remote store yields a temp dir and uploads.
* :meth:`ArtifactStore.materialize` does the reverse for loading: it returns a local
  directory containing the artifact, downloading first if the store is remote.

On concurrency, plainly: ``registry.json`` is read-modify-write. Two jobs promoting
at the same instant can lose one update, on any store. The local store narrows the
window with an atomic rename; object stores are last-writer-wins. If you need
concurrent registration, use the MLflow registry backend, which has a server to
serialise writes.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterator, Protocol, runtime_checkable
from urllib.parse import urlparse


@runtime_checkable
class ArtifactStore(Protocol):
    """What the registry needs from storage. Nothing more.

    Structural: implement these methods and you are a store. No base class.
    """

    def join(self, *parts: str) -> str:
        """Combine path segments into one location string."""
        ...

    def exists(self, path: str) -> bool: ...

    def read_text(self, path: str) -> str: ...

    def write_text(self, path: str, text: str) -> None:
        """Write text, creating parent locations as needed."""
        ...

    def delete(self, path: str) -> None:
        """Remove a location and everything under it. Missing is not an error."""
        ...

    def staging_dir(self, target: str) -> "contextlib.AbstractContextManager[str]":
        """A real local directory to write model files into.

        When the block exits without error, the contents are published to
        ``target``. Models need this because their save formats need a real disk.
        """
        ...

    def materialize(self, path: str) -> str:
        """Return a local directory containing the artifact at ``path``.

        Local stores return the path itself. Remote stores download to a
        temporary directory and return that.
        """
        ...


class LocalArtifactStore:
    """Plain directories. The default, and exactly what the registry always did."""

    def __init__(self, root: str = "") -> None:
        self.root = root

    def join(self, *parts: str) -> str:
        return str(Path(*parts))

    def exists(self, path: str) -> bool:
        return Path(path).exists()

    def read_text(self, path: str) -> str:
        return Path(path).read_text(encoding="utf-8")

    def write_text(self, path: str, text: str) -> None:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        # Write-then-rename narrows the torn-write window: a reader never sees a
        # half-written registry.json, even if the process dies mid-write.
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_text(text, encoding="utf-8")
        os.replace(tmp, p)

    def delete(self, path: str) -> None:
        p = Path(path)
        if p.is_dir():
            shutil.rmtree(p, ignore_errors=True)
        elif p.exists():
            p.unlink()

    @contextlib.contextmanager
    def staging_dir(self, target: str) -> Iterator[str]:
        # Local disk IS the destination, so stage in place: no pointless copy.
        Path(target).mkdir(parents=True, exist_ok=True)
        yield target

    def materialize(self, path: str) -> str:
        return path


class FsspecArtifactStore:
    """Any storage fsspec can reach: s3://, gs://, abfss://, memory:// and more.

    fsspec is itself a plugin ecosystem. Installing ``s3fs`` teaches it S3,
    ``gcsfs`` teaches it GCS, and a third-party filesystem teaches it anything
    else. So this single adapter gives the registry every backend that ecosystem
    has, and ever grows.
    """

    def __init__(self, scheme: str) -> None:
        try:
            import fsspec
        except ImportError as exc:
            raise ImportError(
                f"A '{scheme}://' model store needs fsspec. "
                "Install it with: pip install 'ubunye-engine[objectstore]' "
                "(plus the filesystem for your cloud, e.g. s3fs or gcsfs)."
            ) from exc
        self._fs = fsspec.filesystem(scheme)
        self._scheme = scheme

    def join(self, *parts: str) -> str:
        head, tail = parts[0].rstrip("/"), [p.strip("/") for p in parts[1:]]
        return "/".join([head, *tail])

    def exists(self, path: str) -> bool:
        return bool(self._fs.exists(path))

    def read_text(self, path: str) -> str:
        with self._fs.open(path, "r") as f:
            return f.read()

    def write_text(self, path: str, text: str) -> None:
        with self._fs.open(path, "w") as f:
            f.write(text)

    def delete(self, path: str) -> None:
        if self._fs.exists(path):
            self._fs.rm(path, recursive=True)

    @contextlib.contextmanager
    def staging_dir(self, target: str) -> Iterator[str]:
        # Models write zips and binary files, which need a REAL disk (a Keras save
        # fails outright on anything that cannot seek). Stage locally, upload after.
        tmp = tempfile.mkdtemp(prefix="ubunye-artifact-")
        try:
            yield tmp
            self._fs.put(tmp + "/", target.rstrip("/") + "/", recursive=True)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def materialize(self, path: str) -> str:
        local = tempfile.mkdtemp(prefix="ubunye-artifact-")
        self._fs.get(path.rstrip("/") + "/", local + "/", recursive=True)
        return local


def _registered_stores() -> Dict[str, Any]:
    """Third-party stores, by scheme, from the ``ubunye.artifact_stores`` group."""
    import importlib.metadata as md

    found: Dict[str, Any] = {}
    try:
        eps: Any = md.entry_points()
        group = (
            eps.get("ubunye.artifact_stores", [])
            if hasattr(eps, "get")
            else eps.select(group="ubunye.artifact_stores")
        )
        for ep in group:
            try:
                found[ep.name] = ep.load()
            except Exception:  # noqa: BLE001 — one broken plugin must not break storage
                pass
    except Exception:  # noqa: BLE001
        pass
    return found


def resolve_store(store_path: str) -> ArtifactStore:
    """Pick the store from the path's scheme. Nothing is hardcoded.

    * no scheme, or ``file://`` — local directories, zero dependencies,
    * a scheme registered under ``ubunye.artifact_stores`` — that plugin,
    * any other scheme — fsspec, which resolves it through its own plugin world.

    Adding support for a new kind of storage therefore never touches this file:
    either register an entry point, or install an fsspec filesystem.
    """
    scheme = urlparse(store_path).scheme

    # Windows drive letters parse as a one-letter "scheme". C:\models is local.
    if not scheme or len(scheme) == 1 or scheme == "file":
        return LocalArtifactStore()

    plugin = _registered_stores().get(scheme)
    if plugin is not None:
        store: ArtifactStore = plugin(scheme)
        return store

    return FsspecArtifactStore(scheme)
