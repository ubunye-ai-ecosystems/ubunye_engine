"""Rerun safety: one live run per task and batch, and a clean rerun after a crash.

The hardening experiments showed a run can be killed at any moment (a power cut, a
killed job) and that the rerun then did harm: an append committed by the killed run
was appended again (E-01, 16 of 16 trials), two runs of the same date at once both
appended (E-02, 7 of 10), and the killed run's record said "running" for ever. The
AbsaOSS ingestion tool Pramen met the same problems in production and answered them
with a lease per table and date, and with repairs made from what was actually written.
This is the same idea, kept small, and it never deletes a file it cannot prove this
run wrote (ADR 008).

**The lease.** A run takes a lease on its *batch*: the task and its variables (``dt``,
``--var``), so runs of different dates never block each other. The lease is a file,
``<usecase_dir>/.ubunye/leases/<usecase>/<package>/<task>/<key>.json``, created
atomically; it names the run, its process, its host and a heartbeat. A second run of
the same batch while the first is alive is refused, naming the first.

**A dead run.** A lease whose process is gone (same host), or whose file has not been
touched for :data:`HEARTBEAT_TIMEOUT` by the shared disk's own clock (another host),
belongs to a run that died. The next run takes it over and marks that run's record
``interrupted``.

**Appends: only claimed files are ever removed.** A backend that can name the files it
appends (pandas: one part file with a fresh UUID in its name; Spark on a local or
shared disk: the files its job wrote into a staging folder named by this run) *claims*
each one in the lease before moving it into the output folder. A run that fails removes
its claimed files; a run that takes over a dead run's lease removes the dead run's. No
output folder is ever listed to decide what to delete, so another run's files are never
touched. Appends a backend cannot claim (JDBC, catalog and Delta tables, Spark paths on
object storage) are never deleted: the run says they may hold the batch, in its log and
in the dead run's record.

**A finished batch.** A run that appends a named batch (``dt`` or a ``--var``) and
succeeds leaves a note beside the lease: which run finished it, and the files it
claimed. A later run of the same batch is refused, since it would append the batch a
second time (F-031). ``--rerun`` (``rerun=True``) replaces it instead: once the new run
has succeeded, the files the finished run claimed are removed. Appends that were not
claimed (JDBC, catalog and Delta tables, Spark paths on object storage) cannot be
removed, so the run says it appends to them again.

**Limits, said plainly.** The lease protects runs that share the usecase folder (one
machine, or a shared disk). Two cloud jobs each with their own disk are not protected
by it. ``UBUNYE_RUN_LEASE=off`` turns it off.
"""

from __future__ import annotations

import contextvars
import copy
import hashlib
import json
import logging
import os
import shutil
import socket
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ubunye.core.errors import UbunyeError

logger = logging.getLogger(__name__)

#: A lease untouched for this long belongs to a run that died, when its process
#: cannot be checked (it ran on another host).
HEARTBEAT_TIMEOUT = 15 * 60
#: How often a live run refreshes its heartbeat.
HEARTBEAT_EVERY = 30
#: How long a takeover holds the lease before taking anything back, so a run judged
#: dead that is alive can show it. A save checks ownership, then replaces the lease,
#: retrying for at most :data:`BUSY_FOR`: a save under way lands within that window.
TAKEOVER_SETTLE = 3.0

_CURRENT: contextvars.ContextVar[Optional["RunLease"]] = contextvars.ContextVar(
    "ubunye_run_lease", default=None
)


class RunLeaseHeld(UbunyeError, RuntimeError):
    """Another live run holds the lease on this task and batch."""


class RunLeaseLost(UbunyeError, RuntimeError):
    """This run's lease was taken over; it must not write any more."""


class BatchFinished(RunLeaseHeld):
    """The batch was already appended by a run that finished; ``--rerun`` replaces it."""


def enabled() -> bool:
    return os.environ.get("UBUNYE_RUN_LEASE", "on").strip().lower() not in ("off", "0", "false")


def names_a_batch(variables: Optional[Dict[str, Any]]) -> bool:
    """Whether the variables say which data a run is for (``dt`` or a ``--var``).

    ``mode`` and ``dtf`` do not: a task run with neither may append a fresh snapshot
    each time, and every such run is the same "batch", so it is never refused."""
    return any(
        v not in (None, "") for k, v in (variables or {}).items() if k not in ("mode", "dtf")
    )


def append_outputs(outputs: Optional[Dict[str, Any]]) -> List[str]:
    """The outputs of a task config that append (a missing mode counts as append)."""
    return sorted(
        name
        for name, ocfg in (outputs or {}).items()
        if isinstance(ocfg, dict) and str(ocfg.get("mode") or "append").lower() == "append"
    )


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _process_start(pid: int) -> Optional[str]:
    """When a process started, as an opaque token, or None when it cannot be told.

    A pid is reused once its process ends; the start time tells the two apart.
    """
    if pid <= 0:
        return None
    if sys.platform == "win32":
        import ctypes

        kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
        handle = kernel32.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
        if not handle:
            return None
        try:
            times = [ctypes.c_ulonglong() for _ in range(4)]
            if not kernel32.GetProcessTimes(handle, *(ctypes.byref(t) for t in times)):
                return None
            return str(times[0].value)
        finally:
            kernel32.CloseHandle(handle)
    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8") as fh:
            return fh.read().rsplit(")", 1)[1].split()[19]  # field 22: starttime
    except (OSError, IndexError):
        return None


def _pid_alive(pid: int, started: Optional[str] = None) -> bool:
    """Whether a process is running on this host (the same one, when ``started`` is
    given). Never signals it."""
    if pid <= 0:
        return False
    if sys.platform == "win32":
        # os.kill(pid, 0) would *terminate* the process on Windows.
        import ctypes

        kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
        handle = kernel32.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
        if not handle:
            # 87 (invalid parameter): no such process. Anything else, access denied
            # included, may be a live process of another user: never call it dead.
            return kernel32.GetLastError() != 87
        try:
            code = ctypes.c_ulong()
            if not kernel32.GetExitCodeProcess(handle, ctypes.byref(code)):
                return True
            if code.value != 259:  # STILL_ACTIVE
                return False
        finally:
            kernel32.CloseHandle(handle)
    else:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            pass
    now = _process_start(pid)
    return started is None or now is None or now == started


def _host() -> str:
    """This host, as far as a pid means anything: containers that share a hostname
    (host networking) but not a pid namespace are different hosts here."""
    name = socket.gethostname()
    try:
        return f"{name}|{os.readlink('/proc/self/ns/pid')}"
    except (OSError, AttributeError):
        return name


def batch_key(task_path: str, variables: Optional[Dict[str, Any]]) -> str:
    """The lease key: the task and its variables (one logical batch)."""
    vars_ = {k: v for k, v in (variables or {}).items() if v is not None}
    raw = json.dumps({"task": task_path, "variables": vars_}, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _remove(files: List[str]) -> List[str]:
    removed = []
    for f in files:
        try:
            os.remove(f)
            removed.append(f)
        except FileNotFoundError:
            continue
        except OSError as exc:
            logger.warning("Could not remove %s: %s", f, exc)
    return removed


#: How long a lease file operation retries a ``PermissionError`` (F-048).
BUSY_FOR = 2.0
_BUSY_PAUSE = 0.01


def _patient(op: Any, *args: Any, **kwargs: Any) -> Any:
    """Run a file operation, retrying a ``PermissionError`` for up to :data:`BUSY_FOR`
    seconds, then raising it.

    On Windows a lease file is refused for a moment while another thread or process
    replaces it (a read, a stat, a rename), and a replace is refused while a reader has
    the file open. Both pass in milliseconds. Every other error, a missing file
    included, is raised at once: a lease that is gone is gone (F-048)."""
    deadline = time.monotonic() + BUSY_FOR
    while True:
        try:
            return op(*args, **kwargs)
        except PermissionError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(_BUSY_PAUSE)


def _read_text(path: Path) -> Optional[str]:
    """A lease file's text, or None when there is none. A file that exists but cannot
    be read, even after the patient retries, raises its ``OSError``: it is never taken
    for a missing one."""
    try:
        return _patient(path.read_text, encoding="utf-8")
    except FileNotFoundError:
        return None


def _parse(text: str) -> Dict[str, Any]:
    """A lease file's document, or {} when it is empty or not valid JSON."""
    try:
        doc = json.loads(text)
    except ValueError:
        return {}
    return doc if isinstance(doc, dict) else {}


class _Unreadable(Exception):
    """A lease file exists but cannot be read or parsed. Never guess what it says."""

    def __init__(self, path: Path, why: str):
        super().__init__(f"{path}: {why}")
        self.path = path
        self.why = why

    def refusal(self, task_path: str) -> "RunLeaseHeld":
        return RunLeaseHeld(
            f"{self.path.name} for {task_path} exists but cannot be read ({self.why}), "
            "so Ubunye cannot tell what it says. Nothing was run or taken back.",
            context={"File": str(self.path)},
            hint="Close whatever holds it (a backup, a scanner, a file permission), then "
            "run again. If it is damaged, check it before removing it: it may list "
            "another run's files.",
        )


def _write_atomic(path: Path, text: str) -> bool:
    tmp = path.with_name(f"{path.stem}.{uuid.uuid4().hex[:8]}.tmp")
    tmp.write_text(text, encoding="utf-8")
    try:
        _patient(os.replace, tmp, path)
        return True
    except PermissionError:  # held open for longer than a moment
        tmp.unlink(missing_ok=True)
        return False


class RunLease:
    """The lease one run holds on its task and batch (see the module docs)."""

    def __init__(
        self,
        root: Path,
        task_path: str,
        variables: Optional[Dict[str, Any]],
        run_id: str,
        lineage_dir: Optional[Path] = None,
    ):
        self.task_path = task_path
        self.run_id = run_id
        self.key = batch_key(task_path, variables)
        self.named = names_a_batch(variables)
        self.leases_dir = Path(root) / ".ubunye" / "leases" / task_path
        self.path = self.leases_dir / f"{self.key}.json"
        self.lineage_dir = Path(lineage_dir) if lineage_dir else Path(root) / ".ubunye" / "lineage"
        self._doc: Dict[str, Any] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._beat: Optional[threading.Thread] = None
        self.root = Path(root)
        self.left: Dict[str, List[str]] = {}
        self.keep = False  # the note could not be written: leave the lease for the next run
        self.recovered: Dict[str, Any] = {}
        self._current_output: Optional[str] = None

    # --- taking and giving back -------------------------------------------------------

    def acquire(self) -> "RunLease":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        pid = os.getpid()
        self._doc = {
            "run_id": self.run_id,
            "task": self.task_path,
            "pid": pid,
            "pid_started": _process_start(pid),
            "host": _host(),
            "started_at": _now(),
            "heartbeat": time.time(),
            "outputs": {},
        }
        for _ in range(3):
            if self._create(json.dumps(self._doc)):
                self._adopt_or_refuse()
                self._start_heartbeat()
                return self
            try:
                text = _read_text(self.path)
            except OSError as exc:
                # It exists but cannot be read: whether its run is alive cannot be
                # told, so never judge it dead by its age.
                raise _Unreadable(self.path, str(exc)).refusal(self.task_path) from None
            if text is None:
                continue  # it vanished between the two calls: try again
            # Empty or not JSON ({}): a crash inside ``_create`` leaves that, and it
            # names no process, so it is judged by its age alone, like another host's.
            held = _parse(text)
            if not self._dead(held):
                raise RunLeaseHeld(
                    f"Run {str(held.get('run_id', '?'))[:8]} of {self.task_path} with the "
                    f"same variables is still running (process {held.get('pid')} on "
                    f"{held.get('host')}, since {held.get('started_at')}).",
                    context={"Lease": str(self.path)},
                    hint="Wait for it to finish. Delete the lease only if that run is surely "
                    "gone (a reboot), after removing the files listed under 'claimed' in it.",
                )
            # Take a dead run's lease over: one rename wins. Then check the file renamed
            # is the one judged dead: another run may have taken it over just before.
            stale = self.path.with_name(f"{self.path.stem}.dead-{uuid.uuid4().hex[:8]}.json")
            try:
                _patient(os.replace, self.path, stale)
            except (FileNotFoundError, PermissionError):
                continue
            try:
                text = _read_text(stale) or ""
            except OSError as exc:
                # Which lease was renamed cannot be told: put it back, take nothing.
                self._put_back(stale)
                raise _Unreadable(stale, str(exc)).refusal(self.task_path) from None
            taken = _parse(text)
            if taken.get("run_id") != held.get("run_id"):
                # A live lease was renamed away. Put it back only if nobody took the
                # name meanwhile (never overwrite a lease); its owner checks ownership
                # before every claim, so if it cannot be put back that run stops.
                if text and self._create(text):
                    stale.unlink(missing_ok=True)
                else:
                    logger.warning("Lease %s was displaced; its run will stop.", self.path)
                    stale.unlink(missing_ok=True)
                continue
            if not self._create(json.dumps(self._doc)):
                # Another run created the lease in the instant between: the dead run's
                # lease stays beside it, and whoever holds the lease adopts it.
                continue
            # Hold the lease, then wait: a run judged dead that is in fact alive (paused
            # for long, or its pid misread) writes its lease back within one save.
            time.sleep(TAKEOVER_SETTLE)
            now = self._read()
            if now and now.get("run_id") == taken.get("run_id"):
                stale.unlink(missing_ok=True)  # the lease is its again, claims and all
                raise RunLeaseHeld(
                    f"Run {str(taken.get('run_id', '?'))[:8]} of {self.task_path} looked "
                    "dead but wrote its lease again: it is running. Nothing was taken back.",
                    context={"Lease": str(self.path)},
                    hint="Wait for it to finish.",
                )
            if self._owner() is not True:
                # The lease changed hands, or cannot be read: keep the dead run's lease
                # beside it (whoever holds the batch adopts it) and take nothing back.
                raise RunLeaseHeld(
                    f"The lease on {self.task_path} changed hands while run "
                    f"{self.run_id[:8]} was taking it over. Nothing was taken back.",
                    context={"Lease": str(self.path)},
                    hint="Run again in a moment.",
                )
            # Mark the dead run as taken over before touching its files. A run frozen
            # in the middle of a save could still overwrite this lease; the mark is how
            # it learns, afterwards, that it lost the batch.
            self._tombstone(str(taken.get("run_id") or "")).write_text(_now(), encoding="utf-8")
            try:
                self.recovered = self._recover(taken)
            except _Unreadable as bad:
                # Its record cannot be read, so whether it finished cannot be told:
                # hand the lease back as it was, and take nothing back.
                if _write_atomic(self.path, json.dumps(taken)):
                    stale.unlink(missing_ok=True)
                raise bad.refusal(self.task_path) from None
            except _InUse as busy:
                # Hand the lease back to the dead run, with what is left, and refuse: a
                # forgotten claim would let its batch land twice.
                if busy.replaces:
                    taken["replaces"] = busy.left
                else:
                    taken["outputs"] = {
                        n: {**taken["outputs"][n], "claimed": c} for n, c in busy.left.items()
                    }
                if _write_atomic(self.path, json.dumps(taken)):
                    stale.unlink(missing_ok=True)
                else:  # kept beside the lease: the next holder adopts it
                    _write_atomic(stale, json.dumps(taken))
                raise RunLeaseHeld(
                    f"Run {str(taken.get('run_id', '?'))[:8]} of {self.task_path} died, and "
                    + (
                        "its appended files cannot be removed yet (in use): "
                        + ", ".join(c for cs in busy.left.values() for c in cs)
                        if not busy.note
                        else "the note of its finished batch cannot be written yet (in "
                        f"use): {self._finished_path()}"
                    ),
                    context={"Lease": str(self.path)},
                    hint="Close whatever has those files open and run again.",
                ) from None
            stale.unlink(missing_ok=True)
            self._adopt_or_refuse()
            self._start_heartbeat()
            return self
        raise RunLeaseHeld(
            f"Could not take the lease on {self.task_path}: another run keeps taking it.",
            context={"Lease": str(self.path)},
            hint="Run again in a moment.",
        )

    def _adopt_or_refuse(self) -> None:
        """Adopt dead leases left beside this one; if their files cannot be removed yet,
        give the batch up rather than run it a second time over them."""
        try:
            self._adopt_orphans()
        except _Unreadable as bad:
            self._stop.set()
            self.path.unlink(missing_ok=True)
            raise bad.refusal(self.task_path) from None
        except _InUse as busy:
            self._stop.set()
            self.path.unlink(missing_ok=True)
            raise RunLeaseHeld(
                f"A dead run of {self.task_path} left appended files that cannot be removed "
                "yet (in use): " + ", ".join(c for cs in busy.left.values() for c in cs),
                context={"Lease": str(self.path)},
                hint="Close whatever has those files open and run again.",
            ) from None

    def _adopt_orphans(self) -> None:
        """Dead leases left beside this one by a takeover that lost the race to create
        the lease: take back their claims now that this run holds the batch."""
        for mark in self.leases_dir.glob(f"{self.path.stem}.taken-*"):
            try:
                if time.time() - mark.stat().st_mtime > 7 * 24 * 3600:
                    mark.unlink()
            except OSError:
                pass
        for orphan in self.leases_dir.glob(f"{self.path.stem}.dead-*.json"):
            try:
                text = _read_text(orphan)
            except OSError as exc:
                # Skipping it would leave its claimed files beside this run's append.
                raise _Unreadable(orphan, str(exc)) from None
            if text is None:
                continue
            # Empty or not JSON: a crash inside ``_create``, which claimed nothing (the
            # same judgement as a takeover of such a lease).
            self._recover(_parse(text))  # _InUse, _Unreadable: kept, the caller refuses
            orphan.unlink(missing_ok=True)

    def check_finished(self, appends: List[str], rerun: bool) -> None:
        """Before anything is written: refuse to append a batch a finished run already
        wrote, or, with ``rerun``, note its claimed files to remove once this run
        succeeds (F-031). The lease is this run's by now."""
        if not appends or not self.named:
            return
        path = self._finished_path()
        try:
            text = _read_text(path)
        except OSError as exc:
            raise _Unreadable(path, str(exc)).refusal(self.task_path) from None
        if text is None:
            return
        last = _parse(text)
        if not last:  # written in one replace, so never partly: something changed it
            raise _Unreadable(path, "not valid JSON").refusal(self.task_path)
        if not last.get("outputs"):
            return
        outputs = last["outputs"]
        who = f"run {str(last.get('run_id', '?'))[:8]} (finished {last.get('finished_at', '?')})"
        # What --rerun can replace (files the finished run claimed), and the outputs
        # this run appends to that it cannot (written by JDBC, a catalog or Delta table,
        # Spark on object storage, or overwritten whole), where the batch would land
        # again.
        replaces = {n: list(o["claimed"]) for n, o in outputs.items() if o.get("claimed")}
        again = [
            n
            for n in appends
            if not ((outputs.get(n) or {}).get("claimed") or (outputs.get(n) or {}).get("staged"))
        ]
        if not rerun:
            if again:
                hint = (
                    f"Output(s) {', '.join(again)} cannot be taken back by Ubunye (not "
                    "written as claimed part files on a local or shared disk), so --rerun "
                    "would append the "
                    "batch to them again. Remove that run's rows first, or write them "
                    "with mode overwrite_partitions."
                )
            else:
                hint = "To replace the batch, run again with --rerun (rerun=True in Python)."
            raise BatchFinished(
                f"{self.task_path} already finished this batch ({who}) and appends to "
                f"{', '.join(appends)}: running it again would add the batch twice.",
                context={"Finished": str(self._finished_path())},
                hint=hint + " If each run adds new data to the same batch, give each run "
                "its own variable, for example --var hour=13.",
            )
        with self._lock:
            self._doc["replaces"] = replaces
            if not self._save():
                raise self._lost()
        if replaces:
            logger.warning(
                "--rerun: replacing the batch written by %s; its %d file(s) are removed "
                "once this run succeeds.",
                who,
                sum(len(c) for c in replaces.values()),
            )
        if again:
            logger.warning(
                "--rerun: output(s) %s hold the batch written by %s and cannot be taken "
                "back (JDBC, a catalog or Delta table, Spark on object storage, or an "
                "overwrite), so this run appends "
                "the batch to them again. Remove that run's rows first, or write the "
                "output with mode overwrite_partitions.",
                ", ".join(again),
                who,
            )

    def commit(self) -> bool:
        """The run succeeded: its appends are the batch now, never to be taken back.

        Returns False, having changed nothing that is not its own, if the lease is no
        longer this run's. Order: a separate done marker (a reader holding the lease
        open, a scanner on Windows, can block rewriting it, never creating a new file);
        ``committed`` saved in the lease, which checks ownership after it writes; the
        note that this run finished the batch; and only then the removal of the files
        of the run it replaces (``--rerun``). A crash at any point leaves the lease
        saying how far it got, and the next run finishes the job (``_recover``)."""
        if self._owner() is False:
            return False
        try:
            self._done_mark(self.run_id).write_text(_now(), encoding="utf-8")
        except OSError as exc:
            logger.warning("Could not mark run %s done: %s", self.run_id[:8], exc)
        with self._lock:
            self._doc["committed"] = True
            if not self._save() and (
                self._owner() is False or self._tombstone(self.run_id).exists()
            ):
                return False  # taken over: the run that took over decides
            # Saved, or only unwritable for now (a reader has it open): the done
            # marker says this run finished, and the lease on disk still lists what
            # it replaces, so a crash from here on is finished by the next run.
            doc = copy.deepcopy(self._doc)
        if not self._note_finished(doc):
            # Without the note a later run could append the batch again, or a later
            # --rerun replace the wrong files. Keep the lease as it is (outputs and
            # replaced files listed): the next run of the batch writes the note first,
            # then removes the files (``_recover``).
            logger.error(
                "Could not note that run %s finished its batch (%s in use?). The lease is "
                "kept so the next run of the batch records it before anything else.",
                self.run_id[:8],
                self._finished_path(),
            )
            self.keep = True
            return True
        with self._lock:
            self._doc["outputs"] = {}
            self._save()
        self._replace(doc.get("replaces") or {})
        return True

    def _note_finished(self, doc: Dict[str, Any]) -> bool:
        """Say which run finished this batch, what it wrote, and the append files it
        claimed. An overwrite is noted too: it wrote the batch, so an append of the
        same batch afterwards would hold it twice."""
        if not self.named:
            return True
        written = {
            n: {
                "claimed": list(o.get("claimed") or []) if o.get("exact") else [],
                # Written through a staging folder: every file it added is claimed,
                # even when there were none (an empty batch).
                **({"staged": True} if o.get("exact") and o.get("staging") else {}),
            }
            for n, o in (doc.get("outputs") or {}).items()
        }
        if not written:
            return True
        note = {
            "run_id": doc.get("run_id"),
            "task": self.task_path,
            "finished_at": _now(),
            "outputs": written,
        }
        try:
            return _write_atomic(self._finished_path(), json.dumps(note, indent=2))
        except OSError:
            return False

    def _replace(self, replaces: Dict[str, List[str]]) -> None:
        """Remove the files of the run this one replaced. Files in use stay listed in
        the lease, which is kept, so the next run removes them (never forgotten)."""
        if not replaces:
            return
        removed, left = self._take_back({n: {"claimed": c} for n, c in replaces.items()})
        with self._lock:
            if left:
                self._doc["replaces"] = left
            else:
                self._doc.pop("replaces", None)
            self._save()
        logger.warning("Replaced the batch: removed %d file(s) of the earlier run.", len(removed))
        if left:
            logger.error(
                "Could not remove %s (in use?). The lease is kept so the next run removes "
                "them; close whatever holds them.",
                ", ".join(c for cs in left.values() for c in cs),
            )
            self.left = left

    def release(self) -> None:
        self._stop.set()
        if self._beat is not None:
            self._beat.join()  # never unlink while a beat could still write
        if self._owner() is not True:
            return
        try:
            _patient(self.path.unlink, missing_ok=True)
            _patient(self._done_mark(self.run_id).unlink, missing_ok=True)
            return
        except PermissionError:  # held open for longer than a moment
            pass
        logger.warning("Could not remove the lease %s; the next run takes it over.", self.path)

    # --- outputs ----------------------------------------------------------------------

    def writing(self, output: str, *, appends: bool, exact: bool) -> None:
        """An output is about to be written. ``exact``: its backend claims its files."""
        with self._lock:
            self._doc["outputs"][output] = {
                "appends": appends,
                "exact": exact,
                "state": "writing",
                "claimed": [],
            }
            if not self._save():
                raise self._lost()
        self._current_output = output

    def written(self, output: str) -> None:
        with self._lock:
            note = self._doc["outputs"].get(output)
            if note is not None:
                note["state"] = "done"
                self._save()

    def claim(self, path: str) -> None:
        """Before a file is moved into an append output: record it as this run's.

        Raises :class:`RunLeaseLost` (and nothing is moved) if the lease is no longer
        this run's, or the claim could not be saved.
        """
        self.claim_all([path])

    def claim_all(self, paths: List[str]) -> None:
        """:meth:`claim` for many files, in one save of the lease."""
        output = self._current_output
        with self._lock:
            note = self._doc["outputs"].get(output) if output else None
            if note is None or not paths:
                return
            before = len(note["claimed"])
            note["claimed"].extend(self._portable(p) for p in paths)
            if not self._save():
                del note["claimed"][before:]
                raise self._lost()

    def staging(self, folder: str) -> None:
        """Before a backend writes an append into a new folder of its own, to move the
        files into the output afterwards (Spark path appends): record that folder, and
        that this output's files will be claimed before they land (``exact``).

        The folder is named by this run (a fresh UUID) and recorded before it exists,
        so taking it back removes only this run's staging. Raises
        :class:`RunLeaseLost` if the lease is no longer this run's."""
        output = self._current_output
        with self._lock:
            note = self._doc["outputs"].get(output) if output else None
            if note is None:
                return
            was = (note.get("exact"), note.get("staging"))
            note["exact"] = True
            note["staging"] = self._portable(folder)
            if not self._save():
                note["exact"], note["staging"] = was
                raise self._lost()

    def landed(self, path: str) -> None:
        """After a claimed file was moved in. A run taken over meanwhile (frozen between
        claim and move) removes it again: the run that took over did not see it.

        One stat: a takeover marks the run it takes over (the tombstone) before it
        takes anything back. Reading the whole lease here, once per file, made a
        12,000 file append take 92 s (F-070 skeptic review); :meth:`all_landed` reads
        it once after the last file."""
        if self._tombstone(self.run_id).exists():
            _remove([path])
            raise self._lost()

    def all_landed(self, paths: List[str]) -> None:
        """After the last claimed file of a write was moved in: the full ownership
        check. A lease that is gone or another run's (displaced, not taken over) has
        this write's files removed again."""
        if self._owner() is False:
            _remove(list(paths))
            raise self._lost()

    def still_owned(self) -> bool:
        """Whether the lease is still this run's (checked before success is recorded)."""
        return self._owner() is not False

    def _lost(self) -> RunLeaseLost:
        return RunLeaseLost(
            f"Run {self.run_id[:8]} no longer holds the lease on {self.task_path} (or could "
            "not record in it); it stops before writing more.",
            context={"Lease": str(self.path)},
        )

    def _portable(self, path: str) -> str:
        """A claim relative to the usecase folder when it can be, so a host that mounts
        the shared disk elsewhere resolves it to the same file."""
        full = os.path.realpath(path)
        try:
            return os.path.relpath(full, os.path.realpath(self.root))
        except ValueError:  # another drive on Windows
            return full

    def _resolve(self, claim: str) -> str:
        if os.path.isabs(claim):
            return claim
        return os.path.normpath(os.path.join(os.path.realpath(self.root), claim))

    def _take_back(self, outputs: Dict[str, Any]) -> Tuple[List[str], Dict[str, List[str]]]:
        """Remove claimed files. Returns (removed, left): ``left`` could not be removed
        (in use) and stays claimed, never forgotten."""
        removed: List[str] = []
        left: Dict[str, List[str]] = {}
        for name, note in outputs.items():
            if note.get("staging"):
                # The run's own staging folder (named by it, recorded before it was
                # made): nothing in it has landed in the output.
                shutil.rmtree(self._resolve(note["staging"]), ignore_errors=True)
            for claim in note.get("claimed") or []:
                full = self._resolve(claim)
                if not os.path.exists(full):
                    if not os.path.isdir(os.path.dirname(full)):
                        note["unseen"] = True  # its folder is not visible from here
                    continue  # claimed, never landed
                if _remove([full]):
                    removed.append(full)
                    _drop_success_marker(os.path.dirname(full))
                else:
                    left.setdefault(name, []).append(claim)
        return removed, left

    def rollback(self) -> List[str]:
        """This run failed: remove the files it claimed; name appends it cannot undo."""
        with self._lock:
            outputs = copy.deepcopy(self._doc.get("outputs") or {})
        removed, left = self._take_back(outputs)
        kept = _unrepaired(outputs)
        if removed:
            logger.warning(
                "Run %s failed; removed the %d file(s) it had appended, so a rerun "
                "appends them once.",
                self.run_id[:8],
                len(removed),
            )
        if kept:
            logger.warning(
                "Run %s failed after appending to %s; that cannot be taken back here, so "
                "a rerun appends the batch again. Check those outputs before rerunning.",
                self.run_id[:8],
                ", ".join(kept),
            )
        with self._lock:
            # Files still in use stay claimed; the lease is then kept (see ``held``) so
            # the next run takes them back.
            self._doc["outputs"] = {
                n: {**outputs[n], "claimed": claims} for n, claims in left.items()
            }
            self._save()
        if left:
            logger.error(
                "Run %s could not remove %s (in use?). The lease is kept so the next run "
                "takes them back; close whatever holds them.",
                self.run_id[:8],
                ", ".join(c for cs in left.values() for c in cs),
            )
        self.left = left
        return removed

    # --- internals --------------------------------------------------------------------

    def _put_back(self, stale: Path) -> None:
        """Put a lease renamed away for a takeover back, never over another lease (a
        hard link fails if the name is taken). If it cannot be, it stays beside the
        lease, where whoever holds the batch adopts it."""
        try:
            os.link(stale, self.path)
        except OSError:
            logger.warning("Lease %s could not be put back; it is kept as %s.", self.path, stale)
            return
        try:
            stale.unlink()
        except OSError:
            pass

    def _create(self, text: str) -> bool:
        try:
            fd = os.open(str(self.path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            return False
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
        return True

    def _read(self, path: Optional[Path] = None) -> Optional[Dict[str, Any]]:
        """The lease: None if there is none, {} if it cannot be read right now.

        A read that meets the heartbeat's replace is retried, not called unreadable
        (F-048)."""
        try:
            return json.loads(_patient((path or self.path).read_text, encoding="utf-8"))
        except FileNotFoundError:
            return None
        except (ValueError, OSError):
            return {}

    def _finished_path(self) -> Path:
        return self.leases_dir / f"{self.path.stem}.finished.json"

    def _done_mark(self, run_id: str) -> Path:
        return self.leases_dir / f"{self.path.stem}.done-{run_id}"

    def _tombstone(self, run_id: str) -> Path:
        return self.leases_dir / f"{self.path.stem}.taken-{run_id}"

    def _owner(self) -> Optional[bool]:
        """True: ours. False: gone, another run's, or this run was taken over. None:
        cannot tell right now."""
        if self._tombstone(self.run_id).exists():
            return False
        for _ in range(5):
            try:
                text = _read_text(self.path)  # one patient read, at most BUSY_FOR
            except OSError:
                return None
            if text is None:
                return False
            held = _parse(text)
            if held:
                return held.get("run_id") == self.run_id
            time.sleep(0.05)  # empty or partial: a lease being put back by ``_create``
        return None

    def _save(self) -> bool:
        """Write the lease, only while it is this run's (caller holds the lock)."""
        if self._owner() is not True:
            return False
        if _write_atomic(self.path, json.dumps(self._doc)):
            # Taken over while this save was under way: the write does not count.
            return not self._tombstone(self.run_id).exists()
        logger.warning("Could not update the lease %s", self.path)
        return False

    def _disk_now(self) -> float:
        """The time by the lease folder's own disk, so no two hosts' clocks are compared."""
        probe = self.leases_dir / f".clock-{uuid.uuid4().hex[:8]}"
        try:
            probe.write_text("", encoding="utf-8")
            return probe.stat().st_mtime
        except OSError:
            return time.time()
        finally:
            probe.unlink(missing_ok=True)

    def _dead(self, held: Dict[str, Any]) -> bool:
        if held and held.get("host") == _host():
            pid = int(held.get("pid") or 0)
            if held.get("kept") and pid == os.getpid():
                return True  # this process's own earlier run, which failed and ended
            if not _pid_alive(pid, held.get("pid_started")):
                return True
            if not (held.get("pid_started") and _process_start(pid) is None):
                return False
            # Alive, but not inspectable (access denied): maybe a reused pid. Judge by
            # the lease's age, as for another host.
        try:
            touched = _patient(self.path.stat).st_mtime
        except OSError:
            return False  # gone, or cannot be told: never dead
        return self._disk_now() - touched > HEARTBEAT_TIMEOUT

    def _start_heartbeat(self) -> None:
        def beat() -> None:
            while not self._stop.wait(HEARTBEAT_EVERY):
                try:
                    with self._lock:
                        held = self._read()
                        if held and held.get("run_id") != self.run_id:
                            return  # taken over: this run no longer writes the lease
                        if not held:
                            continue  # missing or unreadable for now: try again
                        self._doc["heartbeat"] = time.time()
                        self._save()
                except Exception as exc:  # noqa: BLE001 (a missed beat is not a failed run)
                    logger.debug("lease heartbeat: %s", exc)

        self._beat = threading.Thread(target=beat, name="ubunye-lease", daemon=True)
        self._beat.start()

    def _recover(self, dead: Dict[str, Any]) -> Dict[str, Any]:
        """Remove the files a dead run claimed; say in its record it died."""
        outputs = dead.get("outputs") or {}
        dead_id = str(dead.get("run_id") or "")
        done = self._done_mark(dead_id) if dead_id else None
        if (
            dead.get("committed")
            or (done is not None and done.exists())
            or self._record_status(dead_id) == "success"
        ):
            # It finished (its lease outlived it): its appends are the batch. If it died
            # before noting that, or before removing the files of the run it replaced
            # (--rerun), finish that for it.
            replaced = dead.get("replaces") or {}
            if dead.get("outputs") and not self._note_finished(dead):
                raise _InUse(replaced, replaces=True, note=True)  # the note, then the files
            removed, left = self._take_back({n: {"claimed": c} for n, c in replaced.items()})
            if left:
                raise _InUse(left, replaces=True)
            if done is not None:
                done.unlink(missing_ok=True)
            return {"run_id": dead_id, "removed": removed, "unrepaired": []}
        removed, left = self._take_back(outputs)
        if left:
            raise _InUse(left)
        unrepaired = _unrepaired(outputs)
        try:
            self._mark_interrupted(dead_id, unrepaired)
        except Exception as exc:  # noqa: BLE001 (the record is a report; the run goes on)
            logger.warning("Could not mark run %s interrupted: %s", dead_id[:8], exc)
        if dead_id:
            logger.warning(
                "Run %s of %s died before it finished; this run takes over. Removed %d "
                "file(s) it had appended.%s",
                dead_id[:8],
                self.task_path,
                len(removed),
                (
                    f" Output(s) {', '.join(unrepaired)} may hold part or all of its batch "
                    "and cannot be taken back here: check them."
                    if unrepaired
                    else ""
                ),
            )
        return {"run_id": dead_id, "removed": removed, "unrepaired": unrepaired}

    def _record_status(self, run_id: str) -> Optional[str]:
        if not run_id:
            return None
        record = self.lineage_dir / self.task_path / f"{run_id}.json"
        try:
            text = _read_text(record)
        except OSError as exc:
            # It may say "success" (it is written before the commit): taking the run
            # for unfinished would take back what may be the batch.
            raise _Unreadable(record, str(exc)) from None
        # Missing, or partly written when the run died (the record is not written in
        # one replace): the run did not get as far as recording success.
        return _parse(text).get("status") if text else None

    def _mark_interrupted(self, run_id: str, unrepaired: List[str]) -> None:
        if not run_id:
            return
        record = self.lineage_dir / self.task_path / f"{run_id}.json"
        try:
            doc = json.loads(record.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return
        if doc.get("status") == "running":
            doc["status"] = "interrupted"
            note = (
                f"The run did not finish; run {self.run_id[:8]} took over its lease at "
                f"{_now()} and removed the files it had appended."
            )
            if unrepaired:
                note += (
                    f" Output(s) {', '.join(unrepaired)} may hold part or all of its batch "
                    "and were not taken back."
                )
            doc["error"] = note
            _write_atomic(record, json.dumps(doc, indent=2, ensure_ascii=False))


def _unrepaired(outputs: Dict[str, Any]) -> List[str]:
    """Append outputs a backend could not claim files for, that may hold data.

    An output written through a staging folder (``staging``, Spark path appends) is
    fully repaired even with no claims: nothing reaches the output unclaimed, so no
    claims means nothing landed (a refusal, a kill before the claims, Spark failing).
    An exact output with no staging and no claims is still named: a custom writer on
    a claiming backend may have appended without claiming."""

    def repaired(o: Dict[str, Any]) -> bool:
        if o.get("unseen") or not o.get("exact"):
            return False
        return bool(o.get("staging") or o.get("claimed"))

    return sorted(n for n, o in outputs.items() if o.get("appends") and not repaired(o))


class _InUse(Exception):
    def __init__(self, left: Dict[str, List[str]], replaces: bool = False, note: bool = False):
        super().__init__("in use")
        self.left = left
        self.replaces = replaces  # files of a replaced run, not the dead run's own
        self.note = note  # the batch note could not be written (nothing was removed)


def _holds_data(folder: str) -> bool:
    """Whether a data file (not ``_`` or ``.``) is in ``folder`` or its partition folders."""
    try:
        entries = list(os.scandir(folder))
    except OSError:
        return True  # cannot tell: leave the marker alone
    for entry in entries:
        name = entry.name
        if name.startswith("."):
            continue
        if entry.is_dir():
            if "=" in name and _holds_data(entry.path):
                return True
        elif not name.startswith("_"):
            return True
    return False


def _drop_success_marker(folder: str) -> None:
    """A folder left with no data files must not say it holds a complete dataset.

    A partitioned output (F-012) keeps its ``_SUCCESS`` at the root, above the
    ``name=value`` folders the file was in: that root is checked, counting the
    data files in all its partition folders.
    """
    root = folder
    while "=" in os.path.basename(root):
        root = os.path.dirname(root)
    marker = os.path.join(root, "_SUCCESS")
    if os.path.exists(marker) and not _holds_data(root):
        try:
            os.remove(marker)
        except OSError:
            pass


def lost() -> Optional[str]:
    """Why this run may not record success (its lease was taken over), or None."""
    lease = _CURRENT.get()
    if lease is None or lease.still_owned():
        return None
    return (
        f"Run {lease.run_id[:8]} lost its lease on {lease.task_path} to another run "
        "(it was judged dead). That run takes back what this one appended and writes the "
        "batch itself: check its record before running the batch again."
    )


def landed(path: str) -> None:
    """Called by a backend just after it moved a claimed file into an append output."""
    lease = _CURRENT.get()
    if lease is not None:
        lease.landed(path)


def all_landed(paths: List[str]) -> None:
    """Called by a backend after the last claimed file of a write was moved in."""
    lease = _CURRENT.get()
    if lease is not None:
        lease.all_landed(paths)


def current() -> Optional[RunLease]:
    """The lease of the run in progress, if any."""
    return _CURRENT.get()


def writing(output: str, *, appends: bool, exact: bool) -> None:
    """Called by the engine before an output is written."""
    lease = _CURRENT.get()
    if lease is not None:
        lease.writing(output, appends=appends, exact=exact)


def written(output: str) -> None:
    """Called by the engine after an output was written."""
    lease = _CURRENT.get()
    if lease is not None:
        lease.written(output)


def claim(path: str) -> None:
    """Called by a backend just before it moves a new file into an append output."""
    lease = _CURRENT.get()
    if lease is not None:
        lease.claim(path)


def claim_all(paths: List[str]) -> None:
    """:func:`claim` for many files at once (one save of the lease)."""
    lease = _CURRENT.get()
    if lease is not None:
        lease.claim_all(paths)


def staging(folder: str) -> None:
    """Called by a backend before it writes an append into a staging folder of its own,
    whose files it then claims and moves into the output (Spark path appends)."""
    lease = _CURRENT.get()
    if lease is not None:
        lease.staging(folder)


class held:
    """``with runs.held(root, task_path, variables, run_id):`` around one run.

    If the lease folder cannot be written (a read-only checkout), the run goes on
    without a lease and says so: rerun safety is never a reason a run cannot start.
    """

    def __init__(
        self,
        root: Path,
        task_path: str,
        variables: Optional[Dict[str, Any]],
        run_id: str,
        lineage_dir: Optional[Path] = None,
        *,
        appends: Optional[List[str]] = None,
        rerun: bool = False,
    ):
        self.lease = (
            RunLease(root, task_path, variables, run_id, lineage_dir) if enabled() else None
        )
        self.appends = list(appends or [])
        self.rerun = rerun
        self._token: Optional[contextvars.Token] = None

    def __enter__(self) -> Optional[RunLease]:
        if self.lease is None:
            return None
        try:
            self.lease.acquire()
            try:
                self.lease.check_finished(self.appends, self.rerun)
            except BaseException:
                self.lease.release()  # nothing was written: the batch stays as it is
                raise
        except RunLeaseHeld:
            raise
        except OSError as exc:
            logger.warning(
                "No run lease (%s): this run is not protected against a concurrent run of "
                "the same batch.",
                exc,
            )
            self.lease = None
            return None
        self._token = _CURRENT.set(self.lease)
        return self.lease

    def __exit__(self, exc_type: Any, *exc: Any) -> None:
        if self.lease is None:
            return
        if self._token is not None:
            _CURRENT.reset(self._token)
        keep = exc_type is not None  # until the take-back has finished
        try:
            if exc_type is not None:
                try:
                    self.lease.rollback()
                    keep = bool(self.lease.left)
                except Exception as rb:  # noqa: BLE001 (never hide the run's own error)
                    logger.error("Could not take back run %s's appends: %s", self.lease.run_id, rb)
        finally:
            if keep:
                # Claims remain (or the take-back was interrupted): leave the lease for
                # the next run to take over, even one started by this same process.
                self.lease._stop.set()
                try:
                    with self.lease._lock:
                        self.lease._doc["kept"] = True
                        self.lease._save()
                except Exception:  # noqa: BLE001 (best effort: pid death also frees it)
                    pass
            else:
                lost_it = exc_type is None and not self.lease.still_owned()
                if exc_type is None and not lost_it:
                    lost_it = not self.lease.commit()
                if self.lease.left or self.lease.keep:
                    # A replaced run's file, or the batch note, is in use: keep the lease (it lists the
                    # file) for the next run to remove, as for a failed run's claims.
                    self.lease._stop.set()
                    try:
                        with self.lease._lock:
                            self.lease._doc["kept"] = True
                            self.lease._save()
                    except Exception:  # noqa: BLE001 (pid death also frees it)
                        pass
                else:
                    self.lease.release()
                if lost_it:
                    raise self.lease._lost()
