# ADR 008: A rerun is safe: one live run per batch, appends land once

**Status:** accepted (hardening programme, 2026-09-29)

## Context

A pipeline run can stop at any moment: a power cut, a killed job, a failed write. What
matters is what the next run does. The hardening experiments measured it:

- E-01: a run killed after committing an `append` output was appended again by the
  rerun, in 16 of 16 trials: the batch landed twice, silently (finding F-011).
- E-02: two runs of the same task and date at once both succeeded and appended twice in
  7 of 10 pairs (F-019); when they started together the loser failed with a raw OS error
  (F-020).
- A killed run's record said `running` for ever (F-013).

The AbsaOSS ingestion tool Pramen met these in production and answered with a lease per
table and information date, and with repairs made from what was actually written.

## Decision

- **A lease per batch.** A run takes a lease on its task and its variables (`dt`,
  `--var`): the batch. The lease is a file under `<usecase_dir>/.ubunye/leases/`,
  created atomically, naming the run, its process, host and a heartbeat. A second run of
  the same batch while the first lives is refused (`RunLeaseHeld`), naming the first.
  Runs of different batches never block each other.
- **A dead run is taken over.** A lease whose process is gone (same host, checked by
  pid and process start time, so a reused pid is not mistaken for it), or whose file
  has not been touched for 15 minutes by the shared disk's own clock (another host, so
  no two clocks are compared), belongs to a dead run. The next run takes it over, marks
  that run's record `interrupted`, and takes back its claimed appends.
- **Only claimed files are ever removed.** A backend that names the files it appends
  *claims* each one in the lease before it lands. The pandas backend does: one part
  file with a fresh UUID in its name, moved into the folder in one step. A run that
  fails removes its claimed files; the run that takes over a dead run removes that
  run's. No folder is ever listed to decide what to delete, so another run's files are
  never touched.
- **What cannot be claimed is named, never deleted.** Spark, JDBC and catalog appends
  are not claimed. If such a run fails or dies after appending, the log and the dead
  run's record say which outputs may hold part or all of the batch. A missing `mode`
  counts as append, since most writers default to it.
- **A claim is never forgotten.** A claimed file that cannot be removed (a reader has
  it open, common on Windows) stays in the lease, and the lease stays: the next run
  takes it back, or refuses and names the file. Claims are kept relative to the
  usecase folder, so a host that mounts the shared disk elsewhere finds them.
- **A run that lost its lease stops, and is never a success.** Every output and every
  claim is recorded in the lease before anything lands, and only while the lease is
  this run's. A takeover holds the lease for a moment before taking anything back (a
  run judged dead that writes its lease again is alive, and is left alone), and leaves
  a mark naming the run it took over. A run that finds that mark stops; if it gets as
  far as the end, its record says `interrupted`, not `success`. A lease is only ever
  created, never overwritten, including when a mistaken takeover puts one back.
- **A refusal is not a crash.** `ubunye run` prints one line naming the run that holds
  the batch and exits 1, with no traceback. The hint says not to delete the lease of a
  run that may be alive: it holds that run's claims.
- **On by default.** `UBUNYE_RUN_LEASE=off` turns it off.

## What it does not do, said plainly

- The batch is the task and **all** its variables. A rerun of the same `dt` with a
  different `dtf`, `--mode` spelling or an extra `--var` is a different batch: it is
  not refused while the first runs, and it does not take back a dead run's claims.
  Rerun a batch with the same variables.
- A successful run that is killed after its success is recorded, but before it marks
  its lease done, is still treated as finished when lineage is on (its record says
  `success`). With lineage off, that narrow window can have its appends taken back by
  the next run of the batch, which then writes the batch itself.
- On macOS a process's start time is not read, so a dead run whose pid was reused
  keeps its lease looking alive until that process ends.
- The lease protects runs that share the usecase folder: one machine, or a shared
  disk. Cloud jobs that each start on their own disk are not protected by it; a lease in
  object storage (a conditional write) is the next step.
- Appends are taken back on the pandas backend only. On Spark use `overwrite`,
  `overwrite_partitions` or Delta for batches that must land once; plain Spark
  appends are named after a crash, not repaired.
- A second run of a batch that already **finished** appends again. The lease is gone
  once a run ends, so it cannot know. E-01 caught this once in 20 (a kill that landed
  after the run completed). Skipping or replacing a finished batch is a product
  decision, recorded as finding F-031.
- It does not make a whole run exactly-once for outputs other than appends: an
  `overwrite` output was already safe (E-01, E-02: correct in every trial), since it is
  replaced in one step.

## Consequences

- E-01 rerun (20 hard kills): 19 of 20 exactly right; the one wrong trial was F-031
  (the killed run had already finished). No debris, no `running` record left.
- E-02 rerun: 0 of 10 doubled (7 of 10 before); every second run refused in one line
  naming the first.
- A fourth review found a successful run's lease left behind by a Windows file lock
  (the next run then took back committed data), a file landing after a takeover, an
  orphan whose file was in use, and a claim made through a drive letter. Each is fixed
  (a separate done marker, a check after each file lands, a refusal, real paths) and
  has a unit test.
- Three adversarial reviews shaped this. The third found no way to delete another
  run's file, and proved two silent doubles (a claim forgotten when its file was in
  use; a claim not found from another mount), a heartbeat that stopped for good, and a
  run frozen mid-save that kept going after a takeover. Each has a unit test.
- Two earlier versions worked out a run's files by listing the output folder (all new
  files, then new files between before and after the write). Adversarial reviews
  proved, by script, that both delete another run's committed append when two batches
  share a folder. Claims made by the writer are the answer: ownership is stated by the
  code that created the file, never inferred.
- **Behaviour change:** a run that fails now removes the appends it wrote; a run of a
  batch that is already running is refused rather than run.
- A `.ubunye/leases/` folder appears next to `.ubunye/lineage/`.
