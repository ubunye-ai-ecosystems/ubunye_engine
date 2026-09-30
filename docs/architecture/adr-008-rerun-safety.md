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
  file with a fresh UUID in its name (one per partition folder for a partitioned
  append, F-012), each moved into place in one step. So does Spark for a path on a
  local or shared disk (F-070, addendum below). A run that
  fails removes its claimed files; the run that takes over a dead run removes that
  run's. No output folder is ever listed to decide what to delete, so another run's
  files are never touched.
- **What cannot be claimed is named, never deleted.** JDBC, catalog and Delta
  appends, and Spark appends to object storage, are not claimed. If such a run fails or dies after appending, the log and the dead
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
- Appends are taken back on the pandas backend, and on Spark for plain file formats
  on a path Spark resolves to the local file system (F-070). A Spark append to object
  storage (`s3a://`, `abfss://`, `gs://`, `dbfs:/`), HDFS, a Delta or catalog table,
  or JDBC is named after a crash, not repaired: use `overwrite`,
  `overwrite_partitions`, `merge` or Delta's `replace_where` for batches that must land
  once (F-071, F-072, F-073).
- A run of a batch that already **finished** is covered by the addendum below (F-031),
  not by the lease, which is gone once a run ends.
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
- On Windows a lease file is refused for a moment while it is replaced, and a replace
  is refused while a reader has the file open (F-048). Every lease read, stat, rename
  and removal retries a `PermissionError` for up to 2 seconds. A missing file is
  never waited for, and it is still absent.
- **A file that exists but cannot be read is never absent, free or dead** (F-048
  skeptic review). If a lease, a dead run's lease left beside it, the finished note
  or a dead run's record still cannot be read after the retries, the run is refused
  and names the file. A takeover that cannot read the lease it renamed puts it back.
  A finished note that is not valid JSON is refused too: it is written in one
  replace, so something else changed it. One exception: a lease that is empty or not
  valid JSON is judged by its age, as before, since a crash inside its creation
  leaves one, and it names no process and claims nothing.
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

## Addendum: a finished batch (F-031, 2026-09-29)

E-01 caught a second full run of a finished batch appending it again. Three answers
were on the table: skip it (Pramen's default for a date already done), refuse it, or
replace it.

**Decision: refuse by default; `--rerun` replaces.**

- A run of a *named* batch (`dt` or a `--var`; `mode` and `dtf` alone name nothing)
  that succeeds leaves `<key>.finished.json` beside its lease: the run, the outputs
  it wrote and the append files it claimed. An overwrite is noted too, since it wrote
  the batch. A later run of that batch, whose task has an append output, is refused
  (`BatchFinished`, a `RunLeaseHeld`: one line, exit 1).
- **Resuming a pipeline is asked for by name.** `run --resume` (`run_pipeline(...,
  resume=True)`) skips the tasks that finished the batch and runs the rest. Skipping
  by default was tried and rejected: in an hourly pipeline (same `dt`, `t1` appends,
  `t2` overwrites) it dropped `t1`'s new rows and exited 0 (skeptic round 6).
- **The note is written before anything is replaced.** If it cannot be written (a
  reader has it open), the run keeps its lease as it is and removes nothing; the next
  run of the batch writes the note first, then removes the replaced files, or waits.
- `--rerun` (`rerun=True`) replaces the batch. The finished run's claimed files are
  listed in the new lease as `replaces` and removed only after the new run has
  committed: done mark, then the new note, then removal. A crash before the done mark
  keeps the old batch (the new files are taken back as usual); a crash after it is
  finished by the next run, which removes the listed files. Files in use keep the
  lease, as for claims. A run whose lease was taken over before it saved `committed`
  removes nothing and is not a success (skeptic round 5 proved the earlier order
  deleted both runs' files).
- Appends that were never claimed (JDBC, catalog and Delta tables, Spark on object
  storage) cannot be replaced: with `--rerun` they are appended again and the run says
  so, naming the outputs.

**Why not skip.** A skip exits 0 and writes nothing. A job that runs `dt=today`
every hour and appends new rows each time would then lose every run after the
first, silently. A refusal fails loudly and loses nothing; its hint names both
ways forward (`--rerun`, or a variable per run such as `--var hour=13`).

**Why not replace by default.** The same hourly job would then delete the morning's
rows at noon. Replacing is right for a backfill and wrong for that job, so it is
asked for by name.

**Not refused:** a run with no named batch (a snapshot job, where every run is the
same key), a task whose outputs all overwrite (already safe), and batches finished
before this change (they left no note). Cloud jobs on their own disks see no note,
as they see no lease. Notes are small and are kept, one per task and batch; nothing
removes them. `--rerun` replaces the files the finished run claimed wherever they
are, even if the output's path has changed since. Files moved or compacted by
something else since are not found, so `--rerun` then removes nothing and appends
(the log says how many files it removed). `dtf` stays part of the batch key:
dropping it would strand leases left by earlier versions.

## Addendum: Spark path appends are claimed (F-070, 2026-09-30)

A Spark `append` to a folder of files was named after a crash, never taken back. On
live Spark 4.2 a run killed after its append was appended again by the rerun, a run
that failed after its append left it there, and `--rerun` appended a finished batch a
second time (9 of 10 new integration cases fail on the old code).

**Why Spark cannot claim its own files.** Spark names each file
`part-NNNNN-<job uuid>.c000<ext>` with a job UUID it makes inside the write
(`InsertIntoHadoopFsRelationCommand`, the same in Spark 3.5.8 and 4.0.1). No option
sets it. When the files appear in the output depends on the committer: at job commit
with `FileOutputCommitter` v1, at each task commit with v2, and a failed v2 job leaves
files behind. So no claim can be made before Spark's files land in the output.

**Decision: Spark writes into a staging folder the lease names, then the files are
claimed and moved in.**

1. The run records a new folder beside the output, `.<name>.ubunye-<uuid>`, in the
   lease, and marks the output as claimed exactly. The folder does not exist yet.
2. Spark writes the batch into that folder (partition folders included).
3. The data files in it are this run's by construction: the run named the folder and
   nothing else writes there. Their names are claimed in the lease, all in one save.
4. Each is moved into the output in one rename (same disk), and checked as landed.
   `_SUCCESS` is touched in the output, as Spark's own append leaves it.
5. The staging folder is removed. A run that fails, or the run that takes over a dead
   one, removes the staging folder named in the lease and the claimed files, nothing
   else.

**Why this is not inferring ownership.** The rule is unchanged: the writer states
what is its own. What is listed is a folder this run named and recorded before it
existed, not the output. The output folder is never listed. A file already in the
output with the same name as one of the run's files is refused before anything is
claimed, so a claim can never name a file that is not this run's.

**Which appends.** A run lease is held; the format is `parquet`, `csv`, `json`,
`orc`, `avro` or `text`; and Spark resolves the path to the local file system (a
laptop, a VM disk, a shared mount; a path with no scheme on a cluster whose default
file system is HDFS is not local). Everything else goes to Spark directly, as before,
and is named after a crash. What Spark itself skips is not moved: `.crc` checksums,
`_SUCCESS`, and Parquet summary files, which describe the staging folder only.

**What it costs.** One rename per file and one lease save for all the claims. Spark
writes the same data either way.

**Not covered, filed:** Delta appends (F-071: Delta's `txnAppId` and `txnVersion`
make a write idempotent, but they also skip a deliberate `--rerun` and a rerun after
the rows were removed by hand, silently); JDBC appends (F-072); Spark appends to
object storage and HDFS (F-073: a rename there is a copy, and a take back needs the
Hadoop file system, which a run that takes over may not have started).

**Proof:** `tests/integration/test_spark_append_claims.py` (E-01 and E-02 ported to
Spark: a real kill of a child process after the append and before the claims, a
foreign file in the folder, `--rerun`, four formats partitioned, a failed run, two
runs at once). On the old code 9 of 10 fail (the batch twice, or a failed run's
append left); on the new code 10 of 10 pass. Two runs at once was already safe (the
lease refused the second) and passes on both.
