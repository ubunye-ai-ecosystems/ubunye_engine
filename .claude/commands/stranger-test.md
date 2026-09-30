---
description: Run a stranger test - a newcomer solves a real problem from the public docs only
argument-hint: <kaggle owner/name or local data path> <problem in one sentence>
---

Run a stranger test for: $ARGUMENTS

1. Get the data. A Kaggle dataset: dispatch `kaggle-to-cloud.yml` in
   `ubunye-ai-ecosystems/ubunye-infra` (`-f dataset=<owner/name> -f clouds=gcp`), wait,
   then `gh run download <id> -n kaggle-data` into a fresh scratch folder. Never into
   this repo.
2. Make a clean venv in that folder with only the engine version under test (PyPI, or a
   wheel built from this branch with `python -m build`).
3. Spawn the `stranger` agent in the background with: the venv path, the data path, the
   work folder, the problem stated as a user would state it, and a timebox (90 minutes).
   Give it no hints about the engine.
4. When it reports: turn every blocker and major into a finding (`/finding`), after
   reproducing it yourself. Verify any claim about Spark with the `parity-checker`
   before you believe it.
5. Add the run to `tasks/hardening/SCOREBOARD.md`: date, engine version, dataset,
   finished or not, friction counts by severity.
