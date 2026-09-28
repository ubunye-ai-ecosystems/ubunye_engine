---
description: Verify, review and commit one fix to the integration branch
argument-hint: <finding id, e.g. F-012>
---

Ship the fix for $ARGUMENTS onto `hardening/real-world`:

1. Confirm the branch: `git branch --show-current` is `hardening/real-world` or a branch
   off it. Never `main`.
2. The finding's test fails without the fix and passes with it. Show both runs
   (`git stash push -- <fix files>`, run, `git stash pop`, run).
3. Behaviour that Spark also has: the live parity case passes.
4. Unit tier green: `python -m pytest tests/unit -q -p no:cacheprovider`.
5. Spawn the `skeptic` on the diff. Act on DO NOT SHIP; weigh SHIP WITH CHANGES.
6. Docs and `docs/changelog.md` `[Unreleased]` move in the same commit. No version bump.
7. Format with the repo's pinned hooks: `python -m pre_commit run --files <changed>`.
8. Commit `fix(<area>): <summary>` naming the finding, push, and set the finding's
   `Status:` to `fixed (<sha>)` with the before/after evidence.
