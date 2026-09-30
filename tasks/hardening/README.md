# The hardening ledger

What real use shows about Ubunye, one proven item at a time. Work happens on the
`hardening/real-world` branch; one release PR goes to `main` at the end.

- `findings/F-NNN-slug.md`: one problem, with a repro and evidence. Filed by `/finding`.
- `experiments/E-NN-slug.md`: a question asked on purpose (does the engine have the
  failure other teams hit in production? does it scale?). An experiment's answer is a
  pass (evidence the engine is fine) or findings.
- `SCOREBOARD.md`: the numbers that say whether it is getting better.

## The promises every finding is checked against

1. **Same result anywhere:** the same task folder gives the same rows and content hash on
   Spark and on pandas, on a laptop and on a cluster.
2. **Replay is the recorded run,** call for call, for nothing.
3. **Nothing is written when a `fail` check breaks.**
4. **A run record never holds a secret.**
5. **Nothing is lost silently:** rows, runs, or errors.
6. **The config owns the task, never the platform.**
7. **The core stays small.**

## Finding template

```markdown
# F-NNN: <one line>

**Status:** open | fixed (<sha>) | wontfix (<why>) | not a bug (<evidence>)
**Severity:** blocker | major | minor
**Source:** stranger | fire-tester | parity-checker | scale-runner | experiment E-NN | review
**Promise:** <number from the list, or none>

## What happens
Exact output, short.

## Repro
Commands or a script another person can run.

## Expected
What should happen, and why (Spark does X; the docs say Y).

## Evidence
Before and after: the failing test, the parity table, the numbers.
```

## Experiment template

```markdown
# E-NN: <the question>

**Status:** planned | running | answered (<date>)
**Why:** where the question comes from (an AbsaOSS production failure, a user, a number).

## Method
What is run, on what data, on what compute, against what baseline.

## Pass means
The observable result that says the engine is fine.

## Result
What happened, with evidence; findings filed.
```
