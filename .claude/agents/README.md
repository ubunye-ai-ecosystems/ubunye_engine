# Subagents

Project-level Claude Code agents, loaded from `.claude/agents/`. They serve the
hardening programme (`tasks/hardening/`): find what breaks in real use, prove it, fix
it, and measure that it got better.

| Agent | Job | Model |
|---|---|---|
| `stranger` | A newcomer solves a real problem from the public docs only; returns a friction log | sonnet |
| `fire-tester` | Runs an example end to end on a real environment, files findings | opus |
| `parity-checker` | Establishes what live Spark does, checks pandas matches, pins a parity case | sonnet |
| `scale-runner` | Runs a size step next to a plain baseline, records overhead and cost | sonnet |
| `engine-fixer` | One finding, one failing test, one minimal fix, one commit | opus |
| `skeptic` | Tries to prove a fix or a claim wrong before it lands | opus |
| `example-author` | Builds an example that meets the programme's bar | opus |
| `task-curator` | Keeps the ledger truthful | sonnet |

Cheap models where the job is to follow a method; opus where judgment decides.

## The loop

```
stranger / fire-tester / parity-checker / scale-runner  ->  finding (tasks/hardening/findings/F-NNN)
    -> engine-fixer (test fails before, passes after)  ->  skeptic  ->  commit on hardening/real-world
    -> scoreboard updated  ->  rerun the stranger: did the friction drop?
```

## Hard invariants none of them break

- Work lands on `hardening/real-world`, never on `main`. One release PR at the end.
- No releases, no PyPI. The `pypip` environment needs a human.
- No skipping pre-commit hooks.
- One commit per fix. Docs and changelog move with code.
- Nothing public under the owner's name, and no spend beyond free tiers, without asking.
- Kaggle data is downloaded at run time, never committed.
