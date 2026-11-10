# F-092: `prove` options with fixed values print a traceback, or take anything

**Status:** fixed on fix/0.9-cli-findings (2026-11-10)
**Severity:** minor (a typo gives a traceback or a wrong observation)
**Source:** review (full CLI test run before 0.9.0)
**Promise:** 5 (nothing is lost silently)

## What happens
`ubunye prove skip --status nonsense` and `ubunye prove observe --cost-basis bogus`
print a raw `ValueError` traceback from `ubunye.proving.evidence`.
`ubunye prove observe --kind bogus` is accepted and written into the observation.

## Repro
```bash
ubunye prove skip --workload w --env e --reason r --status nonsense
ubunye prove observe --workload w --env e --record rec.json --kind bogus
```

## Expected
A usage error (exit 2) naming the option and listing its valid values, as the help
text lists them.

## Fix
`ubunye/cli/prove.py`: an option callback checks `--kind` (local, cloud, managed),
`--cost-basis` (the evidence module's `COST_BASES`, or empty for the default) and
`--status` (the evidence module's `STATUSES` less `executed`) and raises
`typer.BadParameter`. Nothing is written for a wrong value.

## Evidence
`tests/unit/cli/test_prove_choices.py`: three tests failed before (traceback, exit 1;
`--kind bogus` exit 0) and pass after.
