# F-094: CLI docs and help text say things the CLI does not do

**Status:** fixed on fix/0.9-cli-findings (2026-11-10)
**Severity:** minor (a reader follows the docs and fails)
**Source:** review (full CLI test run before 0.9.0)
**Promise:** none

## What happens
- `docs/cli.md` says `python -m ubunye` runs the same CLI. `ubunye/__main__.py` is a
  separate argparse runner for spark-submit: `python -m ubunye version` exits 2.
- The `ubunye version` example shows v0.8.0.
- "package" is left in `docs/cli.md` where it means the middle level, now a pipeline
  (`-p/--pipeline`): the workflow file name, `validate --all`, `run --all`, the
  lineage path.
- The CLI surface list in the repo `CLAUDE.md` misses `doctor`, `gate`, `backends`,
  `mcp`, `runs`, `prove` and most `deploy`, `export` and `lineage` subcommands.
- `ubunye config --help` says "Show and validate config files"; it only validates.

## Expected
Docs and help match `ubunye --help` and the code.

## Fix
Docs only, plus the `config` help line. `__main__` behaviour is unchanged (it is the
spark-submit entry point; making it the Typer CLI would break that use).

## Evidence
`tests/unit/cli/test_validate.py::test_config_help_says_it_validates_not_shows` failed
before and passes after. The command list was taken from `ubunye --help` and each
group's `--help` on this branch.
