# F-093: `deploy dockerfile --out` fails on a missing folder or a folder

**Status:** fixed on fix/0.9-cli-findings (2026-11-10)
**Severity:** minor (the documented example fails with a traceback)
**Source:** review (full CLI test run before 0.9.0)
**Promise:** none

## What happens
The docs/cli.md example `ubunye deploy dockerfile container --pipelines pipelines
--out build/Dockerfile` raises `FileNotFoundError` when `build/` does not exist.
`--out build` (an existing folder) raises `IsADirectoryError`.

## Repro
```bash
ubunye deploy dockerfile container --out build/Dockerfile   # no build/ yet
mkdir build && ubunye deploy dockerfile container --out build
```

## Expected
The example works as written; a folder means "put the Dockerfile in it".

## Fix
`ubunye/cli/deploy.py`: `--out` that is a folder becomes `<folder>/Dockerfile`, and
the parent folder is made. The entry script still goes next to the Dockerfile.

## Evidence
`tests/unit/deploy/test_container_deploy.py::test_dockerfile_out_makes_its_missing_folders`
and `::test_dockerfile_out_as_a_folder_writes_a_dockerfile_in_it` failed before and
pass after.
