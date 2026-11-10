# F-111: `deploy dockerfile --out img/` writes a file named img

**Status:** fixed (0.9.1)
**Severity:** minor
**Source:** review (0.9.1 CLI pass)
**Promise:** `--out`: "A folder gets <folder>/Dockerfile; missing folders are made."

## What happens
`ubunye deploy dockerfile container --out img/` with no `img/` folder wrote the
Dockerfile to a file named `img` (and the entry script next to it). The option was a
`Path`, which drops the trailing slash, and only an existing folder counted as one.

## Repro
```bash
ubunye deploy dockerfile container --out img/ && ls -l img
```

## Fix
`--out` is read as text. A value ending in a path separator, or an existing folder,
is a folder: it is made and gets `Dockerfile` inside.

## Evidence
`tests/unit/deploy/test_container_deploy.py::test_dockerfile_out_ending_in_a_separator_is_a_folder_even_if_missing`
failed before (`img/Dockerfile` missing) and passes after.
