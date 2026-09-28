# F-022: an editable install's run records carry the version it was installed at

**Status:** open
**Severity:** minor
**Source:** proving ground, C01 on the dev box (2026-09-28)
**Promise:** the run record is correct (ADR 006)

## What happens
The dev venv has the engine installed editable at 0.7.0; the source is now past 0.7.1.
Every run record and `ubunye prove report` says `engine_version: 0.7.0`.

## Root cause
`engine_version` comes from the installed package metadata
(`importlib.metadata.version("ubunye-engine")`), which an editable install writes once.

## Expected
A record says the version of the code that ran, or marks a development build as such
(for example `0.7.2.dev` plus the commit), so the proving ground can never compare two
builds believing they are one.

## Evidence
Tutorial 1 run on the dev box: both records `0.7.0` while `pyproject.toml` says 0.7.2.
