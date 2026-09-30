# Experiments

Scripts behind the hardening experiments (`tasks/hardening/experiments/`). Not
collected by pytest; run by hand against an installed engine, for example
`python tests/experiments/e01_crash.py $(which ubunye) 40`.

E-06 (the scale ladder): `python tests/experiments/e06_scale.py --help`; it runs on a
free runner through `.github/workflows/scale-ladder.yml`.
