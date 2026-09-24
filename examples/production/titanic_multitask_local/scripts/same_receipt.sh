#!/usr/bin/env bash
# Check that a task's latest Spark run and latest pandas run left the same
# receipt: the same config hash and, for every output, the same data hash.
#
#   same_receipt.sh <usecase_dir> <usecase> <package> <task>
#
# Uses only the ubunye CLI: `lineage list --json` finds the two runs and
# `lineage compare --json` says whether each hash changed.
set -euo pipefail

where=(-d "$1" -u "$2" -p "$3" -t "$4")

latest_run() {
  ubunye lineage list "${where[@]}" --json | python -c "
import json, sys
runs = [r for r in json.load(sys.stdin) if r['backend'] == sys.argv[1]]
if not runs:
    sys.exit('no ' + sys.argv[1] + ' run recorded')
print(runs[0]['run_id'])  # newest first
" "$1"
}

spark_run=$(latest_run spark)
pandas_run=$(latest_run pandas)

ubunye lineage compare "${where[@]}" --run-id1 "$spark_run" --run-id2 "$pandas_run"
ubunye lineage compare "${where[@]}" --run-id1 "$spark_run" --run-id2 "$pandas_run" --json |
  python -c "
import json, sys
report = json.load(sys.stdin)
problems = []
if report['config_hash']['changed']:
    problems.append('the config hash differs')
for name, out in report['outputs'].items():
    if out['data_hash']['state'] != 'unchanged':
        problems.append(name + ': the data hash is ' + out['data_hash']['state'])
if problems:
    sys.exit('Spark and pandas disagree: ' + '; '.join(problems))
print('OK: Spark and pandas left the same receipt for', report['task'])
"
