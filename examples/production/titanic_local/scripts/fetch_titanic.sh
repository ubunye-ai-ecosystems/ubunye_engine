#!/usr/bin/env bash
# Fetch the canonical Titanic CSV from a stable public mirror.
# Used by both the CI workflow and the README instructions so the URL is
# declared in one place.

set -euo pipefail

TITANIC_URL="${TITANIC_URL:-https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv}"

# Default destination is relative to this script so the fetch works from any CWD.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEST="${1:-${SCRIPT_DIR}/../data/titanic.csv}"

mkdir -p "$(dirname "$DEST")"

echo "Fetching Titanic CSV -> $DEST"
curl --fail --silent --show-error --location "$TITANIC_URL" -o "$DEST"

# Sanity check: header and row count. 891 data rows + 1 header row = 892.
lines=$(wc -l <"$DEST")
echo "Downloaded $lines lines."
if [[ "$lines" -ne 892 ]]; then
  echo "ERROR: expected 892 lines (891 rows + header), got $lines" >&2
  exit 1
fi
