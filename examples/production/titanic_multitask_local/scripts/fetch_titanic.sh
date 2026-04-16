#!/usr/bin/env bash
# Fetch the canonical Titanic CSV from a stable public mirror.
set -euo pipefail

TITANIC_URL="${TITANIC_URL:-https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEST="${1:-${SCRIPT_DIR}/../data/titanic.csv}"

mkdir -p "$(dirname "$DEST")"

echo "Fetching Titanic CSV -> $DEST"
curl --fail --silent --show-error --location "$TITANIC_URL" -o "$DEST"

lines=$(wc -l <"$DEST")
echo "Downloaded $lines lines."
if [[ "$lines" -ne 892 ]]; then
  echo "ERROR: expected 892 lines (891 rows + header), got $lines" >&2
  exit 1
fi
