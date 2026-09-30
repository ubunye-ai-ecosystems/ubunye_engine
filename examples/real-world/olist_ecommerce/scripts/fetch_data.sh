#!/usr/bin/env bash
# Fetch the real Olist data (9 CSV files, about 120 MB unzipped) from Kaggle into data/.
#
# Needs a Kaggle account and API token: https://www.kaggle.com/settings -> "Create New
# Token", which saves kaggle.json (see https://github.com/Kaggle/kaggle-api). The data
# is Olist's, published on Kaggle under CC BY-NC-SA 4.0: fine to download and use for
# learning, not to commit to this repository or republish. data/ is in .gitignore.
#
#   pip install kaggle
#   bash scripts/fetch_data.sh
#   ubunye run -d pipelines -u olist -p sales -t clean --backend pandas --var data_dir=data
set -euo pipefail
here="$(cd "$(dirname "$0")/.." && pwd)"
kaggle datasets download -d olistbr/brazilian-ecommerce -p "$here/data" --unzip
ls -lh "$here/data"
