#!/usr/bin/env bash
# Fetch the full WFP food price data (2015 onwards, about 465 MB) from Kaggle into data/.
#
# Needs a Kaggle account and API token: https://www.kaggle.com/settings -> "Create New
# Token", which saves kaggle.json (see https://github.com/Kaggle/kaggle-api). The data
# is WFP's, published under CC BY 3.0 IGO; the Kaggle dataset republishes it.
#
#   pip install kaggle
#   bash scripts/fetch_data.sh
#   ubunye run -d pipelines -u food -p prices -t clean --backend pandas --var data_dir=data
set -euo pipefail
here="$(cd "$(dirname "$0")/.." && pwd)"
kaggle datasets download -d abhishekgupta56447/global-food-prices-database-wfp \
  -p "$here/data" --unzip
ls -lh "$here/data"
