#!/bin/bash
set -euo pipefail
KEY="$HOME/.ssh/garuda_bench"
HOST="grepjava@192.168.100.100"
SRC="/mnt/d/code/weft/"
rsync -az --delete -e "ssh -i $KEY -o BatchMode=yes" \
  --exclude .venv --exclude .venv-ft --exclude target \
  --exclude bench/results --exclude .pytest_cache --exclude .ruff_cache \
  --exclude __pycache__ --exclude '*.pyd' --exclude '*.pyc' --exclude .git \
  --exclude '*.so' \
  "$SRC" "$HOST:~/weft-parity/"
ssh -i "$KEY" -o BatchMode=yes "$HOST" bash ~/weft-parity/bench/_remote_setup.sh
