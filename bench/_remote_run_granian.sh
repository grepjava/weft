#!/bin/bash
set -euo pipefail
cd "$HOME/weft-parity"
export PATH="$HOME/.local/bin:/usr/local/bin:$PATH"
uv pip install --python .venv/bin/python 'granian'
.venv/bin/granian --version
export VENV="$HOME/weft-parity/.venv"
export ZRK=/usr/local/bin/zrk
export PIN=0,4,1,5:2,6,3,7
export WORKERS=4
export CONNS=256
export RUNS=3
export DURATION=15s
export FRAMEWORKS="asgi wsgi fastapi"
export SERVERS="weft granian"
bash bench/vs_peregrine.sh | tee "$HOME/weft-vs-granian.tsv"
echo GRANIAN_OK
