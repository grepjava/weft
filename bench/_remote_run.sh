#!/bin/bash
set -euo pipefail
cd "$HOME/weft-parity"
export PATH="$HOME/.local/bin:/usr/local/bin:$PATH"
export VENV="$HOME/weft-parity/.venv"
export ZRK=/usr/local/bin/zrk
# Physical cores 0+1 for the server, 2+3 for zrk (siblings n and n+4).
export PIN=0,4,1,5:2,6,3,7
export WORKERS=4
export CONNS=256
export RUNS=3
export DURATION=15s
export FRAMEWORKS="asgi wsgi"
export SERVERS="weft peregrine"
bash bench/vs_peregrine.sh | tee "$HOME/weft-vs-peregrine.tsv"
echo RUN_OK
