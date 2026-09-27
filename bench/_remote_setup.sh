#!/bin/bash
set -euo pipefail
ROOT=$HOME/weft-parity
cd "$ROOT"
export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"

if [ ! -x .venv/bin/python ]; then
  uv venv --python 3.14.7 .venv
fi
# maturin + peregrine + curl helper
uv pip install --python .venv/bin/python maturin 'peregrine-server>=1.1,<1.2'

echo "=== building weft (release, 2 jobs) ==="
export CARGO_BUILD_JOBS=2
.venv/bin/maturin develop --release

echo "=== versions ==="
.venv/bin/python -c 'import weft, peregrine; print("weft", weft.__version__); print("peregrine", peregrine.__version__)'
chmod +x bench/vs_peregrine.sh
echo SETUP_OK
