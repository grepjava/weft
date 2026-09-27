#!/bin/bash
set +e
echo "=== disk ==="
df -h ~ /tmp
echo "=== venv ==="
ls -l ~/fastapi-bench-venv/bin/python 2>/dev/null
if [ -x ~/fastapi-bench-venv/bin/python ]; then
  ~/fastapi-bench-venv/bin/python -c 'import peregrine,sys; print("venv peregrine", peregrine.__version__, sys.executable)'
fi
python3 -c 'import peregrine,sys; print("sys peregrine", peregrine.__version__)' 2>&1 | tail -3
echo "=== tools ==="
command -v uv
command -v maturin
command -v pip
ls ~/.local/bin 2>/dev/null
echo "=== weft tree ==="
test -f ~/weft/Cargo.toml && head -15 ~/weft/Cargo.toml
echo "=== pgbench ==="
ls ~/pgbench
ls -l ~/pgbuild/release/peregrine 2>/dev/null
echo "=== oha ==="
ls -l ~/.cargo/bin/oha /usr/local/bin/oha 2>/dev/null
echo "=== rust ==="
rustc --version
echo "=== python ==="
python3 --version
python3 -m pip --version
