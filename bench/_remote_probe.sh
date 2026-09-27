#!/bin/bash
set +e
echo "=== uv python ==="
~/.local/bin/uv python list 2>/dev/null | head
echo "=== pip index peregrine ==="
python3 -m pip index versions peregrine-server 2>&1 | tail -5
echo "=== pgbench peregrine ==="
ls ~/pgbench/peregrine/python/peregrine 2>/dev/null | head
find ~/pgbench -name '_native*' -o -name 'peregrine' -type f 2>/dev/null | head
echo "=== docker images ==="
docker images --format '{{.Repository}}:{{.Tag}} {{.Size}}' 2>/dev/null | head
