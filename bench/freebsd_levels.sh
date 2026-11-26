#!/bin/sh
# the-benchmarker style levels on FreeBSD (see BENCHMARKS.md): workers = nproc,
# load at 64/256/512 connections, routes GET /, GET /user/42, POST /user,
# 5 s warmup (50 conns) + 15 s measured per route. Interleaved rounds.
set -u
export PATH=$HOME/.cargo/bin:$PATH
ROUNDS=${ROUNDS:-2}; DUR=${DUR:-15}; W=$(sysctl -n hw.ncpu)
OUT=${OUT:-/tmp/levels.jsonl}; : > $OUT
WEFT=${WEFT_PY:-$HOME/weft-venv/bin/python}   # weft built into this venv
REF=${REF_BIN:-$HOME/ref-venv/bin}   # uvicorn[standard], granian, same fastapi
PORT=18850
NCPU=$(sysctl -n hw.ncpu)
for i in $(seq 0 $((NCPU - 1))); do eval "epp$i=\$(sysctl -n dev.hwpstate_intel.$i.epp)"; done
restore() {
  for i in $(seq 0 $((NCPU - 1))); do eval "v=\$epp$i"; sudo -n sysctl -q dev.hwpstate_intel.$i.epp=$v >/dev/null; done
  [ -n "${SRV:-}" ] && kill -TERM -- -$SRV 2>/dev/null
  true
}
trap restore EXIT INT TERM
for i in $(seq 0 $((NCPU - 1))); do sudo -n sysctl -q dev.hwpstate_intel.$i.epp=0 >/dev/null || { echo "cannot set epp"; exit 1; }; done
set -m  # background jobs get their own process group
cd "$(dirname "$0")/apps" || exit 1
command -v oha >/dev/null || { echo "no oha"; exit 1; }

up() {
  $WEFT - $PORT <<'EOF'
import sys, time, urllib.request
for _ in range(240):
    try:
        urllib.request.urlopen(f'http://127.0.0.1:{sys.argv[1]}/', timeout=1).read(); sys.exit(0)
    except Exception:
        time.sleep(0.25)
sys.exit(1)
EOF
}

start() {  # server app
  s=$1; a=$2
  case $s in
    weft) $WEFT -m weft $a:app --protocol asgi --host 127.0.0.1 --port $PORT --workers $W --log-level error >/tmp/srv.log 2>&1 & ;;
    uvicorn) $REF/python -m uvicorn $a:app --host 127.0.0.1 --port $PORT --workers $W --log-level critical --no-access-log >/tmp/srv.log 2>&1 & ;;
    granian) $REF/granian --log-level critical --interface asgi --host 127.0.0.1 --port $PORT --workers $W $a:app >/tmp/srv.log 2>&1 & ;;
  esac
  SRV=$!
  up && kill -0 $SRV 2>/dev/null && return 0
  echo "$s $a did not come up"; tail -5 /tmp/srv.log; return 1
}
stop() {
  kill -TERM -- -$SRV 2>/dev/null
  for i in $(seq 40); do kill -0 $SRV 2>/dev/null || break; sleep 0.25; done
  kill -KILL -- -$SRV 2>/dev/null; SRV=; sleep 2
}

COMBOS="weft:tb_fastapi uvicorn:tb_fastapi granian:tb_fastapi weft:tb_asgi uvicorn:tb_asgi granian:tb_asgi"
for r in $(seq $ROUNDS); do
  order=$COMBOS; [ $((r % 2)) = 0 ] && order=$(echo $COMBOS | tr ' ' '\n' | tail -r | tr '\n' ' ')
  for combo in $order; do
    s=${combo%%:*}; a=${combo#*:}
    start $s $a || { echo "{\"server\":\"$s\",\"app\":\"$a\",\"round\":$r,\"error\":\"start\"}" >> $OUT; continue; }
    for c in 64 256 512; do
      for route in "GET /" "GET /user/42" "POST /user"; do
        m=${route% *}; p=${route#* }
        oha --no-tui -m $m -c 50 -z 5s http://127.0.0.1:$PORT$p >/dev/null 2>&1
        oha --no-tui --output-format json -m $m -c $c -z ${DUR}s http://127.0.0.1:$PORT$p > /tmp/oha.json 2>/dev/null
        $WEFT - $s $a $r $c "$m" "$p" >> $OUT <<'EOF'
import json, sys
s, a, r, c, m, p = sys.argv[1:7]
d = json.load(open('/tmp/oha.json')); codes = d.get('statusCodeDistribution', {})
print(json.dumps({'server': s, 'app': a, 'round': int(r), 'conns': int(c), 'route': f'{m} {p}',
                  'rps': d['summary']['requestsPerSec'], 'p99': d['latencyPercentiles']['p99'] * 1000,
                  'ok': codes.get('200', 0), 'non200': sum(codes.values()) - codes.get('200', 0),
                  'errors': sum(d.get('errorDistribution', {}).values())}))
EOF
        tail -1 $OUT
      done
    done
    stop
  done
done
echo LEVELS DONE
