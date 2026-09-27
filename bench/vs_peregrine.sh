#!/usr/bin/env bash
# Weft vs Peregrine on the-benchmarker contract, zrk open-loop ramp.
# Intended for 192.168.100.100: 4 physical cores, pin server vs load.
#
#   FRAMEWORKS="asgi wsgi" SERVERS="weft peregrine" CONNS=256 \
#     bash bench/vs_peregrine.sh
set -u

ROOT=$(cd "$(dirname "$0")/.." && pwd)
VENV=${VENV:-$ROOT/.venv}
ZRK=${ZRK:-zrk}
PORT=${PORT:-3000}
WORKERS=${WORKERS:-4}
CONNS=${CONNS:-256}
RUNS=${RUNS:-3}
DURATION=${DURATION:-15s}
RATE=${RATE:-1000:500000}
FRAMEWORKS=${FRAMEWORKS:-"asgi wsgi"}
SERVERS=${SERVERS:-"weft peregrine"}
APPDIR=${APPDIR:-$ROOT/bench/apps}
PIN=${PIN:-0,4,1,5:2,6,3,7}
URL="http://127.0.0.1:$PORT/"
OUT=$(mktemp -d)

PIN_SERVER=()
PIN_LOAD=()
if [ -n "$PIN" ]; then
    PIN_SERVER=(taskset -c "${PIN%%:*}")
    PIN_LOAD=(taskset -c "${PIN#*:}")
fi

PY="$VENV/bin/python"
if [ ! -x "$PY" ]; then
    echo "missing $PY" >&2
    exit 1
fi

cleanup() {
    if [ -n "${SERVER_PID:-}" ]; then
        kill -- -"$SERVER_PID" 2>/dev/null || kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

start() {
    local server=$1 framework=$2 app interface
    case "$framework" in
    asgi) app=tb_asgi:app; interface=asgi ;;
    wsgi) app=tb_wsgi:application; interface=wsgi ;;
    fastapi) app=tb_fastapi:app; interface=asgi ;;
    *) echo "unknown framework $framework" >&2; return 1 ;;
    esac
    export PYTHONPATH="$APPDIR"
    local log="$OUT/$server-$framework.log"
    set +e
    case "$server" in
    weft)
        setsid "${PIN_SERVER[@]}" "$PY" -m weft "$app" --host 127.0.0.1 --port "$PORT" \
            --workers "$WORKERS" --protocol "$interface" --log-level error \
            --app-dir "$APPDIR" --no-lifespan >"$log" 2>&1 &
        ;;
    peregrine)
        setsid "${PIN_SERVER[@]}" "$PY" -m peregrine --log-level error --protocol "$interface" \
            --host 127.0.0.1 --port "$PORT" --workers "$WORKERS" \
            --venv "$VENV" --python-path "$APPDIR" "$app" >"$log" 2>&1 &
        ;;
    granian)
        # Same flags as the-benchmarker's granian engine / peregrine frameworks.sh.
        setsid "${PIN_SERVER[@]}" "$VENV/bin/granian" --log-level critical --interface "$interface" \
            --host 127.0.0.1 --port "$PORT" --workers "$WORKERS" "$app" >"$log" 2>&1 &
        ;;
    *) echo "unknown server $server" >&2; return 1 ;;
    esac
    SERVER_PID=$!
    set -e
    local i
    for i in $(seq 1 80); do
        curl -s -o /dev/null --max-time 1 "$URL" && return 0
        if ! kill -0 "$SERVER_PID" 2>/dev/null; then
            echo "server exited"
            tail -20 "$log"
            return 1
        fi
        sleep 0.25
    done
    echo "server did not become ready"
    tail -20 "$log"
    return 1
}

stop() {
    if [ -n "${SERVER_PID:-}" ]; then
        kill -- -"$SERVER_PID" 2>/dev/null || kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
        SERVER_PID=
    fi
    sleep 0.5
}

one_run() {
    local json="$OUT/run.json"
    rm -f "$json"
    "${PIN_LOAD[@]}" "$ZRK" --plain -c "$1" -d "$DURATION" -m GET --format json --output "$json" \
        -R"$RATE" --interval 1s --timeout 8s --latency "$URL" >/dev/null 2>&1
    python3 - "$json" <<'PY'
import json, sys
try:
    d = json.load(open(sys.argv[1]))
except Exception:
    print("0 0 0 0 0 -1")
    raise SystemExit
lat = d.get("latency_us") or {}
errors = sum(int(v or 0) for v in (d.get("errors") or {}).values())
ms = lambda key: (lat.get(key) or 0) / 1000.0
print("%.0f %.3f %.3f %.3f %.3f %d" % (d.get("achieved_rate") or 0, ms("p50"), ms("p75"),
                                       ms("p90"), ms("p99"), errors))
PY
}

printf 'framework\tserver\tworkers\tconnections\treq/s p50_ms p75_ms p90_ms p99_ms errors\truns\n'
for framework in $FRAMEWORKS; do
    for server in $SERVERS; do
        stop
        if ! start "$server" "$framework"; then
            printf '%s\t%s\t%s\tFAILED TO START\n' "$framework" "$server" "$WORKERS"
            continue
        fi
        "${PIN_LOAD[@]}" "$ZRK" -c 50 -d 5s --plain "$URL" >/dev/null 2>&1
        runs=""
        for _ in $(seq 1 "$RUNS"); do
            runs="$runs$(one_run "$CONNS")"$'\n'
        done
        figure=$(printf '%s' "$runs" | grep -v '^$' | sort -n -k1,1 \
            | sed -n "$(( (RUNS + 1) / 2 ))p")
        all=$(printf '%s' "$runs" | grep -v '^$' | awk '{printf "%s ", $1}')
        printf '%s\t%s\t%s\t%s\t%s\t[%s]\n' "$framework" "$server" "$WORKERS" "$CONNS" \
            "$figure" "$all"
    done
done
stop
rm -rf "$OUT"
