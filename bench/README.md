# Weft benchmarks

Reproducible commands; raw results land in `bench/results/raw/<label>/`
(one JSON per scenario and repeat plus `meta.json` with the full environment),
and `bench/report.py` renders Markdown summaries.

## Prerequisites (this repository, Windows layout)

| Component | Location |
|---|---|
| Weft, standard Python 3.14.7 | `.venv` (`maturin develop --release`) |
| Weft, free-threaded Python 3.14.7t | `.venv-ft` (`maturin develop --release`) |
| uvicorn with httptools, the comparison baseline | `.venv` (`uv pip install --group bench`) |
| Load generator | `oha` 1.16.0 (`cargo install oha`) |
| Optional PostgreSQL | `WEFT_BENCH_PG_DSN=postgresql://user:password@127.0.0.1/db` |

Apps: `bench/apps/fastapi_app.py` (FastAPI 0.141.1 / Starlette 1.6.0 /
pydantic 2.13.5) and `bench/apps/fastapi_mw.py` (the same app behind
BaseHTTPMiddleware + CORSMiddleware, `--app fastapi-mw`). Weft is tuned for
FastAPI, so every benchmark is FastAPI-based; the `fastapi` scenario set
covers the typical request shapes (path params, response models, list
responses of 5/20 KB, dependencies, sync endpoints, JSON bodies).

## Running

```
# one configuration, one scenario set
python bench/run.py --label weft-1w --server weft --workers 1 --scenarios core

# the-benchmarker routes (GET /, GET /user/:id, POST /user) at 256 connections
python bench/run.py --label weft-asgi-min-256 --server weft --app asgi-min --scenarios benchmarker --concurrency 256
# same, against Peregrine (needs `python -m peregrine` on WEFT_BENCH_PY_PEREGRINE)
python bench/run.py --label peregrine-asgi-min-256 --server peregrine --app asgi-min --scenarios benchmarker --concurrency 256

# the matrix (python bench/matrix.py --list shows every configuration)
python bench/matrix.py --set fastapi
python bench/matrix.py --set scaling
python bench/matrix.py --set python-modes
python bench/matrix.py --set baseline --scenarios fastapi
python bench/matrix.py --set baseline --app fastapi-mw --scenarios fastapi

# fixed arrival rate instead of closed loop
python bench/matrix.py --set baseline --scenarios hello --rate 5000

# summary
python bench/report.py --out bench/results/summary.md
```

`run.py` records: offered load (closed loop concurrency or fixed rate),
successful throughput (2xx per second), status distribution, oha error
distribution, p50/p95/p99 latency, server process-tree CPU seconds and
average cores, peak and end RSS, native thread count, and the exact server
command line. Warmup (3 s by default) and server startup are outside the
measured intervals.

Memory is recorded three ways, each summed over the process tree
(`server_mem_idle` / `server_mem_peak` / `server_mem_mean` /
`server_mem_end`, keys `rss`, `private`, `uss`):

* `rss`: working set; on Windows shared DLL and interpreter pages are counted
  once *per process*, so summed RSS over-states multi-process configurations;
* `private`: committed private bytes (no shared pages), the fair number for
  comparing worker processes with worker threads;
* `uss`: unique set size, resident pages private to the process.

`server_mem_idle` is sampled after startup and the 1 s settle, before any
request. `--retained-wait S` sleeps `S` seconds after each run and records
`server_mem_retained`: memory still held once the server is idle again.

## Caveats that apply to every number

* Load generator and server share one machine; `matrix.py` pins them to
  disjoint CPU sets (server CPUs 0-7, oha CPUs 12-19 by default).
* Closed-loop tests (`-c`) measure throughput at saturation; fixed-rate tests
  (`--rate`) measure latency under a given offered load with latency
  correction. A configuration that rejects more requests is not "faster";
  compare the 2xx count.
* Repeats are reported as mean ± standard deviation.
* Back-to-back runs with thousands of short-lived connections exhaust the
  client's ephemeral ports on Windows (error 10048); space such runs by
  2 minutes.
