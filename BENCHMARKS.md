<div align="center">
  <img width="420" src="assets/weft-spectral-continuum.svg" alt="weft">
</div>

# Benchmarks

Weft, Peregrine and Granian on the-benchmarker contract, 256 connections.

Measured 2026-09-27 on `192.168.100.100` (`sanctuary3`): Intel i5-8250U,
4 cores / 8 threads, 3.2 GB, Ubuntu 26.04. CPython 3.14.7. `zrk` 2.5.0.
Weft 0.1.0 (release). Peregrine 1.1.7. Granian 2.8.3.

Every run answered every request. No errors, timeouts, or non-2xx.

Peregrine was measured in an earlier session the same day; Weft and Granian
in a later one. Weft’s ASGI and WSGI medians moved by 2% between sessions;
the Weft column below is the later session.

## Requests per second

Median of three 15 s open-loop ramps. Bold is the highest in the row.

| app | Weft | Peregrine | Granian | Weft / Peregrine | Weft / Granian |
|---|---:|---:|---:|---:|---:|
| raw ASGI | 140,570 | **141,791** | 74,026 | 99% | 1.90× |
| raw WSGI | 151,752 | **200,227** | 87,733 | 76% | 1.73× |
| FastAPI 0.141.1 | 34,551 | **36,433** | 23,061 | 95% | 1.50× |

Latency, p50 / p99, milliseconds (zrk coordinated-omission queue time; the
ramp is faster than every server here):

| app | Weft | Peregrine | Granian |
|---|---|---|---|
| raw ASGI | 1,029 / 5,155 | 1,216 / 4,667 | 2,500 / 7,088 |
| raw WSGI | 948 / 6,207 | 851 / 3,540 | 1,758 / 6,195 |
| FastAPI | 3,796 / 9,736 | 3,761 / 9,253 | 4,499 / 10,408 |

Three runs, requests per second:

| app | Weft | Peregrine | Granian |
|---|---|---|---|
| raw ASGI | 141,997 · 140,570 · 120,547 | 138,643 · 141,791 · 142,277 | 79,280 · 74,026 · 73,422 |
| raw WSGI | 152,262 · 142,664 · 151,752 | 173,657 · 200,565 · 200,227 | 87,733 · 86,959 · 88,907 |
| FastAPI | 36,618 · 34,551 · 34,145 | 39,435 · 36,220 · 36,433 | 23,522 · 23,061 · 22,637 |

Raw ASGI: Weft and Peregrine are inside the spread; both about 1.9× Granian.
WSGI: Peregrine is ahead (asyncio-free poller). FastAPI: the framework
dominates; Weft and Peregrine stay close, both about 1.5× Granian.

## Method

The-benchmarker collect command
([web-frameworks](https://github.com/the-benchmarker/web-frameworks) `develop`
at 4bb9eaa):

```
warm-up    zrk -c 50 -d 5s --plain URL
per run    zrk --plain -c 256 -d 15s -m GET --format json -R1000:500000 \
               --interval 1s --timeout 8s --latency URL
```

Ramp 1,000→500,000 req/s over 15 s, keep-alive on. Figure is `achieved_rate`,
median of three runs. Load hits `GET /` on the suite’s three-route contract.
FastAPI 0.141.1 / Starlette 1.7.0 / Pydantic 2.13.5.

Four worker processes. Server on physical cores 0+1 (`0,4,1,5`), zrk on 2+3
(`2,6,3,7`). `127.0.0.1:3000`.

```
FRAMEWORKS="asgi wsgi fastapi" SERVERS="weft peregrine granian" CONNS=256 \
  WORKERS=4 RUNS=3 PIN=0,4,1,5:2,6,3,7 bash bench/vs_peregrine.sh
```

TSV: `bench/results/weft-vs-peregrine-256.tsv`,
`bench/results/weft-vs-granian-256.tsv`.

Not comparable with the site’s published dataset (this machine, 4 workers,
load generator on the same host). The three columns are comparable with
each other.
