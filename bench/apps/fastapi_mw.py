"""`fastapi_app` behind the middleware stack a typical FastAPI deployment carries.

- CORSMiddleware (pure ASGI, wraps `send`)
- a `@app.middleware("http")` function, i.e. Starlette's BaseHTTPMiddleware:
  this one spawns an anyio task group per request, streams the response
  through memory channels and listens for `http.disconnect` on `receive`
  concurrently with the endpoint, so it is the pattern that stresses the
  server's receive/send/disconnect machinery the most.
"""

import time

from fastapi import Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi_app import app


@app.middleware('http')
async def timing_header(request: Request, call_next):
    t0 = time.perf_counter()
    response = await call_next(request)
    response.headers['x-process-time'] = f'{(time.perf_counter() - t0) * 1000:.3f}'
    return response


app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_methods=['*'],
    allow_headers=['*'],
)
