# Progress

## Current milestone
M1: Pass-through proxy. M1a (non-streaming) is done; next is M1b (SSE streaming).

## Done
- M0 (2026-11-24): pyproject, FastAPI app with `GET /healthz` (pings Redis), config loader, example `agentmeter.yaml`, fake OpenAI `mock_upstream`, Dockerfiles, docker-compose (redis, mock_upstream, proxy), CI (ruff + pytest with a Redis service). `docker compose up` gives `/healthz` → `{"status":"ok","redis":true}`.

- M1a (2026-11-24): `POST /v1/chat/completions` non-streaming pass-through (`app/proxy.py`, `app/identity.py`). Identity headers are parsed, the run id is generated or echoed, and X-AM-* plus hop-by-hop headers are stripped. Upstream errors pass through as they are, a timeout returns 504, a connection error returns 502, and `stream: true` returns 501. The OpenAI SDK works against the proxy, in-process and via compose.

## Decisions
- **Money parsing:** YAML and env values are in USD and converted to integer micro-USD via `Decimal(str(v))`. A value that isn't a whole number of micro-USD is rejected, not rounded. Prices are stored as micro-USD per 1M tokens (`input_per_mtok`, `output_per_mtok`). Model fields drop the `_usd` suffix (`budgets.default_run`, `agents[x].daily`) and use the YAML key as an alias, so `_usd` never labels a micro-USD int.
- **Config precedence:** `AGENTMETER_*` env vars (nested with `__`) > YAML file > defaults. The YAML path comes from `AGENTMETER_CONFIG`, defaulting to `./agentmeter.yaml`. `${VAR}` and `${VAR:-default}` are interpolated in YAML strings. An unset var becomes `""`, and for `webhook_url` that becomes `None`.
- **Strict nested config:** nested sections use `extra="forbid"` to catch typos. The top level uses `extra="ignore"` because pydantic-settings would otherwise reject unrelated `AGENTMETER_*` env vars such as `AGENTMETER_CONFIG`.
- **`redis_url`:** added as a setting (default `redis://localhost:6379/0`). It isn't in PLAN §4.
- **`/healthz`:** always HTTP 200. The `redis` field reports a ping with a 1 s timeout.
- **Python:** 3.12 in Docker and CI (`requires-python >=3.12`). Local dev has also been run on 3.13.
- **Ports:** proxy on 8080, mock_upstream on host 8001 (8000 inside the container), redis on 6379.
- **Test Redis:** the Redis-up health test skips if Redis isn't reachable, but CI sets `AGENTMETER_REQUIRE_REDIS=1` so it fails there instead.

- **Request body:** the original request bytes are forwarded unchanged. They're parsed only to check `stream`.
- **Identity values** (run id, agent, team) must match `[A-Za-z0-9._:-]{1,128}`, otherwise the request gets a 400. They'll become Redis key parts in M2.
- **`X-AM-Run-Id` is echoed on every response** after identity parsing, errors included. It isn't echoed on the 400 for an invalid identity header.
- **`upstream_timeout_seconds`:** new setting, default 600 s, applied to all httpx phases. Any httpx timeout, connect timeout included, maps to 504. Other transport errors map to 502.
- **Response headers:** upstream `content-encoding` and `content-length` are dropped, because httpx has already decoded the body.
- **Mock upstream:** now records the last request (`GET /_mock/last_request`) and supports `MOCK_FORCE_STATUS`.
- **Dev dependency:** added `openai` for SDK compatibility tests.

## Open questions
- Should the 400 for a bad identity header also echo a (generated) run id?
- Should a connect timeout give 502 instead of 504?
- Alert thresholds are floats (ratios, not money). Should they become integer basis points before the M4 threshold math?
- Dependencies use lower bounds only, with no lockfile. Should we add pins or `uv.lock` for reproducible builds?
- Starlette deprecation warning: `starlette.testclient` with `httpx` is deprecated in favour of `httpx2`. It's harmless for now but needs watching.

## Measured numbers
None yet (benchmarks start in M6).
