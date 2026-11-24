# Progress

## Current milestone
M1 is done (non-streaming and SSE streaming pass-through). Next is M2 (atomic budgets).

## Done
- M0 (2026-11-24): pyproject, FastAPI app with `GET /healthz` (pings Redis), config loader, example `agentmeter.yaml`, fake OpenAI `mock_upstream`, Dockerfiles, docker-compose (redis, mock_upstream, proxy), CI (ruff + pytest with a Redis service). `docker compose up` gives `/healthz` → `{"status":"ok","redis":true}`.
- M1a (2026-11-24): `POST /v1/chat/completions` non-streaming pass-through (`app/proxy.py`, `app/identity.py`). Identity headers are parsed, the run id is generated or echoed, and X-AM-* plus hop-by-hop headers are stripped. Upstream errors pass through as they are, a timeout returns 504 and a connection error returns 502.
- M1b (2026-11-24): SSE streaming pass-through. `stream_options.include_usage` is injected, the bytes are relayed unchanged, and the final `usage` is captured into a `StreamResult` (logged for now). The upstream is always closed, including when the client disconnects.
- **M1 ✅** The OpenAI Python SDK works unchanged against the proxy, with and without streaming. This is tested in-process and smoke-tested via compose.

## Decisions
- **Money parsing:** YAML and env values are in USD and converted to integer micro-USD via `Decimal(str(v))`. A value that isn't a whole number of micro-USD is rejected, not rounded. Prices are stored as micro-USD per 1M tokens (`input_per_mtok`, `output_per_mtok`). Model fields drop the `_usd` suffix (`budgets.default_run`, `agents[x].daily`) and use the YAML key as an alias, so `_usd` never labels a micro-USD int.
- **Config precedence:** `AGENTMETER_*` env vars (nested with `__`) > YAML file > defaults. The YAML path comes from `AGENTMETER_CONFIG`, defaulting to `./agentmeter.yaml`. `${VAR}` and `${VAR:-default}` are interpolated in YAML strings. An unset var becomes `""`, and for `webhook_url` that becomes `None`.
- **Strict nested config:** nested sections use `extra="forbid"` to catch typos. The top level uses `extra="ignore"` because pydantic-settings would otherwise reject unrelated `AGENTMETER_*` env vars such as `AGENTMETER_CONFIG`.
- **`redis_url`:** added as a setting (default `redis://localhost:6379/0`). It isn't in PLAN §4.
- **`/healthz`:** always HTTP 200. The `redis` field reports a ping with a 1 s timeout.
- **Python:** 3.12 in Docker and CI (`requires-python >=3.12`). Local dev has also been run on 3.13.
- **Ports:** proxy on 8080, mock_upstream on host 8001 (8000 inside the container), redis on 6379.
- **Test Redis:** the Redis-up health test skips if Redis isn't reachable, but CI sets `AGENTMETER_REQUIRE_REDIS=1` so it fails there instead.
- **Request body:** non-streaming requests forward the original bytes unchanged. Streaming requests are re-serialized because `stream_options` is rewritten. Other `stream_options` keys are kept, and a non-object `stream_options` gets a 400.
- **Identity values** (run id, agent, team) must match `[A-Za-z0-9._:-]{1,128}`, otherwise the request gets a 400. They'll become Redis key parts in M2.
- **`X-AM-Run-Id` is echoed on every response** after identity parsing, errors and streams included. It isn't echoed on the 400 for an invalid identity header.
- **Timeouts:** `upstream_timeout_seconds` (default 600 s) applies to every httpx phase.
  - A connect timeout or other transport error before the response gives 502.
  - A read timeout before the first byte gives 504.
  - A timeout or transport error mid-stream ends the stream and is logged, with `StreamResult.error` set.
- **Response headers:** upstream `content-encoding` and `content-length` are dropped, because httpx decodes the body. The stream relays the decoded bytes (`aiter_bytes`) as they are, so gzip is never re-encoded.
- **SSE usage capture:** an incremental line parser holds only the current partial line, capped at 1 MiB (longer lines are skipped), so the stream is never buffered. It keeps the last `usage` object and whether `[DONE]` was seen.
- **Stream cleanup:** `UpstreamStreamingResponse.__call__` always runs `close()` in a shielded anyio cancel scope. That closes the relay generator and the upstream response, then calls the `on_stream_finish` hook once. It works both for ASGI ≥ 2.4 disconnects (`OSError`) and for older Starlette disconnect-listener cancellation.
- **`on_stream_finish` hook:** `create_app(on_stream_finish=...)` receives `(Identity, StreamResult)`. It defaults to logging and is the place where M2 settles the budget.
- **Upstream error before streaming:** a non-2xx response before streaming starts is read in full (error bodies are small) and relayed with its own status and content type, as in M1a.
- **Mock upstream:**
  - `GET /_mock/last_request` records the last request.
  - `MOCK_FORCE_STATUS` returns an error with the given status.
  - `stream=true` streams SSE chunks, plus a usage chunk when `include_usage` is set.
  - `MOCK_STREAM_CUT=1` raises mid-stream so the server drops the connection. Compose passes it through.
- **Dev dependency:** added `openai` for SDK compatibility tests.

## Open questions
- **Cut upstream stream:** the client currently sees a cleanly ended but truncated stream, with no `[DONE]` and no usage. The SDK doesn't raise. Should the proxy abort the downstream connection or send an SSE error event instead, so clients know the stream was cut?
- **Bad identity header:** should the 400 also echo a (generated) run id?
- **Alert thresholds** are floats (ratios, not money). Should they become integer basis points before the M4 threshold math?
- **Lockfile:** dependencies use lower bounds only. Should we add pins or `uv.lock` for reproducible builds?
- **Starlette deprecation warning:** `starlette.testclient` with `httpx` is deprecated in favour of `httpx2`. It's harmless for now but needs watching.

## Measured numbers
None yet (benchmarks start in M6).
