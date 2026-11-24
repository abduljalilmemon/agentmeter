# Progress

## Current milestone
M2: Atomic budgets. M2a (pricing and worst-case estimate) is done; next is M2b (Lua reserve/settle in Redis).

## Done
- M0 (2026-11-24): pyproject, FastAPI app with `GET /healthz` (pings Redis), config loader, example `agentmeter.yaml`, fake OpenAI `mock_upstream`, Dockerfiles, docker-compose (redis, mock_upstream, proxy), CI (ruff + pytest with a Redis service). `docker compose up` gives `/healthz` → `{"status":"ok","redis":true}`.
- M1a (2026-11-24): `POST /v1/chat/completions` non-streaming pass-through (`app/proxy.py`, `app/identity.py`). Identity headers are parsed, the run id is generated or echoed, and X-AM-* plus hop-by-hop headers are stripped. Upstream errors pass through as they are, a timeout returns 504 and a connection error returns 502.
- M1b (2026-11-24): SSE streaming pass-through. `stream_options.include_usage` is injected, the bytes are relayed unchanged, and the final `usage` is captured into a `StreamResult` (logged for now). The upstream is always closed, including when the client disconnects.
- **M1 ✅** The OpenAI Python SDK works unchanged against the proxy, with and without streaming. This is tested in-process and smoke-tested via compose.
- M2a (2026-11-24): `app/pricing.py` (`PriceTable`: integer micro-USD, ceiling rounding, exact-then-prefix lookup) and `app/estimate.py` (tiktoken input count + max output × n). `max_tokens` is injected when the client sets no limit. Unknown models fail closed with a 400 (`agentmeter_unknown_model`). The estimate and the actual cost are logged per call. A cut upstream stream now ends with an SSE `agentmeter_upstream_cut` error event, so the OpenAI SDK raises `APIError`. The Docker image ships the tiktoken encodings, verified with `--network none`.

## Decisions
- **Money parsing:** YAML and env values are in USD and converted to integer micro-USD via `Decimal(str(v))`. A value that isn't a whole number of micro-USD is rejected, not rounded. Prices are stored as micro-USD per 1M tokens (`input_per_mtok`, `output_per_mtok`). Model fields drop the `_usd` suffix (`budgets.default_run`, `agents[x].daily`) and use the YAML key as an alias, so `_usd` never labels a micro-USD int.
- **Config precedence:** `AGENTMETER_*` env vars (nested with `__`) > YAML file > defaults. The YAML path comes from `AGENTMETER_CONFIG`, defaulting to `./agentmeter.yaml`. `${VAR}` and `${VAR:-default}` are interpolated in YAML strings. An unset var becomes `""`, and for `webhook_url` that becomes `None`.
- **Strict nested config:** nested sections use `extra="forbid"` to catch typos. The top level uses `extra="ignore"` because pydantic-settings would otherwise reject unrelated `AGENTMETER_*` env vars such as `AGENTMETER_CONFIG`.
- **`redis_url`:** added as a setting (default `redis://localhost:6379/0`). It isn't in PLAN §4.
- **`/healthz`:** always HTTP 200. The `redis` field reports a ping with a 1 s timeout.
- **Python:** 3.12 in Docker and CI (`requires-python >=3.12`). Local dev has also been run on 3.13.
- **Ports:** proxy on 8080, mock_upstream on host 8001 (8000 inside the container), redis on 6379.
- **Test Redis:** the Redis-up health test skips if Redis isn't reachable, but CI sets `AGENTMETER_REQUIRE_REDIS=1` so it fails there instead.
- **Request body:** non-streaming requests forward the original bytes unchanged unless `max_tokens` is injected. Streaming requests are always re-serialized because `stream_options` is rewritten. Other `stream_options` keys are kept, and a non-object `stream_options` gets a 400.
- **Identity values** (run id, agent, team) must match `[A-Za-z0-9._:-]{1,128}`, otherwise the request gets a 400. They'll become Redis key parts in M2.
- **`X-AM-Run-Id` is echoed on every response** after identity parsing, errors and streams included. It isn't echoed on the 400 for an invalid identity header.
- **Timeouts:** `upstream_timeout_seconds` (default 600 s) applies to every httpx phase.
  - A connect timeout or other transport error before the response gives 502.
  - A read timeout before the first byte gives 504.
  - A timeout or transport error mid-stream ends the stream and is logged, with `StreamResult.error` set.
- **Response headers:** upstream `content-encoding` and `content-length` are dropped, because httpx decodes the body. The stream relays the decoded bytes (`aiter_bytes`) as they are, so gzip is never re-encoded.
- **SSE relay (`SSERelay`):** it relays *whole SSE events* unchanged and holds back only the current partial event, capped at 1 MiB (a larger event is passed through raw and not parsed). Event alignment is what lets a cut stream end with a clean error event rather than half a `data:` line. `usage` and `[DONE]` count only from completed events.
- **Cut upstream stream:** on a transport error mid-stream, the partial event is dropped and `data: {"error":{"type":"agentmeter_upstream_cut","code":"upstream_cut"|"upstream_timeout",...}}` is sent, then the stream closes. The OpenAI SDK raises `APIError` on it. An upstream that ends *cleanly* without `[DONE]` is not treated as cut.
- **Stream cleanup:** `UpstreamStreamingResponse.__call__` always runs `close()` in a shielded anyio cancel scope. That closes the relay generator and the upstream response, then calls the `on_stream_finish` hook once. It works both for ASGI ≥ 2.4 disconnects (`OSError`) and for older Starlette disconnect-listener cancellation.
- **`on_stream_finish` hook:** `create_app(on_stream_finish=...)` receives `(CallContext, StreamResult)`. `CallContext` carries the identity, the model, `estimate_micro` and `actual_micro(usage)`. It defaults to logging and is the place where M2 settles the budget.
- **Upstream error before streaming:** a non-2xx response before streaming starts is read in full (error bodies are small) and relayed with its own status and content type, as in M1a.
- **Mock upstream:**
  - `GET /_mock/last_request` records the last request.
  - `MOCK_FORCE_STATUS` returns an error with the given status.
  - `stream=true` streams SSE chunks, plus a usage chunk when `include_usage` is set.
  - `MOCK_STREAM_CUT=1` raises mid-stream so the server drops the connection. Compose passes it through.
- **Dev dependency:** added `openai` for SDK compatibility tests.
- **Pricing:** `cost_micro = ceil((in × in_price + out × out_price) / 1e6)` uses one ceiling over the exact total, so it never under-counts and is off by less than 1 µUSD.
- **Model lookup:** exact match first, then the longest configured prefix *followed by `-`*. So `gpt-4o-mini-2024-07-18` → `gpt-4o-mini`, but a `gpt-4` entry never prices `gpt-4o`. The limit of this rule: `gpt-4o-<anything>` falls back to `gpt-4o` if nothing longer matches.
- **Token counting:** the per-message constants come from OpenAI's cookbook (3 per message, +1 for `name`, +3 reply priming). They're verified by a test that reproduces the cookbook's API-confirmed 124 (gpt-4o/mini) and 129 (gpt-4).
  - Encoding: the model's own, falling back to `o200k_base` (e.g. Gemini).
  - `tool_calls` and the `tools` definitions are counted as compact JSON, with the cookbook's per-function and per-request constants (7 and 12) added for tools. That's more than the cookbook's own formula, so it rounds up.
  - Image parts count a fixed `image_part_tokens` (default 1105, TODO). Other non-text parts are counted as their JSON.
- **Output side:** `max_completion_tokens` > `max_tokens` > `default_max_tokens`, multiplied by `n`. `n` isn't in PLAN, but without it an `n=3` request could cost 3× the estimate.
- **Injection:** `max_tokens = default_max_tokens` is injected only when the client sends neither limit, so the upstream is capped at exactly what we estimated.
- **Fail closed:** no price for the model gives a 400 `agentmeter_unknown_model`, with nothing sent upstream. A missing `model`, or invalid `messages`/limits, gives a 400 `invalid_request_error`.
- **tiktoken offline:** the proxy image sets `TIKTOKEN_CACHE_DIR=/opt/tiktoken` and fetches `o200k_base` and `cl100k_base` at build time. CI caches the same directory.

## Open questions
- **o-series models:** they reject `max_tokens` and require `max_completion_tokens`. Should we inject `max_completion_tokens` for them, or for everyone (OpenAI's newer name, but less supported by OpenAI-compatible upstreams)?
- **Image tokens:** the image estimate is a flat constant. gpt-4o-mini bills images at roughly 33× the token count, so 1105 can under-count there. We need a per-model image formula before relying on budgets with vision.
- **Cached prompt tokens:** settlement prices all `prompt_tokens` at the full input price. Cached-token discounts are ignored, so the cost is over-counted, which is safe. Is that OK for v1?
- **Bad identity header:** should the 400 also echo a (generated) run id?
- **Alert thresholds** are floats (ratios, not money). Should they become integer basis points before the M4 threshold math?
- **Lockfile:** dependencies use lower bounds only. Should we add pins or `uv.lock` for reproducible builds?
- **Starlette deprecation warning:** `starlette.testclient` with `httpx` is deprecated in favour of `httpx2`. It's harmless for now but needs watching.

## Measured numbers
None yet (benchmarks start in M6).
