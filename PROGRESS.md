# Progress

## Current milestone
M2 is done. Next is M3: loop detection, step cap, breaker state and admin reset.

## Done
- M0 (2026-11-24): pyproject, FastAPI app with `GET /healthz` (pings Redis), config loader, example `agentmeter.yaml`, fake OpenAI `mock_upstream`, Dockerfiles, docker-compose (redis, mock_upstream, proxy), CI (ruff + pytest with a Redis service). `docker compose up` gives `/healthz` → `{"status":"ok","redis":true}`.
- M1a (2026-11-24): `POST /v1/chat/completions` non-streaming pass-through (`app/proxy.py`, `app/identity.py`). Identity headers are parsed, the run id is generated or echoed, and X-AM-* plus hop-by-hop headers are stripped. Upstream errors pass through as they are, a timeout returns 504 and a connection error returns 502.
- M1b (2026-11-24): SSE streaming pass-through. `stream_options.include_usage` is injected, the bytes are relayed unchanged, and the final `usage` is captured into a `StreamResult` (logged for now). The upstream is always closed, including when the client disconnects.
- **M1 ✅** The OpenAI Python SDK works unchanged against the proxy, with and without streaming. This is tested in-process and smoke-tested via compose.
- M2a (2026-11-24): `app/pricing.py` (`PriceTable`: integer micro-USD, ceiling rounding, exact-then-prefix lookup) and `app/estimate.py` (tiktoken input count + max output × n). `max_tokens` is injected when the client sets no limit. Unknown models fail closed with a 400 (`agentmeter_unknown_model`). The estimate and the actual cost are logged per call. A cut upstream stream now ends with an SSE `agentmeter_upstream_cut` error event, so the OpenAI SDK raises `APIError`. The Docker image ships the tiktoken encodings, verified with `--network none`.
- M0 follow-ups (2026-11-24): `/healthz` returns 503 without Redis, alert thresholds are integer basis points, and `.gitattributes` enforces LF.
- M2b (2026-11-24): atomic hierarchical budgets.
  - `app/lua/reserve.lua` and `settle.lua` run through `app/budget.py` (`BudgetClient`, EVALSHA via `register_script`).
  - The flow is wired in `app/chat.py`: estimate → reserve → forward → settle.
  - An exhausted level gives a 429 `agentmeter_breaker_open`/`budget`, and a Redis outage gives a 503 (fail closed).
  - A compose smoke test exhausted a 700 µUSD run after exactly 7 calls, ending at `spent=98` and `reserved=0`.
- M2c (2026-11-25): reclaiming reservations and the multi-replica proof.
  - Reservations are indexed in the `am:resv` ZSET (score = expiry in Unix ms from Redis `TIME`). `app/lua/sweep.lua` and `app/sweeper.py` release expired ones, and `app/lua/extend.lua` is the heartbeat that keeps a live stream's reservation.
  - `settle.lua` is exactly-once: a second settle is a no-op, and a settle after a sweep adds the cost only.
  - A client disconnect with no usage now settles the full estimate.
  - `tests/test_concurrency.py`: 2 proxy apps under their own uvicorn servers, one Redis, the live mock. 100 parallel calls against a run budget that fits 10 give exactly 10 × 200 and 90 × 429, with `spent == limit` and `reserved == 0`. It runs as its own CI step. With a deliberately non-atomic reserve swapped in, all 100 calls went through and the test failed.
  - Compose check by hand, with 2 proxy containers (`proxy` on 8080, plus a `proxy2` service extending it on 8081), Redis and the mock set to usage = estimate: 4 runs of 100 calls split evenly across the two, each gave `{200: 10, 429: 90}` and ended at `limit=620 spent=620 reserved=0` with an empty `am:resv`. Both containers served successful calls in every run (8/2, 5/5, 4/6, 6/4). `docker compose --scale proxy=2` with a host port range didn't work on Docker Desktop for Windows (both replicas tried to bind 8080), hence the second service.
- **M2 ✅** The concurrency test passes with 2 replicas, and the spent total matches the sum of mock usage exactly.

## Decisions
- **Money parsing:** YAML and env values are in USD and converted to integer micro-USD via `Decimal(str(v))`. A value that isn't a whole number of micro-USD is rejected, not rounded. Prices are stored as micro-USD per 1M tokens (`input_per_mtok`, `output_per_mtok`). Model fields drop the `_usd` suffix (`budgets.default_run`, `agents[x].daily`) and use the YAML key as an alias, so `_usd` never labels a micro-USD int.
- **Config precedence:** `AGENTMETER_*` env vars (nested with `__`) > YAML file > defaults. The YAML path comes from `AGENTMETER_CONFIG`, defaulting to `./agentmeter.yaml`. `${VAR}` and `${VAR:-default}` are interpolated in YAML strings. An unset var becomes `""`, and for `webhook_url` that becomes `None`.
- **Strict nested config:** nested sections use `extra="forbid"` to catch typos. The top level uses `extra="ignore"` because pydantic-settings would otherwise reject unrelated `AGENTMETER_*` env vars such as `AGENTMETER_CONFIG`.
- **`redis_url`:** added as a setting (default `redis://localhost:6379/0`). It isn't in PLAN §4.
- **`/healthz`:** HTTP 200 `{"status":"ok","redis":true}`, or 503 `{"status":"degraded","redis":false}` when a 1 s ping fails. Budgets can't be enforced without Redis.
- **Alert thresholds:** YAML still takes ratios (`thresholds: [0.5, 0.75, 0.9]`), stored exactly as integer basis points (`alerts.thresholds_bps == [5000, 7500, 9000]`).
- **Python:** 3.12 in Docker and CI (`requires-python >=3.12`). Local dev has also been run on 3.13.
- **Ports:** proxy on 8080, mock_upstream on host 8001 (8000 inside the container), redis on 6379.
- **Test Redis:** tests use database 15 (`AGENTMETER_TEST_REDIS_URL`) and `FLUSHDB` it before each Redis test. They skip if Redis isn't running, and fail instead when `AGENTMETER_REQUIRE_REDIS=1` (CI).
- **Request body:** non-streaming requests forward the original bytes unchanged unless `max_tokens` is injected. Streaming requests are always re-serialized because `stream_options` is rewritten. Other `stream_options` keys are kept, and a non-object `stream_options` gets a 400.
- **Identity values** (run id, agent, team) must match `[A-Za-z0-9._:-]{1,128}`, otherwise the request gets a 400. They'll become Redis key parts in M2.
- **`X-AM-Run-Id` is echoed on every response** after identity parsing, errors and streams included. It isn't echoed on the 400 for an invalid identity header.
- **Timeouts:** `upstream_timeout_seconds` (default 600 s) applies to every httpx phase.
  - A connect timeout or other transport error before the response gives 502.
  - A read timeout before the first byte gives 504.
  - A timeout or transport error mid-stream ends the stream and is logged, with `StreamResult.error` set.
- **Response headers:** upstream `content-encoding` and `content-length` are dropped, because httpx decodes the body. The stream relays the decoded bytes (`aiter_bytes`) as they are, so gzip is never re-encoded.
- **SSE relay (`SSERelay`):** it relays *whole SSE events* unchanged and holds back only the current partial event, capped at 1 MiB (a larger event is passed through raw and not parsed). Event alignment is what lets a cut stream end with a clean error event rather than half a `data:` line. `usage` and `[DONE]` count only from completed events.
- **Cut upstream stream:** on a transport error mid-stream, the partial event is dropped and `data: {"error":{"type":"agentmeter_upstream_cut","code":"upstream_cut"|"upstream_timeout",...}}` is sent, then the stream closes. The OpenAI SDK raises `APIError` on it. Since M2b, an upstream that ends *cleanly* without `[DONE]` is treated as cut too (`code: upstream_incomplete`).
- **Stream cleanup:** `UpstreamStreamingResponse.__call__` always runs `close()` in a shielded anyio cancel scope. That closes the relay generator and the upstream response, then calls the `on_stream_finish` hook once. It works both for ASGI ≥ 2.4 disconnects (`OSError`) and for older Starlette disconnect-listener cancellation.
- **`on_stream_finish` hook:** `create_app(on_stream_finish=...)` is now an *observer* that runs after settlement and receives `(CallContext, StreamResult)`. Settlement itself lives in `app/chat.py`.
- **Upstream error before streaming:** a non-2xx response before streaming starts is read in full (error bodies are small) and relayed with its own status and content type, as in M1a.
- **Mock upstream:**
  - `GET /_mock/last_request` records the last request.
  - `MOCK_DELAY_MS` (default 0) delays each answer, so concurrent calls overlap. Compose passes it through.
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
- **Budget keys:** see the `app/budget.py` docstring.
  - `am:b:run:{id}`, `am:b:agent:{agent}:{YYYYMMDD}` and `am:b:team:{team}:{YYYYMM}` (UTC) are hashes with `limit`, `spent` and `reserved`.
  - `am:r:{id}` holds `amount` and the level keys (renamed from `am:resv:{id}` in M2c).
  - `am:resv` is a ZSET of in-flight reservation ids, scored by expiry in Unix ms.
- **Levels:**
  - The run level always applies.
  - The agent and team levels apply only when the header is set *and* a budget is configured for that name.
  - The run limit is written with HSETNX, so it's fixed at the first call and a later header can't raise or lower it. Agent and team limits use HSET, so config changes apply right away.
- **Expiry:**
  - Run keys get `now + run_ttl_seconds` (default 7 days), refreshed on every reserve, so a busy run never expires mid-way.
  - Agent and team keys expire at period end + 2 days of grace.
  - A reservation's expiry is its `am:resv` score: Redis `TIME` + `reservation_ttl_seconds` (default 3600, must exceed `upstream_timeout_seconds`). The hash has no TTL of its own, so it can't vanish before the sweep that releases it.
  - Settle re-applies a level key's expiry if the key lost it.
- **Redis clock for reservation expiry:** reserve, extend and sweep read `TIME` inside the script, so replicas never disagree about when a reservation expired. Day and month key names still come from the replica's clock: replicas must be NTP-synced (README).
- **Sweeper:** every replica runs it from the lifespan, once at startup, then every `sweep_interval_seconds` (default 30). One `sweep.lua` call takes up to 100 expired ids. A tick keeps calling it while batches come back full, up to 50 batches. It logs the count at INFO when it released anything (DEBUG otherwise), and a Redis error is logged and retried on the next tick.
  - Sweep skips level keys that no longer exist, so it never re-creates an expired period key without a TTL.
  - Sweep reads the reservation and level keys from the index rather than from `KEYS`. That's fine on the single Redis node we support, not on Cluster.
- **Exactly-once:** settle replaces the hash with a `{settled: 1}` tombstone (it lives `reservation_ttl_seconds`) and removes the id from `am:resv`. Sweep deletes the hash. Both are atomic, so whichever runs first releases.
  - `BudgetClient.settle` returns `RELEASED`, `GONE` (swept earlier: the cost is added, nothing released) or `DUPLICATE` (already settled: nothing changes).
  - The tombstone goes beyond the spec. It also stops a repeated settle from counting `spent` twice, not only from releasing twice.
- **Heartbeat:** a live stream calls `extend.lua` every `reservation_heartbeat_seconds` (default 60, must be less than the TTL). Extend sets the expiry to now + TTL with `ZADD XX GT`, so it never shortens it or re-adds a gone id, and returns 0 once the reservation is gone. Then the heartbeat stops with a warning.
  - The heartbeat runs as a task alongside `UpstreamStreamingResponse.__call__` (the `keepalive` hook), and its shielded `close()` cancels it before settling.
  - Non-streaming calls don't send heartbeats. They're bounded by the TTL > upstream-timeout rule.
- **Lua money handling:** amounts are passed and written as decimal strings (the negation in settle is string-based), because Lua 5.1 would format large numbers as `1e+14`. Comparisons are exact up to 2^53 µUSD. A test covers a ~123M USD amount.
- **Settlement amounts:**
  - 2xx: the cost from `usage`. A 2xx without usable usage settles the estimate.
  - Upstream non-2xx, or a connection failure (including a connect timeout): 0.
  - Read timeout (504): the full estimate, because the upstream had the request and may have billed it. This goes beyond the spec.
  - Stream: `usage` if it arrived.
  - **Client disconnect without usage (M2c): the full estimate.** After the client leaves, the upstream may have generated tokens we never read, so a tiktoken count of what was relayed could under-bill. A disconnect after usage arrived still settles the usage.
  - Upstream cut or clean end without usage: estimated input + a tiktoken count of the streamed deltas (content, refusal, tool-call name/args). The count is taken per event, so nothing is buffered.
  - Cancellation or crash mid-call: the estimate, settled in a shielded scope.
  - Each reservation is settled exactly once (`_Settlement`).
- **429 body:** `{"error":{"type":"agentmeter_breaker_open","reason":"budget","level","run_id","spent_usd","reserved_usd","limit_usd","estimate_usd","message","code":"budget_exhausted"}}`. The `*_usd` fields are floats for display only. The response sets `x-should-retry: false` so the OpenAI SDKs don't retry.
- **Redis down:** reserve fails closed with a 503 `agentmeter_budget_unavailable`. A failed settle is logged, and the sweeper releases its reservation after the TTL. The call's cost is then never added to `spent`.
- **Concurrency test design:** the mock reports usage equal to the worst-case estimate, so a settle frees no headroom and "exactly 10 of 100" holds whatever the timing. With actual < estimate, an 11th call could rightly fit once others settle. The two replicas are separate uvicorn servers in threads, each with its own event loop, Redis pool and upstream client. CI runs it as a separate step (`pytest -m concurrency`). A local `pytest -q` runs it too.
- **Single Redis node:** a request's keys span hash slots, so Redis Cluster isn't supported as is.

## Open questions
- **o-series models:** they reject `max_tokens` and require `max_completion_tokens`. Should we inject `max_completion_tokens` for them, or for everyone (OpenAI's newer name, but less supported by OpenAI-compatible upstreams)?
- **Image tokens:** the image estimate is a flat constant. gpt-4o-mini bills images at roughly 33× the token count, so 1105 can under-count there. We need a per-model image formula before relying on budgets with vision.
- **Heartbeat lost for longer than the TTL:** if one replica can't reach Redis for over an hour while its stream keeps running, the sweeper (on another replica) releases that reservation mid-call. Other calls can then use that headroom, and the late settle only adds the cost. This is a bounded overspend, at most one estimate per affected stream. Should a stream whose heartbeat finds the reservation gone be cut instead?
- **Period boundaries:** day and month key names still come from each replica's clock (NTP assumed). Moving them to Redis `TIME` would mean computing the key names inside the script, which breaks the declared-`KEYS` contract further. Worth it?
- **Failed settle loses the cost:** when settle itself fails (Redis blip), the sweeper releases the reservation but the actual cost is never added to `spent`. Should the proxy retry settles from a local queue?
- **Cached prompt tokens:** settlement prices all `prompt_tokens` at the full input price. Cached-token discounts are ignored, so the cost is over-counted, which is safe. Is that OK for v1?
- **Bad identity header:** should the 400 also echo a (generated) run id?
- **Lockfile:** dependencies use lower bounds only. Should we add pins or `uv.lock` for reproducible builds?
- **Starlette deprecation warning:** `starlette.testclient` with `httpx` is deprecated in favour of `httpx2`. It's harmless for now but needs watching.

## Measured numbers
None yet (benchmarks start in M6).
