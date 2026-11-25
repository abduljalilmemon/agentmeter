# AgentMeter

A budget circuit breaker for AI agents. AgentMeter is a drop-in, OpenAI-compatible proxy. It gives every agent run its own budget and refuses the call that would overspend it, before that call reaches the model.

> Status: early development. Budgets, loop detection, the circuit breaker and webhook alerts work today (M4). Metrics, the run report and the demo are planned. See [Status](#status).

## Why

Agents call LLMs in loops. One bug can burn money all night: a failing tool, a retry with no backoff, or a planner that never decides it's done. Provider consoles and API keys only offer coarse monthly budgets, so nothing stops *this one run* when it goes wrong.

Gateways such as LiteLLM and Portkey already budget per key, user or team. AgentMeter has a narrower aim:

1. **Budgets per run,** nested inside a daily agent budget and a monthly team budget.
2. **Correct under concurrency:** parallel calls, across several proxy replicas, can't overspend. Each call reserves its worst-case cost atomically in Redis before it is forwarded, then settles with the real usage.
3. **Loop detection and alerts:** trip the breaker when an agent repeats itself, and warn once at 50%, 75% and 90% of each budget.

It is not a full gateway. It has no routing, caching or key vault. You can put LiteLLM *behind* AgentMeter if you need those.

## Quickstart

```bash
docker compose up -d --build     # redis, mock_upstream (fake OpenAI), proxy on :8080
curl localhost:8080/healthz      # {"status":"ok","redis":true}
```

Point any OpenAI client at the proxy. That is the one-line change:

```python
from openai import OpenAI

# The API key still comes from OPENAI_API_KEY as usual (any value works against the mock).
client = OpenAI(base_url="http://localhost:8080/v1")  # was: the default api.openai.com

resp = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[{"role": "user", "content": "hi"}],
    extra_headers={"X-AM-Run-Id": "ticket-4711", "X-AM-Run-Budget": "0.25"},
)
```

In compose the proxy talks to `mock_upstream`, which returns canned answers and costs nothing. To use a real provider, set `AGENTMETER_UPSTREAM` (for example `https://api.openai.com/v1`) on the `proxy` service. Your `Authorization` header is passed through unchanged; AgentMeter stores no API keys.

## Headers

| Request header | Meaning |
|---|---|
| `X-AM-Run-Id` | Groups calls into one run. If missing, the proxy generates one (uuid4 hex). |
| `X-AM-Agent` | The agent name. It enforces the agent's daily budget when one is configured. |
| `X-AM-Team` | The team name. It enforces the team's monthly budget when one is configured. |
| `X-AM-Run-Budget` | The run limit in USD, e.g. `0.25`. Defaults to `budgets.default_run_usd`. The first call of a run fixes the limit, and later values are ignored. |
| `X-AM-Loop-Exempt` | `true` or `false` (default). `true` skips loop detection for this call, both the request fingerprint and the response tool-call streak, for legitimate polling. The step cap still applies. |

Ids and names must match `[A-Za-z0-9._:-]{1,128}`. All `X-AM-*` headers are removed before forwarding. Every response carries `X-AM-Run-Id`, except the 400 for an invalid `X-AM-*` header.

## How a run is stopped

Before any money is reserved, one atomic Lua script ([app/lua/gate.lua](app/lua/gate.lua)) checks the run's circuit breaker:

1. **Breaker OPEN:** the call is refused right away.
2. **Step cap:** every call that gets past the breaker counts as a step. Call `step_cap + 1` opens the breaker with `reason=step_cap`.
3. **Repeated request:** the last two messages are fingerprinted (sha256 of canonical JSON, with tool-call ids dropped and tool arguments re-serialized, so an agent re-sending the same tool call and the same failing result matches every turn). The `repeat_threshold`-th identical fingerprint in a run opens the breaker with `reason=loop`. Requests with fewer than two messages have no fingerprint.

The call that opens the breaker is refused too, and a refused call reserves nothing and never reaches the upstream.

After a successful response, the response side is checked as well. If the first tool call in the response (name plus canonical arguments) is the same as in the run's previous `consecutive_tool_calls - 1` responses, the breaker opens with `reason=loop` for the next call. A response without a tool call breaks the streak, and a cut stream doesn't count.

An OPEN breaker stays open until an admin resets it (see [Admin](#admin)) or the run's keys expire with its budget.

## How a call is budgeted

1. **Estimate the worst case:**
   - Input: prompt tokens, counted with tiktoken.
   - Output: `max_completion_tokens`, else `max_tokens`, else `default_max_tokens`, multiplied by `n`.
   - If the client sets no limit, the proxy injects `max_tokens = default_max_tokens`, so the upstream can't exceed the estimate.
2. **Reserve:** one Lua script checks `spent + reserved + estimate <= limit` at every level (run, agent, team) and reserves only if all of them pass.
3. **Forward** to the upstream. Streaming is supported, with `stream_options.include_usage` injected.
4. **Settle:** release the reservation and add the actual cost from `usage`:
   - **Upstream error or connection failure:** settles 0.
   - **Read timeout:** settles the full estimate, because the provider may have billed it.
   - **Cut stream:** settles the estimated input plus a tiktoken count of the streamed output.
   - **Client disconnects mid-stream before usage arrives:** settles the full estimate, because the upstream may have kept generating tokens the proxy never saw.
5. **Reclaim:** a sweeper on every replica releases reservations whose calls never settled (a crash, a killed replica). A live stream sends a heartbeat that keeps its reservation from being swept, however long it runs.

Money is stored as integer micro-USD everywhere, and costs round up, never down.

## Errors

Errors use OpenAI's error shape, so SDKs raise their usual exception types.

**429 Budget exhausted.** No request was sent upstream. The response sets `x-should-retry: false`, so the OpenAI SDKs don't retry it.

```json
{"error": {
  "type": "agentmeter_breaker_open", "reason": "budget", "level": "run",
  "run_id": "ticket-4711", "spent_usd": 0.000098, "reserved_usd": 0.0,
  "limit_usd": 0.0007, "estimate_usd": 0.000616,
  "message": "run budget exhausted: ...", "param": null, "code": "budget_exhausted"}}
```

`level` is `run`, `agent` or `team`. The `*_usd` values are floats for display only.

**429 Breaker open** (loop or step cap). No request was sent upstream, and `x-should-retry: false` is set:

```json
{"error": {
  "type": "agentmeter_breaker_open", "reason": "loop", "run_id": "ticket-4711",
  "spent_usd": 0.000042, "steps": 4,
  "message": "Circuit breaker open for run ticket-4711: ...", "param": null, "code": "loop_detected"}}
```

`reason` is `loop` (code `loop_detected`) or `step_cap` (code `step_cap_reached`). `spent_usd` is formatted as in the budget 429.

**400 Bad request:**

```json
{"error": {"type": "agentmeter_unknown_model", "code": "unknown_model", "message": "No price configured for model 'x'", "param": null}}
{"error": {"type": "invalid_request_error", "code": "invalid_header", "message": "X-AM-Run-Budget: ...", "param": null}}
```

An unknown model fails closed: a call that can't be priced can't be budgeted. Invalid JSON, a missing `model` or a malformed `max_tokens` also return `invalid_request_error`.

**502 / 504 Upstream failure:**

```json
{"error": {"type": "upstream_error", "code": "upstream_error", "message": "Upstream unreachable: ConnectError", "param": null}}
{"error": {"type": "upstream_error", "code": "upstream_connect_timeout", "message": "Upstream connect timed out", "param": null}}
{"error": {"type": "upstream_error", "code": "upstream_timeout", "message": "Upstream timed out", "param": null}}
```

The first two are 502 (the upstream was never reached). The last is 504 (no response before `upstream_timeout_seconds`). When the upstream itself answers 4xx or 5xx, its status and body are passed through unchanged.

**503 Budget store unavailable:** Redis is down, so no budget can be enforced and the call is refused (`type: agentmeter_budget_unavailable`). `/healthz` returns 503 at the same time.

**Cut streams:** if the upstream drops a stream, or ends it without `data: [DONE]`, the proxy finishes the stream with this event, and the OpenAI SDK raises `APIError`:

```
data: {"error": {"type": "agentmeter_upstream_cut", "code": "upstream_cut", "message": "...", "param": null}}
```

`code` is `upstream_cut`, `upstream_timeout` or `upstream_incomplete`. A stream whose budget reservation was lost ends the same way with `type: agentmeter_reservation_lost` (see the limitations).

## Events and alerts

Every state change worth knowing about is written to one Redis Stream, `am:events`, from inside the Lua script that makes the change. So an event exists exactly when the change happened, and a duplicate settle writes nothing.

| `type` | Written by | Fields |
|---|---|---|
| `call_settled` | `settle.lua`, once per call | `run`, `agent`, `team`, `model`, `prompt_tokens`, `completion_tokens`, `cost_micro`, `estimate_micro`, `how` (`usage`, `tiktoken`, `estimate:*`, ...), `reservation`, `reservation_state` (`released` or `gone`) |
| `threshold_crossed` | `settle.lua` | `level` (`run`, `agent`, `team`), `key`, `threshold_bp`, `spent`, `limit`, and the `run`, `agent`, `team` of the call that crossed it |
| `breaker_open` | `gate.lua`, `toolcall.lua` | `run`, `agent`, `team`, `reason`, `source`, `steps`, `spent` |

All values are strings: money in integer micro-USD, thresholds in integer basis points. The stream is capped with `XADD MAXLEN ~ events.stream_maxlen`.

**Thresholds fire once.** After a settle adds its cost, the script checks every level the call touched against every threshold: `spent * 10000 >= limit * threshold_bp`, measured on `spent`, not `spent + reserved`. The check uses integers only and stays exact for any limit below 2^53 µUSD. A crossing is written only if `SET NX` on its flag (`am:alert:run:{id}:5000`, `am:alert:agent:{name}:{day}:7500`, ...) succeeds, so each (level, threshold) fires exactly once, however many replicas settle at the same moment. A flag expires with its budget key: the run TTL, or the end of the agent's day or team's month plus grace. A single call that jumps from 40% to 95% fires 50, 75 and 90 together.

**The alert worker** ([worker/alerts.py](worker/alerts.py), the `worker` service in compose, or `python -m worker.alerts`) reads the stream in the consumer group `am:alerts`:

- `threshold_crossed` and `breaker_open` are posted as Slack-compatible JSON, `{"text": "..."}`, to `alerts.webhook_url`. Other events are acknowledged without a post.
- An entry is acknowledged (`XACK`) only after the webhook answers 2xx. A failed post stays pending and is retried once it has been idle for `alerts.retry_idle_seconds`, by this worker or any other (`XAUTOCLAIM`). Delivery is at least once.
- Without a `webhook_url`, alerts are logged at WARNING and acknowledged.
- An alert delivered more than `alerts.max_attempts` times (default 10) is logged at ERROR, copied to the `am:alerts:dead` stream (with `event_id` and `attempts`) and acknowledged, so a poison alert stops being retried.
- Each post ends with `(event <stream id>)`, so a repeated delivery can be recognized as the same alert.
- The group is created on first start from the beginning of the stream, so alerts written before the worker ever ran are still delivered.

Example post:

```
:warning: AgentMeter: run budget `run:ticket-4711` crossed 75%: spent $0.000056 of $0.00007 (last call: run `ticket-4711`) (event 1795649434478-0)
```

## Admin

```bash
curl -X POST localhost:8080/admin/runs/ticket-4711/reset -H "Authorization: Bearer $AGENTMETER_ADMIN_TOKEN"
# {"run_id": "ticket-4711", "reset": true, "breaker_was": {"reason": "loop", ...}, "steps_was": 4}
```

The reset closes the run's breaker and clears its step count, request fingerprints and tool-call streak. It never touches money: `spent`, `reserved` and the limit stay as they are, so a run whose budget is exhausted is still refused with `reason=budget`.

The endpoint is enabled only when `admin_token` is set (for example with `AGENTMETER_ADMIN_TOKEN`). Without one it returns 404. A missing or wrong bearer token gives 401.

## Configuration

AgentMeter reads `agentmeter.yaml` (path from `AGENTMETER_CONFIG`). Money is written in USD and stored as integer micro-USD; amounts finer than 1 µUSD are rejected.

```yaml
upstream: https://api.openai.com/v1
default_max_tokens: 1024
pricing:            # USD per 1M tokens
  gpt-4o-mini: {input: 0.15, output: 0.60}
  gemini-2.5-flash: {input: 0.30, output: 2.50}
budgets:
  default_run_usd: 0.50
  agents:
    support-bot: {daily_usd: 20}
  teams:
    platform: {monthly_usd: 500}
loop:
  repeat_threshold: 3
  step_cap: 50
  consecutive_tool_calls: 3
  agents:            # per-agent overrides (X-AM-Agent), e.g. for a legitimate poller
    status-poller: {repeat_threshold: 20, step_cap: 500}
alerts:
  thresholds: [0.5, 0.75, 0.9]
  webhook_url: ${ALERT_WEBHOOK}
  retry_idle_seconds: 30
events:
  stream_maxlen: 100000
```

> **Check the prices.** The prices above are placeholders. Compare them with your provider's current price page before you rely on any budget. Prices change, and the table is the only source AgentMeter uses.

| Key | Default | Meaning |
|---|---|---|
| `upstream` | `https://api.openai.com/v1` | The OpenAI-compatible base URL to forward to. |
| `redis_url` | `redis://localhost:6379/0` | The Redis that holds budgets (single node, see limitations). |
| `default_max_tokens` | `1024` | Injected when a request sets no output limit; also the estimate's output side. |
| `pricing.<model>` | none | `{input, output}` in USD per 1M tokens. Lookup is exact first, then the longest configured prefix followed by `-` (so `gpt-4o-mini-2024-07-18` uses `gpt-4o-mini`). |
| `budgets.default_run_usd` | `0.50` | The run limit when there is no `X-AM-Run-Budget` header. |
| `budgets.agents.<name>.daily_usd` | none | The agent's limit per UTC day. |
| `budgets.teams.<name>.monthly_usd` | none | The team's limit per UTC month. |
| `image_part_tokens` | `1105` | Flat token count per image in the estimate. |
| `upstream_timeout_seconds` | `600` | httpx timeout for upstream calls. |
| `run_ttl_seconds` | `604800` (7 days) | A run's budget is forgotten this long after its last call. |
| `reservation_ttl_seconds` | `3600` | The sweeper releases an unsettled reservation this long after its last reserve or heartbeat. It must exceed `upstream_timeout_seconds`. |
| `reservation_heartbeat_seconds` | `60` | How often a live stream extends its reservation. It must be less than `reservation_ttl_seconds`. |
| `sweep_interval_seconds` | `30` | How often each replica sweeps expired reservations (up to 100 per batch). |
| `loop.repeat_threshold` | `3` | The same request fingerprint this many times in a run opens the breaker (`loop`). At least 2. |
| `loop.step_cap` | `50` | Calls allowed per run; call `step_cap + 1` opens the breaker (`step_cap`). |
| `loop.consecutive_tool_calls` | `3` | The same tool call in this many responses in a row opens the breaker (`loop`) for the next call. At least 2. |
| `loop.agents.<name>` | none | Overrides of the three `loop` values above for one agent. |
| `admin_token` | unset | Bearer token for `POST /admin/runs/{id}/reset`. Unset or empty disables the endpoint (404). Prefer setting it with `AGENTMETER_ADMIN_TOKEN`. |
| `alerts.thresholds` | `[0.5, 0.75, 0.9]` | Alert thresholds as ratios, stored as integer basis points. Strictly increasing, finer than 1 bp is rejected. |
| `alerts.webhook_url` | unset | Slack-compatible incoming webhook for the worker. Unset or empty: alerts are only logged. |
| `alerts.retry_idle_seconds` | `30` | A failed webhook post is retried once it has been pending this long. |
| `alerts.webhook_timeout_seconds` | `10` | httpx timeout for one webhook post. |
| `alerts.max_attempts` | `10` | Deliveries after which a failing alert is dead-lettered to `am:alerts:dead` and acknowledged. |
| `events.stream_maxlen` | `100000` | Approximate length cap of `am:events` (`XADD MAXLEN ~`). |

`${VAR}` and `${VAR:-default}` placeholders in the YAML are filled from the environment. Every key can also be overridden with an `AGENTMETER_` variable, using `__` for nesting. For example: `AGENTMETER_UPSTREAM`, `AGENTMETER_REDIS_URL`, `AGENTMETER_BUDGETS__DEFAULT_RUN_USD=1.25`.

## Architecture

```
Agent (OpenAI SDK / LangGraph)
   │  headers: X-AM-Run-Id, X-AM-Agent, X-AM-Team, X-AM-Run-Budget
   ▼
┌──────────────── AgentMeter (FastAPI, async) ────────────────┐
│ 1. Identify   → run / agent / team from headers             │
│ 2. Breaker?   → if the run is OPEN, return 429 right away   │
│ 3. Loop check → fingerprint the request and count repeats   │
│ 4. Reserve    → estimate the worst-case cost, then run an   │
│                 atomic Lua reserve across run⊂agent⊂team    │
│ 5. Forward    → httpx to the upstream (streaming supported) │
│ 6. Settle     → actual usage → spent += actual, release the │
│                 rest of the reservation                     │
│ 7. Emit       → event to a Redis Stream                     │
└─────────────────────────────────────────────────────────────┘
   │                                   │
   ▼                                   ▼
Upstream LLM API              Worker: threshold alerts → webhook
(OpenAI / Gemini / LiteLLM)            Prometheus /metrics
```

Steps 1 to 7 and the alert worker are implemented; steps 2 and 3 run as one atomic script, before the reserve, and step 7 happens inside the settle and breaker scripts. `/metrics` is planned (M5).

Budget state in Redis (all values integer micro-USD):

| Key | Contents |
|---|---|
| `am:b:run:{run_id}` | `limit`, `spent`, `reserved`; expires `run_ttl_seconds` after the last call |
| `am:b:agent:{agent}:{YYYYMMDD}` | the same fields, per UTC day |
| `am:b:team:{team}:{YYYYMM}` | the same fields, per UTC month |
| `am:r:{id}` | one in-flight reservation: `amount` and its level keys (a short-lived `settled` tombstone after settle) |
| `am:resv` | sorted set of in-flight reservation ids, scored by expiry in Unix ms from Redis `TIME` |
| `am:brk:{run_id}` | the run's breaker; present means OPEN: `reason`, `source`, `steps`, `opened_ms` |
| `am:steps:{run_id}` | the run's step count |
| `am:fp:{run_id}` | request fingerprint → times seen |
| `am:tc:{run_id}` | the response tool-call streak: last fingerprint and count in a row |
| `am:alert:{level}:{id}:{bp}` | one-shot flag of a threshold that has fired; expires with its budget key |
| `am:events` | the event stream (consumer group `am:alerts`) |

The breaker keys expire together with the run's budget key.

The scripts are in [app/lua/](app/lua/): `gate` and `toolcall` (the breaker, in [app/breaker.py](app/breaker.py)), and `reserve`, `settle`, `extend` and `sweep`. Each runs atomically, so a settle and a sweep racing for the same reservation release it exactly once. The client and the full key documentation are in [app/budget.py](app/budget.py). The sweeper loop is in [app/sweeper.py](app/sweeper.py).

## Status

| Milestone | State |
|---|---|
| M0: Skeleton (config, `/healthz`, compose, CI) | ✅ done |
| M1: Pass-through proxy, non-streaming and SSE streaming | ✅ done |
| M2a: Pricing table and worst-case estimate | ✅ done |
| M2b: Atomic reserve/settle across run ⊂ agent ⊂ team | ✅ done |
| M2c: Reservation sweeper, stream heartbeat; 100-request concurrency test across 2 replicas | ✅ done |
| M3: Loop detection, step cap, breaker state, admin reset | ✅ done (the LangGraph demo moves to M6) |
| M4: Redis Stream events, one-shot 50/75/90% alerts, webhook worker | ✅ done |
| M5: Prometheus metrics, `GET /runs/{id}` report | planned |
| M6: Runaway-agent demo, benchmark, docs | planned |

Progress notes and design decisions are in [PROGRESS.md](PROGRESS.md). The full plan is in [PLAN.md](PLAN.md).

## Known limitations

- **Flat image tokens:** every image part counts as `image_part_tokens`, whatever its size or detail. Some models (for example gpt-4o-mini) bill images at many times that, so budgets for vision requests can under-estimate.
- **Cached tokens are charged in full:** prompt-cache discounts are ignored, so the recorded spend can be higher than the invoice (never lower).
- **Single Redis node:** one request's keys span several hash slots, so Redis Cluster is not supported. Use a single primary, with replicas for failover if you need them.
- **Clocks:** reservation expiry uses the Redis clock (`TIME` inside the scripts), so replicas never disagree about it. Day and month budget keys are named from each replica's own clock, so replicas should be NTP-synced. With skew, a call made near midnight UTC can land in a different day's key depending on which replica serves it.
- **A failed settle loses that call's cost:** if Redis is unreachable when a call settles, the sweeper later releases its reservation, but the actual cost is never added to `spent`. The proxy logs it at ERROR level (`settle failed: cost NOT recorded`, with the run id, reservation id and amount) so it can be reconciled by hand.
- **A stream whose reservation is reclaimed is ended:** if a stream's heartbeat finds its reservation gone (Redis unreachable from that replica for longer than `reservation_ttl_seconds`), the stream ends with an `agentmeter_reservation_lost` error event at the next upstream chunk and settles from what was relayed.
- **Reclaiming takes time:** a crashed call's reservation is held until `reservation_ttl_seconds` has passed and the next sweep runs. That blocks budget for up to an hour by default; it never allows overspend.
- **`max_tokens` injection and o-series models:** the injected `max_tokens` is rejected by models that only accept `max_completion_tokens`. Set `max_completion_tokens` yourself for those.
- **Loop detection is heuristic:** only the last two messages and the first tool call of a response are compared. A loop that alternates between three or more states, or varies an argument each time (a counter, a timestamp), is not detected; the step cap is the backstop for those. Polling agents that legitimately repeat should send `X-AM-Loop-Exempt: true` or get a per-agent override.
- **Breaker state is per run only:** there is no agent-wide or team-wide breaker, and an OPEN breaker never closes on its own (no half-open state) before its keys expire with the run.
- **Alerts are at least once:** if the worker dies after the webhook accepted a post but before the `XACK`, that alert is posted again. An alert that keeps failing is retried every `retry_idle_seconds` up to `max_attempts` deliveries, then dead-lettered; an entry trimmed from the stream by `MAXLEN` while still pending is dropped.
- **No alerts for reserved money:** thresholds are measured on `spent` only, so a burst of large in-flight reservations alerts only once those calls settle.
- **Only `/v1/chat/completions`** is proxied. Other endpoints, including the Responses API and Anthropic's `/v1/messages`, are not.

## Demo

Placeholder. M6 adds `demo/runaway_agent.py`, an agent with a deliberately broken tool, run with and without AgentMeter.

## Benchmark

Placeholder. M6 adds a Locust benchmark that measures the proxy's overhead against the mock upstream. Numbers will be published here only once they have been measured.

## Development

```bash
python -m venv .venv && . .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
docker compose up -d redis mock_upstream
ruff check . && ruff format --check . && pytest -q
```

Tests run against the mock upstream and never call a real LLM API. Redis tests use database 15 and flush it before each test. Set `AGENTMETER_TEST_REDIS_URL` to use a different one.
