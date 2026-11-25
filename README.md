# AgentMeter

A budget circuit breaker for AI agents. AgentMeter is a drop-in, OpenAI-compatible proxy. It gives every agent run its own budget and refuses the call that would overspend it, before that call reaches the model.

> Status: early development. Budgets work today (M2b). Loop detection, alerts and metrics are planned. See [Status](#status).

## Why

Agents call LLMs in loops. One bug can burn money all night: a failing tool, a retry with no backoff, or a planner that never decides it's done. Provider consoles and API keys only offer coarse monthly budgets, so nothing stops *this one run* when it goes wrong.

Gateways such as LiteLLM and Portkey already budget per key, user or team. AgentMeter has a narrower aim:

1. **Budgets per run,** nested inside a daily agent budget and a monthly team budget.
2. **Correct under concurrency:** parallel calls, across several proxy replicas, can't overspend. Each call reserves its worst-case cost atomically in Redis before it is forwarded, then settles with the real usage.
3. **Loop detection and alerts** (planned): trip the breaker when an agent repeats itself, and warn at 50%, 75% and 90% of a budget.

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

Ids and names must match `[A-Za-z0-9._:-]{1,128}`. All `X-AM-*` headers are removed before forwarding. Every response carries `X-AM-Run-Id`, except the 400 for an invalid `X-AM-*` header.

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

`code` is `upstream_cut`, `upstream_timeout` or `upstream_incomplete`.

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
alerts:
  thresholds: [0.5, 0.75, 0.9]
  webhook_url: ${ALERT_WEBHOOK}
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
| `loop.*`, `alerts.*` | see YAML | Parsed and validated, but not used yet (M3, M4). |

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

Steps 1, 4, 5 and 6 are implemented. Steps 2, 3 and 7, the worker and `/metrics` are planned.

Budget state in Redis (all values integer micro-USD):

| Key | Contents |
|---|---|
| `am:b:run:{run_id}` | `limit`, `spent`, `reserved`; expires `run_ttl_seconds` after the last call |
| `am:b:agent:{agent}:{YYYYMMDD}` | the same fields, per UTC day |
| `am:b:team:{team}:{YYYYMM}` | the same fields, per UTC month |
| `am:r:{id}` | one in-flight reservation: `amount` and its level keys (a short-lived `settled` tombstone after settle) |
| `am:resv` | sorted set of in-flight reservation ids, scored by expiry in Unix ms from Redis `TIME` |

The scripts are in [app/lua/](app/lua/): `reserve`, `settle`, `extend` and `sweep`. Each runs atomically, so a settle and a sweep racing for the same reservation release it exactly once. The client and the full key documentation are in [app/budget.py](app/budget.py). The sweeper loop is in [app/sweeper.py](app/sweeper.py).

## Status

| Milestone | State |
|---|---|
| M0: Skeleton (config, `/healthz`, compose, CI) | ✅ done |
| M1: Pass-through proxy, non-streaming and SSE streaming | ✅ done |
| M2a: Pricing table and worst-case estimate | ✅ done |
| M2b: Atomic reserve/settle across run ⊂ agent ⊂ team | ✅ done |
| M2c: Reservation sweeper, stream heartbeat; 100-request concurrency test across 2 replicas | ✅ done |
| M3: Loop detection, step cap, breaker state, admin reset | planned |
| M4: Redis Stream events, one-shot 50/75/90% alerts, webhook worker | planned |
| M5: Prometheus metrics, `GET /runs/{id}` report | planned |
| M6: Runaway-agent demo, benchmark, docs | planned |

Progress notes and design decisions are in [PROGRESS.md](PROGRESS.md). The full plan is in [PLAN.md](PLAN.md).

## Known limitations

- **Flat image tokens:** every image part counts as `image_part_tokens`, whatever its size or detail. Some models (for example gpt-4o-mini) bill images at many times that, so budgets for vision requests can under-estimate.
- **Cached tokens are charged in full:** prompt-cache discounts are ignored, so the recorded spend can be higher than the invoice (never lower).
- **Single Redis node:** one request's keys span several hash slots, so Redis Cluster is not supported. Use a single primary, with replicas for failover if you need them.
- **Clocks:** reservation expiry uses the Redis clock (`TIME` inside the scripts), so replicas never disagree about it. Day and month budget keys are named from each replica's own clock, so replicas should be NTP-synced. With skew, a call made near midnight UTC can land in a different day's key depending on which replica serves it.
- **Reclaiming takes time:** a crashed call's reservation is held until `reservation_ttl_seconds` has passed and the next sweep runs. That blocks budget for up to an hour by default; it never allows overspend.
- **`max_tokens` injection and o-series models:** the injected `max_tokens` is rejected by models that only accept `max_completion_tokens`. Set `max_completion_tokens` yourself for those.
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
