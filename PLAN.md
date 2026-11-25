# AgentMeter: PLAN
 
> A budget circuit breaker for AI agents. A drop-in, OpenAI-compatible proxy that gives every agent run its own budget, kills runaway loops, and alerts before money is gone, not after.
 
Working name. Check that the name is free on PyPI and GitHub before you publish.
 
---
 
## 1. Problem
 
Agents call LLMs in loops. A single bug (a failing tool, a retry with no backoff, a planner that never decides it's done) can burn hundreds of dollars overnight. Teams in 2026 report the same gaps:
 
- No spend visibility per agent, per run or per task. They only see one total on the provider invoice.
- No hard stop. Budgets on provider consoles and API keys are monthly and coarse. Nothing stops *this one run* when it goes wrong.
- Alerts arrive at 100%, when it's already too late.
Existing gateways (LiteLLM, Portkey, cloud AI gateways) already handle budgets per **key, user or team**. AgentMeter's angle is narrower:
 
1. **Budgets per run and per task**, nested inside agent and team budgets
2. **Loop detection**, which trips a breaker when an agent repeats itself
3. **Correct under concurrency**, so parallel calls on several proxy replicas can never overspend
## 2. Goals and non-goals
 
**Goals**
- Use it by changing one line in an existing app: `base_url="http://localhost:8080/v1"`
- Budgets in micro-USD, enforced atomically in Redis
- A circuit breaker per run, with these triggers: budget exhausted, loop detected, step cap reached
- Alerts that fire once each at 50%, 75% and 90%, sent to a webhook
- Prometheus metrics plus a small read API for run reports
- A reproducible demo that shows a runaway agent being stopped
**Non-goals (v1)**
- Not a full LLM gateway: no model routing, no caching, no key vault. You can put LiteLLM *behind* AgentMeter if you need those.
- No UI beyond a minimal run report page
- No multi-tenant auth beyond a static admin token
## 3. How it works
 
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
 
### 3.1 Reserve, then settle (the core idea)
- Before forwarding, compute `estimate = input_tokens × in_price + max_tokens × out_price`. Count input tokens with tiktoken. If the request has no `max_tokens`, inject a configured default.
- A Lua script checks **every level** (run, agent, team). It reserves only if `spent + reserved + estimate <= limit` holds at all levels, so the whole check is all-or-nothing.
- After the response, settle with the real `usage`, so `spent += actual` and `reserved -= estimate`.
- Store money as **integer micro-USD**, never floats.
- Each reservation gets an ID with a TTL. A sweeper releases reservations whose requests crashed mid-flight, so budgets don't leak.
### 3.2 Loop detection
- Fingerprint = hash of the normalized last two messages (the last assistant tool call, with canonical JSON args, plus the tool result).
- If the same fingerprint shows up `N` times in one run (default 3), the breaker opens with `reason=loop`.
- If the response contains the same tool call `K` times in a row, that also counts as a loop.
- The step cap is a hard maximum on calls per run (default 50).
### 3.3 Circuit breaker
- The breaker has two states for each run, CLOSED and OPEN. While it's OPEN, the proxy returns HTTP 429 with a structured body:
  `{"error": {"type": "agentmeter_breaker_open", "reason": "loop|budget|step_cap", "run_id": "...", "spent_usd": 0.41}}`
- An admin endpoint can reset it manually: `POST /admin/runs/{id}/reset`.
- An exhausted budget is not a breaker state: it returns 429 with `reason=budget` on each call that doesn't fit, and it does not open the breaker. A cheaper call can still fit, and a reset never has to touch money. Only `loop` and `step_cap` open the breaker.
### 3.4 Alerts
- At 50%, 75% and 90% of any budget level, the proxy publishes an event. A `SET NX` flag on each threshold makes sure each alert fires once.
- A worker consumes the Redis Stream and posts to a Slack-compatible webhook.
### 3.5 Streaming
- Inject `stream_options: {"include_usage": true}` so the final chunk carries the token usage.
- If a stream is cut off, settle with a tiktoken count of what was streamed so far.
- Stretch goal: cut the stream mid-way when the run budget hits zero.
## 4. Configuration
 
```yaml
# agentmeter.yaml
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
 
Check the prices against the providers' current price pages before the demo. The numbers above are placeholders.
 
## 5. Repo structure
 
```
agentmeter/
  app/
    main.py            # FastAPI app, routes
    proxy.py           # forward + streaming
    budget.py          # reserve/settle client
    lua/reserve.lua
    lua/settle.lua
    loops.py           # fingerprinting + detection
    breaker.py
    pricing.py
    config.py
    events.py          # Redis Streams producer
  worker/alerts.py
  mock_upstream/       # fake OpenAI server with canned usage, so tests cost nothing
  demo/runaway_agent.py  # LangGraph agent with a deliberately broken tool
  tests/
  bench/locustfile.py
  docker-compose.yml   # proxy, worker, redis, prometheus, mock upstream
  PLAN.md
  README.md
```
 
## 6. Milestones
 
Each milestone should end with green tests and a commit.
 
**M0: Skeleton (½ day)**
- FastAPI app, config loading, docker-compose with Redis and the mock upstream
- `/healthz`, CI that runs ruff and pytest
- ✅ `docker compose up` works, and CI is green
**M1: Pass-through proxy (1 day)**
- `/v1/chat/completions` forwarded with httpx (non-streaming first, then SSE streaming)
- Read the identity headers; generate a run ID if none is given
- ✅ The OpenAI Python SDK works unchanged against the proxy, with and without streaming
**M2: Atomic budgets (1–2 days)**
- Pricing table, token estimate, Lua reserve and settle across run, agent and team
- Reservation TTL plus the sweeper
- ✅ **Concurrency test:** 100 parallel requests against a run budget that fits 10 → exactly 10 succeed and the rest get 429. It also passes with 2 proxy replicas.
- ✅ The spent total matches the sum of mock usage exactly
**M3: Loop detection and breaker (1 day)**
- Fingerprinting, repeat threshold, step cap, breaker state, admin reset
- ✅ The demo agent with a broken tool gets stopped after N repeats, with `reason=loop`
**M4: Alerts and events (1 day)**
- Redis Stream events, alert worker, one-shot thresholds, webhook
- ✅ The 50, 75 and 90 alerts each fire exactly once, even under concurrent load
**M5: Observability (1 day)**
- Prometheus metrics: `am_spend_usd_total{agent,team}`, `am_breaker_trips_total{reason}`, `am_proxy_overhead_seconds`
- `GET /runs/{id}` report: calls, tokens, cost, breaker events
- Optional: OpenTelemetry spans
- ✅ A Grafana or Prometheus screenshot for the README
**M6: Demo, benchmark, README (1–2 days)**
- `demo/runaway_agent.py`: run once without AgentMeter (capped by a mock so it doesn't cost real money) and once with it. Report calls, cost and time-to-stop for both.
- Locust benchmark: measure p50 and p95 proxy overhead against the mock upstream
- README with the problem, a GIF of the demo, a one-line quickstart, an architecture diagram and the benchmark table
- ✅ Someone who has never seen the repo can run the demo in under 5 minutes
**Stretch**
- Cut streams mid-way at budget zero
- Anthropic `/v1/messages` support
- Adaptive action: when the budget gets close, downgrade the model instead of blocking
- Export per-run cost to **SwapCheck**, so model-swap verdicts include a cost dimension
## 7. Testing strategy
- Unit tests for pricing math, fingerprinting and the breaker state machine
- Integration tests against real Redis (testcontainers or the compose service) plus the mock upstream
- Property test: no ordering of concurrent reserve and settle calls can ever push `spent` over the `limit`
- Most tests use the mock upstream, so CI costs nothing. One optional smoke test runs against a real cheap model.
## 8. Resume bullet (template: fill in with measured numbers only)
 
> Built AgentMeter, an OpenAI-compatible FastAPI + Redis proxy that enforces atomic, hierarchical per-run token budgets and detects agent loops; stopped a runaway LangGraph agent after `__` calls (vs `__` uncapped) with `__` ms p95 overhead, and guaranteed zero overspend under `__` concurrent requests across replicas.
 
## 9. Risks
- **Token estimates are off for some models.** Settle always uses the real usage. The estimate only gates the reservation, so being off makes the gate slightly conservative but never wrong.
- **Prices change.** The pricing table lives in config, and the README tells users to keep it up to date.
- **False loop positives** (legitimate polling). Make the threshold configurable for each agent, and allow an `X-AM-Loop-Exempt` header.
 