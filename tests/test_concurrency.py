"""M2 acceptance: 100 parallel calls, 2 proxy replicas, a run budget that fits exactly 10.

Two proxy apps each run under their own uvicorn server, in their own thread and event
loop, with their own Redis connection pool and upstream client, the way two replicas
would. Both share one real Redis and the live mock upstream.

The mock is set to report exactly the worst-case usage, so each call's actual cost equals
its estimate. A settle then frees no headroom, and exactly 10 calls fit whatever the
timing. With actual < estimate, settled calls would return room and an 11th might fit.
"""

import asyncio
import time
from collections import Counter
from decimal import Decimal

import httpx
import pytest
import redis

from app.budget import RESERVATIONS_KEY, run_key
from app.estimate import estimate
from app.main import create_app
from app.pricing import PriceTable
from tests.conftest import make_settings, serve

pytestmark = [pytest.mark.concurrency, pytest.mark.usefixtures("redis_db")]

CALLS = 100
NO_STEP_CAP = {"step_cap": 10_000}  # these tests are about money; 100 calls > default cap
FITS = 10
BODY = {
    "model": "gpt-4o-mini",
    "messages": [{"role": "user", "content": "hi"}],
    "max_tokens": 100,
}


async def _fire(urls: list[str], headers: dict[str, str]) -> list[httpx.Response]:
    limits = httpx.Limits(max_connections=CALLS, max_keepalive_connections=CALLS)
    async with httpx.AsyncClient(timeout=30, limits=limits) as http:
        calls = [
            http.post(
                f"{urls[i % 2]}/v1/chat/completions",
                json={**BODY, "stream": i % 4 >= 2},  # both replicas get plain and streaming
                headers=headers,
            )
            for i in range(CALLS)
        ]
        return await asyncio.gather(*calls)


def test_100_calls_2_replicas_exactly_10_fit(
    redis_db: redis.Redis, live_mock: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = make_settings(live_mock, loop=NO_STEP_CAP)
    est = estimate(PriceTable(settings.pricing), BODY["model"], BODY, settings.default_max_tokens)
    monkeypatch.setenv("MOCK_PROMPT_TOKENS", str(est.input_tokens))
    monkeypatch.setenv("MOCK_COMPLETION_TOKENS", str(est.max_output_tokens))
    monkeypatch.setenv("MOCK_DELAY_MS", "50")  # keep calls in flight at the same time
    limit = FITS * est.micro
    headers = {
        "X-AM-Run-Id": "c100",
        "X-AM-Run-Budget": format(Decimal(limit) / 1_000_000, "f"),
    }

    with serve(create_app(settings)) as a, serve(create_app(settings)) as b:
        responses = asyncio.run(_fire([a, b], headers))

        statuses = Counter(r.status_code for r in responses)
        assert statuses == {200: FITS, 429: CALLS - FITS}, statuses
        for r in responses:
            if r.status_code == 429:
                assert r.json()["error"]["type"] == "agentmeter_breaker_open"
            elif r.headers["content-type"].startswith("text/event-stream"):
                assert r.text.endswith("data: [DONE]\n\n")
            else:
                assert r.json()["usage"]["completion_tokens"] == est.max_output_tokens

        # A stream settles in the server just after its last byte, so give it a moment.
        state: dict[str, int] = {}
        for _ in range(250):
            state = {k: int(v) for k, v in redis_db.hgetall(run_key("c100")).items()}
            if state.get("reserved") == 0 and state.get("spent") == FITS * est.micro:
                break
            time.sleep(0.02)

    assert state["limit"] == limit
    assert state["spent"] <= state["limit"]
    assert state["spent"] == FITS * est.micro  # each success billed its usage, once
    assert state["reserved"] == 0
    assert redis_db.zcard(RESERVATIONS_KEY) == 0


@pytest.mark.parametrize(("budget_micro", "label"), [(10_000_000, "roomy"), (2_000, "tight")])
def test_usage_below_estimate_spent_is_exact_sum(
    redis_db: redis.Redis, live_mock: str, monkeypatch: pytest.MonkeyPatch, budget_micro, label
) -> None:
    """Mock usage (8 in, 20 out: 14 µUSD) is far below the estimate (62). Settles then
    return headroom while other calls are in flight; spent must still be the exact sum."""
    settings = make_settings(live_mock, loop=NO_STEP_CAP)
    monkeypatch.setenv("MOCK_PROMPT_TOKENS", "8")
    monkeypatch.setenv("MOCK_COMPLETION_TOKENS", "20")
    monkeypatch.setenv("MOCK_DELAY_MS", "20")
    actual = PriceTable(settings.pricing).cost_micro(BODY["model"], 8, 20)
    est = estimate(PriceTable(settings.pricing), BODY["model"], BODY, settings.default_max_tokens)
    assert actual < est.micro
    run_id = f"under-{label}"
    headers = {
        "X-AM-Run-Id": run_id,
        "X-AM-Run-Budget": format(Decimal(budget_micro) / 1_000_000, "f"),
    }

    with serve(create_app(settings)) as a, serve(create_app(settings)) as b:
        responses = asyncio.run(_fire([a, b], headers))
        ok = sum(r.status_code == 200 for r in responses)
        assert ok + sum(r.status_code == 429 for r in responses) == CALLS
        state: dict[str, int] = {}
        for _ in range(250):
            state = {k: int(v) for k, v in redis_db.hgetall(run_key(run_id)).items()}
            if state.get("reserved") == 0 and state.get("spent") == ok * actual:
                break
            time.sleep(0.02)

    assert state["spent"] == ok * actual  # exactly the sum of the mock usage
    assert state["spent"] <= state["limit"] == budget_micro
    assert state["reserved"] == 0
    if label == "roomy":
        assert ok == CALLS
    else:
        # 2_000 fits at least 2_000 // 62 = 32 concurrent worst cases; settles free more.
        assert 2_000 // est.micro <= ok < CALLS
