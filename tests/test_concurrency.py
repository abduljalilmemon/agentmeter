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
    settings = make_settings(live_mock)
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
