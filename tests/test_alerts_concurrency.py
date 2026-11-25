"""M4 acceptance: 50/75/90 fire exactly once per budget level under concurrent load,
across 2 proxy replicas (own uvicorn servers, event loops and Redis pools)."""

import asyncio
from collections import Counter

import httpx
import pytest
import redis

from app.budget import run_key
from app.events import STREAM_KEY
from app.main import create_app
from mock_upstream.main import app as mock_app
from tests.conftest import make_settings, serve

pytestmark = [pytest.mark.concurrency, pytest.mark.usefixtures("redis_db")]

RUNS = ["ra", "rb", "rc"]
RUN_LIMIT = 600  # micro-USD per run
SHARED_LIMIT = 1_800  # agent day and team month: all three runs together
CALL_MICRO = 14  # mock usage: 10 in + 20 out on gpt-4o-mini
WAVE = 30
BODY = {
    "model": "gpt-4o-mini",
    "messages": [{"role": "user", "content": "hi"}],
    "max_tokens": 100,  # worst case 62 micro: a wave over-reserves and some calls get 429
}
THRESHOLDS = (5_000, 7_500, 9_000)


async def wave(http: httpx.AsyncClient, urls: list[str]) -> list[int]:
    calls = [
        http.post(
            f"{urls[i % 2]}/v1/chat/completions",
            json=BODY,
            headers={
                "X-AM-Run-Id": RUNS[i % 3],
                "X-AM-Run-Budget": f"0.000{RUN_LIMIT}",
                "X-AM-Agent": "bot",
                "X-AM-Team": "core",
            },
        )
        for i in range(WAVE)
    ]
    return [r.status_code for r in await asyncio.gather(*calls)]


async def drive(urls: list[str]) -> int:
    """Fire waves until a whole wave is refused: every level is then (nearly) full."""
    ok = 0
    limits = httpx.Limits(max_connections=WAVE, max_keepalive_connections=WAVE)
    async with httpx.AsyncClient(timeout=30, limits=limits) as http:
        # Open the connections first so each wave arrives as one burst (overlapping settles).
        await asyncio.gather(*(http.get(f"{urls[i % 2]}/healthz") for i in range(WAVE)))
        for _ in range(80):
            statuses = await wave(http, urls)
            assert set(statuses) <= {200, 429}, statuses
            ok += statuses.count(200)
            if 200 not in statuses:
                return ok
    raise AssertionError("budgets never filled up")


def test_thresholds_fire_once_per_level_on_2_replicas(redis_db: redis.Redis) -> None:
    settings = make_settings(
        budgets={
            "agents": {"bot": {"daily_usd": "0.0018"}},
            "teams": {"core": {"monthly_usd": "0.0018"}},
        },
        loop={"step_cap": 100_000},  # refused calls count as steps; this test is about money
    )
    apps = [
        create_app(settings, upstream_transport=httpx.ASGITransport(app=mock_app)) for _ in range(2)
    ]
    with serve(apps[0]) as a, serve(apps[1]) as b:
        ok = asyncio.run(drive([a, b]))

    levels = {key: limit for key, limit in ((run_key(r), RUN_LIMIT) for r in RUNS)}
    agent_key = next(iter(redis_db.scan_iter("am:b:agent:bot:*")))
    team_key = next(iter(redis_db.scan_iter("am:b:team:core:*")))
    levels |= {agent_key: SHARED_LIMIT, team_key: SHARED_LIMIT}

    # Every level really did cross 90%, so all 15 (level, threshold) pairs are due.
    for key, limit in levels.items():
        state = {k: int(v) for k, v in redis_db.hgetall(key).items()}
        assert state["reserved"] == 0, key
        assert state["spent"] * 10_000 >= limit * 9_000, (key, state)
        assert state["spent"] <= limit, (key, state)

    entries = [fields for _id, fields in redis_db.xrange(STREAM_KEY)]
    fired = Counter(
        (e["key"], int(e["threshold_bp"])) for e in entries if e["type"] == "threshold_crossed"
    )
    expected = {(key, bp): 1 for key in levels for bp in THRESHOLDS}
    assert fired == expected  # exactly one per (level, threshold): 3 runs + agent + team
    assert {e["level"] for e in entries if e["type"] == "threshold_crossed"} == {
        "run",
        "agent",
        "team",
    }

    settled = [e for e in entries if e["type"] == "call_settled"]
    assert len(settled) == ok
    total = sum(int(redis_db.hget(run_key(r), "spent")) for r in RUNS)
    assert total == ok * CALL_MICRO == int(redis_db.hget(agent_key, "spent"))
