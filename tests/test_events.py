"""Events on am:events and one-shot threshold alerts in settle.lua (M4)."""

import random
from collections.abc import AsyncIterator

import httpx
import pytest
import redis
import redis.asyncio as aredis
from fastapi.testclient import TestClient

from app.budget import BudgetClient, Reservation, SettleResult, run_key
from app.events import (
    STREAM_KEY,
    alert_text,
    format_bp,
    format_usd,
    threshold_flag_key,
    threshold_reached,
)
from app.identity import Identity
from app.main import create_app
from mock_upstream.main import app as mock_app
from tests.conftest import TEST_REDIS_URL, make_settings

BUDGETS = {
    "default_run_usd": "1",
    "agents": {"bot": {"daily_usd": "0.02"}},  # 20_000 micro
    "teams": {"core": {"monthly_usd": "0.025"}},  # 25_000 micro
}


def ident(run="r1", agent=None, team=None, budget=None) -> Identity:
    return Identity(run, agent, team, budget, run_id_generated=False)


def events(r: redis.Redis, kind: str | None = None) -> list[dict[str, str]]:
    entries = [fields for _id, fields in r.xrange(STREAM_KEY)]
    return [e for e in entries if kind is None or e["type"] == kind]


def crossings(r: redis.Redis) -> list[tuple[str, int]]:
    return [(e["level"], int(e["threshold_bp"])) for e in events(r, "threshold_crossed")]


@pytest.fixture
async def budget(redis_db: redis.Redis) -> AsyncIterator[BudgetClient]:
    client = aredis.from_url(TEST_REDIS_URL)
    yield BudgetClient(client, make_settings(budgets=BUDGETS))
    await client.aclose()


async def spend(budget: BudgetClient, identity: Identity, amount: int, **details) -> None:
    """One call that reserves and then settles exactly ``amount``."""
    resv = await budget.reserve(identity, amount)
    assert isinstance(resv, Reservation), resv
    assert await budget.settle(resv, amount, details) is SettleResult.RELEASED


# --- threshold math -------------------------------------------------------------------


async def test_exact_50_percent_boundary(budget: BudgetClient, redis_db: redis.Redis) -> None:
    who = ident(budget=10_000)
    await spend(budget, who, 4_999)  # 49.99%
    assert crossings(redis_db) == []
    await spend(budget, who, 1)  # exactly 50%
    assert crossings(redis_db) == [("run", 5_000)]
    (event,) = events(redis_db, "threshold_crossed")
    assert event["key"] == run_key("r1")
    assert (event["spent"], event["limit"], event["run"]) == ("5000", "10000", "r1")


async def test_one_settle_from_40_to_95_fires_each_once(
    budget: BudgetClient, redis_db: redis.Redis
) -> None:
    who = ident(budget=10_000)
    await spend(budget, who, 4_000)
    assert crossings(redis_db) == []
    await spend(budget, who, 5_500)  # 40% -> 95%
    assert crossings(redis_db) == [("run", 5_000), ("run", 7_500), ("run", 9_000)]
    assert {e["spent"] for e in events(redis_db, "threshold_crossed")} == {"9500"}
    await spend(budget, who, 400)  # 99%: nothing new
    assert len(crossings(redis_db)) == 3


async def test_measured_on_spent_not_reserved(budget: BudgetClient, redis_db: redis.Redis) -> None:
    who = ident(budget=10_000)
    big = await budget.reserve(who, 9_000)  # 90% reserved, nothing spent
    assert isinstance(big, Reservation)
    await spend(budget, who, 100)
    assert crossings(redis_db) == []
    await budget.settle(big, 6_000)  # spent 6_100: only 50% crossed
    assert crossings(redis_db) == [("run", 5_000)]


@pytest.mark.parametrize(
    ("limit", "spent", "fired"),
    [
        (3, 1, []),  # 33%
        (3, 2, [5_000]),  # 66.7%: 2 * 10000 >= 3 * 5000
        (3, 3, [5_000, 7_500, 9_000]),
        (10_001, 5_000, []),  # 49.995%
        (10_001, 5_001, [5_000]),  # 50.005%
        (9_999, 8_999, [5_000, 7_500]),  # 89.999%
        (9_999, 9_000, [5_000, 7_500, 9_000]),  # 90.009%
    ],
)
async def test_threshold_rounding(
    budget: BudgetClient, redis_db: redis.Redis, limit: int, spent: int, fired: list[int]
) -> None:
    await spend(budget, ident(budget=limit), spent)
    assert crossings(redis_db) == [("run", bp) for bp in fired]


async def test_exact_math_beyond_double_precision(
    budget: BudgetClient, redis_db: redis.Redis
) -> None:
    # limit * 10000 is far past 2^53, where a naive Lua spent*10000 >= limit*bp rounds.
    limit = 123_456_789_012_345
    need = -(-limit * 7_500 // 10_000)  # smallest spent with spent*10000 >= limit*7500
    who = ident(budget=limit)
    await spend(budget, who, need - 1)
    assert crossings(redis_db) == [("run", 5_000)]
    await spend(budget, who, 1)
    assert crossings(redis_db) == [("run", 5_000), ("run", 7_500)]
    assert events(redis_db, "threshold_crossed")[1]["spent"] == str(need)


async def test_lua_matches_reference_rule(budget: BudgetClient, redis_db: redis.Redis) -> None:
    """Random limits (up to 2^53) and spends right around each threshold."""
    rng = random.Random(4)
    for case in range(150):
        limit = rng.choice([rng.randint(1, 10_000), rng.randint(1, 2**53 - 1)])
        bp = rng.choice([5_000, 7_500, 9_000])
        need = -(-limit * bp // 10_000)
        spent = max(0, need + rng.choice([-1, 0, 1]))
        run = f"p{case}"
        resv = await budget.reserve(ident(run=run, budget=limit), 0)
        assert isinstance(resv, Reservation)
        await budget.settle(resv, spent)
        fired = {
            int(e["threshold_bp"]) for e in events(redis_db, "threshold_crossed") if e["run"] == run
        }
        for b in (5_000, 7_500, 9_000):
            assert (b in fired) == threshold_reached(spent, limit, b), (limit, spent, b)


# --- every level, flags ---------------------------------------------------------------


async def test_every_level_fires_with_its_own_flag(
    budget: BudgetClient, redis_db: redis.Redis
) -> None:
    who = ident(agent="bot", team="core", budget=200_000)
    await spend(budget, who, 10_000)  # run 5%, agent 50%, team 40%
    assert crossings(redis_db) == [("agent", 5_000)]
    await spend(budget, who, 9_000)  # run 9.5%, agent 95%, team 76%
    assert sorted(crossings(redis_db)) == [
        ("agent", 5_000),
        ("agent", 7_500),
        ("agent", 9_000),
        ("team", 5_000),
        ("team", 7_500),
    ]
    _run, agent, team = budget.levels(who)
    expected = {agent.key: (5_000, 7_500, 9_000), team.key: (5_000, 7_500)}
    for key, fired in expected.items():
        key_ttl = redis_db.ttl(key)  # end of the UTC day / month + grace
        for bp in fired:
            flag = threshold_flag_key(key, bp)
            assert redis_db.get(flag) == "1"
            assert abs(redis_db.ttl(flag) - key_ttl) <= 2, flag  # expires with the key
    assert not redis_db.exists(threshold_flag_key(team.key, 9_000))


async def test_run_flag_follows_the_sliding_run_ttl(
    budget: BudgetClient, redis_db: redis.Redis
) -> None:
    who = ident(budget=10_000)
    await spend(budget, who, 6_000)
    flag = threshold_flag_key(run_key("r1"), 5_000)
    redis_db.expire(flag, 100)  # pretend the run key's expiry has moved on since
    await spend(budget, who, 1)  # the next settle pushes it out again (EXPIREAT GT)
    assert abs(redis_db.ttl(flag) - redis_db.ttl(run_key("r1"))) <= 2


async def test_existing_flags_fire_nothing_more(
    budget: BudgetClient, redis_db: redis.Redis
) -> None:
    who = ident(agent="bot", budget=20_000)
    await spend(budget, who, 19_000)  # run and agent both at 95%
    first = crossings(redis_db)
    assert len(first) == 6  # run and agent, 3 each
    # Spend falls back below every threshold (e.g. restored by hand) and climbs again.
    for level in budget.levels(who):
        redis_db.hset(level.key, "spent", 0)
    await spend(budget, who, 19_000)
    assert crossings(redis_db) == first


# --- call_settled ---------------------------------------------------------------------


async def test_call_settled_event(budget: BudgetClient, redis_db: redis.Redis) -> None:
    who = ident(run="cs", agent="bot", team="core")
    resv = await budget.reserve(who, 300)
    assert isinstance(resv, Reservation)
    await budget.settle(resv, 42, {"model": "gpt-4o-mini", "prompt_tokens": 8, "how": "usage"})
    (event,) = events(redis_db, "call_settled")
    assert event == {
        "type": "call_settled",
        "run": "cs",
        "agent": "bot",
        "team": "core",
        "cost_micro": "42",
        "reservation": resv.id,
        "reservation_state": "released",
        "model": "gpt-4o-mini",
        "prompt_tokens": "8",
        "how": "usage",
    }
    assert await budget.settle(resv, 42) is SettleResult.DUPLICATE
    assert len(events(redis_db, "call_settled")) == 1  # a duplicate settle emits nothing


async def test_call_settled_after_sweep(budget: BudgetClient, redis_db: redis.Redis) -> None:
    resv = await budget.reserve(ident(), 300)
    assert isinstance(resv, Reservation)
    redis_db.zadd("am:resv", {resv.id: 0})
    await budget.sweep()
    assert await budget.settle(resv, 5) is SettleResult.GONE
    (event,) = events(redis_db, "call_settled")
    assert (event["reservation_state"], event["agent"], event["team"]) == ("gone", "", "")


@pytest.mark.parametrize("stream", [False, True])
def test_call_settled_through_the_proxy(redis_db: redis.Redis, stream: bool) -> None:
    app = create_app(make_settings(), upstream_transport=httpx.ASGITransport(app=mock_app))
    body = {"model": "gpt-4o-mini", "messages": [{"role": "user", "content": "hi"}]}
    with TestClient(app) as c:
        resp = c.post(
            "/v1/chat/completions",
            json={**body, "stream": stream},
            headers={"X-AM-Run-Id": "px", "X-AM-Agent": "a1", "X-AM-Team": "t1"},
        )
    assert resp.status_code == 200
    (event,) = events(redis_db, "call_settled")
    assert event["run"] == "px" and event["agent"] == "a1" and event["team"] == "t1"
    assert event["model"] == "gpt-4o-mini"
    assert (event["prompt_tokens"], event["completion_tokens"]) == ("10", "20")  # mock usage
    assert event["cost_micro"] == "14"
    assert event["estimate_micro"] == "616"
    assert event["how"] == "usage"
    assert int(redis_db.hget(run_key("px"), "spent")) == 14


# --- formatting -----------------------------------------------------------------------


def test_format_usd_and_bp() -> None:
    assert format_usd(0) == "$0.00"
    assert format_usd(1_234_500) == "$1.2345"
    assert format_usd(500_000) == "$0.50"
    assert format_usd(7) == "$0.000007"
    assert format_usd(20_000_000) == "$20.00"
    assert format_bp(5_000) == "50%"
    assert format_bp(1_250) == "12.5%"
    assert format_bp(9_999) == "99.99%"


def test_alert_text() -> None:
    text = alert_text(
        {
            "type": "threshold_crossed",
            "level": "agent",
            "key": "am:b:agent:bot:20261126",
            "threshold_bp": "7500",
            "spent": "15000000",
            "limit": "20000000",
            "run": "r1",
            "agent": "bot",
            "team": "",
        }
    )
    assert "agent budget `agent:bot:20261126` crossed 75%" in text
    assert "spent $15.00 of $20.00" in text
    assert "run `r1`, agent `bot`" in text
    breaker = alert_text(
        {
            "type": "breaker_open",
            "run": "r9",
            "reason": "loop",
            "source": "request_repeats",
            "steps": "4",
            "spent": "42",
            "agent": "",
            "team": "",
        }
    )
    assert "circuit breaker OPEN for run `r9`" in breaker
    assert "reason loop (request_repeats) after 4 steps, spent $0.000042" in breaker
    assert "POST /admin/runs/r9/reset" in breaker
