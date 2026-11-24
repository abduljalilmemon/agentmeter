"""BudgetClient + Lua scripts against a real Redis (test database, flushed per test)."""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest
import redis
import redis.asyncio as aredis

from app.budget import (
    PERIOD_GRACE_SECONDS,
    BudgetClient,
    Rejection,
    Reservation,
    agent_key,
    run_key,
    team_key,
)
from app.identity import Identity
from tests.conftest import TEST_REDIS_URL, make_settings

NOW = datetime(2026, 11, 24, 15, 30, tzinfo=UTC)
BUDGETS = {
    "default_run_usd": "0.001",  # 1_000 micro
    "agents": {"bot": {"daily_usd": "0.0005"}},  # 500
    "teams": {"core": {"monthly_usd": "0.0003"}},  # 300
}


def ident(run: str = "r1", agent: str | None = None, team: str | None = None, budget=None):
    return Identity(run, agent, team, budget, run_id_generated=False)


@pytest.fixture
async def budget(redis_db: redis.Redis) -> AsyncIterator[BudgetClient]:
    client = aredis.from_url(TEST_REDIS_URL)
    yield BudgetClient(client, make_settings(budgets=BUDGETS))
    await client.aclose()


def fields(r: redis.Redis, key: str) -> dict[str, int]:
    return {k: int(v) for k, v in r.hgetall(key).items()}


async def test_reserve_writes_every_level(budget: BudgetClient, redis_db: redis.Redis) -> None:
    resv = await budget.reserve(ident(agent="bot", team="core"), 100, now=NOW)
    assert isinstance(resv, Reservation)
    assert [lv.name for lv in resv.levels] == ["run", "agent", "team"]
    assert fields(redis_db, "am:b:run:r1") == {"limit": 1_000, "reserved": 100}
    assert fields(redis_db, "am:b:agent:bot:20261124") == {"limit": 500, "reserved": 100}
    assert fields(redis_db, "am:b:team:core:202611") == {"limit": 300, "reserved": 100}
    stored = redis_db.hgetall(resv.key)
    assert stored["amount"] == "100"
    assert stored["keys"].split("\n") == [lv.key for lv in resv.levels]
    assert 0 < redis_db.ttl(resv.key) <= 3_600


def test_key_names() -> None:
    assert run_key("r1") == "am:b:run:r1"
    assert agent_key("bot", NOW) == "am:b:agent:bot:20261124"
    assert team_key("core", NOW) == "am:b:team:core:202611"


async def test_key_expiry(budget: BudgetClient, redis_db: redis.Redis) -> None:
    await budget.reserve(ident(agent="bot", team="core"), 1)  # real "now"
    assert 0 < redis_db.ttl(run_key("r1")) <= 7 * 86_400
    levels = budget.levels(ident(agent="bot", team="core"), NOW)
    day_end = int(datetime(2026, 11, 25, tzinfo=UTC).timestamp())
    month_end = int(datetime(2026, 12, 1, tzinfo=UTC).timestamp())
    assert levels[1].expire_at == day_end + PERIOD_GRACE_SECONDS
    assert levels[2].expire_at == month_end + PERIOD_GRACE_SECONDS
    december = budget.levels(ident(team="core"), datetime(2026, 12, 31, 23, 59, tzinfo=UTC))
    assert december[1].expire_at == int(datetime(2027, 1, 1, tzinfo=UTC).timestamp()) + (
        PERIOD_GRACE_SECONDS
    )


async def test_unconfigured_agent_and_team_are_skipped(budget: BudgetClient) -> None:
    levels = budget.levels(ident(agent="nobody", team="nowhere"), NOW)
    assert [lv.name for lv in levels] == ["run"]
    assert [lv.name for lv in budget.levels(ident(agent="bot"), NOW)] == ["run", "agent"]


async def test_run_limit_from_header(budget: BudgetClient, redis_db: redis.Redis) -> None:
    await budget.reserve(ident(budget=2_500), 1, now=NOW)
    assert fields(redis_db, run_key("r1"))["limit"] == 2_500


@pytest.mark.parametrize(
    ("identity", "estimate", "level"),
    [
        (ident(), 1_001, "run"),
        (ident(agent="bot"), 501, "agent"),
        (ident(agent="bot", team="core"), 301, "team"),
    ],
)
async def test_reserve_fails_at_each_level(
    budget: BudgetClient, redis_db: redis.Redis, identity: Identity, estimate: int, level: str
) -> None:
    result = await budget.reserve(identity, estimate, now=NOW)
    assert isinstance(result, Rejection)
    assert result.level.name == level
    assert (result.spent, result.reserved, result.estimate) == (0, 0, estimate)
    # All-or-nothing: no level was touched, not even the ones that had room.
    assert redis_db.keys("am:*") == []


async def test_agent_failure_leaves_run_untouched(
    budget: BudgetClient, redis_db: redis.Redis
) -> None:
    first = await budget.reserve(ident(agent="bot"), 400, now=NOW)
    assert isinstance(first, Reservation)
    # Run r2 has 1_000 of room, but agent "bot" only has 100 left today.
    result = await budget.reserve(ident(run="r2", agent="bot"), 200, now=NOW)
    assert isinstance(result, Rejection) and result.level.name == "agent"
    assert (result.reserved, result.limit) == (400, 500)
    assert not redis_db.exists(run_key("r2"))
    assert fields(redis_db, agent_key("bot", NOW))["reserved"] == 400


async def test_exact_fit_is_allowed(budget: BudgetClient) -> None:
    assert isinstance(await budget.reserve(ident(), 1_000, now=NOW), Reservation)
    assert isinstance(await budget.reserve(ident(), 1, now=NOW), Rejection)


async def test_settle_moves_reserved_into_spent(
    budget: BudgetClient, redis_db: redis.Redis
) -> None:
    resv = await budget.reserve(ident(agent="bot", team="core"), 250, now=NOW)
    assert isinstance(resv, Reservation)
    assert await budget.settle(resv, 37) is True
    for lv in resv.levels:
        state = fields(redis_db, lv.key)
        assert (state["spent"], state["reserved"]) == (37, 0)
    assert not redis_db.exists(resv.key)
    state = await budget.state(run_key("r1"))
    assert (state.limit, state.spent, state.reserved) == (1_000, 37, 0)


async def test_settle_after_reservation_expired(
    budget: BudgetClient, redis_db: redis.Redis
) -> None:
    resv = await budget.reserve(ident(), 250, now=NOW)
    assert isinstance(resv, Reservation)
    redis_db.delete(resv.key)  # what expiry (or the M2c sweeper) does
    assert await budget.settle(resv, 37) is False
    assert fields(redis_db, run_key("r1")) == {"limit": 1_000, "spent": 37, "reserved": 250}


async def test_settle_reapplies_ttl_on_recreated_key(
    budget: BudgetClient, redis_db: redis.Redis
) -> None:
    resv = await budget.reserve(ident(), 250, now=NOW)
    assert isinstance(resv, Reservation)
    redis_db.delete(run_key("r1"))  # level key vanished mid-call
    await budget.settle(resv, 37)
    assert fields(redis_db, run_key("r1")) == {"spent": 37, "reserved": 0}  # clamped at 0
    assert redis_db.ttl(run_key("r1")) > 0


async def test_header_cannot_raise_run_limit(budget: BudgetClient, redis_db: redis.Redis) -> None:
    await budget.reserve(ident(budget=500), 100, now=NOW)
    result = await budget.reserve(ident(budget=1_000_000), 600, now=NOW)
    assert isinstance(result, Rejection) and result.limit == 500
    assert fields(redis_db, run_key("r1"))["limit"] == 500


async def test_config_limit_change_applies(redis_db: redis.Redis) -> None:
    client = aredis.from_url(TEST_REDIS_URL)
    try:
        old = BudgetClient(client, make_settings(budgets=BUDGETS))
        await old.reserve(ident(agent="bot"), 400, now=NOW)
        raised = {**BUDGETS, "agents": {"bot": {"daily_usd": "0.002"}}}
        new = BudgetClient(client, make_settings(budgets=raised))
        assert isinstance(
            await new.reserve(ident(run="r2", agent="bot"), 400, now=NOW), Reservation
        )
        assert fields(redis_db, agent_key("bot", NOW))["limit"] == 2_000
    finally:
        await client.aclose()


async def test_large_amounts_stay_exact(budget: BudgetClient, redis_db: redis.Redis) -> None:
    big = 123_456_789_012_345  # ~123M USD: past Lua's %.14g formatting
    resv = await budget.reserve(ident(budget=big * 2), big, now=NOW)
    assert isinstance(resv, Reservation)
    await budget.settle(resv, big - 1)
    assert fields(redis_db, run_key("r1")) == {"limit": big * 2, "spent": big - 1, "reserved": 0}


async def test_concurrent_reserves_never_overspend(
    budget: BudgetClient, redis_db: redis.Redis
) -> None:
    # Budget 1_000 fits exactly 5 reservations of 200.
    results = await asyncio.gather(*(budget.reserve(ident(), 200, now=NOW) for _ in range(50)))
    assert sum(isinstance(r, Reservation) for r in results) == 5
    assert sum(isinstance(r, Rejection) for r in results) == 45
    assert fields(redis_db, run_key("r1")) == {"limit": 1_000, "reserved": 1_000}


async def test_concurrent_reserve_settle_cycles(
    budget: BudgetClient, redis_db: redis.Redis
) -> None:
    async def call() -> bool:
        resv = await budget.reserve(ident(), 300, now=NOW)
        if isinstance(resv, Rejection):
            return False
        await asyncio.sleep(0)
        await budget.settle(resv, 100)
        return True

    ok = sum(await asyncio.gather(*(call() for _ in range(40))))
    state = fields(redis_db, run_key("r1"))
    assert state["reserved"] == 0
    assert state["spent"] == ok * 100
    assert state["spent"] <= 1_000
