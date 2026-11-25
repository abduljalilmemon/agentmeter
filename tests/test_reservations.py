"""Reservation lifecycle (M2c): sweep, extend/heartbeat, exactly-once, disconnect billing."""

import asyncio
import json
import time
from collections.abc import AsyncIterator

import anyio
import httpx
import pytest
import redis
import redis.asyncio as aredis
from fastapi.testclient import TestClient

from app import chat
from app.budget import (
    RESERVATIONS_KEY,
    BudgetClient,
    Reservation,
    SettleResult,
    run_key,
)
from app.config import Settings
from app.identity import Identity
from app.main import create_app
from app.sweeper import sweep_once
from tests.conftest import TEST_REDIS_URL, make_settings

BUDGETS = {
    "default_run_usd": "0.001",  # 1_000 micro
    "agents": {"bot": {"daily_usd": "1"}},
    "teams": {"core": {"monthly_usd": "1"}},
}
# Short TTL so tests can outlive it; the upstream timeout must stay below it.
SHORT_TTL = {
    "reservation_ttl_seconds": 1,
    "upstream_timeout_seconds": 0.5,
    "reservation_heartbeat_seconds": 0.3,
}
REQUEST = {"model": "gpt-4o-mini", "messages": [{"role": "user", "content": "hi"}]}
ESTIMATE_MICRO = 616  # 8 in + 1024 (default max_tokens) out on gpt-4o-mini
USAGE_EVENT = b'data: {"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":20}}\n\n'
USAGE_MICRO = 14  # 10 × 0.15 + 20 × 0.60 per 1M tokens = 13.5 → 14
CONTENT_EVENT = b'data: {"choices":[{"index":0,"delta":{"content":"tick "}}]}\n\n'


def ident(run: str = "r1", agent: str | None = None, team: str | None = None, budget=None):
    return Identity(run, agent, team, budget, run_id_generated=False)


def fields(r: redis.Redis, key: str) -> dict[str, int]:
    return {k: int(v) for k, v in r.hgetall(key).items()}


def expire_now(r: redis.Redis, resv: Reservation) -> None:
    """Move a reservation's expiry into the past (what waiting out the TTL does)."""
    r.zadd(RESERVATIONS_KEY, {resv.id: 0})


async def _client(**settings: object) -> BudgetClient:
    return BudgetClient(aredis.from_url(TEST_REDIS_URL), make_settings(**settings))


@pytest.fixture
async def budget(redis_db: redis.Redis) -> AsyncIterator[BudgetClient]:
    client = await _client(budgets=BUDGETS)
    yield client
    await client._redis.aclose()


@pytest.fixture
async def short_budget(redis_db: redis.Redis) -> AsyncIterator[BudgetClient]:
    client = await _client(budgets=BUDGETS, **SHORT_TTL)
    yield client
    await client._redis.aclose()


# --- settings -------------------------------------------------------------------------


def test_heartbeat_must_be_shorter_than_ttl() -> None:
    with pytest.raises(ValueError, match="reservation_heartbeat_seconds"):
        Settings(reservation_ttl_seconds=3_600, reservation_heartbeat_seconds=3_600)
    defaults = Settings()
    assert (defaults.sweep_interval_seconds, defaults.reservation_heartbeat_seconds) == (30, 60)


# --- sweep ----------------------------------------------------------------------------


async def test_sweep_releases_expired_at_all_levels(
    budget: BudgetClient, redis_db: redis.Redis
) -> None:
    expired = await budget.reserve(ident(agent="bot", team="core"), 100)
    live = await budget.reserve(ident(run="r2", agent="bot", team="core"), 30)
    assert isinstance(expired, Reservation) and isinstance(live, Reservation)
    expire_now(redis_db, expired)

    result = await budget.sweep()
    assert (result.released, result.examined) == (1, 1)
    assert fields(redis_db, run_key("r1")) == {"limit": 1_000, "reserved": 0}
    levels = {lv.name: lv.key for lv in expired.levels}
    assert fields(redis_db, levels["agent"])["reserved"] == 30  # only `live` is left
    assert fields(redis_db, levels["team"])["reserved"] == 30
    assert not redis_db.exists(expired.key)
    assert redis_db.zrange(RESERVATIONS_KEY, 0, -1) == [live.id]

    # Nothing left to sweep; the late settle of the swept call adds its cost only.
    assert (await budget.sweep()).released == 0
    assert await budget.settle(expired, 40) is SettleResult.GONE
    assert fields(redis_db, run_key("r1")) == {"limit": 1_000, "spent": 40, "reserved": 0}


async def test_sweep_waits_for_the_ttl(short_budget: BudgetClient, redis_db: redis.Redis) -> None:
    resv = await short_budget.reserve(ident(), 100)
    assert isinstance(resv, Reservation)
    assert (await short_budget.sweep()).released == 0  # not expired yet
    await asyncio.sleep(1.1)
    assert (await short_budget.sweep()).released == 1
    assert fields(redis_db, run_key("r1"))["reserved"] == 0


async def test_sweep_works_in_batches(budget: BudgetClient, redis_db: redis.Redis) -> None:
    for i in range(250):
        resv = await budget.reserve(ident(run=f"b{i}"), 1)
        assert isinstance(resv, Reservation)
        expire_now(redis_db, resv)
    first = await budget.sweep()
    assert (first.released, first.examined) == (100, 100)
    assert await sweep_once(budget) == 150  # keeps going until a batch comes back short
    assert redis_db.zcard(RESERVATIONS_KEY) == 0


async def test_sweep_does_not_recreate_expired_level_keys(
    budget: BudgetClient, redis_db: redis.Redis
) -> None:
    resv = await budget.reserve(ident(agent="bot"), 100)
    assert isinstance(resv, Reservation)
    expire_now(redis_db, resv)
    agent = resv.levels[1].key
    redis_db.delete(agent)  # the period key already expired
    assert (await budget.sweep()).released == 1
    assert not redis_db.exists(agent)  # not re-created as {reserved: 0} without a TTL
    assert fields(redis_db, run_key("r1"))["reserved"] == 0


async def test_concurrent_sweep_and_settle_release_once(
    budget: BudgetClient, redis_db: redis.Redis
) -> None:
    # A live reservation (30) shares every level with 20 expired ones (100 each). Were any
    # expired one released twice, `reserved` would drop below 30 (or be clamped to 0).
    who = ident(agent="bot", team="core", budget=10_000)
    live = await budget.reserve(who, 30)
    assert isinstance(live, Reservation)
    expired = []
    for _ in range(20):
        resv = await budget.reserve(who, 100)
        assert isinstance(resv, Reservation)
        expire_now(redis_db, resv)
        expired.append(resv)

    results = await asyncio.gather(
        *(budget.settle(r, 7) for r in expired),
        *(budget.sweep(batch=3) for _ in range(10)),
        *(budget.settle(r, 7) for r in expired),  # and every settle a second time
    )
    first, sweeps, second = results[:20], results[20:30], results[30:]
    released_by_settle = sum(r is SettleResult.RELEASED for r in first + second)
    released_by_sweep = sum(r.released for r in sweeps)
    assert released_by_settle + released_by_sweep == 20
    for key in (lv.key for lv in live.levels):
        state = fields(redis_db, key)
        assert state["reserved"] == 30, key
        # Each call's cost is counted once: a duplicate settle is a no-op, while a settle
        # after a sweep (GONE) still counts the cost that the call really had.
        counted = sum(r is not SettleResult.DUPLICATE for r in first + second)
        assert counted == 20
        assert state["spent"] == 20 * 7, key
    assert redis_db.zrange(RESERVATIONS_KEY, 0, -1) == [live.id]


async def test_sweep_after_settle_finds_nothing(
    budget: BudgetClient, redis_db: redis.Redis
) -> None:
    resv = await budget.reserve(ident(), 100)
    assert isinstance(resv, Reservation)
    assert await budget.settle(resv, 10) is SettleResult.RELEASED
    expire_now(redis_db, resv)  # even if an index entry somehow lingered
    redis_db.zrem(RESERVATIONS_KEY, resv.id)
    assert (await budget.sweep()).released == 0
    assert fields(redis_db, run_key("r1")) == {"limit": 1_000, "spent": 10, "reserved": 0}


# --- extend ---------------------------------------------------------------------------


async def test_extend_outlives_original_ttl(
    short_budget: BudgetClient, redis_db: redis.Redis
) -> None:
    resv = await short_budget.reserve(ident(), 100)
    assert isinstance(resv, Reservation)
    for _ in range(3):  # 3 × 0.6 s = 1.8 s, well past the 1 s TTL
        await asyncio.sleep(0.6)
        assert await short_budget.extend(resv) is True
        assert (await short_budget.sweep()).released == 0
    assert fields(redis_db, run_key("r1"))["reserved"] == 100
    assert await short_budget.settle(resv, 10) is SettleResult.RELEASED


async def test_extend_never_shortens(budget: BudgetClient, redis_db: redis.Redis) -> None:
    resv = await budget.reserve(ident(), 100)
    assert isinstance(resv, Reservation)
    far = int(time.time() * 1000) + 10 * 3_600_000
    redis_db.zadd(RESERVATIONS_KEY, {resv.id: far})
    assert await budget.extend(resv) is True
    assert redis_db.zscore(RESERVATIONS_KEY, resv.id) == far


async def test_extend_on_swept_or_settled_returns_false(
    budget: BudgetClient, redis_db: redis.Redis
) -> None:
    swept = await budget.reserve(ident(), 100)
    settled = await budget.reserve(ident(), 100)
    assert isinstance(swept, Reservation) and isinstance(settled, Reservation)
    expire_now(redis_db, swept)
    assert (await budget.sweep()).released == 1
    assert await budget.extend(swept) is False
    await budget.settle(settled, 5)
    assert await budget.extend(settled) is False
    assert redis_db.zcard(RESERVATIONS_KEY) == 0  # extend never re-adds an id


# --- through the proxy ----------------------------------------------------------------


class SlowStream(httpx.AsyncByteStream):
    """An upstream stream that outlives the reservation TTL, then sends usage + [DONE].

    Partway through (after ``check_after`` seconds) it records the run's budget state.
    """

    def __init__(self, r: redis.Redis, run_id: str, seconds: float, check_after: float):
        self.r, self.run_id, self.seconds, self.check_after = r, run_id, seconds, check_after
        self.mid: dict[str, int] | None = None

    async def __aiter__(self) -> AsyncIterator[bytes]:
        start = time.monotonic()
        while (elapsed := time.monotonic() - start) < self.seconds:
            if self.mid is None and elapsed >= self.check_after:
                self.mid = fields(self.r, run_key(self.run_id))
            yield CONTENT_EVENT
            await anyio.sleep(0.1)  # well under the 0.5 s upstream timeout
        yield USAGE_EVENT
        yield b"data: [DONE]\n\n"


@pytest.mark.parametrize("heartbeat", [True, False])
def test_long_stream_keeps_its_reservation(
    redis_db: redis.Redis, monkeypatch: pytest.MonkeyPatch, heartbeat: bool
) -> None:
    if not heartbeat:  # control: prove the sweeper would take it otherwise

        async def no_heartbeat(*args: object) -> None:
            await anyio.sleep_forever()

        monkeypatch.setattr(chat, "_heartbeat", no_heartbeat)
    stream = SlowStream(redis_db, "long", seconds=2.5, check_after=2.0)  # TTL is 1 s
    transport = httpx.MockTransport(
        lambda req: httpx.Response(
            200, headers={"content-type": "text/event-stream"}, stream=stream
        )
    )
    settings = make_settings("http://up/v1", sweep_interval_seconds=0.1, **SHORT_TTL)
    with TestClient(create_app(settings, upstream_transport=transport)) as c:
        resp = c.post(
            "/v1/chat/completions",
            json={**REQUEST, "stream": True},
            headers={"X-AM-Run-Id": "long"},
        )
    assert resp.status_code == 200 and resp.text.endswith("data: [DONE]\n\n")
    assert stream.mid is not None
    if heartbeat:
        assert stream.mid["reserved"] == ESTIMATE_MICRO  # still held at 2 s
    else:
        assert stream.mid["reserved"] == 0  # swept after 1 s
    # Either way the call ends settled at its real cost with nothing left reserved.
    assert fields(redis_db, run_key("long")) == {
        "limit": 500_000,
        "spent": USAGE_MICRO,
        "reserved": 0,
    }
    assert redis_db.zcard(RESERVATIONS_KEY) == 0


async def test_client_disconnect_without_usage_settles_estimate(redis_db: redis.Redis) -> None:
    class Endless(httpx.AsyncByteStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            while True:
                yield CONTENT_EVENT
                await anyio.sleep(0.01)

    transport = httpx.MockTransport(
        lambda req: httpx.Response(
            200, headers={"content-type": "text/event-stream"}, stream=Endless()
        )
    )
    app = create_app(make_settings("http://up/v1"), upstream_transport=transport)
    body = json.dumps({**REQUEST, "stream": True}).encode()
    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.4"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/v1/chat/completions",
        "raw_path": b"/v1/chat/completions",
        "root_path": "",
        "query_string": b"",
        "headers": [(b"content-type", b"application/json"), (b"x-am-run-id", b"gone")],
        "client": ("test", 1),
        "server": ("test", 80),
    }
    sent_request = False
    chunks = 0

    async def receive() -> dict:
        nonlocal sent_request
        if not sent_request:
            sent_request = True
            return {"type": "http.request", "body": body, "more_body": False}
        await anyio.sleep_forever()
        raise AssertionError("unreachable")

    async def send(message: dict) -> None:
        nonlocal chunks
        if message["type"] == "http.response.body":
            chunks += 1
            if chunks >= 3:  # the client reads a little, then goes away
                raise OSError("client went away")

    async with app.router.lifespan_context(app):
        with anyio.fail_after(5), pytest.raises(Exception):  # noqa: B017 (ClientDisconnect)
            await app(scope, receive, send)

    assert chunks >= 3
    # Some content was streamed but no usage arrived: the worst case is billed.
    assert fields(redis_db, run_key("gone")) == {
        "limit": 500_000,
        "spent": ESTIMATE_MICRO,
        "reserved": 0,
    }


def test_stream_cost_on_disconnect_is_the_estimate() -> None:
    from app import proxy
    from app.estimate import Estimate
    from app.pricing import PriceTable

    ctx = chat.CallContext(
        ident(),
        "gpt-4o-mini",
        Estimate(8, 1024, ESTIMATE_MICRO),
        PriceTable(make_settings().pricing),
    )
    gone = proxy.StreamResult(client_disconnected=True, streamed_output_tokens=3)
    assert ctx.stream_cost(gone) == (ESTIMATE_MICRO, "estimate:client_disconnected")
    gone_with_usage = proxy.StreamResult(
        usage={"prompt_tokens": 10, "completion_tokens": 20}, client_disconnected=True
    )
    assert ctx.stream_cost(gone_with_usage) == (USAGE_MICRO, "usage")
    cut = proxy.StreamResult(error="ReadError", streamed_output_tokens=3)
    assert ctx.stream_cost(cut)[1] == "tiktoken"


def test_lifespan_sweeper_reclaims_crashed_calls(redis_db: redis.Redis) -> None:
    async def crashed_call() -> Reservation:
        client = await _client(budgets=BUDGETS)
        try:
            resv = await client.reserve(ident(run="crash"), 300)
            assert isinstance(resv, Reservation)
            return resv
        finally:
            await client._redis.aclose()

    resv = asyncio.run(crashed_call())  # reserved, then the "replica" died
    expire_now(redis_db, resv)
    assert fields(redis_db, run_key("crash"))["reserved"] == 300
    settings = make_settings(budgets=BUDGETS, sweep_interval_seconds=0.05)
    with TestClient(create_app(settings)):
        deadline = time.monotonic() + 5
        while fields(redis_db, run_key("crash"))["reserved"] and time.monotonic() < deadline:
            time.sleep(0.02)
    assert fields(redis_db, run_key("crash"))["reserved"] == 0
    assert redis_db.zcard(RESERVATIONS_KEY) == 0


class LosesReservation(httpx.AsyncByteStream):
    """Streams for a while; partway through, the reservation is reclaimed behind its back
    (what a sweep on another replica does after a long Redis outage on this one)."""

    def __init__(self, r: redis.Redis, run_id: str):
        self.r, self.run_id, self.reclaimed = r, run_id, False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        start = time.monotonic()
        while time.monotonic() - start < 3:
            if not self.reclaimed and time.monotonic() - start > 0.4:
                (resv_id,) = self.r.zrange(RESERVATIONS_KEY, 0, -1)
                self.r.zrem(RESERVATIONS_KEY, resv_id)
                self.r.delete(f"am:r:{resv_id}")
                self.r.hset(run_key(self.run_id), "reserved", 0)
                self.reclaimed = True
            yield CONTENT_EVENT
            await anyio.sleep(0.05)
        yield USAGE_EVENT
        yield b"data: [DONE]\n\n"


def test_stream_ends_when_reservation_is_lost(redis_db: redis.Redis) -> None:
    from app.estimate import count_text_tokens

    stream = LosesReservation(redis_db, "lost")
    transport = httpx.MockTransport(
        lambda req: httpx.Response(
            200, headers={"content-type": "text/event-stream"}, stream=stream
        )
    )
    settings = make_settings("http://up/v1", **SHORT_TTL)  # heartbeat every 0.3 s
    started = time.monotonic()
    with TestClient(create_app(settings, upstream_transport=transport)) as c:
        resp = c.post(
            "/v1/chat/completions",
            json={**REQUEST, "stream": True},
            headers={"X-AM-Run-Id": "lost"},
        )
    assert time.monotonic() - started < 2.5  # ended early, not after the 3 s upstream
    assert stream.reclaimed
    last = json.loads(resp.text.strip().splitlines()[-1][6:])
    assert last["error"]["type"] == "agentmeter_reservation_lost"
    assert last["error"]["code"] == "reservation_lost"
    assert "data: [DONE]" not in resp.text
    # Settled normally (no usage arrived): estimated input + the relayed output tokens.
    relayed = resp.text.count('"content":"tick "')
    out = relayed * count_text_tokens("gpt-4o-mini", "tick ")
    expected = -(-(8 * 150_000 + out * 600_000) // 1_000_000)
    assert fields(redis_db, run_key("lost")) == {"limit": 500_000, "spent": expected, "reserved": 0}


def test_failed_settle_logs_error(
    redis_db: redis.Redis, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    async def broken_settle(self, reservation, actual, details=None):
        raise redis.ConnectionError("redis went away")

    monkeypatch.setattr(BudgetClient, "settle", broken_settle)
    from mock_upstream.main import app as mock_app

    app = create_app(make_settings(), upstream_transport=httpx.ASGITransport(app=mock_app))
    with caplog.at_level("INFO", logger="agentmeter"), TestClient(app) as c:
        resp = c.post("/v1/chat/completions", json=REQUEST, headers={"X-AM-Run-Id": "nosettle"})
    assert resp.status_code == 200  # the caller still gets its answer
    errors = [r for r in caplog.records if r.levelname == "ERROR"]
    assert len(errors) == 1
    assert "cost NOT recorded" in errors[0].getMessage()
    assert "actual_micro=14" in errors[0].getMessage()
    assert errors[0].exc_info is not None
