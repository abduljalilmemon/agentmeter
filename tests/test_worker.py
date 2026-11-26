"""Alert worker (worker/alerts.py): am:events -> webhook, at least once, ack after 2xx."""

import asyncio
import json
from collections.abc import AsyncIterator, Callable

import httpx
import pytest
import redis
import redis.asyncio as aredis
from fastapi.testclient import TestClient

from app.events import ALERTS_GROUP, STREAM_KEY
from app.main import create_app
from mock_upstream.main import app as mock_app
from tests.conftest import TEST_REDIS_URL, make_settings
from worker.alerts import AlertWorker

pytestmark = pytest.mark.usefixtures("redis_db")

HOOK = "http://hooks.test/slack"
IDLE = 0.2  # retry_idle_seconds for these tests

THRESHOLD = {
    "type": "threshold_crossed",
    "level": "run",
    "key": "am:b:run:r1",
    "threshold_bp": "5000",
    "spent": "500",
    "limit": "1000",
    "run": "r1",
    "agent": "",
    "team": "",
}
BREAKER = {
    "type": "breaker_open",
    "run": "r1",
    "agent": "",
    "team": "",
    "reason": "loop",
    "source": "request_repeats",
    "steps": "3",
    "spent": "28",
}
SETTLED = {"type": "call_settled", "run": "r1", "cost_micro": "14", "reservation": "x"}


class Hook:
    """A fake Slack webhook. ``statuses`` scripts the answers; then it answers 200."""

    def __init__(self, statuses: list[int | Exception] | None = None) -> None:
        self.statuses = list(statuses or [])
        self.attempts: list[str] = []  # every text POSTed, successful or not
        self.delivered: list[str] = []  # texts that got a 2xx

    def __call__(self, request: httpx.Request) -> httpx.Response:
        assert request.url == HOOK
        text = json.loads(request.content)["text"]
        self.attempts.append(text)
        outcome = self.statuses.pop(0) if self.statuses else 200
        if isinstance(outcome, Exception):
            raise outcome
        if 200 <= outcome < 300:
            self.delivered.append(text)
        return httpx.Response(outcome, text="ok")


@pytest.fixture
async def make_worker() -> AsyncIterator[Callable[..., AlertWorker]]:
    clients: list = []

    def make(hook: Hook | None, consumer: str = "w1", webhook: str | None = HOOK) -> AlertWorker:
        client = aredis.from_url(TEST_REDIS_URL, decode_responses=True)
        transport = httpx.MockTransport(hook or Hook())
        http = httpx.AsyncClient(transport=transport)
        clients.extend([client, http])
        alerts = {"webhook_url": webhook or "", "retry_idle_seconds": IDLE}
        return AlertWorker(client, make_settings(alerts=alerts), http, consumer)

    yield make
    for c in clients:
        await c.aclose()


def pending(r: redis.Redis) -> int:
    return r.xpending(STREAM_KEY, ALERTS_GROUP)["pending"]


async def test_posts_each_alert_once_and_acks_the_rest(redis_db: redis.Redis, make_worker) -> None:
    for fields in (THRESHOLD, SETTLED, BREAKER, SETTLED):
        redis_db.xadd(STREAM_KEY, fields)
    hook = Hook()
    worker = make_worker(hook)
    await worker.ensure_group()
    assert await worker.run_once() == 4
    assert len(hook.delivered) == 2
    assert "run budget `run:r1` crossed 50%" in hook.delivered[0]
    assert "circuit breaker OPEN for run `r1`" in hook.delivered[1]
    assert pending(redis_db) == 0  # all four acked, call_settled without a post
    assert await worker.run_once() == 0
    assert len(hook.attempts) == 2


async def test_failed_post_is_retried_and_acked_only_after_2xx(
    redis_db: redis.Redis, make_worker
) -> None:
    redis_db.xadd(STREAM_KEY, THRESHOLD)
    hook = Hook([500, 503])
    worker = make_worker(hook)
    await worker.ensure_group()

    await worker.run_once()
    assert (len(hook.attempts), hook.delivered, pending(redis_db)) == (1, [], 1)
    await worker.run_once()  # not idle long enough yet: no retry
    assert len(hook.attempts) == 1

    await asyncio.sleep(IDLE + 0.05)
    await worker.run_once()  # second attempt: 503, still pending
    assert (len(hook.attempts), pending(redis_db)) == (2, 1)

    await asyncio.sleep(IDLE + 0.05)
    await worker.run_once()  # third attempt: 200, acked
    assert (len(hook.attempts), len(hook.delivered), pending(redis_db)) == (3, 1, 0)
    await asyncio.sleep(IDLE + 0.05)
    await worker.run_once()
    assert len(hook.attempts) == 3  # nothing left to retry


async def test_connection_error_is_retried(redis_db: redis.Redis, make_worker) -> None:
    redis_db.xadd(STREAM_KEY, BREAKER)
    hook = Hook([httpx.ConnectError("refused")])
    worker = make_worker(hook)
    await worker.ensure_group()
    await worker.run_once()
    assert pending(redis_db) == 1
    await asyncio.sleep(IDLE + 0.05)
    await worker.run_once()
    assert (len(hook.delivered), pending(redis_db)) == (1, 0)


async def test_no_duplicate_posts_after_restart(redis_db: redis.Redis, make_worker) -> None:
    for fields in (THRESHOLD, BREAKER):
        redis_db.xadd(STREAM_KEY, fields)
    first = Hook()
    worker = make_worker(first)
    await worker.ensure_group()
    await worker.run_once()
    assert len(first.delivered) == 2

    # Restart: same consumer name (same host) and a brand-new one (another replica).
    after = Hook()
    for name in ("w1", "w2"):
        restarted = make_worker(after, consumer=name)
        await restarted.ensure_group()  # group exists: no reset to the start
        await asyncio.sleep(IDLE + 0.05)
        await restarted.run_once()
    assert after.attempts == []


async def test_crash_before_ack_is_delivered_by_the_next_worker(
    redis_db: redis.Redis, make_worker
) -> None:
    redis_db.xadd(STREAM_KEY, THRESHOLD)
    hook = Hook([500])
    crashed = make_worker(hook, consumer="w1")
    await crashed.ensure_group()
    await crashed.run_once()  # delivered to w1, post failed, w1 "dies" with it pending

    other = make_worker(hook, consumer="w2")
    await other.run_once()
    assert len(hook.attempts) == 1  # not claimed while still fresh
    await asyncio.sleep(IDLE + 0.05)
    await other.run_once()
    assert (len(hook.attempts), len(hook.delivered), pending(redis_db)) == (2, 1, 0)


async def test_without_webhook_alerts_are_logged_and_acked(
    redis_db: redis.Redis, make_worker, caplog: pytest.LogCaptureFixture
) -> None:
    redis_db.xadd(STREAM_KEY, THRESHOLD)
    worker = make_worker(None, webhook=None)
    await worker.ensure_group()
    with caplog.at_level("WARNING", logger="agentmeter.worker"):
        await worker.run_once()
    assert pending(redis_db) == 0
    assert any("crossed 50%" in r.getMessage() for r in caplog.records)


async def test_group_is_created_with_mkstream_and_reads_from_the_start(
    redis_db: redis.Redis, make_worker
) -> None:
    worker = make_worker(Hook())
    await worker.ensure_group()  # no stream yet: MKSTREAM
    assert redis_db.exists(STREAM_KEY)
    await worker.ensure_group()  # idempotent (BUSYGROUP)
    assert redis_db.xinfo_groups(STREAM_KEY)[0]["name"] == ALERTS_GROUP

    redis_db.delete(STREAM_KEY)
    redis_db.xadd(STREAM_KEY, BREAKER)  # emitted before any worker ever ran
    hook = Hook()
    late = make_worker(hook)
    await late.ensure_group()
    await late.run_once()
    assert len(hook.delivered) == 1


async def test_entry_trimmed_while_pending_is_dropped(redis_db: redis.Redis, make_worker) -> None:
    entry = redis_db.xadd(STREAM_KEY, THRESHOLD)
    hook = Hook([500])
    worker = make_worker(hook)
    await worker.ensure_group()
    await worker.run_once()
    redis_db.xdel(STREAM_KEY, entry)  # what MAXLEN trimming does to an old entry
    await asyncio.sleep(IDLE + 0.05)
    await worker.run_once()  # XAUTOCLAIM reports it deleted; nothing to post
    assert len(hook.attempts) == 1
    assert pending(redis_db) == 0


def test_end_to_end_proxy_to_webhook(redis_db: redis.Redis) -> None:
    """Calls through the proxy cross 50/75/90 of a run budget; the worker posts 3."""
    app = create_app(make_settings(), upstream_transport=httpx.ASGITransport(app=mock_app))
    headers = {"X-AM-Run-Id": "e2e", "X-AM-Run-Budget": "0.00007"}  # 70 micro, 5 calls of 14
    body = {
        "model": "gpt-4o-mini",
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 10,
    }  # worst case 8 micro, so all 5 fit
    with TestClient(app) as c:
        statuses = [
            c.post("/v1/chat/completions", json=body, headers=headers).status_code for _ in range(6)
        ]
    assert statuses == [200] * 5 + [429]

    async def deliver() -> Hook:
        hook = Hook()
        client = aredis.from_url(TEST_REDIS_URL, decode_responses=True)
        async with httpx.AsyncClient(transport=httpx.MockTransport(hook)) as http:
            worker = AlertWorker(client, make_settings(alerts={"webhook_url": HOOK}), http, "e2e")
            await worker.ensure_group()
            await worker.run_once()
        await client.aclose()
        return hook

    hook = asyncio.run(deliver())
    assert len(hook.delivered) == 3
    assert [t.split("crossed ")[1].split(":")[0] for t in hook.delivered] == ["50%", "75%", "90%"]


async def test_webhook_text_carries_the_stream_id(redis_db: redis.Redis, make_worker) -> None:
    entry = redis_db.xadd(STREAM_KEY, THRESHOLD)
    hook = Hook()
    worker = make_worker(hook)
    await worker.ensure_group()
    await worker.run_once()
    assert hook.delivered[0].endswith(f"(event {entry})")


async def test_poison_alert_is_dead_lettered_after_max_attempts(
    redis_db: redis.Redis, make_worker, caplog: pytest.LogCaptureFixture
) -> None:
    entry = redis_db.xadd(STREAM_KEY, THRESHOLD)
    good = redis_db.xadd(STREAM_KEY, BREAKER)
    hook = Hook()  # scripted per round below: THRESHOLD always fails, BREAKER succeeds
    worker = make_worker(hook)
    worker._settings = make_settings(
        alerts={"webhook_url": HOOK, "retry_idle_seconds": IDLE, "max_attempts": 3}
    )
    await worker.ensure_group()
    # Delivery 1 is the first read; deliveries 2 and 3 are XAUTOCLAIM retries.
    hook.statuses = [500, 200]  # attempt 1 of THRESHOLD fails, BREAKER is delivered
    await worker.run_once()
    for _ in range(2):
        hook.statuses = [500]
        await asyncio.sleep(IDLE + 0.05)
        await worker.run_once()
    assert len([t for t in hook.attempts if "crossed 50%" in t]) == 3
    assert redis_db.xlen("am:alerts:dead") == 0

    # Delivery 4 exceeds max_attempts=3: dead-lettered without another post.
    await asyncio.sleep(IDLE + 0.05)
    with caplog.at_level("ERROR", logger="agentmeter.worker"):
        await worker.run_once()
    assert len([t for t in hook.attempts if "crossed 50%" in t]) == 3
    assert pending(redis_db) == 0
    assert worker.dead == 1
    ((_dead_id, dead),) = redis_db.xrange("am:alerts:dead")
    assert dead == {**THRESHOLD, "event_id": entry, "attempts": "3"}
    errors = [r for r in caplog.records if r.levelname == "ERROR"]
    assert len(errors) == 1 and entry in errors[0].getMessage()
    assert len(hook.delivered) == 1 and hook.delivered[0].endswith(f"(event {good})")

    await asyncio.sleep(IDLE + 0.05)
    await worker.run_once()  # gone for good: no more attempts
    assert len(hook.attempts) == 4


async def test_worker_metrics(redis_db: redis.Redis, make_worker) -> None:
    redis_db.xadd(STREAM_KEY, THRESHOLD)
    redis_db.xadd(STREAM_KEY, BREAKER)
    redis_db.xadd(STREAM_KEY, SETTLED)
    hook = Hook([200, 500])  # THRESHOLD delivered, BREAKER fails every time from here
    worker = make_worker(hook)
    worker._settings = make_settings(
        alerts={"webhook_url": HOOK, "retry_idle_seconds": IDLE, "max_attempts": 1}
    )
    await worker.ensure_group()
    await worker.run_once()
    await asyncio.sleep(IDLE + 0.05)
    await worker.run_once()  # BREAKER's 2nd delivery exceeds max_attempts=1

    def value(name: str) -> float:
        return worker.metrics.registry.get_sample_value(name) or 0.0

    assert value("am_alerts_delivered_total") == 1  # call_settled is acked, not "delivered"
    assert value("am_alerts_dead_total") == 1
