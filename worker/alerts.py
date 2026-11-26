"""Alert worker (PLAN §3.4): am:events → Slack-compatible webhook.

Run with ``python -m worker.alerts``. It reads ``am:events`` in the consumer group
``am:alerts`` (created with MKSTREAM from the stream's start if missing, so alerts emitted
before the worker first ran are still delivered):

- ``threshold_crossed`` and ``breaker_open`` are POSTed as ``{"text": ...}`` to
  ``alerts.webhook_url``. The entry is XACKed only after a 2xx. Otherwise it stays
  pending and is retried, by this or any other worker, once it has been idle for
  ``alerts.retry_idle_seconds`` (XAUTOCLAIM). Delivery is at least once.
- Every other event type is acked without posting.
- With no ``webhook_url``, alerts are logged at WARNING and acked.
- An alert delivered more than ``alerts.max_attempts`` times (XPENDING) is logged at
  ERROR, copied to the ``am:alerts:dead`` stream and acked: a poison alert stops
  being retried.

Any number of workers can share the group: each entry goes to one of them.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
from typing import Any

import httpx
import redis.asyncio as redis
from prometheus_client import start_http_server

from app.config import Settings, load_settings
from app.events import (
    ALERT_TYPES,
    ALERTS_GROUP,
    DEAD_LETTER_KEY,
    STREAM_KEY,
    alert_text,
    decode,
)
from app.metrics import WorkerMetrics

log = logging.getLogger("agentmeter.worker")

BATCH = 50
BLOCK_MS = 5_000


class AlertWorker:
    def __init__(
        self,
        client: redis.Redis,
        settings: Settings,
        http: httpx.AsyncClient,
        consumer: str,
        metrics: WorkerMetrics | None = None,
    ) -> None:
        self.metrics = metrics or WorkerMetrics()
        self._redis = client
        self._settings = settings
        self._http = http
        self.consumer = consumer
        self._claim_cursor = "0-0"
        self.posted = 0  # webhook posts that got a 2xx (for logs and tests)
        self.dead = 0  # alerts dead-lettered after max_attempts

    async def ensure_group(self) -> None:
        try:
            await self._redis.xgroup_create(STREAM_KEY, ALERTS_GROUP, id="0", mkstream=True)
            log.info("created consumer group %s on %s", ALERTS_GROUP, STREAM_KEY)
        except redis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def run_once(self, block_ms: int | None = None) -> int:
        """Retry idle pending entries, then read new ones. Returns entries handled."""
        handled = 0
        min_idle = int(self._settings.alerts.retry_idle_seconds * 1000)
        cursor, claimed, *_ = await self._redis.xautoclaim(
            STREAM_KEY, ALERTS_GROUP, self.consumer, min_idle, self._claim_cursor, count=BATCH
        )
        self._claim_cursor = _s(cursor)
        live = [(_s(i), f) for i, f in claimed if f is not None]  # None: trimmed by MAXLEN
        deliveries = await self._delivery_counts([i for i, _ in live])
        for entry_id, fields in live:
            if deliveries.get(entry_id, 0) > self._settings.alerts.max_attempts:
                await self._dead_letter(entry_id, decode(fields), deliveries[entry_id])
            else:
                await self._handle(entry_id, decode(fields), retry=True)
            handled += 1

        response = await self._redis.xreadgroup(
            ALERTS_GROUP, self.consumer, {STREAM_KEY: ">"}, count=BATCH, block=block_ms
        )
        for _stream, entries in response or []:
            for entry_id, fields in entries:
                await self._handle(_s(entry_id), decode(fields), retry=False)
                handled += 1
        return handled

    async def run_forever(self) -> None:
        await self.ensure_group()
        log.info("alert worker %s started (webhook %s)", self.consumer, self._webhook_state())
        while True:
            try:
                await self.run_once(block_ms=BLOCK_MS)
            except (redis.RedisError, OSError) as exc:
                log.warning("worker loop error: %r", exc)
                await asyncio.sleep(1)

    async def _delivery_counts(self, entry_ids: list[str]) -> dict[str, int]:
        """times_delivered for claimed entries (XAUTOCLAIM has just incremented it)."""
        if not entry_ids:
            return {}
        rows = await self._redis.xpending_range(
            STREAM_KEY,
            ALERTS_GROUP,
            min=entry_ids[0],
            max=entry_ids[-1],
            count=len(entry_ids),
            consumername=self.consumer,
        )
        return {_s(row["message_id"]): int(row["times_delivered"]) for row in rows}

    async def _dead_letter(self, entry_id: str, fields: dict[str, str], deliveries: int) -> None:
        attempts = deliveries - 1  # this delivery is not attempted
        log.error(
            "alert %s dead-lettered after %d failed attempts: %s",
            entry_id,
            attempts,
            alert_text(fields, entry_id),
        )
        # Copy and ack in one MULTI, so a crash can't leave it acked but not dead-lettered.
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.xadd(
                DEAD_LETTER_KEY,
                {**fields, "event_id": entry_id, "attempts": attempts},
                maxlen=self._settings.events.stream_maxlen,
                approximate=True,
            )
            pipe.xack(STREAM_KEY, ALERTS_GROUP, entry_id)
            await pipe.execute()
        self.dead += 1
        self.metrics.dead.inc()

    def _webhook_state(self) -> str:
        return "set" if self._settings.alerts.webhook_url else "not set: alerts are logged"

    async def _ack(self, entry_id: str) -> None:
        await self._redis.xack(STREAM_KEY, ALERTS_GROUP, entry_id)

    async def _handle(self, entry_id: str, fields: dict[str, str], *, retry: bool) -> None:
        if fields.get("type") not in ALERT_TYPES:
            await self._ack(entry_id)
            return
        text = alert_text(fields, entry_id)
        url = self._settings.alerts.webhook_url
        if not url:
            log.warning("ALERT (no webhook_url) %s: %s", entry_id, text)
            await self._ack(entry_id)
            return
        try:
            resp = await self._http.post(url, json={"text": text})
        except httpx.HTTPError as exc:
            log.warning("webhook post failed for %s (will retry): %r", entry_id, exc)
            return
        if not resp.is_success:
            log.warning("webhook answered %d for %s (will retry)", resp.status_code, entry_id)
            return
        await self._ack(entry_id)
        self.posted += 1
        self.metrics.delivered.inc()
        log.info("alert %s delivered%s: %s", entry_id, " on retry" if retry else "", text)


def _s(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def consumer_name() -> str:
    # Stable across restarts of the same container, so its own pending entries are
    # picked up again; another worker's are claimed once idle.
    return os.environ.get("AGENTMETER_WORKER_NAME") or socket.gethostname()


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    settings = load_settings()
    client = redis.from_url(settings.redis_url, decode_responses=True)
    metrics = WorkerMetrics()
    start_http_server(settings.worker_metrics_port, registry=metrics.registry)
    log.info("worker metrics on :%d/metrics", settings.worker_metrics_port)
    timeout = settings.alerts.webhook_timeout_seconds
    async with httpx.AsyncClient(timeout=timeout) as http:
        try:
            worker = AlertWorker(client, settings, http, consumer_name(), metrics)
            await worker.run_forever()
        finally:
            await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
