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

from app.config import Settings, load_settings
from app.events import ALERT_TYPES, ALERTS_GROUP, STREAM_KEY, alert_text, decode

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
    ) -> None:
        self._redis = client
        self._settings = settings
        self._http = http
        self.consumer = consumer
        self._claim_cursor = "0-0"
        self.posted = 0  # webhook posts that got a 2xx (for logs and tests)

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
        for entry_id, fields in claimed:
            if fields is not None:  # None: trimmed by MAXLEN while pending
                await self._handle(_s(entry_id), decode(fields), retry=True)
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

    def _webhook_state(self) -> str:
        return "set" if self._settings.alerts.webhook_url else "not set: alerts are logged"

    async def _ack(self, entry_id: str) -> None:
        await self._redis.xack(STREAM_KEY, ALERTS_GROUP, entry_id)

    async def _handle(self, entry_id: str, fields: dict[str, str], *, retry: bool) -> None:
        if fields.get("type") not in ALERT_TYPES:
            await self._ack(entry_id)
            return
        text = alert_text(fields)
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
    timeout = settings.alerts.webhook_timeout_seconds
    async with httpx.AsyncClient(timeout=timeout) as http:
        try:
            await AlertWorker(client, settings, http, consumer_name()).run_forever()
        finally:
            await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
