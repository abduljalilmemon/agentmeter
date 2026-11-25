"""Background sweeper: releases reservations that expired without a settle (PLAN §3.1).

A call that crashes, or a replica that dies mid-call, leaves its reservation in
``reserved``. Every ``sweep_interval_seconds`` each replica runs sweep.lua, which
atomically takes up to SWEEP_BATCH expired ids and releases them at every level.
Replicas may sweep at the same time: each id is released by exactly one script call.
"""

from __future__ import annotations

import asyncio
import logging

import redis.asyncio as redis

from app.budget import SWEEP_BATCH, BudgetClient

log = logging.getLogger("agentmeter")

MAX_BATCHES_PER_TICK = 50  # bound one tick's work; the rest waits for the next tick


async def sweep_once(budget: BudgetClient, batch: int = SWEEP_BATCH) -> int:
    """Sweep until a batch comes back short. Returns how many reservations were released."""
    released = 0
    for _ in range(MAX_BATCHES_PER_TICK):
        result = await budget.sweep(batch)
        released += result.released
        if result.examined < batch:
            break
    return released


async def run_sweeper(budget: BudgetClient, interval: float) -> None:
    """Sweep now, then every ``interval`` seconds, until cancelled."""
    while True:
        try:
            released = await sweep_once(budget)
        except (redis.RedisError, OSError) as exc:
            log.warning("sweep failed: %r", exc)
        else:
            if released:
                log.info("sweeper released %d expired reservation(s)", released)
            else:
                log.debug("sweeper released 0 expired reservations")
        await asyncio.sleep(interval)
