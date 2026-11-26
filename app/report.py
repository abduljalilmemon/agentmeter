"""GET /runs/{id}: one run's report, from keys the Lua scripts maintain (PLAN §6, M5).

Nothing here scans the event stream. The totals come from ``am:run:{id}:stats``
(settle.lua HINCRBYs it for every settled call), the money from the run's budget key,
and the breaker from ``am:brk:{id}`` plus the event list ``am:run:{id}:breaker``. All of
them expire with the run key.
"""

from __future__ import annotations

import json
from typing import Any

import redis.asyncio as redis

from app.breaker import breaker_key, history_key, steps_key
from app.budget import run_key, run_stats_key
from app.config import MICRO_PER_USD


def _usd(micro: int) -> float:
    return micro / MICRO_PER_USD  # display only, as in the 429 bodies


def _s(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _hash(raw: dict[Any, Any]) -> dict[str, str]:
    return {_s(k): _s(v) for k, v in raw.items()}


def _int(fields: dict[str, str], name: str) -> int:
    return int(fields.get(name, "0"))


async def run_report(client: redis.Redis, run_id: str) -> dict[str, Any] | None:
    """The run's report, or None if Redis knows nothing about the run."""
    async with client.pipeline(transaction=True) as pipe:
        pipe.hgetall(run_stats_key(run_id))
        pipe.hgetall(run_key(run_id))
        pipe.hgetall(breaker_key(run_id))
        pipe.lrange(history_key(run_id), 0, -1)
        pipe.get(steps_key(run_id))
        stats_raw, budget_raw, breaker_raw, history_raw, steps = await pipe.execute()
    stats, budget, breaker = _hash(stats_raw), _hash(budget_raw), _hash(breaker_raw)
    if not (stats or budget or breaker or history_raw):
        return None

    prompt, completion = _int(stats, "prompt_tokens"), _int(stats, "completion_tokens")
    cost, estimate = _int(stats, "cost_micro"), _int(stats, "estimate_micro")
    limit = int(budget["limit"]) if "limit" in budget else None
    spent, reserved = _int(budget, "spent"), _int(budget, "reserved")
    events = []
    for entry in history_raw:
        try:
            events.append(json.loads(_s(entry)))
        except ValueError:
            continue
    for event in events:
        for name in ("steps", "count", "at_ms"):
            if name in event:
                event[name] = int(event[name])

    return {
        "run_id": run_id,
        "agent": stats.get("agent"),
        "team": stats.get("team"),
        "calls": _int(stats, "calls"),
        "steps": int(steps or 0),
        "tokens": {"prompt": prompt, "completion": completion, "total": prompt + completion},
        "cost_usd": _usd(cost),
        "cost_micro": cost,
        "estimate_usd": _usd(estimate),
        "estimate_micro": estimate,
        "limit_usd": None if limit is None else _usd(limit),
        "limit_micro": limit,
        "spent_usd": _usd(spent),
        "spent_micro": spent,
        "reserved_usd": _usd(reserved),
        "reserved_micro": reserved,
        "breaker": {
            "state": "open" if breaker else "closed",
            "reason": breaker.get("reason"),
            "source": breaker.get("source"),
            "steps": int(breaker["steps"]) if "steps" in breaker else None,
            "opened_ms": int(breaker["opened_ms"]) if "opened_ms" in breaker else None,
        },
        "breaker_events": events,
    }
