"""Per-run circuit breaker, step cap and loop detection (PLAN §3.2, §3.3).

Keys (all expire with the run's budget key, ``now + run_ttl_seconds``):

    am:brk:{run_id}     the breaker: present = OPEN. Fields: reason (loop | step_cap),
                        source (request_repeats | response_tool_calls | step_cap),
                        steps, opened_ms
    am:steps:{run_id}   calls that passed the breaker check (step_cap + 1 opens it)
    am:fp:{run_id}      hash: request fingerprint -> times seen (repeat_threshold opens)
    am:tc:{run_id}      the response tool-call streak: last fp + count in a row

``gate`` (gate.lua) runs before the budget reserve. ``record_tool_call`` (toolcall.lua)
runs after a successful response. Both are atomic, so concurrent calls on any number of
replicas see one count. ``reset`` clears all four keys and never touches budget keys.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import redis.asyncio as redis

from app.budget import run_key
from app.config import LoopLimits, Settings
from app.events import STREAM_KEY

_LUA_DIR = Path(__file__).parent / "lua"
_INT_FIELDS = frozenset({"steps", "opened_ms"})


def breaker_key(run_id: str) -> str:
    return f"am:brk:{run_id}"


def steps_key(run_id: str) -> str:
    return f"am:steps:{run_id}"


def fingerprints_key(run_id: str) -> str:
    return f"am:fp:{run_id}"


def tool_calls_key(run_id: str) -> str:
    return f"am:tc:{run_id}"


def _s(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


@dataclass(frozen=True, slots=True)
class GateResult:
    allowed: bool
    reason: str | None  # "loop" | "step_cap" when refused
    spent: int  # the run's spent micro-USD (for the 429 body)
    steps: int
    repeats: int  # times this request's fingerprint has been seen, this call included
    opened_now: bool  # this call opened the breaker


@dataclass(frozen=True, slots=True)
class ResetResult:
    breaker: dict[str, str | int] | None  # the breaker's fields before the reset, if open
    steps: int


class BreakerClient:
    def __init__(self, client: redis.Redis, settings: Settings) -> None:
        self._redis = client
        self._settings = settings
        self._gate = client.register_script((_LUA_DIR / "gate.lua").read_text("utf-8"))
        self._toolcall = client.register_script((_LUA_DIR / "toolcall.lua").read_text("utf-8"))

    def limits(self, agent: str | None) -> LoopLimits:
        return self._settings.loop.limits_for(agent)

    def _expire_at(self) -> int:
        return int(time.time()) + self._settings.run_ttl_seconds

    async def gate(
        self,
        run_id: str,
        agent: str | None,
        fingerprint: str | None,
        team: str | None = None,
    ) -> GateResult:
        limits = self.limits(agent)
        result = await self._gate(
            keys=[
                breaker_key(run_id),
                steps_key(run_id),
                fingerprints_key(run_id),
                run_key(run_id),
                STREAM_KEY,
            ],
            args=[
                limits.step_cap,
                limits.repeat_threshold,
                fingerprint or "",
                self._expire_at(),
                self._settings.events.stream_maxlen,
                run_id,
                agent or "",
                team or "",
            ],
        )
        if int(result[0]) == 1:
            return GateResult(True, None, 0, int(result[1]), int(result[2]), False)
        _, reason, spent, steps, repeats, new = result
        return GateResult(False, _s(reason), int(spent), int(steps), int(repeats), bool(int(new)))

    async def record_tool_call(
        self,
        run_id: str,
        agent: str | None,
        fingerprint: str | None,
        team: str | None = None,
    ) -> tuple[int, bool]:
        """Track the response tool-call streak. Returns (count in a row, breaker opened)."""
        count, opened = await self._toolcall(
            keys=[tool_calls_key(run_id), breaker_key(run_id), STREAM_KEY, run_key(run_id)],
            args=[
                fingerprint or "",
                self.limits(agent).consecutive_tool_calls,
                self._expire_at(),
                self._settings.events.stream_maxlen,
                run_id,
                agent or "",
                team or "",
            ],
        )
        return int(count), bool(int(opened))

    async def reset(self, run_id: str) -> ResetResult:
        """Close the breaker and forget the step count and fingerprints (not the money)."""
        keys = [breaker_key(run_id), steps_key(run_id), fingerprints_key(run_id)]
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.hgetall(breaker_key(run_id))
            pipe.get(steps_key(run_id))
            pipe.delete(*keys, tool_calls_key(run_id))
            breaker, steps, _ = await pipe.execute()
        fields: dict[str, str | int] = {_s(k): _s(v) for k, v in breaker.items()}
        for name in _INT_FIELDS & fields.keys():
            fields[name] = int(fields[name])
        return ResetResult(fields or None, int(steps or 0))
