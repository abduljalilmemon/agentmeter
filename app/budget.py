"""Atomic hierarchical budgets in Redis (PLAN §3.1).

Keys (all integer micro-USD, each a hash with fields ``limit``, ``spent``, ``reserved``):

    am:b:run:{run_id}                 the run's budget; limit fixed at the first reserve
    am:b:agent:{agent}:{YYYYMMDD}     the agent's daily budget (UTC day)
    am:b:team:{team}:{YYYYMM}         the team's monthly budget (UTC month)
    am:r:{reservation_id}             one in-flight reservation: amount + level keys
    am:resv                           ZSET of in-flight reservation ids, score = expiry (unix ms)

Levels:
- run: always present. Limit = X-AM-Run-Budget, else ``budgets.default_run``. The limit is
  written with HSETNX, so a later request with a different header can't change it.
- agent / team: only when the header is set *and* a budget is configured for that name.
  Otherwise the level has no limit and is skipped. Their limit is written with HSET, so a
  config change applies on the next request.

Expiry (EXPIREAT, absolute):
- run keys: now + ``run_ttl_seconds``, refreshed on every reserve (a busy run never expires).
- agent / team keys: end of their UTC period + PERIOD_GRACE_SECONDS.
- reservations: their score in ``am:resv`` is Redis TIME + ``reservation_ttl_seconds``.
  Long streams push it forward (extend.lua, every ``reservation_heartbeat_seconds``). The
  sweeper (sweep.lua, app/sweeper.py) releases the amount of any reservation whose score
  has passed. The hash itself has no TTL: only a sweep or a settle removes it.

Exactly once: settle.lua replaces the hash with a ``settled`` tombstone and drops the id
from ``am:resv``; sweep.lua deletes the hash. Both are atomic, so whichever runs first
releases the reservation and the other finds nothing to release.

Reserve checks ``spent + reserved + estimate <= limit`` at every level before writing
anything (reserve.lua). Settle releases the reservation and adds the actual cost
(settle.lua). Both run as one Lua script each, so concurrent requests on any number of
proxy replicas see a consistent budget.

Assumes a single Redis node: a request's keys span hash slots, so this does not work on
Redis Cluster as is.
"""

from __future__ import annotations

import calendar
import enum
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import redis.asyncio as redis

from app.config import Settings
from app.identity import Identity

PERIOD_GRACE_SECONDS = 2 * 86_400  # keep yesterday's / last month's numbers readable
RESERVATIONS_KEY = "am:resv"
RESERVATION_PREFIX = "am:r:"
SWEEP_BATCH = 100
_LUA_DIR = Path(__file__).parent / "lua"


def run_key(run_id: str) -> str:
    return f"am:b:run:{run_id}"


def agent_key(agent: str, now: datetime) -> str:
    return f"am:b:agent:{agent}:{now:%Y%m%d}"


def team_key(team: str, now: datetime) -> str:
    return f"am:b:team:{team}:{now:%Y%m}"


def reservation_key(reservation_id: str) -> str:
    return f"{RESERVATION_PREFIX}{reservation_id}"


def _end_of_day(now: datetime) -> int:
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return int((start + timedelta(days=1)).timestamp())


def _end_of_month(now: datetime) -> int:
    days = calendar.monthrange(now.year, now.month)[1]
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return int((start + timedelta(days=days)).timestamp())


@dataclass(frozen=True, slots=True)
class Level:
    name: str  # "run" | "agent" | "team"
    key: str
    limit: int  # micro-USD
    expire_at: int  # unix seconds
    fixed_limit: bool  # HSETNX (run) vs HSET (configured levels)


@dataclass(frozen=True, slots=True)
class Reservation:
    id: str
    amount: int  # micro-USD
    levels: tuple[Level, ...]

    @property
    def key(self) -> str:
        return reservation_key(self.id)


@dataclass(frozen=True, slots=True)
class Rejection:
    level: Level
    spent: int
    reserved: int
    limit: int  # the effective limit (for a run: the stored one)
    estimate: int


class SettleResult(enum.Enum):
    RELEASED = "released"  # the reservation was released and the cost added
    GONE = "gone"  # a sweep had already released it: the cost was added only
    DUPLICATE = "duplicate"  # already settled: nothing changed


@dataclass(frozen=True, slots=True)
class SweepResult:
    released: int
    examined: int


@dataclass(frozen=True, slots=True)
class LevelState:
    limit: int | None
    spent: int
    reserved: int


class BudgetClient:
    def __init__(self, client: redis.Redis, settings: Settings) -> None:
        self._redis = client
        self._settings = settings
        self._ttl_ms = settings.reservation_ttl_seconds * 1000
        self._reserve = self._script(client, "reserve")
        self._settle = self._script(client, "settle")
        self._extend = self._script(client, "extend")
        self._sweep = self._script(client, "sweep")

    @staticmethod
    def _script(client: redis.Redis, name: str):
        return client.register_script((_LUA_DIR / f"{name}.lua").read_text("utf-8"))

    def levels(self, identity: Identity, now: datetime | None = None) -> tuple[Level, ...]:
        now = now or datetime.now(UTC)
        budgets = self._settings.budgets
        run_limit = identity.run_budget if identity.run_budget is not None else budgets.default_run
        levels = [
            Level(
                "run",
                run_key(identity.run_id),
                run_limit,
                int(now.timestamp()) + self._settings.run_ttl_seconds,
                fixed_limit=True,
            )
        ]
        if identity.agent and (agent := budgets.agents.get(identity.agent)):
            levels.append(
                Level(
                    "agent",
                    agent_key(identity.agent, now),
                    agent.daily,
                    _end_of_day(now) + PERIOD_GRACE_SECONDS,
                    fixed_limit=False,
                )
            )
        if identity.team and (team := budgets.teams.get(identity.team)):
            levels.append(
                Level(
                    "team",
                    team_key(identity.team, now),
                    team.monthly,
                    _end_of_month(now) + PERIOD_GRACE_SECONDS,
                    fixed_limit=False,
                )
            )
        return tuple(levels)

    async def reserve(
        self, identity: Identity, estimate: int, now: datetime | None = None
    ) -> Reservation | Rejection:
        if estimate < 0:
            raise ValueError("estimate must be >= 0")
        levels = self.levels(identity, now)
        reservation = Reservation(uuid.uuid4().hex, estimate, levels)
        args: list[int | str] = [reservation.id, estimate, self._ttl_ms]
        for level in levels:
            args += [level.limit, level.expire_at, "1" if level.fixed_limit else "0"]
        result = await self._reserve(
            keys=[RESERVATIONS_KEY, reservation.key, *(lv.key for lv in levels)], args=args
        )
        status = int(result[0])
        if status == 1:
            return reservation
        if status == 0:
            _, index, spent, reserved, limit = (int(x) for x in result)
            return Rejection(levels[index - 1], spent, reserved, limit, estimate)
        raise RuntimeError(f"reservation id collision: {reservation.id}")

    async def settle(self, reservation: Reservation, actual: int) -> SettleResult:
        """Release the reservation and add ``actual``. Safe to call more than once."""
        if actual < 0:
            raise ValueError("actual cost must be >= 0")
        code = await self._settle(
            keys=[RESERVATIONS_KEY, reservation.key, *(lv.key for lv in reservation.levels)],
            args=[
                reservation.id,
                actual,
                self._ttl_ms,  # tombstone lifetime
                *(lv.expire_at for lv in reservation.levels),
            ],
        )
        return {1: SettleResult.RELEASED, 0: SettleResult.GONE, 2: SettleResult.DUPLICATE}[
            int(code)
        ]

    async def extend(self, reservation: Reservation) -> bool:
        """Push the reservation's expiry to now + TTL. False if it is already gone."""
        alive = await self._extend(
            keys=[RESERVATIONS_KEY, reservation.key], args=[reservation.id, self._ttl_ms]
        )
        return bool(int(alive))

    async def sweep(self, batch: int = SWEEP_BATCH) -> SweepResult:
        """Release up to ``batch`` expired reservations (one atomic script call)."""
        released, examined = await self._sweep(
            keys=[RESERVATIONS_KEY], args=[batch, RESERVATION_PREFIX]
        )
        return SweepResult(int(released), int(examined))

    async def state(self, key: str) -> LevelState:
        raw = await self._redis.hgetall(key)
        values = {k.decode() if isinstance(k, bytes) else k: int(v) for k, v in raw.items()}
        return LevelState(values.get("limit"), values.get("spent", 0), values.get("reserved", 0))
