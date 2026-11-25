"""The event stream (PLAN §3, step 7) and alert formatting (PLAN §3.4).

One Redis Stream, ``am:events``, written only from inside the Lua scripts, so an event
exists exactly when the state change it describes happened:

    call_settled       settle.lua   every settled call (M5 builds the run report from it)
                                    fields: run, agent, team, model, prompt_tokens,
                                    completion_tokens, cost_micro, estimate_micro, how,
                                    reservation, reservation_state (released | gone)
    threshold_crossed  settle.lua   a budget level's ``spent`` reached a threshold; once per
                                    (level key, threshold), guarded by a SET NX flag
                                    fields: level, key, threshold_bp, spent, limit, run,
                                    agent, team
    breaker_open       gate.lua,    a run's breaker opened
                       toolcall.lua fields: run, agent, team, reason, source, steps, spent

Every field is a string; money is integer micro-USD, thresholds integer basis points.
Each XADD uses ``MAXLEN ~ events.stream_maxlen``.

Threshold flags: ``am:alert:{level key without am:b:}:{bp}``, for example
``am:alert:run:r1:5000``. A flag expires with its budget key (the run TTL, or the end
of the agent's day / team's month plus grace).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

STREAM_KEY = "am:events"
ALERTS_GROUP = "am:alerts"
DEAD_LETTER_KEY = "am:alerts:dead"

CALL_SETTLED = "call_settled"
THRESHOLD_CROSSED = "threshold_crossed"
BREAKER_OPEN = "breaker_open"
ALERT_TYPES = frozenset({THRESHOLD_CROSSED, BREAKER_OPEN})

BPS = 10_000
_BUDGET_PREFIX = "am:b:"


def threshold_flag_key(level_key: str, bp: int) -> str:
    return f"am:alert:{level_key.removeprefix(_BUDGET_PREFIX)}:{bp}"


def threshold_reached(spent: int, limit: int, bp: int) -> bool:
    """Reference rule (settle.lua implements the same, without large products)."""
    return limit > 0 and spent * BPS >= limit * bp


def format_usd(micro: int) -> str:
    """Integer micro-USD as a USD string, without floats: 1_234_500 -> '$1.2345'."""
    sign = "-" if micro < 0 else ""
    whole, frac = divmod(abs(micro), 1_000_000)
    frac_s = f"{frac:06d}".rstrip("0")
    frac_s = frac_s.ljust(2, "0")
    return f"{sign}${whole}.{frac_s}"


def format_bp(bp: int) -> str:
    """Basis points as a percentage string: 5000 -> '50%', 1250 -> '12.5%'."""
    whole, frac = divmod(bp, 100)
    return f"{whole}%" if frac == 0 else f"{whole}.{f'{frac:02d}'.rstrip('0')}%"


def decode(fields: Mapping[Any, Any]) -> dict[str, str]:
    def s(v: Any) -> str:
        return v.decode() if isinstance(v, bytes) else str(v)

    return {s(k): s(v) for k, v in fields.items()}


def _int(fields: Mapping[str, str], name: str) -> int:
    try:
        return int(fields.get(name, "0"))
    except ValueError:
        return 0


def _who(fields: Mapping[str, str]) -> str:
    parts = [f"run `{fields.get('run', '?')}`"]
    if fields.get("agent"):
        parts.append(f"agent `{fields['agent']}`")
    if fields.get("team"):
        parts.append(f"team `{fields['team']}`")
    return ", ".join(parts)


def alert_text(fields: Mapping[str, str], entry_id: str | None = None) -> str:
    """Slack-compatible message text for an alert event.

    With ``entry_id`` (the am:events stream id) the text ends with ``(event <id>)``, so a
    duplicate delivery (at least once) can be recognized as the same alert.
    """
    text = _alert_text(fields)
    return f"{text} (event {entry_id})" if entry_id else text


def _alert_text(fields: Mapping[str, str]) -> str:
    kind = fields.get("type")
    if kind == THRESHOLD_CROSSED:
        spent, limit = _int(fields, "spent"), _int(fields, "limit")
        key = fields.get("key", "").removeprefix(_BUDGET_PREFIX)
        return (
            f":warning: AgentMeter: {fields.get('level', '?')} budget `{key}` crossed "
            f"{format_bp(_int(fields, 'threshold_bp'))}: spent {format_usd(spent)} of "
            f"{format_usd(limit)} (last call: {_who(fields)})"
        )
    if kind == BREAKER_OPEN:
        run = fields.get("run", "?")
        return (
            f":no_entry: AgentMeter: circuit breaker OPEN for {_who(fields)}: "
            f"reason {fields.get('reason', '?')} ({fields.get('source', '?')}) after "
            f"{_int(fields, 'steps')} steps, spent {format_usd(_int(fields, 'spent'))}. "
            f"Reset: POST /admin/runs/{run}/reset"
        )
    return f"AgentMeter event {kind}: {dict(fields)}"
