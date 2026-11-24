"""Run identity from X-AM-* request headers."""

from __future__ import annotations

import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass

from app.config import usd_to_micro

RUN_ID_HEADER = "X-AM-Run-Id"
AGENT_HEADER = "X-AM-Agent"
TEAM_HEADER = "X-AM-Team"
RUN_BUDGET_HEADER = "X-AM-Run-Budget"

# Identity values become Redis key parts later, so keep them to a safe charset.
_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")


class IdentityError(ValueError):
    def __init__(self, header: str, message: str) -> None:
        super().__init__(message)
        self.header = header


@dataclass(frozen=True, slots=True)
class Identity:
    run_id: str
    agent: str | None
    team: str | None
    run_budget: int | None  # micro-USD; None means "use the configured default"
    run_id_generated: bool


def _id_header(headers: Mapping[str, str], name: str) -> str | None:
    value = headers.get(name)
    if value is None:
        return None
    value = value.strip()
    if not _ID_RE.fullmatch(value):
        raise IdentityError(name, f"{name} must match {_ID_RE.pattern}")
    return value


def parse_identity(headers: Mapping[str, str]) -> Identity:
    """Parse identity headers. ``headers`` must be case-insensitive (e.g. Starlette's)."""
    run_id = _id_header(headers, RUN_ID_HEADER)
    generated = run_id is None
    if run_id is None:
        run_id = uuid.uuid4().hex

    budget: int | None = None
    raw_budget = headers.get(RUN_BUDGET_HEADER)
    if raw_budget is not None:
        try:
            budget = usd_to_micro(raw_budget)
        except ValueError as exc:
            raise IdentityError(RUN_BUDGET_HEADER, f"{RUN_BUDGET_HEADER}: {exc}") from exc

    return Identity(
        run_id=run_id,
        agent=_id_header(headers, AGENT_HEADER),
        team=_id_header(headers, TEAM_HEADER),
        run_budget=budget,
        run_id_generated=generated,
    )
