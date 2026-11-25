"""Configuration loading.

Sources, highest priority first:
  1. Environment variables, prefixed ``AGENTMETER_`` with ``__`` for nesting,
     e.g. ``AGENTMETER_BUDGETS__DEFAULT_RUN_USD=1.25`` or ``AGENTMETER_UPSTREAM=...``.
  2. The YAML file (``AGENTMETER_CONFIG`` or ``./agentmeter.yaml``), after ``${VAR}``
     / ``${VAR:-default}`` interpolation.
  3. Field defaults.

Money is written in USD in YAML/env and stored as integer micro-USD (``MicroUSD``).
Conversion goes through ``Decimal`` so no float ever touches a money value, and a value
that is not a whole number of micro-USD is rejected rather than rounded.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Annotated, Any

import yaml
from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    SecretStr,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

MICRO_PER_USD = 1_000_000
DEFAULT_CONFIG_PATH = "agentmeter.yaml"


def usd_to_micro(value: Any) -> int:
    """Convert a USD amount (str/int/float from YAML or env) to integer micro-USD."""
    if isinstance(value, bool):
        raise ValueError("money must be a number, not a bool")
    try:
        # str() of a float is its shortest round-trip repr, so 0.15 -> "0.15" exactly.
        amount = Decimal(str(value).strip())
    except InvalidOperation as exc:
        raise ValueError(f"not a valid USD amount: {value!r}") from exc
    if not amount.is_finite() or amount < 0:
        raise ValueError(f"USD amount must be finite and >= 0: {value!r}")
    micro = amount * MICRO_PER_USD
    if micro != micro.to_integral_value():
        raise ValueError(f"USD amount has sub-micro-USD precision: {value!r}")
    return int(micro)


MicroUSD = Annotated[int, BeforeValidator(usd_to_micro)]
"""Integer micro-USD. Input is USD (e.g. ``0.5``), stored value is ``500_000``."""


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True, frozen=True)


class ModelPrice(_Model):
    """Price of one model, in micro-USD per 1M tokens (YAML: USD per 1M tokens)."""

    input_per_mtok: MicroUSD = Field(alias="input")
    output_per_mtok: MicroUSD = Field(alias="output")


class AgentBudget(_Model):
    daily: MicroUSD = Field(alias="daily_usd")


class TeamBudget(_Model):
    monthly: MicroUSD = Field(alias="monthly_usd")


class Budgets(_Model):
    default_run: MicroUSD = Field(default=500_000, alias="default_run_usd")
    agents: dict[str, AgentBudget] = Field(default_factory=dict)
    teams: dict[str, TeamBudget] = Field(default_factory=dict)


class LoopOverride(_Model):
    """Per-agent loop settings (PLAN §9: legitimate polling agents need looser limits)."""

    repeat_threshold: int | None = Field(default=None, ge=2)
    step_cap: int | None = Field(default=None, ge=1)
    consecutive_tool_calls: int | None = Field(default=None, ge=2)


@dataclass(frozen=True, slots=True)
class LoopLimits:
    repeat_threshold: int
    step_cap: int
    consecutive_tool_calls: int


class LoopConfig(_Model):
    # The same request fingerprint this many times in one run opens the breaker (loop).
    repeat_threshold: int = Field(default=3, ge=2)
    # Calls allowed per run; call step_cap + 1 opens the breaker (step_cap).
    step_cap: int = Field(default=50, ge=1)
    # The same tool call in this many responses in a row opens the breaker (loop).
    consecutive_tool_calls: int = Field(default=3, ge=2)
    agents: dict[str, LoopOverride] = Field(default_factory=dict)

    def limits_for(self, agent: str | None) -> LoopLimits:
        override = self.agents.get(agent) if agent else None

        def pick(name: str) -> int:
            value = getattr(override, name, None) if override else None
            return value if value is not None else getattr(self, name)

        return LoopLimits(
            pick("repeat_threshold"), pick("step_cap"), pick("consecutive_tool_calls")
        )


BPS_PER_UNIT = 10_000


def ratio_to_bps(value: Any) -> int:
    """Convert a budget ratio (YAML: 0.75) to integer basis points (7500), exactly."""
    if isinstance(value, bool):
        raise ValueError("threshold must be a number, not a bool")
    try:
        ratio = Decimal(str(value).strip())
    except InvalidOperation as exc:
        raise ValueError(f"not a valid ratio: {value!r}") from exc
    if not ratio.is_finite() or not 0 < ratio < 1:
        raise ValueError(f"alert threshold must be between 0 and 1 (exclusive): {value!r}")
    bps = ratio * BPS_PER_UNIT
    if bps != bps.to_integral_value():
        raise ValueError(f"alert threshold finer than 1 basis point: {value!r}")
    return int(bps)


Bps = Annotated[int, BeforeValidator(ratio_to_bps)]
"""Integer basis points of a budget. Input is a ratio (``0.75``), stored as ``7500``."""


class AlertsConfig(_Model):
    # YAML key "thresholds" holds ratios; stored as integer basis points so the M4
    # threshold math (spent * 10_000 >= limit * bps) never touches a float.
    thresholds_bps: list[Bps] = Field(
        default_factory=lambda: [5_000, 7_500, 9_000], alias="thresholds"
    )
    webhook_url: str | None = None

    @field_validator("thresholds_bps")
    @classmethod
    def _check_thresholds(cls, v: list[int]) -> list[int]:
        if v != sorted(set(v)):
            raise ValueError("alert thresholds must be strictly increasing")
        return v

    @field_validator("webhook_url", mode="before")
    @classmethod
    def _empty_is_none(cls, v: Any) -> Any:
        return None if v == "" else v


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="AGENTMETER_",
        env_nested_delimiter="__",
        populate_by_name=True,
        extra="ignore",
        frozen=True,
    )

    upstream: str = "https://api.openai.com/v1"
    redis_url: str = "redis://localhost:6379/0"
    upstream_timeout_seconds: float = Field(default=600.0, gt=0)
    default_max_tokens: int = Field(default=1024, ge=1)
    # Fixed token count per image part in the input estimate (placeholder, see estimate.py).
    image_part_tokens: int = Field(default=1105, ge=0)
    pricing: dict[str, ModelPrice] = Field(default_factory=dict)
    budgets: Budgets = Field(default_factory=Budgets)
    loop: LoopConfig = Field(default_factory=LoopConfig)
    alerts: AlertsConfig = Field(default_factory=AlertsConfig)
    # Bearer token for /admin/*. Unset (or empty) disables the admin endpoints (404).
    admin_token: SecretStr | None = None
    # A run's budget key lives this long after its last reserve.
    run_ttl_seconds: int = Field(default=7 * 86_400, gt=0)
    # An unsettled reservation is released by the sweeper this long after its last
    # reserve or heartbeat. Must outlast a non-streaming call (streams send heartbeats).
    reservation_ttl_seconds: int = Field(default=3_600, gt=0)
    # A live stream pushes its reservation's expiry forward this often.
    reservation_heartbeat_seconds: float = Field(default=60.0, gt=0)
    # How often each replica sweeps expired reservations.
    sweep_interval_seconds: float = Field(default=30.0, gt=0)

    @field_validator("admin_token", mode="before")
    @classmethod
    def _empty_token_is_none(cls, v: Any) -> Any:
        return None if v is None or (isinstance(v, str) and not v.strip()) else v

    @model_validator(mode="after")
    def _reservation_outlives_calls(self) -> Settings:
        if self.reservation_ttl_seconds <= self.upstream_timeout_seconds:
            raise ValueError("reservation_ttl_seconds must exceed upstream_timeout_seconds")
        if self.reservation_heartbeat_seconds >= self.reservation_ttl_seconds:
            raise ValueError(
                "reservation_heartbeat_seconds must be less than reservation_ttl_seconds"
            )
        return self

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # Env beats init kwargs, because load_settings() passes the YAML file as init kwargs.
        return (env_settings, init_settings)


_ENV_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def _interpolate(node: Any, env: dict[str, str]) -> Any:
    """Replace ${VAR} and ${VAR:-default} in every string of a parsed YAML tree."""
    if isinstance(node, str):
        return _ENV_REF.sub(lambda m: env.get(m.group(1), m.group(2) or ""), node)
    if isinstance(node, dict):
        return {k: _interpolate(v, env) for k, v in node.items()}
    if isinstance(node, list):
        return [_interpolate(v, env) for v in node]
    return node


def read_yaml_config(path: str | Path, env: dict[str, str] | None = None) -> dict[str, Any]:
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError(f"{path}: top level must be a mapping")
    return _interpolate(data, dict(os.environ) if env is None else env)


def load_settings(path: str | Path | None = None) -> Settings:
    """Load settings from the YAML file, with AGENTMETER_* env vars taking precedence."""
    path = Path(path or os.environ.get("AGENTMETER_CONFIG", DEFAULT_CONFIG_PATH))
    return Settings(**read_yaml_config(path))
