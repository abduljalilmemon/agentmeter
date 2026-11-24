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
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Annotated, Any

import yaml
from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, field_validator
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


class LoopConfig(_Model):
    repeat_threshold: int = Field(default=3, ge=2)
    step_cap: int = Field(default=50, ge=1)


class AlertsConfig(_Model):
    # Ratios of a budget, not money amounts.
    thresholds: list[float] = Field(default_factory=lambda: [0.5, 0.75, 0.9])
    webhook_url: str | None = None

    @field_validator("thresholds")
    @classmethod
    def _check_thresholds(cls, v: list[float]) -> list[float]:
        if any(not 0 < t < 1 for t in v):
            raise ValueError("alert thresholds must be between 0 and 1 (exclusive)")
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
    pricing: dict[str, ModelPrice] = Field(default_factory=dict)
    budgets: Budgets = Field(default_factory=Budgets)
    loop: LoopConfig = Field(default_factory=LoopConfig)
    alerts: AlertsConfig = Field(default_factory=AlertsConfig)

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
