from pathlib import Path

import pytest
from pydantic import ValidationError

from app.config import Settings, load_settings, read_yaml_config, usd_to_micro
from tests.conftest import EXAMPLE_CONFIG


def test_example_yaml_loads() -> None:
    s = load_settings(EXAMPLE_CONFIG)

    assert s.upstream == "https://api.openai.com/v1"
    assert s.default_max_tokens == 1024

    assert set(s.pricing) == {"gpt-4o-mini", "gemini-2.5-flash"}
    assert s.pricing["gpt-4o-mini"].input_per_mtok == 150_000
    assert s.pricing["gpt-4o-mini"].output_per_mtok == 600_000
    assert s.pricing["gemini-2.5-flash"].input_per_mtok == 300_000
    assert s.pricing["gemini-2.5-flash"].output_per_mtok == 2_500_000

    assert s.budgets.default_run == 500_000
    assert s.budgets.agents["support-bot"].daily == 20_000_000
    assert s.budgets.teams["platform"].monthly == 500_000_000

    assert s.loop.repeat_threshold == 3
    assert s.loop.step_cap == 50
    assert s.alerts.thresholds == [0.5, 0.75, 0.9]
    assert s.alerts.webhook_url is None  # ${ALERT_WEBHOOK} unset


def test_money_fields_are_ints() -> None:
    s = load_settings(EXAMPLE_CONFIG)
    values = [s.budgets.default_run, s.budgets.agents["support-bot"].daily]
    values += [p.input_per_mtok for p in s.pricing.values()]
    assert all(type(v) is int for v in values)


def test_config_path_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AGENTMETER_CONFIG", str(EXAMPLE_CONFIG))
    assert load_settings().budgets.default_run == 500_000


def test_webhook_interpolated_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALERT_WEBHOOK", "https://hooks.example.test/x")
    assert load_settings(EXAMPLE_CONFIG).alerts.webhook_url == "https://hooks.example.test/x"


def test_interpolation_default(tmp_path: Path) -> None:
    cfg = tmp_path / "c.yaml"
    cfg.write_text("upstream: ${UP:-http://fallback/v1}\n", encoding="utf-8")
    assert read_yaml_config(cfg, env={})["upstream"] == "http://fallback/v1"
    assert read_yaml_config(cfg, env={"UP": "http://x/v1"})["upstream"] == "http://x/v1"


def test_env_overrides_yaml(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AGENTMETER_UPSTREAM", "http://mock_upstream:8000/v1")
    monkeypatch.setenv("AGENTMETER_REDIS_URL", "redis://redis:6379/1")
    monkeypatch.setenv("AGENTMETER_BUDGETS__DEFAULT_RUN_USD", "1.25")
    monkeypatch.setenv("AGENTMETER_LOOP__STEP_CAP", "7")

    s = load_settings(EXAMPLE_CONFIG)

    assert s.upstream == "http://mock_upstream:8000/v1"
    assert s.redis_url == "redis://redis:6379/1"
    assert s.budgets.default_run == 1_250_000
    assert s.loop.step_cap == 7
    # Siblings of an overridden nested field still come from YAML.
    assert s.budgets.agents["support-bot"].daily == 20_000_000
    assert s.loop.repeat_threshold == 3


@pytest.mark.parametrize(
    ("usd", "micro"),
    [(0.15, 150_000), ("0.000001", 1), (20, 20_000_000), ("2.50", 2_500_000), (0.1 + 0.2, None)],
)
def test_usd_to_micro(usd: object, micro: int | None) -> None:
    if micro is None:
        # 0.30000000000000004 is not a whole number of micro-USD: reject, don't round.
        with pytest.raises(ValueError):
            usd_to_micro(usd)
    else:
        assert usd_to_micro(usd) == micro


@pytest.mark.parametrize("bad", [-1, "abc", True, "nan", "inf"])
def test_usd_to_micro_rejects(bad: object) -> None:
    with pytest.raises(ValueError):
        usd_to_micro(bad)


def test_unknown_nested_key_rejected() -> None:
    with pytest.raises(ValidationError):
        Settings(loop={"repeat_threshold": 3, "step_kap": 50})


def test_thresholds_validated() -> None:
    with pytest.raises(ValidationError):
        Settings(alerts={"thresholds": [0.9, 0.5]})
    with pytest.raises(ValidationError):
        Settings(alerts={"thresholds": [1.5]})
