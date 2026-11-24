import os
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
EXAMPLE_CONFIG = REPO_ROOT / "agentmeter.yaml"

# Test-harness knobs; not settings fields, so they survive the env scrub below.
_TEST_KNOBS = {"AGENTMETER_TEST_REDIS_URL", "AGENTMETER_REQUIRE_REDIS"}


@pytest.fixture(autouse=True)
def _clean_agentmeter_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the developer's AGENTMETER_* / ALERT_WEBHOOK vars from leaking into tests."""
    for name in list(os.environ):
        if (name.startswith("AGENTMETER_") and name not in _TEST_KNOBS) or name == "ALERT_WEBHOOK":
            monkeypatch.delenv(name)


NO_REDIS = "redis://127.0.0.1:1/0"
TEST_PRICING = {
    "gpt-4o-mini": {"input": "0.15", "output": "0.60"},
    "gemini-2.5-flash": {"input": "0.30", "output": "2.50"},
}


def make_settings(upstream: str = "http://mock_upstream/v1", **kw: object):
    """Settings for proxy tests: priced models, no Redis."""
    from app.config import Settings

    kw.setdefault("redis_url", NO_REDIS)
    kw.setdefault("pricing", TEST_PRICING)
    return Settings(upstream=upstream, **kw)
