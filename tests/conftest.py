import importlib
import os
import socket
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest
import redis
import uvicorn

REPO_ROOT = Path(__file__).resolve().parent.parent
EXAMPLE_CONFIG = REPO_ROOT / "agentmeter.yaml"

# Test-harness knobs; not settings fields, so they survive the env scrub below.
_TEST_KNOBS = {
    "AGENTMETER_TEST_REDIS_URL",
    "AGENTMETER_REQUIRE_REDIS",
    "AGENTMETER_REQUIRE_DEMO",
}

# Tests own this Redis database and FLUSHDB it before each test that uses it.
TEST_REDIS_URL = os.environ.get("AGENTMETER_TEST_REDIS_URL", "redis://localhost:6379/15")
REQUIRE_REDIS = os.environ.get("AGENTMETER_REQUIRE_REDIS") == "1"
NO_REDIS = "redis://127.0.0.1:1/0"  # nothing listens on port 1

TEST_PRICING = {
    "gpt-4o-mini": {"input": "0.15", "output": "0.60"},
    "gemini-2.5-flash": {"input": "0.30", "output": "2.50"},
}


@pytest.fixture(autouse=True)
def _clean_agentmeter_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the developer's AGENTMETER_* / ALERT_WEBHOOK vars from leaking into tests."""
    for name in list(os.environ):
        if (name.startswith("AGENTMETER_") and name not in _TEST_KNOBS) or name == "ALERT_WEBHOOK":
            monkeypatch.delenv(name)


def require_demo_extra(importer=importlib.import_module) -> None:
    """Skip a test when the ``demo`` extra (langgraph) isn't installed, or fail it when
    AGENTMETER_REQUIRE_DEMO=1 (CI), so the demo tests can't silently stop running."""
    try:
        importer("langgraph")
    except ImportError:
        if os.environ.get("AGENTMETER_REQUIRE_DEMO") == "1":
            pytest.fail(
                "AGENTMETER_REQUIRE_DEMO=1 but the demo extra is not installed "
                '(pip install -c requirements.lock -e ".[dev,demo]")'
            )
        pytest.skip("demo extra not installed")


def redis_reachable(url: str = TEST_REDIS_URL) -> bool:
    try:
        return bool(redis.Redis.from_url(url, socket_connect_timeout=0.5).ping())
    except (redis.RedisError, OSError):
        return False


@pytest.fixture
def redis_db() -> redis.Redis:
    """A clean test database (sync client for assertions). Skips if Redis is not running,
    fails instead when AGENTMETER_REQUIRE_REDIS=1 (CI)."""
    if not redis_reachable():
        if REQUIRE_REDIS:
            pytest.fail(f"Redis required but not reachable at {TEST_REDIS_URL}")
        pytest.skip("Redis not running (docker compose up -d redis)")
    client = redis.Redis.from_url(TEST_REDIS_URL, decode_responses=True)
    client.flushdb()
    return client


def make_settings(upstream: str = "http://mock_upstream/v1", **kw: object):
    """Settings for proxy tests: priced models, the test Redis database."""
    from app.config import Settings

    kw.setdefault("redis_url", TEST_REDIS_URL)
    kw.setdefault("pricing", TEST_PRICING)
    return Settings(upstream=upstream, **kw)


@contextmanager
def serve(app) -> Iterator[str]:
    """Run an ASGI app under uvicorn on a free port in a thread (own event loop).

    Yields its base URL. uvicorn runs the app's lifespan, as in production.
    """
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="critical"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        assert thread.is_alive() and time.monotonic() < deadline, "server did not start"
        time.sleep(0.02)
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        thread.join(timeout=10)


@pytest.fixture(scope="session")
def live_mock() -> Iterator[str]:
    """mock_upstream on a real socket, so a cut stream is a real dropped connection."""
    with serve(_mock_app()) as url:
        yield f"{url}/v1"


def _mock_app():
    from mock_upstream.main import app

    return app
