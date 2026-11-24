import os

import pytest
import redis
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app

TEST_REDIS_URL = os.environ.get("AGENTMETER_TEST_REDIS_URL", "redis://localhost:6379/0")
REQUIRE_REDIS = os.environ.get("AGENTMETER_REQUIRE_REDIS") == "1"


def _redis_reachable(url: str) -> bool:
    try:
        return bool(redis.Redis.from_url(url, socket_connect_timeout=0.5).ping())
    except (redis.RedisError, OSError):
        return False


def test_healthz_redis_down() -> None:
    # Port 1 is never a Redis server: connection is refused immediately.
    with TestClient(create_app(Settings(redis_url="redis://127.0.0.1:1/0"))) as client:
        resp = client.get("/healthz")
    assert resp.status_code == 503
    assert resp.json() == {"status": "degraded", "redis": False}


def test_healthz_redis_up() -> None:
    if not _redis_reachable(TEST_REDIS_URL):
        if REQUIRE_REDIS:
            pytest.fail(f"Redis required but not reachable at {TEST_REDIS_URL}")
        pytest.skip("Redis not running (docker compose up -d redis)")
    with TestClient(create_app(Settings(redis_url=TEST_REDIS_URL))) as client:
        resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "redis": True}
