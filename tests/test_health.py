import redis
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from tests.conftest import NO_REDIS, TEST_REDIS_URL


def test_healthz_redis_down() -> None:
    with TestClient(create_app(Settings(redis_url=NO_REDIS))) as client:
        resp = client.get("/healthz")
    assert resp.status_code == 503
    assert resp.json() == {"status": "degraded", "redis": False}


def test_healthz_redis_up(redis_db: redis.Redis) -> None:
    with TestClient(create_app(Settings(redis_url=TEST_REDIS_URL))) as client:
        resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "redis": True}
