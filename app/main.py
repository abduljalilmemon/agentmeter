from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import redis.asyncio as redis
from fastapi import FastAPI, Request

from app.config import Settings, load_settings

REDIS_PING_TIMEOUT_S = 1.0


def create_app(settings: Settings | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        cfg = settings or load_settings()
        app.state.settings = cfg
        app.state.redis = redis.from_url(
            cfg.redis_url,
            socket_connect_timeout=REDIS_PING_TIMEOUT_S,
            socket_timeout=REDIS_PING_TIMEOUT_S,
        )
        try:
            yield
        finally:
            await app.state.redis.aclose()

    app = FastAPI(title="AgentMeter", lifespan=lifespan)

    @app.get("/healthz")
    async def healthz(request: Request) -> dict[str, object]:
        return {"status": "ok", "redis": await _redis_ok(request.app.state.redis)}

    return app


async def _redis_ok(client: redis.Redis) -> bool:
    try:
        return bool(await asyncio.wait_for(client.ping(), timeout=REDIS_PING_TIMEOUT_S))
    except (redis.RedisError, OSError, TimeoutError):
        return False


app = create_app()
