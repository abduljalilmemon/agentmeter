from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
import redis.asyncio as redis
from fastapi import FastAPI, Request
from fastapi.responses import Response

from app import proxy
from app.config import Settings, load_settings
from app.identity import RUN_ID_HEADER, IdentityError, parse_identity

REDIS_PING_TIMEOUT_S = 1.0


def create_app(
    settings: Settings | None = None,
    upstream_transport: httpx.AsyncBaseTransport | None = None,
) -> FastAPI:
    """``upstream_transport`` lets tests route upstream calls in-process."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        cfg = settings or load_settings()
        app.state.settings = cfg
        app.state.redis = redis.from_url(
            cfg.redis_url,
            socket_connect_timeout=REDIS_PING_TIMEOUT_S,
            socket_timeout=REDIS_PING_TIMEOUT_S,
        )
        app.state.http = proxy.create_client(cfg.upstream_timeout_seconds, upstream_transport)
        try:
            yield
        finally:
            await app.state.http.aclose()
            await app.state.redis.aclose()

    app = FastAPI(title="AgentMeter", lifespan=lifespan)

    @app.get("/healthz")
    async def healthz(request: Request) -> dict[str, object]:
        return {"status": "ok", "redis": await _redis_ok(request.app.state.redis)}

    @app.post("/v1/chat/completions")
    async def chat_completions(request: Request) -> Response:
        try:
            identity = parse_identity(request.headers)
        except IdentityError as exc:
            return proxy.openai_error(400, str(exc), "invalid_request_error", "invalid_header")

        response = await _chat_completions(request)
        response.headers[RUN_ID_HEADER] = identity.run_id
        return response

    return app


async def _chat_completions(request: Request) -> Response:
    raw = await request.body()
    try:
        body = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return proxy.openai_error(400, "Request body is not valid JSON", "invalid_request_error")
    if not isinstance(body, dict):
        return proxy.openai_error(
            400, "Request body must be a JSON object", "invalid_request_error"
        )
    if body.get("stream"):
        return proxy.openai_error(
            501, "Streaming is not supported yet", "invalid_request_error", "stream_not_supported"
        )

    cfg: Settings = request.app.state.settings
    return await proxy.forward(
        request.app.state.http,
        f"{cfg.upstream.rstrip('/')}/chat/completions",
        raw,
        request.headers.items(),
        params=request.query_params,
    )


async def _redis_ok(client: redis.Redis) -> bool:
    try:
        return bool(await asyncio.wait_for(client.ping(), timeout=REDIS_PING_TIMEOUT_S))
    except (redis.RedisError, OSError, TimeoutError):
        return False


app = create_app()
