from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

import httpx
import redis.asyncio as redis
from fastapi import FastAPI, Request
from fastapi.responses import Response

from app import proxy
from app.config import Settings, load_settings
from app.identity import RUN_ID_HEADER, Identity, IdentityError, parse_identity

REDIS_PING_TIMEOUT_S = 1.0
log = logging.getLogger("agentmeter")

StreamHook = Callable[[Identity, proxy.StreamResult], Awaitable[None]]


async def log_stream_result(identity: Identity, result: proxy.StreamResult) -> None:
    """Default stream hook. M2 replaces this with budget settlement."""
    # TODO(M2): if result.usage is None (stream cut / client gone), settle from a tiktoken
    # count of the streamed content instead of the reservation estimate.
    log.info(
        "stream finished run_id=%s usage=%s done=%s error=%s client_disconnected=%s bytes=%d",
        identity.run_id,
        result.usage,
        result.done,
        result.error,
        result.client_disconnected,
        result.bytes_relayed,
    )


def _configure_logging() -> None:
    if not log.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
        log.addHandler(handler)
    log.setLevel(logging.INFO)


def create_app(
    settings: Settings | None = None,
    upstream_transport: httpx.AsyncBaseTransport | None = None,
    on_stream_finish: StreamHook = log_stream_result,
) -> FastAPI:
    """``upstream_transport`` lets tests route upstream calls in-process."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        _configure_logging()
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

        response = await _chat_completions(request, identity, on_stream_finish)
        response.headers[RUN_ID_HEADER] = identity.run_id
        return response

    return app


async def _chat_completions(request: Request, identity: Identity, hook: StreamHook) -> Response:
    raw = await request.body()
    try:
        body = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return proxy.openai_error(400, "Request body is not valid JSON", "invalid_request_error")
    if not isinstance(body, dict):
        return proxy.openai_error(
            400, "Request body must be a JSON object", "invalid_request_error"
        )

    cfg: Settings = request.app.state.settings
    url = f"{cfg.upstream.rstrip('/')}/chat/completions"
    client: httpx.AsyncClient = request.app.state.http

    if not body.get("stream"):
        return await proxy.forward(
            client, url, raw, request.headers.items(), params=request.query_params
        )

    try:
        body = proxy.with_include_usage(body)
    except ValueError as exc:
        return proxy.openai_error(400, str(exc), "invalid_request_error")

    async def on_finish(result: proxy.StreamResult) -> None:
        await hook(identity, result)

    return await proxy.forward_stream(
        client, url, body, request.headers.items(), request.query_params, on_finish
    )


async def _redis_ok(client: redis.Redis) -> bool:
    try:
        return bool(await asyncio.wait_for(client.ping(), timeout=REDIS_PING_TIMEOUT_S))
    except (redis.RedisError, OSError, TimeoutError):
        return False


app = create_app()
