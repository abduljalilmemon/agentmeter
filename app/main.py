from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
import redis.asyncio as redis
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from app import proxy
from app.budget import BudgetClient
from app.chat import StreamHook, handle_chat, log_stream_result
from app.config import Settings, load_settings
from app.identity import RUN_ID_HEADER, IdentityError, parse_identity
from app.pricing import PriceTable

REDIS_TIMEOUT_S = 1.0
log = logging.getLogger("agentmeter")


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
    """``upstream_transport`` lets tests route upstream calls in-process.

    ``on_stream_finish`` observes each finished stream after it has been settled.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        _configure_logging()
        cfg = settings or load_settings()
        app.state.settings = cfg
        app.state.prices = PriceTable(cfg.pricing)
        app.state.redis = redis.from_url(
            cfg.redis_url,
            socket_connect_timeout=REDIS_TIMEOUT_S,
            socket_timeout=REDIS_TIMEOUT_S,
        )
        app.state.budget = BudgetClient(app.state.redis, cfg)
        app.state.http = proxy.create_client(cfg.upstream_timeout_seconds, upstream_transport)
        try:
            yield
        finally:
            await app.state.http.aclose()
            await app.state.redis.aclose()

    app = FastAPI(title="AgentMeter", lifespan=lifespan)

    @app.get("/healthz")
    async def healthz(request: Request) -> JSONResponse:
        # Budgets can't be enforced without Redis, so the proxy is not healthy without it.
        if await _redis_ok(request.app.state.redis):
            return JSONResponse({"status": "ok", "redis": True})
        return JSONResponse({"status": "degraded", "redis": False}, status_code=503)

    @app.post("/v1/chat/completions")
    async def chat_completions(request: Request) -> Response:
        try:
            identity = parse_identity(request.headers)
        except IdentityError as exc:
            return proxy.openai_error(400, str(exc), "invalid_request_error", "invalid_header")

        response = await handle_chat(request, identity, on_stream_finish)
        response.headers[RUN_ID_HEADER] = identity.run_id
        return response

    return app


async def _redis_ok(client: redis.Redis) -> bool:
    try:
        return bool(await asyncio.wait_for(client.ping(), timeout=REDIS_TIMEOUT_S))
    except (redis.RedisError, OSError, TimeoutError):
        return False


app = create_app()
