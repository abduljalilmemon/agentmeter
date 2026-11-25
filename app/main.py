from __future__ import annotations

import asyncio
import contextlib
import hmac
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
import redis.asyncio as redis
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from app import proxy
from app.breaker import BreakerClient
from app.budget import BudgetClient
from app.chat import StreamHook, handle_chat, log_stream_result
from app.config import Settings, load_settings
from app.identity import RUN_ID_HEADER, IdentityError, is_valid_id, parse_identity
from app.pricing import PriceTable
from app.sweeper import run_sweeper

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
        app.state.breaker = BreakerClient(app.state.redis, cfg)
        app.state.http = proxy.create_client(cfg.upstream_timeout_seconds, upstream_transport)
        sweeper = asyncio.create_task(run_sweeper(app.state.budget, cfg.sweep_interval_seconds))
        try:
            yield
        finally:
            sweeper.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await sweeper
            await app.state.http.aclose()
            await app.state.redis.aclose()

    app = FastAPI(title="AgentMeter", lifespan=lifespan)

    @app.get("/healthz")
    async def healthz(request: Request) -> JSONResponse:
        # Budgets can't be enforced without Redis, so the proxy is not healthy without it.
        if await _redis_ok(request.app.state.redis):
            return JSONResponse({"status": "ok", "redis": True})
        return JSONResponse({"status": "degraded", "redis": False}, status_code=503)

    @app.post("/admin/runs/{run_id}/reset")
    async def reset_run(run_id: str, request: Request) -> JSONResponse:
        """Close a run's breaker and forget its step count and fingerprints (PLAN §3.3).

        Budget keys (spent, reserved, limit) are never touched.
        """
        cfg: Settings = request.app.state.settings
        if cfg.admin_token is None:
            return JSONResponse({"detail": "Not Found"}, status_code=404)
        if not _bearer_matches(request, cfg.admin_token.get_secret_value()):
            return JSONResponse(
                {"detail": "Invalid or missing admin token"},
                status_code=401,
                headers={"WWW-Authenticate": "Bearer"},
            )
        if not is_valid_id(run_id):
            return JSONResponse({"detail": "Invalid run id"}, status_code=400)
        try:
            result = await request.app.state.breaker.reset(run_id)
        except (redis.RedisError, OSError):
            log.exception("reset failed run_id=%s", run_id)
            return JSONResponse({"detail": "Budget store unavailable"}, status_code=503)
        log.warning(
            "admin reset run_id=%s breaker_was=%s steps_was=%d",
            run_id,
            result.breaker,
            result.steps,
        )
        return JSONResponse(
            {
                "run_id": run_id,
                "reset": True,
                "breaker_was": result.breaker,
                "steps_was": result.steps,
            }
        )

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


def _bearer_matches(request: Request, token: str) -> bool:
    scheme, _, given = request.headers.get("authorization", "").partition(" ")
    if scheme.lower() != "bearer" or not given:
        return False
    return hmac.compare_digest(given.strip().encode(), token.encode())


async def _redis_ok(client: redis.Redis) -> bool:
    try:
        return bool(await asyncio.wait_for(client.ping(), timeout=REDIS_TIMEOUT_S))
    except (redis.RedisError, OSError, TimeoutError):
        return False


app = create_app()
