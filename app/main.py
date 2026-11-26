from __future__ import annotations

import asyncio
import contextlib
import hmac
import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
import redis.asyncio as redis
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app import proxy
from app.breaker import BreakerClient
from app.budget import BudgetClient
from app.chat import StreamHook, handle_chat, log_stream_result
from app.config import Settings, load_settings
from app.estimate import encoding_for
from app.identity import RUN_ID_HEADER, IdentityError, is_valid_id, parse_identity
from app.metrics import ProxyMetrics
from app.pricing import PriceTable
from app.report import run_report
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
        # Build the tokenizers now, not on the first call (it takes ~0.5 s and would show
        # up as that call's proxy overhead).
        await asyncio.to_thread(_warm_tokenizers, list(cfg.pricing))
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
    metrics = ProxyMetrics()  # per app, so test apps never share counters
    app.state.metrics = metrics

    @app.get("/metrics")
    async def prometheus_metrics() -> Response:
        return Response(generate_latest(metrics.registry), media_type=CONTENT_TYPE_LATEST)

    @app.get("/runs/{run_id}")
    async def get_run(run_id: str, request: Request) -> JSONResponse:
        """The run's report (calls, tokens, cost, budget, breaker). Admin token required."""
        if (denied := _admin_denied(request, run_id)) is not None:
            return denied
        try:
            report = await run_report(request.app.state.redis, run_id)
        except (redis.RedisError, OSError):
            log.exception("run report failed run_id=%s", run_id)
            return JSONResponse({"detail": "Budget store unavailable"}, status_code=503)
        if report is None:
            return JSONResponse({"detail": "Unknown run"}, status_code=404)
        return JSONResponse(report)

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
        if (denied := _admin_denied(request, run_id)) is not None:
            return denied
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
        started = time.perf_counter()
        request.state.am_upstream_seconds = 0.0  # set by handle_chat around the upstream
        request.state.am_reason = request.state.am_level = ""
        try:
            identity = parse_identity(request.headers)
        except IdentityError as exc:
            response: Response = proxy.openai_error(
                400, str(exc), "invalid_request_error", "invalid_header"
            )
        else:
            response = await handle_chat(request, identity, on_stream_finish)
            response.headers[RUN_ID_HEADER] = identity.run_id
        state = request.state
        overhead = time.perf_counter() - started - state.am_upstream_seconds
        metrics.overhead.observe(max(overhead, 0.0))
        metrics.requests.labels(str(response.status_code), state.am_reason, state.am_level).inc()
        return response

    return app


def _warm_tokenizers(models: list[str]) -> None:
    for model in models:
        encoding_for(model).encode("warm up")


def _admin_denied(request: Request, run_id: str) -> JSONResponse | None:
    """Admin endpoint rules: 404 without a configured token, 401 for a wrong or missing
    bearer token, 400 for an invalid run id. None means the request may proceed."""
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
    return None


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
