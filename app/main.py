from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

import httpx
import redis.asyncio as redis
from fastapi import FastAPI, Request
from fastapi.responses import Response

from app import proxy
from app.config import Settings, load_settings
from app.estimate import EstimateError, estimate_micro
from app.identity import RUN_ID_HEADER, Identity, IdentityError, parse_identity
from app.pricing import PriceTable, UnknownModel

REDIS_PING_TIMEOUT_S = 1.0
log = logging.getLogger("agentmeter")


@dataclass(frozen=True, slots=True)
class CallContext:
    """One proxied call: who made it and what it can cost at most."""

    identity: Identity
    model: str
    estimate_micro: int
    prices: PriceTable

    def actual_micro(self, usage: dict[str, Any] | None) -> int | None:
        if not usage:
            return None
        prompt, completion = usage.get("prompt_tokens"), usage.get("completion_tokens")
        if not isinstance(prompt, int) or not isinstance(completion, int):
            return None
        return self.prices.cost_micro(self.model, prompt, completion)


StreamHook = Callable[[CallContext, proxy.StreamResult], Awaitable[None]]


def _log_cost(ctx: CallContext, usage: dict[str, Any] | None, kind: str, **extra: Any) -> None:
    details = " ".join(f"{k}={v}" for k, v in extra.items())
    log.info(
        "%s finished run_id=%s model=%s estimate_micro=%d actual_micro=%s usage=%s %s",
        kind,
        ctx.identity.run_id,
        ctx.model,
        ctx.estimate_micro,
        ctx.actual_micro(usage),
        usage,
        details,
    )


async def log_stream_result(ctx: CallContext, result: proxy.StreamResult) -> None:
    """Default stream hook. M2 replaces this with budget settlement."""
    # TODO(M2): if result.usage is None (stream cut / client gone), settle from a tiktoken
    # count of the streamed content instead of the reservation estimate.
    _log_cost(
        ctx,
        result.usage,
        "stream",
        done=result.done,
        error=result.error,
        client_disconnected=result.client_disconnected,
        bytes=result.bytes_relayed,
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
        app.state.prices = PriceTable(cfg.pricing)
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
    prices: PriceTable = request.app.state.prices
    model = body.get("model")
    if not isinstance(model, str) or not model:
        return proxy.openai_error(400, "model is required", "invalid_request_error")

    # Cap the upstream at exactly what we estimate when the client sets no limit.
    if body.get("max_tokens") is None and body.get("max_completion_tokens") is None:
        body = {**body, "max_tokens": cfg.default_max_tokens}
        raw = b""  # body changed: must be re-serialized

    try:
        estimate = estimate_micro(
            prices, model, body, cfg.default_max_tokens, cfg.image_part_tokens
        )
    except UnknownModel as exc:
        # Fail closed: a call we can't price is a call we can't budget.
        return proxy.openai_error(400, str(exc), "agentmeter_unknown_model", "unknown_model")
    except EstimateError as exc:
        return proxy.openai_error(400, str(exc), "invalid_request_error")

    ctx = CallContext(identity=identity, model=model, estimate_micro=estimate, prices=prices)
    url = f"{cfg.upstream.rstrip('/')}/chat/completions"
    client: httpx.AsyncClient = request.app.state.http

    if not body.get("stream"):
        content = raw or json.dumps(body, separators=(",", ":")).encode()
        response = await proxy.forward(
            client, url, content, request.headers.items(), params=request.query_params
        )
        _log_cost(ctx, _usage_from(response), "call", status=response.status_code)
        return response

    try:
        body = proxy.with_include_usage(body)
    except ValueError as exc:
        return proxy.openai_error(400, str(exc), "invalid_request_error")

    async def on_finish(result: proxy.StreamResult) -> None:
        await hook(ctx, result)

    return await proxy.forward_stream(
        client, url, body, request.headers.items(), request.query_params, on_finish
    )


def _usage_from(response: Response) -> dict[str, Any] | None:
    if not 200 <= response.status_code < 300:
        return None
    try:
        usage = json.loads(response.body).get("usage")
    except (ValueError, AttributeError):
        return None
    return usage if isinstance(usage, dict) else None


async def _redis_ok(client: redis.Redis) -> bool:
    try:
        return bool(await asyncio.wait_for(client.ping(), timeout=REDIS_PING_TIMEOUT_S))
    except (redis.RedisError, OSError, TimeoutError):
        return False


app = create_app()
