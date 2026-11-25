"""POST /v1/chat/completions: estimate → reserve → forward → settle (PLAN §3)."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from functools import partial
from typing import Any

import anyio
import httpx
import redis.asyncio as redis
from fastapi import Request
from fastapi.responses import JSONResponse, Response

from app import proxy
from app.budget import BudgetClient, Rejection, Reservation, SettleResult
from app.config import MICRO_PER_USD, Settings
from app.estimate import Estimate, EstimateError, count_text_tokens, estimate
from app.identity import Identity
from app.pricing import PriceTable, UnknownModel

log = logging.getLogger("agentmeter")


@dataclass(frozen=True, slots=True)
class CallContext:
    """One proxied call: who made it and what it can cost at most."""

    identity: Identity
    model: str
    estimate: Estimate
    prices: PriceTable

    @property
    def estimate_micro(self) -> int:
        return self.estimate.micro

    def cost(self, input_tokens: int, output_tokens: int) -> int:
        return self.prices.cost_micro(self.model, input_tokens, output_tokens)

    def actual_micro(self, usage: dict[str, Any] | None) -> int | None:
        if not usage:
            return None
        prompt, completion = usage.get("prompt_tokens"), usage.get("completion_tokens")
        if not isinstance(prompt, int) or not isinstance(completion, int):
            return None
        if isinstance(prompt, bool) or isinstance(completion, bool) or prompt < 0 or completion < 0:
            return None
        return self.cost(prompt, completion)

    def stream_cost(self, result: proxy.StreamResult) -> tuple[int, str]:
        """What a finished stream cost, and how we know."""
        actual = self.actual_micro(result.usage)
        if actual is not None:
            return actual, "usage"
        if result.client_disconnected:
            # The upstream may have kept generating after we stopped reading, and we can't
            # see those tokens: bill the worst case.
            return self.estimate_micro, "estimate:client_disconnected"
        # Upstream cut or ended without usage: bill the estimated prompt plus a tiktoken
        # count of what was generated and relayed.
        return self.cost(self.estimate.input_tokens, result.streamed_output_tokens), "tiktoken"


StreamHook = Callable[[CallContext, proxy.StreamResult], Awaitable[None]]


async def log_stream_result(ctx: CallContext, result: proxy.StreamResult) -> None:
    """Default stream observer (settlement has already happened when this runs)."""
    log.info(
        "stream result run_id=%s done=%s error=%s client_disconnected=%s bytes=%d "
        "streamed_tokens=%d",
        ctx.identity.run_id,
        result.done,
        result.error,
        result.client_disconnected,
        result.bytes_relayed,
        result.streamed_output_tokens,
    )


def _usd(micro: int) -> float:
    return micro / MICRO_PER_USD  # display only


def budget_exhausted(identity: Identity, rejection: Rejection) -> JSONResponse:
    level = rejection.level.name
    response = JSONResponse(
        status_code=429,
        content={
            "error": {
                "message": (
                    f"{level} budget exhausted: spent {_usd(rejection.spent)} + reserved "
                    f"{_usd(rejection.reserved)} + this call's worst case "
                    f"{_usd(rejection.estimate)} USD exceeds the limit "
                    f"{_usd(rejection.limit)} USD"
                ),
                "type": "agentmeter_breaker_open",
                "reason": "budget",
                "level": level,
                "run_id": identity.run_id,
                "spent_usd": _usd(rejection.spent),
                "reserved_usd": _usd(rejection.reserved),
                "limit_usd": _usd(rejection.limit),
                "estimate_usd": _usd(rejection.estimate),
                "param": None,
                "code": "budget_exhausted",
            }
        },
    )
    # Retrying can't help until the budget changes; tell the OpenAI SDKs not to.
    response.headers["x-should-retry"] = "false"
    return response


class _Settlement:
    """Settles one reservation exactly once, even across cancellation."""

    def __init__(self, budget: BudgetClient, reservation: Reservation, ctx: CallContext) -> None:
        self._budget = budget
        self._reservation = reservation
        self._ctx = ctx
        self.done = False

    async def __call__(self, actual: int, how: str, **extra: Any) -> None:
        if self.done:
            return
        self.done = True
        with anyio.CancelScope(shield=True):
            outcome: SettleResult | None = None
            try:
                outcome = await self._budget.settle(self._reservation, actual)
            except (redis.RedisError, OSError):
                # The sweeper releases the reservation once it expires, but this call's
                # cost is never added to `spent`: log enough to reconcile it by hand.
                log.error(
                    "settle failed: cost NOT recorded run_id=%s reservation_id=%s "
                    "actual_micro=%d how=%s",
                    self._ctx.identity.run_id,
                    self._reservation.id,
                    actual,
                    how,
                    exc_info=True,
                )
        details = " ".join(f"{k}={v}" for k, v in extra.items())
        log.info(
            "settled run_id=%s model=%s estimate_micro=%d actual_micro=%d how=%s reservation=%s %s",
            self._ctx.identity.run_id,
            self._ctx.model,
            self._ctx.estimate_micro,
            actual,
            how,
            outcome.value if outcome else "error",
            details,
        )


async def _heartbeat(
    budget: BudgetClient, reservation: Reservation, interval: float, run_id: str
) -> bool:
    """Keep a streaming call's reservation from being swept while the stream is open.

    Returns True once the reservation is gone, which ends the stream.
    """
    while True:
        await asyncio.sleep(interval)
        try:
            alive = await budget.extend(reservation)
        except (redis.RedisError, OSError) as exc:
            log.warning("reservation heartbeat failed run_id=%s: %r", run_id, exc)
            continue
        if not alive:
            # Swept (e.g. Redis was unreachable for longer than the TTL), so its headroom
            # may already be used by other calls: end the stream. Settle adds the cost.
            log.warning("reservation gone mid-stream run_id=%s", run_id)
            return True


def _usage_from(response: Response) -> dict[str, Any] | None:
    try:
        usage = json.loads(response.body).get("usage")
    except (ValueError, AttributeError):
        return None
    return usage if isinstance(usage, dict) else None


async def _settle_forwarded(
    settle: _Settlement, ctx: CallContext, forwarded: proxy.Forwarded
) -> None:
    status = forwarded.response.status_code
    match forwarded.outcome:
        case proxy.Outcome.OK:
            actual = ctx.actual_micro(_usage_from(forwarded.response))
            if actual is None:
                log.warning("2xx without usable usage; settling the estimate")
                await settle(ctx.estimate_micro, "estimate:no_usage", status=status)
            else:
                await settle(actual, "usage", status=status)
        case proxy.Outcome.UPSTREAM_ERROR | proxy.Outcome.UNREACHABLE:
            await settle(0, forwarded.outcome.value, status=status)
        case proxy.Outcome.TIMEOUT:
            # The upstream had the request and may have billed it: assume the worst case.
            await settle(ctx.estimate_micro, "estimate:timeout", status=status)


async def handle_chat(request: Request, identity: Identity, observer: StreamHook) -> Response:
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
    budget: BudgetClient = request.app.state.budget
    model = body.get("model")
    if not isinstance(model, str) or not model:
        return proxy.openai_error(400, "model is required", "invalid_request_error")
    streaming = bool(body.get("stream"))

    # Cap the upstream at exactly what we estimate when the client sets no limit.
    if body.get("max_tokens") is None and body.get("max_completion_tokens") is None:
        body = {**body, "max_tokens": cfg.default_max_tokens}
        raw = b""  # body changed: must be re-serialized
    if streaming:
        try:
            body = proxy.with_include_usage(body)
        except ValueError as exc:
            return proxy.openai_error(400, str(exc), "invalid_request_error")

    try:
        est = estimate(prices, model, body, cfg.default_max_tokens, cfg.image_part_tokens)
    except UnknownModel as exc:
        # Fail closed: a call we can't price is a call we can't budget.
        return proxy.openai_error(400, str(exc), "agentmeter_unknown_model", "unknown_model")
    except EstimateError as exc:
        return proxy.openai_error(400, str(exc), "invalid_request_error")
    ctx = CallContext(identity=identity, model=model, estimate=est, prices=prices)

    try:
        reserved = await budget.reserve(identity, est.micro)
    except (redis.RedisError, OSError):
        log.exception("reserve failed run_id=%s", identity.run_id)
        # Fail closed: without Redis no budget can be enforced.
        return proxy.openai_error(
            503, "Budget store unavailable", "agentmeter_budget_unavailable", "budget_unavailable"
        )
    if isinstance(reserved, Rejection):
        log.info(
            "budget exhausted run_id=%s level=%s spent=%d reserved=%d limit=%d estimate=%d",
            identity.run_id,
            reserved.level.name,
            reserved.spent,
            reserved.reserved,
            reserved.limit,
            reserved.estimate,
        )
        return budget_exhausted(identity, reserved)

    settle = _Settlement(budget, reserved, ctx)
    url = f"{cfg.upstream.rstrip('/')}/chat/completions"
    client: httpx.AsyncClient = request.app.state.http
    try:
        if not streaming:
            forwarded = await proxy.forward(
                client,
                url,
                raw or json.dumps(body, separators=(",", ":")).encode(),
                request.headers.items(),
                params=request.query_params,
            )
            await _settle_forwarded(settle, ctx, forwarded)
            return forwarded.response

        async def on_finish(result: proxy.StreamResult) -> None:
            actual, how = ctx.stream_cost(result)
            await settle(actual, how, done=result.done, error=result.error)
            await observer(ctx, result)

        forwarded = await proxy.forward_stream(
            client,
            url,
            body,
            request.headers.items(),
            request.query_params,
            on_finish,
            partial(count_text_tokens, model),
            keepalive=partial(
                _heartbeat, budget, reserved, cfg.reservation_heartbeat_seconds, identity.run_id
            ),
        )
        if forwarded.outcome is not proxy.Outcome.STREAMING:
            await _settle_forwarded(settle, ctx, forwarded)
        return forwarded.response
    except BaseException:
        # Cancelled or crashed mid-call: we can't know the cost, so assume the worst case.
        await settle(ctx.estimate_micro, "estimate:aborted")
        raise
