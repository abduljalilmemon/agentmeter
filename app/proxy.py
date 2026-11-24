"""Forwarding to the upstream LLM API, non-streaming and SSE streaming."""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

import anyio
import httpx
from fastapi.responses import JSONResponse, Response, StreamingResponse
from starlette.types import Receive, Scope, Send

log = logging.getLogger("agentmeter.proxy")

# RFC 9110 §7.6.1 hop-by-hop headers, plus ones httpx/Starlette must recompute.
HOP_BY_HOP = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "proxy-connection",
        "te",
        "trailer",
        "trailers",
        "transfer-encoding",
        "upgrade",
    }
)
_REQUEST_DROP = HOP_BY_HOP | {"host", "content-length", "accept-encoding"}
# httpx decodes the body, so the upstream's encoding/length no longer apply.
_RESPONSE_DROP = HOP_BY_HOP | {"content-length", "content-encoding"}

# The SSE parser holds at most one partial line; longer lines are skipped, not buffered.
MAX_SSE_LINE_BYTES = 1 << 20


def _filter(headers: Iterable[tuple[str, str]], drop: frozenset[str]) -> list[tuple[str, str]]:
    pairs = [(k.lower(), v) for k, v in headers]
    # Headers named in Connection are hop-by-hop too.
    listed = {
        token.strip().lower() for k, v in pairs if k == "connection" for token in v.split(",")
    }
    return [(k, v) for k, v in pairs if k not in drop and k not in listed]


def upstream_request_headers(headers: Iterable[tuple[str, str]]) -> list[tuple[str, str]]:
    return [(k, v) for k, v in _filter(headers, _REQUEST_DROP) if not k.startswith("x-am-")]


def client_response_headers(headers: Iterable[tuple[str, str]]) -> list[tuple[str, str]]:
    return _filter(headers, _RESPONSE_DROP)


def openai_error(status: int, message: str, err_type: str, code: str | None = None) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={"error": {"message": message, "type": err_type, "param": None, "code": code}},
    )


def _transport_error_response(exc: httpx.TransportError) -> JSONResponse:
    # ConnectTimeout is a TimeoutException, but the upstream was never reached: 502.
    if isinstance(exc, httpx.ConnectTimeout):
        return openai_error(
            502, "Upstream connect timed out", "upstream_error", "upstream_connect_timeout"
        )
    if isinstance(exc, httpx.TimeoutException):
        return openai_error(504, "Upstream timed out", "upstream_error", "upstream_timeout")
    return openai_error(
        502, f"Upstream unreachable: {type(exc).__name__}", "upstream_error", "upstream_error"
    )


def _relay_headers(response: Response, upstream: httpx.Response) -> None:
    for k, v in client_response_headers(upstream.headers.multi_items()):
        response.headers.append(k, v)


def create_client(
    timeout_s: float, transport: httpx.AsyncBaseTransport | None = None
) -> httpx.AsyncClient:
    return httpx.AsyncClient(timeout=httpx.Timeout(timeout_s), transport=transport)


async def forward(
    client: httpx.AsyncClient,
    url: str,
    body: bytes,
    headers: Iterable[tuple[str, str]],
    params: Mapping[str, str] | None = None,
) -> Response:
    """POST ``body`` unchanged to ``url``; relay status, headers and body as they are."""
    try:
        upstream = await client.post(
            url, content=body, headers=upstream_request_headers(headers), params=params
        )
    except httpx.TransportError as exc:
        return _transport_error_response(exc)
    response = Response(content=upstream.content, status_code=upstream.status_code)
    _relay_headers(response, upstream)
    return response


# --- streaming ---------------------------------------------------------------------------


@dataclass
class StreamResult:
    """What one streamed response told us. M2 settles the budget from this."""

    usage: dict[str, Any] | None = None
    done: bool = False  # saw "data: [DONE]"
    error: str | None = None  # set if the upstream stream broke off
    bytes_relayed: int = 0
    client_disconnected: bool = False


StreamFinish = Callable[[StreamResult], Awaitable[None]]


class SSEUsageParser:
    """Incremental SSE scan that keeps only the last ``usage`` object and [DONE]."""

    def __init__(self, result: StreamResult) -> None:
        self._result = result
        self._buf = b""
        self._skipping = False  # inside an over-long line

    def feed(self, chunk: bytes) -> None:
        *lines, rest = (self._buf + chunk).split(b"\n")
        for line in lines:
            if self._skipping:
                self._skipping = False
                continue
            self._line(line.rstrip(b"\r"))
        if len(rest) > MAX_SSE_LINE_BYTES:
            self._skipping, rest = True, b""
        self._buf = rest

    def _line(self, line: bytes) -> None:
        if not line.startswith(b"data:"):
            return
        payload = line[5:].strip()
        if payload == b"[DONE]":
            self._result.done = True
            return
        try:
            event = json.loads(payload)
        except ValueError:
            return
        if isinstance(event, dict) and isinstance(event.get("usage"), dict):
            self._result.usage = event["usage"]


def with_include_usage(body: dict[str, Any]) -> dict[str, Any]:
    """Copy of ``body`` with stream_options.include_usage=true, other options kept."""
    options = body.get("stream_options") or {}
    if not isinstance(options, dict):
        raise ValueError("stream_options must be an object")
    return {**body, "stream_options": {**options, "include_usage": True}}


class UpstreamStreamingResponse(StreamingResponse):
    """Relays an open upstream stream; always closes it, even on disconnect or cancel."""

    def __init__(
        self,
        upstream: httpx.Response,
        result: StreamResult,
        on_finish: StreamFinish | None,
    ) -> None:
        self._upstream = upstream
        self._result = result
        self._on_finish = on_finish
        self._finished = False
        self._relay_ended = False  # upstream ran out (normally or with an error)
        self._iterator = self._relay()
        super().__init__(self._iterator, status_code=upstream.status_code)
        _relay_headers(self, upstream)

    async def _relay(self) -> AsyncIterator[bytes]:
        parser = SSEUsageParser(self._result)
        try:
            async for chunk in self._upstream.aiter_bytes():
                parser.feed(chunk)
                self._result.bytes_relayed += len(chunk)
                yield chunk
        except httpx.TransportError as exc:
            # Headers are already sent, so the only option is to end the stream.
            self._result.error = type(exc).__name__
            log.warning("upstream stream broke off: %r", exc)
        self._relay_ended = True

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        # The client can go away as OSError/ClientDisconnect (ASGI spec >= 2.4) or as a
        # cancellation from Starlette's disconnect listener that is swallowed by its task
        # group; either way the relay never reached its end.
        try:
            await super().__call__(scope, receive, send)
        finally:
            await self.close()

    async def close(self) -> None:
        """Idempotent cleanup: stop the relay, close upstream, report the result once."""
        if self._finished:
            return
        self._finished = True
        self._result.client_disconnected = not self._relay_ended
        with anyio.CancelScope(shield=True):
            try:
                await self._iterator.aclose()
            finally:
                await self._upstream.aclose()
                if self._on_finish is not None:
                    try:
                        await self._on_finish(self._result)
                    except Exception:
                        log.exception("stream on_finish hook failed")


async def forward_stream(
    client: httpx.AsyncClient,
    url: str,
    body: dict[str, Any],
    headers: Iterable[tuple[str, str]],
    params: Mapping[str, str] | None = None,
    on_finish: StreamFinish | None = None,
) -> Response:
    """Open a streaming upstream request (``body`` already has include_usage set)."""
    fwd_headers = [(k, v) for k, v in upstream_request_headers(headers) if k != "content-type"]
    request = client.build_request(
        "POST",
        url,
        # The body was rewritten (include_usage), so it is re-serialized; the response is not.
        content=json.dumps(body, separators=(",", ":")).encode(),
        headers=[*fwd_headers, ("content-type", "application/json")],
        params=params,
    )
    try:
        upstream = await client.send(request, stream=True)
    except httpx.TransportError as exc:
        return _transport_error_response(exc)

    if not upstream.is_success:
        # Error before streaming starts: relay it like a normal response. Error bodies are small.
        try:
            content = await upstream.aread()
        except httpx.TransportError as exc:
            return _transport_error_response(exc)
        finally:
            await upstream.aclose()
        response = Response(content=content, status_code=upstream.status_code)
        _relay_headers(response, upstream)
        return response

    return UpstreamStreamingResponse(upstream, StreamResult(), on_finish)
