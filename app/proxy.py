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

# The SSE relay holds back at most one partial event; larger events pass through unparsed.
MAX_SSE_EVENT_BYTES = 1 << 20


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


class SSERelay:
    """Incremental SSE scanner that relays whole events only.

    ``feed`` returns the bytes of every event completed so far (unchanged), holding back a
    partial event. That keeps the client stream event-aligned, so if the upstream breaks off
    mid-event we can drop the fragment and append a clean error event. ``usage`` and
    ``[DONE]`` are only recorded from completed events. Memory is bounded: an event larger
    than MAX_SSE_EVENT_BYTES is passed through as-is and not parsed.
    """

    def __init__(self, result: StreamResult) -> None:
        self._result = result
        self._line_buf = b""  # partial line
        self._event = bytearray()  # complete lines of the current, unfinished event
        self._usage: dict[str, Any] | None = None
        self._done = False
        self._oversized = False

    def feed(self, chunk: bytes) -> bytes:
        out = bytearray()
        *lines, self._line_buf = (self._line_buf + chunk).split(b"\n")
        for line in lines:
            self._event += line + b"\n"
            stripped = line.rstrip(b"\r")
            if stripped == b"":
                out += self._complete_event()
            elif not self._oversized:
                self._line(stripped)
        if len(self._event) + len(self._line_buf) > MAX_SSE_EVENT_BYTES:
            # Give up holding this event back; relay it raw and skip parsing it.
            out += self._event + self._line_buf
            self._event.clear()
            self._line_buf = b""
            self._oversized = True
            self._usage, self._done = None, False
        return bytes(out)

    def flush(self) -> bytes:
        """Upstream ended normally: relay whatever is left, even without a final blank line."""
        rest = bytes(self._event) + self._line_buf
        if not self._oversized:
            self._commit()
        self._event.clear()
        self._line_buf = b""
        return rest

    def discard(self) -> int:
        """Upstream broke off: drop the incomplete event. Returns the bytes dropped."""
        dropped = len(self._event) + len(self._line_buf)
        self._event.clear()
        self._line_buf = b""
        return dropped

    def _complete_event(self) -> bytes:
        if not self._oversized:
            self._commit()
        self._oversized = False
        event = bytes(self._event)
        self._event.clear()
        return event

    def _commit(self) -> None:
        if self._usage is not None:
            self._result.usage = self._usage
        if self._done:
            self._result.done = True
        self._usage, self._done = None, False

    def _line(self, line: bytes) -> None:
        if not line.startswith(b"data:"):
            return
        payload = line[5:].strip()
        if payload == b"[DONE]":
            self._done = True
            return
        try:
            event = json.loads(payload)
        except ValueError:
            return
        if isinstance(event, dict) and isinstance(event.get("usage"), dict):
            self._usage = event["usage"]


def sse_error_event(message: str, err_type: str, code: str | None) -> bytes:
    body = {"error": {"message": message, "type": err_type, "param": None, "code": code}}
    return b"data: " + json.dumps(body).encode() + b"\n\n"


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
        relay = SSERelay(self._result)
        try:
            async for chunk in self._upstream.aiter_bytes():
                out = relay.feed(chunk)
                if out:
                    self._result.bytes_relayed += len(out)
                    yield out
            if rest := relay.flush():
                self._result.bytes_relayed += len(rest)
                yield rest
        except httpx.TransportError as exc:
            # Headers are already sent: drop the partial event, tell the client, end.
            self._result.error = type(exc).__name__
            dropped = relay.discard()
            log.warning("upstream stream broke off (%d partial bytes dropped): %r", dropped, exc)
            yield sse_error_event(
                f"Upstream stream ended unexpectedly: {type(exc).__name__}",
                "agentmeter_upstream_cut",
                "upstream_timeout" if isinstance(exc, httpx.TimeoutException) else "upstream_cut",
            )
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
