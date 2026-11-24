"""Forwarding to the upstream LLM API (non-streaming)."""

from __future__ import annotations

from collections.abc import Iterable, Mapping

import httpx
from fastapi.responses import JSONResponse, Response

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
    except httpx.TimeoutException:
        return openai_error(504, "Upstream timed out", "upstream_error", "upstream_timeout")
    except httpx.TransportError as exc:
        return openai_error(
            502, f"Upstream unreachable: {type(exc).__name__}", "upstream_error", "upstream_error"
        )
    response = Response(content=upstream.content, status_code=upstream.status_code)
    for k, v in client_response_headers(upstream.headers.multi_items()):
        response.headers.append(k, v)
    return response
