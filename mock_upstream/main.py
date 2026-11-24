"""Fake OpenAI server for tests and demos. Costs nothing, returns canned usage.

Env vars (read per request, so tests can change them):
  MOCK_PROMPT_TOKENS      default 10
  MOCK_COMPLETION_TOKENS  default 20
  MOCK_CONTENT            default "Hello from mock_upstream."
  MOCK_FORCE_STATUS       if set (e.g. 400), return an OpenAI-style error with that status
  MOCK_STREAM_CUT         if "1", a streamed response drops the connection after the
                          content chunks, before the usage chunk and [DONE]

GET /_mock/last_request returns the headers and JSON body of the last chat request,
so tests can check what the proxy forwarded.
"""

from __future__ import annotations

import json
import os
import re
import time
import uuid
from collections.abc import AsyncIterator
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

app = FastAPI(title="mock_upstream")
_last_request: dict[str, Any] = {}


class StreamCut(Exception):
    """Raised mid-stream so the server drops the connection without finishing it."""


def _env_int(name: str, default: int) -> int:
    value = int(os.environ.get(name, default))
    if value < 0:
        raise ValueError(f"{name} must be >= 0")
    return value


def _usage() -> dict[str, int]:
    prompt = _env_int("MOCK_PROMPT_TOKENS", 10)
    completion = _env_int("MOCK_COMPLETION_TOKENS", 20)
    return {
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "total_tokens": prompt + completion,
    }


def _content() -> str:
    return os.environ.get("MOCK_CONTENT", "Hello from mock_upstream.")


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/_mock/last_request")
async def last_request() -> dict[str, Any]:
    return _last_request


@app.post("/v1/chat/completions", response_model=None)
async def chat_completions(request: Request) -> dict[str, Any] | JSONResponse | StreamingResponse:
    body = await request.json()
    _last_request.clear()
    _last_request.update({"headers": dict(request.headers), "body": body})

    forced = os.environ.get("MOCK_FORCE_STATUS")
    if forced:
        return JSONResponse(
            status_code=int(forced),
            content={
                "error": {
                    "message": f"mock forced status {forced}",
                    "type": "invalid_request_error",
                    "param": None,
                    "code": "mock_forced",
                }
            },
        )

    completion_id = f"chatcmpl-mock-{uuid.uuid4().hex[:12]}"
    model = body.get("model", "mock-model")
    created = int(time.time())

    if body.get("stream"):
        include_usage = bool((body.get("stream_options") or {}).get("include_usage"))
        cut = os.environ.get("MOCK_STREAM_CUT") == "1"
        return StreamingResponse(
            _stream(completion_id, model, created, include_usage, cut),
            media_type="text/event-stream",
        )

    return {
        "id": completion_id,
        "object": "chat.completion",
        "created": created,
        "model": model,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": _content()},
                "finish_reason": "stop",
            }
        ],
        "usage": _usage(),
    }


async def _stream(
    completion_id: str, model: str, created: int, include_usage: bool, cut: bool
) -> AsyncIterator[bytes]:
    def event(choices: list[dict[str, Any]], **extra: Any) -> bytes:
        chunk = {
            "id": completion_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": choices,
            **extra,
        }
        return f"data: {json.dumps(chunk)}\n\n".encode()

    # OpenAI sends usage: null on every chunk but the last when include_usage is set.
    nulls = {"usage": None} if include_usage else {}
    pieces = re.findall(r"\S+\s*", _content()) or [""]
    yield event([{"index": 0, "delta": {"role": "assistant", "content": ""}}], **nulls)
    for piece in pieces:
        yield event([{"index": 0, "delta": {"content": piece}, "finish_reason": None}], **nulls)
    yield event([{"index": 0, "delta": {}, "finish_reason": "stop"}], **nulls)
    if cut:
        raise StreamCut("MOCK_STREAM_CUT")
    if include_usage:
        yield event([], usage=_usage())
    yield b"data: [DONE]\n\n"
