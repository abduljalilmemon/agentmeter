"""Fake OpenAI server for tests and demos. Costs nothing, returns canned usage.

Env vars (read per request, so tests can change them):
  MOCK_PROMPT_TOKENS      default 10
  MOCK_COMPLETION_TOKENS  default 20
  MOCK_CONTENT            default "Hello from mock_upstream."
"""

from __future__ import annotations

import os
import time
import uuid
from typing import Any

from fastapi import FastAPI, Request

app = FastAPI(title="mock_upstream")


def _env_int(name: str, default: int) -> int:
    value = int(os.environ.get(name, default))
    if value < 0:
        raise ValueError(f"{name} must be >= 0")
    return value


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/v1/chat/completions")
async def chat_completions(request: Request) -> dict[str, Any]:
    body = await request.json()
    prompt_tokens = _env_int("MOCK_PROMPT_TOKENS", 10)
    completion_tokens = _env_int("MOCK_COMPLETION_TOKENS", 20)
    return {
        "id": f"chatcmpl-mock-{uuid.uuid4().hex[:12]}",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": body.get("model", "mock-model"),
        "choices": [
            {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": os.environ.get("MOCK_CONTENT", "Hello from mock_upstream."),
                },
                "finish_reason": "stop",
            }
        ],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens,
        },
    }
