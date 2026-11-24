"""Worst-case cost estimate for a chat completion request (PLAN §3.1).

estimate = input_tokens × input_price + max_output_tokens × n × output_price

The estimate only gates the reservation; settlement uses the real usage. Where the exact
count is unknown, this module rounds *up* (overestimating makes the gate conservative,
underestimating could let a run overspend).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

import tiktoken

from app.pricing import PriceTable

FALLBACK_ENCODING = "o200k_base"

# Chat format overhead, from OpenAI's cookbook "How to count tokens with tiktoken",
# num_tokens_from_messages(), checked 2026-11-24:
# https://github.com/openai/openai-cookbook/blob/main/examples/How_to_count_tokens_with_tiktoken.ipynb
# (also https://developers.openai.com/cookbook/examples/how_to_count_tokens_with_tiktoken)
# The values hold for gpt-3.5-turbo-0125, gpt-4-0613 and gpt-4o(-mini)-2024-*.
TOKENS_PER_MESSAGE = 3
TOKENS_PER_NAME = 1
REPLY_PRIMING_TOKENS = 3  # every reply is primed with <|start|>assistant<|message|>

# Tool definitions: the cookbook's num_tokens_for_tools() uses func_init=7 per function and
# func_end=12 per request for gpt-4o. We keep those constants but count each function as its
# compact JSON, which is longer than the cookbook's "name:desc" lines, so it rounds up and
# also covers nested schemas the cookbook formula ignores.
TOOL_FUNC_INIT = 7
TOOL_FUNC_END = 12

# TODO: image tokens depend on size, detail and model (gpt-4o-mini bills images at a large
# multiplier). A fixed configurable count is a placeholder; replace with a per-model formula.
DEFAULT_IMAGE_PART_TOKENS = 1105


class EstimateError(ValueError):
    """The request body can't be estimated (malformed limits or messages)."""


@lru_cache(maxsize=32)
def encoding_for(model: str) -> tiktoken.Encoding:
    try:
        return tiktoken.encoding_for_model(model)
    except KeyError:
        return tiktoken.get_encoding(FALLBACK_ENCODING)


def _json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def _count_content(enc: tiktoken.Encoding, content: Any, image_part_tokens: int) -> int:
    if content is None:
        return 0
    if isinstance(content, str):
        return len(enc.encode(content))
    if isinstance(content, list):
        total = 0
        for part in content:
            if isinstance(part, dict) and part.get("type") == "text":
                total += len(enc.encode(str(part.get("text", ""))))
            elif isinstance(part, dict) and part.get("type") == "image_url":
                total += image_part_tokens
            else:  # audio, files, refusals, unknown parts: count their JSON (rounds up)
                total += len(enc.encode(_json(part)))
        return total
    return len(enc.encode(_json(content)))


def count_input_tokens(
    model: str,
    messages: list[dict[str, Any]],
    image_part_tokens: int = DEFAULT_IMAGE_PART_TOKENS,
) -> int:
    """Prompt tokens for ``messages`` (cookbook method, extended to parts and tool calls)."""
    if not isinstance(messages, list):
        raise EstimateError("messages must be a list")
    enc = encoding_for(model)
    total = 0
    for message in messages:
        if not isinstance(message, dict):
            raise EstimateError("each message must be an object")
        total += TOKENS_PER_MESSAGE
        for key, value in message.items():
            if value is None:
                continue
            if key == "content":
                total += _count_content(enc, value, image_part_tokens)
            elif key == "tool_calls":
                total += len(enc.encode(_json(value)))
            elif isinstance(value, str):
                total += len(enc.encode(value))
                if key == "name":
                    total += TOKENS_PER_NAME
            else:
                total += len(enc.encode(_json(value)))
    return total + REPLY_PRIMING_TOKENS


def count_tool_tokens(model: str, tools: Any) -> int:
    if not tools:
        return 0
    if not isinstance(tools, list):
        raise EstimateError("tools must be a list")
    enc = encoding_for(model)
    per_tool = sum(TOOL_FUNC_INIT + len(enc.encode(_json(t))) for t in tools)
    return per_tool + TOOL_FUNC_END


def _non_negative_int(body: dict[str, Any], key: str) -> int | None:
    value = body.get(key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise EstimateError(f"{key} must be a non-negative integer")
    return value


def effective_max_output(body: dict[str, Any], default_max_tokens: int) -> int:
    """max_completion_tokens if present, else max_tokens, else the configured default."""
    for key in ("max_completion_tokens", "max_tokens"):
        value = _non_negative_int(body, key)
        if value is not None:
            return value
    return default_max_tokens


def choice_count(body: dict[str, Any]) -> int:
    n = _non_negative_int(body, "n")
    return 1 if n is None else max(n, 1)


def estimate_input_tokens(
    model: str, body: dict[str, Any], image_part_tokens: int = DEFAULT_IMAGE_PART_TOKENS
) -> int:
    return count_input_tokens(
        model, body.get("messages", []), image_part_tokens
    ) + count_tool_tokens(model, body.get("tools"))


def count_text_tokens(model: str, text: str) -> int:
    """Tokens in a piece of generated text (used to settle a stream that lost its usage)."""
    return len(encoding_for(model).encode(text)) if text else 0


@dataclass(frozen=True, slots=True)
class Estimate:
    input_tokens: int
    max_output_tokens: int  # max output per choice × n
    micro: int  # worst-case cost, micro-USD


def estimate(
    prices: PriceTable,
    model: str,
    body: dict[str, Any],
    default_max_tokens: int,
    image_part_tokens: int = DEFAULT_IMAGE_PART_TOKENS,
) -> Estimate:
    """Worst-case token counts and cost. Raises UnknownModel or EstimateError."""
    prices.lookup(model)  # fail fast on unknown models, before tokenizing
    input_tokens = estimate_input_tokens(model, body, image_part_tokens)
    output_tokens = effective_max_output(body, default_max_tokens) * choice_count(body)
    return Estimate(
        input_tokens, output_tokens, prices.cost_micro(model, input_tokens, output_tokens)
    )


def estimate_micro(
    prices: PriceTable,
    model: str,
    body: dict[str, Any],
    default_max_tokens: int,
    image_part_tokens: int = DEFAULT_IMAGE_PART_TOKENS,
) -> int:
    """Worst-case cost in micro-USD. Raises UnknownModel or EstimateError."""
    return estimate(prices, model, body, default_max_tokens, image_part_tokens).micro
