"""Loop fingerprints (PLAN §3.2).

Request side: a sha256 over the normalized last two messages. For an agent stuck in a
loop that is typically the assistant's tool call plus the tool's (failing) result, which
repeat with fresh ids every turn.

Normalization makes logically identical requests hash the same:
- ids are dropped: ``tool_call_id``, a tool call's ``id`` and streaming ``index``
- tool-call ``arguments`` (a JSON string) are parsed and re-dumped canonically
- a tool result that is JSON is re-dumped canonically, too
- text has its whitespace collapsed (runs of whitespace become one space, ends stripped)
- the whole structure is dumped with sorted keys and compact separators

Response side: a sha256 of the first tool call's name and canonical arguments.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

_DROPPED_KEYS = frozenset({"id", "tool_call_id", "index"})


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _text(value: str) -> str:
    return " ".join(value.split())


def canonical_arguments(arguments: Any) -> Any:
    """Tool-call arguments as a canonical value: parsed JSON if possible, else text."""
    if isinstance(arguments, str):
        try:
            return json.loads(arguments)
        except ValueError:
            return _text(arguments)
    return arguments


def _tool_call(call: Any) -> Any:
    if not isinstance(call, dict):
        return call
    out = {k: v for k, v in call.items() if k not in _DROPPED_KEYS}
    function = out.get("function")
    if isinstance(function, dict):
        out["function"] = {
            **function,
            "arguments": canonical_arguments(function.get("arguments")),
        }
    return out


def _content(content: Any, *, tool_result: bool) -> Any:
    if isinstance(content, str):
        if tool_result:
            try:
                return {"json": json.loads(content)}
            except ValueError:
                pass
        return _text(content)
    if isinstance(content, list):
        parts = []
        for part in content:
            if isinstance(part, dict) and isinstance(part.get("text"), str):
                part = {**part, "text": _content(part["text"], tool_result=tool_result)}
            parts.append(part)
        return parts
    return content


def normalize_message(message: Any) -> Any:
    if not isinstance(message, dict):
        return message
    out = {k: v for k, v in message.items() if k not in _DROPPED_KEYS}
    if "content" in out:
        out["content"] = _content(out["content"], tool_result=out.get("role") == "tool")
    if isinstance(out.get("tool_calls"), list):
        out["tool_calls"] = [_tool_call(c) for c in out["tool_calls"]]
    return out


def request_fingerprint(messages: Any) -> str | None:
    """Fingerprint of the last two messages, or None if there are fewer than two."""
    if not isinstance(messages, list) or len(messages) < 2:
        return None
    last_two = [normalize_message(m) for m in messages[-2:]]
    return hashlib.sha256(_canonical(last_two).encode()).hexdigest()


def tool_call_fingerprint(name: Any, arguments: Any) -> str | None:
    """Fingerprint of one tool call (name + canonical arguments)."""
    if not isinstance(name, str) or not name:
        return None
    value = {"name": name, "arguments": canonical_arguments(arguments)}
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def first_tool_call(response: Any) -> tuple[str, Any] | None:
    """(name, arguments) of the first tool call in a non-streaming chat completion."""
    if not isinstance(response, dict):
        return None
    choices = response.get("choices")
    if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
        return None
    message = choices[0].get("message")
    calls = message.get("tool_calls") if isinstance(message, dict) else None
    if not isinstance(calls, list) or not calls or not isinstance(calls[0], dict):
        return None
    function = calls[0].get("function")
    if not isinstance(function, dict) or not isinstance(function.get("name"), str):
        return None
    return function["name"], function.get("arguments")
