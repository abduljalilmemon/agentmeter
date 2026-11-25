"""Loop fingerprints (app/loops.py) and streamed tool-call assembly. No Redis needed."""

import json

import pytest

from app import proxy
from app.loops import (
    first_tool_call,
    normalize_message,
    request_fingerprint,
    tool_call_fingerprint,
)


def turn(call_id: str, args: str, result: str = '{"error": "service unavailable"}') -> list:
    """One agent turn: the assistant's tool call and the tool's result."""
    return [
        {
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {
                    "id": call_id,
                    "type": "function",
                    "function": {"name": "get_weather", "arguments": args},
                }
            ],
        },
        {"role": "tool", "tool_call_id": call_id, "content": result},
    ]


BASE = turn("call_1", '{"city": "Paris", "unit": "c"}')


def test_same_across_tool_call_ids() -> None:
    assert request_fingerprint(BASE) == request_fingerprint(
        turn("call_2", '{"city": "Paris", "unit": "c"}')
    )


def test_same_across_argument_key_order_and_whitespace() -> None:
    assert request_fingerprint(BASE) == request_fingerprint(
        turn("call_9", '{ "unit":"c",\n   "city" :  "Paris" }')
    )


def test_same_across_message_key_order() -> None:
    reordered = [dict(reversed(list(m.items()))) for m in BASE]
    assert request_fingerprint(BASE) == request_fingerprint(reordered)


def test_same_across_tool_result_formatting_and_text_whitespace() -> None:
    a = turn("a", "{}", '{"error": "down", "retry": false}')
    b = turn("b", "{}", '{\n  "retry": false,\n  "error": "down"\n}')
    assert request_fingerprint(a) == request_fingerprint(b)
    text_a = [
        {"role": "user", "content": "check  the\nweather"},
        {"role": "assistant", "content": "ok"},
    ]
    text_b = [
        {"role": "user", "content": " check the weather "},
        {"role": "assistant", "content": "ok"},
    ]
    assert request_fingerprint(text_a) == request_fingerprint(text_b)


@pytest.mark.parametrize(
    "other",
    [
        turn("call_1", '{"city": "Lyon", "unit": "c"}'),  # different argument value
        turn("call_1", '{"city": "Paris"}'),  # missing argument
        turn("call_1", '{"city": "Paris", "unit": "c"}', '{"error": "timeout"}'),  # result
    ],
)
def test_differs_across_args_and_results(other: list) -> None:
    assert request_fingerprint(BASE) != request_fingerprint(other)


def test_only_the_last_two_messages_count() -> None:
    history = [{"role": "user", "content": "go"}, *turn("x", "{}"), *BASE]
    assert request_fingerprint(history) == request_fingerprint(BASE)


@pytest.mark.parametrize("messages", [[], [{"role": "user", "content": "hi"}], None, "nope"])
def test_fewer_than_two_messages_have_no_fingerprint(messages) -> None:
    assert request_fingerprint(messages) is None


def test_unparseable_arguments_are_kept_as_text() -> None:
    normalized = normalize_message(turn("c", "not { json")[0])
    assert normalized["tool_calls"][0]["function"]["arguments"] == "not { json"
    assert "id" not in normalized["tool_calls"][0]


def test_tool_call_fingerprint() -> None:
    assert tool_call_fingerprint("f", '{"a": 1, "b": 2}') == tool_call_fingerprint(
        "f", '{"b":2,"a":1}'
    )
    assert tool_call_fingerprint("f", '{"a": 1}') != tool_call_fingerprint("f", '{"a": 2}')
    assert tool_call_fingerprint("f", "{}") != tool_call_fingerprint("g", "{}")
    assert tool_call_fingerprint(None, "{}") is None


def test_first_tool_call_from_response() -> None:
    response = {"choices": [{"message": BASE[0]}]}
    assert first_tool_call(response) == ("get_weather", '{"city": "Paris", "unit": "c"}')
    assert first_tool_call({"choices": [{"message": {"content": "hi"}}]}) is None
    assert first_tool_call({"choices": []}) is None
    assert first_tool_call(None) is None


def _chunk(delta: dict, index: int = 0) -> bytes:
    return f"data: {json.dumps({'choices': [{'index': index, 'delta': delta}]})}\n\n".encode()


def test_streamed_tool_call_is_assembled() -> None:
    result = proxy.StreamResult()
    relay = proxy.SSERelay(result)
    call = {"index": 0, "id": "call_1", "type": "function"}
    stream = [
        _chunk({"role": "assistant"}),
        _chunk({"tool_calls": [{**call, "function": {"name": "get_weather", "arguments": ""}}]}),
        _chunk({"tool_calls": [{"index": 0, "function": {"arguments": '{"city":'}}]}),
        # A second tool call and a second choice must not leak into the first one.
        _chunk({"tool_calls": [{"index": 1, "function": {"name": "other", "arguments": "{}"}}]}),
        _chunk({"tool_calls": [{"index": 0, "function": {"arguments": "x"}}]}, index=1),
        _chunk({"tool_calls": [{"index": 0, "function": {"arguments": ' "Paris"}'}}]}),
        b"data: [DONE]\n\n",
    ]
    data = b"".join(stream)
    for i in range(0, len(data), 7):  # arbitrary network chunking
        relay.feed(data[i : i + 7])
    relay.flush()
    assert result.first_tool_call() == ("get_weather", '{"city": "Paris"}')


def test_streamed_tool_call_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(proxy, "MAX_TOOL_CALL_ARGS_BYTES", 10)
    result = proxy.StreamResult()
    relay = proxy.SSERelay(result)
    relay.feed(
        _chunk({"tool_calls": [{"index": 0, "function": {"name": "f", "arguments": "x" * 11}}]})
    )
    assert result.tool_call_truncated and result.first_tool_call() is None
