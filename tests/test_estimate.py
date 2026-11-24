import pytest

from app.config import Settings
from app.estimate import (
    EstimateError,
    count_input_tokens,
    count_tool_tokens,
    effective_max_output,
    encoding_for,
    estimate_micro,
)
from app.pricing import PriceTable, UnknownModel
from tests.conftest import TEST_PRICING

PRICES = PriceTable(Settings(pricing=TEST_PRICING).pricing)

# OpenAI cookbook "How to count tokens with tiktoken": the API reported 124 prompt tokens
# for gpt-4o / gpt-4o-mini and 129 for gpt-4 on exactly these messages.
COOKBOOK_MESSAGES = [
    {
        "role": "system",
        "content": "You are a helpful, pattern-following assistant that translates corporate "
        "jargon into plain English.",
    },
    {
        "role": "system",
        "name": "example_user",
        "content": "New synergies will help drive top-line growth.",
    },
    {
        "role": "system",
        "name": "example_assistant",
        "content": "Things working well together will increase revenue.",
    },
    {
        "role": "system",
        "name": "example_user",
        "content": "Let's circle back when we have more bandwidth to touch base on "
        "opportunities for increased leverage.",
    },
    {
        "role": "system",
        "name": "example_assistant",
        "content": "Let's talk later when we're less busy about how to do better.",
    },
    {
        "role": "user",
        "content": "This late pivot means we don't have time to boil the ocean for the client "
        "deliverable.",
    },
]

HI = [{"role": "user", "content": "hi"}]


def test_cookbook_counts_match_api() -> None:
    assert count_input_tokens("gpt-4o-mini", COOKBOOK_MESSAGES) == 124
    assert count_input_tokens("gpt-4o", COOKBOOK_MESSAGES) == 124
    assert count_input_tokens("gpt-4", COOKBOOK_MESSAGES) == 129


def test_small_fixture() -> None:
    # 3 (message) + "user" (1) + "hi" (1) + 3 (priming)
    assert count_input_tokens("gpt-4o-mini", HI) == 8


def test_fallback_encoding_for_unknown_model() -> None:
    assert encoding_for("gemini-2.5-flash").name == "o200k_base"
    assert encoding_for("gpt-4").name == "cl100k_base"
    assert count_input_tokens("gemini-2.5-flash", COOKBOOK_MESSAGES) == 124


def test_content_parts_and_images() -> None:
    parts = [
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "hi"},
                {"type": "image_url", "image_url": {"url": "https://x/y.png"}},
            ],
        }
    ]
    assert count_input_tokens("gpt-4o-mini", parts, image_part_tokens=100) == 8 + 100
    assert count_input_tokens("gpt-4o-mini", parts, image_part_tokens=0) == 8


def test_tool_calls_counted() -> None:
    msgs = [
        *HI,
        {
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {
                    "id": "call_1",
                    "type": "function",
                    "function": {"name": "get_weather", "arguments": '{"city":"Paris"}'},
                }
            ],
        },
        {"role": "tool", "tool_call_id": "call_1", "content": "sunny"},
    ]
    assert count_input_tokens("gpt-4o-mini", msgs) == 50


def test_tool_definitions_counted() -> None:
    tools = [
        {
            "type": "function",
            "function": {
                "name": "get_weather",
                "description": "Get the weather",
                "parameters": {"type": "object", "properties": {"city": {"type": "string"}}},
            },
        }
    ]
    assert count_tool_tokens("gpt-4o-mini", None) == 0
    # Cookbook formula gives ~35 for this tool; JSON counting rounds up.
    assert count_tool_tokens("gpt-4o-mini", tools) == 53


def test_max_output_precedence() -> None:
    assert effective_max_output({}, 1024) == 1024
    assert effective_max_output({"max_tokens": 50}, 1024) == 50
    assert effective_max_output({"max_completion_tokens": 70, "max_tokens": 50}, 1024) == 70
    assert effective_max_output({"max_completion_tokens": None, "max_tokens": 50}, 1024) == 50
    assert effective_max_output({"max_tokens": 0}, 1024) == 0


@pytest.mark.parametrize("bad", [-1, "10", 1.5, True])
def test_max_output_invalid(bad: object) -> None:
    with pytest.raises(EstimateError):
        effective_max_output({"max_tokens": bad}, 1024)


def test_estimate_micro() -> None:
    body = {"model": "gpt-4o-mini", "messages": HI, "max_tokens": 100}
    # 8 * 150_000 + 100 * 600_000 = 61_200_000 / 1e6 = 61.2 -> 62
    assert estimate_micro(PRICES, "gpt-4o-mini", body, 1024) == 62
    # n=3 triples the output side: (1_200_000 + 300*600_000) / 1e6 = 181.2 -> 182
    assert estimate_micro(PRICES, "gpt-4o-mini", {**body, "n": 3}, 1024) == 182


def test_estimate_covers_mock_usage() -> None:
    body = {"model": "gpt-4o-mini", "messages": HI}
    estimate = estimate_micro(PRICES, "gpt-4o-mini", body, 1024)
    assert estimate >= PRICES.cost_micro("gpt-4o-mini", 10, 20)


def test_estimate_unknown_model() -> None:
    with pytest.raises(UnknownModel):
        estimate_micro(PRICES, "mystery-1", {"messages": HI}, 1024)
