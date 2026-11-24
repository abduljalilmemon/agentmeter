import pytest
from fastapi.testclient import TestClient

from mock_upstream.main import app

client = TestClient(app)
REQUEST = {"model": "gpt-4o-mini", "messages": [{"role": "user", "content": "hi"}]}


def test_default_usage() -> None:
    resp = client.post("/v1/chat/completions", json=REQUEST)
    assert resp.status_code == 200
    body = resp.json()
    assert body["object"] == "chat.completion"
    assert body["model"] == "gpt-4o-mini"
    assert body["choices"][0]["message"]["role"] == "assistant"
    assert body["usage"] == {"prompt_tokens": 10, "completion_tokens": 20, "total_tokens": 30}


def test_usage_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOCK_PROMPT_TOKENS", "123")
    monkeypatch.setenv("MOCK_COMPLETION_TOKENS", "456")
    usage = client.post("/v1/chat/completions", json=REQUEST).json()["usage"]
    assert usage == {"prompt_tokens": 123, "completion_tokens": 456, "total_tokens": 579}
