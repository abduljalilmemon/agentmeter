"""Budgets enforced through the proxy: real Redis, mock upstream."""

import json
import re

import httpx
import pytest
import redis
from fastapi.testclient import TestClient
from openai import OpenAI, RateLimitError

from app.budget import RESERVATIONS_KEY, run_key
from app.estimate import count_text_tokens
from app.main import create_app
from mock_upstream.main import app as mock_app
from tests.conftest import NO_REDIS, make_settings

REQUEST = {"model": "gpt-4o-mini", "messages": [{"role": "user", "content": "hi"}]}
MOCK_CALL_MICRO = 14  # 10 in × 0.15 + 20 out × 0.60 per 1M tokens = 13.5 µUSD → 14
ESTIMATE_MICRO = 616  # 8 in + 1024 (default max_tokens) out on gpt-4o-mini


def _client(**settings: object) -> TestClient:
    return TestClient(
        create_app(make_settings(**settings), upstream_transport=httpx.ASGITransport(app=mock_app))
    )


def run_state(r: redis.Redis, run_id: str) -> dict[str, int]:
    return {k: int(v) for k, v in r.hgetall(run_key(run_id)).items()}


def test_spent_equals_sum_of_mock_usage(redis_db: redis.Redis) -> None:
    n = 12
    with _client() as c:
        for i in range(n):
            body = {**REQUEST, "stream": i % 2 == 1}  # mix of plain and streaming calls
            resp = c.post("/v1/chat/completions", json=body, headers={"X-AM-Run-Id": "sum"})
            assert resp.status_code == 200
    state = run_state(redis_db, "sum")
    assert state["spent"] == n * MOCK_CALL_MICRO
    assert state["reserved"] == 0
    assert redis_db.zcard(RESERVATIONS_KEY) == 0
    tombstones = redis_db.keys("am:r:*")
    assert len(tombstones) == n  # one per call, and nothing but the settled marker in each
    assert all(redis_db.hgetall(k) == {"settled": "1"} for k in tombstones)


def test_exhausted_run_gets_429(redis_db: redis.Redis) -> None:
    # 700 µUSD: a call needs 616 of headroom and costs 14, so exactly 7 fit:
    # after 6 calls spent=84 (84+616=700 fits), after 7 spent=98 (98+616 > 700).
    headers = {"X-AM-Run-Id": "tight", "X-AM-Run-Budget": "0.0007"}
    with _client() as c:
        statuses = [
            c.post("/v1/chat/completions", json=REQUEST, headers=headers).status_code
            for _ in range(10)
        ]
        resp = c.post("/v1/chat/completions", json=REQUEST, headers=headers)
    assert statuses == [200] * 7 + [429] * 3
    assert resp.status_code == 429
    assert resp.headers["x-am-run-id"] == "tight"
    assert resp.headers["x-should-retry"] == "false"
    error = resp.json()["error"]
    assert error["type"] == "agentmeter_breaker_open"
    assert error["reason"] == "budget"
    assert error["level"] == "run"
    assert error["run_id"] == "tight"
    assert error["spent_usd"] == pytest.approx(7 * MOCK_CALL_MICRO / 1e6)
    assert error["limit_usd"] == pytest.approx(0.0007)
    assert error["estimate_usd"] == pytest.approx(ESTIMATE_MICRO / 1e6)
    assert run_state(redis_db, "tight") == {"limit": 700, "spent": 98, "reserved": 0}


def test_rejected_call_never_reaches_upstream(redis_db: redis.Redis) -> None:
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={"usage": {"prompt_tokens": 1, "completion_tokens": 1}})

    app = create_app(make_settings("http://up/v1"), upstream_transport=httpx.MockTransport(handler))
    with TestClient(app) as c:
        resp = c.post("/v1/chat/completions", json=REQUEST, headers={"X-AM-Run-Budget": "0"})
    assert resp.status_code == 429
    assert calls == []


def test_sdk_sees_rate_limit_error_without_retrying(redis_db: redis.Redis) -> None:
    with _client() as c:
        sdk = OpenAI(base_url="http://testserver/v1", api_key="sk-test", http_client=c)
        with pytest.raises(RateLimitError) as excinfo:
            sdk.chat.completions.create(**REQUEST, extra_headers={"X-AM-Run-Budget": "0.0001"})
    assert excinfo.value.body["type"] == "agentmeter_breaker_open"
    assert excinfo.value.response.headers["x-should-retry"] == "false"


def test_agent_budget_enforced_through_proxy(redis_db: redis.Redis) -> None:
    budgets = {"agents": {"bot": {"daily_usd": "0.0007"}}}
    with _client(budgets=budgets) as c:
        statuses = [
            c.post(
                "/v1/chat/completions",
                json=REQUEST,
                headers={"X-AM-Run-Id": f"run-{i}", "X-AM-Agent": "bot"},  # fresh run each time
            ).status_code
            for i in range(9)
        ]
    assert statuses == [200] * 7 + [429] * 2


@pytest.mark.parametrize("status", [400, 500])
def test_upstream_error_settles_zero(
    redis_db: redis.Redis, monkeypatch: pytest.MonkeyPatch, status: int
) -> None:
    monkeypatch.setenv("MOCK_FORCE_STATUS", str(status))
    with _client() as c:
        for stream in (False, True):
            resp = c.post(
                "/v1/chat/completions",
                json={**REQUEST, "stream": stream},
                headers={"X-AM-Run-Id": "err"},
            )
            assert resp.status_code == status
    assert run_state(redis_db, "err") == {"limit": 500_000, "spent": 0, "reserved": 0}


def test_connection_failure_settles_zero(redis_db: redis.Redis) -> None:
    with TestClient(create_app(make_settings("http://127.0.0.1:1/v1"))) as c:
        resp = c.post("/v1/chat/completions", json=REQUEST, headers={"X-AM-Run-Id": "down"})
    assert resp.status_code == 502
    assert run_state(redis_db, "down") == {"limit": 500_000, "spent": 0, "reserved": 0}


def test_timeout_settles_worst_case(redis_db: redis.Redis) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow", request=request)

    app = create_app(make_settings("http://up/v1"), upstream_transport=httpx.MockTransport(handler))
    with TestClient(app) as c:
        resp = c.post("/v1/chat/completions", json=REQUEST, headers={"X-AM-Run-Id": "slow"})
    assert resp.status_code == 504
    state = run_state(redis_db, "slow")
    assert (state["spent"], state["reserved"]) == (ESTIMATE_MICRO, 0)


def _mock_stream_output_tokens() -> int:
    pieces = re.findall(r"\S+\s*", "Hello from mock_upstream.")
    return sum(count_text_tokens("gpt-4o-mini", p) for p in ["", *pieces])


def _tiktoken_settlement() -> int:
    # Estimated prompt (8 tokens) + tiktoken count of the streamed deltas, gpt-4o-mini prices.
    out = _mock_stream_output_tokens()
    return -(-(8 * 150_000 + out * 600_000) // 1_000_000)


def test_cut_stream_settles_with_tiktoken_count(
    redis_db: redis.Redis,
    live_mock: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MOCK_STREAM_CUT", "1")
    with TestClient(create_app(make_settings(live_mock))) as c:
        resp = c.post(
            "/v1/chat/completions",
            json={**REQUEST, "stream": True},
            headers={"X-AM-Run-Id": "cut"},
        )
    last = json.loads(resp.text.strip().splitlines()[-1][6:])
    assert last["error"]["type"] == "agentmeter_upstream_cut"
    state = run_state(redis_db, "cut")
    assert state["spent"] == _tiktoken_settlement()
    assert state["reserved"] == 0


def test_clean_end_without_done_settles_as_cut(
    redis_db: redis.Redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MOCK_STREAM_CUT", "1")
    app = create_app(
        make_settings(),
        upstream_transport=httpx.ASGITransport(app=mock_app, raise_app_exceptions=False),
    )
    with TestClient(app) as c:
        resp = c.post(
            "/v1/chat/completions", json={**REQUEST, "stream": True}, headers={"X-AM-Run-Id": "eof"}
        )
    assert "upstream_incomplete" in resp.text
    assert run_state(redis_db, "eof")["spent"] == _tiktoken_settlement()
    assert run_state(redis_db, "eof")["reserved"] == 0


def test_redis_down_fails_closed() -> None:
    with TestClient(create_app(make_settings(redis_url=NO_REDIS))) as c:
        resp = c.post("/v1/chat/completions", json=REQUEST)
    assert resp.status_code == 503
    assert resp.json()["error"]["type"] == "agentmeter_budget_unavailable"
