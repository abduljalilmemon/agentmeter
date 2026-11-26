"""GET /runs/{id} (M5): totals kept by the Lua scripts, breaker state and events, auth."""

import json

import httpx
import pytest
import redis
from fastapi.testclient import TestClient

from app.breaker import history_key
from app.budget import run_key, run_stats_key
from app.events import STREAM_KEY
from app.main import create_app
from mock_upstream.main import app as mock_app
from tests.conftest import make_settings

pytestmark = pytest.mark.usefixtures("redis_db")

TOKEN = "report-token"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
MODEL = "gpt-4o-mini"


def client(**settings: object) -> TestClient:
    settings.setdefault("admin_token", TOKEN)
    app = create_app(
        make_settings(**settings), upstream_transport=httpx.ASGITransport(app=mock_app)
    )
    return TestClient(app)


def call(c: TestClient, run: str, i: int, stream: bool = False, **headers: str):
    hdrs = {"X-AM-Run-Id": run, **{k.replace("_", "-"): v for k, v in headers.items()}}
    messages = [{"role": "user", "content": f"q{i}"}, {"role": "assistant", "content": "ok"}]
    body = {"model": MODEL, "messages": messages, "stream": stream}
    return c.post("/v1/chat/completions", json=body, headers=hdrs)


def settled(r: redis.Redis, run: str) -> list[dict[str, str]]:
    return [f for _id, f in r.xrange(STREAM_KEY) if f["type"] == "call_settled" and f["run"] == run]


def test_report_matches_the_sum_of_call_settled_events(
    redis_db: redis.Redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    with client() as c:
        plan = [(False, 10, 20), (True, 10, 20), (False, 7, 3), (True, 50, 100), (False, 1, 1)]
        for i, (stream, prompt, completion) in enumerate(plan):
            monkeypatch.setenv("MOCK_PROMPT_TOKENS", str(prompt))
            monkeypatch.setenv("MOCK_COMPLETION_TOKENS", str(completion))
            resp = call(c, "rep", i, stream=stream, X_AM_Agent="bot", X_AM_Team="core")
            assert resp.status_code == 200
        report = c.get("/runs/rep", headers=AUTH).json()

    events = settled(redis_db, "rep")
    assert len(events) == len(plan)

    def total(field: str) -> int:
        return sum(int(e[field]) for e in events)

    assert report["run_id"] == "rep"
    assert (report["agent"], report["team"]) == ("bot", "core")
    assert report["calls"] == len(plan)
    assert report["steps"] == len(plan)
    assert report["tokens"] == {
        "prompt": total("prompt_tokens"),
        "completion": total("completion_tokens"),
        "total": total("prompt_tokens") + total("completion_tokens"),
    }
    assert report["tokens"]["prompt"] == sum(p for _, p, _ in plan)
    assert report["cost_micro"] == total("cost_micro")
    assert report["estimate_micro"] == total("estimate_micro")
    assert report["cost_usd"] == pytest.approx(total("cost_micro") / 1e6)
    budget = {k: int(v) for k, v in redis_db.hgetall(run_key("rep")).items()}
    assert report["spent_micro"] == budget["spent"] == report["cost_micro"]
    assert (report["limit_micro"], report["reserved_micro"]) == (budget["limit"], 0)
    assert report["limit_usd"] == pytest.approx(budget["limit"] / 1e6)
    assert report["breaker"] == {
        "state": "closed",
        "reason": None,
        "source": None,
        "steps": None,
        "opened_ms": None,
    }
    assert report["breaker_events"] == []


def test_report_counts_zero_cost_calls(redis_db: redis.Redis, monkeypatch) -> None:
    monkeypatch.setenv("MOCK_FORCE_STATUS", "500")
    with client() as c:
        assert call(c, "err", 0).status_code == 500
        report = c.get("/runs/err", headers=AUTH).json()
    assert (report["calls"], report["cost_micro"], report["tokens"]["total"]) == (1, 0, 0)


def test_breaker_events_show_up(redis_db: redis.Redis, monkeypatch) -> None:
    loop = {"repeat_threshold": 2, "consecutive_tool_calls": 2}
    with client(loop=loop) as c:
        call(c, "brk", 0)
        assert call(c, "brk", 0).status_code == 429  # request-side loop opens
        report = c.get("/runs/brk", headers=AUTH).json()
        assert report["breaker"]["state"] == "open"
        assert report["breaker"]["reason"] == "loop"
        assert report["breaker"]["source"] == "request_repeats"
        (opened,) = report["breaker_events"]
        assert opened["event"] == "open" and opened["reason"] == "loop"
        assert opened["steps"] == 2 and isinstance(opened["at_ms"], int)

        assert c.post("/admin/runs/brk/reset", headers=AUTH).status_code == 200
        monkeypatch.setenv("MOCK_TOOL_CALL", json.dumps({"name": "f", "arguments": {}}))
        for i in range(1, 4):  # response side: opens after 2 identical tool calls
            call(c, "brk", i)
        report = c.get("/runs/brk", headers=AUTH).json()
    assert report["breaker"]["source"] == "response_tool_calls"
    assert report["breaker"]["steps"] is None  # documented: no step count for this source
    last = report["breaker_events"][-1]
    assert last["count"] == 2 and "steps" not in last
    assert [(e["event"], e.get("source")) for e in report["breaker_events"]] == [
        ("open", "request_repeats"),
        ("reset", None),
        ("open", "response_tool_calls"),
    ]
    assert report["calls"] == 3  # 1 before the loop + 2 tool-call responses


def test_step_cap_event(redis_db: redis.Redis) -> None:
    with client(loop={"step_cap": 2}) as c:
        for i in range(3):
            call(c, "cap", i)
        report = c.get("/runs/cap", headers=AUTH).json()
    assert report["breaker"]["reason"] == "step_cap"
    assert [e["reason"] for e in report["breaker_events"]] == ["step_cap"]
    assert (report["calls"], report["steps"]) == (2, 3)


def test_history_is_capped(redis_db: redis.Redis) -> None:
    with client(loop={"step_cap": 1}) as c:
        for i in range(25):  # open, reset, open, reset, ...
            call(c, "many", 2 * i)
            call(c, "many", 2 * i + 1)
            c.post("/admin/runs/many/reset", headers=AUTH)
        report = c.get("/runs/many", headers=AUTH).json()
    assert len(report["breaker_events"]) == 20
    assert report["breaker_events"][-1]["event"] == "reset"


def test_report_keys_share_the_run_ttl(redis_db: redis.Redis) -> None:
    with client(loop={"repeat_threshold": 2}) as c:
        call(c, "ttl", 0)
        call(c, "ttl", 0)
    run_ttl = redis_db.ttl(run_key("ttl"))
    assert run_ttl > 0
    for key in (run_stats_key("ttl"), history_key("ttl")):
        assert abs(redis_db.ttl(key) - run_ttl) <= 2, key


# --- access ---------------------------------------------------------------------------


def test_unknown_run_is_404(redis_db: redis.Redis) -> None:
    with client() as c:
        resp = c.get("/runs/never-seen", headers=AUTH)
    assert resp.status_code == 404
    assert resp.json() == {"detail": "Unknown run"}


@pytest.mark.parametrize("auth", [{}, {"Authorization": "Bearer wrong"}, {"Authorization": TOKEN}])
def test_wrong_or_missing_token_is_401(redis_db: redis.Redis, auth: dict) -> None:
    with client() as c:
        call(c, "secret", 0)
        resp = c.get("/runs/secret", headers=auth)
    assert resp.status_code == 401
    assert resp.headers["www-authenticate"] == "Bearer"


def test_no_admin_token_configured_is_404(redis_db: redis.Redis) -> None:
    with client(admin_token=None) as c:
        call(c, "r", 0)
        assert c.get("/runs/r", headers=AUTH).status_code == 404


def test_invalid_run_id_is_400(redis_db: redis.Redis) -> None:
    with client() as c:
        assert c.get("/runs/bad*id", headers=AUTH).status_code == 400
