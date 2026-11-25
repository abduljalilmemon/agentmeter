"""Loop detection, step cap and the per-run circuit breaker (M3), through the proxy."""

import asyncio
import json
from collections.abc import Iterator

import httpx
import pytest
import redis
from fastapi.testclient import TestClient
from openai import OpenAI, RateLimitError

from app.breaker import breaker_key, fingerprints_key, steps_key, tool_calls_key
from app.budget import run_key
from app.config import Settings
from app.main import create_app
from mock_upstream.main import app as mock_app
from tests.conftest import make_settings, serve

pytestmark = pytest.mark.usefixtures("redis_db")

MODEL = "gpt-4o-mini"
WEATHER = {"name": "get_weather", "arguments": {"city": "Paris"}}
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_weather",
            "parameters": {"type": "object", "properties": {"city": {"type": "string"}}},
        },
    }
]
BROKEN_RESULT = '{"error": "weather service unavailable"}'
MOCK_CALL_MICRO = 14  # mock usage 10 in + 20 out on gpt-4o-mini
TOKEN = "s3cret-admin"


class Counting(httpx.AsyncBaseTransport):
    """Counts the calls that actually reach the mock upstream."""

    def __init__(self) -> None:
        self.inner = httpx.ASGITransport(app=mock_app)
        self.count = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.count += 1
        return await self.inner.handle_async_request(request)


@pytest.fixture
def upstream() -> Counting:
    return Counting()


def proxy_client(upstream: Counting, **settings: object) -> TestClient:
    return TestClient(create_app(make_settings(**settings), upstream_transport=upstream))


@pytest.fixture
def client(upstream: Counting) -> Iterator[TestClient]:
    with proxy_client(upstream, admin_token=TOKEN) as c:
        yield c


def chat(c: TestClient, messages: list, run: str = "r1", **headers: str) -> httpx.Response:
    hdrs = {"X-AM-Run-Id": run, **{k.replace("_", "-"): v for k, v in headers.items()}}
    return c.post("/v1/chat/completions", json={"model": MODEL, "messages": messages}, headers=hdrs)


def two(i: int | str = 0) -> list:
    """A two-message request; the same i gives the same fingerprint."""
    return [{"role": "user", "content": f"step {i}"}, {"role": "assistant", "content": "ok"}]


def run_budget(r: redis.Redis, run: str) -> dict[str, int]:
    return {k: int(v) for k, v in r.hgetall(run_key(run)).items()}


def assert_breaker_429(resp: httpx.Response, reason: str, run: str = "r1") -> dict:
    assert resp.status_code == 429, resp.text
    assert resp.headers["x-should-retry"] == "false"
    assert resp.headers["x-am-run-id"] == run
    error = resp.json()["error"]
    assert error["type"] == "agentmeter_breaker_open"
    assert error["reason"] == reason
    assert error["run_id"] == run
    assert isinstance(error["spent_usd"], float)
    return error


# --- the broken-tool agent ------------------------------------------------------------


def test_broken_tool_agent_is_stopped_with_reason_loop(
    redis_db: redis.Redis, monkeypatch: pytest.MonkeyPatch, upstream: Counting
) -> None:
    """An SDK agent whose tool always fails resends the same tool call + failing result
    (with fresh ids each turn). The third identical request is refused: call 4 overall,
    since the first call has a single message and no fingerprint."""
    monkeypatch.setenv("MOCK_TOOL_CALL", json.dumps(WEATHER))
    loop = {"repeat_threshold": 3, "consecutive_tool_calls": 100}  # isolate the request side
    with proxy_client(upstream, loop=loop) as c:
        sdk = OpenAI(base_url="http://testserver/v1", api_key="sk-test", http_client=c)
        messages: list = [{"role": "user", "content": "What's the weather in Paris?"}]
        stopped_at, error = None, None
        for call in range(1, 11):
            try:
                resp = sdk.chat.completions.create(
                    model=MODEL,
                    messages=messages,
                    tools=TOOLS,
                    extra_headers={"X-AM-Run-Id": "agent"},
                )
            except RateLimitError as exc:
                stopped_at, error = call, exc.body
                break
            tool_call = resp.choices[0].message.tool_calls[0]
            messages += [
                resp.choices[0].message.model_dump(exclude_none=True),
                {"role": "tool", "tool_call_id": tool_call.id, "content": BROKEN_RESULT},
            ]

        assert stopped_at == 4
        assert error["type"] == "agentmeter_breaker_open"
        assert error["reason"] == "loop"
        assert error["code"] == "loop_detected"
        assert error["spent_usd"] == pytest.approx(3 * MOCK_CALL_MICRO / 1e6)
        assert upstream.count == 3
        spent_when_stopped = run_budget(redis_db, "agent")

        # Every later call is refused without reaching the mock, whatever it sends.
        for i in range(3):
            resp = chat(c, two(f"new {i}"), run="agent")
            assert_breaker_429(resp, "loop", run="agent")
        assert upstream.count == 3
        assert run_budget(redis_db, "agent") == spent_when_stopped  # nothing reserved
        assert spent_when_stopped["reserved"] == 0
    assert redis_db.hget(breaker_key("agent"), "source") == "request_repeats"


# --- response side --------------------------------------------------------------------


@pytest.mark.parametrize("stream", [False, True])
def test_consecutive_tool_calls_trip_after_k(
    redis_db: redis.Redis, monkeypatch: pytest.MonkeyPatch, upstream: Counting, stream: bool
) -> None:
    monkeypatch.setenv("MOCK_TOOL_CALL", json.dumps(WEATHER))
    with proxy_client(upstream, loop={"consecutive_tool_calls": 3}) as c:
        for i in range(3):  # different requests, the same tool call in every response
            body = {"model": MODEL, "messages": two(i), "stream": stream}
            resp = c.post("/v1/chat/completions", json=body, headers={"X-AM-Run-Id": "tc"})
            assert resp.status_code == 200
        assert redis_db.hget(breaker_key("tc"), "source") == "response_tool_calls"
        assert_breaker_429(chat(c, two(99), run="tc"), "loop", run="tc")
    assert upstream.count == 3


def test_tool_call_streak_resets_on_a_different_call(
    redis_db: redis.Redis, monkeypatch: pytest.MonkeyPatch, upstream: Counting
) -> None:
    other = {"name": "get_weather", "arguments": {"city": "Lyon"}}
    with proxy_client(upstream, loop={"consecutive_tool_calls": 3}) as c:
        for i, spec in enumerate([WEATHER, WEATHER, other, WEATHER, WEATHER, None, WEATHER]):
            if spec:
                monkeypatch.setenv("MOCK_TOOL_CALL", json.dumps(spec))
            else:
                monkeypatch.delenv("MOCK_TOOL_CALL")  # a text answer
            assert chat(c, two(i), run="streak").status_code == 200
    assert not redis_db.exists(breaker_key("streak"))
    assert redis_db.hget(tool_calls_key("streak"), "count") == "1"


def test_cut_stream_does_not_count_as_a_tool_call(
    redis_db: redis.Redis, monkeypatch: pytest.MonkeyPatch, upstream: Counting
) -> None:
    monkeypatch.setenv("MOCK_TOOL_CALL", json.dumps(WEATHER))
    monkeypatch.setenv("MOCK_STREAM_CUT", "1")
    upstream.inner = httpx.ASGITransport(app=mock_app, raise_app_exceptions=False)
    with proxy_client(upstream, loop={"consecutive_tool_calls": 2}) as c:
        for i in range(3):
            body = {"model": MODEL, "messages": two(i), "stream": True}
            c.post("/v1/chat/completions", json=body, headers={"X-AM-Run-Id": "cutc"})
    assert not redis_db.exists(breaker_key("cutc"))


# --- step cap, exempt header, overrides -----------------------------------------------


def test_step_cap_trips_at_cap_plus_one(redis_db: redis.Redis, upstream: Counting) -> None:
    with proxy_client(upstream, loop={"step_cap": 5}) as c:
        assert [chat(c, two(i)).status_code for i in range(5)] == [200] * 5
        error = assert_breaker_429(chat(c, two(5)), "step_cap")
        assert error["code"] == "step_cap_reached"
        assert error["steps"] == 6
        assert error["spent_usd"] == pytest.approx(5 * MOCK_CALL_MICRO / 1e6)
        assert_breaker_429(chat(c, two(6)), "step_cap")  # stays open
    assert upstream.count == 5
    assert int(redis_db.get(steps_key("r1"))) == 6  # refused-while-open calls don't count


def test_loop_exempt_skips_fingerprints_but_not_the_step_cap(
    redis_db: redis.Redis, monkeypatch: pytest.MonkeyPatch, upstream: Counting
) -> None:
    monkeypatch.setenv("MOCK_TOOL_CALL", json.dumps(WEATHER))  # same tool call every time
    with proxy_client(upstream, loop={"step_cap": 6, "consecutive_tool_calls": 2}) as c:
        # Identical requests and identical tool calls: would trip at 2-3 without the header.
        statuses = [chat(c, two(), X_AM_Loop_Exempt="true").status_code for _ in range(6)]
        assert statuses == [200] * 6
        assert_breaker_429(chat(c, two(), X_AM_Loop_Exempt="true"), "step_cap")
    assert not redis_db.exists(fingerprints_key("r1"))
    assert not redis_db.exists(tool_calls_key("r1"))


def test_loop_exempt_header_must_be_a_bool(client: TestClient) -> None:
    resp = chat(client, two(), X_AM_Loop_Exempt="sometimes")
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "invalid_header"


def test_per_agent_overrides(redis_db: redis.Redis, upstream: Counting) -> None:
    loop = {
        "repeat_threshold": 3,
        "step_cap": 50,
        "agents": {"poller": {"repeat_threshold": 5}, "short": {"step_cap": 2}},
    }
    with proxy_client(upstream, loop=loop) as c:
        default = [chat(c, two(), run="d").status_code for _ in range(3)]
        poller = [chat(c, two(), run="p", X_AM_Agent="poller").status_code for _ in range(5)]
        short = [chat(c, two(i), run="s", X_AM_Agent="short").status_code for i in range(3)]
    assert default == [200, 200, 429]  # third identical request opens it
    assert poller == [200, 200, 200, 200, 429]  # the poller is allowed 4 repeats
    assert short == [200, 200, 429]  # step cap 2: call 3 is refused
    assert redis_db.hget(breaker_key("s"), "reason") == "step_cap"


def test_loop_config_overrides_parse() -> None:
    cfg = Settings(loop={"agents": {"poller": {"repeat_threshold": 10, "step_cap": 500}}}).loop
    assert cfg.limits_for("poller").repeat_threshold == 10
    assert cfg.limits_for("poller").step_cap == 500
    assert cfg.limits_for("poller").consecutive_tool_calls == 3
    assert cfg.limits_for("someone-else").repeat_threshold == 3
    with pytest.raises(ValueError):
        Settings(loop={"agents": {"poller": {"repeat_treshold": 10}}})  # typo is caught


# --- state in Redis -------------------------------------------------------------------


def test_refused_call_reserves_nothing(redis_db: redis.Redis, client: TestClient) -> None:
    for _ in range(2):
        assert chat(client, two()).status_code == 200
    before = run_budget(redis_db, "r1")
    assert_breaker_429(chat(client, two()), "loop")
    assert run_budget(redis_db, "r1") == before
    assert before == {"limit": 500_000, "spent": 2 * MOCK_CALL_MICRO, "reserved": 0}


def test_breaker_keys_share_the_run_ttl(redis_db: redis.Redis, client: TestClient) -> None:
    for _ in range(3):
        chat(client, two())
    run_ttl = redis_db.ttl(run_key("r1"))
    for key in (breaker_key("r1"), steps_key("r1"), fingerprints_key("r1")):
        assert abs(redis_db.ttl(key) - run_ttl) <= 2, key


def test_redis_down_fails_closed_at_the_gate(upstream: Counting) -> None:
    from tests.conftest import NO_REDIS

    with proxy_client(upstream, redis_url=NO_REDIS) as c:
        resp = chat(c, two())
    assert resp.status_code == 503
    assert upstream.count == 0


# --- concurrency ----------------------------------------------------------------------


@pytest.mark.concurrency
def test_parallel_identical_calls_let_at_most_two_through(redis_db: redis.Redis) -> None:
    upstreams = [Counting(), Counting()]
    settings = make_settings(loop={"repeat_threshold": 3})
    body = {"model": MODEL, "messages": two()}

    async def fire(urls: list[str]) -> list[httpx.Response]:
        limits = httpx.Limits(max_connections=20, max_keepalive_connections=20)
        async with httpx.AsyncClient(timeout=30, limits=limits) as http:
            # Open the 20 connections first, so the calls below arrive as one burst
            # instead of being spread out by connection setup.
            await asyncio.gather(*(http.get(f"{urls[i % 2]}/healthz") for i in range(20)))
            return await asyncio.gather(
                *(
                    http.post(
                        f"{urls[i % 2]}/v1/chat/completions",
                        json=body,
                        headers={"X-AM-Run-Id": "par"},
                    )
                    for i in range(20)
                )
            )

    apps = [create_app(settings, upstream_transport=u) for u in upstreams]
    with serve(apps[0]) as a, serve(apps[1]) as b:
        responses = asyncio.run(fire([a, b]))
    reached = sum(u.count for u in upstreams)
    assert reached <= 2  # the spec's bound
    assert reached == 2  # and with an atomic gate, exactly: sightings 1 and 2 pass
    assert sum(r.status_code == 200 for r in responses) == reached
    assert sum(r.status_code == 429 for r in responses) == 20 - reached
    assert all(r.json()["error"]["reason"] == "loop" for r in responses if r.status_code == 429)


# --- admin reset ----------------------------------------------------------------------


def reset(c: TestClient, run: str = "r1", token: str | None = TOKEN) -> httpx.Response:
    headers = {"Authorization": f"Bearer {token}"} if token is not None else {}
    return c.post(f"/admin/runs/{run}/reset", headers=headers)


def test_reset_without_configured_token_is_404(upstream: Counting) -> None:
    with proxy_client(upstream) as c:
        assert reset(c).status_code == 404
        assert reset(c, token="").status_code == 404
    with proxy_client(upstream, admin_token="  ") as c:  # blank counts as unset
        assert reset(c).status_code == 404


@pytest.mark.parametrize("token", ["wrong", None, ""])
def test_reset_with_wrong_token_is_401(client: TestClient, token: str | None) -> None:
    resp = reset(client, token=token)
    assert resp.status_code == 401
    assert resp.headers["www-authenticate"] == "Bearer"


def test_reset_rejects_basic_auth(client: TestClient) -> None:
    resp = client.post("/admin/runs/r1/reset", headers={"Authorization": f"Basic {TOKEN}"})
    assert resp.status_code == 401


def test_reset_reopens_the_run_and_keeps_its_money(
    redis_db: redis.Redis, client: TestClient, upstream: Counting
) -> None:
    for _ in range(2):
        assert chat(client, two()).status_code == 200
    assert_breaker_429(chat(client, two()), "loop")
    money = run_budget(redis_db, "r1")

    resp = reset(client)
    assert resp.status_code == 200
    body = resp.json()
    assert body["reset"] is True and body["run_id"] == "r1"
    assert body["breaker_was"]["reason"] == "loop"
    assert body["breaker_was"]["source"] == "request_repeats"
    assert body["breaker_was"]["steps"] == 3
    assert isinstance(body["breaker_was"]["opened_ms"], int)
    assert body["steps_was"] == 3
    assert run_budget(redis_db, "r1") == money  # reset never touches budgets
    for key in (breaker_key("r1"), steps_key("r1"), fingerprints_key("r1")):
        assert not redis_db.exists(key)

    # The run works again: the same request is allowed (its count started over).
    assert chat(client, two()).status_code == 200
    assert run_budget(redis_db, "r1")["spent"] == money["spent"] + MOCK_CALL_MICRO
    assert upstream.count == 3


def test_reset_clears_the_step_count(redis_db: redis.Redis, upstream: Counting) -> None:
    with proxy_client(upstream, loop={"step_cap": 2}, admin_token=TOKEN) as c:
        assert [chat(c, two(i)).status_code for i in range(3)] == [200, 200, 429]
        assert reset(c).json()["breaker_was"]["reason"] == "step_cap"
        assert [chat(c, two(i)).status_code for i in range(3, 6)] == [200, 200, 429]


def test_reset_of_a_closed_run(client: TestClient) -> None:
    resp = reset(client, run="never-seen")
    assert resp.status_code == 200
    assert resp.json() == {
        "run_id": "never-seen",
        "reset": True,
        "breaker_was": None,
        "steps_was": 0,
    }


def test_reset_rejects_an_invalid_run_id(client: TestClient) -> None:
    assert reset(client, run="bad*id").status_code == 400


def test_alternating_loop_trips(redis_db: redis.Redis, upstream: Counting) -> None:
    """A, B, A, B, A: no two consecutive requests are equal, but A's third sighting in the
    run opens the breaker (the count is per fingerprint per run, not vs the last call)."""
    with proxy_client(upstream, loop={"repeat_threshold": 3}) as c:
        statuses = [chat(c, two(step), run="alt").status_code for step in "ABABA"]
        assert statuses == [200, 200, 200, 200, 429]
        assert_breaker_429(chat(c, two("C"), run="alt"), "loop", run="alt")
    assert redis_db.hget(breaker_key("alt"), "source") == "request_repeats"
    assert upstream.count == 4
