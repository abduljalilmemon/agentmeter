"""Prometheus metrics (M5): values for known mock usage, trips once per opening, labels."""

import asyncio
import json
from collections.abc import AsyncIterator

import httpx
import pytest
import redis
from fastapi.testclient import TestClient

from app.budget import BudgetClient
from app.main import create_app
from mock_upstream.main import app as mock_app
from tests.conftest import make_settings

pytestmark = pytest.mark.usefixtures("redis_db")

MODEL = "gpt-4o-mini"
CALL_USD = 14 / 1e6  # mock usage 10 in + 20 out on gpt-4o-mini = 14 micro-USD


def two(i: object = 0) -> list:
    return [{"role": "user", "content": f"step {i}"}, {"role": "assistant", "content": "ok"}]


def post(c: TestClient, run: str, messages: list | None = None, stream=False, **headers):
    hdrs = {"X-AM-Run-Id": run, **{k.replace("_", "-"): v for k, v in headers.items()}}
    body = {"model": MODEL, "messages": messages or two(), "stream": stream}
    return c.post("/v1/chat/completions", json=body, headers=hdrs)


def sample(c: TestClient, name: str, **labels: str) -> float:
    value = c.app.state.metrics.registry.get_sample_value(name, labels)
    return 0.0 if value is None else value


def client(**settings: object) -> TestClient:
    app = create_app(
        make_settings(**settings), upstream_transport=httpx.ASGITransport(app=mock_app)
    )
    return TestClient(app)


def test_spend_counter_moves_by_mock_usage(redis_db: redis.Redis) -> None:
    with client() as c:
        for i in range(3):  # plain, streaming, plain
            resp = post(c, "s1", two(i), stream=i == 1, X_AM_Agent="bot", X_AM_Team="core")
            assert resp.status_code == 200
        assert post(c, "s2", two()).status_code == 200  # no agent / team headers
        labels = {"agent": "bot", "team": "core"}
        assert sample(c, "am_spend_usd_total", **labels) == pytest.approx(3 * CALL_USD)
        assert sample(c, "am_spend_usd_total", agent="", team="") == pytest.approx(CALL_USD)
        # The counter matches the integer budget exactly (after the export-edge division).
        assert int(redis_db.hget("am:b:run:s1", "spent")) == 3 * 14


def test_requests_total_by_status_reason_and_level(redis_db: redis.Redis) -> None:
    with client(loop={"repeat_threshold": 2}) as c:
        assert post(c, "q1", two("a")).status_code == 200
        assert post(c, "q1", two("a")).status_code == 429  # loop opens on repeat 2
        assert post(c, "q2", X_AM_Run_Budget="0").status_code == 429  # budget, run level
        assert post(c, "q3", X_AM_Run_Id="bad*id").status_code == 400
        assert sample(c, "am_requests_total", status="200", reason="", level="") == 1
        assert sample(c, "am_requests_total", status="429", reason="loop", level="") == 1
        assert sample(c, "am_requests_total", status="429", reason="budget", level="run") == 1
        assert sample(c, "am_requests_total", status="400", reason="", level="") == 1


def test_breaker_trips_counted_once_per_opening(
    redis_db: redis.Redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    loop = {"repeat_threshold": 3, "step_cap": 4, "consecutive_tool_calls": 2}
    with client(loop=loop) as c:
        for _ in range(6):  # opens on call 3; calls 4-6 are refused while open
            post(c, "loop-run", two())
        assert sample(c, "am_breaker_trips_total", reason="loop") == 1
        for i in range(7):  # opens on call 5 (step_cap + 1), then stays open
            post(c, "cap-run", two(i))
        assert sample(c, "am_breaker_trips_total", reason="step_cap") == 1
        monkeypatch.setenv("MOCK_TOOL_CALL", json.dumps({"name": "f", "arguments": {"x": 1}}))
        for i in range(4):  # response side: opens after the 2nd identical tool call
            post(c, "tc-run", two(f"t{i}"))
        assert sample(c, "am_breaker_trips_total", reason="loop") == 2
        assert sample(c, "am_requests_total", status="429", reason="loop", level="") == 6


class SlowUpstream(httpx.AsyncBaseTransport):
    """Answers after ``delay``; a stream sends headers at once and its body slowly."""

    def __init__(self, delay: float) -> None:
        self.delay = delay

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        usage = {"prompt_tokens": 10, "completion_tokens": 20}
        if body.get("stream"):
            delay = self.delay

            class Body(httpx.AsyncByteStream):
                async def __aiter__(self) -> AsyncIterator[bytes]:
                    await asyncio.sleep(delay)
                    yield f"data: {json.dumps({'choices': [], 'usage': usage})}\n\n".encode()
                    yield b"data: [DONE]\n\n"

            return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=Body())
        await asyncio.sleep(self.delay)
        return httpx.Response(200, json={"choices": [], "usage": usage})


@pytest.mark.parametrize("stream", [False, True])
def test_overhead_excludes_the_upstream(redis_db: redis.Redis, stream: bool) -> None:
    app = create_app(make_settings("http://up/v1"), upstream_transport=SlowUpstream(0.4))
    with TestClient(app) as c:
        for i in range(3):
            assert post(c, f"oh-{stream}", two(i), stream=stream).status_code == 200
        count = sample(c, "am_proxy_overhead_seconds_count", outcome="forwarded")
        total = sample(c, "am_proxy_overhead_seconds_sum", outcome="forwarded")
        assert sample(c, "am_proxy_overhead_seconds_count", outcome="refused") == 0
    assert count == 3
    # Each call spends 0.4 s upstream (in the response for plain calls, in the stream body
    # for streams); none of it may show up as proxy overhead.
    assert 0 < total < 3 * 0.2, total


def test_settle_failures_counted(redis_db: redis.Redis, monkeypatch: pytest.MonkeyPatch) -> None:
    async def broken(self, reservation, actual, details=None):
        raise redis.ConnectionError("down")

    monkeypatch.setattr(BudgetClient, "settle", broken)
    with client() as c:
        assert post(c, "sf").status_code == 200
        assert sample(c, "am_settle_failures_total") == 1
        assert sample(c, "am_spend_usd_total", agent="", team="") == 0  # nothing recorded


def test_metrics_endpoint_has_no_run_id_label(redis_db: redis.Redis) -> None:
    runs = ["metrics-run-aaa", "metrics-run-bbb"]
    with client(loop={"repeat_threshold": 2}) as c:
        for run in runs:
            post(c, run, X_AM_Agent="bot")
            post(c, run, X_AM_Agent="bot")  # opens the breaker
        resp = c.get("/metrics")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/plain")
    text = resp.text
    for name in (
        "am_spend_usd_total",
        "am_breaker_trips_total",
        "am_proxy_overhead_seconds_bucket",
        "am_settle_failures_total",
        "am_requests_total",
    ):
        assert name in text, name
    assert "run_id" not in text
    assert not any(run in text for run in runs)
    labels = {
        part.split("=")[0]
        for line in text.splitlines()
        if line.startswith("am_") and "{" in line
        for part in line[line.index("{") + 1 : line.index("}")].split(",")
    }
    assert labels <= {"agent", "team", "reason", "status", "level", "outcome", "le"}, labels


def test_overhead_outcome_label(redis_db: redis.Redis) -> None:
    with client(loop={"repeat_threshold": 2}) as c:
        assert post(c, "o1", two("a")).status_code == 200  # forwarded
        assert post(c, "o1", two("b"), stream=True).status_code == 200  # forwarded
        assert post(c, "o1", two("a")).status_code == 429  # refused: loop
        assert post(c, "o2", X_AM_Run_Budget="0").status_code == 429  # refused: budget
        assert post(c, "o3", X_AM_Run_Id="bad*id").status_code == 400  # refused: header
        forwarded = sample(c, "am_proxy_overhead_seconds_count", outcome="forwarded")
        refused = sample(c, "am_proxy_overhead_seconds_count", outcome="refused")
    assert (forwarded, refused) == (2, 3)


def test_upstream_failure_still_counts_as_forwarded(redis_db: redis.Redis) -> None:
    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    app = create_app(make_settings("http://up/v1"), upstream_transport=httpx.MockTransport(refuse))
    with TestClient(app) as c:
        assert post(c, "uf").status_code == 502
        assert sample(c, "am_proxy_overhead_seconds_count", outcome="forwarded") == 1


def test_each_replica_runs_one_uvicorn_process() -> None:
    """Metrics live in process memory (no prometheus_client multiprocess mode), so the
    image must not start several uvicorn workers; scale with replicas instead."""
    from tests.conftest import REPO_ROOT

    cmd = next(
        line
        for line in (REPO_ROOT / "Dockerfile").read_text().splitlines()
        if line.startswith("CMD")
    )
    assert "uvicorn" in cmd and "--workers" not in cmd, cmd
    compose = (REPO_ROOT / "docker-compose.yml").read_text()
    assert "--workers" not in compose and "WEB_CONCURRENCY" not in compose
