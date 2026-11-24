from collections.abc import Iterator

import httpx
import pytest
from fastapi.testclient import TestClient
from openai import OpenAI

from app.main import create_app
from app.proxy import upstream_request_headers
from mock_upstream.main import app as mock_app
from tests.conftest import make_settings

pytestmark = pytest.mark.usefixtures("redis_db")

UPSTREAM = "http://mock_upstream/v1"
REQUEST = {"model": "gpt-4o-mini", "messages": [{"role": "user", "content": "hi"}]}


def _settings(**kw: object):
    return make_settings(UPSTREAM, **kw)


@pytest.fixture
def client() -> Iterator[TestClient]:
    """Proxy wired to the in-process mock upstream."""
    app = create_app(_settings(), upstream_transport=httpx.ASGITransport(app=mock_app))
    with TestClient(app) as c:
        yield c


def _proxy_with(handler) -> TestClient:
    return TestClient(create_app(_settings(), upstream_transport=httpx.MockTransport(handler)))


def _last_upstream_request() -> dict:
    return TestClient(mock_app).get("/_mock/last_request").json()


def test_openai_sdk_through_proxy(client: TestClient) -> None:
    sdk = OpenAI(base_url="http://testserver/v1", api_key="sk-test", http_client=client)
    completion = sdk.chat.completions.create(**REQUEST)
    assert completion.choices[0].message.content == "Hello from mock_upstream."
    assert completion.usage is not None
    assert (
        completion.usage.prompt_tokens,
        completion.usage.completion_tokens,
        completion.usage.total_tokens,
    ) == (10, 20, 30)
    assert _last_upstream_request()["headers"]["authorization"] == "Bearer sk-test"


def test_run_id_generated(client: TestClient) -> None:
    resp = client.post("/v1/chat/completions", json=REQUEST)
    assert resp.status_code == 200
    run_id = resp.headers["x-am-run-id"]
    assert len(run_id) == 32 and int(run_id, 16) >= 0


def test_run_id_echoed(client: TestClient) -> None:
    resp = client.post("/v1/chat/completions", json=REQUEST, headers={"X-AM-Run-Id": "run-42"})
    assert resp.headers["x-am-run-id"] == "run-42"


def test_am_headers_not_forwarded(client: TestClient) -> None:
    headers = {
        "Authorization": "Bearer sk-abc",
        "X-AM-Run-Id": "run-1",
        "X-AM-Agent": "support-bot",
        "X-AM-Team": "platform",
        "X-AM-Run-Budget": "0.5",
        "X-Custom": "kept",
    }
    assert client.post("/v1/chat/completions", json=REQUEST, headers=headers).status_code == 200
    seen = _last_upstream_request()
    assert not [h for h in seen["headers"] if h.startswith("x-am-")]
    assert seen["headers"]["authorization"] == "Bearer sk-abc"
    assert seen["headers"]["x-custom"] == "kept"
    assert seen["body"] == {**REQUEST, "max_tokens": 1024}  # default injected


def test_hop_by_hop_headers_dropped() -> None:
    out = dict(
        upstream_request_headers(
            [
                ("Connection", "keep-alive, X-Secret"),
                ("X-Secret", "1"),
                ("Keep-Alive", "timeout=5"),
                ("Transfer-Encoding", "chunked"),
                ("Host", "proxy"),
                ("Authorization", "Bearer x"),
            ]
        )
    )
    assert out == {"authorization": "Bearer x"}


def test_invalid_budget_returns_400(client: TestClient) -> None:
    resp = client.post("/v1/chat/completions", json=REQUEST, headers={"X-AM-Run-Budget": "lots"})
    assert resp.status_code == 400
    assert resp.json()["error"]["type"] == "invalid_request_error"


def test_upstream_400_passes_through(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOCK_FORCE_STATUS", "400")
    resp = client.post("/v1/chat/completions", json=REQUEST, headers={"X-AM-Run-Id": "r"})
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "mock_forced"
    assert resp.headers["x-am-run-id"] == "r"


def test_upstream_500_passes_through() -> None:
    with _proxy_with(lambda req: httpx.Response(503, text="overloaded")) as c:
        resp = c.post("/v1/chat/completions", json=REQUEST)
    assert (resp.status_code, resp.text) == (503, "overloaded")


def test_upstream_timeout_returns_504() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow", request=request)

    with _proxy_with(handler) as c:
        resp = c.post("/v1/chat/completions", json=REQUEST)
    assert resp.status_code == 504
    assert resp.json()["error"]["code"] == "upstream_timeout"
    assert "x-am-run-id" in resp.headers


def test_connection_refused_returns_502() -> None:
    # Real socket: nothing listens on port 1.
    app = create_app(make_settings("http://127.0.0.1:1/v1"))
    with TestClient(app) as c:
        resp = c.post("/v1/chat/completions", json=REQUEST)
    assert resp.status_code == 502
    assert resp.json()["error"]["type"] == "upstream_error"


def test_connect_timeout_returns_502() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("no route", request=request)

    with _proxy_with(handler) as c:
        resp = c.post("/v1/chat/completions", json=REQUEST)
    assert resp.status_code == 502
    assert resp.json()["error"]["code"] == "upstream_connect_timeout"


def test_invalid_json_returns_400(client: TestClient) -> None:
    resp = client.post("/v1/chat/completions", content=b"{nope", headers={"content-type": "x"})
    assert resp.status_code == 400


def test_max_tokens_injected_when_missing(client: TestClient) -> None:
    client.post("/v1/chat/completions", json=REQUEST)
    assert _last_upstream_request()["body"]["max_tokens"] == 1024


@pytest.mark.parametrize(
    "limits",
    [
        {"max_tokens": 77},
        {"max_completion_tokens": 88},
        {"max_tokens": 5, "max_completion_tokens": 9},
    ],
)
def test_client_limits_kept(client: TestClient, limits: dict) -> None:
    client.post("/v1/chat/completions", json={**REQUEST, **limits})
    body = _last_upstream_request()["body"]
    assert {k: body.get(k) for k in limits} == limits
    if "max_tokens" not in limits:
        assert "max_tokens" not in body  # no injection next to max_completion_tokens


def test_client_body_forwarded_byte_for_byte_when_unchanged(client: TestClient) -> None:
    raw = (
        b'{"messages": [{"role": "user", "content": "hi"}],'
        b'   "model": "gpt-4o-mini", "max_tokens": 9}'
    )
    client.post("/v1/chat/completions", content=raw, headers={"content-type": "application/json"})
    assert _last_upstream_request()["raw"] == raw.decode()


def test_unknown_model_fails_closed(client: TestClient) -> None:
    resp = client.post("/v1/chat/completions", json={**REQUEST, "model": "mystery-1"})
    assert resp.status_code == 400
    assert resp.json()["error"]["type"] == "agentmeter_unknown_model"
    assert "x-am-run-id" in resp.headers


def test_dated_model_priced_by_prefix(client: TestClient) -> None:
    resp = client.post("/v1/chat/completions", json={**REQUEST, "model": "gpt-4o-mini-2024-07-18"})
    assert resp.status_code == 200


@pytest.mark.parametrize("body", [{"messages": []}, {**REQUEST, "max_tokens": -5}])
def test_bad_body_400(client: TestClient, body: dict) -> None:
    assert client.post("/v1/chat/completions", json=body).status_code == 400
