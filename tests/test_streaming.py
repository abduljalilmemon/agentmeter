import json
import socket
import threading
import time
from collections.abc import AsyncIterator, Iterator

import anyio
import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient
from openai import OpenAI
from starlette.requests import ClientDisconnect

from app import proxy
from app.config import Settings
from app.identity import Identity
from app.main import create_app
from mock_upstream.main import app as mock_app

REQUEST = {
    "model": "gpt-4o-mini",
    "messages": [{"role": "user", "content": "hi"}],
    "stream": True,
}
NO_REDIS = "redis://127.0.0.1:1/0"
USAGE = {"prompt_tokens": 10, "completion_tokens": 20, "total_tokens": 30}


class Recorder:
    """Stream hook that keeps every StreamResult."""

    def __init__(self) -> None:
        self.results: list[proxy.StreamResult] = []

    async def __call__(self, identity: Identity, result: proxy.StreamResult) -> None:
        self.results.append(result)

    @property
    def last(self) -> proxy.StreamResult:
        assert len(self.results) == 1, self.results
        return self.results[0]


@pytest.fixture
def recorder() -> Recorder:
    return Recorder()


@pytest.fixture
def client(recorder: Recorder) -> Iterator[TestClient]:
    """Proxy wired to the in-process mock upstream."""
    app = create_app(
        Settings(upstream="http://mock_upstream/v1", redis_url=NO_REDIS),
        upstream_transport=httpx.ASGITransport(app=mock_app),
        on_stream_finish=recorder,
    )
    with TestClient(app) as c:
        yield c


@pytest.fixture(scope="module")
def live_mock() -> Iterator[str]:
    """mock_upstream on a real socket, so a cut stream is a real dropped connection."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    server = uvicorn.Server(
        uvicorn.Config(mock_app, host="127.0.0.1", port=port, log_level="critical")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        assert time.monotonic() < deadline, "mock_upstream did not start"
        time.sleep(0.02)
    yield f"http://127.0.0.1:{port}/v1"
    server.should_exit = True
    thread.join(timeout=5)


def _last_upstream_body() -> dict:
    return TestClient(mock_app).get("/_mock/last_request").json()["body"]


def _sse_events(text: str) -> list[str]:
    return [line[6:] for line in text.splitlines() if line.startswith("data: ")]


def test_openai_sdk_streaming(client: TestClient, recorder: Recorder) -> None:
    sdk = OpenAI(base_url="http://testserver/v1", api_key="sk-test", http_client=client)
    chunks = list(
        sdk.chat.completions.create(
            model="gpt-4o-mini", messages=[{"role": "user", "content": "hi"}], stream=True
        )
    )
    content = "".join(c.choices[0].delta.content or "" for c in chunks if c.choices)
    assert content == "Hello from mock_upstream."
    last = chunks[-1]
    assert last.choices == []
    assert last.usage is not None
    assert last.usage.model_dump(include=set(USAGE)) == USAGE
    assert recorder.last.usage == USAGE
    assert recorder.last.done and recorder.last.error is None
    assert not recorder.last.client_disconnected


def test_stream_relays_sse_bytes(client: TestClient) -> None:
    resp = client.post("/v1/chat/completions", json=REQUEST, headers={"X-AM-Run-Id": "s1"})
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/event-stream")
    assert resp.headers["x-am-run-id"] == "s1"
    events = _sse_events(resp.text)
    assert events[-1] == "[DONE]"
    assert json.loads(events[-2])["usage"] == USAGE


def test_include_usage_injected(client: TestClient) -> None:
    client.post("/v1/chat/completions", json=REQUEST)
    assert _last_upstream_body()["stream_options"] == {"include_usage": True}


def test_existing_stream_options_kept(client: TestClient) -> None:
    body = {**REQUEST, "stream_options": {"include_usage": False, "x_extra": 7}}
    client.post("/v1/chat/completions", json=body)
    assert _last_upstream_body()["stream_options"] == {"include_usage": True, "x_extra": 7}


def test_bad_stream_options_400(client: TestClient) -> None:
    resp = client.post("/v1/chat/completions", json={**REQUEST, "stream_options": "yes"})
    assert resp.status_code == 400


def test_stream_upstream_4xx_returns_json(
    client: TestClient, recorder: Recorder, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MOCK_FORCE_STATUS", "429")
    resp = client.post("/v1/chat/completions", json=REQUEST)
    assert resp.status_code == 429
    assert resp.headers["content-type"].startswith("application/json")
    assert resp.json()["error"]["code"] == "mock_forced"
    assert recorder.results == []  # nothing was streamed


def test_stream_cut_real_socket(live_mock: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOCK_STREAM_CUT", "1")
    recorder = Recorder()
    app = create_app(Settings(upstream=live_mock, redis_url=NO_REDIS), on_stream_finish=recorder)
    with TestClient(app) as c:
        resp = c.post("/v1/chat/completions", json=REQUEST)
        assert resp.status_code == 200
        events = _sse_events(resp.text)
        assert events and "[DONE]" not in events
        assert all(json.loads(e).get("usage") is None for e in events)
        assert recorder.last.usage is None
        assert not recorder.last.done
        assert recorder.last.error == "RemoteProtocolError"
        # The proxy is still fine afterwards.
        monkeypatch.delenv("MOCK_STREAM_CUT")
        assert _sse_events(c.post("/v1/chat/completions", json=REQUEST).text)[-1] == "[DONE]"


def test_stream_cut_in_process(monkeypatch: pytest.MonkeyPatch) -> None:
    # ASGITransport can't drop a socket; the stream simply ends early.
    monkeypatch.setenv("MOCK_STREAM_CUT", "1")
    app = create_app(
        Settings(upstream="http://mock_upstream/v1", redis_url=NO_REDIS),
        upstream_transport=httpx.ASGITransport(app=mock_app, raise_app_exceptions=False),
        on_stream_finish=(rec := Recorder()),
    )
    with TestClient(app) as c:
        resp = c.post("/v1/chat/completions", json=REQUEST)
    assert resp.status_code == 200
    assert "[DONE]" not in resp.text
    assert rec.last.usage is None and not rec.last.done


class ScriptedStream(httpx.AsyncByteStream):
    """Upstream body: yields ``chunks``, then raises ``fail`` or idles until closed."""

    def __init__(self, chunks: list[bytes], fail: Exception | None = None, endless=False):
        self.chunks, self.fail, self.endless, self.closed = chunks, fail, endless, False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            yield chunk
        if self.fail is not None:
            raise self.fail
        while self.endless:
            await anyio.sleep(0.01)
            yield b": keep-alive\n\n"

    async def aclose(self) -> None:
        self.closed = True


def _sse_upstream(stream: ScriptedStream) -> httpx.MockTransport:
    return httpx.MockTransport(
        lambda req: httpx.Response(
            200, headers={"content-type": "text/event-stream"}, stream=stream
        )
    )


def test_mid_stream_read_timeout_ends_stream() -> None:
    first = b'data: {"choices":[{"delta":{"content":"hi"}}]}\n\n'
    stream = ScriptedStream([first], fail=httpx.ReadTimeout("slow"))
    rec = Recorder()
    app = create_app(
        Settings(upstream="http://up/v1", redis_url=NO_REDIS),
        upstream_transport=_sse_upstream(stream),
        on_stream_finish=rec,
    )
    with TestClient(app) as c:
        resp = c.post("/v1/chat/completions", json=REQUEST)
    assert resp.status_code == 200
    assert resp.content == first
    assert rec.last.error == "ReadTimeout"
    assert stream.closed


def test_stream_connect_timeout_502() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("no route", request=request)

    app = create_app(
        Settings(upstream="http://up/v1", redis_url=NO_REDIS),
        upstream_transport=httpx.MockTransport(handler),
    )
    with TestClient(app) as c:
        resp = c.post("/v1/chat/completions", json=REQUEST)
    assert resp.status_code == 502
    assert resp.json()["error"]["code"] == "upstream_connect_timeout"


@pytest.mark.parametrize("spec_version", ["2.4", "2.0"])
async def test_client_disconnect_closes_upstream(spec_version: str) -> None:
    stream = ScriptedStream([b"data: {}\n\n"], endless=True)
    rec: list[proxy.StreamResult] = []

    async def on_finish(result: proxy.StreamResult) -> None:
        rec.append(result)

    async with httpx.AsyncClient(transport=_sse_upstream(stream)) as http:
        response = await proxy.forward_stream(http, "http://up/v1", REQUEST, [], None, on_finish)
    assert isinstance(response, proxy.UpstreamStreamingResponse)

    bodies = 0

    async def send(message: dict) -> None:
        nonlocal bodies
        if message["type"] == "http.response.body":
            bodies += 1
            if spec_version == "2.4" and bodies >= 3:
                raise OSError("client went away")

    async def receive() -> dict:
        if spec_version == "2.4":
            await anyio.sleep_forever()
        await anyio.sleep(0.05)
        return {"type": "http.disconnect"}

    scope = {"type": "http", "asgi": {"spec_version": spec_version}}
    with anyio.fail_after(5):
        if spec_version == "2.4":
            with pytest.raises(ClientDisconnect):
                await response(scope, receive, send)
        else:
            await response(scope, receive, send)

    assert stream.closed
    assert len(rec) == 1 and rec[0].client_disconnected and rec[0].usage is None


def test_sse_parser_handles_split_lines() -> None:
    result = proxy.StreamResult()
    parser = proxy.SSEUsageParser(result)
    data = b'data: {"choices":[],"usage":{"total_tokens":3}}\r\n\r\ndata: [DONE]\n\n'
    for i in range(len(data)):  # one byte at a time
        parser.feed(data[i : i + 1])
    assert result.usage == {"total_tokens": 3}
    assert result.done
