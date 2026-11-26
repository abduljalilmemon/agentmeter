"""The LangGraph demo (examples/langgraph_demo): builds, and is stopped with reason=loop.

Skipped when the ``demo`` extra isn't installed, or failed instead when
AGENTMETER_REQUIRE_DEMO=1 (CI). Runs entirely in-process against the
proxy and the mock upstream; never calls a real LLM API.
"""

import importlib.util
import json
import os
import sys
from types import ModuleType

import httpx
import pytest
import redis
from fastapi.testclient import TestClient
from openai import OpenAI

from app.main import create_app
from mock_upstream.main import app as mock_app
from tests.conftest import REPO_ROOT, make_settings, require_demo_extra

DEMO = REPO_ROOT / "examples" / "langgraph_demo" / "agent.py"
TOKEN = "demo-token"


def load_demo() -> ModuleType:
    spec = importlib.util.spec_from_file_location("langgraph_demo_agent", DEMO)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # dataclasses look their module up here
    spec.loader.exec_module(module)
    return module


def test_demo_imports_and_its_graph_builds() -> None:
    require_demo_extra()
    demo = load_demo()
    assert os.environ["LANGSMITH_TRACING"] == "false"  # the demo turns tracing off
    graph = demo.build_graph(OpenAI(base_url="http://unused.invalid/v1", api_key="x"), {})
    assert {"agent", "tools"} <= set(graph.get_graph().nodes)
    assert json.loads(demo.get_weather("Paris"))["error"].startswith("weather service")


def test_demo_is_stopped_with_reason_loop(
    redis_db: redis.Redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    require_demo_extra()
    demo = load_demo()
    monkeypatch.setenv(
        "MOCK_TOOL_CALL", json.dumps({"name": "get_weather", "arguments": {"city": "Paris"}})
    )
    app = create_app(
        make_settings(admin_token=TOKEN), upstream_transport=httpx.ASGITransport(app=mock_app)
    )
    headers = {"X-AM-Run-Id": "lg-demo", "X-AM-Agent": "weather-bot", "X-AM-Team": "demo"}
    with TestClient(app) as c:
        client = OpenAI(base_url="http://testserver/v1", api_key="sk-demo", http_client=c)
        state = demo.run_agent(client, headers, "What's the weather in Paris?")
        report = c.get("/runs/lg-demo", headers={"Authorization": f"Bearer {TOKEN}"}).json()

    stopped = state["stopped"]
    assert stopped is not None
    assert (stopped["status"], stopped["type"], stopped["reason"]) == (
        429,
        "agentmeter_breaker_open",
        "loop",
    )
    assert state["calls"] == 3  # call 4 is refused (threshold 3 / three identical tool calls)
    tool_results = [m for m in state["messages"] if m["role"] == "tool"]
    assert len(tool_results) == 3 and all("unavailable" in m["content"] for m in tool_results)
    assert report["calls"] == 3
    assert (report["agent"], report["team"]) == ("weather-bot", "demo")
    assert report["breaker"]["state"] == "open" and report["breaker"]["reason"] == "loop"


# --- the skip / fail switch (runs whether or not the demo extra is installed) ----------


def _missing(name: str) -> None:
    raise ImportError(f"No module named {name!r}")


def test_missing_demo_extra_skips_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AGENTMETER_REQUIRE_DEMO", raising=False)
    with pytest.raises(pytest.skip.Exception, match="demo extra not installed"):
        require_demo_extra(importer=_missing)


def test_missing_demo_extra_fails_when_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AGENTMETER_REQUIRE_DEMO", "1")
    with pytest.raises(pytest.fail.Exception, match="AGENTMETER_REQUIRE_DEMO=1"):
        require_demo_extra(importer=_missing)


def test_installed_demo_extra_passes_the_check(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AGENTMETER_REQUIRE_DEMO", "1")
    require_demo_extra(importer=lambda name: None)  # neither skips nor fails
