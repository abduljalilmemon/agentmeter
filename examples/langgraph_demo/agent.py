"""A LangGraph agent with a broken tool, stopped by AgentMeter's loop breaker.

The agent asks the model for the weather; the model calls ``get_weather``; the tool always
fails; the agent hands the error back and the model calls the tool again, forever. Without
AgentMeter this burns money until a recursion limit or a human stops it. Through
AgentMeter, the proxy sees the same tool call and the same failing result repeat and
answers 429 ``agentmeter_breaker_open`` with ``reason=loop``.

Run it against docker compose, where the "model" is mock_upstream (no provider keys, no
cost). ``MOCK_TOOL_CALL`` makes the mock answer with a tool call:

    MOCK_TOOL_CALL='{"name": "get_weather", "arguments": {"city": "Paris"}}' \\
    AGENTMETER_ADMIN_TOKEN=demo-token docker compose up -d --build
    pip install -c requirements.lock -e ".[demo]"
    AGENTMETER_ADMIN_TOKEN=demo-token python examples/langgraph_demo/agent.py

Model calls go through the ``openai`` SDK (``base_url`` = the proxy, ``X-AM-*`` headers
for run, agent and team). LangGraph orchestrates the agent -> tools -> agent loop.
"""

from __future__ import annotations

import argparse
import json
import operator
import os
import sys
import uuid
from typing import Annotated, Any, TypedDict

# Never send traces anywhere: this demo talks to the local proxy and nothing else.
os.environ["LANGSMITH_TRACING"] = "false"
os.environ["LANGCHAIN_TRACING_V2"] = "false"

import httpx  # noqa: E402
from langgraph.graph import END, START, StateGraph  # noqa: E402
from openai import APIStatusError, OpenAI  # noqa: E402

MODEL = "gpt-4o-mini"
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_weather",
            "description": "Current weather for a city.",
            "parameters": {
                "type": "object",
                "properties": {"city": {"type": "string"}},
                "required": ["city"],
            },
        },
    }
]


class AgentState(TypedDict):
    messages: Annotated[list[dict[str, Any]], operator.add]
    calls: int  # model calls that got an answer
    stopped: dict[str, Any] | None  # AgentMeter's error body, once the proxy refuses


def get_weather(city: str) -> str:
    """The deliberately broken tool: the weather service is always down."""
    return json.dumps({"error": f"weather service unavailable for {city}", "retry": True})


def build_graph(client: OpenAI, headers: dict[str, str]):
    """agent -> (tool calls?) -> tools -> agent ... until the proxy refuses the call."""

    def agent(state: AgentState) -> dict[str, Any]:
        try:
            response = client.chat.completions.create(
                model=MODEL, messages=state["messages"], tools=TOOLS, extra_headers=headers
            )
        except APIStatusError as exc:  # 429 from the breaker (or budget): stop the agent
            body = exc.body if isinstance(exc.body, dict) else {"message": str(exc)}
            return {"stopped": {"status": exc.status_code, **body}}
        message = response.choices[0].message.model_dump(exclude_none=True)
        return {"messages": [message], "calls": state["calls"] + 1}

    def tools(state: AgentState) -> dict[str, Any]:
        results = []
        for call in state["messages"][-1].get("tool_calls", []):
            args = json.loads(call["function"]["arguments"] or "{}")
            output = get_weather(**args) if call["function"]["name"] == "get_weather" else "{}"
            results.append({"role": "tool", "tool_call_id": call["id"], "content": output})
        return {"messages": results}

    def route(state: AgentState) -> str:
        if state["stopped"] is not None:
            return END
        return "tools" if state["messages"][-1].get("tool_calls") else END

    graph = StateGraph(AgentState)
    graph.add_node("agent", agent)
    graph.add_node("tools", tools)
    graph.add_edge(START, "agent")
    graph.add_conditional_edges("agent", route, {"tools": "tools", END: END})
    graph.add_edge("tools", "agent")
    return graph.compile()


def run_agent(client: OpenAI, headers: dict[str, str], question: str) -> AgentState:
    graph = build_graph(client, headers)
    initial: AgentState = {
        "messages": [{"role": "user", "content": question}],
        "calls": 0,
        "stopped": None,
    }
    # The recursion limit is LangGraph's own last resort; AgentMeter should stop it first.
    return graph.invoke(initial, {"recursion_limit": 200})


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--proxy", default="http://localhost:8080", help="AgentMeter base URL")
    parser.add_argument("--run-id", default=f"demo-{uuid.uuid4().hex[:8]}")
    parser.add_argument("--agent", default="weather-bot")
    parser.add_argument("--team", default="demo")
    args = parser.parse_args(argv)

    headers = {"X-AM-Run-Id": args.run_id, "X-AM-Agent": args.agent, "X-AM-Team": args.team}
    # The mock ignores the key; with a real upstream the proxy passes it through unchanged.
    client = OpenAI(
        base_url=f"{args.proxy}/v1", api_key=os.environ.get("OPENAI_API_KEY", "sk-demo")
    )
    state = run_agent(client, headers, "What's the weather in Paris right now?")

    stopped = state["stopped"]
    print(f"run {args.run_id}: {state['calls']} model calls answered")
    if stopped is None:
        print("the agent finished on its own (no loop detected)")
    else:
        print(
            f"stopped by AgentMeter: HTTP {stopped.get('status')} type={stopped.get('type')} "
            f"reason={stopped.get('reason')} spent_usd={stopped.get('spent_usd')}"
        )

    token = os.environ.get("AGENTMETER_ADMIN_TOKEN")
    if token:
        report = httpx.get(
            f"{args.proxy}/runs/{args.run_id}", headers={"Authorization": f"Bearer {token}"}
        )
        print(f"GET /runs/{args.run_id} -> {report.status_code}")
        print(json.dumps(report.json(), indent=2))
    else:
        print("set AGENTMETER_ADMIN_TOKEN (same as the proxy's) to print the run report")
    return 0 if stopped is not None and stopped.get("reason") == "loop" else 1


if __name__ == "__main__":
    sys.exit(main())
