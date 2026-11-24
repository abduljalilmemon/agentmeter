# AgentMeter

Budget circuit breaker proxy for AI agents. Read PLAN.md for the architecture and milestones.

## Stack
Python 3.12, FastAPI (async), httpx, redis-py asyncio, Pydantic v2, pytest + pytest-asyncio, ruff, Docker Compose.

## Rules
- Money is integer micro-USD. Never use floats for money.
- Budget reserve and settle must be atomic, using Lua scripts in app/lua/.
- Tests run against mock_upstream/ by default. Never call a real LLM API in tests.
- Every feature ships with tests. Run `ruff check . && pytest` before committing.
- Don't add dependencies without saying so in your summary.
- Stay inside the current milestone's scope.
- No brackets in file names.

## Commands
- `docker compose up -d redis mock_upstream`
- `pytest -q`
- `ruff check . --fix`

## Status
See PROGRESS.md for the current milestone.