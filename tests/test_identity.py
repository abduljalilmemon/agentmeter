import pytest
from starlette.datastructures import Headers

from app.identity import IdentityError, parse_identity


def test_all_headers() -> None:
    ident = parse_identity(
        Headers(
            {
                "x-am-run-id": "run-1",
                "X-AM-Agent": "support-bot",
                "X-AM-Team": "platform",
                "X-AM-Run-Budget": "0.25",
            }
        )
    )
    assert ident.run_id == "run-1"
    assert not ident.run_id_generated
    assert ident.agent == "support-bot"
    assert ident.team == "platform"
    assert ident.run_budget == 250_000


def test_defaults() -> None:
    ident = parse_identity(Headers({}))
    assert ident.run_id_generated
    assert len(ident.run_id) == 32 and int(ident.run_id, 16) >= 0
    assert ident.agent is ident.team is ident.run_budget is None


@pytest.mark.parametrize("budget", ["abc", "-1", "0.0000001", "nan", ""])
def test_bad_budget(budget: str) -> None:
    with pytest.raises(IdentityError):
        parse_identity(Headers({"X-AM-Run-Budget": budget}))


@pytest.mark.parametrize("run_id", ["", "has space", "x" * 129, "a/b"])
def test_bad_run_id(run_id: str) -> None:
    with pytest.raises(IdentityError):
        parse_identity(Headers({"X-AM-Run-Id": run_id}))
