import pytest

from app.config import ModelPrice
from app.pricing import PriceTable, UnknownModel


def _table(**prices: tuple[str, str]) -> PriceTable:
    return PriceTable({m: ModelPrice(input=i, output=o) for m, (i, o) in prices.items()})


TABLE = _table(
    **{
        "gpt-4o-mini": ("0.15", "0.60"),
        "gpt-4o": ("2.50", "10.00"),
        "gemini-2.5-flash": ("0.30", "2.50"),
    }
)


def test_exact_integer_cost() -> None:
    # 1M input at $0.15 + 1M output at $0.60 = $0.75
    assert TABLE.cost_micro("gpt-4o-mini", 1_000_000, 1_000_000) == 750_000
    # 10 in, 20 out: (10*150_000 + 20*600_000) / 1e6 = 13.5 -> 14
    assert TABLE.cost_micro("gpt-4o-mini", 10, 20) == 14
    assert TABLE.cost_micro("gemini-2.5-flash", 2_000, 400) == 1_600
    assert type(TABLE.cost_micro("gpt-4o", 1, 1)) is int


def test_ceiling_never_undercounts() -> None:
    assert TABLE.cost_micro("gpt-4o-mini", 0, 0) == 0
    assert TABLE.cost_micro("gpt-4o-mini", 1, 0) == 1  # 0.15 micro -> 1
    assert TABLE.cost_micro("gpt-4o-mini", 6, 0) == 1  # 0.9 -> 1
    assert TABLE.cost_micro("gpt-4o-mini", 7, 0) == 2  # 1.05 -> 2
    assert TABLE.cost_micro("gpt-4o-mini", 20, 0) == 3  # exactly 3.0 stays 3


@pytest.mark.parametrize("tokens", [(1, 1), (3, 7), (123_457, 98_765), (999_999, 1)])
def test_ceiling_matches_exact_math(tokens: tuple[int, int]) -> None:
    from fractions import Fraction

    i, o = tokens
    exact = Fraction(i * 150_000 + o * 600_000, 1_000_000)
    cost = TABLE.cost_micro("gpt-4o-mini", i, o)
    assert cost >= exact > cost - 1


def test_prefix_match_longest_wins() -> None:
    assert TABLE.lookup("gpt-4o-mini-2024-07-18") is TABLE.lookup("gpt-4o-mini")
    assert TABLE.lookup("gpt-4o-2024-08-06") is TABLE.lookup("gpt-4o")
    assert TABLE.lookup("gemini-2.5-flash-preview-05-20") is TABLE.lookup("gemini-2.5-flash")


def test_prefix_needs_boundary() -> None:
    table = _table(**{"gpt-4": ("30", "60")})
    with pytest.raises(UnknownModel):
        table.lookup("gpt-4o")  # not a gpt-4 snapshot
    assert table.lookup("gpt-4-0613").input_per_mtok == 30_000_000


@pytest.mark.parametrize("model", ["claude-x", "gpt-4", "", "gpt-4omini", "gpt-4o_mini"])
def test_unknown_model(model: str) -> None:
    with pytest.raises(UnknownModel):
        TABLE.cost_micro(model, 1, 1)


def test_negative_tokens_rejected() -> None:
    with pytest.raises(ValueError):
        TABLE.cost_micro("gpt-4o", -1, 0)


def test_prefix_rule_limit() -> None:
    # Documented limit: any "<configured>-..." name is treated as a variant of it.
    assert TABLE.lookup("gpt-4o-min") is TABLE.lookup("gpt-4o")
