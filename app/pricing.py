"""Model pricing: integer micro-USD, never under-counted."""

from __future__ import annotations

from collections.abc import Mapping

from app.config import ModelPrice

TOKENS_PER_PRICE_UNIT = 1_000_000  # prices are micro-USD per 1M tokens

# A prefix only matches at a version boundary, so "gpt-4" does not price "gpt-4o"
# but "gpt-4o-mini" does price "gpt-4o-mini-2024-07-18".
_PREFIX_BOUNDARY = "-"


class UnknownModel(LookupError):
    def __init__(self, model: str) -> None:
        super().__init__(f"No price configured for model {model!r}")
        self.model = model


def _ceil_div(n: int, d: int) -> int:
    return -(-n // d)


class PriceTable:
    def __init__(self, prices: Mapping[str, ModelPrice]) -> None:
        self._prices = dict(prices)
        # Longest first, so the first boundary match is the longest prefix.
        self._prefixes = sorted(self._prices, key=len, reverse=True)

    def lookup(self, model: str) -> ModelPrice:
        """Exact match, else the longest configured prefix ending at a '-' boundary."""
        if model in self._prices:
            return self._prices[model]
        for prefix in self._prefixes:
            if model.startswith(prefix + _PREFIX_BOUNDARY):
                return self._prices[prefix]
        raise UnknownModel(model)

    def cost_micro(self, model: str, input_tokens: int, output_tokens: int) -> int:
        """Cost in micro-USD, rounded up (one ceiling over the exact total)."""
        if input_tokens < 0 or output_tokens < 0:
            raise ValueError("token counts must be >= 0")
        price = self.lookup(model)
        exact_numerator = (
            input_tokens * price.input_per_mtok + output_tokens * price.output_per_mtok
        )
        return _ceil_div(exact_numerator, TOKENS_PER_PRICE_UNIT)
