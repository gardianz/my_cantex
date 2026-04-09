from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

CC_SYMBOL = "CC"
TRACKED_SYMBOLS = ("CC", "USDCx", "CBTC")
DEFAULT_MIN_CC_RESERVE = Decimal("5")
MIN_TICKET_SIZE_CC = Decimal("10")
DUST_BY_SYMBOL = {
    "CC": Decimal("0.000001"),
    "USDCx": Decimal("0.000001"),
    "CBTC": Decimal("0.00000001"),
}


@dataclass(frozen=True)
class StrategyDefinition:
    key: str
    label: str
    steps: tuple[tuple[str, str], ...]

    def step_for_round(self, round_index: int) -> tuple[str, str]:
        return self.steps[round_index % len(self.steps)]


STRATEGIES = {
    "strategy_1": StrategyDefinition(
        key="strategy_1",
        label="Strategi 1: CC -> USDCx",
        steps=(("CC", "USDCx"),),
    ),
    "strategy_3": StrategyDefinition(
        key="strategy_3",
        label="Strategi 3: CC -> CBTC",
        steps=(("CC", "CBTC"),),
    ),
    "strategy_7": StrategyDefinition(
        key="strategy_7",
        label="Strategi 7: CC -> USDCx -> CBTC",
        steps=(
            ("CC", "USDCx"),
            ("CC", "CBTC"),
            ("USDCx", "CBTC"),
            ("CBTC", "USDCx"),
            ("CBTC", "CC"),
            ("USDCx", "CC"),
        ),
    ),
}

STRATEGY_ALIASES = {
    "1": "strategy_1",
    "3": "strategy_3",
    "7": "strategy_7",
    "cc_to_usdcx": "strategy_1",
    "cc_to_cbtc": "strategy_3",
    "cc_to_usdcx_to_cbtc": "strategy_7",
}


def get_strategy_definition(raw_name: str) -> StrategyDefinition:
    normalized = STRATEGY_ALIASES.get(raw_name.strip().lower(), raw_name.strip().lower())
    if normalized not in STRATEGIES:
        valid = ", ".join(sorted(STRATEGY_ALIASES))
        raise ValueError(f"Strategi '{raw_name}' tidak dikenal. Gunakan salah satu: {valid}")
    return STRATEGIES[normalized]


def dust_for_symbol(symbol: str) -> Decimal:
    return DUST_BY_SYMBOL.get(symbol, Decimal("0.00000001"))
