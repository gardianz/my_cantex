from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

CC_SYMBOL = "CC"
TRACKED_SYMBOLS = ("CC", "USDCx", "CBTC")
DEFAULT_RESERVE_FEE = Decimal("5")
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
    "strategy_2": StrategyDefinition(
        key="strategy_2",
        label="Strategi 2: CC -> CBTC",
        steps=(("CC", "CBTC"),),
    ),
    "strategy_3_cycle": StrategyDefinition(
        key="strategy_3_cycle",
        label="Strategi 3: CC -> USDCx -> CBTC",
        steps=(
            ("CC", "USDCx"),
            ("CC", "CBTC"),
            ("USDCx", "CBTC"),
            ("CBTC", "USDCx"),
            ("CBTC", "CC"),
            ("USDCx", "CC"),
        ),
    ),
    "strategy_4_reserve": StrategyDefinition(
        key="strategy_4_reserve",
        label="Strategi 4: CC -> USDCx -> CBTC",
        steps=(
            ("CC", "USDCx"),
            ("USDCx", "CBTC"),
            ("CBTC", "USDCx"),
            ("USDCx", "CC"),
            ("CBTC", "CC"),
        ),
    ),
}

STRATEGY_ALIASES = {
    "1": "strategy_1",
    "2": "strategy_2",
    "3": "strategy_3_cycle",
    "4": "strategy_4_reserve",
    "7": "strategy_3_cycle",
    "strategy_1": "strategy_1",
    "strategy_2": "strategy_2",
    "strategy_3": "strategy_2",
    "strategy_4": "strategy_4_reserve",
    "strategy_7": "strategy_3_cycle",
    "cc_to_usdcx": "strategy_1",
    "cc_to_cbtc": "strategy_2",
    "cc_to_usdcx_to_cbtc": "strategy_3_cycle",
    "cc_to_usdcx_to_cbtc_reserve": "strategy_4_reserve",
}


def get_strategy_definition(raw_name: str) -> StrategyDefinition:
    normalized = STRATEGY_ALIASES.get(raw_name.strip().lower(), raw_name.strip().lower())
    if normalized not in STRATEGIES:
        valid = ", ".join(sorted(STRATEGY_ALIASES))
        raise ValueError(f"Strategi '{raw_name}' tidak dikenal. Gunakan salah satu: {valid}")
    return STRATEGIES[normalized]


def dust_for_symbol(symbol: str) -> Decimal:
    return DUST_BY_SYMBOL.get(symbol, Decimal("0.00000001"))
