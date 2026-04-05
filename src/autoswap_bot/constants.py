from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

CC_SYMBOL = "CC"
TRACKED_SYMBOLS = ("CC", "USDCx", "CBTC")
DEFAULT_MIN_CC_RESERVE = Decimal("5")
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
        label="Strategi 2: USDCx -> CC",
        steps=(("USDCx", "CC"),),
    ),
    "strategy_3": StrategyDefinition(
        key="strategy_3",
        label="Strategi 3: CC -> CBTC",
        steps=(("CC", "CBTC"),),
    ),
    "strategy_4": StrategyDefinition(
        key="strategy_4",
        label="Strategi 4: CBTC -> CC",
        steps=(("CBTC", "CC"),),
    ),
    "strategy_5": StrategyDefinition(
        key="strategy_5",
        label="Strategi 5: USDCx -> CBTC",
        steps=(("USDCx", "CBTC"),),
    ),
    "strategy_6": StrategyDefinition(
        key="strategy_6",
        label="Strategi 6: CBTC -> USDCx",
        steps=(("CBTC", "USDCx"),),
    ),
    "strategy_7": StrategyDefinition(
        key="strategy_7",
        label="Strategi 7: Siklik CC -> USDCx -> CBTC",
        steps=(("CC", "USDCx"), ("USDCx", "CBTC"), ("CBTC", "CC")),
    ),
    "strategy_8": StrategyDefinition(
        key="strategy_8",
        label="Strategi 8: Siklik USDCx -> CBTC -> CC",
        steps=(("USDCx", "CBTC"), ("CBTC", "CC"), ("CC", "USDCx")),
    ),
    "strategy_9": StrategyDefinition(
        key="strategy_9",
        label="Strategi 9: Siklik CBTC -> CC -> USDCx",
        steps=(("CBTC", "CC"), ("CC", "USDCx"), ("USDCx", "CBTC")),
    ),
}

STRATEGY_ALIASES = {
    "1": "strategy_1",
    "2": "strategy_2",
    "3": "strategy_3",
    "4": "strategy_4",
    "5": "strategy_5",
    "6": "strategy_6",
    "7": "strategy_7",
    "8": "strategy_8",
    "9": "strategy_9",
    "cc_to_usdcx": "strategy_1",
    "usdcx_to_cc": "strategy_2",
    "cc_to_cbtc": "strategy_3",
    "cbtc_to_cc": "strategy_4",
    "usdcx_to_cbtc": "strategy_5",
    "cbtc_to_usdcx": "strategy_6",
}


def get_strategy_definition(raw_name: str) -> StrategyDefinition:
    normalized = STRATEGY_ALIASES.get(raw_name.strip().lower(), raw_name.strip().lower())
    if normalized not in STRATEGIES:
        valid = ", ".join(sorted(STRATEGY_ALIASES))
        raise ValueError(f"Strategi '{raw_name}' tidak dikenal. Gunakan salah satu: {valid}")
    return STRATEGIES[normalized]


def dust_for_symbol(symbol: str) -> Decimal:
    return DUST_BY_SYMBOL.get(symbol, Decimal("0.00000001"))
