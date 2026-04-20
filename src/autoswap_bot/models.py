from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class RouteHop:
    sell_symbol: str
    buy_symbol: str
    sell_amount: Decimal
    returned_amount: Decimal
    fee_percentage: Decimal
    admin_fee_amount: Decimal
    liquidity_fee_amount: Decimal
    fee_symbol: str
    network_fee_amount: Decimal
    network_fee_symbol: str
    estimated_time_seconds: Decimal
    slippage: Decimal
    raw_quote: Any


@dataclass(frozen=True)
class RoutePlan:
    path_symbols: tuple[str, ...]
    hops: tuple[RouteHop, ...]
    final_amount: Decimal
    total_network_fee_cc: Decimal
    total_network_fee_by_symbol: dict[str, Decimal]
    total_admin_and_liquidity_by_symbol: dict[str, Decimal]

    @property
    def label(self) -> str:
        return " -> ".join(self.path_symbols)

    @property
    def tx_count(self) -> int:
        return len(self.hops)


@dataclass(frozen=True)
class PlanStep:
    round_number: int
    sell_symbol: str
    buy_symbol: str
    requested_amount: Decimal
    route: RoutePlan


@dataclass(frozen=True)
class PlanIssue:
    round_number: int
    sell_symbol: str
    requested_amount: Decimal
    available_amount: Decimal
    reason: str


@dataclass(frozen=True)
class AccountPlan:
    steps: tuple[PlanStep, ...]
    issues: tuple[PlanIssue, ...]
    estimated_network_fee_by_symbol: dict[str, Decimal]
    estimated_admin_and_liquidity_by_symbol: dict[str, Decimal]
    estimated_swap_count: int

    @property
    def can_fully_execute(self) -> bool:
        return not self.issues


@dataclass(frozen=True)
class ActivitySummary:
    source_path: str | None = None
    history_source_path: str | None = None
    swaps_24h: str | None = None
    volume_24h: str | None = None
    volume_24h_usd: str | None = None
    swaps_7d: str | None = None
    volume_7d: str | None = None
    swaps_30d: str | None = None
    volume_30d: str | None = None
    total_swaps: str | None = None
    total_volume: str | None = None
    reward_total: str | None = None
    tx_count: str | None = None
    rank: str | None = None
    volume_usd: str | None = None
    rebates: dict[str, str] = field(default_factory=dict)
    recent_items: tuple[str, ...] = ()
    raw_preview: str | None = None


@dataclass
class AccountResult:
    account_name: str
    strategy_label: str
    requested_rounds: int
    completed_rounds: int = 0
    skipped_rounds: int = 0
    swap_transactions: int = 0
    aborted: bool = False
    error: str | None = None
    stop_reason: str | None = None
    estimated_network_fee_by_symbol: dict[str, Decimal] = field(default_factory=dict)
    used_network_fee_by_symbol: dict[str, Decimal] = field(default_factory=dict)
    used_swap_fee_by_symbol: dict[str, Decimal] = field(default_factory=dict)
    final_balances: dict[str, Decimal] = field(default_factory=dict)
    activity_summary: ActivitySummary | None = None
    retry_after_seconds: float | None = None

    @property
    def ok(self) -> bool:
        return self.error is None and not self.aborted and self.stop_reason is None
