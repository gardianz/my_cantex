from __future__ import annotations

import asyncio
import html
import json
import logging
import re
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

import aiohttp

from .config import AccountConfig, PreparedAccountRun, RuntimeConfig
from .models import ActivitySummary, RoutePlan


SYMBOL_SHORT = {
    "CC": "CC",
    "USDCx": "U",
    "CBTC": "B",
}


@dataclass
class TelegramCardState:
    account_name: str
    display_index: int
    proxy_label: str
    strategy_label: str
    session_started_utc: datetime
    total_rounds: int
    pair_targets: dict[str, int]
    current_utc_date: str = ""
    current_utc_week: str = ""
    day_index: int = 1
    daily_swap_base: int = 0
    daily_ok_tx_base: int = 0
    daily_fail_tx_base: int = 0
    daily_network_fee_base: dict[str, Decimal] = field(default_factory=dict)
    daily_swap_fee_base: dict[str, Decimal] = field(default_factory=dict)
    weekly_network_fee_base: dict[str, Decimal] = field(default_factory=dict)
    weekly_swap_fee_base: dict[str, Decimal] = field(default_factory=dict)
    day_session_swap_offset: int = 0
    day_session_ok_tx_offset: int = 0
    day_session_fail_tx_offset: int = 0
    day_session_network_fee_offset: dict[str, Decimal] = field(default_factory=dict)
    day_session_free_network_fee_offset: dict[str, Decimal] = field(default_factory=dict)
    day_session_swap_fee_offset: dict[str, Decimal] = field(default_factory=dict)
    week_session_network_fee_offset: dict[str, Decimal] = field(default_factory=dict)
    week_session_free_network_fee_offset: dict[str, Decimal] = field(default_factory=dict)
    week_session_swap_fee_offset: dict[str, Decimal] = field(default_factory=dict)
    lifetime_swap_base: int = 0
    lifetime_ok_tx_base: int = 0
    lifetime_fail_tx_base: int = 0
    lifetime_network_fee_base: dict[str, Decimal] = field(default_factory=dict)
    lifetime_swap_fee_base: dict[str, Decimal] = field(default_factory=dict)
    current_pair_key: str | None = None
    progress_completed_base: int = 0
    current_round_number: int = 0
    phase: str = "STARTING"
    balances: dict[str, Decimal] = field(default_factory=dict)
    total_network_fee: dict[str, Decimal] = field(default_factory=dict)
    total_swap_fee: dict[str, Decimal] = field(default_factory=dict)
    free_network_fee_credit: dict[str, Decimal] = field(default_factory=dict)
    current_route_label: str | None = None
    current_route_network_fee: dict[str, Decimal] = field(default_factory=dict)
    current_route_swap_fee: dict[str, Decimal] = field(default_factory=dict)
    daily_free_fee_used: int = 0
    daily_free_fee_limit: int = 3
    swap_transactions: int = 0
    tx_ok_count: int = 0
    tx_fail_count: int = 0
    bot_start_volume_cc: Decimal = field(default_factory=lambda: Decimal("0"))
    last_observed_total_volume_cc: Decimal | None = None
    pair_completed: dict[str, int] = field(default_factory=dict)
    activity_summary: ActivitySummary | None = None
    baseline_activity: ActivitySummary | None = None
    latest_logs: deque[str] = field(default_factory=lambda: deque(maxlen=6))
    next_scheduled_utc: datetime | None = None
    next_wait_seconds: float | None = None
    session_finished_utc: datetime | None = None
    message_id: int | None = None
    last_render_text: str | None = None
    last_publish_monotonic: float = 0.0


@dataclass
class TelegramAccountTotals:
    current_utc_date: str = ""
    current_utc_week: str = ""
    day_index: int = 1
    daily_swaps: int = 0
    daily_ok_tx: int = 0
    daily_fail_tx: int = 0
    daily_network_fee: dict[str, Decimal] = field(default_factory=dict)
    daily_swap_fee: dict[str, Decimal] = field(default_factory=dict)
    weekly_network_fee: dict[str, Decimal] = field(default_factory=dict)
    weekly_swap_fee: dict[str, Decimal] = field(default_factory=dict)
    lifetime_swaps: int = 0
    lifetime_ok_tx: int = 0
    lifetime_fail_tx: int = 0
    lifetime_network_fee: dict[str, Decimal] = field(default_factory=dict)
    lifetime_swap_fee: dict[str, Decimal] = field(default_factory=dict)


class TelegramRateLimitError(RuntimeError):
    def __init__(self, retry_after_seconds: int, description: str) -> None:
        super().__init__(description)
        self.retry_after_seconds = max(int(retry_after_seconds), 1)
        self.description = description


class TelegramMonitor:
    def __init__(self, runtime: RuntimeConfig) -> None:
        self.runtime = runtime
        self.log = logging.getLogger("autoswap_bot.telegram")
        self._session: aiohttp.ClientSession | None = None
        self._account_totals: dict[str, TelegramAccountTotals] = {}
        self._cards: dict[str, TelegramCardState] = {}
        self._terminal_logs: deque[str] = deque(
            maxlen=self.runtime.terminal_dashboard_logs_limit
        )
        self._terminal_last_render_monotonic: float = 0.0
        self._terminal_dashboard_paused = False
        self._state_file = runtime.telegram_state_file
        self._state_loaded = False
        self._telegram_backoff_until_monotonic: float = 0.0
        self._telegram_backoff_last_logged_second: int = 0
        self._telegram_message_id: int | None = None
        self._telegram_last_render_text: str | None = None
        self._telegram_last_publish_monotonic: float = 0.0
        self._publish_lock = asyncio.Lock()

    async def start(self) -> None:
        self._load_state()
        if not self.runtime.telegram_enabled:
            return
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    def set_terminal_dashboard_paused(self, paused: bool) -> None:
        self._terminal_dashboard_paused = paused
        if not paused:
            self._render_terminal_dashboard(force=True)

    async def _refresh_outputs(self, card: TelegramCardState, *, force: bool) -> None:
        self._cards[card.account_name] = card
        self._render_terminal_dashboard(force=force)
        if self.runtime.telegram_enabled:
            await self._publish(force=force)

    def create_card(
        self,
        account: AccountConfig,
        prepared_run: PreparedAccountRun,
        strategy_label: str,
    ) -> TelegramCardState:
        persisted = self._get_account_totals(account.name)

        card = TelegramCardState(
            account_name=account.name,
            display_index=account.display_index,
            proxy_label=account.proxy_label,
            strategy_label=strategy_label,
            session_started_utc=datetime.now(timezone.utc),
            total_rounds=prepared_run.rounds,
            pair_targets={},
            current_utc_date=persisted.current_utc_date,
            current_utc_week=persisted.current_utc_week,
            day_index=persisted.day_index,
            daily_swap_base=persisted.daily_swaps,
            daily_ok_tx_base=persisted.daily_ok_tx,
            daily_fail_tx_base=persisted.daily_fail_tx,
            daily_network_fee_base=dict(persisted.daily_network_fee),
            daily_swap_fee_base=dict(persisted.daily_swap_fee),
            weekly_network_fee_base=dict(persisted.weekly_network_fee),
            weekly_swap_fee_base=dict(persisted.weekly_swap_fee),
            lifetime_swap_base=persisted.lifetime_swaps,
            lifetime_ok_tx_base=persisted.lifetime_ok_tx,
            lifetime_fail_tx_base=persisted.lifetime_fail_tx,
            lifetime_network_fee_base=dict(persisted.lifetime_network_fee),
            lifetime_swap_fee_base=dict(persisted.lifetime_swap_fee),
            balances={symbol: Decimal("0") for symbol in SYMBOL_SHORT},
            latest_logs=deque(maxlen=self.runtime.telegram_latest_logs_limit),
        )
        self._cards[account.name] = card
        self._persist_card_state(card)
        return card

    async def attach_card(self, card: TelegramCardState | None) -> None:
        if card is None:
            return
        self._cards[card.account_name] = card
        self._render_terminal_dashboard(force=True)

    async def sync_round_progress(
        self,
        card: TelegramCardState | None,
        *,
        completed_rounds: int,
        force: bool = False,
    ) -> None:
        if card is None:
            return
        self._rollover_card_if_needed(card)
        normalized_completed = max(int(completed_rounds), 0)
        card.progress_completed_base = normalized_completed
        card.current_round_number = normalized_completed
        self._persist_card_state(card)
        await self._refresh_outputs(card, force=force)

    async def sync_daily_ok_tx_from_history(
        self,
        card: TelegramCardState | None,
        *,
        ok_tx_count: int,
        force: bool = False,
    ) -> None:
        if card is None:
            return
        self._rollover_card_if_needed(card)
        normalized_ok_tx = max(int(ok_tx_count), 0)
        card.daily_ok_tx_base = max(self._current_day_ok_tx_total(card), normalized_ok_tx)
        card.day_session_ok_tx_offset = card.tx_ok_count
        self._persist_card_state(card)
        await self._refresh_outputs(card, force=force)

    async def log_event(
        self,
        card: TelegramCardState | None,
        message: str,
        *,
        force: bool = False,
    ) -> None:
        if card is None:
            return
        self._rollover_card_if_needed(card)
        card.latest_logs.append(self._timestamped_log(message))
        self._append_terminal_log(card, message)
        await self._refresh_outputs(card, force=force)

    async def update_status(
        self,
        card: TelegramCardState | None,
        *,
        pair_key: str | None = None,
        round_number: int | None = None,
        phase: str | None = None,
        next_scheduled_utc: datetime | None = None,
        next_wait_seconds: float | None = None,
        route_plan: RoutePlan | None = None,
        clear_route: bool = False,
        force: bool = False,
    ) -> None:
        if card is None:
            return
        self._rollover_card_if_needed(card)
        if pair_key is not None:
            card.current_pair_key = pair_key
        if round_number is not None:
            card.current_round_number = round_number
        if phase is not None:
            card.phase = phase
        card.next_scheduled_utc = next_scheduled_utc
        card.next_wait_seconds = next_wait_seconds
        if clear_route:
            self._clear_route_preview(card)
        elif route_plan is not None:
            self._set_route_preview(card, route_plan)
        elif phase in {"WAITING", "WAITING_NEXT_DAY", "FINISHED"}:
            self._clear_route_preview(card)
        await self._refresh_outputs(card, force=force)

    async def update_balances(
        self,
        card: TelegramCardState | None,
        balances: dict[str, Decimal],
        *,
        force: bool = False,
    ) -> None:
        if card is None:
            return
        self._rollover_card_if_needed(card)
        card.balances.update(balances)
        await self._refresh_outputs(card, force=force)

    async def update_fee_totals(
        self,
        card: TelegramCardState | None,
        *,
        total_network_fee: dict[str, Decimal],
        total_swap_fee: dict[str, Decimal],
        force: bool = False,
    ) -> None:
        if card is None:
            return
        self._rollover_card_if_needed(card)
        card.total_network_fee = dict(total_network_fee)
        card.total_swap_fee = dict(total_swap_fee)
        self._persist_card_state(card)
        await self._refresh_outputs(card, force=force)

    async def update_free_fee_status(
        self,
        card: TelegramCardState | None,
        *,
        used: int,
        limit: int,
        network_fee_credit: dict[str, Decimal] | None = None,
        force: bool = False,
    ) -> None:
        if card is None:
            return
        self._rollover_card_if_needed(card)
        card.daily_free_fee_used = max(int(used), 0)
        card.daily_free_fee_limit = max(int(limit), 0)
        if network_fee_credit:
            card.free_network_fee_credit = self._merge_amount_maps(
                card.free_network_fee_credit,
                network_fee_credit,
            )
        self._persist_card_state(card)
        await self._refresh_outputs(card, force=force)

    async def record_tx_success(
        self,
        card: TelegramCardState | None,
        *,
        force: bool = False,
    ) -> None:
        if card is None:
            return
        self._rollover_card_if_needed(card)
        card.tx_ok_count += 1
        self._persist_card_state(card)
        await self._refresh_outputs(card, force=force)

    async def record_tx_failure(
        self,
        card: TelegramCardState | None,
        *,
        force: bool = False,
    ) -> None:
        if card is None:
            return
        self._rollover_card_if_needed(card)
        card.tx_fail_count += 1
        self._persist_card_state(card)
        await self._refresh_outputs(card, force=force)

    async def record_round_completed(
        self,
        card: TelegramCardState | None,
        *,
        pair_key: str,
        force: bool = True,
    ) -> None:
        if card is None:
            return
        self._rollover_card_if_needed(card)
        card.swap_transactions += 1
        card.pair_completed[pair_key] = card.pair_completed.get(pair_key, 0) + 1
        card.phase = "COMPLETED"
        card.next_scheduled_utc = None
        card.next_wait_seconds = None
        self._persist_card_state(card)
        await self._refresh_outputs(card, force=force)

    async def update_activity(
        self,
        card: TelegramCardState | None,
        summary: ActivitySummary | None,
        *,
        force: bool = False,
    ) -> None:
        if card is None or summary is None:
            return
        self._rollover_card_if_needed(card)
        if card.baseline_activity is None:
            card.baseline_activity = summary
        observed_total_volume = self._to_decimal_like(summary.total_volume)
        if observed_total_volume is not None:
            if card.last_observed_total_volume_cc is None:
                card.last_observed_total_volume_cc = observed_total_volume
            else:
                delta_volume = observed_total_volume - card.last_observed_total_volume_cc
                if delta_volume > Decimal("0"):
                    card.bot_start_volume_cc += delta_volume
                card.last_observed_total_volume_cc = observed_total_volume
        card.activity_summary = summary
        await self._refresh_outputs(card, force=force)

    def _set_route_preview(self, card: TelegramCardState, route_plan: RoutePlan) -> None:
        card.current_route_label = route_plan.label
        card.current_route_network_fee = dict(route_plan.total_network_fee_by_symbol)
        card.current_route_swap_fee = dict(route_plan.total_admin_and_liquidity_by_symbol)

    def _clear_route_preview(self, card: TelegramCardState) -> None:
        card.current_route_label = None
        card.current_route_network_fee = {}
        card.current_route_swap_fee = {}

    async def finalize(
        self,
        card: TelegramCardState | None,
        *,
        phase: str,
    ) -> None:
        if card is None:
            return
        self._rollover_card_if_needed(card)
        card.phase = phase
        card.session_finished_utc = datetime.now(timezone.utc)
        card.next_scheduled_utc = None
        card.next_wait_seconds = None
        self._clear_route_preview(card)
        self._persist_card_state(card)
        await self._refresh_outputs(card, force=True)

    async def _publish(self, *, force: bool) -> None:
        if not self.runtime.telegram_enabled:
            return
        if self._session is None:
            raise RuntimeError("TelegramMonitor belum di-start")
        async with self._publish_lock:
            cards = sorted(self._cards.values(), key=lambda item: item.display_index)
            if not cards:
                return
            for card in cards:
                self._rollover_card_if_needed(card)

            now = time.monotonic()
            if now < self._telegram_backoff_until_monotonic:
                return
            if (
                not force
                and self._telegram_message_id is not None
                and now - self._telegram_last_publish_monotonic < self.runtime.telegram_update_min_interval_seconds
            ):
                return

            text = self._trim_message(self._render_combined_card(cards))
            if self._telegram_message_id is not None and text == self._telegram_last_render_text:
                return

            try:
                if self._telegram_message_id is None:
                    response = await self._request(
                        "sendMessage",
                        {
                            "chat_id": self.runtime.telegram_chat_id,
                            "text": text,
                            "parse_mode": "HTML",
                            "disable_web_page_preview": True,
                        },
                    )
                    self._telegram_message_id = response.get("result", {}).get("message_id")
                else:
                    await self._request(
                        "editMessageText",
                        {
                            "chat_id": self.runtime.telegram_chat_id,
                            "message_id": self._telegram_message_id,
                            "text": text,
                            "parse_mode": "HTML",
                            "disable_web_page_preview": True,
                        },
                    )
                self._telegram_last_render_text = text
                self._telegram_last_publish_monotonic = now
            except TelegramRateLimitError as exc:
                self._telegram_backoff_until_monotonic = max(
                    self._telegram_backoff_until_monotonic,
                    time.monotonic() + exc.retry_after_seconds,
                )
                current_second = int(self._telegram_backoff_until_monotonic)
                if current_second != self._telegram_backoff_last_logged_second:
                    self._telegram_backoff_last_logged_second = current_second
                    self.log.warning(
                        "Telegram rate limit, jeda update %s detik: %s",
                        exc.retry_after_seconds,
                        exc.description,
                    )
            except Exception as exc:  # pragma: no cover - network/runtime guard
                self.log.warning("Gagal update Telegram combined card: %s", exc)

    def _append_terminal_log(self, card: TelegramCardState, message: str) -> None:
        timestamp = datetime.now().astimezone().strftime("%H:%M:%S")
        account_label = f"A{card.display_index}"
        rendered = self._terminalize_text(message, preserve_case=True)
        self._terminal_logs.append(f"[{timestamp}] {account_label:<4} {rendered}")

    def _render_terminal_dashboard(self, *, force: bool) -> None:
        if (
            not self.runtime.terminal_dashboard_enabled
            or self._terminal_dashboard_paused
            or not sys.stdout.isatty()
        ):
            return

        now = time.monotonic()
        if (
            not force
            and now - self._terminal_last_render_monotonic
            < self.runtime.terminal_dashboard_min_interval_seconds
        ):
            return

        cards = sorted(self._cards.values(), key=lambda item: item.display_index)
        if not cards:
            return

        col_widths, table_lines = self._dashboard_table_lines(cards)
        header_row = table_lines[0]
        row_width = len(header_row) - 2
        border = "+" + ("-" * row_width) + "+"
        strong_border = "+" + ("=" * row_width) + "+"

        lines = [
            strong_border,
            *self._dashboard_summary_lines(cards, row_width=row_width),
            border,
            *table_lines,
        ]

        lines.extend(
            [
                border,
                "",
                f"--- Execution Logs (last {self.runtime.terminal_dashboard_logs_limit}) ---",
            ]
        )
        if self._terminal_logs:
            lines.extend(self._terminal_logs)
        else:
            lines.append("-")
        lines.extend(
            [
                "",
                (
                    "Ctrl+C to stop  |  Round delay: "
                    f"{self.runtime.swap_delay_seconds_range.describe()}"
                ),
            ]
        )

        rendered = "\n".join(lines)
        sys.stdout.write("\x1b[2J\x1b[H")
        sys.stdout.write(rendered + "\n")
        sys.stdout.flush()
        self._terminal_last_render_monotonic = now

    def _dashboard_col_widths(self) -> tuple[int, ...]:
        return (12, 9, 10, 10, 12, 12, 24, 22, 12, 12, 64)

    def _dashboard_table_lines(self, cards: list[TelegramCardState]) -> tuple[tuple[int, ...], list[str]]:
        col_widths = self._dashboard_col_widths()
        lines = [
            self._table_row(
                (
                    "Akun",
                    "Status",
                    "CC",
                    "USDCx",
                    "CBTC",
                    "Progress",
                    "Plan",
                    "Fee Route",
                    "Distributed",
                    "Funding",
                    "Metrics",
                ),
                col_widths,
            ),
            self._table_row(
                ("", "", "", "", "", "", "", "", "", "", self._dashboard_metrics_header()),
                col_widths,
            ),
            self._table_row(
                tuple("-" * width for width in col_widths),
                col_widths,
            ),
        ]
        for card in cards:
            lines.append(
                self._table_row(
                    (
                        card.account_name,
                        self._dashboard_status(card),
                        self._fmt_balance(card.balances.get("CC", Decimal("0")), 4),
                        self._fmt_balance(card.balances.get("USDCx", Decimal("0")), 4),
                        self._fmt_balance(card.balances.get("CBTC", Decimal("0")), 8),
                        self._dashboard_progress(card),
                        self._dashboard_plan(card),
                        self._dashboard_route_fee(card),
                        self._dashboard_distributed(card),
                        self._dashboard_funding(card),
                        self._dashboard_metrics(card),
                    ),
                    col_widths,
                )
            )
        return col_widths, lines

    def _dashboard_summary_lines(self, cards: list[TelegramCardState], *, row_width: int) -> list[str]:
        total_round_targets = sum(card.total_rounds for card in cards)
        total_swaps = sum(self._progress_completed_total(card) for card in cards)
        failed_accounts = sum(1 for card in cards if card.phase.startswith("FAILED"))
        stopped_accounts = sum(1 for card in cards if card.phase.startswith("STOPPED"))
        active_accounts = len(cards) - failed_accounts - stopped_accounts
        local_now = datetime.now().astimezone()
        yesterday_total = self._aggregate_rebate_total(cards, "yesterday")
        this_week_total = self._aggregate_rebate_total(cards, "this_week")
        distributed_total = self._aggregate_distributed_total(cards)
        funding_total = self._aggregate_funding_total(cards)
        paid_fee_today = self._aggregate_current_day_total_fee(cards)
        paid_fee_week = self._aggregate_current_week_total_fee(cards)
        return [
            self._padded_line(
                (
                    f"Cantex Autoswap Bot | {local_now.strftime('%d/%m/%Y, %H.%M.%S %Z')} | "
                    f"{len(cards)} akun | Mode: {self._dashboard_mode_label()}"
                ),
                row_width,
            ),
            self._padded_line(
                (
                    f"Swaps: {total_swaps}/{total_round_targets} total | "
                    f"Active: {active_accounts} | Fail: {failed_accounts} | Stopped: {stopped_accounts}"
                ),
                row_width,
            ),
            self._padded_line(
                (
                    f"Rewards | yesterday: {self._format_cc_total(yesterday_total)} | "
                    f"this week: {self._format_cc_total(this_week_total)}"
                ),
                row_width,
            ),
            self._padded_line(
                f"Reward distributed total: {self._format_cc_total(distributed_total)}",
                row_width,
            ),
            self._padded_line(
                f"Funding total (excl rewards/rebates): {self._format_cc_total(funding_total)}",
                row_width,
            ),
            self._padded_line(
                (
                    "Fee paid (today | week, excl free): "
                    f"{self._format_amount_map_display(paid_fee_today)} | "
                    f"{self._format_amount_map_display(paid_fee_week)}"
                ),
                row_width,
            ),
            self._padded_line(
                f"State: {self._dashboard_state_label(cards)}",
                row_width,
            ),
        ]

    def _dashboard_mode_label(self) -> str:
        if self.runtime.full_24h_mode:
            return f"24H-{self.runtime.full_24h_startup_mode.upper()}"
        return self.runtime.execution_mode.upper()

    def _dashboard_state_label(self, cards: list[TelegramCardState]) -> str:
        phases = {card.phase for card in cards}
        if any(phase == "PROCESSING" for phase in phases):
            return "processing"
        if any(phase in {"WAITING", "WAITING_FEE", "WAITING_NEXT_DAY"} for phase in phases):
            return "cooldown"
        if all(phase in {"FINISHED", "STOPPED_MANUAL"} or phase.startswith("FAILED") for phase in phases):
            return "finished"
        if any(phase == "STARTING" for phase in phases):
            return "starting"
        return "live"

    def _dashboard_status(self, card: TelegramCardState) -> str:
        if card.phase == "WAITING":
            return "COOLDOWN"
        if card.phase == "WAITING_FEE":
            return "WAIT-FEE"
        if card.phase == "WAITING_NEXT_DAY":
            return "NEXT-DAY"
        if card.phase == "PROCESSING":
            return "PROCESS"
        if card.phase == "COMPLETED":
            return "OK"
        if card.phase == "FINISHED":
            return "FINISHED"
        if card.phase == "STOPPED_INSUFFICIENT_BALANCE":
            return "SALDO"
        if card.phase.startswith("FAILED"):
            return "FAILED"
        if card.phase.startswith("STOPPED"):
            return "STOPPED"
        return self._terminalize_text(card.phase)

    def _dashboard_progress(self, card: TelegramCardState) -> str:
        completed_total = self._progress_completed_total(card)
        activity_24h_count = self._activity_24h_swap_count(card.activity_summary)
        ok_total = activity_24h_count if activity_24h_count is not None else completed_total
        return f"R{completed_total}/{card.total_rounds} ok{ok_total}"

    def _dashboard_plan(self, card: TelegramCardState) -> str:
        strategy = self._build_strategy_line(card).split(":", 1)[-1].strip()
        route = (
            self._route_label_ascii(card.current_route_label)
            if card.current_route_label
            else (
                self._short_pair_ascii(card.current_pair_key)
                if card.current_pair_key
                else self._strategy_ascii(card.strategy_label)
            )
        )
        if card.phase == "WAITING_FEE" and card.next_wait_seconds is not None:
            return f"S{strategy} {route} fee {int(max(card.next_wait_seconds, 0))}s"
        if card.phase == "WAITING" and card.next_wait_seconds is not None:
            return f"S{strategy} {route} cd {int(max(card.next_wait_seconds, 0))}s"
        if card.phase == "WAITING_NEXT_DAY" and card.next_wait_seconds is not None:
            return f"S{strategy} quota done {int(max(card.next_wait_seconds, 0))}s"
        if card.phase == "PROCESSING":
            return f"S{strategy} {route} processing"
        if card.phase == "COMPLETED":
            return f"S{strategy} {route} completed"
        return f"S{strategy} {route} {self._dashboard_status(card).lower()}"

    def _dashboard_route_fee(self, card: TelegramCardState) -> str:
        network_fee = self._dashboard_compact_amount_map(card.current_route_network_fee)
        swap_fee = self._dashboard_compact_amount_map(card.current_route_swap_fee)
        if network_fee == "-" and swap_fee == "-":
            return "-"
        if swap_fee == "-":
            return f"N {network_fee}"
        if network_fee == "-":
            return f"S {swap_fee}"
        return f"N {network_fee} / S {swap_fee}"

    def _dashboard_distributed(self, card: TelegramCardState) -> str:
        summary = card.activity_summary
        if summary is None:
            return "-"
        return self._distributed_amount_compact(summary.distributed_reward)

    def _dashboard_funding(self, card: TelegramCardState) -> str:
        summary = card.activity_summary
        if summary is None:
            return "-"
        return self._funding_amount_compact(summary.funding_total)

    def _dashboard_metrics(self, card: TelegramCardState) -> str:
        summary = card.activity_summary
        activity_24h = self._dashboard_24h_activity(summary)
        yesterday = self._rebate_amount_compact(summary.rebates.get("yesterday")) if summary else "-"
        this_week = self._rebate_amount_compact(summary.rebates.get("this_week")) if summary else "-"
        gas_today = self._dashboard_gas(self._current_day_network_fee(card))
        free_fee = f"{card.daily_free_fee_used}/{card.daily_free_fee_limit}" if card.daily_free_fee_limit > 0 else "-"
        return self._compose_dashboard_metrics(
            activity_24h,
            yesterday,
            this_week,
            gas_today,
            free_fee,
        )

    def _dashboard_metrics_header(self) -> str:
        return self._compose_dashboard_metrics(
            "24h",
            "yesterday",
            "t week",
            "gas fee",
            "free swap",
            header=True,
        )

    def _compose_dashboard_metrics(
        self,
        activity: str,
        yesterday: str,
        this_week: str,
        gas_fee: str,
        free_swap: str,
        *,
        header: bool = False,
    ) -> str:
        align = "center" if header else "right"
        parts = (
            self._align_terminal(activity, 17, align=align),
            self._align_terminal(yesterday, 9, align=align),
            self._align_terminal(this_week, 8, align=align),
            self._align_terminal(gas_fee, 9, align=align),
            self._align_terminal(free_swap, 9, align=align),
        )
        return " | ".join(parts)

    def _dashboard_24h_activity(self, summary: ActivitySummary | None) -> str:
        if summary is None:
            return "-"
        volume_24h = self._metric_decimal(summary.volume_24h, places=2)
        swaps_24h = self._metric_value(summary.swaps_24h)
        if volume_24h == "-" and swaps_24h == "-":
            return "-"
        if swaps_24h == "-":
            return f"{volume_24h} CC"
        if volume_24h == "-":
            return f"{swaps_24h}x"
        return f"{volume_24h} CC / {swaps_24h}x"

    def _table_row(self, values: tuple[str, ...], widths: tuple[int, ...]) -> str:
        padded = [
            self._truncate_terminal(value, width).ljust(width)
            for value, width in zip(values, widths)
        ]
        return "| " + " | ".join(padded) + " |"

    def _truncate_terminal(self, value: str, width: int) -> str:
        text = self._terminalize_text(value, preserve_spacing=True)
        if len(text) <= width:
            return text
        if width <= 3:
            return text[:width]
        return text[: width - 3] + "..."

    def _align_terminal(self, value: str, width: int, *, align: str) -> str:
        text = self._truncate_terminal(value, width)
        if align == "right":
            return text.rjust(width)
        if align == "center":
            return text.center(width)
        return text.ljust(width)

    def _padded_line(self, value: str, width: int) -> str:
        text = self._truncate_terminal(value, width)
        return "| " + text.ljust(width) + " |"

    def _terminalize_text(
        self,
        value: str,
        *,
        preserve_case: bool = False,
        preserve_spacing: bool = False,
    ) -> str:
        text = str(value)
        replacements = {
            "→": "->",
            "🎁": "[free]",
            "⏳": "[wait]",
            "🔄": "[step]",
            "✅": "[ok]",
            "❌": "[fail]",
            "⏭️": "[skip]",
            "⏭": "[skip]",
            "🛟": "[refill]",
            "🚀": "[start]",
            "🏁": "[done]",
            "🎉": "[done]",
            "ℹ️": "[info]",
            "⚠️": "[warn]",
        }
        for source, target in replacements.items():
            text = text.replace(source, target)
        text = re.sub(r"[^\x20-\x7E]", "", text)
        if preserve_spacing:
            text = text.replace("\t", " ").rstrip()
        else:
            text = re.sub(r"\s+", " ", text).strip()
        return text if preserve_case else text

    async def _request(self, method: str, payload: dict) -> dict:
        if self._session is None:
            raise RuntimeError("TelegramMonitor belum di-start")
        url = f"https://api.telegram.org/bot{self.runtime.telegram_bot_token}/{method}"
        async with self._session.post(url, json=payload) as response:
            data = await response.json(content_type=None)
            if response.status == 429 or data.get("error_code") == 429:
                parameters = data.get("parameters") or {}
                retry_after = parameters.get("retry_after", 30)
                raise TelegramRateLimitError(
                    retry_after_seconds=int(retry_after),
                    description=str(data.get("description", "Too Many Requests")),
                )
            if not response.ok or not data.get("ok", False):
                raise RuntimeError(f"Telegram API error: {data}")
            return data

    def _render_card(self, card: TelegramCardState) -> str:
        latest_logs = "\n".join(card.latest_logs) if card.latest_logs else "-"
        edited_at = datetime.now(timezone.utc).strftime("%H.%M.%S")

        sections = [
            f"<b>{html.escape(f'🔵 Acc {card.display_index}')}</b>",
            f"Status: {html.escape(self._build_status_line(card))}",
            "",
            html.escape(self._build_strategy_line(card)),
            html.escape(self._build_balances_line(card)),
            "",
            html.escape(self._build_day_line(card)),
            html.escape(self._build_swap_totals_line(card)),
            html.escape(self._build_gas_totals_line(card)),
            f"Uptime: {html.escape(self._format_duration(datetime.now(timezone.utc) - card.session_started_utc))}",
            f"🌐 Proxy: {html.escape(card.proxy_label)}",
            html.escape(self._build_activity_line(card)),
            html.escape(self._build_rebates_line(card)),
            "",
            "<b>📝 Latest Logs</b>",
            f"<pre>{html.escape(latest_logs)}</pre>",
            f"<i>Edited {html.escape(edited_at)} UTC</i>",
        ]
        return "\n".join(sections)

    def _render_combined_card(self, cards: list[TelegramCardState]) -> str:
        local_now = datetime.now().astimezone()
        total_round_targets = sum(card.total_rounds for card in cards)
        total_swaps = sum(self._progress_completed_total(card) for card in cards)
        failed_accounts = sum(1 for card in cards if card.phase.startswith("FAILED"))
        stopped_accounts = sum(1 for card in cards if card.phase.startswith("STOPPED"))
        active_accounts = len(cards) - failed_accounts - stopped_accounts
        yesterday_total = self._aggregate_rebate_total(cards, "yesterday")
        this_week_total = self._aggregate_rebate_total(cards, "this_week")
        distributed_total = self._aggregate_distributed_total(cards)
        funding_total = self._aggregate_funding_total(cards)
        paid_fee_today = self._aggregate_current_day_total_fee(cards)
        paid_fee_week = self._aggregate_current_week_total_fee(cards)
        edited_at = datetime.now(timezone.utc).strftime("%H.%M.%S")

        sections = [
            "<b>🚀 Cantex Autoswap Bot</b>",
            html.escape(f"🕒 {local_now.strftime('%d/%m/%Y, %H.%M.%S %Z')}"),
            html.escape(
                f"📦 {len(cards)} akun | Mode: {self._dashboard_mode_label()} | State: {self._dashboard_state_label(cards)}"
            ),
            html.escape(
                f"🔁 Swaps: {total_swaps}/{total_round_targets} total | Active: {active_accounts} | Fail: {failed_accounts} | Stopped: {stopped_accounts}"
            ),
            html.escape(f"🎁 Reward yesterday total: {self._format_cc_total(yesterday_total)}"),
            html.escape(f"🏆 Reward this week total: {self._format_cc_total(this_week_total)}"),
            html.escape(f"💰 Reward distributed total: {self._format_cc_total(distributed_total)}"),
            html.escape(f"Funding total (excl rewards/rebates): {self._format_cc_total(funding_total)}"),
            html.escape(
                "💸 Fee paid total (today | week, excl free): "
                f"{self._format_amount_map_display(paid_fee_today)} | "
                f"{self._format_amount_map_display(paid_fee_week)}"
            ),
            "",
            "<b>📱 Ringkasan Akun</b>",
        ]
        for card in cards:
            sections.extend(self._render_combined_account_section(card))
        sections.extend(
            [
                "<b>📝 Update Terakhir</b>",
                *self._render_combined_updates(),
                f"<i>Edited {html.escape(edited_at)} UTC</i>",
            ]
        )
        return "\n".join(sections)

    def _render_combined_dashboard_table(self, cards: list[TelegramCardState]) -> str:
        _, table_lines = self._dashboard_table_lines(cards)
        header_row = table_lines[0]
        row_width = len(header_row) - 2
        border = "+" + ("-" * row_width) + "+"
        strong_border = "+" + ("=" * row_width) + "+"
        return "\n".join([strong_border, *table_lines, border])

    def _combined_plan(self, card: TelegramCardState) -> str:
        strategy = self._build_strategy_line(card).split(":", 1)[-1].strip()
        route = (
            self._route_label_ascii(card.current_route_label)
            if card.current_route_label
            else (
                self._short_pair_ascii(card.current_pair_key)
                if card.current_pair_key
                else self._strategy_ascii(card.strategy_label)
            )
        )
        if card.phase == "WAITING_FEE" and card.next_wait_seconds is not None:
            return f"S{strategy} {route} | fee {int(max(card.next_wait_seconds, 0))}s"
        if card.phase == "WAITING" and card.next_wait_seconds is not None:
            return f"S{strategy} {route} | cd {int(max(card.next_wait_seconds, 0))}s"
        if card.phase == "WAITING_NEXT_DAY" and card.next_wait_seconds is not None:
            return f"S{strategy} quota done | {int(max(card.next_wait_seconds, 0))}s"
        return f"S{strategy} {route}"

    def _render_combined_account_section(self, card: TelegramCardState) -> list[str]:
        summary = card.activity_summary
        yesterday_rebate = self._rebate_amount(summary.rebates.get("yesterday")) if summary else "-"
        this_week_rebate = self._rebate_amount(summary.rebates.get("this_week")) if summary else "-"
        distributed_rebate = self._distributed_amount(summary.distributed_reward) if summary else "-"
        funding_total = self._funding_amount(summary.funding_total) if summary else "-"
        daily_fee_spent = self._format_amount_map_display(self._current_day_network_fee(card))
        activity_24h = self._dashboard_24h_activity(summary)
        progress = self._dashboard_progress(card)
        plan = self._combined_plan(card)
        fee_route = self._dashboard_route_fee(card)
        cc_balance = self._fmt_balance(card.balances.get("CC", Decimal("0")), 4)
        usdcx_balance = self._fmt_balance(card.balances.get("USDCx", Decimal("0")), 4)
        cbtc_balance = self._fmt_balance(card.balances.get("CBTC", Decimal("0")), 8)
        title = (
            f"{self._telegram_status_emoji(card)} {self._display_account_name(card.account_name)} "
            f"[ {self._telegram_status_text(card)} ]"
        )
        return [
            f"<b>{html.escape(title)}</b>",
            html.escape(f"{progress} | {plan}"),
            html.escape(f"Balance | CC {cc_balance} | U {usdcx_balance} | B {cbtc_balance}"),
            html.escape(
                " | ".join(
                    [
                        f"Fee route {fee_route}",
                        f"24h {activity_24h}",
                        f"Y {yesterday_rebate}",
                        f"W {this_week_rebate}",
                        f"Dist {distributed_rebate}",
                        f"Fund {funding_total}",
                        f"Gas {daily_fee_spent}",
                        f"Free {card.daily_free_fee_used}/{card.daily_free_fee_limit}",
                    ]
                )
            ),
            "",
        ]

    def _render_combined_logs(self) -> str:
        if not self._terminal_logs:
            return "-"
        return "\n".join(list(self._terminal_logs)[-8:])

    def _render_combined_updates(self) -> list[str]:
        if not self._terminal_logs:
            return ["-"]
        return [html.escape(f"• {entry}") for entry in list(self._terminal_logs)[-5:]]

    def _combined_status(self, card: TelegramCardState) -> str:
        mapping = {
            "WAITING": "IDLE",
            "WAITING_FEE": "WAIT-FEE",
            "WAITING_NEXT_DAY": "NEXT-DAY",
            "PROCESSING": "PROCESS",
            "COMPLETED": "OK",
            "FINISHED": "DONE",
        }
        if card.phase.startswith("FAILED"):
            return "FAILED"
        if card.phase.startswith("STOPPED"):
            return "STOPPED"
        return mapping.get(card.phase, card.phase)

    def _display_account_name(self, account_name: str) -> str:
        if account_name.lower().startswith("wallet-"):
            suffix = account_name.split("-", 1)[1]
            return f"Wallet-{suffix}"
        return account_name

    def _telegram_status_text(self, card: TelegramCardState) -> str:
        mapping = {
            "STARTING": "STARTING",
            "WAITING": "WAIT",
            "WAITING_FEE": "WAIT FEE",
            "WAITING_NEXT_DAY": "NEXT DAY",
            "PROCESSING": "PROCESS",
            "COMPLETED": "DONE",
            "FINISHED": "FINISHED",
        }
        if card.phase == "STOPPED_INSUFFICIENT_BALANCE":
            return "SALDO KURANG"
        if card.phase.startswith("FAILED"):
            return "FAILED"
        if card.phase.startswith("STOPPED"):
            return "STOPPED"
        return mapping.get(card.phase, card.phase.replace("_", " "))

    def _telegram_status_emoji(self, card: TelegramCardState) -> str:
        if card.phase.startswith("FAILED"):
            return "❌"
        if card.phase.startswith("STOPPED"):
            return "⛔"
        mapping = {
            "STARTING": "🚀",
            "WAITING": "⏳",
            "WAITING_FEE": "⛽",
            "WAITING_NEXT_DAY": "🌙",
            "PROCESSING": "🔄",
            "COMPLETED": "✅",
            "FINISHED": "🏁",
        }
        return mapping.get(card.phase, "📌")

    def _combined_fail_count(self, card: TelegramCardState) -> int:
        return sum(1 for entry in card.latest_logs if "failed" in entry.lower() or "error" in entry.lower())

    def _build_status_line(self, card: TelegramCardState) -> str:
        pair = (
            self._short_pair(card.current_pair_key)
            if card.current_pair_key
            else self._strategy_short(card.strategy_label)
        )
        round_number = max(card.current_round_number, self._progress_completed_total(card), 0)
        if card.phase == "WAITING" and card.next_wait_seconds is not None:
            wait_seconds = int(max(card.next_wait_seconds, 0))
            return f"{pair} R{round_number}/{card.total_rounds} | ⏳ Wait {wait_seconds}s"
        if card.phase == "WAITING_FEE" and card.next_wait_seconds is not None:
            wait_seconds = int(max(card.next_wait_seconds, 0))
            return f"{pair} R{round_number}/{card.total_rounds} | ⛽ Wait Fee {wait_seconds}s"
        return f"{pair} R{round_number}/{card.total_rounds} | {self._display_phase(card.phase)}"

    def _build_balances_line(self, card: TelegramCardState) -> str:
        cc = self._fmt_balance(card.balances.get("CC", Decimal("0")), 2)
        usdcx = self._fmt_balance(card.balances.get("USDCx", Decimal("0")), 4)
        cbtc = self._fmt_balance(card.balances.get("CBTC", Decimal("0")), 8)
        return f"💰 Balances: CC {cc} | U {usdcx} | B {cbtc}"

    def _build_strategy_line(self, card: TelegramCardState) -> str:
        match = re.search(r"Strategi\s+(\d+)", card.strategy_label, re.IGNORECASE)
        if match:
            return f"Strategi: {match.group(1)}"
        return f"Strategi: {card.strategy_label}"

    def _build_day_line(self, card: TelegramCardState) -> str:
        now_utc = datetime.now(timezone.utc)
        next_midnight_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        if next_midnight_utc <= now_utc:
            next_midnight_utc += timedelta(days=1)
        remaining = self._format_duration(next_midnight_utc - now_utc)
        return f"🗓️ Hari Ke: {card.day_index} Berakhir dalam: {remaining}"

    def _build_swap_totals_line(self, card: TelegramCardState) -> str:
        total_daily_swaps = self._current_day_swap_total(card)
        total_lifetime_swaps = card.lifetime_swap_base + card.swap_transactions
        return (
            f"🔢 Total Swap Hari Ini: {total_daily_swaps} | "
            f"Total Swap Sejak Bot Start: {total_lifetime_swaps}"
        )

    def _build_gas_totals_line(self, card: TelegramCardState) -> str:
        total_daily_fee = self._current_day_network_fee(card)
        total_lifetime_fee = self._current_lifetime_network_fee(card)
        return (
            f"Gas Fee Hari Ini: {self._format_amount_map(total_daily_fee)} | "
            f"Total Gas Fee Dibayar: {self._format_amount_map(total_lifetime_fee)}"
        )

    def _build_fee_line(self, card: TelegramCardState) -> str:
        today_fee = self._current_day_total_fee(card)
        week_fee = self._current_week_total_fee(card)
        total_fee = self._merge_amount_maps(
            self._current_lifetime_network_fee(card),
            self._current_lifetime_swap_fee(card),
        )
        return (
            f"Fees: Today {self._format_amount_map(today_fee)} | "
            f"Week {self._format_amount_map(week_fee)} | "
            f"Total {self._format_amount_map(total_fee)}"
        )

    def _build_swaps_line(self, card: TelegramCardState) -> str:
        parts = [f"🔁 Swaps: {card.swap_transactions}"]
        pair_keys = list(dict.fromkeys([*card.pair_targets.keys(), *card.pair_completed.keys()]))
        for pair_key in pair_keys:
            completed = card.pair_completed.get(pair_key, 0)
            target = card.pair_targets.get(pair_key)
            if target:
                parts.append(f"{self._short_pair(pair_key)}: {completed}/{target}")
            else:
                parts.append(f"{self._short_pair(pair_key)}: {completed}")
        return " | ".join(parts)

    def _build_activity_line(self, card: TelegramCardState) -> str:
        summary = card.activity_summary
        if summary is None:
            return "🏆 Activity 24h: -"

        if summary.swaps_24h is not None or summary.volume_24h is not None:
            return (
                "🏆 Activity 24h: "
                f"{self._metric_value(summary.swaps_24h)} swaps | "
                f"{self._metric_decimal(summary.volume_24h)} CC"
            )

        reward = self._metric_value(summary.reward_total)
        volume = self._metric_value(summary.volume_usd if summary.volume_usd is not None else summary.total_volume)
        tx = self._metric_value(summary.tx_count)
        rank = self._metric_value(summary.rank)
        return f"🏆 Stats: Reward {reward} | Volume {volume} | Tx {tx} | Rank {rank}"

    def _build_rebates_line(self, card: TelegramCardState) -> str:
        summary = card.activity_summary
        distributed = self._distributed_amount(
            summary.distributed_reward if summary is not None else None
        )
        if summary is None or not summary.rebates:
            return f"💸 CC Rebates: Yesterday - | This Week - | Last Week - | Distributed {distributed}"
        return (
            "💸 CC Rebates: "
            f"Yesterday {self._rebate_amount(summary.rebates.get('yesterday'))} | "
            f"This Week {self._rebate_amount(summary.rebates.get('this_week'))} | "
            f"Last Week {self._rebate_amount(summary.rebates.get('last_week'))} | "
            f"Distributed {distributed}"
        )

    def _timestamped_log(self, message: str) -> str:
        now = datetime.now(timezone.utc)
        return f"{now.strftime('%H.%M.%S')} {message[:160]}"

    def _fmt_balance(self, value: Decimal, places: int) -> str:
        return f"{value:.{places}f}"

    def _fmt_fee(self, value: Decimal) -> str:
        quantized = value.quantize(Decimal("0.001"))
        rendered = format(quantized, "f")
        if "." in rendered:
            rendered = rendered.rstrip("0").rstrip(".")
        return rendered or "0"

    def _format_duration(self, duration) -> str:
        total_seconds = int(duration.total_seconds())
        hours, remainder = divmod(max(total_seconds, 0), 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours}h{minutes:02d}m{seconds:02d}s"

    def _short_pair(self, pair_key: str) -> str:
        sell_symbol, buy_symbol = pair_key.split("->", 1)
        return f"{SYMBOL_SHORT.get(sell_symbol, sell_symbol)}→{SYMBOL_SHORT.get(buy_symbol, buy_symbol)}"

    def _strategy_short(self, strategy_label: str) -> str:
        upper = strategy_label.upper()
        if "CC -> USDCX -> CBTC" in upper:
            return "CC→U→B"
        for left, right in (
            ("CC", "USDCX"),
            ("CC", "CBTC"),
        ):
            token = f"{left} -> {right}"
            if token in upper:
                return f"{SYMBOL_SHORT.get(left, left)}→{SYMBOL_SHORT.get(right, right)}"
        return strategy_label

    def _short_pair_ascii(self, pair_key: str) -> str:
        sell_symbol, buy_symbol = pair_key.split("->", 1)
        return f"{SYMBOL_SHORT.get(sell_symbol, sell_symbol)}->{SYMBOL_SHORT.get(buy_symbol, buy_symbol)}"

    def _strategy_ascii(self, strategy_label: str) -> str:
        upper = strategy_label.upper()
        if "CC -> USDCX -> CBTC" in upper:
            return "CC->U->B"
        for left, right in (
            ("CC", "USDCX"),
            ("CC", "CBTC"),
        ):
            token = f"{left} -> {right}"
            if token in upper:
                return f"{SYMBOL_SHORT.get(left, left)}->{SYMBOL_SHORT.get(right, right)}"
        return self._terminalize_text(strategy_label)

    def _route_label_ascii(self, route_label: str | None) -> str:
        if not route_label:
            return "-"
        symbols = [segment.strip() for segment in route_label.split("->")]
        shortened = [SYMBOL_SHORT.get(symbol, symbol) for symbol in symbols]
        return "->".join(shortened)

    def _dashboard_compact_amount_map(self, values: dict[str, Decimal]) -> str:
        if not values:
            return "-"
        parts: list[str] = []
        for symbol, amount in sorted(values.items()):
            rendered = self._fmt_fee(amount)
            if rendered == "0":
                continue
            short_symbol = SYMBOL_SHORT.get(symbol, symbol)
            parts.append(rendered if short_symbol == "CC" else f"{short_symbol}{rendered}")
        return " ".join(parts) if parts else "-"

    def _to_decimal_like(self, value: str | None) -> Decimal | None:
        if value in {None, ""}:
            return None
        cleaned = re.sub(r"[^0-9eE+.\-]+", "", value)
        if cleaned in {"", "-", ".", "-."}:
            return None
        try:
            return Decimal(cleaned)
        except InvalidOperation:
            return None

    def _metric_value(self, value: str | None) -> str:
        return value if value not in {None, ""} else "-"

    def _metric_decimal(self, value: str | None, *, places: int = 3) -> str:
        decimal_value = self._to_decimal_like(value)
        if decimal_value is None:
            return self._metric_value(value)
        return self._format_decimal_value(decimal_value, places=places)

    def _format_decimal_value(self, value: Decimal, *, places: int = 3) -> str:
        quantizer = Decimal("1").scaleb(-places)
        rendered = format(value.quantize(quantizer), ",f")
        if "." in rendered:
            rendered = rendered.rstrip("0").rstrip(".")
        return rendered

    def _format_bot_start_volume(self, card: TelegramCardState) -> str:
        if card.bot_start_volume_cc <= Decimal("0"):
            return "-"
        return f"{self._format_decimal_value(card.bot_start_volume_cc, places=2)} CC"

    def _format_24h_volume(self, summary: ActivitySummary | None) -> str:
        if summary is None:
            return "-"
        rendered = self._metric_decimal(summary.volume_24h, places=2)
        if rendered == "-":
            return "-"
        return f"{rendered} CC"

    def _rebate_amount(self, value: str | None) -> str:
        if value in {None, ""}:
            return "-"
        match = re.search(r"(-?[0-9][0-9,]*\.?[0-9]*)\s*CC", value, re.IGNORECASE)
        if match:
            decimal_value = self._to_decimal_like(match.group(1))
            if decimal_value is None:
                return f"{match.group(1)} CC"
            return f"{self._format_decimal_value(decimal_value, places=4)} CC"
        decimal_value = self._to_decimal_like(value)
        if decimal_value is not None:
            return f"{self._format_decimal_value(decimal_value, places=4)} CC"
        return value

    def _rebates_summary(self, summary: ActivitySummary | None) -> str:
        if summary is None:
            return "Y- TW- LW-"
        return (
            f"Y{self._rebate_amount(summary.rebates.get('yesterday'))} "
            f"TW{self._rebate_amount(summary.rebates.get('this_week'))} "
            f"LW{self._rebate_amount(summary.rebates.get('last_week'))}"
        )

    def _rebates_summary_compact(self, summary: ActivitySummary | None) -> str:
        if summary is None:
            return "Y- TW- LW-"
        return (
            f"Y{self._rebate_amount_compact(summary.rebates.get('yesterday'))} "
            f"TW{self._rebate_amount_compact(summary.rebates.get('this_week'))} "
            f"LW{self._rebate_amount_compact(summary.rebates.get('last_week'))}"
        )

    def _rebate_amount_compact(self, value: str | None) -> str:
        rendered = self._rebate_amount(value)
        if rendered == "-":
            return rendered
        return rendered.removesuffix(" CC")

    def _distributed_amount(self, value: str | None) -> str:
        return self._rebate_amount(value)

    def _distributed_amount_compact(self, value: str | None) -> str:
        rendered = self._distributed_amount(value)
        if rendered == "-":
            return rendered
        return rendered.removesuffix(" CC")

    def _funding_amount(self, value: str | None) -> str:
        return self._rebate_amount(value)

    def _funding_amount_compact(self, value: str | None) -> str:
        rendered = self._funding_amount(value)
        if rendered == "-":
            return rendered
        return rendered.removesuffix(" CC")

    def _aggregate_rebate_total(self, cards: list[TelegramCardState], rebate_key: str) -> Decimal | None:
        total = Decimal("0")
        found = False
        for card in cards:
            summary = card.activity_summary
            if summary is None:
                continue
            rebate_value = self._rebate_amount(summary.rebates.get(rebate_key))
            decimal_value = self._to_decimal_like(rebate_value)
            if decimal_value is None:
                continue
            total += decimal_value
            found = True
        return total if found else None

    def _aggregate_distributed_total(self, cards: list[TelegramCardState]) -> Decimal | None:
        total = Decimal("0")
        found = False
        for card in cards:
            summary = card.activity_summary
            if summary is None:
                continue
            distributed_value = self._distributed_amount(summary.distributed_reward)
            decimal_value = self._to_decimal_like(distributed_value)
            if decimal_value is None:
                continue
            total += decimal_value
            found = True
        return total if found else None

    def _aggregate_funding_total(self, cards: list[TelegramCardState]) -> Decimal | None:
        total = Decimal("0")
        found = False
        for card in cards:
            summary = card.activity_summary
            if summary is None:
                continue
            funding_value = self._funding_amount(summary.funding_total)
            decimal_value = self._to_decimal_like(funding_value)
            if decimal_value is None:
                continue
            total += decimal_value
            found = True
        return total if found else None

    def _format_cc_total(self, value: Decimal | None) -> str:
        if value is None:
            return "-"
        return f"{self._format_decimal_value(value, places=4)} CC"

    def _dashboard_gas(self, values: dict[str, Decimal]) -> str:
        rendered = self._format_amount_map(values)
        if rendered == "-":
            return rendered
        return (
            rendered.replace("CC ", "")
            .replace("U ", "U")
            .replace("B ", "B")
            .replace(", ", " ")
        )

    def _display_phase(self, phase: str) -> str:
        mapping = {
            "STARTING": "🚀 Starting",
            "PROCESSING": "🔄 Processing",
            "COMPLETED": "✅ Completed",
            "FINISHED": "🏁 Finished",
            "DRY-RUN": "🧪 Dry Run",
            "STOPPED_USER": "⛔ Stopped by User",
            "STOPPED_MANUAL": "⛔ Manual Stop",
        }
        stop_mapping = {
            "STOPPED_INSUFFICIENT_BALANCE": "⛔ Saldo Kurang",
            "STOPPED_LOW_BALANCE_MODE_I": "⛔ Stop on Low Balance",
            "STOPPED_ROUND_AFFORDABILITY_CHECK_FAILED": "⛔ Round Not Affordable",
            "STOPPED_MIN_TICKET_SIZE": "⛔ Below Min Ticket",
            "STOPPED_MANUAL_STOP": "⛔ Manual Stop",
            "STOPPED_SWAP_HOP_FAILED": "⛔ Swap Failed",
        }
        if phase in stop_mapping:
            return stop_mapping[phase]
        if phase.startswith("STOPPED_"):
            raw_reason = phase.removeprefix("STOPPED_")
            pretty = raw_reason.replace("_", " ").title()
            return f"⛔ {pretty}"
        if phase.startswith("FAILED"):
            return "❌ Failed"
        return mapping.get(phase, phase.replace("_", " ").title())

    def _trim_message(self, text: str) -> str:
        if len(text) <= 3900:
            return text
        return text[:3600] + "\n<i>Message trimmed</i>"

    def _format_amount_map_display(self, values: dict[str, Decimal]) -> str:
        if not values:
            return "-"
        parts: list[str] = []
        for symbol, amount in sorted(values.items()):
            rendered = self._fmt_fee(amount)
            if rendered == "0":
                continue
            parts.append(f"{rendered} {symbol}")
        return ", ".join(parts) if parts else "-"

    def _format_amount_map(self, values: dict[str, Decimal]) -> str:
        if not values:
            return "-"
        parts: list[str] = []
        for symbol, amount in sorted(values.items()):
            rendered = self._fmt_fee(amount)
            if rendered == "0":
                continue
            parts.append(f"{SYMBOL_SHORT.get(symbol, symbol)} {rendered}")
        return ", ".join(parts) if parts else "-"

    def _merge_amount_maps(
        self,
        left: dict[str, Decimal],
        right: dict[str, Decimal],
    ) -> dict[str, Decimal]:
        merged: dict[str, Decimal] = {}
        for source in (left, right):
            for symbol, amount in source.items():
                merged[symbol] = merged.get(symbol, Decimal("0")) + amount
        return merged

    def _subtract_amount_maps(
        self,
        left: dict[str, Decimal],
        right: dict[str, Decimal],
    ) -> dict[str, Decimal]:
        merged: dict[str, Decimal] = dict(left)
        for symbol, amount in right.items():
            merged[symbol] = merged.get(symbol, Decimal("0")) - amount
        return {
            symbol: amount
            for symbol, amount in merged.items()
            if amount > Decimal("0")
        }

    def _session_paid_network_fee(self, card: TelegramCardState) -> dict[str, Decimal]:
        return self._subtract_amount_maps(
            card.total_network_fee,
            card.free_network_fee_credit,
        )

    def _current_lifetime_network_fee(self, card: TelegramCardState) -> dict[str, Decimal]:
        return self._merge_amount_maps(
            card.lifetime_network_fee_base,
            self._session_paid_network_fee(card),
        )

    def _current_day_swap_fee(self, card: TelegramCardState) -> dict[str, Decimal]:
        session_today_swap_fee = self._subtract_amount_maps(
            card.total_swap_fee,
            card.day_session_swap_fee_offset,
        )
        return self._merge_amount_maps(card.daily_swap_fee_base, session_today_swap_fee)

    def _current_lifetime_swap_fee(self, card: TelegramCardState) -> dict[str, Decimal]:
        return self._merge_amount_maps(
            card.lifetime_swap_fee_base,
            card.total_swap_fee,
        )

    def _current_week_swap_fee(self, card: TelegramCardState) -> dict[str, Decimal]:
        session_week_swap_fee = self._subtract_amount_maps(
            card.total_swap_fee,
            card.week_session_swap_fee_offset,
        )
        return self._merge_amount_maps(card.weekly_swap_fee_base, session_week_swap_fee)

    def _session_total_fee(self, card: TelegramCardState) -> dict[str, Decimal]:
        return self._merge_amount_maps(
            self._session_paid_network_fee(card),
            card.total_swap_fee,
        )

    def _current_day_total_fee(self, card: TelegramCardState) -> dict[str, Decimal]:
        return self._merge_amount_maps(
            self._current_day_network_fee(card),
            self._current_day_swap_fee(card),
        )

    def _current_week_total_fee(self, card: TelegramCardState) -> dict[str, Decimal]:
        return self._merge_amount_maps(
            self._current_week_network_fee(card),
            self._current_week_swap_fee(card),
        )

    def _aggregate_current_day_total_fee(self, cards: list[TelegramCardState]) -> dict[str, Decimal]:
        merged: dict[str, Decimal] = {}
        for card in cards:
            merged = self._merge_amount_maps(merged, self._current_day_total_fee(card))
        return merged

    def _aggregate_current_week_total_fee(self, cards: list[TelegramCardState]) -> dict[str, Decimal]:
        merged: dict[str, Decimal] = {}
        for card in cards:
            merged = self._merge_amount_maps(merged, self._current_week_total_fee(card))
        return merged

    def _progress_completed_total(self, card: TelegramCardState) -> int:
        return max(card.progress_completed_base, 0)

    def _activity_24h_swap_count(self, summary: ActivitySummary | None) -> int | None:
        if summary is None:
            return None
        decimal_value = self._to_decimal_like(summary.swaps_24h)
        if decimal_value is None:
            return None
        return max(int(decimal_value), 0)

    def _current_day_swap_total(self, card: TelegramCardState) -> int:
        return card.daily_swap_base + max(card.swap_transactions - card.day_session_swap_offset, 0)

    def _current_day_ok_tx_total(self, card: TelegramCardState) -> int:
        return card.daily_ok_tx_base + max(card.tx_ok_count - card.day_session_ok_tx_offset, 0)

    def _current_day_fail_tx_total(self, card: TelegramCardState) -> int:
        return card.daily_fail_tx_base + max(card.tx_fail_count - card.day_session_fail_tx_offset, 0)

    def _lifetime_ok_tx_total(self, card: TelegramCardState) -> int:
        return card.lifetime_ok_tx_base + card.tx_ok_count

    def _lifetime_fail_tx_total(self, card: TelegramCardState) -> int:
        return card.lifetime_fail_tx_base + card.tx_fail_count

    def _current_day_network_fee(self, card: TelegramCardState) -> dict[str, Decimal]:
        session_today_fee = self._subtract_amount_maps(
            card.total_network_fee,
            card.day_session_network_fee_offset,
        )
        session_today_free_fee = self._subtract_amount_maps(
            card.free_network_fee_credit,
            card.day_session_free_network_fee_offset,
        )
        session_today_paid_fee = self._subtract_amount_maps(
            session_today_fee,
            session_today_free_fee,
        )
        return self._merge_amount_maps(card.daily_network_fee_base, session_today_paid_fee)

    def _current_week_network_fee(self, card: TelegramCardState) -> dict[str, Decimal]:
        session_week_fee = self._subtract_amount_maps(
            card.total_network_fee,
            card.week_session_network_fee_offset,
        )
        session_week_free_fee = self._subtract_amount_maps(
            card.free_network_fee_credit,
            card.week_session_free_network_fee_offset,
        )
        session_week_paid_fee = self._subtract_amount_maps(
            session_week_fee,
            session_week_free_fee,
        )
        return self._merge_amount_maps(card.weekly_network_fee_base, session_week_paid_fee)

    def _utc_today(self) -> date:
        return datetime.now(timezone.utc).date()

    def _utc_week_key(self, day_value: date | None = None) -> str:
        target_day = day_value or self._utc_today()
        iso_year, iso_week, _ = target_day.isocalendar()
        return f"{iso_year}-W{iso_week:02d}"

    def _load_state(self) -> None:
        if self._state_loaded:
            return
        self._state_loaded = True
        if not self._state_file.exists():
            return
        try:
            raw = json.loads(self._state_file.read_text(encoding="utf-8"))
        except Exception as exc:
            self.log.warning("Gagal membaca state Telegram %s: %s", self._state_file, exc)
            return
        accounts = raw.get("accounts", {})
        for account_name, payload in accounts.items():
            current_utc_date = str(payload.get("current_utc_date", ""))
            current_utc_week = str(payload.get("current_utc_week", ""))
            daily_network_fee = self._deserialize_amount_map(payload.get("daily_network_fee"))
            daily_swap_fee = self._deserialize_amount_map(payload.get("daily_swap_fee"))
            weekly_network_fee = self._deserialize_amount_map(payload.get("weekly_network_fee"))
            weekly_swap_fee = self._deserialize_amount_map(payload.get("weekly_swap_fee"))
            if not current_utc_week and current_utc_date:
                try:
                    current_utc_week = self._utc_week_key(date.fromisoformat(current_utc_date))
                except ValueError:
                    current_utc_week = ""
            if not weekly_network_fee and current_utc_date:
                try:
                    if self._utc_week_key(date.fromisoformat(current_utc_date)) == self._utc_week_key():
                        weekly_network_fee = dict(daily_network_fee)
                except ValueError:
                    pass
            if not weekly_swap_fee and current_utc_date:
                try:
                    if self._utc_week_key(date.fromisoformat(current_utc_date)) == self._utc_week_key():
                        weekly_swap_fee = dict(daily_swap_fee)
                except ValueError:
                    pass
            daily_ok_tx = max(int(payload.get("daily_ok_tx", 0)), 0)
            lifetime_ok_tx = max(int(payload.get("lifetime_ok_tx", 0)), 0)
            daily_network_fee = self._sanitize_loaded_network_fee(
                daily_network_fee,
                ok_tx_count=daily_ok_tx,
            )
            weekly_network_fee = self._sanitize_loaded_network_fee(
                weekly_network_fee,
                ok_tx_count=max(daily_ok_tx, lifetime_ok_tx),
            )
            lifetime_network_fee = self._sanitize_loaded_network_fee(
                self._deserialize_amount_map(payload.get("lifetime_network_fee")),
                ok_tx_count=lifetime_ok_tx,
            )
            self._account_totals[account_name] = TelegramAccountTotals(
                current_utc_date=current_utc_date,
                current_utc_week=current_utc_week,
                day_index=max(int(payload.get("day_index", 1)), 1),
                daily_swaps=max(int(payload.get("daily_swaps", 0)), 0),
                daily_ok_tx=daily_ok_tx,
                daily_fail_tx=max(int(payload.get("daily_fail_tx", 0)), 0),
                daily_network_fee=daily_network_fee,
                daily_swap_fee=daily_swap_fee,
                weekly_network_fee=weekly_network_fee,
                weekly_swap_fee=weekly_swap_fee,
                lifetime_swaps=max(int(payload.get("lifetime_swaps", 0)), 0),
                lifetime_ok_tx=lifetime_ok_tx,
                lifetime_fail_tx=max(int(payload.get("lifetime_fail_tx", 0)), 0),
                lifetime_network_fee=lifetime_network_fee,
                lifetime_swap_fee=self._deserialize_amount_map(payload.get("lifetime_swap_fee")),
            )
        self._normalize_all_accounts()

    def _normalize_all_accounts(self) -> None:
        for account_name in list(self._account_totals):
            self._account_totals[account_name] = self._normalized_totals(
                self._account_totals[account_name]
            )
        self._save_state()

    def _get_account_totals(self, account_name: str) -> TelegramAccountTotals:
        totals = self._account_totals.get(account_name, TelegramAccountTotals())
        normalized = self._normalized_totals(totals)
        self._account_totals[account_name] = normalized
        return normalized

    def _normalized_totals(self, totals: TelegramAccountTotals) -> TelegramAccountTotals:
        today = self._utc_today()
        today_str = today.isoformat()
        today_week = self._utc_week_key(today)
        if not totals.current_utc_date:
            totals.current_utc_date = today_str
            totals.current_utc_week = today_week
            totals.day_index = max(totals.day_index, 1)
            return totals
        try:
            recorded_date = date.fromisoformat(totals.current_utc_date)
        except ValueError:
            totals.current_utc_date = today_str
            totals.current_utc_week = today_week
            totals.day_index = max(totals.day_index, 1)
            totals.daily_swaps = 0
            totals.daily_ok_tx = 0
            totals.daily_fail_tx = 0
            totals.daily_network_fee = {}
            totals.daily_swap_fee = {}
            totals.weekly_network_fee = {}
            totals.weekly_swap_fee = {}
            return totals
        day_gap = (today - recorded_date).days
        if day_gap > 0:
            totals.current_utc_date = today_str
            totals.day_index = max(totals.day_index + day_gap, 1)
            totals.daily_swaps = 0
            totals.daily_ok_tx = 0
            totals.daily_fail_tx = 0
            totals.daily_network_fee = {}
            totals.daily_swap_fee = {}
        elif day_gap < 0:
            totals.current_utc_date = today_str
        if totals.current_utc_week != today_week:
            totals.current_utc_week = today_week
            totals.weekly_network_fee = {}
            totals.weekly_swap_fee = {}
        return totals

    def _sanitize_loaded_network_fee(
        self,
        values: dict[str, Decimal],
        *,
        ok_tx_count: int,
    ) -> dict[str, Decimal]:
        cc_fee = values.get("CC")
        if cc_fee is None:
            return values
        per_tx_limit = Decimal("2")
        fee_cap = self.runtime.max_network_fee_cc_per_execution
        if fee_cap is not None:
            per_tx_limit = max(per_tx_limit, fee_cap * Decimal("5"))
        plausible_limit = per_tx_limit * Decimal(max(ok_tx_count, 1))
        if cc_fee <= plausible_limit:
            return values
        cleaned = dict(values)
        cleaned.pop("CC", None)
        return cleaned

    def _rollover_card_if_needed(self, card: TelegramCardState) -> None:
        today_str = self._utc_today().isoformat()
        today_week = self._utc_week_key()
        if not card.current_utc_date:
            card.current_utc_date = today_str
            card.current_utc_week = today_week
            self._persist_card_state(card)
            return
        changed = False
        if card.current_utc_date != today_str:
            try:
                recorded_date = date.fromisoformat(card.current_utc_date)
            except ValueError:
                recorded_date = self._utc_today()
            day_gap = max((self._utc_today() - recorded_date).days, 0)
            card.current_utc_date = today_str
            card.day_index = max(card.day_index + max(day_gap, 1), 1)
            card.daily_swap_base = 0
            card.daily_ok_tx_base = 0
            card.daily_fail_tx_base = 0
            card.daily_network_fee_base = {}
            card.daily_swap_fee_base = {}
            card.day_session_swap_offset = card.swap_transactions
            card.day_session_ok_tx_offset = card.tx_ok_count
            card.day_session_fail_tx_offset = card.tx_fail_count
            card.day_session_network_fee_offset = dict(card.total_network_fee)
            card.day_session_free_network_fee_offset = dict(card.free_network_fee_credit)
            card.day_session_swap_fee_offset = dict(card.total_swap_fee)
            changed = True
        if card.current_utc_week != today_week:
            card.current_utc_week = today_week
            card.weekly_network_fee_base = {}
            card.weekly_swap_fee_base = {}
            card.week_session_network_fee_offset = dict(card.total_network_fee)
            card.week_session_free_network_fee_offset = dict(card.free_network_fee_credit)
            card.week_session_swap_fee_offset = dict(card.total_swap_fee)
            changed = True
        if changed:
            self._persist_card_state(card)

    def _persist_card_state(self, card: TelegramCardState) -> None:
        totals = self._get_account_totals(card.account_name)
        totals.current_utc_date = card.current_utc_date or self._utc_today().isoformat()
        totals.current_utc_week = card.current_utc_week or self._utc_week_key()
        totals.day_index = max(card.day_index, 1)
        totals.daily_swaps = self._current_day_swap_total(card)
        totals.daily_ok_tx = self._current_day_ok_tx_total(card)
        totals.daily_fail_tx = self._current_day_fail_tx_total(card)
        totals.daily_network_fee = self._current_day_network_fee(card)
        totals.daily_swap_fee = self._current_day_swap_fee(card)
        totals.weekly_network_fee = self._current_week_network_fee(card)
        totals.weekly_swap_fee = self._current_week_swap_fee(card)
        totals.lifetime_swaps = card.lifetime_swap_base + card.swap_transactions
        totals.lifetime_ok_tx = self._lifetime_ok_tx_total(card)
        totals.lifetime_fail_tx = self._lifetime_fail_tx_total(card)
        totals.lifetime_network_fee = self._current_lifetime_network_fee(card)
        totals.lifetime_swap_fee = self._current_lifetime_swap_fee(card)
        self._account_totals[card.account_name] = totals
        self._save_state()

    def _save_state(self) -> None:
        payload = {
            "version": 3,
            "accounts": {
                account_name: {
                    "current_utc_date": totals.current_utc_date,
                    "current_utc_week": totals.current_utc_week,
                    "day_index": totals.day_index,
                    "daily_swaps": totals.daily_swaps,
                    "daily_ok_tx": totals.daily_ok_tx,
                    "daily_fail_tx": totals.daily_fail_tx,
                    "daily_network_fee": self._serialize_amount_map(totals.daily_network_fee),
                    "daily_swap_fee": self._serialize_amount_map(totals.daily_swap_fee),
                    "weekly_network_fee": self._serialize_amount_map(totals.weekly_network_fee),
                    "weekly_swap_fee": self._serialize_amount_map(totals.weekly_swap_fee),
                    "lifetime_swaps": totals.lifetime_swaps,
                    "lifetime_ok_tx": totals.lifetime_ok_tx,
                    "lifetime_fail_tx": totals.lifetime_fail_tx,
                    "lifetime_network_fee": self._serialize_amount_map(totals.lifetime_network_fee),
                    "lifetime_swap_fee": self._serialize_amount_map(totals.lifetime_swap_fee),
                }
                for account_name, totals in sorted(self._account_totals.items())
            },
        }
        try:
            self._state_file.parent.mkdir(parents=True, exist_ok=True)
            temp_path = Path(f"{self._state_file}.tmp")
            temp_path.write_text(
                json.dumps(payload, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            temp_path.replace(self._state_file)
        except Exception as exc:
            self.log.warning("Gagal menyimpan state Telegram %s: %s", self._state_file, exc)

    def _serialize_amount_map(self, values: dict[str, Decimal]) -> dict[str, str]:
        return {
            symbol: str(amount)
            for symbol, amount in sorted(values.items())
            if amount > Decimal("0")
        }

    def _deserialize_amount_map(self, values: object) -> dict[str, Decimal]:
        if not isinstance(values, dict):
            return {}
        parsed: dict[str, Decimal] = {}
        for symbol, amount in values.items():
            try:
                parsed[str(symbol)] = Decimal(str(amount))
            except InvalidOperation:
                continue
        return {
            symbol: amount
            for symbol, amount in parsed.items()
            if amount > Decimal("0")
        }
