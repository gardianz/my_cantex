from __future__ import annotations

import asyncio
import json
import logging
import random
import sys
import time
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from cantex_sdk import (
    AccountInfo,
    CantexAPIError,
    CantexAuthError,
    CantexTimeoutError,
    InstrumentId,
    IntentTradingKeySigner,
    OperatorKeySigner,
)

from .config import AccountConfig, BotConfig, PreparedAccountRun
from .constants import CC_SYMBOL, MIN_TICKET_SIZE_CC, TRACKED_SYMBOLS, dust_for_symbol
from .models import ActivitySummary, AccountResult, PlanIssue, RouteHop, RoutePlan
from .routing import RouteOptimizer
from .runtime_state import BotRuntimeStateStore, DailyFreeFeeStatus
from .sdk_ext import ExtendedCantexSDK
from .telegram_monitor import TelegramCardState, TelegramMonitor


class AccountLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return f"[{self.extra['account']}] {msg}", kwargs


class StopRequested(Exception):
    pass


@dataclass(frozen=True)
class ScheduledRound:
    round_index: int
    execute_at_utc: datetime


@dataclass
class StrategyRuntimeState:
    primary_index: int = 0
    recycle_index: int = 0
    recovery_index: int = 0
    reserve_recovery_active: bool = False
    consecutive_balance_blocked_rounds: int = 0


@dataclass(frozen=True)
class StrategyAction:
    sell_symbol: str
    buy_symbol: str
    amount_mode: str
    pointer_group: str | None = None
    pointer_next_index: int | None = None
    fraction: Decimal | None = None
    cc_reserve_override: Decimal | None = None


@dataclass(frozen=True)
class RoundExecutionResult:
    completed: bool
    tx_count: int
    stop_reason: str | None = None
    skipped: bool = False


MAX_CONSECUTIVE_BALANCE_BLOCKED_ROUNDS = 5


class AutoswapBot:
    def __init__(self, config: BotConfig, *, repo_root: Path, startup_mode: str) -> None:
        self.config = config
        self.repo_root = repo_root
        self.startup_mode = startup_mode
        self.log = logging.getLogger("autoswap_bot")
        self._prompt_lock = asyncio.Lock()
        self._free_fee_swap_lock = asyncio.Lock()
        self._rng = random.Random(self.config.runtime.random_seed)
        self.monitor = TelegramMonitor(self.config.runtime)
        self.runtime_state = BotRuntimeStateStore(
            self.config.runtime.bot_state_file,
            logging.getLogger("autoswap_bot.state"),
        )
        self._stop_requested = asyncio.Event()

    async def request_stop(self) -> None:
        self._stop_requested.set()

    def stop_requested(self) -> bool:
        return self._stop_requested.is_set()

    async def run(self) -> list[AccountResult]:
        await self.monitor.start()
        try:
            return await asyncio.gather(
                *(self._run_account(account) for account in self.config.accounts)
            )
        finally:
            await self.monitor.close()

    async def _run_account(self, account: AccountConfig) -> AccountResult:
        logger = AccountLoggerAdapter(self.log, {"account": account.name})
        session_number = 0
        last_result: AccountResult | None = None

        while True:
            self._raise_if_stop_requested()
            session_number += 1
            last_result = await self._run_account_session(
                account=account,
                logger=logger,
                session_number=session_number,
            )
            if (
                not self.config.runtime.full_24h_mode
                or not self.config.runtime.full_24h_auto_restart
                or last_result.error is not None
                or last_result.aborted
                or last_result.stop_reason is not None
                or self.config.runtime.dry_run
            ):
                return last_result

            logger.info(
                "Sesi 24 jam berikutnya akan dimulai ulang otomatis | sesi sebelumnya=%s selesai",
                session_number,
            )

    async def _run_account_session(
        self,
        *,
        account: AccountConfig,
        logger: AccountLoggerAdapter,
        session_number: int,
    ) -> AccountResult:
        prepared_run = account.prepare_run(self._rng)
        result = AccountResult(
            account_name=account.name,
            strategy_label=account.strategy().label,
            requested_rounds=prepared_run.rounds,
        )
        monitor_card = self.monitor.create_card(
            account,
            prepared_run,
            account.strategy().label,
        )
        self.runtime_state.ensure_account(account.name)
        sdk = self._build_sdk(account)

        try:
            async with sdk:
                await self.monitor.attach_card(monitor_card)
                logger.info("Autentikasi dimulai | sesi=%s", session_number)
                await self.monitor.log_event(
                    monitor_card,
                    f"🚀 Session {session_number} started",
                    force=True,
                )
                await sdk.authenticate(force=True)
                logger.info("Autentikasi sukses | sesi=%s", session_number)
                await self._sync_daily_free_fee_state_from_history(
                    sdk=sdk,
                    account_name=account.name,
                    logger=logger,
                    monitor_card=monitor_card,
                    force_log=True,
                )

                if account.auto_create_intent_account:
                    created = await sdk.ensure_intent_trading_account()
                    if created:
                        logger.info("Intent trading account berhasil dibuat")
                        await self.monitor.log_event(
                            monitor_card,
                            "🧩 Intent account created",
                        )

                intent_mismatch = await sdk.detect_intent_signer_mismatch()
                if intent_mismatch:
                    raise RuntimeError(intent_mismatch)

                admin = await sdk.get_account_admin()
                info = await sdk.get_account_info()
                instruments_by_symbol = self._resolve_instruments(admin.instruments, info)
                router = RouteOptimizer(
                    sdk,
                    instruments_by_symbol,
                    route_mode=self.config.runtime.route_mode,
                )

                logger.info(
                    "Strategi=%s | putaran=%s | nominal-range=%s | delay-range=%s | startup-mode=%s | seed=%s",
                    account.strategy().label,
                    prepared_run.rounds,
                    self._format_text_map(account.describe_amount_ranges()),
                    self.config.runtime.swap_delay_seconds_range.describe(),
                    self._startup_mode_label(),
                    self.config.runtime.random_seed if self.config.runtime.random_seed is not None else "-",
                )
                self._log_balances(logger, info, "Balance awal")
                await self.monitor.update_balances(
                    monitor_card,
                    self._balances_by_symbol(info),
                    force=True,
                )
                baseline_activity = await self._fetch_activity_summary(sdk, logger)
                result.activity_summary = baseline_activity
                await self.monitor.update_activity(
                    monitor_card,
                    baseline_activity,
                    force=True,
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"🗓️ Ready for {prepared_run.rounds} swap rounds",
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"🧭 Startup mode: {self._startup_mode_label()}",
                )

                if self.config.runtime.dry_run:
                    logger.info("Dry-run aktif, tidak ada swap yang dieksekusi")
                    result.final_balances = self._balances_by_symbol(info)
                    result.activity_summary = await self._fetch_activity_summary(sdk, logger)
                    await self.monitor.log_event(
                        monitor_card,
                        "🧪 Dry run only",
                        force=True,
                    )
                    await self.monitor.finalize(monitor_card, phase="DRY-RUN")
                    return result

                used_network_fee: defaultdict[str, Decimal] = defaultdict(Decimal)
                used_swap_fee: defaultdict[str, Decimal] = defaultdict(Decimal)
                strategy_state = StrategyRuntimeState()
                await self.monitor.log_event(
                    monitor_card,
                    (
                        "🗓️ 24h startup mode: planned"
                        if self._startup_mode_is_planned()
                        else "⚡ 24h startup mode: direct"
                    ),
                    force=True,
                )
                if self._startup_mode_is_planned():
                    while result.completed_rounds < prepared_run.rounds:
                        session_start_utc = datetime.now(timezone.utc)
                        session_end_utc = self._next_utc_midnight(session_start_utc)
                        remaining_rounds = prepared_run.rounds - result.completed_rounds
                        execution_buffer_seconds = self._estimate_24h_execution_buffer_seconds(remaining_rounds)
                        schedule = self._build_24h_schedule(
                            rounds=remaining_rounds,
                            start_utc=session_start_utc,
                            end_utc=session_end_utc,
                            execution_buffer_seconds=execution_buffer_seconds,
                        )
                        self._log_24h_schedule(
                            logger,
                            remaining_rounds,
                            session_start_utc,
                            session_end_utc,
                            schedule,
                            execution_buffer_seconds,
                            start_round_number=result.completed_rounds + 1,
                        )
                        await self._run_24h_session(
                            sdk=sdk,
                            router=router,
                            account=account,
                            prepared_run=prepared_run,
                            strategy_state=strategy_state,
                            logger=logger,
                            monitor_card=monitor_card,
                            used_network_fee=used_network_fee,
                            used_swap_fee=used_swap_fee,
                            result=result,
                            session_end_utc=session_end_utc,
                            schedule=schedule,
                        )
                        if result.stop_reason:
                            break
                        if result.completed_rounds < prepared_run.rounds:
                            logger.info(
                                "Rounds tersisa %s, lanjut ke sesi UTC berikutnya",
                                prepared_run.rounds - result.completed_rounds,
                            )
                            await self.monitor.log_event(
                                monitor_card,
                                f"⏭️ {prepared_run.rounds - result.completed_rounds} rounds remaining, continue next UTC session",
                                force=True,
                            )
                else:
                    await self._run_24h_direct_session(
                        sdk=sdk,
                        router=router,
                        account=account,
                        prepared_run=prepared_run,
                        strategy_state=strategy_state,
                        logger=logger,
                        monitor_card=monitor_card,
                        used_network_fee=used_network_fee,
                        used_swap_fee=used_swap_fee,
                        result=result,
                    )

                final_info = await sdk.get_account_info()
                result.final_balances = self._balances_by_symbol(final_info)
                result.used_network_fee_by_symbol = dict(used_network_fee)
                result.used_swap_fee_by_symbol = dict(used_swap_fee)
                result.activity_summary = await self._fetch_activity_summary(sdk, logger)
                self._log_balances(logger, final_info, "Balance akhir")
                await self.monitor.update_balances(
                    monitor_card,
                    result.final_balances,
                    force=True,
                )
                await self.monitor.update_activity(
                    monitor_card,
                    result.activity_summary,
                    force=True,
                )
                if result.error is None and not result.aborted:
                    if result.stop_reason:
                        await self.monitor.log_event(
                            monitor_card,
                            f"⛔ Session stopped: {self._message_for_stop_reason(result.stop_reason)}",
                            force=True,
                        )
                        await self.monitor.finalize(
                            monitor_card,
                            phase=f"STOPPED_{result.stop_reason}",
                        )
                    else:
                        await self.monitor.log_event(
                            monitor_card,
                            "🏁 Session completed",
                            force=True,
                        )
                        await self.monitor.finalize(monitor_card, phase="FINISHED")
        except StopRequested:
            result.aborted = True
            result.stop_reason = "MANUAL_STOP"
            result.error = "Dihentikan user"
            logger.info("Eksekusi dihentikan user")
            await self.monitor.log_event(
                monitor_card,
                "⛔ Stopped by user",
                force=True,
            )
            await self.monitor.finalize(monitor_card, phase="STOPPED_MANUAL")
        except (CantexAuthError, CantexAPIError, CantexTimeoutError) as exc:
            result.error = str(exc)
            logger.error("Eksekusi gagal: %s", exc)
            await self.monitor.log_event(
                monitor_card,
                f"❌ Error: {exc}",
                force=True,
            )
            await self.monitor.finalize(monitor_card, phase="FAILED")
        except Exception as exc:  # pragma: no cover - runtime guard
            result.error = str(exc)
            logger.exception("Error tak terduga: %s", exc)
            await self.monitor.log_event(
                monitor_card,
                f"❌ Unexpected: {exc}",
                force=True,
            )
            await self.monitor.finalize(monitor_card, phase="FAILED")

        return result

    def _build_sdk(self, account: AccountConfig) -> ExtendedCantexSDK:
        api_key_dir = self.repo_root / ".secrets" / "api_keys"
        api_key_dir.mkdir(parents=True, exist_ok=True)
        return ExtendedCantexSDK(
            OperatorKeySigner.from_hex(account.operator_key),
            IntentTradingKeySigner.from_hex(account.trading_key),
            base_url=self.config.runtime.base_url,
            api_key_path=str(api_key_dir / f"{account.key_slug}.txt"),
            max_retries=self.config.runtime.max_retries,
            retry_base_delay=self.config.runtime.retry_base_delay,
        )

    def _resolve_instruments(self, admin_instruments, info: AccountInfo) -> dict[str, InstrumentId]:
        resolved: dict[str, InstrumentId] = {}
        for instrument in admin_instruments:
            if instrument.instrument_symbol in TRACKED_SYMBOLS:
                resolved[instrument.instrument_symbol] = instrument.instrument
        for token in info.tokens:
            if token.instrument_symbol in TRACKED_SYMBOLS:
                resolved[token.instrument_symbol] = token.instrument

        missing = [symbol for symbol in TRACKED_SYMBOLS if symbol not in resolved]
        if missing:
            raise RuntimeError(f"Instrument tidak ditemukan untuk simbol: {', '.join(missing)}")
        return resolved

    def _build_round_robin_candidates(
        self,
        actions: tuple[tuple[str, str, str, Decimal | None], ...],
        *,
        start_index: int,
        pointer_group: str,
    ) -> list[StrategyAction]:
        candidates: list[StrategyAction] = []
        action_count = len(actions)
        for offset in range(action_count):
            current_index = (start_index + offset) % action_count
            sell_symbol, buy_symbol, amount_mode, fraction = actions[current_index]
            candidates.append(
                StrategyAction(
                    sell_symbol=sell_symbol,
                    buy_symbol=buy_symbol,
                    amount_mode=amount_mode,
                    pointer_group=pointer_group,
                    pointer_next_index=(current_index + 1) % action_count,
                    fraction=fraction,
                )
            )
        return candidates

    def _strategy_action_candidates(
        self,
        *,
        account: AccountConfig,
        balances: dict[str, Decimal],
        strategy_state: StrategyRuntimeState,
    ) -> tuple[list[StrategyAction], str]:
        strategy_key = account.strategy().key
        if strategy_key == "strategy_1":
            return self._strategy_1_action_candidates(account=account, balances=balances)
        if strategy_key == "strategy_2":
            return self._strategy_2_action_candidates(account=account, balances=balances)
        if strategy_key == "strategy_3_cycle":
            return self._strategy_3_action_candidates(
                account=account,
                balances=balances,
                strategy_state=strategy_state,
            )
        if strategy_key == "strategy_4_reserve":
            return self._strategy_4_action_candidates(
                account=account,
                balances=balances,
                strategy_state=strategy_state,
            )
        raise ValueError(f"Strategi {strategy_key} tidak lagi didukung")

    def _strategy_1_action_candidates(
        self,
        *,
        account: AccountConfig,
        balances: dict[str, Decimal],
    ) -> tuple[list[StrategyAction], str]:
        candidates: list[StrategyAction] = []
        reserve_fee = self._effective_cc_reserve(account)
        foreign_balance = self._spendable_amount(
            "CBTC",
            balances.get("CBTC", Decimal("0")),
            reserve_fee,
        )
        if foreign_balance > dust_for_symbol("CBTC"):
            candidates.append(StrategyAction(sell_symbol="CBTC", buy_symbol="CC", amount_mode="max"))

        cc_amount_range = account.amount_range_for_symbol(CC_SYMBOL)
        spendable_cc = self._spendable_amount(
            CC_SYMBOL,
            balances.get(CC_SYMBOL, Decimal("0")),
            reserve_fee,
        )
        if spendable_cc >= cc_amount_range.min_value:
            candidates.append(StrategyAction(sell_symbol="CC", buy_symbol="USDCx", amount_mode="config"))

        strategy_balance = self._spendable_amount(
            "USDCx",
            balances.get("USDCx", Decimal("0")),
            reserve_fee,
        )
        if strategy_balance > dust_for_symbol("USDCx"):
            candidates.append(StrategyAction(sell_symbol="USDCx", buy_symbol="CC", amount_mode="max"))

        if candidates:
            return candidates, "strategy_1 ready"
        return [], self._cc_source_block_reason(
            balance_cc=balances.get(CC_SYMBOL, Decimal("0")),
            spendable_cc=spendable_cc,
            required_min_amount=cc_amount_range.min_value,
            reserve_threshold=reserve_fee,
        )

    def _strategy_2_action_candidates(
        self,
        *,
        account: AccountConfig,
        balances: dict[str, Decimal],
    ) -> tuple[list[StrategyAction], str]:
        candidates: list[StrategyAction] = []
        reserve_fee = self._effective_cc_reserve(account)
        foreign_balance = self._spendable_amount(
            "USDCx",
            balances.get("USDCx", Decimal("0")),
            reserve_fee,
        )
        if foreign_balance > dust_for_symbol("USDCx"):
            candidates.append(StrategyAction(sell_symbol="USDCx", buy_symbol="CC", amount_mode="max"))

        cc_amount_range = account.amount_range_for_symbol(CC_SYMBOL)
        spendable_cc = self._spendable_amount(
            CC_SYMBOL,
            balances.get(CC_SYMBOL, Decimal("0")),
            reserve_fee,
        )
        if spendable_cc >= cc_amount_range.min_value:
            candidates.append(StrategyAction(sell_symbol="CC", buy_symbol="CBTC", amount_mode="config"))

        strategy_balance = self._spendable_amount(
            "CBTC",
            balances.get("CBTC", Decimal("0")),
            reserve_fee,
        )
        if strategy_balance > dust_for_symbol("CBTC"):
            candidates.append(StrategyAction(sell_symbol="CBTC", buy_symbol="CC", amount_mode="max"))

        if candidates:
            return candidates, "strategy_2 ready"
        return [], self._cc_source_block_reason(
            balance_cc=balances.get(CC_SYMBOL, Decimal("0")),
            spendable_cc=spendable_cc,
            required_min_amount=cc_amount_range.min_value,
            reserve_threshold=reserve_fee,
        )

    def _strategy_3_action_candidates(
        self,
        *,
        account: AccountConfig,
        balances: dict[str, Decimal],
        strategy_state: StrategyRuntimeState,
    ) -> tuple[list[StrategyAction], str]:
        reserve_fee = self._effective_cc_reserve(account)
        cc_amount_range = account.amount_range_for_symbol(CC_SYMBOL)
        spendable_cc = self._spendable_amount(
            CC_SYMBOL,
            balances.get(CC_SYMBOL, Decimal("0")),
            reserve_fee,
        )
        if spendable_cc >= cc_amount_range.min_value:
            return self._build_round_robin_candidates(
                (
                    ("CC", "USDCx", "config", None),
                    ("CC", "CBTC", "config", None),
                ),
                start_index=strategy_state.primary_index,
                pointer_group="primary",
            ), "strategy_3 spend phase"

        recycle_candidates: list[StrategyAction] = []
        recycle_actions = self._build_round_robin_candidates(
            (
                ("USDCx", "CBTC", "fraction", Decimal("0.5")),
                ("CBTC", "USDCx", "fraction", Decimal("0.5")),
                ("CBTC", "CC", "max", None),
                ("USDCx", "CC", "max", None),
            ),
            start_index=strategy_state.recycle_index,
            pointer_group="recycle",
        )
        for action in recycle_actions:
            spendable_source = self._spendable_amount(
                action.sell_symbol,
                balances.get(action.sell_symbol, Decimal("0")),
                reserve_fee,
            )
            if spendable_source <= dust_for_symbol(action.sell_symbol):
                continue
            recycle_candidates.append(action)
        if recycle_candidates:
            return recycle_candidates, "strategy_3 recycle phase"
        return [], self._cc_source_block_reason(
            balance_cc=balances.get(CC_SYMBOL, Decimal("0")),
            spendable_cc=spendable_cc,
            required_min_amount=cc_amount_range.min_value,
            reserve_threshold=reserve_fee,
        )

    def _strategy_4_action_candidates(
        self,
        *,
        account: AccountConfig,
        balances: dict[str, Decimal],
        strategy_state: StrategyRuntimeState,
    ) -> tuple[list[StrategyAction], str]:
        reserve_fee = self._strategy_4_reserve_fee(account)
        reserve_kritis = self._strategy_4_reserve_kritis(account)
        cc_balance = balances.get(CC_SYMBOL, Decimal("0"))

        foreign_available = {
            symbol: self._spendable_amount(
                symbol,
                balances.get(symbol, Decimal("0")),
                reserve_fee,
            )
            for symbol in ("USDCx", "CBTC")
        }
        has_foreign_balance = any(
            amount > dust_for_symbol(symbol)
            for symbol, amount in foreign_available.items()
        )

        if strategy_state.reserve_recovery_active and not has_foreign_balance:
            strategy_state.reserve_recovery_active = False

        if has_foreign_balance and (
            strategy_state.reserve_recovery_active or cc_balance <= reserve_kritis
        ):
            strategy_state.reserve_recovery_active = True
            recovery_candidates: list[StrategyAction] = []
            recovery_actions = self._build_round_robin_candidates(
                (
                    ("USDCx", "CC", "max", None),
                    ("CBTC", "CC", "max", None),
                ),
                start_index=strategy_state.recovery_index,
                pointer_group="recovery",
            )
            for action in recovery_actions:
                spendable_source = self._spendable_amount(
                    action.sell_symbol,
                    balances.get(action.sell_symbol, Decimal("0")),
                    reserve_fee,
                )
                if spendable_source <= dust_for_symbol(action.sell_symbol):
                    continue
                recovery_candidates.append(action)
            if recovery_candidates:
                return recovery_candidates, "strategy_4 recovery phase"
            return [], "strategy_4 recovery waiting foreign balance"

        recycle_candidates: list[StrategyAction] = []
        recycle_actions = self._build_round_robin_candidates(
            (
                ("USDCx", "CBTC", "max", None),
                ("CBTC", "USDCx", "max", None),
            ),
            start_index=strategy_state.recycle_index,
            pointer_group="recycle",
        )
        for action in recycle_actions:
            spendable_source = self._spendable_amount(
                action.sell_symbol,
                balances.get(action.sell_symbol, Decimal("0")),
                reserve_fee,
            )
            if spendable_source <= dust_for_symbol(action.sell_symbol):
                continue
            recycle_candidates.append(action)

        spendable_cc = self._spendable_amount(
            CC_SYMBOL,
            cc_balance,
            reserve_fee,
        )
        if spendable_cc > dust_for_symbol(CC_SYMBOL):
            recycle_candidates.append(
                StrategyAction(
                    sell_symbol="CC",
                    buy_symbol="USDCx",
                    amount_mode="config",
                    cc_reserve_override=reserve_fee,
                )
            )
        if recycle_candidates:
            return recycle_candidates, (
                "strategy_4 recycle phase"
                if any(action.sell_symbol != CC_SYMBOL for action in recycle_candidates)
                else "strategy_4 spend phase"
            )

        return [], self._cc_source_block_reason(
            balance_cc=cc_balance,
            spendable_cc=spendable_cc,
            required_min_amount=account.amount_range_for_symbol(CC_SYMBOL).min_value,
            reserve_threshold=reserve_fee,
        )

    async def _resolve_strategy_action_amount(
        self,
        *,
        account: AccountConfig,
        action: StrategyAction,
        balances: dict[str, Decimal],
        router: RouteOptimizer,
    ) -> tuple[Decimal | None, Decimal | None, str | None]:
        cc_reserve = self._effective_cc_reserve(account, action.cc_reserve_override)
        available_amount = self._spendable_amount(
            action.sell_symbol,
            balances.get(action.sell_symbol, Decimal("0")),
            cc_reserve,
        )
        if available_amount <= dust_for_symbol(action.sell_symbol):
            return None, None, f"{action.sell_symbol} balance terlalu kecil"

        if action.amount_mode == "config":
            amount_range = account.amount_range_for_symbol(action.sell_symbol)
            max_allowed_amount = min(available_amount, amount_range.max_value)
            if max_allowed_amount < amount_range.min_value:
                reason = (
                    self._cc_source_block_reason(
                        balance_cc=balances.get(CC_SYMBOL, Decimal("0")),
                        spendable_cc=available_amount,
                        required_min_amount=amount_range.min_value,
                        reserve_threshold=cc_reserve,
                    )
                    if action.sell_symbol == CC_SYMBOL
                    else f"{action.sell_symbol} balance below user config min ({available_amount} < {amount_range.min_value})"
                )
                return None, amount_range.min_value, reason
            target_amount = self._sample_execution_amount(amount_range, max_allowed_amount)
            actual_amount, min_ticket_reason = await self._normalize_amount_for_min_ticket(
                router=router,
                sell_symbol=action.sell_symbol,
                buy_symbol=action.buy_symbol,
                desired_amount=target_amount,
                max_available_amount=max_allowed_amount,
            )
            return actual_amount, amount_range.min_value, min_ticket_reason

        if action.amount_mode == "max":
            actual_amount, min_ticket_reason = await self._normalize_amount_for_min_ticket(
                router=router,
                sell_symbol=action.sell_symbol,
                buy_symbol=action.buy_symbol,
                desired_amount=available_amount,
                max_available_amount=available_amount,
            )
            return actual_amount, None, min_ticket_reason

        if action.amount_mode == "fraction":
            fraction = action.fraction or Decimal("0")
            desired_amount = available_amount * fraction
            if desired_amount <= dust_for_symbol(action.sell_symbol):
                return None, None, f"{action.sell_symbol} balance terlalu kecil untuk mode {fraction}"
            actual_amount, min_ticket_reason = await self._normalize_amount_for_min_ticket(
                router=router,
                sell_symbol=action.sell_symbol,
                buy_symbol=action.buy_symbol,
                desired_amount=desired_amount,
                max_available_amount=available_amount,
            )
            return actual_amount, None, min_ticket_reason

        raise ValueError(f"Mode amount tidak didukung: {action.amount_mode}")

    def _advance_strategy_state_after_success(
        self,
        *,
        strategy_state: StrategyRuntimeState,
        action: StrategyAction,
    ) -> None:
        if action.pointer_group == "primary" and action.pointer_next_index is not None:
            strategy_state.primary_index = action.pointer_next_index
        if action.pointer_group == "recycle" and action.pointer_next_index is not None:
            strategy_state.recycle_index = action.pointer_next_index
        if action.pointer_group == "recovery" and action.pointer_next_index is not None:
            strategy_state.recovery_index = action.pointer_next_index

    def _is_balance_blocking_stop_reason(self, stop_reason: str | None) -> bool:
        return stop_reason in {
            "WAIT_SOURCE_BALANCE",
            "MIN_TICKET_SIZE",
            "USER_CONFIG_MIN_NOT_MET",
        }

    def _reset_balance_block_counter(self, strategy_state: StrategyRuntimeState) -> None:
        strategy_state.consecutive_balance_blocked_rounds = 0

    async def _build_skipped_round_result(
        self,
        *,
        account: AccountConfig,
        round_number: int,
        tx_count: int,
        strategy_state: StrategyRuntimeState,
        stop_reason: str,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
    ) -> RoundExecutionResult:
        if not self._is_balance_blocking_stop_reason(stop_reason):
            self._reset_balance_block_counter(strategy_state)
            return RoundExecutionResult(
                completed=False,
                tx_count=tx_count,
                stop_reason=stop_reason,
                skipped=True,
            )

        strategy_state.consecutive_balance_blocked_rounds += 1
        blocked_rounds = strategy_state.consecutive_balance_blocked_rounds
        if blocked_rounds <= MAX_CONSECUTIVE_BALANCE_BLOCKED_ROUNDS:
            return RoundExecutionResult(
                completed=False,
                tx_count=tx_count,
                stop_reason=stop_reason,
                skipped=True,
            )

        logger.warning(
            "Saldo akun %s tidak lagi cukup untuk lanjut setelah %s round tertahan berturut-turut",
            account.name,
            blocked_rounds,
        )
        await self.monitor.log_event(
            monitor_card,
            (
                f"⛔ Round {round_number} stopped: saldo kurang "
                f"setelah {blocked_rounds} pending berturut-turut"
            ),
            force=True,
        )
        return RoundExecutionResult(
            completed=False,
            tx_count=tx_count,
            stop_reason="INSUFFICIENT_BALANCE",
            skipped=False,
        )

    async def _execute_round(
        self,
        *,
        sdk: ExtendedCantexSDK,
        router: RouteOptimizer,
        account: AccountConfig,
        prepared_run: PreparedAccountRun,
        round_number: int,
        strategy_state: StrategyRuntimeState,
        fee_retry_deadline_utc: datetime | None,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        used_network_fee: defaultdict[str, Decimal],
        used_swap_fee: defaultdict[str, Decimal],
    ) -> RoundExecutionResult:
        return await self._execute_round_dynamic(
            sdk=sdk,
            router=router,
            account=account,
            prepared_run=prepared_run,
            round_number=round_number,
            strategy_state=strategy_state,
            fee_retry_deadline_utc=fee_retry_deadline_utc,
            logger=logger,
            monitor_card=monitor_card,
            used_network_fee=used_network_fee,
            used_swap_fee=used_swap_fee,
        )

    async def _execute_round_dynamic(
        self,
        *,
        sdk: ExtendedCantexSDK,
        router: RouteOptimizer,
        account: AccountConfig,
        prepared_run: PreparedAccountRun,
        round_number: int,
        strategy_state: StrategyRuntimeState,
        fee_retry_deadline_utc: datetime | None,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        used_network_fee: defaultdict[str, Decimal],
        used_swap_fee: defaultdict[str, Decimal],
    ) -> RoundExecutionResult:
        tx_count = 0
        sell_symbol = "-"
        buy_symbol = "-"
        pair_key = "-"
        selected_action: StrategyAction | None = None
        daily_free_fee_status: DailyFreeFeeStatus | None = None
        daily_free_fee_consumed = False
        return await self._execute_round_dynamic_v2(
            sdk=sdk,
            router=router,
            account=account,
            prepared_run=prepared_run,
            round_number=round_number,
            strategy_state=strategy_state,
            fee_retry_deadline_utc=fee_retry_deadline_utc,
            logger=logger,
            monitor_card=monitor_card,
            used_network_fee=used_network_fee,
            used_swap_fee=used_swap_fee,
        )
        try:
            info = await sdk.get_account_info()
            balances = self._balances_by_symbol(info)
            await self.monitor.update_balances(monitor_card, balances)
            amount_range = account.amount_range_for_symbol(sell_symbol)
            available_amount = self._spendable_amount(
                sell_symbol,
                balances.get(sell_symbol, Decimal("0")),
                self._effective_cc_reserve(account),
            )
            max_allowed_amount = min(available_amount, amount_range.max_value)
            if max_allowed_amount < amount_range.min_value or max_allowed_amount <= dust_for_symbol(sell_symbol):
                if sell_symbol == CC_SYMBOL:
                    refill_tx, balances, refill_satisfied = await self._refill_cc_for_source_step(
                        sdk=sdk,
                        router=router,
                        required_amount=amount_range.min_value,
                        cc_reserve=self._effective_cc_reserve(account),
                        logger=logger,
                        monitor_card=monitor_card,
                        used_network_fee=used_network_fee,
                        used_swap_fee=used_swap_fee,
                    )
                    tx_count += refill_tx
                    available_amount = self._spendable_amount(
                        sell_symbol,
                        balances.get(sell_symbol, Decimal("0")),
                        self._effective_cc_reserve(account),
                    )
                    max_allowed_amount = min(available_amount, amount_range.max_value)
                    if not refill_satisfied and max_allowed_amount < amount_range.min_value:
                        reason = self._cc_source_block_reason(
                            balance_cc=balances.get(CC_SYMBOL, Decimal("0")),
                            spendable_cc=available_amount,
                            required_min_amount=amount_range.min_value,
                        )
                        await self.monitor.log_event(
                            monitor_card,
                            f"⏭️ Round {round_number} pending: {reason}",
                            force=True,
                        )
                        return RoundExecutionResult(
                            completed=False,
                            tx_count=tx_count,
                            stop_reason="WAIT_SOURCE_BALANCE",
                            skipped=True,
                        )
                if max_allowed_amount < amount_range.min_value or max_allowed_amount <= dust_for_symbol(sell_symbol):
                    reason = (
                        self._cc_source_block_reason(
                            balance_cc=balances.get(CC_SYMBOL, Decimal("0")),
                            spendable_cc=available_amount,
                            required_min_amount=amount_range.min_value,
                        )
                        if sell_symbol == CC_SYMBOL
                        else f"{sell_symbol} balance below user config min ({available_amount} < {amount_range.min_value})"
                    )
                    logger.info(
                        "Putaran %s belum bisa dieksekusi | %s -> %s | %s",
                        round_number,
                        sell_symbol,
                        buy_symbol,
                        reason,
                    )
                    await self.monitor.update_status(
                        monitor_card,
                        pair_key=self._monitor_pair_key(pair_key),
                        round_number=round_number,
                        phase="PROCESSING",
                    )
                    await self.monitor.log_event(
                        monitor_card,
                        f"⏭️ Round {round_number} pending: {reason}",
                    )
                    return RoundExecutionResult(
                        completed=False,
                        tx_count=tx_count,
                        stop_reason="WAIT_SOURCE_BALANCE",
                        skipped=True,
                    )

            target_amount = self._sample_execution_amount(amount_range, max_allowed_amount)
            actual_amount, min_ticket_reason = await self._normalize_amount_for_min_ticket(
                router=router,
                sell_symbol=sell_symbol,
                buy_symbol=buy_symbol,
                desired_amount=target_amount,
                max_available_amount=max_allowed_amount,
            )
            if actual_amount is None:
                logger.info(
                    "Putaran %s belum valid di protocol | %s -> %s | %s",
                    round_number,
                    sell_symbol,
                    buy_symbol,
                    min_ticket_reason,
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"⏭️ Round {round_number} pending: {min_ticket_reason}",
                    force=True,
                )
                return RoundExecutionResult(
                    completed=False,
                    tx_count=tx_count,
                    stop_reason="MIN_TICKET_SIZE",
                    skipped=True,
                )
            route, issue = await self._prepare_affordable_route(
                router=router,
                balances=balances,
                sell_symbol=sell_symbol,
                buy_symbol=buy_symbol,
                proposed_amount=actual_amount,
                round_number=round_number,
                cc_reserve=self._effective_cc_reserve(account),
            )
            if issue is not None:
                if sell_symbol == CC_SYMBOL and issue.sell_symbol == CC_SYMBOL:
                    refill_tx, balances, refill_satisfied = await self._refill_cc_for_source_step(
                        sdk=sdk,
                        router=router,
                        required_amount=amount_range.min_value,
                        cc_reserve=self._effective_cc_reserve(account),
                        logger=logger,
                        monitor_card=monitor_card,
                        used_network_fee=used_network_fee,
                        used_swap_fee=used_swap_fee,
                    )
                    tx_count += refill_tx
                    if refill_satisfied:
                        route, issue = await self._prepare_affordable_route(
                            router=router,
                            balances=balances,
                            sell_symbol=sell_symbol,
                            buy_symbol=buy_symbol,
                            proposed_amount=min(
                                amount_range.max_value,
                                self._spendable_amount(
                                    sell_symbol,
                                    balances.get(sell_symbol, Decimal("0")),
                                    self._effective_cc_reserve(account),
                                ),
                            ),
                            round_number=round_number,
                            cc_reserve=self._effective_cc_reserve(account),
                        )
                if issue is not None:
                    logger.info(
                        "Putaran %s belum affordable | %s -> %s | %s",
                        round_number,
                        sell_symbol,
                        buy_symbol,
                        issue.reason,
                    )
                    await self.monitor.log_event(
                        monitor_card,
                        f"⏭️ Round {round_number} pending: {issue.reason}",
                        force=True,
                    )
                    return RoundExecutionResult(
                        completed=False,
                        tx_count=tx_count,
                        stop_reason="ROUND_AFFORDABILITY_CHECK_FAILED",
                        skipped=True,
                    )
        except (CantexAPIError, CantexTimeoutError) as exc:
            logger.warning(
                "Putaran %s gagal sementara | %s -> %s | %s",
                round_number,
                sell_symbol,
                buy_symbol,
                exc,
            )
            await self.monitor.log_event(
                monitor_card,
                f"⏭️ Round {round_number} pending: transient API error ({exc})",
                force=True,
            )
            return await self._build_skipped_round_result(
                account=account,
                round_number=round_number,
                tx_count=tx_count,
                strategy_state=strategy_state,
                stop_reason="TRANSIENT_API_ERROR",
                logger=logger,
                monitor_card=monitor_card,
            )
        except RuntimeError as exc:
            if not self._is_retryable_route_error(exc):
                raise
            logger.warning(
                "Putaran %s gagal sementara saat siapkan route | %s -> %s | %s",
                round_number,
                sell_symbol,
                buy_symbol,
                exc,
            )
            await self.monitor.log_event(
                monitor_card,
                f"⏭️ Round {round_number} pending: transient quote error ({exc})",
                force=True,
            )
            return await self._build_skipped_round_result(
                account=account,
                round_number=round_number,
                tx_count=tx_count,
                strategy_state=strategy_state,
                stop_reason="TRANSIENT_ROUTE_ERROR",
                logger=logger,
                monitor_card=monitor_card,
            )

        try:
            route, issue, daily_free_fee_status = await self._wait_for_network_fee_below_cap(
                sdk=sdk,
                router=router,
                balances=balances,
                sell_symbol=sell_symbol,
                buy_symbol=buy_symbol,
                actual_amount=actual_amount,
                round_number=round_number,
                cc_reserve=self._effective_cc_reserve(account, selected_action.cc_reserve_override),
                fee_retry_deadline_utc=fee_retry_deadline_utc,
                logger=logger,
                monitor_card=monitor_card,
                current_route=route,
                account_name=account.name,
            )
        except (CantexAPIError, CantexTimeoutError) as exc:
            logger.warning(
                "Putaran %s gagal sementara saat cek fee | %s -> %s | %s",
                round_number,
                sell_symbol,
                buy_symbol,
                exc,
            )
            await self.monitor.log_event(
                monitor_card,
                f"⏭️ Round {round_number} pending: transient API error ({exc})",
                force=True,
            )
            return await self._build_skipped_round_result(
                account=account,
                round_number=round_number,
                tx_count=tx_count,
                strategy_state=strategy_state,
                stop_reason="TRANSIENT_API_ERROR",
                logger=logger,
                monitor_card=monitor_card,
            )
        except RuntimeError as exc:
            if not self._is_retryable_route_error(exc):
                raise
            logger.warning(
                "Putaran %s gagal sementara saat tunggu fee | %s -> %s | %s",
                round_number,
                sell_symbol,
                buy_symbol,
                exc,
            )
            await self.monitor.log_event(
                monitor_card,
                f"⏭️ Round {round_number} pending: transient quote error ({exc})",
                force=True,
            )
            return await self._build_skipped_round_result(
                account=account,
                round_number=round_number,
                tx_count=tx_count,
                strategy_state=strategy_state,
                stop_reason="TRANSIENT_ROUTE_ERROR",
                logger=logger,
                monitor_card=monitor_card,
            )
        if issue is not None:
            message = (
                f"⏭️ Round {round_number} slot skipped: {issue.reason}"
                if "30 detik sebelum jadwal berikutnya" in issue.reason
                else f"⏭️ Round {round_number} pending: {issue.reason}"
            )
            await self.monitor.log_event(
                monitor_card,
                message,
                force=True,
            )
            return await self._build_skipped_round_result(
                account=account,
                round_number=round_number,
                tx_count=tx_count,
                strategy_state=strategy_state,
                stop_reason="ROUND_AFFORDABILITY_CHECK_FAILED",
                logger=logger,
                monitor_card=monitor_card,
            )
        if route.hops and route.hops[0].sell_amount < amount_range.min_value:
            reason = f"route adjusted amount below user config min ({route.hops[0].sell_amount} < {amount_range.min_value})"
            logger.info(
                "Putaran %s belum bisa dieksekusi | %s -> %s | %s",
                round_number,
                sell_symbol,
                buy_symbol,
                reason,
            )
            await self.monitor.log_event(
                monitor_card,
                f"⏭️ Round {round_number} pending: {reason}",
                force=True,
            )
            return RoundExecutionResult(
                completed=False,
                tx_count=tx_count,
                stop_reason="USER_CONFIG_MIN_NOT_MET",
                skipped=True,
            )

        await self.monitor.update_status(
            monitor_card,
            pair_key=self._monitor_pair_key(pair_key),
            round_number=round_number,
            phase="PROCESSING",
            route_plan=route,
        )
        logger.info(
            "Putaran %s | %s -> %s | nominal=%s | route=%s | fee est=%s | network fee est=%s",
            round_number,
            sell_symbol,
            buy_symbol,
            actual_amount,
            route.label,
            self._format_amount_map(route.total_admin_and_liquidity_by_symbol),
            self._format_amount_map(route.total_network_fee_by_symbol),
        )
        await self.monitor.log_event(
            monitor_card,
            f"🔄 Round {round_number}/{prepared_run.rounds} {self._monitor_pair_key(pair_key)} ({actual_amount})",
        )

        for hop_index, hop in enumerate(route.hops, start=1):
            tx_result, failure_reason = await self._swap_hop_with_retry(
                sdk=sdk,
                hop=hop,
                hop_index=hop_index,
                hop_total=len(route.hops),
                round_number=round_number,
                logger=logger,
                monitor_card=monitor_card,
                free_fee_sequential_account_name=(
                    account.name if daily_free_fee_status is not None and hop_index == 1 else None
                ),
            )
            if tx_result is None:
                await self.monitor.log_event(
                    monitor_card,
                    f"⏭️ Round {round_number} pending: {failure_reason or 'retry limit reached'}",
                    force=True,
                )
                return await self._build_skipped_round_result(
                    account=account,
                    round_number=round_number,
                    tx_count=tx_count,
                    strategy_state=strategy_state,
                    stop_reason=failure_reason or "SWAP_HOP_FAILED_SKIPPED",
                    logger=logger,
                    monitor_card=monitor_card,
                )

            tx_count += 1
            used_network_fee[hop.network_fee_symbol] += hop.network_fee_amount
            used_swap_fee[hop.fee_symbol] += hop.admin_fee_amount + hop.liquidity_fee_amount
            await self.monitor.update_fee_totals(
                monitor_card,
                total_network_fee=dict(used_network_fee),
                total_swap_fee=dict(used_swap_fee),
            )
            tx_identifier = tx_result.get("id") or tx_result.get("transactionId") or tx_result.get("contract_id")
            logger.info(
                "Tx hop %s/%s berhasil | %s -> %s | tx=%s | output est=%s %s",
                hop_index,
                len(route.hops),
                hop.sell_symbol,
                hop.buy_symbol,
                tx_identifier or "-",
                hop.returned_amount,
                hop.buy_symbol,
            )
            await self.monitor.log_event(
                monitor_card,
                f"✅ Hop {hop_index}/{len(route.hops)} {hop.sell_symbol}->{hop.buy_symbol} tx={tx_identifier or '-'}",
            )
            await self.monitor.log_event(
                monitor_card,
                self._format_fee_log_line(
                    prefix="Fee tx",
                    network_fee={hop.network_fee_symbol: hop.network_fee_amount},
                    swap_fee={hop.fee_symbol: hop.admin_fee_amount + hop.liquidity_fee_amount},
                ),
            )
            await self.monitor.log_event(
                monitor_card,
                self._format_fee_log_line(
                    prefix="Fee total",
                    network_fee=dict(used_network_fee),
                    swap_fee=dict(used_swap_fee),
                ),
            )
            await self._sleep_between_swaps()

        latest_info = await sdk.get_account_info()
        latest_balances = self._balances_by_symbol(latest_info)
        await self.monitor.update_balances(
            monitor_card,
            latest_balances,
            force=True,
        )
        await self.monitor.record_round_completed(
            monitor_card,
            pair_key=self._monitor_pair_key(pair_key),
            force=True,
        )
        latest_activity = await self._fetch_activity_summary(sdk, logger)
        await self.monitor.update_activity(
            monitor_card,
            latest_activity,
            force=True,
        )
        await self.monitor.log_event(
            monitor_card,
            "🎉 Swap completed!",
            force=True,
        )
        return RoundExecutionResult(
            completed=True,
            tx_count=tx_count,
        )

    async def _execute_round_dynamic_v2(
        self,
        *,
        sdk: ExtendedCantexSDK,
        router: RouteOptimizer,
        account: AccountConfig,
        prepared_run: PreparedAccountRun,
        round_number: int,
        strategy_state: StrategyRuntimeState,
        fee_retry_deadline_utc: datetime | None,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        used_network_fee: defaultdict[str, Decimal],
        used_swap_fee: defaultdict[str, Decimal],
    ) -> RoundExecutionResult:
        tx_count = 0
        sell_symbol = "-"
        buy_symbol = "-"
        pair_key = "-"
        selected_action: StrategyAction | None = None
        daily_free_fee_status: DailyFreeFeeStatus | None = None
        daily_free_fee_consumed = False

        try:
            info = await sdk.get_account_info()
            balances = self._balances_by_symbol(info)
            await self.monitor.update_balances(monitor_card, balances)
            action_candidates, pending_reason = self._strategy_action_candidates(
                account=account,
                balances=balances,
                strategy_state=strategy_state,
            )
            if not action_candidates:
                await self.monitor.log_event(
                    monitor_card,
                    f"⏭️ Round {round_number} pending: {pending_reason}",
                    force=True,
                )
                return await self._build_skipped_round_result(
                    account=account,
                    round_number=round_number,
                    tx_count=tx_count,
                    strategy_state=strategy_state,
                    stop_reason="WAIT_SOURCE_BALANCE",
                    logger=logger,
                    monitor_card=monitor_card,
                )

            route: RoutePlan | None = None
            actual_amount: Decimal | None = None
            last_invalid_reason = pending_reason
            last_invalid_stop_reason = "WAIT_SOURCE_BALANCE"

            for action in action_candidates:
                sell_symbol = action.sell_symbol
                buy_symbol = action.buy_symbol
                pair_key = f"{sell_symbol}->{buy_symbol}"
                actual_amount, user_min_amount, amount_reason = await self._resolve_strategy_action_amount(
                    account=account,
                    action=action,
                    balances=balances,
                    router=router,
                )
                if actual_amount is None:
                    if amount_reason:
                        last_invalid_reason = amount_reason
                        last_invalid_stop_reason = (
                            "MIN_TICKET_SIZE"
                            if "minimum ticket size" in amount_reason.lower()
                            else "WAIT_SOURCE_BALANCE"
                        )
                        logger.info(
                            "Putaran %s kandidat belum valid | %s -> %s | %s",
                            round_number,
                            sell_symbol,
                            buy_symbol,
                            amount_reason,
                        )
                    continue

                route, issue = await self._prepare_affordable_route(
                    router=router,
                    balances=balances,
                    sell_symbol=sell_symbol,
                    buy_symbol=buy_symbol,
                    proposed_amount=actual_amount,
                    round_number=round_number,
                    cc_reserve=self._effective_cc_reserve(account, action.cc_reserve_override),
                )
                if issue is not None:
                    last_invalid_reason = issue.reason
                    last_invalid_stop_reason = "ROUND_AFFORDABILITY_CHECK_FAILED"
                    logger.info(
                        "Putaran %s kandidat belum affordable | %s -> %s | %s",
                        round_number,
                        sell_symbol,
                        buy_symbol,
                        issue.reason,
                    )
                    continue

                if user_min_amount is not None and route.hops and route.hops[0].sell_amount < user_min_amount:
                    last_invalid_reason = (
                        f"route adjusted amount below user config min ({route.hops[0].sell_amount} < {user_min_amount})"
                    )
                    last_invalid_stop_reason = "USER_CONFIG_MIN_NOT_MET"
                    logger.info(
                        "Putaran %s kandidat belum bisa dieksekusi | %s -> %s | %s",
                        round_number,
                        sell_symbol,
                        buy_symbol,
                        last_invalid_reason,
                    )
                    continue

                selected_action = action
                break

            if selected_action is None or actual_amount is None or route is None:
                await self.monitor.log_event(
                    monitor_card,
                    f"⏭️ Round {round_number} pending: {last_invalid_reason}",
                    force=True,
                )
                return await self._build_skipped_round_result(
                    account=account,
                    round_number=round_number,
                    tx_count=tx_count,
                    strategy_state=strategy_state,
                    stop_reason=last_invalid_stop_reason,
                    logger=logger,
                    monitor_card=monitor_card,
                )
        except (CantexAPIError, CantexTimeoutError) as exc:
            logger.warning(
                "Putaran %s gagal sementara | %s -> %s | %s",
                round_number,
                sell_symbol,
                buy_symbol,
                exc,
            )
            await self.monitor.log_event(
                monitor_card,
                f"⏭️ Round {round_number} pending: transient API error ({exc})",
                force=True,
            )
            return await self._build_skipped_round_result(
                account=account,
                round_number=round_number,
                tx_count=tx_count,
                strategy_state=strategy_state,
                stop_reason="TRANSIENT_API_ERROR",
                logger=logger,
                monitor_card=monitor_card,
            )
        except RuntimeError as exc:
            if not self._is_retryable_route_error(exc):
                raise
            logger.warning(
                "Putaran %s gagal sementara saat siapkan route | %s -> %s | %s",
                round_number,
                sell_symbol,
                buy_symbol,
                exc,
            )
            await self.monitor.log_event(
                monitor_card,
                f"⏭️ Round {round_number} pending: transient quote error ({exc})",
                force=True,
            )
            return await self._build_skipped_round_result(
                account=account,
                round_number=round_number,
                tx_count=tx_count,
                strategy_state=strategy_state,
                stop_reason="TRANSIENT_ROUTE_ERROR",
                logger=logger,
                monitor_card=monitor_card,
            )

        try:
            route, issue, daily_free_fee_status = await self._wait_for_network_fee_below_cap(
                sdk=sdk,
                router=router,
                balances=balances,
                sell_symbol=sell_symbol,
                buy_symbol=buy_symbol,
                actual_amount=actual_amount,
                round_number=round_number,
                cc_reserve=self._effective_cc_reserve(account, selected_action.cc_reserve_override),
                fee_retry_deadline_utc=fee_retry_deadline_utc,
                logger=logger,
                monitor_card=monitor_card,
                current_route=route,
                account_name=account.name,
            )
        except (CantexAPIError, CantexTimeoutError) as exc:
            logger.warning(
                "Putaran %s gagal sementara saat cek fee | %s -> %s | %s",
                round_number,
                sell_symbol,
                buy_symbol,
                exc,
            )
            await self.monitor.log_event(
                monitor_card,
                f"⏭️ Round {round_number} pending: transient API error ({exc})",
                force=True,
            )
            return await self._build_skipped_round_result(
                account=account,
                round_number=round_number,
                tx_count=tx_count,
                strategy_state=strategy_state,
                stop_reason="TRANSIENT_API_ERROR",
                logger=logger,
                monitor_card=monitor_card,
            )
        except RuntimeError as exc:
            if not self._is_retryable_route_error(exc):
                raise
            logger.warning(
                "Putaran %s gagal sementara saat tunggu fee | %s -> %s | %s",
                round_number,
                sell_symbol,
                buy_symbol,
                exc,
            )
            await self.monitor.log_event(
                monitor_card,
                f"⏭️ Round {round_number} pending: transient quote error ({exc})",
                force=True,
            )
            return await self._build_skipped_round_result(
                account=account,
                round_number=round_number,
                tx_count=tx_count,
                strategy_state=strategy_state,
                stop_reason="TRANSIENT_ROUTE_ERROR",
                logger=logger,
                monitor_card=monitor_card,
            )

        if issue is not None:
            message = (
                f"⏭️ Round {round_number} slot skipped: {issue.reason}"
                if "30 detik sebelum jadwal berikutnya" in issue.reason
                else f"⏭️ Round {round_number} pending: {issue.reason}"
            )
            await self.monitor.log_event(
                monitor_card,
                message,
                force=True,
            )
            return await self._build_skipped_round_result(
                account=account,
                round_number=round_number,
                tx_count=tx_count,
                strategy_state=strategy_state,
                stop_reason="ROUND_AFFORDABILITY_CHECK_FAILED",
                logger=logger,
                monitor_card=monitor_card,
            )

        await self.monitor.update_status(
            monitor_card,
            pair_key=self._monitor_pair_key(pair_key),
            round_number=round_number,
            phase="PROCESSING",
            route_plan=route,
        )
        logger.info(
            "Putaran %s | %s -> %s | nominal=%s | route=%s | fee est=%s | network fee est=%s",
            round_number,
            sell_symbol,
            buy_symbol,
            actual_amount,
            route.label,
            self._format_amount_map(route.total_admin_and_liquidity_by_symbol),
            self._format_amount_map(route.total_network_fee_by_symbol),
        )
        await self.monitor.log_event(
            monitor_card,
            f"🔄 Round {round_number}/{prepared_run.rounds} {self._monitor_pair_key(pair_key)} ({actual_amount})",
        )

        for hop_index, hop in enumerate(route.hops, start=1):
            tx_result, failure_reason = await self._swap_hop_with_retry(
                sdk=sdk,
                hop=hop,
                hop_index=hop_index,
                hop_total=len(route.hops),
                round_number=round_number,
                logger=logger,
                monitor_card=monitor_card,
                free_fee_sequential_account_name=(
                    account.name if daily_free_fee_status is not None and hop_index == 1 else None
                ),
            )
            if tx_result is None:
                await self.monitor.log_event(
                    monitor_card,
                    f"⏭️ Round {round_number} pending: {failure_reason or 'retry limit reached'}",
                    force=True,
                )
                return await self._build_skipped_round_result(
                    account=account,
                    round_number=round_number,
                    tx_count=tx_count,
                    strategy_state=strategy_state,
                    stop_reason=failure_reason or "SWAP_HOP_FAILED_SKIPPED",
                    logger=logger,
                    monitor_card=monitor_card,
                )

            tx_count += 1
            if daily_free_fee_status is not None and not daily_free_fee_consumed:
                updated_free_fee_status = self._consume_daily_free_fee_swap(account.name)
                daily_free_fee_consumed = True
                await self.monitor.update_free_fee_status(
                    monitor_card,
                    used=updated_free_fee_status.used,
                    limit=3,
                    network_fee_credit={hop.network_fee_symbol: hop.network_fee_amount},
                    force=True,
                )
                logger.info(
                    "Free fee swap harian terpakai | %s | %s/3 | tanggal UTC=%s",
                    account.name,
                    updated_free_fee_status.used,
                    updated_free_fee_status.utc_date,
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"🎁 Free fee swap used {updated_free_fee_status.used}/3 for {updated_free_fee_status.utc_date} UTC",
                    force=True,
                )
            used_network_fee[hop.network_fee_symbol] += hop.network_fee_amount
            used_swap_fee[hop.fee_symbol] += hop.admin_fee_amount + hop.liquidity_fee_amount
            await self.monitor.update_fee_totals(
                monitor_card,
                total_network_fee=dict(used_network_fee),
                total_swap_fee=dict(used_swap_fee),
            )
            tx_identifier = tx_result.get("id") or tx_result.get("transactionId") or tx_result.get("contract_id")
            logger.info(
                "Tx hop %s/%s berhasil | %s -> %s | tx=%s | output est=%s %s",
                hop_index,
                len(route.hops),
                hop.sell_symbol,
                hop.buy_symbol,
                tx_identifier or "-",
                hop.returned_amount,
                hop.buy_symbol,
            )
            await self.monitor.log_event(
                monitor_card,
                f"✅ Hop {hop_index}/{len(route.hops)} {hop.sell_symbol}->{hop.buy_symbol} tx={tx_identifier or '-'}",
            )
            await self.monitor.log_event(
                monitor_card,
                self._format_fee_log_line(
                    prefix="Fee tx",
                    network_fee={hop.network_fee_symbol: hop.network_fee_amount},
                    swap_fee={hop.fee_symbol: hop.admin_fee_amount + hop.liquidity_fee_amount},
                ),
            )
            await self.monitor.log_event(
                monitor_card,
                self._format_fee_log_line(
                    prefix="Fee total",
                    network_fee=dict(used_network_fee),
                    swap_fee=dict(used_swap_fee),
                ),
            )
            await self._sleep_between_swaps()

        latest_info = await sdk.get_account_info()
        latest_balances = self._balances_by_symbol(latest_info)
        await self.monitor.update_balances(
            monitor_card,
            latest_balances,
            force=True,
        )
        await self.monitor.record_round_completed(
            monitor_card,
            pair_key=self._monitor_pair_key(pair_key),
            force=True,
        )
        latest_activity = await self._fetch_activity_summary(sdk, logger)
        await self.monitor.update_activity(
            monitor_card,
            latest_activity,
            force=True,
        )
        await self.monitor.log_event(
            monitor_card,
            "🎉 Swap completed!",
            force=True,
        )
        self._reset_balance_block_counter(strategy_state)
        self._advance_strategy_state_after_success(
            strategy_state=strategy_state,
            action=selected_action,
        )
        return RoundExecutionResult(
            completed=True,
            tx_count=tx_count,
        )

    async def _recover_until_target_available(
        self,
        *,
        sdk: ExtendedCantexSDK,
        router: RouteOptimizer,
        target_symbol: str,
        required_amount: Decimal,
        cc_reserve: Decimal,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        used_network_fee: defaultdict[str, Decimal],
        used_swap_fee: defaultdict[str, Decimal],
    ) -> tuple[int, dict[str, Decimal], bool]:
        total_tx = 0
        last_balances = self._balances_by_symbol(await sdk.get_account_info())

        while True:
            spendable = self._spendable_amount(
                target_symbol,
                last_balances.get(target_symbol, Decimal("0")),
                cc_reserve,
            )
            if spendable >= required_amount:
                return total_tx, last_balances, True

            recovered_tx = await self._recover_to_symbol(
                sdk=sdk,
                router=router,
                target_symbol=target_symbol,
                cc_reserve=cc_reserve,
                logger=logger,
                monitor_card=monitor_card,
                used_network_fee=used_network_fee,
                used_swap_fee=used_swap_fee,
            )
            total_tx += recovered_tx
            if recovered_tx <= 0:
                return total_tx, last_balances, False

            updated_balances = await self._wait_for_balance_settlement(
                sdk=sdk,
                target_symbol=target_symbol,
                previous_balances=last_balances,
                required_amount=required_amount,
                cc_reserve=cc_reserve,
                logger=logger,
                monitor_card=monitor_card,
            )
            if updated_balances == last_balances:
                return total_tx, updated_balances, False
            last_balances = updated_balances

    async def _wait_for_balance_settlement(
        self,
        *,
        sdk: ExtendedCantexSDK,
        target_symbol: str,
        previous_balances: dict[str, Decimal],
        required_amount: Decimal,
        cc_reserve: Decimal,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
    ) -> dict[str, Decimal]:
        wait_seconds = max(self.config.runtime.retry_base_delay, 2.0)
        max_polls = max(3, self.config.runtime.max_retries * 2)

        for poll_index in range(max_polls):
            self._raise_if_stop_requested()
            info = await sdk.get_account_info()
            balances = self._balances_by_symbol(info)
            previous_amount = previous_balances.get(target_symbol, Decimal("0"))
            current_amount = balances.get(target_symbol, Decimal("0"))
            spendable = self._spendable_amount(
                target_symbol,
                current_amount,
                cc_reserve,
            )
            if current_amount > previous_amount or spendable >= required_amount:
                return balances

            if poll_index < max_polls - 1:
                logger.info(
                    "Menunggu settlement recovery %s | poll %s/%s | balance=%s",
                    target_symbol,
                    poll_index + 1,
                    max_polls,
                    current_amount,
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"⏳ Waiting recovery settlement {target_symbol} ({poll_index + 1}/{max_polls})",
                )
                await self._sleep_or_stop(wait_seconds)

        return previous_balances

    def _cc_source_block_reason(
        self,
        *,
        balance_cc: Decimal,
        spendable_cc: Decimal,
        required_min_amount: Decimal,
        reserve_threshold: Decimal | None = None,
    ) -> str:
        effective_reserve = reserve_threshold if reserve_threshold is not None else Decimal("0")
        if balance_cc <= effective_reserve or spendable_cc <= dust_for_symbol(CC_SYMBOL):
            return f"CC reserve reached ({balance_cc} <= {effective_reserve})"
        return f"CC spendable below user config min ({spendable_cc} < {required_min_amount})"

    async def _refill_cc_for_source_step(
        self,
        *,
        sdk: ExtendedCantexSDK,
        router: RouteOptimizer,
        required_amount: Decimal,
        cc_reserve: Decimal,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        used_network_fee: defaultdict[str, Decimal],
        used_swap_fee: defaultdict[str, Decimal],
    ) -> tuple[int, dict[str, Decimal], bool]:
        logger.info(
            "Source CC belum cukup, mencoba refill CC hingga spendable >= %s",
            required_amount,
        )
        await self.monitor.log_event(
            monitor_card,
            f"🛟 Refill CC target spendable {required_amount}",
        )
        recovered_tx, balances, refill_satisfied = await self._recover_until_target_available(
            sdk=sdk,
            router=router,
            target_symbol=CC_SYMBOL,
            required_amount=required_amount,
            cc_reserve=cc_reserve,
            logger=logger,
            monitor_card=monitor_card,
            used_network_fee=used_network_fee,
            used_swap_fee=used_swap_fee,
        )
        if refill_satisfied:
            await self.monitor.log_event(
                monitor_card,
                "✅ Refill CC ready",
            )
        else:
            await self.monitor.log_event(
                monitor_card,
                "⏭️ Refill CC belum cukup, lanjut ke step berikutnya",
            )
        return recovered_tx, balances, refill_satisfied

    def _sample_execution_amount(
        self,
        amount_range,
        max_allowed_amount: Decimal,
    ) -> Decimal:
        if max_allowed_amount <= amount_range.min_value:
            return max_allowed_amount
        fraction = Decimal(str(self._rng.random()))
        return amount_range.min_value + ((max_allowed_amount - amount_range.min_value) * fraction)

    async def _normalize_amount_for_min_ticket(
        self,
        *,
        router: RouteOptimizer,
        sell_symbol: str,
        buy_symbol: str,
        desired_amount: Decimal,
        max_available_amount: Decimal,
    ) -> tuple[Decimal | None, str | None]:
        if sell_symbol == CC_SYMBOL:
            if desired_amount >= MIN_TICKET_SIZE_CC:
                return desired_amount, None
            if max_available_amount >= MIN_TICKET_SIZE_CC:
                return MIN_TICKET_SIZE_CC, "amount adjusted to minimum 10 CC"
            return None, "amount below minimum ticket size (10 CC equivalent)"

        desired_cc_equivalent = await self._estimate_cc_equivalent(
            router=router,
            sell_symbol=sell_symbol,
            amount=desired_amount,
        )
        if desired_cc_equivalent >= MIN_TICKET_SIZE_CC:
            return desired_amount, None

        available_cc_equivalent = await self._estimate_cc_equivalent(
            router=router,
            sell_symbol=sell_symbol,
            amount=max_available_amount,
        )
        if available_cc_equivalent >= MIN_TICKET_SIZE_CC:
            return max_available_amount, "amount raised to available balance to satisfy minimum ticket"

        return None, "amount below minimum ticket size (10 CC equivalent)"

    def _is_min_ticket_error(self, exc: Exception) -> bool:
        message = str(exc).lower()
        return (
            "minimum ticket size" in message
            or "too small amount" in message
            or "10 cc" in message
        )

    def _is_signature_verification_error(self, exc: Exception) -> bool:
        return "signature verification failed" in str(exc).lower()

    def _is_retryable_route_error(self, exc: Exception) -> bool:
        message = str(exc).lower()
        return (
            "quote gagal" in message
            or "tidak ada route valid" in message
            or "http 500" in message
            or "http 502" in message
            or "http 503" in message
            or "http 504" in message
        )

    def _recovery_source_order(self, target_symbol: str) -> tuple[str, ...]:
        if target_symbol == CC_SYMBOL:
            return tuple(symbol for symbol in TRACKED_SYMBOLS if symbol != target_symbol)
        ordered = [CC_SYMBOL]
        ordered.extend(
            symbol
            for symbol in TRACKED_SYMBOLS
            if symbol not in {target_symbol, CC_SYMBOL}
        )
        return tuple(ordered)

    async def _estimate_cc_equivalent(
        self,
        *,
        router: RouteOptimizer,
        sell_symbol: str,
        amount: Decimal,
    ) -> Decimal:
        if sell_symbol == CC_SYMBOL:
            return amount
        if amount <= 0:
            return Decimal("0")
        try:
            route = await router.choose_best_route(sell_symbol, CC_SYMBOL, amount)
        except Exception:
            return Decimal("0")
        return route.final_amount

    async def _swap_hop_with_retry(
        self,
        *,
        sdk: ExtendedCantexSDK,
        hop: RouteHop,
        hop_index: int,
        hop_total: int,
        round_number: int,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        free_fee_sequential_account_name: str | None = None,
    ) -> tuple[dict[str, Any] | None, str | None]:
        max_attempts = max(1, self.config.runtime.max_retries)
        confirm_timeout = max(float(hop.estimated_time_seconds) * 3.0, 30.0)
        lock_acquired = False
        if free_fee_sequential_account_name is not None:
            lock_acquired = await self._acquire_free_fee_sequence_slot(
                account_name=free_fee_sequential_account_name,
                round_number=round_number,
                logger=logger,
                monitor_card=monitor_card,
            )
        try:
            for attempt in range(1, max_attempts + 1):
                self._raise_if_stop_requested()
                try:
                    event = await sdk.swap_and_confirm(
                        sell_amount=hop.sell_amount,
                        sell_instrument=hop.raw_quote.sell_instrument,
                        buy_instrument=hop.raw_quote.buy_instrument,
                        timeout=confirm_timeout,
                    )
                    await self.monitor.record_tx_success(monitor_card)
                    return (
                        {
                            "id": getattr(event, "event_id", ""),
                            "ledger_created_at": getattr(event, "ledger_created_at", ""),
                            "output_amount": getattr(event, "output_amount", None),
                            "output_instrument": getattr(getattr(event, "output_instrument", None), "id", ""),
                            "raw": getattr(event, "raw", {}),
                        },
                        None,
                    )
                except Exception as exc:
                    if self._is_signature_verification_error(exc):
                        await self.monitor.record_tx_failure(monitor_card)
                        raise RuntimeError(
                            "Signature verification failed untuk swap intent. "
                            "Kemungkinan CANTEX_TRADING_KEY tidak cocok dengan intent account wallet ini."
                        ) from exc
                    if self._is_min_ticket_error(exc):
                        logger.warning(
                            "Swap hop %s/%s round %s gagal karena minimum ticket size: %s",
                            hop_index,
                            hop_total,
                            round_number,
                            exc,
                        )
                        await self.monitor.record_tx_failure(monitor_card)
                        await self.monitor.log_event(
                            monitor_card,
                            f"⏭️ Hop {hop_index}/{hop_total} skipped: minimum ticket size",
                            force=True,
                        )
                        return None, "MIN_TICKET_SIZE"
                    logger.warning(
                        "Swap gagal pada hop %s/%s round %s percobaan %s/%s: %s",
                    hop_index,
                    hop_total,
                    round_number,
                    attempt,
                        max_attempts,
                        exc,
                    )
                    if attempt >= max_attempts:
                        await self.monitor.record_tx_failure(monitor_card)
                        await self.monitor.log_event(
                            monitor_card,
                            f"❌ Hop {hop_index}/{hop_total} failed after {max_attempts} attempts: {exc}",
                            force=True,
                        )
                        return None, "SWAP_RETRY_EXHAUSTED"

                    wait_seconds = max(self.config.runtime.retry_base_delay, 1.0) * attempt
                    await self.monitor.log_event(
                        monitor_card,
                        f"🔁 Retry hop {hop_index}/{hop_total} attempt {attempt + 1}/{max_attempts} in {int(wait_seconds)}s",
                    )
                    await self._sleep_or_stop(wait_seconds)
            return None, "SWAP_RETRY_EXHAUSTED"
        finally:
            await self._release_free_fee_sequence_slot(
                lock_acquired=lock_acquired,
                logger=logger,
                monitor_card=monitor_card,
                apply_delay=free_fee_sequential_account_name is not None,
            )

    async def _wait_for_network_fee_below_cap(
        self,
        *,
        sdk: ExtendedCantexSDK,
        router: RouteOptimizer,
        balances: dict[str, Decimal],
        sell_symbol: str,
        buy_symbol: str,
        actual_amount: Decimal,
        round_number: int,
        cc_reserve: Decimal,
        fee_retry_deadline_utc: datetime | None,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        current_route: RoutePlan,
        account_name: str | None = None,
    ) -> tuple[RoutePlan, PlanIssue | None, DailyFreeFeeStatus | None]:
        fee_cap = self.config.runtime.max_network_fee_cc_per_execution
        if fee_cap is None:
            return current_route, None, None

        route = current_route
        while True:
            self._raise_if_stop_requested()
            violating_hop = self._first_network_fee_cap_violation(
                route,
                fee_cap=fee_cap,
            )
            bypassed_free_fee_status: DailyFreeFeeStatus | None = None
            if (
                violating_hop is not None
                and violating_hop[0] == 1
                and account_name is not None
                and self._startup_mode_uses_free_swap()
            ):
                daily_free_fee_status = await self._sync_daily_free_fee_state_from_history(
                    sdk=sdk,
                    account_name=account_name,
                    logger=logger,
                    monitor_card=monitor_card,
                )
                if daily_free_fee_status.window_open and daily_free_fee_status.remaining > 0:
                    bypass_candidate = self._first_network_fee_cap_violation(
                        route,
                        fee_cap=fee_cap,
                        allow_first_hop_free=True,
                    )
                    if bypass_candidate is None:
                        bypassed_free_fee_status = daily_free_fee_status

            if violating_hop is None:
                return route, None, None

            if bypassed_free_fee_status is not None:
                free_swap_number = bypassed_free_fee_status.used + 1
                first_hop = route.hops[0]
                logger.info(
                    "Round %s memakai free fee swap harian %s/3 | hop 1 %s -> %s | fee=%s CC | batas=%s CC",
                    round_number,
                    free_swap_number,
                    first_hop.sell_symbol,
                    first_hop.buy_symbol,
                    first_hop.network_fee_amount,
                    fee_cap,
                )
                await self.monitor.log_event(
                    monitor_card,
                    (
                        f"🎁 Free fee swap {free_swap_number}/3 active for "
                        f"{first_hop.sell_symbol}->{first_hop.buy_symbol}, fee cap bypassed "
                        f"({first_hop.network_fee_amount} CC)"
                    ),
                    force=True,
                )
                return route, None, bypassed_free_fee_status

            violating_hop_index, violating_hop_data = violating_hop
            current_fee = violating_hop_data.network_fee_amount
            account_name = None
            if False:
                daily_free_fee_status = await self._sync_daily_free_fee_state_from_history(
                    sdk=sdk,
                    account_name=account_name,
                    logger=logger,
                    monitor_card=monitor_card,
                )
                if daily_free_fee_status.window_open and daily_free_fee_status.remaining > 0:
                    bypass_candidate = self._first_network_fee_cap_violation(
                        route,
                        fee_cap=fee_cap,
                        allow_first_hop_free=True,
                    )
                    logger.info(
                        "Round %s memakai free fee swap harian %s/3 | fee=%s CC | batas=%s CC",
                        round_number,
                        free_swap_number,
                        current_fee,
                        fee_cap,
                    )
                    await self.monitor.log_event(
                        monitor_card,
                        f"🎁 Free fee swap {free_swap_number}/3 active, fee cap bypassed ({current_fee} CC)",
                        force=True,
                    )
                    return route, None, daily_free_fee_status
            now_utc = datetime.now(timezone.utc)
            if fee_retry_deadline_utc is not None and now_utc >= fee_retry_deadline_utc:
                return (
                    route,
                    PlanIssue(
                        round_number=round_number,
                        sell_symbol=sell_symbol,
                        requested_amount=actual_amount,
                        available_amount=balances.get(sell_symbol, Decimal("0")),
                        reason="network fee tetap di atas batas sampai 30 detik sebelum jadwal berikutnya",
                    ),
                    None,
                )

            wait_seconds = self._sample_network_fee_poll_seconds()
            if fee_retry_deadline_utc is not None:
                seconds_left = (fee_retry_deadline_utc - now_utc).total_seconds()
                if seconds_left <= 0:
                    return (
                        route,
                        PlanIssue(
                            round_number=round_number,
                            sell_symbol=sell_symbol,
                            requested_amount=actual_amount,
                            available_amount=balances.get(sell_symbol, Decimal("0")),
                            reason="network fee tetap di atas batas sampai 30 detik sebelum jadwal berikutnya",
                        ),
                        None,
                    )
                wait_seconds = min(wait_seconds, max(1.0, seconds_left))

            logger.warning(
                "Round %s menunggu network fee turun | hop=%s/%s | %s -> %s | fee=%s CC | batas=%s CC",
                round_number,
                violating_hop_index,
                len(route.hops),
                violating_hop_data.sell_symbol,
                violating_hop_data.buy_symbol,
                current_fee,
                fee_cap,
            )
            await self.monitor.log_event(
                monitor_card,
                (
                    f"⏳ Network fee hop {violating_hop_index}/{len(route.hops)} "
                    f"{violating_hop_data.sell_symbol}->{violating_hop_data.buy_symbol} "
                    f"{current_fee} CC > limit {fee_cap} CC, waiting {int(wait_seconds)}s"
                ),
            )
            await self.monitor.update_status(
                monitor_card,
                round_number=round_number,
                phase="WAITING_FEE",
                next_wait_seconds=wait_seconds,
                route_plan=route,
            )
            await self._sleep_or_stop(wait_seconds)
            info = await sdk.get_account_info()
            balances = self._balances_by_symbol(info)
            try:
                route, issue = await self._prepare_affordable_route(
                    router=router,
                    balances=balances,
                    sell_symbol=sell_symbol,
                    buy_symbol=buy_symbol,
                    proposed_amount=actual_amount,
                    round_number=round_number,
                    cc_reserve=cc_reserve,
                )
            except RuntimeError as exc:
                if not self._is_retryable_route_error(exc):
                    raise
                logger.warning(
                    "Round %s quote gagal sementara saat tunggu fee turun | %s -> %s | %s",
                    round_number,
                    sell_symbol,
                    buy_symbol,
                    exc,
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"⏭️ Round {round_number} pending: transient quote error ({exc})",
                    force=True,
                )
                continue
            if issue is not None:
                return route, issue, None
        return route, None, None

    def _first_network_fee_cap_violation(
        self,
        route: RoutePlan,
        *,
        fee_cap: Decimal,
        allow_first_hop_free: bool = False,
    ) -> tuple[int, RouteHop] | None:
        for hop_index, hop in enumerate(route.hops, start=1):
            if hop.network_fee_symbol != CC_SYMBOL:
                continue
            if allow_first_hop_free and hop_index == 1:
                continue
            if hop.network_fee_amount > fee_cap:
                return hop_index, hop
        return None

    async def _acquire_free_fee_sequence_slot(
        self,
        *,
        account_name: str,
        round_number: int,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
    ) -> bool:
        if self._free_fee_swap_lock.locked():
            logger.info(
                "Round %s menunggu giliran free fee sequential",
                round_number,
            )
            await self.monitor.log_event(
                monitor_card,
                f"â³ Free fee queue: waiting turn for {account_name}",
            )
        await self._free_fee_swap_lock.acquire()
        logger.info(
            "Round %s masuk giliran free fee sequential",
            round_number,
        )
        await self.monitor.log_event(
            monitor_card,
            f"ðŸŽ Free fee sequential turn active for {account_name}",
            force=True,
        )
        return True

    async def _release_free_fee_sequence_slot(
        self,
        *,
        lock_acquired: bool,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        apply_delay: bool,
    ) -> None:
        if not lock_acquired:
            return

        stop_requested: StopRequested | None = None
        try:
            if apply_delay:
                wait_seconds = self._sample_swap_delay_seconds()
                logger.info(
                    "Free fee sequential cooldown %.0f detik sebelum akun berikutnya",
                    wait_seconds,
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"â³ Free fee queue cooldown {int(wait_seconds)}s",
                )
                try:
                    await self._sleep_or_stop(wait_seconds)
                except StopRequested as exc:
                    stop_requested = exc
        finally:
            if self._free_fee_swap_lock.locked():
                self._free_fee_swap_lock.release()

        if stop_requested is not None:
            raise stop_requested

    async def _wait_for_recovery_route_ready(
        self,
        *,
        sdk: ExtendedCantexSDK,
        router: RouteOptimizer,
        balances: dict[str, Decimal],
        source_symbol: str,
        target_symbol: str,
        recovery_amount: Decimal,
        initial_route: RoutePlan,
        initial_issue: PlanIssue | None,
        cc_reserve: Decimal,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
    ) -> tuple[RoutePlan, PlanIssue | None]:
        route = initial_route
        issue = initial_issue
        waiting_logged = False

        while True:
            self._raise_if_stop_requested()
            if issue is None:
                route, issue, _ = await self._wait_for_network_fee_below_cap(
                    sdk=sdk,
                    router=router,
                    balances=balances,
                    sell_symbol=source_symbol,
                    buy_symbol=target_symbol,
                    actual_amount=route.hops[0].sell_amount if route.hops else recovery_amount,
                    round_number=0,
                    cc_reserve=cc_reserve,
                    fee_retry_deadline_utc=None,
                    logger=logger,
                    monitor_card=monitor_card,
                    current_route=route,
                )
                if issue is None:
                    return route, None

            if issue.reason != "balance fee tidak cukup":
                return route, issue

            current_fee = route.total_network_fee_by_symbol.get(CC_SYMBOL, Decimal("0"))
            current_cc = balances.get(CC_SYMBOL, Decimal("0"))
            if not waiting_logged:
                logger.info(
                    "Recovery %s -> %s menunggu fee turun | fee=%s CC | balance CC=%s",
                    source_symbol,
                    target_symbol,
                    current_fee,
                    current_cc,
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"⏭️ Recovery {source_symbol}->{target_symbol} : balance fee tidak cukup, tunggu fee turun",
                    force=True,
                )
                waiting_logged = True

            poll_seconds = self._sample_network_fee_poll_seconds()
            fee_cap = self.config.runtime.max_network_fee_cc_per_execution
            violating_hop = (
                self._first_network_fee_cap_violation(route, fee_cap=fee_cap)
                if fee_cap is not None
                else None
            )
            if violating_hop is not None:
                violating_hop_index, violating_hop_data = violating_hop
                await self.monitor.log_event(
                    monitor_card,
                    (
                        f"⏳ Network fee hop {violating_hop_index}/{len(route.hops)} "
                        f"{violating_hop_data.sell_symbol}->{violating_hop_data.buy_symbol} "
                        f"{violating_hop_data.network_fee_amount} CC > limit {fee_cap} CC, "
                        f"waiting {int(poll_seconds)}s"
                    ),
                )

            await self._sleep_or_stop(poll_seconds)
            info = await sdk.get_account_info()
            balances = self._balances_by_symbol(info)
            available_amount = self._spendable_amount(
                source_symbol,
                balances.get(source_symbol, Decimal("0")),
                cc_reserve,
            )
            if available_amount <= dust_for_symbol(source_symbol):
                return route, PlanIssue(
                    round_number=0,
                    sell_symbol=source_symbol,
                    requested_amount=recovery_amount,
                    available_amount=available_amount,
                    reason=f"{source_symbol} balance tidak cukup untuk recovery",
                )
            recovery_amount = min(recovery_amount, available_amount)
            try:
                route, issue = await self._prepare_affordable_route(
                    router=router,
                    balances=balances,
                    sell_symbol=source_symbol,
                    buy_symbol=target_symbol,
                    proposed_amount=recovery_amount,
                    round_number=0,
                    cc_reserve=cc_reserve,
                )
            except RuntimeError as exc:
                if not self._is_retryable_route_error(exc):
                    raise
                logger.warning(
                    "Recovery %s -> %s quote gagal sementara: %s",
                    source_symbol,
                    target_symbol,
                    exc,
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"⏭️ Recovery {source_symbol}->{target_symbol} : transient quote error ({exc})",
                )
                continue

    def _format_fee_log_line(
        self,
        *,
        prefix: str,
        network_fee: dict[str, Decimal],
        swap_fee: dict[str, Decimal],
    ) -> str:
        total_fee = self._merge_amount_maps(network_fee, swap_fee)
        return (
            f"{prefix} | net={self._format_amount_map(network_fee)} | "
            f"swap={self._format_amount_map(swap_fee)} | "
            f"total={self._format_amount_map(total_fee)}"
        )

    def _merge_amount_maps(
        self,
        left: dict[str, Decimal],
        right: dict[str, Decimal],
    ) -> dict[str, Decimal]:
        merged: defaultdict[str, Decimal] = defaultdict(Decimal)
        for symbol, amount in left.items():
            merged[symbol] += amount
        for symbol, amount in right.items():
            merged[symbol] += amount
        return dict(merged)

    async def _recover_to_symbol(
        self,
        *,
        sdk: ExtendedCantexSDK,
        router: RouteOptimizer,
        target_symbol: str,
        cc_reserve: Decimal,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        used_network_fee: defaultdict[str, Decimal],
        used_swap_fee: defaultdict[str, Decimal],
    ) -> int:
        total_tx = 0
        info = await sdk.get_account_info()
        balances = self._balances_by_symbol(info)
        for source_symbol in self._recovery_source_order(target_symbol):
            available_amount = self._spendable_amount(
                source_symbol,
                balances.get(source_symbol, Decimal("0")),
                cc_reserve,
            )
            if available_amount <= dust_for_symbol(source_symbol):
                continue

            recovery_amount, min_ticket_reason = await self._normalize_amount_for_min_ticket(
                router=router,
                sell_symbol=source_symbol,
                buy_symbol=target_symbol,
                desired_amount=available_amount,
                max_available_amount=available_amount,
            )
            if recovery_amount is None:
                logger.info(
                    "Recovery source %s -> %s dilewati: %s | available=%s",
                    source_symbol,
                    target_symbol,
                    min_ticket_reason,
                    available_amount,
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"⏭️ Recovery {source_symbol}->{target_symbol} skipped: {min_ticket_reason}",
                )
                continue

            while True:
                try:
                    route, issue = await self._prepare_affordable_route(
                        router=router,
                        balances=balances,
                        sell_symbol=source_symbol,
                        buy_symbol=target_symbol,
                        proposed_amount=recovery_amount,
                        round_number=0,
                        cc_reserve=cc_reserve,
                    )
                    break
                except RuntimeError as exc:
                    if not self._is_retryable_route_error(exc):
                        raise
                    wait_seconds = self._sample_network_fee_poll_seconds()
                    logger.warning(
                        "Recovery %s -> %s quote gagal sementara: %s",
                        source_symbol,
                        target_symbol,
                        exc,
                    )
                    await self.monitor.log_event(
                        monitor_card,
                        f"⏭️ Recovery {source_symbol}->{target_symbol} : transient quote error ({exc}), retry {int(wait_seconds)}s",
                    )
                    await self._sleep_or_stop(wait_seconds)
                    info = await sdk.get_account_info()
                    balances = self._balances_by_symbol(info)
                    available_amount = self._spendable_amount(
                        source_symbol,
                        balances.get(source_symbol, Decimal("0")),
                        cc_reserve,
                    )
                    if available_amount <= dust_for_symbol(source_symbol):
                        route = None
                        issue = PlanIssue(
                            round_number=0,
                            sell_symbol=source_symbol,
                            requested_amount=recovery_amount,
                            available_amount=available_amount,
                            reason=f"{source_symbol} balance tidak cukup untuk recovery",
                        )
                        break
                    recovery_amount = min(recovery_amount, available_amount)
            if route is None:
                if issue is not None:
                    logger.info(
                        "Recovery source %s -> %s dilewati: %s | available=%s",
                        source_symbol,
                        target_symbol,
                        issue.reason,
                        available_amount,
                    )
                    await self.monitor.log_event(
                        monitor_card,
                        f"⏭️ Recovery {source_symbol}->{target_symbol} skipped: {issue.reason}",
                    )
                continue
            route, issue = await self._wait_for_recovery_route_ready(
                sdk=sdk,
                router=router,
                balances=balances,
                source_symbol=source_symbol,
                target_symbol=target_symbol,
                recovery_amount=recovery_amount,
                initial_route=route,
                initial_issue=issue,
                cc_reserve=cc_reserve,
                logger=logger,
                monitor_card=monitor_card,
            )
            if issue is not None:
                logger.info(
                    "Recovery source %s -> %s belum affordable: %s",
                    source_symbol,
                    target_symbol,
                    issue.reason,
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"⏭️ Recovery {source_symbol}->{target_symbol} skipped: {issue.reason}",
                )
                continue

            route_amount = route.hops[0].sell_amount if route.hops else recovery_amount
            logger.info(
                "Recovery aset sisa | %s -> %s | nominal=%s | route=%s",
                source_symbol,
                target_symbol,
                route_amount,
                route.label,
            )
            await self.monitor.log_event(
                monitor_card,
                f"🛟 Recovery {source_symbol}->{target_symbol} ({route_amount})",
            )
            recovery_failed = False
            for hop_index, hop in enumerate(route.hops, start=1):
                tx_result, failure_reason = await self._swap_hop_with_retry(
                    sdk=sdk,
                    hop=hop,
                    hop_index=hop_index,
                    hop_total=len(route.hops),
                    round_number=0,
                    logger=logger,
                    monitor_card=monitor_card,
                )
                if tx_result is None:
                    recovery_failed = True
                    await self.monitor.log_event(
                        monitor_card,
                        f"⏭️ Recovery {source_symbol}->{target_symbol} skipped: {failure_reason or 'retry limit reached'}",
                        force=True,
                    )
                    break
                total_tx += 1
                used_network_fee[hop.network_fee_symbol] += hop.network_fee_amount
                used_swap_fee[hop.fee_symbol] += hop.admin_fee_amount + hop.liquidity_fee_amount
                await self.monitor.update_fee_totals(
                    monitor_card,
                    total_network_fee=dict(used_network_fee),
                    total_swap_fee=dict(used_swap_fee),
                )
                tx_identifier = tx_result.get("id") or tx_result.get("transactionId") or tx_result.get("contract_id")
                logger.info(
                    "Recovery tx | %s -> %s | tx=%s",
                    hop.sell_symbol,
                    hop.buy_symbol,
                    tx_identifier or "-",
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"🛟 Recovery tx {hop.sell_symbol}->{hop.buy_symbol} {tx_identifier or '-'}",
                )
                await self._sleep_between_swaps()
            if recovery_failed:
                continue
            if target_symbol == CC_SYMBOL:
                await self.monitor.log_event(
                    monitor_card,
                    f"✅ Recovery {source_symbol}->{target_symbol} : refill berhasil",
                    force=True,
                )
            info = await sdk.get_account_info()
            balances = self._balances_by_symbol(info)
        return total_tx

    async def _run_24h_direct_session(
        self,
        *,
        sdk: ExtendedCantexSDK,
        router: RouteOptimizer,
        account: AccountConfig,
        prepared_run: PreparedAccountRun,
        strategy_state: StrategyRuntimeState,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        used_network_fee: defaultdict[str, Decimal],
        used_swap_fee: defaultdict[str, Decimal],
        result: AccountResult,
    ) -> None:
        while result.completed_rounds < prepared_run.rounds:
            self._raise_if_stop_requested()
            current_round_number = result.completed_rounds + 1

            if self._startup_mode_is_free_only():
                daily_free_fee_status = await self._sync_daily_free_fee_state_from_history(
                    sdk=sdk,
                    account_name=account.name,
                    logger=logger,
                    monitor_card=monitor_card,
                )
                if not daily_free_fee_status.window_open:
                    wait_seconds = max(
                        1.0,
                        (daily_free_fee_status.window_opens_at_utc - datetime.now(timezone.utc)).total_seconds(),
                    )
                    logger.info(
                        "Mode free swap menunggu window buka pada %s UTC",
                        self._format_utc(daily_free_fee_status.window_opens_at_utc),
                    )
                    await self.monitor.update_status(
                        monitor_card,
                        round_number=current_round_number,
                        phase="WAITING",
                        next_scheduled_utc=daily_free_fee_status.window_opens_at_utc,
                        next_wait_seconds=wait_seconds,
                        clear_route=True,
                    )
                    await self.monitor.log_event(
                        monitor_card,
                        f"🎁 Free swap window opens in {int(wait_seconds)}s",
                    )
                    await self._sleep_or_stop(wait_seconds)
                    continue
                if daily_free_fee_status.remaining <= 0:
                    if self.config.runtime.full_24h_auto_restart:
                        await self._wait_until_next_utc_day_after_quota(
                            logger=logger,
                            monitor_card=monitor_card,
                        )
                    return

            round_result = await self._execute_round(
                sdk=sdk,
                router=router,
                account=account,
                prepared_run=prepared_run,
                round_number=current_round_number,
                strategy_state=strategy_state,
                fee_retry_deadline_utc=None,
                logger=logger,
                monitor_card=monitor_card,
                used_network_fee=used_network_fee,
                used_swap_fee=used_swap_fee,
            )
            if round_result.completed:
                result.completed_rounds += 1
                result.swap_transactions += 1
                if result.completed_rounds >= prepared_run.rounds:
                    if self.config.runtime.full_24h_auto_restart:
                        await self._wait_until_next_utc_day_after_quota(
                            logger=logger,
                            monitor_card=monitor_card,
                        )
                    return
                await self._sleep_after_direct_24h_success(
                    logger=logger,
                    monitor_card=monitor_card,
                    next_round_number=result.completed_rounds + 1,
                    pair_key=None,
                )
                continue

            if not round_result.skipped and round_result.stop_reason is not None:
                result.stop_reason = round_result.stop_reason
                return

            if round_result.skipped:
                result.skipped_rounds += 1
            await self._sleep_after_direct_24h_pending(
                logger=logger,
                monitor_card=monitor_card,
                next_round_number=current_round_number,
                pair_key=None,
            )
        return

    async def _run_24h_session(
        self,
        *,
        sdk: ExtendedCantexSDK,
        router: RouteOptimizer,
        account: AccountConfig,
        prepared_run: PreparedAccountRun,
        strategy_state: StrategyRuntimeState,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        used_network_fee: defaultdict[str, Decimal],
        used_swap_fee: defaultdict[str, Decimal],
        result: AccountResult,
        session_end_utc: datetime,
        schedule: tuple[ScheduledRound, ...],
    ) -> None:
        for scheduled_round in schedule:
            self._raise_if_stop_requested()
            if result.completed_rounds >= prepared_run.rounds:
                return
            now_utc = datetime.now(timezone.utc)
            if now_utc >= session_end_utc:
                logger.info(
                    "Mode 24 jam selesai pada %s UTC",
                    self._format_utc(session_end_utc),
                )
                return

            wait_seconds = (scheduled_round.execute_at_utc - now_utc).total_seconds()
            current_round_number = result.completed_rounds + 1
            next_slot_utc = (
                schedule[scheduled_round.round_index + 1].execute_at_utc
                if scheduled_round.round_index + 1 < len(schedule)
                else session_end_utc
            )
            fee_retry_deadline_utc = next_slot_utc - timedelta(seconds=30)
            if wait_seconds > 0:
                logger.info(
                    "Menunggu round %s sampai %s UTC (%.0f detik lagi)",
                    current_round_number,
                    self._format_utc(scheduled_round.execute_at_utc),
                    wait_seconds,
                )
                await self.monitor.update_status(
                    monitor_card,
                    round_number=current_round_number,
                    phase="WAITING",
                    next_scheduled_utc=scheduled_round.execute_at_utc,
                    next_wait_seconds=wait_seconds,
                    clear_route=True,
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"⏳ Next swap in {int(wait_seconds)}s",
                )
                await self._sleep_or_stop(wait_seconds)
            else:
                logger.info(
                    "Round %s sudah melewati jadwal %.0f detik, dieksekusi sekarang",
                    current_round_number,
                    abs(wait_seconds),
                )

            round_result = await self._execute_round(
                sdk=sdk,
                router=router,
                account=account,
                prepared_run=prepared_run,
                round_number=current_round_number,
                strategy_state=strategy_state,
                fee_retry_deadline_utc=fee_retry_deadline_utc,
                logger=logger,
                monitor_card=monitor_card,
                used_network_fee=used_network_fee,
                used_swap_fee=used_swap_fee,
            )
            if round_result.completed:
                result.completed_rounds += 1
                result.swap_transactions += 1
            elif not round_result.skipped and round_result.stop_reason is not None:
                result.stop_reason = round_result.stop_reason
                return
            elif round_result.skipped:
                result.skipped_rounds += 1
        logger.info("Sesi 24 jam selesai pada %s UTC", self._format_utc(session_end_utc))
        return

    def _build_24h_schedule(
        self,
        *,
        rounds: int,
        start_utc: datetime,
        end_utc: datetime,
        execution_buffer_seconds: float,
    ) -> tuple[ScheduledRound, ...]:
        if rounds < 1:
            return ()

        total_seconds = max(1.0, (end_utc - start_utc).total_seconds())
        reserved_seconds = min(max(0.0, execution_buffer_seconds), max(0.0, total_seconds - 1.0))
        schedulable_seconds = max(1.0, total_seconds - reserved_seconds)

        min_gap_seconds = max(0.0, self.config.runtime.full_24h_min_gap_minutes * 60.0)
        if rounds > 1:
            required_gap_total = min_gap_seconds * (rounds - 1)
            if required_gap_total >= schedulable_seconds:
                min_gap_seconds = max(0.0, (schedulable_seconds * 0.8) / (rounds - 1))
        else:
            min_gap_seconds = 0.0

        remaining_seconds = max(1.0, schedulable_seconds - (min_gap_seconds * max(0, rounds - 1)))
        weights = [self._rng.expovariate(1.0) for _ in range(rounds + 1)]
        weight_total = sum(weights)

        timestamps: list[datetime] = []
        elapsed = 0.0
        for round_index in range(rounds):
            elapsed += (weights[round_index] / weight_total) * remaining_seconds
            scheduled_at = start_utc + timedelta(seconds=elapsed)
            timestamps.append(scheduled_at)
            elapsed += min_gap_seconds

        return tuple(
            ScheduledRound(round_index=index, execute_at_utc=timestamp)
            for index, timestamp in enumerate(timestamps)
        )

    def _log_24h_schedule(
        self,
        logger: AccountLoggerAdapter,
        remaining_rounds: int,
        session_start_utc: datetime,
        session_end_utc: datetime,
        schedule: tuple[ScheduledRound, ...],
        execution_buffer_seconds: float,
        start_round_number: int,
    ) -> None:
        logger.info(
            "Mode 24 jam aktif | mulai=%s UTC | selesai=%s UTC | rounds=%s | buffer-eksekusi=%.0f detik",
            self._format_utc(session_start_utc),
            self._format_utc(session_end_utc),
            remaining_rounds,
            execution_buffer_seconds,
        )
        displayed_schedule = self._compress_schedule_for_logging(schedule)
        for scheduled_round in displayed_schedule:
            if scheduled_round is None:
                logger.info("Jadwal random | ... disingkat ...")
                continue
            logger.info(
                "Jadwal random round %s | waktu=%s UTC",
                start_round_number + scheduled_round.round_index,
                self._format_utc(scheduled_round.execute_at_utc),
            )

    def _compress_schedule_for_logging(
        self,
        schedule: tuple[ScheduledRound, ...],
    ) -> tuple[ScheduledRound | None, ...]:
        limit = self.config.runtime.full_24h_schedule_log_limit
        if len(schedule) <= limit:
            return schedule

        head_count = max(1, limit // 2)
        tail_count = max(1, limit - head_count)
        compressed: list[ScheduledRound | None] = list(schedule[:head_count])
        compressed.append(None)
        compressed.extend(schedule[-tail_count:])
        return tuple(compressed)

    def _estimate_24h_execution_buffer_seconds(self, remaining_rounds: int) -> float:
        return max(300.0, remaining_rounds * 90.0)

    async def _wait_until_next_utc_day_after_quota(
        self,
        *,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
    ) -> None:
        now_utc = datetime.now(timezone.utc)
        next_midnight_utc = self._next_utc_midnight(now_utc)
        wait_seconds = max(0.0, (next_midnight_utc - now_utc).total_seconds())
        if wait_seconds <= 0:
            return
        logger.info(
            "Quota harian tercapai, menunggu sampai %s UTC untuk sesi berikutnya",
            self._format_utc(next_midnight_utc),
        )
        await self.monitor.update_status(
            monitor_card,
            phase="WAITING_NEXT_DAY",
            next_scheduled_utc=next_midnight_utc,
            next_wait_seconds=wait_seconds,
            clear_route=True,
        )
        await self.monitor.log_event(
            monitor_card,
            f"🌙 Daily quota reached, waiting until {self._format_utc(next_midnight_utc)} UTC",
            force=True,
        )
        await self._sleep_or_stop(wait_seconds)

    async def _sleep_after_direct_24h_success(
        self,
        *,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        next_round_number: int,
        pair_key: str | None,
    ) -> None:
        wait_seconds = self._sample_swap_delay_seconds()
        logger.info(
            "Mode 24 jam direct menunggu %.0f detik sebelum swap berikutnya",
            wait_seconds,
        )
        await self.monitor.update_status(
            monitor_card,
            pair_key=pair_key,
            round_number=next_round_number,
            phase="WAITING",
            next_wait_seconds=wait_seconds,
            clear_route=True,
        )
        await self.monitor.log_event(
            monitor_card,
            f"⏳ Next swap in {int(wait_seconds)}s",
        )
        await self._sleep_or_stop(wait_seconds)

    async def _sleep_after_direct_24h_pending(
        self,
        *,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        next_round_number: int,
        pair_key: str | None,
    ) -> None:
        wait_seconds = max(1.0, self.config.runtime.retry_base_delay)
        logger.info(
            "Mode 24 jam direct retry lagi dalam %.0f detik",
            wait_seconds,
        )
        await self.monitor.update_status(
            monitor_card,
            pair_key=pair_key,
            round_number=next_round_number,
            phase="WAITING",
            next_wait_seconds=wait_seconds,
            clear_route=True,
        )
        await self.monitor.log_event(
            monitor_card,
            f"⏳ Retry next attempt in {int(wait_seconds)}s",
        )
        await self._sleep_or_stop(wait_seconds)

    def _sample_swap_delay_seconds(self) -> float:
        return self.config.runtime.swap_delay_seconds_range.sample(self._rng)

    async def _sleep_between_swaps(self) -> None:
        if self.config.runtime.full_24h_mode:
            return
        await self._sleep_or_stop(self._sample_swap_delay_seconds())

    def _sample_network_fee_poll_seconds(self) -> float:
        return max(1.0, self.config.runtime.network_fee_poll_seconds_range.sample(self._rng))

    def _startup_mode_label(self) -> str:
        labels = {
            "free_only": "free-only",
            "free_then_swap": "free-then-swap",
            "swap_only": "swap-only",
            "planned_fee": "planned-fee",
        }
        return labels.get(self.startup_mode, self.startup_mode)

    def _startup_mode_is_planned(self) -> bool:
        return self.startup_mode == "planned_fee"

    def _startup_mode_is_free_only(self) -> bool:
        return self.startup_mode == "free_only"

    def _startup_mode_uses_free_swap(self) -> bool:
        return self.startup_mode in {"free_only", "free_then_swap"}

    def _daily_free_fee_status(self, account_name: str) -> DailyFreeFeeStatus:
        return self.runtime_state.get_daily_free_fee_status(account_name)

    def _available_daily_free_fee_status(self, account_name: str) -> DailyFreeFeeStatus | None:
        status = self._daily_free_fee_status(account_name)
        if status.window_open and status.remaining > 0:
            return status
        return None

    def _consume_daily_free_fee_swap(self, account_name: str) -> DailyFreeFeeStatus:
        return self.runtime_state.consume_daily_free_fee_swap(account_name)

    async def _sync_daily_free_fee_state_from_history(
        self,
        *,
        sdk: ExtendedCantexSDK,
        account_name: str,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        force_log: bool = False,
    ) -> DailyFreeFeeStatus:
        local_status = self._daily_free_fee_status(account_name)
        try:
            source_path, payload = await sdk.get_trading_history_payload()
        except Exception as exc:
            logger.warning("Gagal mengambil trading history untuk free fee sync: %s", exc)
            await self.monitor.update_free_fee_status(
                monitor_card,
                used=local_status.used,
                limit=3,
                force=force_log,
            )
            if force_log:
                await self.monitor.log_event(
                    monitor_card,
                    f"ℹ️ Free fee sync fallback to local state ({local_status.used}/3)",
                )
            return local_status

        if payload is None:
            await self.monitor.update_free_fee_status(
                monitor_card,
                used=local_status.used,
                limit=3,
                force=force_log,
            )
            if force_log:
                await self.monitor.log_event(
                    monitor_card,
                    f"ℹ️ Free fee sync fallback to local state ({local_status.used}/3)",
                )
            return local_status

        history_today_count = self._count_today_trading_history_swaps(payload)
        if history_today_count is None:
            await self.monitor.update_free_fee_status(
                monitor_card,
                used=local_status.used,
                limit=3,
                force=force_log,
            )
            if force_log:
                await self.monitor.log_event(
                    monitor_card,
                    f"ℹ️ Free fee history unavailable, using local state ({local_status.used}/3)",
                )
            return local_status

        synced_status = self.runtime_state.sync_daily_free_fee_swaps(
            account_name,
            min(history_today_count, 3),
            exact=force_log,
        )
        await self.monitor.update_free_fee_status(
            monitor_card,
            used=synced_status.used,
            limit=3,
            force=force_log,
        )
        if force_log or synced_status.used != local_status.used:
            logger.info(
                "Free fee sync | source=%s | swap_hari_ini=%s | used=%s/3 | remaining=%s",
                source_path or "-",
                history_today_count,
                synced_status.used,
                synced_status.remaining,
            )
            logger.info(
                "Free fee startup sync: history=%s -> effective=%s/3",
                history_today_count,
                synced_status.used,
            )
            await self.monitor.log_event(
                monitor_card,
                f"🎁 Free fee sync from history: {synced_status.used}/3 used today",
                force=force_log,
            )
        return synced_status

    async def _sleep_or_stop(self, seconds: float) -> None:
        if seconds <= 0:
            self._raise_if_stop_requested()
            return
        try:
            await asyncio.wait_for(self._stop_requested.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            return
        raise StopRequested()

    def _raise_if_stop_requested(self) -> None:
        if self.stop_requested():
            raise StopRequested()

    def _format_utc(self, dt: datetime) -> str:
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    def _monitor_pair_key(self, pair_key: str) -> str:
        return pair_key

    def _next_utc_midnight(self, dt: datetime) -> datetime:
        normalized = dt.astimezone(timezone.utc)
        next_day = normalized.date() + timedelta(days=1)
        return datetime.combine(next_day, datetime.min.time(), tzinfo=timezone.utc)

    def _count_today_trading_history_swaps(self, payload: Any) -> int | None:
        items = self._extract_trading_history_items(payload)
        if not items:
            return None
        today_utc = datetime.now(timezone.utc).date()
        count = 0
        for item in items:
            timestamp = self._extract_item_timestamp(item)
            if timestamp is None or timestamp.date() != today_utc:
                continue
            if self._is_failed_history_item(item):
                continue
            count += 1
        return count

    def _extract_trading_history_items(self, payload: Any) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []

        def visit(node: Any) -> None:
            if isinstance(node, list):
                if node and all(isinstance(entry, dict) for entry in node):
                    timestamped = [
                        entry
                        for entry in node
                        if self._extract_item_timestamp(entry) is not None
                    ]
                    if timestamped:
                        items.extend(timestamped)
                        return
                for child in node:
                    visit(child)
                return

            if isinstance(node, dict):
                for value in node.values():
                    visit(value)

        visit(payload)
        return items

    def _extract_item_timestamp(self, item: dict[str, Any]) -> datetime | None:
        for key in (
            "createdAt",
            "created_at",
            "timestamp",
            "timestamp_utc",
            "time",
            "updatedAt",
            "updated_at",
            "executedAt",
            "executed_at",
        ):
            raw_value = item.get(key)
            parsed = self._parse_datetime_like(raw_value)
            if parsed is not None:
                return parsed
        return None

    def _parse_datetime_like(self, value: Any) -> datetime | None:
        if value in {None, ""}:
            return None
        if isinstance(value, (int, float)):
            timestamp = float(value)
            if timestamp > 10_000_000_000:
                timestamp /= 1000.0
            try:
                return datetime.fromtimestamp(timestamp, tz=timezone.utc)
            except (ValueError, OSError):
                return None

        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _is_failed_history_item(self, item: dict[str, Any]) -> bool:
        status_parts = [
            str(item.get(key, "")).strip().lower()
            for key in ("status", "state", "result")
            if item.get(key) not in {None, ""}
        ]
        if not status_parts:
            return False
        failed_tokens = {"failed", "error", "rejected", "cancelled", "canceled"}
        return any(token in failed_tokens for token in status_parts)

    async def _prepare_affordable_route(
        self,
        *,
        router: RouteOptimizer,
        balances: dict[str, Decimal],
        sell_symbol: str,
        buy_symbol: str,
        proposed_amount: Decimal,
        round_number: int,
        cc_reserve: Decimal,
    ) -> tuple[RoutePlan, PlanIssue | None]:
        effective_cc_reserve = cc_reserve
        route = await router.choose_best_route(sell_symbol, buy_symbol, proposed_amount)
        issue = self._check_route_affordability(
            balances=balances,
            route=route,
            cc_reserve=effective_cc_reserve,
            round_number=round_number,
        )
        if issue is None:
            return route, None

        if sell_symbol == CC_SYMBOL:
            fee_buffer = route.total_network_fee_by_symbol.get(CC_SYMBOL, Decimal("0"))
            adjusted_amount = max(
                Decimal("0"),
                self._spendable_amount(
                    CC_SYMBOL,
                    balances.get(CC_SYMBOL, Decimal("0")),
                    effective_cc_reserve,
                )
                - fee_buffer,
            )
            if adjusted_amount > dust_for_symbol(CC_SYMBOL) and adjusted_amount < proposed_amount:
                route = await router.choose_best_route(sell_symbol, buy_symbol, adjusted_amount)
                issue = self._check_route_affordability(
                    balances=balances,
                    route=route,
                    cc_reserve=effective_cc_reserve,
                    round_number=round_number,
                )
        return route, issue

    def _check_route_affordability(
        self,
        *,
        balances: dict[str, Decimal],
        route: RoutePlan,
        cc_reserve: Decimal,
        round_number: int,
    ) -> PlanIssue | None:
        simulated = deepcopy(balances)
        for hop in route.hops:
            current_sell = simulated.get(hop.sell_symbol, Decimal("0"))
            spendable_sell = self._source_spendable_amount(
                sell_symbol=hop.sell_symbol,
                buy_symbol=hop.buy_symbol,
                balance=current_sell,
                cc_reserve=cc_reserve,
            )
            combined_spend = hop.sell_amount
            if hop.sell_symbol == hop.network_fee_symbol:
                combined_spend += hop.network_fee_amount

            if spendable_sell < combined_spend:
                return PlanIssue(
                    round_number=round_number,
                    sell_symbol=hop.sell_symbol,
                    requested_amount=combined_spend,
                    available_amount=spendable_sell,
                    reason="balance sell token tidak cukup setelah fee",
                )

            if hop.sell_symbol != hop.network_fee_symbol:
                network_fee_balance = simulated.get(hop.network_fee_symbol, Decimal("0"))
                spendable_fee = self._fee_spendable_amount(
                    symbol=hop.network_fee_symbol,
                    balance=network_fee_balance,
                )
                if spendable_fee < hop.network_fee_amount:
                    return PlanIssue(
                        round_number=round_number,
                        sell_symbol=hop.network_fee_symbol,
                        requested_amount=hop.network_fee_amount,
                        available_amount=spendable_fee,
                        reason="balance fee tidak cukup",
                    )

            simulated[hop.sell_symbol] = current_sell - hop.sell_amount
            simulated[hop.network_fee_symbol] = (
                simulated.get(hop.network_fee_symbol, Decimal("0")) - hop.network_fee_amount
            )
            simulated[hop.buy_symbol] = simulated.get(hop.buy_symbol, Decimal("0")) + hop.returned_amount
        return None

    def _apply_route_to_balances(self, balances: dict[str, Decimal], route: RoutePlan) -> None:
        for hop in route.hops:
            balances[hop.sell_symbol] = balances.get(hop.sell_symbol, Decimal("0")) - hop.sell_amount
            balances[hop.network_fee_symbol] = (
                balances.get(hop.network_fee_symbol, Decimal("0")) - hop.network_fee_amount
            )
            balances[hop.buy_symbol] = balances.get(hop.buy_symbol, Decimal("0")) + hop.returned_amount

    async def _fetch_activity_summary(
        self,
        sdk: ExtendedCantexSDK,
        logger: AccountLoggerAdapter,
    ) -> ActivitySummary | None:
        if not self.config.runtime.activity_enabled:
            return None

        activity_source_path: str | None = None
        activity_payload: Any | None = None
        history_source_path: str | None = None
        history_payload: Any | None = None

        try:
            activity_source_path, activity_payload = await sdk.get_activity_payload()
        except Exception as exc:
            logger.warning("Gagal mengambil activity: %s", exc)
        try:
            history_source_path, history_payload = await sdk.get_trading_history_payload()
        except Exception as exc:
            logger.warning("Gagal mengambil trading history: %s", exc)

        if activity_payload is None and history_payload is None:
            logger.info("Activity user tidak tersedia dari endpoint yang dicoba")
            return None

        summary = self._normalize_activity_payload(
            source_path=activity_source_path,
            payload=activity_payload,
            history_source_path=history_source_path,
            history_payload=history_payload,
        )
        self._log_activity_summary(logger, summary)
        return summary

    def _normalize_activity_payload(
        self,
        *,
        source_path: str | None,
        payload: Any | None,
        history_source_path: str | None,
        history_payload: Any | None,
    ) -> ActivitySummary:
        if (
            isinstance(payload, dict)
            and isinstance(payload.get("stats"), dict)
            and isinstance(payload.get("rebates"), dict)
        ):
            return self._normalize_reward_activity_payload(
                source_path=source_path,
                payload=payload,
                history_source_path=history_source_path,
                history_payload=history_payload,
            )

        effective_payload = payload if payload is not None else history_payload
        if effective_payload is None:
            return ActivitySummary()

        swaps_7d = self._find_value(effective_payload, {"swaps7d", "swaps_7d", "seven_day_swaps", "sevenDaySwaps"})
        volume_7d = self._find_value(effective_payload, {"volume7d", "volume_7d", "seven_day_volume", "sevenDayVolume"})
        total_swaps = self._find_value(effective_payload, {"total_swaps", "totalSwaps", "all_time_swaps"})
        total_volume = self._find_value(effective_payload, {"total_volume", "totalVolume", "all_time_volume"})
        reward_total = self._find_value(effective_payload, {"reward", "rewards", "total_reward", "totalReward"})
        tx_count = self._find_value(
            effective_payload,
            {"tx", "tx_count", "transactions", "transactionCount", "total_tx"},
        )
        rank = self._find_value(effective_payload, {"rank", "leaderboard_rank", "leaderboardRank"})
        volume_usd = self._find_value(effective_payload, {"volume_usd", "volumeUsd", "usd_volume", "usdVolume"})
        rebates: dict[str, str] = {}

        rebate_payload = self._find_container(effective_payload, "rebate")
        if isinstance(rebate_payload, dict):
            for label, value in rebate_payload.items():
                rebates[str(label)] = self._stringify_value(value)

        recent_items = self._extract_recent_items(history_payload if history_payload is not None else effective_payload)
        raw_preview = json.dumps(effective_payload, default=str)[:400]
        return ActivitySummary(
            source_path=source_path,
            history_source_path=history_source_path,
            swaps_7d=self._stringify_optional(swaps_7d),
            volume_7d=self._stringify_optional(volume_7d),
            total_swaps=self._stringify_optional(total_swaps),
            total_volume=self._stringify_optional(total_volume),
            reward_total=self._stringify_optional(reward_total),
            tx_count=self._stringify_optional(tx_count),
            rank=self._stringify_optional(rank),
            volume_usd=self._stringify_optional(volume_usd),
            rebates=rebates,
            recent_items=tuple(recent_items[: self.config.runtime.activity_items_limit]),
            raw_preview=raw_preview,
        )

    def _normalize_reward_activity_payload(
        self,
        *,
        source_path: str | None,
        payload: dict[str, Any],
        history_source_path: str | None,
        history_payload: Any | None,
    ) -> ActivitySummary:
        stats = payload.get("stats") or {}
        rebates_payload = payload.get("rebates") or {}
        rebates: dict[str, str] = {}
        for label, entry in rebates_payload.items():
            if isinstance(entry, dict):
                amount = entry.get("cc_amount")
                status = str(entry.get("status", "")).strip()
                parts: list[str] = []
                if amount not in {None, ""}:
                    parts.append(f"{amount} CC")
                if status:
                    parts.append(status)
                rebates[str(label)] = " | ".join(parts) if parts else self._stringify_value(entry)
            else:
                rebates[str(label)] = self._stringify_value(entry)

        recent_items = self._extract_recent_trading_items(history_payload)
        history_preview = self._extract_trading_history_items(history_payload)[:2] if history_payload is not None else []
        raw_preview = json.dumps(
            {
                "reward_activity": payload,
                "history_trading": history_preview,
            },
            default=str,
        )[:400]
        this_week_rebate = rebates_payload.get("this_week") if isinstance(rebates_payload, dict) else None

        return ActivitySummary(
            source_path=source_path,
            history_source_path=history_source_path,
            swaps_24h=self._stringify_optional(stats.get("count_24h")),
            volume_24h=self._stringify_optional(stats.get("cc_volume_24h")),
            volume_24h_usd=self._stringify_optional(
                stats.get("usd_volume_24h")
                or stats.get("volume_usd_24h")
                or stats.get("cc_volume_24h_usd")
            ),
            swaps_7d=self._stringify_optional(stats.get("count_7d")),
            volume_7d=self._stringify_optional(stats.get("cc_volume_7d")),
            swaps_30d=self._stringify_optional(stats.get("count_30d")),
            volume_30d=self._stringify_optional(stats.get("cc_volume_30d")),
            total_swaps=self._stringify_optional(stats.get("count_alltime")),
            total_volume=self._stringify_optional(stats.get("cc_volume_alltime")),
            reward_total=self._extract_rebate_cc_amount(this_week_rebate),
            tx_count=self._stringify_optional(stats.get("count_alltime")),
            rebates=rebates,
            recent_items=tuple(recent_items[: self.config.runtime.activity_items_limit]),
            raw_preview=raw_preview,
        )

    def _extract_recent_items(self, payload: Any) -> list[str]:
        trading_items = self._extract_recent_trading_items(payload)
        if trading_items:
            return trading_items

        items: list[str] = []
        if isinstance(payload, list):
            iterable = payload
        elif isinstance(payload, dict):
            iterable = []
            for value in payload.values():
                if isinstance(value, list):
                    iterable.extend(value[: self.config.runtime.activity_items_limit])
        else:
            iterable = []

        for item in iterable[: self.config.runtime.activity_items_limit]:
            if isinstance(item, dict):
                parts = []
                for key in (
                    "type",
                    "status",
                    "instrument",
                    "instrumentSymbol",
                    "amount",
                    "createdAt",
                    "timestamp",
                    "timestamp_utc",
                ):
                    if key in item:
                        parts.append(f"{key}={item[key]}")
                if parts:
                    items.append(", ".join(parts))
        return items

    def _extract_recent_trading_items(self, payload: Any) -> list[str]:
        history_items = self._extract_trading_history_items(payload)
        formatted: list[str] = []
        for item in history_items[: self.config.runtime.activity_items_limit]:
            rendered = self._format_trading_history_item(item)
            if rendered:
                formatted.append(rendered)
        return formatted

    def _format_trading_history_item(self, item: dict[str, Any]) -> str | None:
        sell_symbol = self._history_symbol(item.get("token_input_instrument_id"))
        buy_symbol = self._history_symbol(item.get("token_output_instrument_id"))
        amount_input = self._compact_decimal_text(item.get("amount_input"))
        amount_output = self._compact_decimal_text(item.get("amount_output"))
        timestamp = self._extract_item_timestamp(item)
        timestamp_text = (
            timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")
            if timestamp is not None
            else self._stringify_optional(item.get("timestamp_utc"))
        )
        if sell_symbol is None or buy_symbol is None:
            return None

        parts = [f"{sell_symbol}->{buy_symbol}"]
        if amount_input is not None and amount_output is not None:
            parts.append(f"in {amount_input}")
            parts.append(f"out {amount_output}")
        price = str(item.get("trade_prices_display", "")).strip()
        if price:
            parts.append(f"price {price}")
        if timestamp_text:
            parts.insert(0, timestamp_text)
        return " | ".join(parts)

    def _history_symbol(self, value: Any) -> str | None:
        if value in {None, ""}:
            return None
        symbol = str(value).strip()
        if symbol == "Amulet":
            return CC_SYMBOL
        return symbol

    def _compact_decimal_text(self, value: Any, *, places: int = 4) -> str | None:
        if value in {None, ""}:
            return None
        try:
            decimal_value = Decimal(str(value))
        except Exception:
            return str(value)
        if decimal_value == 0:
            return "0"
        quantizer = Decimal("1").scaleb(-places)
        rounded = decimal_value.quantize(quantizer)
        rendered = format(rounded, "f")
        if "." in rendered:
            rendered = rendered.rstrip("0").rstrip(".")
        return rendered

    def _extract_rebate_cc_amount(self, rebate_entry: Any) -> str | None:
        if not isinstance(rebate_entry, dict):
            return None
        amount = rebate_entry.get("cc_amount")
        if amount in {None, ""}:
            return None
        return f"{amount} CC"

    def _find_value(self, payload: Any, candidates: set[str]) -> Any | None:
        normalized_candidates = {candidate.lower() for candidate in candidates}
        if isinstance(payload, dict):
            for key, value in payload.items():
                normalized_key = str(key).lower()
                if normalized_key in normalized_candidates:
                    return value
                nested = self._find_value(value, candidates)
                if nested is not None:
                    return nested
        elif isinstance(payload, list):
            for item in payload:
                nested = self._find_value(item, candidates)
                if nested is not None:
                    return nested
        return None

    def _find_container(self, payload: Any, substring: str) -> Any | None:
        if isinstance(payload, dict):
            for key, value in payload.items():
                if substring.lower() in str(key).lower():
                    return value
                nested = self._find_container(value, substring)
                if nested is not None:
                    return nested
        elif isinstance(payload, list):
            for item in payload:
                nested = self._find_container(item, substring)
                if nested is not None:
                    return nested
        return None

    def _log_activity_summary(
        self,
        logger: AccountLoggerAdapter,
        summary: ActivitySummary,
    ) -> None:
        logger.info(
            (
                "Activity | source=%s | 24h swaps=%s | 24h volume=%s | "
                "7d swaps=%s | 7d volume=%s | 30d swaps=%s | 30d volume=%s | "
                "all-time swaps=%s | all-time volume=%s"
            ),
            summary.source_path or "-",
            summary.swaps_24h or "-",
            summary.volume_24h or "-",
            summary.swaps_7d or "-",
            summary.volume_7d or "-",
            summary.swaps_30d or "-",
            summary.volume_30d or "-",
            summary.total_swaps or "-",
            summary.total_volume or "-",
        )
        if summary.history_source_path:
            logger.info("Trading history | source=%s", summary.history_source_path)
        if summary.rebates:
            logger.info("Activity rebates | %s", self._format_text_map(summary.rebates))
        for item in summary.recent_items:
            logger.info("Trading recent | %s", item)

    def _log_balances(self, logger: AccountLoggerAdapter, info: AccountInfo, title: str) -> None:
        logger.info("%s | %s", title, self._format_amount_map(self._balances_by_symbol(info)))

    def _balances_by_symbol(self, info: AccountInfo) -> dict[str, Decimal]:
        balances = {symbol: Decimal("0") for symbol in TRACKED_SYMBOLS}
        for token in info.tokens:
            if token.instrument_symbol in balances:
                balances[token.instrument_symbol] = token.unlocked_amount
        return balances

    def _spendable_amount(
        self,
        symbol: str,
        balance: Decimal,
        cc_reserve: Decimal,
    ) -> Decimal:
        if symbol == CC_SYMBOL:
            return max(Decimal("0"), balance - cc_reserve)
        return max(Decimal("0"), balance)

    def _effective_cc_reserve(
        self,
        account: AccountConfig,
        reserve_override: Decimal | None = None,
    ) -> Decimal:
        if reserve_override is None:
            return account.reserve_fee
        return max(account.reserve_fee, reserve_override)

    def _strategy_4_reserve_fee(self, account: AccountConfig) -> Decimal:
        return account.reserve_fee

    def _strategy_4_reserve_kritis(self, account: AccountConfig) -> Decimal:
        if account.reserve_kritis is None:
            raise RuntimeError("Strategi 4 membutuhkan reserve_kritis")
        return account.reserve_kritis

    def _source_spendable_amount(
        self,
        *,
        sell_symbol: str,
        buy_symbol: str,
        balance: Decimal,
        cc_reserve: Decimal,
    ) -> Decimal:
        if sell_symbol == CC_SYMBOL and buy_symbol != CC_SYMBOL:
            return self._spendable_amount(sell_symbol, balance, cc_reserve)
        return max(Decimal("0"), balance)

    def _fee_spendable_amount(
        self,
        *,
        symbol: str,
        balance: Decimal,
    ) -> Decimal:
        return max(Decimal("0"), balance)

    def _format_amount_map(self, values: dict[str, Decimal]) -> str:
        if not values:
            return "-"
        return ", ".join(f"{symbol}={amount}" for symbol, amount in sorted(values.items()))

    def _format_text_map(self, values: dict[str, str]) -> str:
        if not values:
            return "-"
        return ", ".join(f"{key}={value}" for key, value in values.items())

    def _stringify_optional(self, value: Any) -> str | None:
        if value is None:
            return None
        return self._stringify_value(value)

    def _stringify_value(self, value: Any) -> str:
        if isinstance(value, dict):
            return json.dumps(value, default=str)
        if isinstance(value, list):
            return json.dumps(value[:3], default=str)
        return str(value)

    def _message_for_stop_reason(self, stop_reason: str) -> str:
        mapping = {
            "USER_ABORT_LOW_BALANCE_PROMPT": "Dihentikan user karena balance tidak cukup",
            "MANUAL_STOP": "Dihentikan user secara manual",
            "LOW_BALANCE_MODE_I": "Eksekusi berhenti karena balance kurang dan mode 'i' aktif",
            "INSUFFICIENT_BALANCE": "saldo kurang",
            "RECOVERY_NOT_ENOUGH": "Round di-skip karena recovery belum menghasilkan balance yang cukup",
            "ROUND_AFFORDABILITY_CHECK_FAILED": "Eksekusi berhenti karena route tidak lagi affordable",
            "SWAP_HOP_FAILED": "Eksekusi berhenti karena transaksi swap gagal",
            "SWAP_HOP_FAILED_SKIPPED": "Round di-skip karena swap gagal setelah retry limit",
            "SWAP_RETRY_EXHAUSTED": "Round di-skip karena swap gagal setelah retry limit",
            "MIN_TICKET_SIZE": "Round di-skip karena nominal di bawah minimum ticket size",
            "USER_CONFIG_MIN_NOT_MET": "Round di-skip karena nominal belum memenuhi amount minimum",
            "ROUND_STOPPED": "Eksekusi berhenti di tengah sesi",
        }
        return mapping.get(stop_reason, f"Eksekusi berhenti: {stop_reason}")


def configure_logging(
    level: str,
    *,
    use_utc: bool = False,
    terminal_dashboard_enabled: bool = False,
) -> None:
    if use_utc:
        logging.Formatter.converter = time.gmtime
    root_logger = logging.getLogger()
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)
    root_logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    if not (terminal_dashboard_enabled and sys.stdout.isatty()):
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)s | %(message)s")
        )
        root_logger.addHandler(handler)
    else:
        root_logger.addHandler(logging.NullHandler())
    sdk_log_level = logging.DEBUG if level.upper() == "DEBUG" else logging.WARNING
    logging.getLogger("cantex_sdk").setLevel(sdk_log_level)
    logging.getLogger("cantex_sdk._sdk").setLevel(sdk_log_level)


def summarize_results(results: list[AccountResult]) -> str:
    lines = ["Ringkasan hasil:"]
    for result in results:
        status = "OK" if result.ok else "FAIL"
        lines.append(
            (
                f"- {result.account_name} | {status} | strategi={result.strategy_label} | "
                f"putaran={result.completed_rounds}/{result.requested_rounds} | "
                f"skipped_rounds={result.skipped_rounds} | "
                f"swap_tx={result.swap_transactions} | "
                f"estimasi_network_fee={_format_summary_map(result.estimated_network_fee_by_symbol)} | "
                f"network_fee_terpakai={_format_summary_map(result.used_network_fee_by_symbol)} | "
                f"swap_fee_terpakai={_format_summary_map(result.used_swap_fee_by_symbol)} | "
                f"balance={_format_summary_map(result.final_balances)} | "
                f"stop_reason={result.stop_reason or '-'}"
            )
        )
        if result.error:
            lines.append(f"  error={result.error}")
    return "\n".join(lines)


def _format_summary_map(values: dict[str, Decimal]) -> str:
    if not values:
        return "-"
    return ", ".join(f"{symbol}={amount}" for symbol, amount in sorted(values.items()))
