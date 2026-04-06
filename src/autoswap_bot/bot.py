from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal

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
from .models import ActivitySummary, AccountPlan, AccountResult, PlanIssue, PlanStep, RouteHop, RoutePlan
from .routing import RouteOptimizer
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


@dataclass(frozen=True)
class RoundExecutionResult:
    completed: bool
    tx_count: int
    stop_reason: str | None = None
    skipped: bool = False


LowBalanceMode = Literal["recover", "stop"]
ShortBalancePromptResult = Literal["recover", "stop", "abort"]


class AutoswapBot:
    def __init__(self, config: BotConfig, *, repo_root: Path) -> None:
        self.config = config
        self.repo_root = repo_root
        self.log = logging.getLogger("autoswap_bot")
        self._prompt_lock = asyncio.Lock()
        self._rng = random.Random(self.config.runtime.random_seed)
        self.monitor = TelegramMonitor(self.config.runtime)
        self._stop_requested = asyncio.Event()

    async def request_stop(self) -> None:
        self._stop_requested.set()

    def stop_requested(self) -> bool:
        return self._stop_requested.is_set()

    async def run(self) -> list[AccountResult]:
        await self.monitor.start()
        try:
            accounts = self.config.accounts
            if self.config.runtime.full_24h_mode:
                return await asyncio.gather(*(self._run_account(account) for account in accounts))

            if self.config.runtime.execution_mode == "concurrent" and len(accounts) > 1:
                semaphore = asyncio.Semaphore(self.config.runtime.max_concurrency)

                async def guarded(account: AccountConfig) -> AccountResult:
                    async with semaphore:
                        return await self._run_account(account)

                return await asyncio.gather(*(guarded(account) for account in accounts))

            results: list[AccountResult] = []
            for account in accounts:
                results.append(await self._run_account(account))
            return results
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
                await sdk.authenticate()

                if account.auto_create_intent_account:
                    created = await sdk.ensure_intent_trading_account()
                    if created:
                        logger.info("Intent trading account berhasil dibuat")
                        await self.monitor.log_event(
                            monitor_card,
                            "🧩 Intent account created",
                        )

                admin = await sdk.get_account_admin()
                info = await sdk.get_account_info()
                instruments_by_symbol = self._resolve_instruments(admin.instruments, info)
                router = RouteOptimizer(
                    sdk,
                    instruments_by_symbol,
                    route_mode=self.config.runtime.route_mode,
                )

                logger.info(
                    "Strategi=%s | putaran=%s | nominal-range=%s | delay-range=%s | seed=%s",
                    account.strategy().label,
                    prepared_run.rounds,
                    self._format_text_map(account.describe_amount_ranges()),
                    self.config.runtime.swap_delay_seconds_range.describe(),
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

                plan = await self._build_plan(prepared_run, info, router)
                result.estimated_network_fee_by_symbol = plan.estimated_network_fee_by_symbol
                self._log_plan(logger, plan)
                await self.monitor.log_event(
                    monitor_card,
                    f"📋 Plan ready: {prepared_run.rounds} rounds / {plan.estimated_swap_count} tx",
                )
                await self.monitor.log_event(
                    monitor_card,
                    self._format_fee_log_line(
                        prefix="Fee est all rounds",
                        network_fee=plan.estimated_network_fee_by_symbol,
                        swap_fee=plan.estimated_admin_and_liquidity_by_symbol,
                    ),
                )

                low_balance_mode: LowBalanceMode = (
                    "recover"
                    if self.config.runtime.full_24h_mode or account.allow_continue_on_low_balance
                    else "stop"
                )
                if self.config.runtime.full_24h_mode and not plan.can_fully_execute:
                    logger.warning(
                        "Mode 24 jam aktif, auto-continue dipaksa walau plan tidak penuh"
                    )
                elif not plan.can_fully_execute:
                    prompt_result = await self._handle_short_balance_prompt(logger, plan)
                    if prompt_result == "abort":
                        result.aborted = True
                        result.error = "Dihentikan user karena balance tidak cukup"
                        result.stop_reason = "USER_ABORT_LOW_BALANCE_PROMPT"
                        result.final_balances = self._balances_by_symbol(info)
                        result.activity_summary = await self._fetch_activity_summary(sdk, logger)
                        await self.monitor.log_event(
                            monitor_card,
                            "⛔ Stopped by user due to low balance",
                            force=True,
                        )
                        await self.monitor.finalize(monitor_card, phase="STOPPED_USER")
                        return result
                    low_balance_mode = "recover" if prompt_result == "recover" else "stop"

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
                if self.config.runtime.full_24h_mode:
                    session_start_utc = datetime.now(timezone.utc)
                    session_end_utc = self._next_utc_midnight(session_start_utc)
                    execution_buffer_seconds = self._estimate_24h_execution_buffer_seconds(plan)
                    schedule = self._build_24h_schedule(
                        rounds=prepared_run.rounds,
                        start_utc=session_start_utc,
                        end_utc=session_end_utc,
                        execution_buffer_seconds=execution_buffer_seconds,
                    )
                    self._log_24h_schedule(
                        logger,
                        prepared_run,
                        session_start_utc,
                        session_end_utc,
                        schedule,
                        execution_buffer_seconds,
                    )
                    await self._run_24h_session(
                        sdk=sdk,
                        router=router,
                        prepared_run=prepared_run,
                        low_balance_mode=low_balance_mode,
                        logger=logger,
                        monitor_card=monitor_card,
                        used_network_fee=used_network_fee,
                        used_swap_fee=used_swap_fee,
                        result=result,
                        session_end_utc=session_end_utc,
                        schedule=schedule,
                    )
                else:
                    for round_index in range(prepared_run.rounds):
                        round_result = await self._execute_round(
                            sdk=sdk,
                            router=router,
                            prepared_run=prepared_run,
                            round_index=round_index,
                            low_balance_mode=low_balance_mode,
                            logger=logger,
                            monitor_card=monitor_card,
                            used_network_fee=used_network_fee,
                            used_swap_fee=used_swap_fee,
                        )
                        result.swap_transactions += round_result.tx_count
                        if round_result.completed:
                            result.completed_rounds += 1
                            continue
                        if round_result.skipped:
                            result.skipped_rounds += 1
                            continue
                        if not round_result.completed:
                            if result.error is None:
                                result.aborted = True
                                result.stop_reason = round_result.stop_reason or "ROUND_STOPPED"
                                result.error = self._message_for_stop_reason(result.stop_reason)
                                await self.monitor.log_event(
                                    monitor_card,
                                    f"⛔ Stop reason: {result.stop_reason}",
                                    force=True,
                                )
                                await self.monitor.finalize(
                                    monitor_card,
                                    phase=f"STOPPED_{result.stop_reason}",
                                )
                            break

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

    async def _build_plan(
        self,
        prepared_run: PreparedAccountRun,
        info: AccountInfo,
        router: RouteOptimizer,
    ) -> AccountPlan:
        estimated_network_fee: defaultdict[str, Decimal] = defaultdict(Decimal)
        estimated_admin_fee: defaultdict[str, Decimal] = defaultdict(Decimal)
        simulated_balances = self._balances_by_symbol(info)
        steps: list[PlanStep] = []
        issues: list[PlanIssue] = []
        estimated_swap_count = 0

        for request in prepared_run.round_requests:
            sell_symbol = request.sell_symbol
            buy_symbol = request.buy_symbol
            requested_amount = request.requested_amount
            route = await router.choose_best_route(sell_symbol, buy_symbol, requested_amount)
            issue = self._check_route_affordability(
                balances=simulated_balances,
                route=route,
                min_cc_reserve=self.config.runtime.min_cc_reserve,
                round_number=request.round_number,
            )

            steps.append(
                PlanStep(
                    round_number=request.round_number,
                    sell_symbol=sell_symbol,
                    buy_symbol=buy_symbol,
                    requested_amount=requested_amount,
                    route=route,
                )
            )
            estimated_swap_count += route.tx_count
            for symbol, amount in route.total_network_fee_by_symbol.items():
                estimated_network_fee[symbol] += amount
            for symbol, amount in route.total_admin_and_liquidity_by_symbol.items():
                estimated_admin_fee[symbol] += amount

            if issue is not None:
                issues.append(issue)
                continue

            self._apply_route_to_balances(simulated_balances, route)

        return AccountPlan(
            steps=tuple(steps),
            issues=tuple(issues),
            estimated_network_fee_by_symbol=dict(estimated_network_fee),
            estimated_admin_and_liquidity_by_symbol=dict(estimated_admin_fee),
            estimated_swap_count=estimated_swap_count,
        )

    async def _handle_short_balance_prompt(
        self,
        logger: AccountLoggerAdapter,
        plan: AccountPlan,
    ) -> ShortBalancePromptResult:
        for issue in plan.issues:
            logger.warning(
                "Putaran %s tidak cukup untuk %s: butuh %s, tersedia %s (%s)",
                issue.round_number,
                issue.sell_symbol,
                issue.requested_amount,
                issue.available_amount,
                issue.reason,
            )

        async with self._prompt_lock:
            while True:
                answer = await asyncio.to_thread(
                    input,
                    "balance kurang apakah anda akan tetap melanjutkan?(y/n/i) ",
                )
                normalized = answer.strip().lower()
                if normalized == "y":
                    return "recover"
                if normalized == "i":
                    return "stop"
                if normalized == "n":
                    return "abort"
                print("Masukkan 'y', 'n', atau 'i'.")

    async def _execute_round(
        self,
        *,
        sdk: ExtendedCantexSDK,
        router: RouteOptimizer,
        prepared_run: PreparedAccountRun,
        round_index: int,
        low_balance_mode: LowBalanceMode,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        used_network_fee: defaultdict[str, Decimal],
        used_swap_fee: defaultdict[str, Decimal],
    ) -> RoundExecutionResult:
        tx_count = 0
        round_request = prepared_run.round_requests[round_index]
        sell_symbol = round_request.sell_symbol
        buy_symbol = round_request.buy_symbol
        target_amount = round_request.requested_amount
        pair_key = f"{sell_symbol}->{buy_symbol}"
        info = await sdk.get_account_info()
        balances = self._balances_by_symbol(info)
        await self.monitor.update_balances(monitor_card, balances)
        available_amount = self._spendable_amount(
            sell_symbol,
            balances.get(sell_symbol, Decimal("0")),
            self.config.runtime.min_cc_reserve,
        )

        if available_amount < target_amount:
            if low_balance_mode == "recover":
                logger.warning(
                    "Balance %s kurang pada putaran %s, mencoba recovery aset sisa",
                    sell_symbol,
                    round_index + 1,
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"⚠️ Low balance {sell_symbol}, try recovery",
                )
                recovered_tx, balances, recovery_satisfied = await self._recover_until_target_available(
                    sdk=sdk,
                    router=router,
                    target_symbol=sell_symbol,
                    required_amount=target_amount,
                    logger=logger,
                    monitor_card=monitor_card,
                    used_network_fee=used_network_fee,
                    used_swap_fee=used_swap_fee,
                )
                tx_count += recovered_tx
                available_amount = self._spendable_amount(
                    sell_symbol,
                    balances.get(sell_symbol, Decimal("0")),
                    self.config.runtime.min_cc_reserve,
                )
                if not recovery_satisfied and available_amount < target_amount:
                    await self.monitor.log_event(
                        monitor_card,
                        f"⏭️ Round {round_index + 1} skipped, recovery not enough for {sell_symbol}",
                        force=True,
                    )
                    return RoundExecutionResult(
                        completed=False,
                        tx_count=tx_count,
                        stop_reason="RECOVERY_NOT_ENOUGH",
                        skipped=True,
                    )
            else:
                logger.warning(
                    "Putaran %s dihentikan karena balance %s (%s) kurang dari target %s",
                    round_index + 1,
                    sell_symbol,
                    available_amount,
                    target_amount,
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"⛔ Round {round_index + 1} stopped, insufficient {sell_symbol}",
                    force=True,
                )
                return RoundExecutionResult(
                    completed=False,
                    tx_count=tx_count,
                    stop_reason="LOW_BALANCE_MODE_I",
                )

        if available_amount <= dust_for_symbol(sell_symbol):
            logger.warning(
                "Putaran %s dihentikan karena balance %s sudah tidak cukup",
                round_index + 1,
                sell_symbol,
            )
            await self.monitor.log_event(
                monitor_card,
                f"⛔ Round {round_index + 1} skipped, insufficient {sell_symbol}",
                force=True,
            )
            return RoundExecutionResult(
                completed=False,
                tx_count=tx_count,
                stop_reason="INSUFFICIENT_BALANCE",
            )

        actual_amount = min(target_amount, available_amount)
        actual_amount, min_ticket_reason = await self._normalize_amount_for_min_ticket(
            router=router,
            sell_symbol=sell_symbol,
            buy_symbol=buy_symbol,
            desired_amount=actual_amount,
            max_available_amount=available_amount,
        )
        if actual_amount is None:
            logger.warning(
                "Putaran %s di-skip karena nominal %s terlalu kecil terhadap minimum ticket",
                round_index + 1,
                sell_symbol,
            )
            await self.monitor.log_event(
                monitor_card,
                f"⏭️ Round {round_index + 1} skipped: {min_ticket_reason}",
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
            round_number=round_index + 1,
        )
        if issue is not None:
            if low_balance_mode != "recover":
                logger.warning("Putaran %s dibatalkan: %s", round_index + 1, issue.reason)
                await self.monitor.log_event(
                    monitor_card,
                    f"⛔ Round {round_index + 1} canceled: {issue.reason}",
                    force=True,
                )
                return RoundExecutionResult(
                    completed=False,
                    tx_count=tx_count,
                    stop_reason="ROUND_AFFORDABILITY_CHECK_FAILED",
                )

            if issue.sell_symbol != sell_symbol:
                logger.warning(
                    "Aset untuk fee kurang pada putaran %s, mencoba recovery ke %s",
                    round_index + 1,
                    issue.sell_symbol,
                )
                recovered_tx = await self._recover_to_symbol(
                    sdk=sdk,
                    router=router,
                    target_symbol=issue.sell_symbol,
                    logger=logger,
                    monitor_card=monitor_card,
                    used_network_fee=used_network_fee,
                    used_swap_fee=used_swap_fee,
                )
                tx_count += recovered_tx
                info = await sdk.get_account_info()
                balances = self._balances_by_symbol(info)

            actual_amount = min(
                available_amount,
                self._spendable_amount(
                    sell_symbol,
                    balances.get(sell_symbol, Decimal("0")),
                    self.config.runtime.min_cc_reserve,
                ),
            )
            route, issue = await self._prepare_affordable_route(
                router=router,
                balances=balances,
                sell_symbol=sell_symbol,
                buy_symbol=buy_symbol,
                proposed_amount=actual_amount,
                round_number=round_index + 1,
            )
            if issue is not None:
                logger.warning("Putaran %s tetap gagal: %s", round_index + 1, issue.reason)
                await self.monitor.log_event(
                    monitor_card,
                    f"❌ Round {round_index + 1} failed: {issue.reason}",
                    force=True,
                )
                return RoundExecutionResult(
                    completed=False,
                    tx_count=tx_count,
                    stop_reason="ROUND_AFFORDABILITY_CHECK_FAILED",
                )

        route, issue = await self._wait_for_network_fee_below_cap(
            router=router,
            balances=balances,
            sell_symbol=sell_symbol,
            buy_symbol=buy_symbol,
            actual_amount=actual_amount,
            round_number=round_index + 1,
            logger=logger,
            monitor_card=monitor_card,
            current_route=route,
        )
        if issue is not None:
            return RoundExecutionResult(
                completed=False,
                tx_count=tx_count,
                stop_reason="ROUND_AFFORDABILITY_CHECK_FAILED",
            )

        await self.monitor.update_status(
            monitor_card,
            pair_key=self._monitor_pair_key(pair_key),
            round_number=round_index + 1,
            phase="PROCESSING",
        )
        logger.info(
            "Putaran %s | %s -> %s | nominal=%s | route=%s | fee est=%s | network fee est=%s",
            round_index + 1,
            sell_symbol,
            buy_symbol,
            actual_amount,
            route.label,
            self._format_amount_map(route.total_admin_and_liquidity_by_symbol),
            self._format_amount_map(route.total_network_fee_by_symbol),
        )
        await self.monitor.log_event(
            monitor_card,
            f"🔄 Round {round_index + 1}/{prepared_run.rounds} {self._monitor_pair_key(pair_key)} ({actual_amount})",
        )

        for hop_index, hop in enumerate(route.hops, start=1):
            tx_result, failure_reason = await self._swap_hop_with_retry(
                sdk=sdk,
                hop=hop,
                hop_index=hop_index,
                hop_total=len(route.hops),
                round_number=round_index + 1,
                logger=logger,
                monitor_card=monitor_card,
            )
            if tx_result is None:
                await self.monitor.log_event(
                    monitor_card,
                    f"⏭️ Round {round_index + 1} skipped: {failure_reason or 'retry limit reached'}",
                    force=True,
                )
                return RoundExecutionResult(
                    completed=False,
                    tx_count=tx_count,
                    stop_reason=failure_reason or "SWAP_HOP_FAILED_SKIPPED",
                    skipped=True,
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
            tx_count_delta=tx_count,
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

    async def _recover_until_target_available(
        self,
        *,
        sdk: ExtendedCantexSDK,
        router: RouteOptimizer,
        target_symbol: str,
        required_amount: Decimal,
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
                self.config.runtime.min_cc_reserve,
            )
            if spendable >= required_amount:
                return total_tx, last_balances, True

            recovered_tx = await self._recover_to_symbol(
                sdk=sdk,
                router=router,
                target_symbol=target_symbol,
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
                self.config.runtime.min_cc_reserve,
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
    ) -> tuple[dict[str, Any] | None, str | None]:
        max_attempts = max(1, self.config.runtime.max_retries)
        for attempt in range(1, max_attempts + 1):
            self._raise_if_stop_requested()
            try:
                return (
                    await sdk.swap(
                    sell_amount=hop.sell_amount,
                    sell_instrument=hop.raw_quote.sell_instrument,
                    buy_instrument=hop.raw_quote.buy_instrument,
                    ),
                    None,
                )
            except Exception as exc:
                if self._is_min_ticket_error(exc):
                    logger.warning(
                        "Swap hop %s/%s round %s gagal karena minimum ticket size: %s",
                        hop_index,
                        hop_total,
                        round_number,
                        exc,
                    )
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

    async def _wait_for_network_fee_below_cap(
        self,
        *,
        router: RouteOptimizer,
        balances: dict[str, Decimal],
        sell_symbol: str,
        buy_symbol: str,
        actual_amount: Decimal,
        round_number: int,
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        current_route: RoutePlan,
    ) -> tuple[RoutePlan, PlanIssue | None]:
        fee_cap = self.config.runtime.max_network_fee_cc_per_execution
        if fee_cap is None:
            return current_route, None

        route = current_route
        while route.total_network_fee_by_symbol.get(CC_SYMBOL, Decimal("0")) > fee_cap:
            self._raise_if_stop_requested()
            current_fee = route.total_network_fee_by_symbol.get(CC_SYMBOL, Decimal("0"))
            logger.warning(
                "Round %s menunggu network fee turun | fee=%s CC | batas=%s CC",
                round_number,
                current_fee,
                fee_cap,
            )
            await self.monitor.log_event(
                monitor_card,
                f"⏳ Network fee {current_fee} CC > limit {fee_cap} CC, waiting {int(self.config.runtime.network_fee_poll_seconds)}s",
            )
            await self.monitor.update_status(
                monitor_card,
                round_number=round_number,
                phase="WAITING_FEE",
                next_wait_seconds=self.config.runtime.network_fee_poll_seconds,
            )
            await self._sleep_or_stop(self.config.runtime.network_fee_poll_seconds)
            route, issue = await self._prepare_affordable_route(
                router=router,
                balances=balances,
                sell_symbol=sell_symbol,
                buy_symbol=buy_symbol,
                proposed_amount=actual_amount,
                round_number=round_number,
            )
            if issue is not None:
                return route, issue
        return route, None

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
        logger: AccountLoggerAdapter,
        monitor_card: TelegramCardState | None,
        used_network_fee: defaultdict[str, Decimal],
        used_swap_fee: defaultdict[str, Decimal],
    ) -> int:
        total_tx = 0
        info = await sdk.get_account_info()
        balances = self._balances_by_symbol(info)
        for source_symbol in TRACKED_SYMBOLS:
            if source_symbol == target_symbol:
                continue
            available_amount = self._spendable_amount(
                source_symbol,
                balances.get(source_symbol, Decimal("0")),
                self.config.runtime.min_cc_reserve,
            )
            if available_amount <= dust_for_symbol(source_symbol):
                continue

            route = await router.choose_best_route(source_symbol, target_symbol, available_amount)
            issue = self._check_route_affordability(
                balances=balances,
                route=route,
                min_cc_reserve=self.config.runtime.min_cc_reserve,
                round_number=0,
            )
            if issue is not None:
                continue

            logger.info(
                "Recovery aset sisa | %s -> %s | nominal=%s | route=%s",
                source_symbol,
                target_symbol,
                available_amount,
                route.label,
            )
            await self.monitor.log_event(
                monitor_card,
                f"🛟 Recovery {source_symbol}->{target_symbol} ({available_amount})",
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
            info = await sdk.get_account_info()
            balances = self._balances_by_symbol(info)
        return total_tx

    async def _run_24h_session(
        self,
        *,
        sdk: ExtendedCantexSDK,
        router: RouteOptimizer,
        prepared_run: PreparedAccountRun,
        low_balance_mode: LowBalanceMode,
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
            now_utc = datetime.now(timezone.utc)
            if now_utc >= session_end_utc:
                logger.info(
                    "Mode 24 jam selesai pada %s UTC",
                    self._format_utc(session_end_utc),
                )
                return

            wait_seconds = (scheduled_round.execute_at_utc - now_utc).total_seconds()
            if wait_seconds > 0:
                logger.info(
                    "Menunggu round %s sampai %s UTC (%.0f detik lagi)",
                    scheduled_round.round_index + 1,
                    self._format_utc(scheduled_round.execute_at_utc),
                    wait_seconds,
                )
                await self.monitor.update_status(
                    monitor_card,
                    pair_key=self._monitor_pair_key(
                        f"{prepared_run.round_requests[scheduled_round.round_index].sell_symbol}->{prepared_run.round_requests[scheduled_round.round_index].buy_symbol}"
                    ),
                    round_number=scheduled_round.round_index + 1,
                    phase="WAITING",
                    next_scheduled_utc=scheduled_round.execute_at_utc,
                    next_wait_seconds=wait_seconds,
                )
                await self.monitor.log_event(
                    monitor_card,
                    f"⏳ Next swap in {int(wait_seconds)}s",
                )
                await self._sleep_or_stop(wait_seconds)
            else:
                logger.info(
                    "Round %s sudah melewati jadwal %.0f detik, dieksekusi sekarang",
                    scheduled_round.round_index + 1,
                    abs(wait_seconds),
                )

            round_result = await self._execute_round(
                sdk=sdk,
                router=router,
                prepared_run=prepared_run,
                round_index=scheduled_round.round_index,
                low_balance_mode=low_balance_mode,
                logger=logger,
                monitor_card=monitor_card,
                used_network_fee=used_network_fee,
                used_swap_fee=used_swap_fee,
            )
            result.swap_transactions += round_result.tx_count
            if round_result.completed:
                result.completed_rounds += 1
                continue
            if round_result.skipped:
                result.skipped_rounds += 1
                continue
            if not round_result.completed:
                if result.error is None:
                    result.aborted = True
                    result.stop_reason = round_result.stop_reason or "ROUND_STOPPED"
                    result.error = self._message_for_stop_reason(result.stop_reason)
                    await self.monitor.log_event(
                        monitor_card,
                        f"⛔ Stop reason: {result.stop_reason}",
                        force=True,
                    )
                    await self.monitor.finalize(
                        monitor_card,
                        phase=f"STOPPED_{result.stop_reason}",
                    )
                return

        now_utc = datetime.now(timezone.utc)
        remaining_seconds = (session_end_utc - now_utc).total_seconds()
        if remaining_seconds > 0:
            logger.info(
                "Menunggu akhir sesi 24 jam sampai %s UTC (%.0f detik lagi)",
                self._format_utc(session_end_utc),
                remaining_seconds,
            )
            await self._sleep_or_stop(remaining_seconds)
        logger.info("Sesi 24 jam selesai pada %s UTC", self._format_utc(session_end_utc))

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
        schedulable_seconds = total_seconds - max(0.0, execution_buffer_seconds)
        if schedulable_seconds <= 0:
            raise ValueError(
                "Window 24 jam tidak cukup setelah memperhitungkan buffer eksekusi swap"
            )

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
        prepared_run: PreparedAccountRun,
        session_start_utc: datetime,
        session_end_utc: datetime,
        schedule: tuple[ScheduledRound, ...],
        execution_buffer_seconds: float,
    ) -> None:
        logger.info(
            "Mode 24 jam aktif | mulai=%s UTC | selesai=%s UTC | rounds=%s | buffer-eksekusi=%.0f detik",
            self._format_utc(session_start_utc),
            self._format_utc(session_end_utc),
            prepared_run.rounds,
            execution_buffer_seconds,
        )
        displayed_schedule = self._compress_schedule_for_logging(schedule)
        for scheduled_round in displayed_schedule:
            if scheduled_round is None:
                logger.info("Jadwal random | ... disingkat ...")
                continue
            request = prepared_run.round_requests[scheduled_round.round_index]
            logger.info(
                "Jadwal random round %s | %s -> %s | nominal=%s | waktu=%s UTC",
                request.round_number,
                request.sell_symbol,
                request.buy_symbol,
                request.requested_amount,
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

    def _estimate_24h_execution_buffer_seconds(self, plan: AccountPlan) -> float:
        quote_based_seconds = 0.0
        for step in plan.steps:
            for hop in step.route.hops:
                quote_based_seconds += max(float(hop.estimated_time_seconds), 45.0)
        safety_padding_seconds = max(300.0, plan.estimated_swap_count * 20.0)
        return quote_based_seconds + safety_padding_seconds

    async def _sleep_between_swaps(self) -> None:
        if self.config.runtime.full_24h_mode:
            return
        await self._sleep_or_stop(self.config.runtime.swap_delay_seconds_range.sample(self._rng))

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

    async def _prepare_affordable_route(
        self,
        *,
        router: RouteOptimizer,
        balances: dict[str, Decimal],
        sell_symbol: str,
        buy_symbol: str,
        proposed_amount: Decimal,
        round_number: int,
    ) -> tuple[RoutePlan, PlanIssue | None]:
        route = await router.choose_best_route(sell_symbol, buy_symbol, proposed_amount)
        issue = self._check_route_affordability(
            balances=balances,
            route=route,
            min_cc_reserve=self.config.runtime.min_cc_reserve,
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
                    self.config.runtime.min_cc_reserve,
                )
                - fee_buffer,
            )
            if adjusted_amount > dust_for_symbol(CC_SYMBOL) and adjusted_amount < proposed_amount:
                route = await router.choose_best_route(sell_symbol, buy_symbol, adjusted_amount)
                issue = self._check_route_affordability(
                    balances=balances,
                    route=route,
                    min_cc_reserve=self.config.runtime.min_cc_reserve,
                    round_number=round_number,
                )
        return route, issue

    def _check_route_affordability(
        self,
        *,
        balances: dict[str, Decimal],
        route: RoutePlan,
        min_cc_reserve: Decimal,
        round_number: int,
    ) -> PlanIssue | None:
        simulated = deepcopy(balances)
        for hop in route.hops:
            current_sell = simulated.get(hop.sell_symbol, Decimal("0"))
            spendable_sell = self._spendable_amount(hop.sell_symbol, current_sell, min_cc_reserve)
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
                spendable_fee = self._spendable_amount(
                    hop.network_fee_symbol,
                    network_fee_balance,
                    min_cc_reserve,
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

        try:
            source_path, payload = await sdk.get_activity_payload()
        except Exception as exc:
            logger.warning("Gagal mengambil activity: %s", exc)
            return None
        if payload is None:
            logger.info("Activity user tidak tersedia dari endpoint yang dicoba")
            return None

        summary = self._normalize_activity_payload(source_path, payload)
        self._log_activity_summary(logger, summary)
        return summary

    def _normalize_activity_payload(
        self,
        source_path: str | None,
        payload: Any,
    ) -> ActivitySummary:
        swaps_7d = self._find_value(payload, {"swaps7d", "swaps_7d", "seven_day_swaps", "sevenDaySwaps"})
        volume_7d = self._find_value(payload, {"volume7d", "volume_7d", "seven_day_volume", "sevenDayVolume"})
        total_swaps = self._find_value(payload, {"total_swaps", "totalSwaps", "all_time_swaps"})
        total_volume = self._find_value(payload, {"total_volume", "totalVolume", "all_time_volume"})
        reward_total = self._find_value(payload, {"reward", "rewards", "total_reward", "totalReward"})
        tx_count = self._find_value(payload, {"tx", "tx_count", "transactions", "transactionCount", "total_tx"})
        rank = self._find_value(payload, {"rank", "leaderboard_rank", "leaderboardRank"})
        volume_usd = self._find_value(payload, {"volume_usd", "volumeUsd", "usd_volume", "usdVolume"})
        rebates: dict[str, str] = {}

        rebate_payload = self._find_container(payload, "rebate")
        if isinstance(rebate_payload, dict):
            for label, value in rebate_payload.items():
                rebates[str(label)] = self._stringify_value(value)

        recent_items = self._extract_recent_items(payload)
        raw_preview = json.dumps(payload, default=str)[:400]
        return ActivitySummary(
            source_path=source_path,
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

    def _extract_recent_items(self, payload: Any) -> list[str]:
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
                for key in ("type", "status", "instrument", "instrumentSymbol", "amount", "createdAt", "timestamp"):
                    if key in item:
                        parts.append(f"{key}={item[key]}")
                if parts:
                    items.append(", ".join(parts))
        return items

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

    def _log_plan(self, logger: AccountLoggerAdapter, plan: AccountPlan) -> None:
        logger.info(
            "Estimasi swap tx=%s | estimasi network fee=%s | estimasi fee swap=%s",
            plan.estimated_swap_count,
            self._format_amount_map(plan.estimated_network_fee_by_symbol),
            self._format_amount_map(plan.estimated_admin_and_liquidity_by_symbol),
        )
        for step in plan.steps:
            logger.info(
                "Plan putaran %s | %s -> %s | nominal=%s | route=%s | tx=%s | output est=%s %s",
                step.round_number,
                step.sell_symbol,
                step.buy_symbol,
                step.requested_amount,
                step.route.label,
                step.route.tx_count,
                step.route.final_amount,
                step.buy_symbol,
            )

    def _log_activity_summary(
        self,
        logger: AccountLoggerAdapter,
        summary: ActivitySummary,
    ) -> None:
        logger.info(
            "Activity | source=%s | 7d swaps=%s | 7d volume=%s | total swaps=%s | total volume=%s",
            summary.source_path or "-",
            summary.swaps_7d or "-",
            summary.volume_7d or "-",
            summary.total_swaps or "-",
            summary.total_volume or "-",
        )
        if summary.rebates:
            logger.info("Activity rebates | %s", self._format_text_map(summary.rebates))
        for item in summary.recent_items:
            logger.info("Activity recent | %s", item)

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
        min_cc_reserve: Decimal,
    ) -> Decimal:
        if symbol == CC_SYMBOL:
            return max(Decimal("0"), balance - min_cc_reserve)
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
            "INSUFFICIENT_BALANCE": "Eksekusi berhenti karena balance tidak cukup",
            "RECOVERY_NOT_ENOUGH": "Round di-skip karena recovery belum menghasilkan balance yang cukup",
            "ROUND_AFFORDABILITY_CHECK_FAILED": "Eksekusi berhenti karena route tidak lagi affordable",
            "SWAP_HOP_FAILED": "Eksekusi berhenti karena transaksi swap gagal",
            "SWAP_HOP_FAILED_SKIPPED": "Round di-skip karena swap gagal setelah retry limit",
            "SWAP_RETRY_EXHAUSTED": "Round di-skip karena swap gagal setelah retry limit",
            "MIN_TICKET_SIZE": "Round di-skip karena nominal di bawah minimum ticket size",
            "ROUND_STOPPED": "Eksekusi berhenti di tengah sesi",
        }
        return mapping.get(stop_reason, f"Eksekusi berhenti: {stop_reason}")


def configure_logging(level: str, *, use_utc: bool = False) -> None:
    if use_utc:
        logging.Formatter.converter = time.gmtime
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    )


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
