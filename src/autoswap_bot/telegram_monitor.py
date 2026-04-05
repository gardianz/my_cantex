from __future__ import annotations

import html
import logging
import re
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import aiohttp

from .config import AccountConfig, PreparedAccountRun, RuntimeConfig
from .models import ActivitySummary


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
    current_pair_key: str | None = None
    current_round_number: int = 0
    phase: str = "STARTING"
    balances: dict[str, Decimal] = field(default_factory=dict)
    swap_transactions: int = 0
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


class TelegramMonitor:
    def __init__(self, runtime: RuntimeConfig) -> None:
        self.runtime = runtime
        self.log = logging.getLogger("autoswap_bot.telegram")
        self._session: aiohttp.ClientSession | None = None

    async def start(self) -> None:
        if not self.runtime.telegram_enabled:
            return
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    def create_card(
        self,
        account: AccountConfig,
        prepared_run: PreparedAccountRun,
        strategy_label: str,
    ) -> TelegramCardState:
        pair_targets: dict[str, int] = {}
        for request in prepared_run.round_requests:
            pair_key = f"{request.sell_symbol}->{request.buy_symbol}"
            pair_targets[pair_key] = pair_targets.get(pair_key, 0) + 1

        return TelegramCardState(
            account_name=account.name,
            display_index=account.display_index,
            proxy_label=account.proxy_label,
            strategy_label=strategy_label,
            session_started_utc=datetime.now(timezone.utc),
            total_rounds=prepared_run.rounds,
            pair_targets=pair_targets,
            balances={symbol: Decimal("0") for symbol in SYMBOL_SHORT},
            latest_logs=deque(maxlen=self.runtime.telegram_latest_logs_limit),
        )

    async def attach_card(self, card: TelegramCardState | None) -> None:
        if card is None or not self.runtime.telegram_enabled:
            return
        await self._publish(card, force=True)

    async def log_event(
        self,
        card: TelegramCardState | None,
        message: str,
        *,
        force: bool = False,
    ) -> None:
        if card is None:
            return
        card.latest_logs.append(self._timestamped_log(message))
        await self._publish(card, force=force)

    async def update_status(
        self,
        card: TelegramCardState | None,
        *,
        pair_key: str | None = None,
        round_number: int | None = None,
        phase: str | None = None,
        next_scheduled_utc: datetime | None = None,
        next_wait_seconds: float | None = None,
        force: bool = False,
    ) -> None:
        if card is None:
            return
        if pair_key is not None:
            card.current_pair_key = pair_key
        if round_number is not None:
            card.current_round_number = round_number
        if phase is not None:
            card.phase = phase
        card.next_scheduled_utc = next_scheduled_utc
        card.next_wait_seconds = next_wait_seconds
        await self._publish(card, force=force)

    async def update_balances(
        self,
        card: TelegramCardState | None,
        balances: dict[str, Decimal],
        *,
        force: bool = False,
    ) -> None:
        if card is None:
            return
        card.balances.update(balances)
        await self._publish(card, force=force)

    async def record_round_completed(
        self,
        card: TelegramCardState | None,
        *,
        pair_key: str,
        tx_count_delta: int,
        force: bool = True,
    ) -> None:
        if card is None:
            return
        card.swap_transactions += tx_count_delta
        card.pair_completed[pair_key] = card.pair_completed.get(pair_key, 0) + 1
        card.phase = "COMPLETED"
        card.next_scheduled_utc = None
        card.next_wait_seconds = None
        await self._publish(card, force=force)

    async def update_activity(
        self,
        card: TelegramCardState | None,
        summary: ActivitySummary | None,
        *,
        force: bool = False,
    ) -> None:
        if card is None or summary is None:
            return
        if card.baseline_activity is None:
            card.baseline_activity = summary
        card.activity_summary = summary
        await self._publish(card, force=force)

    async def finalize(
        self,
        card: TelegramCardState | None,
        *,
        phase: str,
    ) -> None:
        if card is None:
            return
        card.phase = phase
        card.session_finished_utc = datetime.now(timezone.utc)
        card.next_scheduled_utc = None
        card.next_wait_seconds = None
        await self._publish(card, force=True)

    async def _publish(self, card: TelegramCardState, *, force: bool) -> None:
        if not self.runtime.telegram_enabled:
            return
        if self._session is None:
            raise RuntimeError("TelegramMonitor belum di-start")

        now = time.monotonic()
        if (
            not force
            and card.message_id is not None
            and now - card.last_publish_monotonic < self.runtime.telegram_update_min_interval_seconds
        ):
            return

        text = self._trim_message(self._render_card(card))
        if card.message_id is not None and text == card.last_render_text:
            return

        try:
            if card.message_id is None:
                response = await self._request(
                    "sendMessage",
                    {
                        "chat_id": self.runtime.telegram_chat_id,
                        "text": text,
                        "parse_mode": "HTML",
                        "disable_web_page_preview": True,
                    },
                )
                card.message_id = response.get("result", {}).get("message_id")
            else:
                await self._request(
                    "editMessageText",
                    {
                        "chat_id": self.runtime.telegram_chat_id,
                        "message_id": card.message_id,
                        "text": text,
                        "parse_mode": "HTML",
                        "disable_web_page_preview": True,
                    },
                )
            card.last_render_text = text
            card.last_publish_monotonic = now
        except Exception as exc:  # pragma: no cover - network/runtime guard
            self.log.warning("Gagal update Telegram card %s: %s", card.account_name, exc)

    async def _request(self, method: str, payload: dict) -> dict:
        if self._session is None:
            raise RuntimeError("TelegramMonitor belum di-start")
        url = f"https://api.telegram.org/bot{self.runtime.telegram_bot_token}/{method}"
        async with self._session.post(url, json=payload) as response:
            data = await response.json(content_type=None)
            if not response.ok or not data.get("ok", False):
                raise RuntimeError(f"Telegram API error: {data}")
            return data

    def _render_card(self, card: TelegramCardState) -> str:
        latest_logs = "\n".join(card.latest_logs) if card.latest_logs else "-"
        edited_at = datetime.now(timezone.utc).strftime("%H.%M.%S")

        sections = [
            f"<b>{html.escape(f'🔵 Acc {card.display_index}')}</b>",
            f"Status: {html.escape(self._build_status_line(card))}",
            f"Uptime: {html.escape(self._format_duration(datetime.now(timezone.utc) - card.session_started_utc))}",
            html.escape(self._build_balances_line(card)),
            html.escape(self._build_swaps_line(card)),
            f"Proxy: {html.escape(card.proxy_label)}",
            html.escape(self._build_reward_line(card)),
            html.escape(self._build_delta_line(card)),
            "",
            "<b>Latest Logs</b>",
            f"<pre>{html.escape(latest_logs)}</pre>",
            f"<i>Edited {html.escape(edited_at)} UTC</i>",
        ]
        return "\n".join(sections)

    def _build_status_line(self, card: TelegramCardState) -> str:
        pair = (
            self._short_pair(card.current_pair_key)
            if card.current_pair_key
            else self._strategy_short(card.strategy_label)
        )
        round_number = max(card.current_round_number, 0)
        if card.phase == "WAITING" and card.next_wait_seconds is not None:
            wait_seconds = int(max(card.next_wait_seconds, 0))
            return f"{pair} R{round_number}/{card.total_rounds} | WAIT {wait_seconds}s"
        return f"{pair} R{round_number}/{card.total_rounds} | {card.phase}"

    def _build_balances_line(self, card: TelegramCardState) -> str:
        cc = self._fmt_balance(card.balances.get("CC", Decimal("0")), 2)
        usdcx = self._fmt_balance(card.balances.get("USDCx", Decimal("0")), 4)
        cbtc = self._fmt_balance(card.balances.get("CBTC", Decimal("0")), 8)
        return f"CC: {cc} | USDCx: {usdcx} | CBTC: {cbtc}"

    def _build_swaps_line(self, card: TelegramCardState) -> str:
        parts = [f"Swaps: Total: {card.swap_transactions}"]
        for pair_key, target in card.pair_targets.items():
            completed = card.pair_completed.get(pair_key, 0)
            parts.append(f"{self._short_pair(pair_key)}: {completed}/{target}")
        return " | ".join(parts)

    def _build_reward_line(self, card: TelegramCardState) -> str:
        summary = card.activity_summary
        reward = self._metric_value(summary.reward_total if summary else None)
        volume = self._metric_value(
            summary.volume_usd if summary and summary.volume_usd is not None else (summary.total_volume if summary else None)
        )
        tx = self._metric_value(summary.tx_count if summary else None)
        rank = self._metric_value(summary.rank if summary else None)
        return f"Reward: {reward} | Volume: {volume} | Tx: {tx} | Rank: {rank}"

    def _build_delta_line(self, card: TelegramCardState) -> str:
        tx_delta = self._build_numeric_delta(
            card.activity_summary.tx_count if card.activity_summary else None,
            card.baseline_activity.tx_count if card.baseline_activity else None,
            fallback=Decimal(card.swap_transactions),
        )
        reward_delta = self._build_numeric_delta(
            card.activity_summary.reward_total if card.activity_summary else None,
            card.baseline_activity.reward_total if card.baseline_activity else None,
            fallback=None,
        )
        return f"Δtx: {tx_delta} | Δrew: {reward_delta}"

    def _build_numeric_delta(
        self,
        current_value: str | None,
        baseline_value: str | None,
        *,
        fallback: Decimal | None,
    ) -> str:
        current = self._to_decimal_like(current_value)
        baseline = self._to_decimal_like(baseline_value)
        if current is not None and baseline is not None:
            return self._format_delta_value(current - baseline)
        if fallback is not None:
            return self._format_delta_value(fallback)
        return "-"

    def _timestamped_log(self, message: str) -> str:
        now = datetime.now(timezone.utc)
        return f"{now.strftime('%H.%M.%S')} {message[:160]}"

    def _fmt_balance(self, value: Decimal, places: int) -> str:
        return f"{value:.{places}f}"

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
        for left, right in (
            ("CC", "USDCX"),
            ("USDCX", "CC"),
            ("CC", "CBTC"),
            ("CBTC", "CC"),
            ("USDCX", "CBTC"),
            ("CBTC", "USDCX"),
        ):
            token = f"{left} -> {right}"
            if token in upper:
                return f"{SYMBOL_SHORT.get(left, left)}→{SYMBOL_SHORT.get(right, right)}"
        return strategy_label

    def _to_decimal_like(self, value: str | None) -> Decimal | None:
        if value in {None, ""}:
            return None
        cleaned = re.sub(r"[^0-9.\-]+", "", value)
        if cleaned in {"", "-", ".", "-."}:
            return None
        try:
            return Decimal(cleaned)
        except InvalidOperation:
            return None

    def _format_delta_value(self, value: Decimal) -> str:
        normalized = format(value.normalize(), "f") if value != 0 else "0"
        return f"{'+' if value >= 0 else ''}{normalized}"

    def _metric_value(self, value: str | None) -> str:
        return value if value not in {None, ""} else "-"

    def _trim_message(self, text: str) -> str:
        if len(text) <= 3900:
            return text
        return text[:3600] + "\n<i>Message trimmed</i>"
