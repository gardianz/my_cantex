from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path


DAILY_FREE_FEE_SWAP_LIMIT = 3
DAILY_FREE_FEE_WINDOW_HOUR_UTC = 1


@dataclass
class AccountRuntimeState:
    current_utc_date: str = ""
    free_fee_swaps_used: int = 0


@dataclass(frozen=True)
class DailyFreeFeeStatus:
    utc_date: str
    used: int
    remaining: int
    window_open: bool
    window_opens_at_utc: datetime


class BotRuntimeStateStore:
    def __init__(self, path: Path, logger: logging.Logger | None = None) -> None:
        self.path = path
        self.log = logger or logging.getLogger("autoswap_bot.state")
        self._accounts: dict[str, AccountRuntimeState] = {}
        self._loaded = False

    def ensure_account(self, account_name: str, *, now_utc: datetime | None = None) -> DailyFreeFeeStatus:
        return self.get_daily_free_fee_status(account_name, now_utc=now_utc)

    def get_daily_free_fee_status(
        self,
        account_name: str,
        *,
        now_utc: datetime | None = None,
    ) -> DailyFreeFeeStatus:
        self._load()
        normalized_now = self._normalize_now(now_utc)
        state = self._normalized_state(account_name, normalized_now, persist=True)
        used = min(max(state.free_fee_swaps_used, 0), DAILY_FREE_FEE_SWAP_LIMIT)
        remaining = max(0, DAILY_FREE_FEE_SWAP_LIMIT - used)
        window_opens_at_utc = self._window_opens_at(normalized_now)
        return DailyFreeFeeStatus(
            utc_date=state.current_utc_date,
            used=used,
            remaining=remaining,
            window_open=normalized_now >= window_opens_at_utc,
            window_opens_at_utc=window_opens_at_utc,
        )

    def consume_daily_free_fee_swap(
        self,
        account_name: str,
        *,
        now_utc: datetime | None = None,
    ) -> DailyFreeFeeStatus:
        self._load()
        normalized_now = self._normalize_now(now_utc)
        state = self._normalized_state(account_name, normalized_now, persist=False)
        window_opens_at_utc = self._window_opens_at(normalized_now)
        if normalized_now >= window_opens_at_utc and state.free_fee_swaps_used < DAILY_FREE_FEE_SWAP_LIMIT:
            state.free_fee_swaps_used += 1
            self._accounts[account_name] = state
            self._save()
        return self.get_daily_free_fee_status(account_name, now_utc=normalized_now)

    def _normalize_now(self, now_utc: datetime | None) -> datetime:
        return (now_utc or datetime.now(timezone.utc)).astimezone(timezone.utc)

    def _window_opens_at(self, now_utc: datetime) -> datetime:
        return datetime.combine(
            now_utc.date(),
            time(hour=DAILY_FREE_FEE_WINDOW_HOUR_UTC),
            tzinfo=timezone.utc,
        )

    def _load(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        if not self.path.exists():
            return
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception as exc:
            self.log.warning("Gagal membaca runtime state %s: %s", self.path, exc)
            return
        for account_name, payload in raw.get("accounts", {}).items():
            self._accounts[str(account_name)] = AccountRuntimeState(
                current_utc_date=str(payload.get("current_utc_date", "")),
                free_fee_swaps_used=max(int(payload.get("free_fee_swaps_used", 0)), 0),
            )

    def _normalized_state(
        self,
        account_name: str,
        now_utc: datetime,
        *,
        persist: bool,
    ) -> AccountRuntimeState:
        today = now_utc.date().isoformat()
        state = self._accounts.get(account_name, AccountRuntimeState())
        if state.current_utc_date != today:
            state = AccountRuntimeState(current_utc_date=today, free_fee_swaps_used=0)
            self._accounts[account_name] = state
            if persist:
                self._save()
        else:
            self._accounts[account_name] = state
        return state

    def _save(self) -> None:
        payload = {
            "version": 1,
            "daily_free_fee_swap_limit": DAILY_FREE_FEE_SWAP_LIMIT,
            "window_hour_utc": DAILY_FREE_FEE_WINDOW_HOUR_UTC,
            "accounts": {
                account_name: {
                    "current_utc_date": state.current_utc_date,
                    "free_fee_swaps_used": state.free_fee_swaps_used,
                }
                for account_name, state in sorted(self._accounts.items())
            },
        }
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = Path(f"{self.path}.tmp")
            temp_path.write_text(
                json.dumps(payload, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            temp_path.replace(self.path)
        except Exception as exc:
            self.log.warning("Gagal menyimpan runtime state %s: %s", self.path, exc)
