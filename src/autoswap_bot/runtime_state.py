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
    active_strategy_name: str = ""
    active_requested_rounds: int = 0
    active_completed_rounds: int = 0
    active_updated_at_utc: str = ""


@dataclass(frozen=True)
class DailyFreeFeeStatus:
    utc_date: str
    used: int
    remaining: int
    window_open: bool
    window_opens_at_utc: datetime


@dataclass(frozen=True)
class RoundSessionProgress:
    strategy_name: str
    requested_rounds: int
    completed_rounds: int
    resumed: bool


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

    def sync_daily_free_fee_swaps(
        self,
        account_name: str,
        used_swaps: int,
        *,
        exact: bool = False,
        now_utc: datetime | None = None,
    ) -> DailyFreeFeeStatus:
        self._load()
        normalized_now = self._normalize_now(now_utc)
        state = self._normalized_state(account_name, normalized_now, persist=False)
        capped_used = min(max(used_swaps, 0), DAILY_FREE_FEE_SWAP_LIMIT)
        if exact:
            should_save = capped_used != state.free_fee_swaps_used
            state.free_fee_swaps_used = capped_used
        else:
            should_save = capped_used > state.free_fee_swaps_used
            if should_save:
                state.free_fee_swaps_used = capped_used
        if should_save:
            self._accounts[account_name] = state
            self._save()
        return self.get_daily_free_fee_status(account_name, now_utc=normalized_now)

    def start_or_resume_round_session(
        self,
        account_name: str,
        *,
        strategy_name: str,
        requested_rounds: int,
        prefer_requested_rounds: bool = False,
        now_utc: datetime | None = None,
    ) -> RoundSessionProgress:
        self._load()
        normalized_now = self._normalize_now(now_utc)
        state = self._normalized_state(account_name, normalized_now, persist=False)
        stored_requested = max(int(state.active_requested_rounds), 0)
        stored_completed = min(max(int(state.active_completed_rounds), 0), stored_requested)
        normalized_requested = max(int(requested_rounds), 1)
        if (
            state.active_strategy_name == strategy_name
            and stored_requested > 0
            and stored_completed < stored_requested
        ):
            effective_requested = (
                normalized_requested
                if prefer_requested_rounds and normalized_requested != stored_requested
                else stored_requested
            )
            effective_completed = min(stored_completed, effective_requested)
            should_save = (
                effective_completed != state.active_completed_rounds
                or effective_requested != state.active_requested_rounds
            )
            state.active_requested_rounds = effective_requested
            state.active_completed_rounds = effective_completed
            state.active_updated_at_utc = normalized_now.isoformat()
            self._accounts[account_name] = state
            if should_save:
                self._save()
            return RoundSessionProgress(
                strategy_name=strategy_name,
                requested_rounds=effective_requested,
                completed_rounds=effective_completed,
                resumed=effective_completed > 0,
            )

        state.active_strategy_name = strategy_name
        state.active_requested_rounds = normalized_requested
        state.active_completed_rounds = 0
        state.active_updated_at_utc = normalized_now.isoformat()
        self._accounts[account_name] = state
        self._save()
        return RoundSessionProgress(
            strategy_name=strategy_name,
            requested_rounds=normalized_requested,
            completed_rounds=0,
            resumed=False,
        )

    def update_round_session_progress(
        self,
        account_name: str,
        *,
        strategy_name: str,
        requested_rounds: int,
        completed_rounds: int,
        now_utc: datetime | None = None,
    ) -> RoundSessionProgress:
        self._load()
        normalized_now = self._normalize_now(now_utc)
        state = self._normalized_state(account_name, normalized_now, persist=False)
        normalized_requested = max(int(requested_rounds), 1)
        normalized_completed = min(max(int(completed_rounds), 0), normalized_requested)
        should_save = (
            state.active_strategy_name != strategy_name
            or state.active_requested_rounds != normalized_requested
            or state.active_completed_rounds != normalized_completed
        )
        state.active_strategy_name = strategy_name
        state.active_requested_rounds = normalized_requested
        state.active_completed_rounds = normalized_completed
        state.active_updated_at_utc = normalized_now.isoformat()
        self._accounts[account_name] = state
        if should_save:
            self._save()
        return RoundSessionProgress(
            strategy_name=strategy_name,
            requested_rounds=normalized_requested,
            completed_rounds=normalized_completed,
            resumed=normalized_completed > 0,
        )

    def clear_round_session(
        self,
        account_name: str,
        *,
        now_utc: datetime | None = None,
    ) -> None:
        self._load()
        normalized_now = self._normalize_now(now_utc)
        state = self._normalized_state(account_name, normalized_now, persist=False)
        if (
            state.active_strategy_name in {"", None}
            and state.active_requested_rounds == 0
            and state.active_completed_rounds == 0
            and state.active_updated_at_utc in {"", None}
        ):
            return
        state.active_strategy_name = ""
        state.active_requested_rounds = 0
        state.active_completed_rounds = 0
        state.active_updated_at_utc = ""
        self._accounts[account_name] = state
        self._save()

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
                active_strategy_name=str(payload.get("active_strategy_name", "")),
                active_requested_rounds=max(int(payload.get("active_requested_rounds", 0)), 0),
                active_completed_rounds=max(int(payload.get("active_completed_rounds", 0)), 0),
                active_updated_at_utc=str(payload.get("active_updated_at_utc", "")),
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
            state = AccountRuntimeState(
                current_utc_date=today,
                free_fee_swaps_used=0,
                active_strategy_name=state.active_strategy_name,
                active_requested_rounds=state.active_requested_rounds,
                active_completed_rounds=state.active_completed_rounds,
                active_updated_at_utc=state.active_updated_at_utc,
            )
            self._accounts[account_name] = state
            if persist:
                self._save()
        else:
            self._accounts[account_name] = state
        return state

    def _save(self) -> None:
        payload = {
            "version": 2,
            "daily_free_fee_swap_limit": DAILY_FREE_FEE_SWAP_LIMIT,
            "window_hour_utc": DAILY_FREE_FEE_WINDOW_HOUR_UTC,
            "accounts": {
                account_name: {
                    "current_utc_date": state.current_utc_date,
                    "free_fee_swaps_used": state.free_fee_swaps_used,
                    "active_strategy_name": state.active_strategy_name,
                    "active_requested_rounds": state.active_requested_rounds,
                    "active_completed_rounds": state.active_completed_rounds,
                    "active_updated_at_utc": state.active_updated_at_utc,
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
