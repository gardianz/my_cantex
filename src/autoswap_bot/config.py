from __future__ import annotations

import os
import random
import re
import tomllib
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from .constants import DEFAULT_RESERVE_FEE, TRACKED_SYMBOLS, get_strategy_definition


def _to_decimal(value: Any, field_name: str) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception as exc:  # pragma: no cover - defensive conversion
        raise ValueError(f"Nilai '{field_name}' tidak valid: {value!r}") from exc


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_-]+", "-", value.strip()).strip("-").lower()
    return slug or "account"


def _read_secret(raw_value: str | None, raw_file: str | None, base_dir: Path, label: str) -> str:
    if raw_value:
        if raw_value.startswith("env:"):
            env_name = raw_value.split(":", 1)[1]
            env_value = os.getenv(env_name)
            if not env_value:
                raise ValueError(f"Environment variable '{env_name}' untuk {label} belum di-set")
            return env_value.strip()
        return raw_value.strip()

    if raw_file:
        file_path = Path(raw_file)
        if not file_path.is_absolute():
            file_path = (base_dir / file_path).resolve()
        if not file_path.exists():
            raise FileNotFoundError(f"File {label} tidak ditemukan: {file_path}")
        return file_path.read_text(encoding="utf-8").strip()

    raise ValueError(f"{label} wajib diisi")


def _read_optional_secret(
    raw_value: str | None,
    raw_file: str | None,
    base_dir: Path,
    label: str,
) -> str | None:
    if raw_value in {None, ""} and raw_file in {None, ""}:
        return None
    return _read_secret(raw_value, raw_file, base_dir, label)


@dataclass(frozen=True)
class IntRange:
    min_value: int
    max_value: int

    def sample(self, rng: random.Random) -> int:
        return rng.randint(self.min_value, self.max_value)


@dataclass(frozen=True)
class FloatRange:
    min_value: float
    max_value: float

    def sample(self, rng: random.Random) -> float:
        return rng.uniform(self.min_value, self.max_value)

    def describe(self) -> str:
        return f"{self.min_value}..{self.max_value}"


@dataclass(frozen=True)
class DecimalRange:
    min_value: Decimal
    max_value: Decimal

    def sample(self, rng: random.Random) -> Decimal:
        if self.min_value == self.max_value:
            return self.min_value
        fraction = Decimal(str(rng.random()))
        return self.min_value + ((self.max_value - self.min_value) * fraction)

    def describe(self) -> str:
        return f"{self.min_value}..{self.max_value}"


@dataclass(frozen=True)
class PreparedAccountRun:
    strategy_name: str
    rounds: int


def _parse_int_range(value: Any, field_name: str) -> IntRange:
    if isinstance(value, dict):
        min_value = int(value.get("min", value.get("max")))
        max_value = int(value.get("max", value.get("min")))
    else:
        min_value = max_value = int(value)
    if min_value < 1 or max_value < 1:
        raise ValueError(f"{field_name} minimal 1")
    if min_value > max_value:
        raise ValueError(f"{field_name}.min tidak boleh lebih besar dari .max")
    return IntRange(min_value=min_value, max_value=max_value)


def _parse_float_range(value: Any, field_name: str) -> FloatRange:
    if isinstance(value, dict):
        min_value = float(value.get("min", value.get("max")))
        max_value = float(value.get("max", value.get("min")))
    else:
        min_value = max_value = float(value)
    if min_value < 0 or max_value < 0:
        raise ValueError(f"{field_name} tidak boleh negatif")
    if min_value > max_value:
        raise ValueError(f"{field_name}.min tidak boleh lebih besar dari .max")
    return FloatRange(min_value=min_value, max_value=max_value)


def _parse_decimal_range(value: Any, field_name: str) -> DecimalRange:
    if isinstance(value, dict):
        min_value = _to_decimal(value.get("min", value.get("max")), f"{field_name}.min")
        max_value = _to_decimal(value.get("max", value.get("min")), f"{field_name}.max")
    else:
        min_value = max_value = _to_decimal(value, field_name)
    if min_value <= 0 or max_value <= 0:
        raise ValueError(f"{field_name} harus > 0")
    if min_value > max_value:
        raise ValueError(f"{field_name}.min tidak boleh lebih besar dari .max")
    return DecimalRange(min_value=min_value, max_value=max_value)


def _parse_optional_decimal(value: Any, field_name: str) -> Decimal | None:
    if value in {None, ""}:
        return None
    return _to_decimal(value, field_name)


def _normalize_full_24h_startup_mode(value: Any) -> str:
    mode = str(value).strip().lower()
    aliases = {
        "planned": "planned",
        "plan": "planned",
        "scheduled": "planned",
        "schedule": "planned",
        "direct": "direct",
        "immediate": "direct",
        "no_plan": "direct",
        "noplan": "direct",
    }
    normalized = aliases.get(mode)
    if normalized is None:
        raise ValueError(
            "settings.full_24h_startup_mode harus salah satu dari: planned, plan, scheduled, direct, immediate, no_plan"
        )
    return normalized


def _parse_amount_ranges(raw_amounts: Any, field_name: str) -> dict[str, DecimalRange]:
    if not isinstance(raw_amounts, dict):
        raise ValueError(f"{field_name} harus berupa table/map")

    amount_ranges: dict[str, DecimalRange] = {}
    for symbol in TRACKED_SYMBOLS:
        if symbol in raw_amounts:
            amount_ranges[symbol] = _parse_decimal_range(
                raw_amounts[symbol],
                f"{field_name}.{symbol}",
            )
    return amount_ranges


@dataclass(frozen=True)
class RuntimeConfig:
    base_url: str
    bot_state_file: Path
    execution_mode: str
    max_concurrency: int
    swap_delay_seconds_range: FloatRange
    max_network_fee_cc_per_execution: Decimal | None
    network_fee_poll_seconds_range: FloatRange
    full_24h_mode: bool
    full_24h_startup_mode: str
    full_24h_min_gap_minutes: float
    full_24h_auto_restart: bool
    weekly_refill_on_monday_utc: bool
    full_24h_schedule_log_limit: int
    random_seed: int | None
    telegram_enabled: bool
    telegram_bot_token: str | None
    telegram_chat_id: str | None
    telegram_state_file: Path
    terminal_dashboard_enabled: bool
    terminal_dashboard_logs_limit: int
    terminal_dashboard_min_interval_seconds: float
    telegram_update_min_interval_seconds: float
    telegram_latest_logs_limit: int
    activity_enabled: bool
    activity_items_limit: int
    dry_run: bool
    log_level: str
    route_mode: str
    default_continue_on_low_balance: bool
    max_retries: int
    retry_base_delay: float


@dataclass(frozen=True)
class AccountConfig:
    name: str
    enabled: bool
    operator_key: str
    trading_key: str
    strategy_name: str
    rounds_range: IntRange
    amount_ranges: dict[str, DecimalRange]
    reserve_fee: Decimal
    reserve_kritis: Decimal | None
    allow_continue_on_low_balance: bool
    auto_create_intent_account: bool
    key_slug: str
    display_index: int
    proxy_label: str

    def strategy(self):
        return get_strategy_definition(self.strategy_name)

    def amount_range_for_symbol(self, symbol: str) -> DecimalRange:
        try:
            amount_range = self.amount_ranges[symbol]
        except KeyError as exc:
            raise ValueError(
                f"Account '{self.name}' belum memiliki amount/range untuk simbol '{symbol}'"
            ) from exc
        return amount_range

    def prepare_run(self, rng: random.Random) -> PreparedAccountRun:
        rounds = self.rounds_range.sample(rng)
        return PreparedAccountRun(
            strategy_name=self.strategy_name,
            rounds=rounds,
        )

    def describe_amount_ranges(self) -> dict[str, str]:
        return {
            symbol: amount_range.describe()
            for symbol, amount_range in sorted(self.amount_ranges.items())
        }


@dataclass(frozen=True)
class BotConfig:
    runtime: RuntimeConfig
    accounts: tuple[AccountConfig, ...]
    config_path: Path


def load_config(path: str | Path) -> BotConfig:
    config_path = Path(path).resolve()
    try:
        raw = tomllib.loads(config_path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        raise ValueError(
            "Format TOML tidak valid di "
            f"{config_path}: {exc}. "
            "Biasanya ini terjadi karena ada key yang ditulis dua kali "
            "(misalnya `network_fee_poll_seconds`, `swap_delay_seconds`, atau field di `[settings]`)."
        ) from exc
    settings = raw.get("settings", {})
    defaults = raw.get("defaults", {})
    telegram_enabled = bool(settings.get("telegram_enabled", False))

    runtime = RuntimeConfig(
        base_url=str(settings.get("base_url", "https://api.cantex.io")).rstrip("/"),
        bot_state_file=(config_path.parent / ".autoswap_bot_runtime_state.json").resolve(),
        execution_mode=str(settings.get("execution_mode", "sequential")).lower(),
        max_concurrency=int(settings.get("max_concurrency", 1)),
        swap_delay_seconds_range=_parse_float_range(
            settings.get("swap_delay_seconds", 2.0),
            "settings.swap_delay_seconds",
        ),
        max_network_fee_cc_per_execution=(
            _to_decimal(
                settings.get("max_network_fee_cc_per_execution"),
                "settings.max_network_fee_cc_per_execution",
            )
            if settings.get("max_network_fee_cc_per_execution") not in {None, ""}
            else None
        ),
        network_fee_poll_seconds_range=_parse_float_range(
            settings.get("network_fee_poll_seconds", 30.0),
            "settings.network_fee_poll_seconds",
        ),
        full_24h_mode=True,
        full_24h_startup_mode=_normalize_full_24h_startup_mode(
            settings.get("full_24h_startup_mode", "planned")
        ),
        full_24h_min_gap_minutes=float(settings.get("full_24h_min_gap_minutes", 5.0)),
        full_24h_auto_restart=bool(settings.get("full_24h_auto_restart", False)),
        weekly_refill_on_monday_utc=bool(settings.get("weekly_refill_on_monday_utc", True)),
        full_24h_schedule_log_limit=int(settings.get("full_24h_schedule_log_limit", 12)),
        random_seed=(
            int(settings["random_seed"])
            if settings.get("random_seed") not in {None, ""}
            else None
        ),
        telegram_enabled=telegram_enabled,
        telegram_bot_token=(
            _read_optional_secret(
                settings.get("telegram_bot_token"),
                settings.get("telegram_bot_token_file"),
                config_path.parent,
                "settings.telegram_bot_token",
            )
            if telegram_enabled
            else None
        ),
        telegram_chat_id=(
            _read_optional_secret(
                settings.get("telegram_chat_id"),
                settings.get("telegram_chat_id_file"),
                config_path.parent,
                "settings.telegram_chat_id",
            )
            if telegram_enabled
            else None
        ),
        telegram_state_file=(config_path.parent / ".autoswap_telegram_state.json").resolve(),
        terminal_dashboard_enabled=bool(settings.get("terminal_dashboard_enabled", True)),
        terminal_dashboard_logs_limit=int(settings.get("terminal_dashboard_logs_limit", 20)),
        terminal_dashboard_min_interval_seconds=float(
            settings.get("terminal_dashboard_min_interval_seconds", 0.25)
        ),
        telegram_update_min_interval_seconds=float(
            settings.get("telegram_update_min_interval_seconds", 5.0)
        ),
        telegram_latest_logs_limit=int(settings.get("telegram_latest_logs_limit", 6)),
        activity_enabled=bool(settings.get("activity_enabled", True)),
        activity_items_limit=int(settings.get("activity_items_limit", 5)),
        dry_run=bool(settings.get("dry_run", False)),
        log_level=str(settings.get("log_level", "INFO")).upper(),
        route_mode=str(settings.get("route_mode", "auto")).lower(),
        default_continue_on_low_balance=bool(
            settings.get("default_continue_on_low_balance", False)
        ),
        max_retries=int(settings.get("max_retries", 3)),
        retry_base_delay=float(settings.get("retry_base_delay", 1.0)),
    )

    if runtime.execution_mode not in {"sequential", "concurrent"}:
        raise ValueError("settings.execution_mode harus 'sequential' atau 'concurrent'")
    if runtime.route_mode not in {"auto", "direct"}:
        raise ValueError("settings.route_mode harus 'auto' atau 'direct'")
    if runtime.max_concurrency < 1:
        raise ValueError("settings.max_concurrency minimal 1")
    if runtime.activity_items_limit < 1:
        raise ValueError("settings.activity_items_limit minimal 1")
    if (
        runtime.max_network_fee_cc_per_execution is not None
        and runtime.max_network_fee_cc_per_execution <= 0
    ):
        raise ValueError("settings.max_network_fee_cc_per_execution harus > 0")
    if runtime.network_fee_poll_seconds_range.min_value <= 0:
        raise ValueError("settings.network_fee_poll_seconds.min harus > 0")
    if runtime.network_fee_poll_seconds_range.max_value <= 0:
        raise ValueError("settings.network_fee_poll_seconds.max harus > 0")
    if runtime.network_fee_poll_seconds_range.min_value > runtime.network_fee_poll_seconds_range.max_value:
        raise ValueError(
            "settings.network_fee_poll_seconds.min tidak boleh lebih besar dari .max"
        )
    if runtime.full_24h_min_gap_minutes < 0:
        raise ValueError("settings.full_24h_min_gap_minutes tidak boleh negatif")
    if runtime.full_24h_schedule_log_limit < 1:
        raise ValueError("settings.full_24h_schedule_log_limit minimal 1")
    if runtime.telegram_update_min_interval_seconds < 0:
        raise ValueError("settings.telegram_update_min_interval_seconds tidak boleh negatif")
    if runtime.telegram_latest_logs_limit < 1:
        raise ValueError("settings.telegram_latest_logs_limit minimal 1")
    if runtime.terminal_dashboard_logs_limit < 1:
        raise ValueError("settings.terminal_dashboard_logs_limit minimal 1")
    if runtime.terminal_dashboard_min_interval_seconds < 0:
        raise ValueError("settings.terminal_dashboard_min_interval_seconds tidak boleh negatif")
    if runtime.telegram_enabled and (
        not runtime.telegram_bot_token or not runtime.telegram_chat_id
    ):
        raise ValueError(
            "settings.telegram_enabled=true membutuhkan telegram_bot_token dan telegram_chat_id"
        )

    default_strategy_name = str(defaults.get("strategy", "1"))
    get_strategy_definition(default_strategy_name)
    default_rounds_range = _parse_int_range(
        defaults.get("rounds", 1),
        "defaults.rounds",
    )
    default_amount_ranges = _parse_amount_ranges(
        defaults.get("amounts", {}),
        "defaults.amounts",
    )
    legacy_reserve_fee = _parse_optional_decimal(
        settings.get("min_cc_reserve"),
        "settings.min_cc_reserve",
    )
    default_reserve_fee = _to_decimal(
        defaults.get(
            "reserve_fee",
            legacy_reserve_fee if legacy_reserve_fee is not None else str(DEFAULT_RESERVE_FEE),
        ),
        "defaults.reserve_fee",
    )
    default_reserve_kritis = _parse_optional_decimal(
        defaults.get("reserve_kritis"),
        "defaults.reserve_kritis",
    )
    if default_reserve_fee <= 0:
        raise ValueError("defaults.reserve_fee harus > 0")

    accounts: list[AccountConfig] = []
    for index, raw_account in enumerate(raw.get("accounts", []), start=1):
        name = str(raw_account.get("name", f"account-{index}"))
        enabled = bool(raw_account.get("enabled", True))

        if not enabled:
            continue

        account_amount_ranges = dict(default_amount_ranges)
        account_amount_ranges.update(
            _parse_amount_ranges(
                raw_account.get("amounts", {}),
                f"accounts[{index}].amounts",
            )
        )

        account = AccountConfig(
            name=name,
            enabled=enabled,
            operator_key=_read_secret(
                raw_account.get("operator_key"),
                raw_account.get("operator_key_file"),
                config_path.parent,
                f"accounts[{index}].operator_key",
            ),
            trading_key=_read_secret(
                raw_account.get("trading_key"),
                raw_account.get("trading_key_file"),
                config_path.parent,
                f"accounts[{index}].trading_key",
            ),
            strategy_name=str(raw_account.get("strategy", default_strategy_name)),
            rounds_range=_parse_int_range(
                raw_account.get("rounds", {
                    "min": default_rounds_range.min_value,
                    "max": default_rounds_range.max_value,
                }),
                f"accounts[{index}].rounds",
            ),
            amount_ranges=account_amount_ranges,
            reserve_fee=_to_decimal(
                raw_account.get("reserve_fee", default_reserve_fee),
                f"accounts[{index}].reserve_fee",
            ),
            reserve_kritis=_parse_optional_decimal(
                raw_account.get("reserve_kritis", default_reserve_kritis),
                f"accounts[{index}].reserve_kritis",
            ),
            allow_continue_on_low_balance=bool(
                raw_account.get(
                    "allow_continue_on_low_balance",
                    runtime.default_continue_on_low_balance,
                )
            ),
            auto_create_intent_account=bool(
                raw_account.get("auto_create_intent_account", True)
            ),
            key_slug=_slugify(name),
            display_index=len(accounts) + 1,
            proxy_label=str(raw_account.get("proxy_label", "No proxy")),
        )
        strategy_definition = account.strategy()
        if account.reserve_fee <= 0:
            raise ValueError(f"accounts[{index}].reserve_fee harus > 0")
        if account.reserve_kritis is not None and account.reserve_kritis < 0:
            raise ValueError(f"accounts[{index}].reserve_kritis tidak boleh negatif")
        if (
            account.reserve_kritis is not None
            and account.reserve_kritis > account.reserve_fee
        ):
            raise ValueError(
                f"accounts[{index}].reserve_kritis tidak boleh lebih besar dari reserve_fee"
            )
        if strategy_definition.key == "strategy_4_reserve":
            if account.reserve_kritis is None:
                raise ValueError(
                    f"accounts[{index}] memakai strategi 4 sehingga reserve_kritis wajib diisi"
                )
        accounts.append(account)

    enabled_accounts = tuple(accounts)
    if not enabled_accounts:
        raise ValueError("Tidak ada account aktif pada konfigurasi")

    return BotConfig(runtime=runtime, accounts=enabled_accounts, config_path=config_path)
