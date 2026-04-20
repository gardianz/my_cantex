from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from .config import AccountConfig, BotConfig


@dataclass(frozen=True)
class RequiredCCEstimate:
    account_name: str
    strategy_label: str
    target_rounds: int
    reserve_fee: Decimal
    reserve_kritis: Decimal | None
    amount_cc_max: Decimal
    bootstrap_cc: Decimal
    fee_cap_cc: Decimal
    fee_hops_min: int
    fee_hops_safe: int
    fee_budget_min_cc: Decimal
    fee_budget_safe_cc: Decimal
    recovery_buffer_cc: Decimal
    estimated_min_cc: Decimal
    estimated_safe_cc: Decimal
    assumptions: tuple[str, ...]


def estimate_required_cc(config: BotConfig) -> tuple[RequiredCCEstimate, ...]:
    fee_cap = config.runtime.max_network_fee_cc_per_execution or Decimal("0")
    estimates: list[RequiredCCEstimate] = []
    for account in config.accounts:
        cc_amount_range = account.amount_ranges.get("CC")
        amount_cc_max = cc_amount_range.max_value if cc_amount_range is not None else Decimal("0")
        target_rounds = account.rounds_range.max_value
        fee_hops_min = 1
        fee_hops_safe = _safe_hops_per_round(account)
        bootstrap_cc = _bootstrap_cc(account, amount_cc_max)
        recovery_buffer_cc = fee_cap * Decimal(_recovery_buffer_hops(account))
        fee_budget_min_cc = fee_cap * Decimal(target_rounds * fee_hops_min)
        fee_budget_safe_cc = fee_cap * Decimal(target_rounds * fee_hops_safe)
        estimated_min_cc = account.reserve_fee + bootstrap_cc + fee_budget_min_cc
        estimated_safe_cc = (
            account.reserve_fee
            + bootstrap_cc
            + fee_budget_safe_cc
            + recovery_buffer_cc
        )

        assumptions = _assumptions_for_account(
            account=account,
            amount_cc_max=amount_cc_max,
            fee_cap=fee_cap,
            fee_hops_safe=fee_hops_safe,
        )
        estimates.append(
            RequiredCCEstimate(
                account_name=account.name,
                strategy_label=account.strategy().label,
                target_rounds=target_rounds,
                reserve_fee=account.reserve_fee,
                reserve_kritis=account.reserve_kritis,
                amount_cc_max=amount_cc_max,
                bootstrap_cc=bootstrap_cc,
                fee_cap_cc=fee_cap,
                fee_hops_min=fee_hops_min,
                fee_hops_safe=fee_hops_safe,
                fee_budget_min_cc=fee_budget_min_cc,
                fee_budget_safe_cc=fee_budget_safe_cc,
                recovery_buffer_cc=recovery_buffer_cc,
                estimated_min_cc=estimated_min_cc,
                estimated_safe_cc=estimated_safe_cc,
                assumptions=assumptions,
            )
        )
    return tuple(estimates)


def render_required_cc_report(config: BotConfig) -> str:
    estimates = estimate_required_cc(config)
    if not estimates:
        return "Tidak ada account aktif untuk dihitung."

    total_min = sum((estimate.estimated_min_cc for estimate in estimates), Decimal("0"))
    total_safe = sum((estimate.estimated_safe_cc for estimate in estimates), Decimal("0"))
    lines = [
        "Estimasi kebutuhan CC dari config saat ini",
        (
            "Dasar hitung: rounds.max + reserve_fee + reserve_kritis + amounts.CC.max + "
            "strategy + max_network_fee_cc_per_execution"
        ),
        (
            "Catatan: endpoint history trading yang saat ini bot pakai belum memberi "
            "breakdown fee terpisah per trade, jadi estimasi ini memakai fee cap config, "
            "bukan fee real dari history."
        ),
        (
            "Catatan: benefit free swap harian tidak dikurangkan, jadi angka di bawah cenderung "
            "konservatif."
        ),
        (
            f"Total semua account | estimasi minimum={_format_decimal(total_min)} CC | "
            f"estimasi aman={_format_decimal(total_safe)} CC"
        ),
        "",
    ]
    for estimate in estimates:
        lines.append(
            (
                f"- {estimate.account_name} | {estimate.strategy_label} | "
                f"target={estimate.target_rounds} round"
            )
        )
        lines.append(
            (
                f"  reserve_fee={_format_decimal(estimate.reserve_fee)} | "
                f"reserve_kritis={_format_decimal_or_dash(estimate.reserve_kritis)} | "
                f"amounts.CC.max={_format_decimal(estimate.amount_cc_max)} | "
                f"bootstrap={_format_decimal(estimate.bootstrap_cc)}"
            )
        )
        lines.append(
            (
                f"  fee_cap={_format_decimal(estimate.fee_cap_cc)} | "
                f"fee_budget_min={_format_decimal(estimate.fee_budget_min_cc)} | "
                f"fee_budget_aman={_format_decimal(estimate.fee_budget_safe_cc)} | "
                f"buffer_recovery={_format_decimal(estimate.recovery_buffer_cc)}"
            )
        )
        lines.append(
            (
                f"  estimasi_min={_format_decimal(estimate.estimated_min_cc)} CC | "
                f"estimasi_aman={_format_decimal(estimate.estimated_safe_cc)} CC"
            )
        )
        if estimate.assumptions:
            lines.append(f"  asumsi: {'; '.join(estimate.assumptions)}")
        lines.append("")
    return "\n".join(lines).rstrip()


def _bootstrap_cc(account: AccountConfig, amount_cc_max: Decimal) -> Decimal:
    strategy_key = account.strategy().key
    if strategy_key == "strategy_3_cycle":
        return amount_cc_max * Decimal("2")
    return amount_cc_max


def _safe_hops_per_round(account: AccountConfig) -> int:
    strategy_key = account.strategy().key
    if strategy_key in {"strategy_3_cycle", "strategy_4_reserve"}:
        return 2
    return 1


def _recovery_buffer_hops(account: AccountConfig) -> int:
    strategy_key = account.strategy().key
    if strategy_key == "strategy_3_cycle":
        return 2
    if strategy_key == "strategy_4_reserve":
        return 2
    return 1


def _assumptions_for_account(
    *,
    account: AccountConfig,
    amount_cc_max: Decimal,
    fee_cap: Decimal,
    fee_hops_safe: int,
) -> tuple[str, ...]:
    assumptions: list[str] = [
        f"target round memakai nilai max config ({account.rounds_range.max_value})",
    ]
    if amount_cc_max <= 0:
        assumptions.append("amounts.CC tidak ada, bootstrap modal CC dianggap 0")
    if fee_cap <= 0:
        assumptions.append(
            "max_network_fee_cc_per_execution belum diisi, komponen fee dianggap 0 sehingga hasil bisa terlalu rendah"
        )

    strategy_key = account.strategy().key
    if strategy_key == "strategy_1":
        assumptions.append("bootstrap mengasumsikan 1 seed CC -> USDCx lalu modal berputar lewat USDCx -> CC")
    elif strategy_key == "strategy_2":
        assumptions.append("bootstrap mengasumsikan 1 seed CC -> CBTC lalu modal berputar lewat CBTC -> CC")
    elif strategy_key == "strategy_3_cycle":
        assumptions.append("bootstrap mengasumsikan seed 2 sisi: CC -> USDCx dan CC -> CBTC")
        assumptions.append(f"estimasi aman mengasumsikan sampai {fee_hops_safe} hop saat swap token <-> token")
    elif strategy_key == "strategy_4_reserve":
        assumptions.append("bootstrap mengasumsikan top-up awal 1x CC -> USDCx sesuai amounts.CC")
        assumptions.append(
            f"reserve_kritis={_format_decimal_or_dash(account.reserve_kritis)} hanya trigger recovery, bukan tambahan modal di atas reserve_fee"
        )
        assumptions.append(f"estimasi aman mengasumsikan sampai {fee_hops_safe} hop saat swap USDCx <-> CBTC")
    return tuple(assumptions)


def _format_decimal(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _format_decimal_or_dash(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return _format_decimal(value)
