from __future__ import annotations

import asyncio
from collections import defaultdict
from decimal import Decimal

from cantex_sdk import CantexAPIError, InstrumentId, SwapQuote

from .constants import CC_SYMBOL, TRACKED_SYMBOLS
from .models import RouteHop, RoutePlan


class RouteOptimizer:
    def __init__(
        self,
        sdk,
        instruments_by_symbol: dict[str, InstrumentId],
        *,
        route_mode: str = "auto",
    ) -> None:
        self._sdk = sdk
        self._instruments_by_symbol = instruments_by_symbol
        self._route_mode = route_mode

    async def choose_best_route(
        self,
        sell_symbol: str,
        buy_symbol: str,
        sell_amount: Decimal,
    ) -> RoutePlan:
        candidate_paths = [(sell_symbol, buy_symbol)]
        if self._route_mode == "auto":
            for intermediate in TRACKED_SYMBOLS:
                if intermediate in {sell_symbol, buy_symbol}:
                    continue
                if intermediate in self._instruments_by_symbol:
                    candidate_paths.append((sell_symbol, intermediate, buy_symbol))

        results = await asyncio.gather(
            *(self._quote_path(path, sell_amount) for path in candidate_paths),
            return_exceptions=True,
        )
        valid_routes = [result for result in results if isinstance(result, RoutePlan)]
        if not valid_routes:
            for result in results:
                if isinstance(result, Exception):
                    raise result
            raise RuntimeError(f"Tidak ada route valid untuk {sell_symbol} -> {buy_symbol}")

        return max(
            valid_routes,
            key=lambda route: (
                route.final_amount,
                -route.total_network_fee_cc,
                -route.tx_count,
            ),
        )

    async def _quote_path(
        self,
        path_symbols: tuple[str, ...],
        initial_sell_amount: Decimal,
    ) -> RoutePlan:
        hops: list[RouteHop] = []
        current_amount = initial_sell_amount
        network_fee_by_symbol: defaultdict[str, Decimal] = defaultdict(Decimal)
        admin_and_liquidity_by_symbol: defaultdict[str, Decimal] = defaultdict(Decimal)
        total_network_fee_cc = Decimal("0")

        for hop_index in range(len(path_symbols) - 1):
            sell_symbol = path_symbols[hop_index]
            buy_symbol = path_symbols[hop_index + 1]
            quote = await self._get_quote(sell_symbol, buy_symbol, current_amount)
            network_fee_symbol = self._symbol_from_instrument(quote.fees.network_fee.instrument)
            fee_symbol = self._symbol_from_instrument(quote.fees.instrument)

            hop = RouteHop(
                sell_symbol=sell_symbol,
                buy_symbol=buy_symbol,
                sell_amount=current_amount,
                returned_amount=quote.returned_amount,
                fee_percentage=quote.fees.fee_percentage,
                admin_fee_amount=quote.fees.amount_admin,
                liquidity_fee_amount=quote.fees.amount_liquidity,
                fee_symbol=fee_symbol,
                network_fee_amount=quote.fees.network_fee.amount,
                network_fee_symbol=network_fee_symbol,
                estimated_time_seconds=quote.estimated_time_seconds,
                slippage=quote.prices.slippage,
                raw_quote=quote,
            )
            hops.append(hop)

            network_fee_by_symbol[network_fee_symbol] += hop.network_fee_amount
            admin_and_liquidity_by_symbol[fee_symbol] += (
                hop.admin_fee_amount + hop.liquidity_fee_amount
            )
            if network_fee_symbol == CC_SYMBOL:
                total_network_fee_cc += hop.network_fee_amount
            current_amount = quote.returned_amount

        return RoutePlan(
            path_symbols=path_symbols,
            hops=tuple(hops),
            final_amount=current_amount,
            total_network_fee_cc=total_network_fee_cc,
            total_network_fee_by_symbol=dict(network_fee_by_symbol),
            total_admin_and_liquidity_by_symbol=dict(admin_and_liquidity_by_symbol),
        )

    async def _get_quote(
        self,
        sell_symbol: str,
        buy_symbol: str,
        sell_amount: Decimal,
    ) -> SwapQuote:
        if sell_amount <= 0:
            raise ValueError(f"Sell amount harus > 0 untuk {sell_symbol} -> {buy_symbol}")
        try:
            return await self._sdk.get_swap_quote(
                sell_amount=sell_amount,
                sell_instrument=self._instruments_by_symbol[sell_symbol],
                buy_instrument=self._instruments_by_symbol[buy_symbol],
            )
        except CantexAPIError as exc:
            raise RuntimeError(
                f"Quote gagal untuk {sell_symbol} -> {buy_symbol}: HTTP {exc.status} {exc.body[:200]}"
            ) from exc

    def _symbol_from_instrument(self, instrument: InstrumentId) -> str:
        for symbol, candidate in self._instruments_by_symbol.items():
            if candidate == instrument:
                return symbol
        return instrument.id
