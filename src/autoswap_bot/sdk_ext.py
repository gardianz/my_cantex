from __future__ import annotations

import asyncio
import json
import re
from decimal import Decimal
from typing import Any

from cantex_sdk import (
    CantexAPIError,
    CantexError,
    CantexSDK,
    CantexTimeoutError,
    FundingEvent,
    InstrumentId,
    SwapExecutedEvent,
    SwapFailedEvent,
    SwapPendingEvent,
)


class ExtendedCantexSDK(CantexSDK):
    async def ensure_intent_trading_account(self) -> bool:
        admin = await self.get_account_admin()
        if admin.has_intent_account:
            return False
        await self.create_intent_trading_account()
        return True

    async def get_activity_payload(self) -> tuple[str | None, Any | None]:
        candidates = (
            "/v1/account/reward_activity",
        )
        for path in candidates:
            try:
                return path, await self._request("GET", path)  # type: ignore[attr-defined]
            except CantexAPIError as exc:
                if exc.status in {404, 405}:
                    continue
                raise
            except json.JSONDecodeError:
                continue
        return None, None

    async def get_trading_history_payload(self) -> tuple[str | None, Any | None]:
        candidates = (
            "/v1/history/trading",
        )
        for path in candidates:
            try:
                return path, await self._request("GET", path)  # type: ignore[attr-defined]
            except CantexAPIError as exc:
                if exc.status in {404, 405}:
                    continue
                raise
            except json.JSONDecodeError:
                continue
        return None, None

    async def get_funding_history_payload(self) -> tuple[str | None, Any | None]:
        candidates = (
            "/v1/history/funding",
        )
        for path in candidates:
            try:
                return path, await self._request("GET", path)  # type: ignore[attr-defined]
            except CantexAPIError as exc:
                if exc.status in {404, 405}:
                    continue
                raise
            except json.JSONDecodeError:
                continue
        return None, None

    async def get_account_admin_payload(self) -> dict[str, Any]:
        return await self._request("GET", "/v1/account/admin")  # type: ignore[attr-defined]

    async def detect_intent_signer_mismatch(self) -> str | None:
        intent_signer = getattr(self, "_intent_signer", None)
        if intent_signer is None:
            return None

        admin = await self.get_account_admin()
        if not admin.has_intent_account:
            return None

        local_candidates = {
            str(intent_signer.get_public_key_hex_der()).lower(),
            str(intent_signer.get_public_key_hex()).lower(),
        }

        try:
            payload = await self.get_account_admin_payload()
        except Exception:
            return None

        contracts = ((payload.get("party_id") or {}).get("contracts") or {})
        intent_account = contracts.get("pool_intent_account")
        if not isinstance(intent_account, dict):
            return None

        remote_keys = self._extract_hex_public_key_candidates(intent_account)
        if not remote_keys:
            return None
        if any(candidate in local_candidates for candidate in remote_keys):
            return None

        contract_id = intent_account.get("contract_id", "-")
        return (
            "Configured trading_key tidak cocok dengan intent account yang sudah ada "
            f"(contract_id={contract_id}). Gunakan CANTEX_TRADING_KEY yang benar untuk wallet ini."
        )

    async def swap_and_confirm(
        self,
        sell_amount: Decimal,
        sell_instrument: InstrumentId,
        buy_instrument: InstrumentId,
        *,
        timeout: float = 60.0,
    ) -> Any:
        base_method = getattr(super(), "swap_and_confirm", None)
        if not callable(base_method):
            raise RuntimeError(
                "cantex_sdk 4.0 tidak termuat dengan benar; method swap_and_confirm tidak tersedia"
            )
        return await base_method(
            sell_amount=sell_amount,
            sell_instrument=sell_instrument,
            buy_instrument=buy_instrument,
            timeout=timeout,
        )

    async def build_swap_intent_payload(
        self,
        *,
        sell_amount: Decimal,
        sell_instrument: InstrumentId,
        buy_instrument: InstrumentId,
    ) -> dict[str, Any]:
        if getattr(self, "_intent_signer", None) is None:
            raise RuntimeError("IntentTradingKeySigner required for build_swap_intent_payload")
        payload = await self._request(  # type: ignore[attr-defined]
            "POST",
            "/v1/intent/build/pool/swap",
            json_data={
                "sellAmount": str(sell_amount),
                "sellInstrumentId": sell_instrument.id,
                "sellInstrumentAdmin": sell_instrument.admin,
                "buyInstrumentId": buy_instrument.id,
                "buyInstrumentAdmin": buy_instrument.admin,
            },
        )
        if not isinstance(payload, dict):
            raise RuntimeError("Intent build response tidak valid")
        return payload

    async def submit_built_intent(self, build_payload: dict[str, Any]) -> dict[str, Any]:
        intent_signer = getattr(self, "_intent_signer", None)
        if intent_signer is None:
            raise RuntimeError("IntentTradingKeySigner required for submit_built_intent")
        build_id = build_payload.get("id")
        intent_payload = build_payload.get("intent") or {}
        digest = intent_payload.get("digest")
        if not build_id or not digest:
            raise RuntimeError("Intent build response tidak memiliki id/digest")
        signature_hex = intent_signer.sign_digest_hex(str(digest))
        response = await self._request(  # type: ignore[attr-defined]
            "POST",
            "/v1/intent/submit",
            json_data={
                "id": build_id,
                "intentTradingKeySignature": signature_hex,
            },
        )
        if not isinstance(response, dict):
            raise RuntimeError("Intent submit response tidak valid")
        return response

    async def open_private_ws(self) -> Any:
        return await self._ws_connect("/v1/ws/private", authenticated=True)  # type: ignore[attr-defined]

    async def wait_for_swap_confirmation(
        self,
        ws: Any,
        *,
        timeout: float,
    ) -> SwapExecutedEvent:
        async def _wait_for_confirmation() -> SwapExecutedEvent:
            async for event in ws:
                if isinstance(event, SwapPendingEvent):
                    continue
                if isinstance(event, SwapExecutedEvent):
                    return event
                if isinstance(event, SwapFailedEvent):
                    raise CantexError(f"Swap failed: {event.error}")
            raise CantexError("WebSocket closed before swap confirmation")

        try:
            return await asyncio.wait_for(_wait_for_confirmation(), timeout=timeout)
        except asyncio.TimeoutError:
            raise CantexTimeoutError(
                f"Swap confirmation timed out after {timeout}s"
            ) from None

    async def wait_for_swap_confirmation_with_funding_events(
        self,
        ws: Any,
        *,
        timeout: float,
        funding_grace_timeout: float = 3.0,
    ) -> tuple[SwapExecutedEvent, list[FundingEvent]]:
        funding_events: list[FundingEvent] = []

        async def _wait_for_execution() -> SwapExecutedEvent:
            async for event in ws:
                if isinstance(event, FundingEvent):
                    funding_events.append(event)
                    continue
                if isinstance(event, SwapPendingEvent):
                    continue
                if isinstance(event, SwapExecutedEvent):
                    return event
                if isinstance(event, SwapFailedEvent):
                    raise CantexError(f"Swap failed: {event.error}")
            raise CantexError("WebSocket closed before swap confirmation")

        async def _collect_funding_events() -> None:
            async for event in ws:
                if isinstance(event, FundingEvent):
                    funding_events.append(event)

        try:
            executed = await asyncio.wait_for(_wait_for_execution(), timeout=timeout)
            try:
                await asyncio.wait_for(
                    _collect_funding_events(),
                    timeout=max(0.1, funding_grace_timeout),
                )
            except asyncio.TimeoutError:
                pass
            return executed, funding_events
        except asyncio.TimeoutError:
            raise CantexTimeoutError(
                f"Swap confirmation timed out after {timeout}s"
            ) from None

    def _extract_hex_public_key_candidates(self, node: Any) -> set[str]:
        candidates: set[str] = set()

        def visit(value: Any, path: str) -> None:
            if isinstance(value, dict):
                for key, child in value.items():
                    next_path = f"{path}.{key}" if path else str(key)
                    visit(child, next_path)
                return
            if isinstance(value, list):
                for index, child in enumerate(value):
                    next_path = f"{path}[{index}]"
                    visit(child, next_path)
                return
            if not isinstance(value, str):
                return

            lowered_path = path.lower()
            compact = value.strip().lower()
            if "public" not in lowered_path and "key" not in lowered_path:
                return
            if len(compact) < 66:
                return
            if not re.fullmatch(r"[0-9a-f]+", compact):
                return
            candidates.add(compact)

        visit(node, "")
        return candidates
