from __future__ import annotations

import json
from typing import Any

from cantex_sdk import CantexAPIError, CantexSDK


class ExtendedCantexSDK(CantexSDK):
    async def ensure_intent_trading_account(self) -> bool:
        admin = await self.get_account_admin()
        if admin.has_intent_account:
            return False
        await self.create_intent_trading_account()
        return True

    async def get_activity_payload(self) -> tuple[str | None, Any | None]:
        candidates = (
            "/v1/account/activity",
            "/v1/account/reward_activity",
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

    async def get_trading_history_payload(self) -> tuple[str | None, Any | None]:
        candidates = (
            "/v1/history/trading",
            "/v1/account/activity",
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
