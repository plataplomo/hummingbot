from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

import aiohttp
from pydantic import ValidationError

from cyberdelta.apis.base.authenticator_interface import AuthenticatedRequestComponents
from cyberdelta.apis.base.exchange_api import ExchangeAPI, MessageHandler
from cyberdelta.apis.hyperliquid.hl_auth import HyperliquidEip712Authenticator
from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper
from cyberdelta.apis.hyperliquid.hl_mapper import (
    HyperliquidCandleMapper,
    HyperliquidMapper,
    HyperliquidOrderMapper,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import (
    HyperliquidRawCandleSnapshot,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeResponse,
    HyperliquidRawExchangeStatusObject,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_fill import HyperliquidRawFill
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetCtx,
    HyperliquidRawMetaAndAssetCtxsResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOpenOrdersResponse,
    HyperliquidRawOrder,
    HyperliquidRawOrderStatusResponse,
    HyperliquidRawTriggerInfo,
    HyperliquidRawTriggerSpec,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_order import (
    HyperliquidRawLimitOrderTypeDetails,
    HyperliquidRawMarketOrderTypeDetails,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_transfer_withdrawal import (
    HyperliquidRawL2UsdTransferPayload,
    HyperliquidRawWithdrawalToL1ActionPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import HyperliquidRawClearinghouseState
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models import (
    DerivativePosition,
    FundingRate,
    Order,
    OrderBook,
    OrderSide,
    OrderType,
    SpotBalance,
    Ticker,
    TimeInForce,
    Trade,
)
from cyberdelta.core.models.enums import (
    OrderStatus,
)
from cyberdelta.core.models.market import Candle
from cyberdelta.utils.logging_config import get_logger

logger = get_logger(__name__)


class HyperliquidAPI(ExchangeAPI):
    """API Client for Hyperliquid DEX."""

    BASE_URL = "https://api.hyperliquid.xyz"
    INFO_URL = "https://info.hyperliquid.xyz"
    WS_URL = "wss://api.hyperliquid.xyz/ws"
    CHAIN_ID = 1337

    def __init__(self, api_config: dict[str, Any], secrets: dict[str, str | None]) -> None:
        """
        Initialize the HyperliquidAPI client.

        Args:
            api_config: Configuration dictionary with connection parameters
            secrets: Dictionary containing private_key and wallet_address
        """
        self.rest_endpoint = api_config.get("rest_endpoint", self.BASE_URL)
        self.ws_endpoint = api_config.get("ws_endpoint", self.WS_URL)

        self._wallet_address = secrets.get("wallet_address")
        private_key = secrets.get("private_key")

        self._hl_authenticator: HyperliquidEip712Authenticator | None = None
        if private_key and self._wallet_address:
            try:
                self._hl_authenticator = HyperliquidEip712Authenticator(
                    private_key_hex=private_key,
                    wallet_address=self._wallet_address,
                    chain_id=self.CHAIN_ID,
                )
            except ValueError as e:
                logger.error(f"Failed to init HL authenticator: {e}. Signed endpoints will fail.")
        elif not self._wallet_address:
            logger.error("HLAPI: Wallet address required, not provided. Most functionality fails.")
        else:
            logger.warning(
                "HLAPI: Private key not provided. Signed endpoints fail or use public data."
            )

        # Instantiate the error mapper
        self._hyperliquid_error_mapper = HyperliquidErrorMapper()

        self._asset_to_index_cache: dict[str, int] = {}

        super().__init__(
            exchange_name="hyperliquid",
            config={
                "rest_endpoint": self.rest_endpoint,
                "ws_endpoint": self.ws_endpoint,
                "rate_limits": api_config.get("rate_limits", {}),
                "request_timeout": api_config.get("request_timeout", 30.0),
                "ws_ping_interval": api_config.get("ws_ping_interval"),
                "ws_reconnect_delay": api_config.get("ws_reconnect_delay"),
                "ws_max_reconnect_attempts": api_config.get("ws_max_reconnect_attempts"),
                "ws_connection_timeout": api_config.get("ws_connection_timeout"),
            },
            secrets=secrets,
            authenticator=self._hl_authenticator,
            error_mapper=self._hyperliquid_error_mapper,
        )

        self.default_headers: dict[str, str] = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        self.trade_callback: MessageHandler | None = None
        self.order_update_callback: MessageHandler | None = None
        self.fill_callback: MessageHandler | None = None
        self.orderbook_callback: MessageHandler | None = None

        self.ws_connection: aiohttp.ClientWebSocketResponse | None = None
        self.ws_lock = asyncio.Lock()
        self._symbol_map: dict[str, str] = {}
        self._ws_handlers: dict[str, MessageHandler] = {}
        self._ws_subscriptions: dict[str, MessageHandler] = {}
        self._is_connected = False

    async def _authenticate(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Uses the HyperliquidEip712Authenticator to prepare request components."""
        if not self.authenticator:
            logger.error(
                f"[{self.exchange_name}] Attempt to call signed endpoint ({method} {path}) "
                "without configured HL authenticator."
            )
            raise APIError(
                "HL authenticator not initialized (e.g., missing/invalid private key).",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        if not isinstance(self.authenticator, HyperliquidEip712Authenticator):
            logger.error(
                f"[{self.exchange_name}] Incorrect auth type for HL: {type(self.authenticator)}"
            )
            raise APIError(
                "Incorrect authenticator type for Hyperliquid.",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        current_headers = self.default_headers.copy()

        auth_components: AuthenticatedRequestComponents = await self.authenticator.prepare_request(
            method, path, params, data, current_headers
        )

        return {
            "headers": auth_components["headers"],
            "params": auth_components["params"],
            "data": auth_components["data"],
        }

    async def _get_asset_index(self, symbol: str) -> int:
        """Fetch or retrieve from cache the asset_index for a given symbol."""
        if symbol in self._asset_to_index_cache:
            return self._asset_to_index_cache[symbol]

        logger.debug(
            f"[{self.exchange_name}] Asset index for {symbol} not cached, fetching meta..."
        )
        response_raw: object = await self._request(
            "POST",
            f"{self.INFO_URL.rstrip('/')}/info",
        )

        try:
            validated_response = HyperliquidRawMetaAndAssetCtxsResponse.model_validate(response_raw)
            for index, asset_def in enumerate(validated_response.meta.universe):
                self._asset_to_index_cache[asset_def.name] = index

            if symbol in self._asset_to_index_cache:
                return self._asset_to_index_cache[symbol]
            else:
                logger.error(
                    f"[{self.exchange_name}] Asset index for {symbol} not found after fetch."
                )
                raise APIError(
                    f"Asset index for symbol '{symbol}' not found.",
                    code=APIErrorCode.SYMBOL_NOT_FOUND.value,
                )
        except ValidationError as e:
            logger.error(
                f"[{self.exchange_name}] Failed to validate metaAndAssetCtxs: {e}. Raw: {response_raw!r}"
            )
            raise APIError(
                "Failed to parse market metadata for asset index mapping.",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e
        except APIError:
            raise
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error fetching asset index for {symbol}: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error fetching asset index for {symbol}: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e

    def _construct_subscription_payload(self, topic: str) -> dict[str, Any] | None:
        """Constructs the subscription payload for a given topic for Hyperliquid.
        Hyperliquid uses a format like:
        {"method": "subscribe", "subscription": payload}
        Payload depends on the subscription type.
        """
        parts = topic.split(":")
        sub_type = parts[0]

        subscription_data: dict[str, Any] = {}

        if sub_type == "l2Book" and len(parts) > 1:
            coin = parts[1]
            subscription_data = {"type": "l2Book", "coin": coin}
        elif sub_type == "trades" and len(parts) > 1:
            coin = parts[1]
            subscription_data = {"type": "trades", "coin": coin}
        elif sub_type == "userEvents":
            if not self._wallet_address:
                logger.error(
                    f"[{self.exchange_name}] Cannot subscribe to userEvents without wallet address."
                )
                return None
            subscription_data = {"type": "userEvents", "user": self._wallet_address}
        # Add other subscription types as needed (e.g., candles, userFills, notifications)
        elif sub_type == "candle" and len(parts) > 2:
            coin = parts[1]
            interval = parts[2]
            subscription_data = {"type": "candle", "coin": coin, "interval": interval}
        else:
            logger.warning(
                f"[{self.exchange_name}] Unknown or invalid topic format for subscription: {topic}"
            )
            return None

        return {"method": "subscribe", "subscription": subscription_data}

    async def _handle_websocket_message(self, message: dict[str, Any]) -> None:
        """Handle raw WebSocket message from WebSocketManager, then route it."""
        # Hyperliquid structure is often {"channel": "...", "data": ...}
        # No special pre-processing needed before routing based on channel.
        await self._route_ws_message(message)

    async def _route_ws_message(self, message: dict[str, Any]) -> None:
        """Route incoming WebSocket messages based on Hyperliquid's 'channel' field."""
        channel = message.get("channel")
        data_payload = message.get("data")  # Corrected: Renamed from 'data' to 'data_payload'
        if not channel:
            logger.debug(f"[{self.exchange_name}] Received WS message without channel: {message}")
            return

        if data_payload is None:  # Should not happen if channel exists based on HL structure
            logger.debug(
                f"[{self.exchange_name}] Received message on channel '{channel}' but no data: {message}"
            )
            return

        handler = self._ws_handlers.get(channel)
        if handler:
            try:
                # Pass the inner 'data' payload to the handler
                await handler(data_payload)
            except Exception as e:
                logger.error(
                    f"[{self.exchange_name}] Error in handler for channel {channel}: {e}",
                    exc_info=True,
                )
        elif channel == "pong":  # Handle built-in pong responses
            logger.debug(f"[{self.exchange_name}] Received pong")
        elif channel == "error":
            logger.error(f"[{self.exchange_name}] Received WS error message: {data_payload}")
        elif channel == "subscriptionResponse":
            logger.info(f"[{self.exchange_name}] Received subscription response: {data_payload}")
            # Can add logic here to confirm subscription success/failure if needed
        else:
            logger.debug(f"[{self.exchange_name}] No handler registered for channel: {channel}")

    async def connect_websocket(self) -> None:
        """Establish the WebSocket connection using the base class logic."""
        await super().connect_websocket()  # Call the base method which uses WebSocketManager

    async def get_balances(self) -> dict[str, SpotBalance]:
        """Get account balances."""
        if not self._wallet_address:
            raise APIError(
                "Wallet address required for get_balances.",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )
        try:
            response: object = await self._request(
                "POST",
                f"{self.INFO_URL.rstrip('/')}/info",
            )
            validated_state = HyperliquidRawClearinghouseState.model_validate(response)
            return HyperliquidMapper.map_raw_clearinghouse_state_to_spot_balances(validated_state)
        except ValidationError as e:
            logger.error(
                f"[{self.exchange_name}] Error validating clearinghouseState for balances: {e}"
            )
            raise APIError(
                f"Failed to validate balance data structure: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e
        except APIError:
            raise
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error in get_balances: {e}", exc_info=True
            )
            raise APIError(
                f"Unexpected error fetching balances: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e

    async def get_positions(self, symbol: str | None = None) -> list[DerivativePosition]:
        """Get current positions."""
        if not self._wallet_address:
            raise APIError(
                "Wallet address required for get_positions.",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )
        try:
            response: object = await self._request(
                "POST",
                f"{self.INFO_URL.rstrip('/')}/info",
            )
            validated_state = HyperliquidRawClearinghouseState.model_validate(response)
            positions_dict = HyperliquidMapper.map_raw_clearinghouse_state_to_derivative_positions(
                validated_state
            )
            positions_list: list[DerivativePosition] = list(positions_dict.values())
            if symbol:
                positions_list = [p for p in positions_list if p.symbol == symbol]
            return positions_list
        except ValidationError as e:
            logger.error(
                f"[{self.exchange_name}] Error parsing/mapping user state for positions: {e}"
            )
            raise APIError(
                f"Failed to parse position data: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e
        except APIError:
            raise
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error getting positions: {e}", exc_info=True
            )
            raise APIError(
                f"Unexpected error getting positions: {e}",
                code=APIErrorCode.SERVER_ERROR.value,
                original_exception=e,
            ) from e

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Get open orders."""
        if not self._wallet_address:
            raise APIError(
                "Wallet address required for get_open_orders.",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )
        try:
            response: object = await self._request(
                "POST",
                f"{self.INFO_URL.rstrip('/')}/info",
            )
            validated: HyperliquidRawOpenOrdersResponse = (
                HyperliquidRawOpenOrdersResponse.model_validate(response)
            )
            open_orders: list[Order] = []
            for order_obj in validated.items:
                order_data: HyperliquidRawOrder = order_obj.order
                order_symbol: str = order_data.asset
                if symbol is None or order_symbol == symbol:
                    trigger_info: HyperliquidRawTriggerInfo | None = order_obj.trigger
                    mapped_order = HyperliquidOrderMapper.transform_raw_order_to_internal(
                        raw=order_data, trigger=trigger_info
                    )
                    if mapped_order and mapped_order.status in [
                        OrderStatus.OPEN,
                        OrderStatus.PARTIALLY_FILLED,
                        OrderStatus.NEW,
                    ]:
                        open_orders.append(mapped_order)
            return open_orders
        except APIError:
            raise
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Error getting open orders: {e}", exc_info=True)
            raise APIError(
                f"Failed to get open orders: {e}",
                code=APIErrorCode.SERVER_ERROR.value,
                original_exception=e,
            ) from e

    async def get_ticker(self, symbol: str) -> Ticker:
        """Get current ticker information for a symbol."""
        try:
            response: object = await self._request(
                "POST",
                f"{self.INFO_URL.rstrip('/')}/info",
            )
            validated = HyperliquidRawMetaAndAssetCtxsResponse.model_validate(response)
            for asset_ctx in validated.asset_ctxs:
                if asset_ctx.name == symbol:
                    return HyperliquidMapper.map_raw_ctx_to_ticker(asset_ctx)
            raise APIError(
                f"Ticker data not found for {symbol}", code=APIErrorCode.SYMBOL_NOT_FOUND.value
            )
        except APIError:
            raise
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Error getting ticker for {symbol}: {e}", exc_info=True
            )
            raise APIError(
                f"Failed to get ticker for {symbol}: {e}",
                code=APIErrorCode.SERVER_ERROR.value,
                original_exception=e,
            ) from e

    async def get_order_book(self, symbol: str, depth: int | None = None) -> OrderBook:
        """Get order book for a symbol."""
        try:
            response: object = await self._request(
                "POST",
                f"{self.INFO_URL.rstrip('/')}/info",
            )
            from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import HyperliquidRawL2Book

            validated = HyperliquidRawL2Book.model_validate(response)
            return HyperliquidMapper.map_raw_order_book(validated, depth)
        except APIError:
            raise
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Error getting order book for {symbol}: {e}", exc_info=True
            )
            raise APIError(
                f"Failed to get order book for {symbol}: {e}",
                code=APIErrorCode.SERVER_ERROR.value,
                original_exception=e,
            ) from e

    async def get_recent_trades(self, symbol: str, limit: int | None = None) -> list[Trade]:
        """Get recent trades for a symbol."""
        try:
            response: object = await self._request(
                "POST",
                f"{self.INFO_URL.rstrip('/')}/info",
            )
            from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import (
                HyperliquidRawRecentTradesResponse,
            )

            validated: HyperliquidRawRecentTradesResponse = (
                HyperliquidRawRecentTradesResponse.model_validate(response)
            )
            return HyperliquidMapper.map_raw_trades(validated.items, limit)
        except APIError:
            raise
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Error getting recent trades for {symbol}: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Failed to get recent trades for {symbol}: {e}",
                code=APIErrorCode.SERVER_ERROR.value,
                original_exception=e,
            ) from e

    async def get_funding_rate(self, symbol: str) -> FundingRate | None:
        """Get funding rate for a symbol."""
        try:
            response: object = await self._request(
                "POST",
                f"{self.INFO_URL.rstrip('/')}/info",
            )
            validated = HyperliquidRawMetaAndAssetCtxsResponse.model_validate(response)
            asset_ctx = next((ctx for ctx in validated.asset_ctxs if ctx.name == symbol), None)
            if asset_ctx is None:
                logger.warning(f"[{self.exchange_name}] No asset context found for {symbol}.")
                return None
            return HyperliquidMapper.map_raw_ctx_to_funding_rate(asset_ctx)
        except APIError:
            raise
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Error getting funding rate for {symbol}: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Failed to get funding rate for {symbol}: {e}",
                code=APIErrorCode.SERVER_ERROR.value,
                original_exception=e,
            ) from e

    async def transfer(
        self, asset: str, amount: Decimal, from_account: str, to_account: str
    ) -> dict[str, Any]:
        """Initiates an L2 USDC transfer on Hyperliquid."""
        if asset.upper() != "USDC":
            raise ValueError("Hyperliquid L2 transfers are currently only supported for USDC.")
        if not to_account:
            raise ValueError(
                "Destination address (to_account) is required for Hyperliquid L2 transfer."
            )

        transfer_action_payload = HyperliquidRawL2UsdTransferPayload(
            destination=to_account, token="USDC", amount=str(amount)
        )
        action_details = {
            "chain": "L2",
            "payload": transfer_action_payload.model_dump(by_alias=True),
        }
        request_data = {"type": "usdTransfer", "action": action_details}

        response_raw: object = None
        try:
            response_raw = await self._request(
                "POST", "/exchange", data=request_data, is_signed=True
            )
            validated_response = HyperliquidRawExchangeResponse.model_validate(response_raw)

            if not validated_response.data or not validated_response.data.statuses:
                logger.warning(
                    f"[{self.exchange_name}] L2 Transfer response 'ok' but data or statuses list is missing/empty. Raw: {response_raw!r}"
                )
                raise APIError(
                    "L2 Transfer 'ok' but no status details returned.",
                    code=APIErrorCode.UNKNOWN.value,
                )

            first_status_obj_raw = validated_response.data.statuses[0]
            if isinstance(first_status_obj_raw, str):
                if "error" in first_status_obj_raw.lower():
                    logger.warning(
                        f"[{self.exchange_name}] L2 Transfer failed with status: {first_status_obj_raw}"
                    )
                    raise APIError(
                        first_status_obj_raw,
                        code=APIErrorCode.EXCHANGE_SPECIFIC.value,
                        exchange_message=first_status_obj_raw,
                    )
                logger.info(f"[{self.exchange_name}] L2 Transfer status: {first_status_obj_raw}")
                return {"status": "success_with_info", "data": first_status_obj_raw}
            else:
                status_object = first_status_obj_raw
                if status_object.error:
                    logger.warning(
                        f"[{self.exchange_name}] L2 Transfer failed: {status_object.error}"
                    )
                    raise APIError(
                        status_object.error,
                        code=APIErrorCode.EXCHANGE_SPECIFIC.value,
                        exchange_message=status_object.error,
                    )
                elif status_object.filled:
                    logger.info(
                        f"[{self.exchange_name}] L2 Transfer resulted in fill-like status: {status_object.filled.model_dump()}"
                    )
                    return {"status": "success", "data": status_object.filled.model_dump()}
                elif status_object.resting:
                    logger.info(
                        f"[{self.exchange_name}] L2 Transfer resulted in resting-like status: {status_object.resting.model_dump()}"
                    )
                    return {"status": "success", "data": status_object.resting.model_dump()}
                else:
                    logger.warning(
                        f"[{self.exchange_name}] L2 Transfer status obj has no clear state: {status_object.model_dump()}"
                    )
                    return {"status": "success_unknown_details", "data": status_object.model_dump()}
        except ValidationError as e:
            logger.error(
                f"[{self.exchange_name}] Failed to validate L2 transfer response: {e}. Raw: {response_raw!r}"
            )
            raise APIError(
                f"Invalid response after L2 transfer: {e}", code=APIErrorCode.UNKNOWN.value
            ) from e
        except APIError:
            raise
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error during L2 transfer: {e}", exc_info=True
            )
            raise APIError(
                f"Unexpected error during L2 transfer: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e

    async def withdraw(
        self, asset: str, amount: Decimal, address: str, network: str | None = None
    ) -> dict[str, Any]:
        """Initiates a withdrawal to L1 on Hyperliquid."""
        if not address:
            raise ValueError("Destination address is required for withdrawal.")

        action_type: str
        action_payload_dict: dict[str, Any]
        if asset.upper() == "ETH":
            action_type = "withdrawEth"
            action_payload_dict = {"amount": str(amount), "destination": address}
        else:
            action_type = "withdraw"
            withdrawal_payload = HyperliquidRawWithdrawalToL1ActionPayload(
                token=asset.upper(), amount=str(amount), destination=address
            )
            action_payload_dict = withdrawal_payload.model_dump()

        request_data = {"type": action_type, "action": action_payload_dict}
        response_raw: object = None
        try:
            response_raw = await self._request(
                "POST", "/exchange", data=request_data, is_signed=True
            )
            validated_response = HyperliquidRawExchangeResponse.model_validate(response_raw)

            if not validated_response.data or not validated_response.data.statuses:
                logger.warning(
                    f"[{self.exchange_name}] Withdraw response 'ok' but data or statuses list is missing/empty. Raw: {response_raw!r}"
                )
                raise APIError(
                    "Withdrawal status unclear: 'ok' but no status details provided.",
                    code=APIErrorCode.EXCHANGE_SPECIFIC.value,
                )

            first_status_obj_raw = validated_response.data.statuses[0]
            if isinstance(first_status_obj_raw, str):
                if "error" in first_status_obj_raw.lower():
                    logger.warning(
                        f"[{self.exchange_name}] Withdraw failed with status string: {first_status_obj_raw}"
                    )
                    raise APIError(
                        first_status_obj_raw,
                        code=APIErrorCode.EXCHANGE_SPECIFIC.value,
                        exchange_message=first_status_obj_raw,
                    )
                logger.info(
                    f"[{self.exchange_name}] Withdraw status (string): {first_status_obj_raw}"
                )
                return {
                    "status": "success_with_info",
                    "message": first_status_obj_raw,
                    "tx_hash": None,
                }
            else:
                status_object = first_status_obj_raw
                if status_object.error:
                    logger.warning(f"[{self.exchange_name}] Withdraw failed: {status_object.error}")
                    raise APIError(
                        status_object.error,
                        code=APIErrorCode.EXCHANGE_SPECIFIC.value,
                        exchange_message=status_object.error,
                    )
                elif status_object.withdrawal_submitted:
                    tx_hash = status_object.withdrawal_submitted
                    logger.info(
                        f"[{self.exchange_name}] Withdrawal for {asset} successful. TxHash: {tx_hash}"
                    )
                    return {"status": "success", "tx_hash": tx_hash}
                elif status_object.success:
                    logger.info(
                        f"[{self.exchange_name}] Withdraw successful with message: {status_object.success}"
                    )
                    return {
                        "status": "success_with_info",
                        "message": status_object.success,
                        "tx_hash": None,
                    }
                else:
                    logger.warning(
                        f"[{self.exchange_name}] Withdraw status 'ok' but unrecognized obj structure: {status_object.model_dump_json()!r}. Raw: {response_raw!r}"
                    )
                    raise APIError(
                        "Withdrawal status unclear: Unrecognized success object structure.",
                        code=APIErrorCode.EXCHANGE_SPECIFIC.value,
                    )
        except ValidationError as e:
            logger.error(
                f"[{self.exchange_name}] Validation error processing withdraw response: {e}. Raw: {response_raw!r}"
            )
            raise APIError(
                f"Failed to validate withdraw response: {e}",
                code=APIErrorCode.EXCHANGE_SPECIFIC.value,
                original_exception=e,
            ) from e
        except APIError:
            raise
        except Exception as e:
            logger.exception(
                f"[{self.exchange_name}] Unexpected error during withdraw for {asset} to {address}: {e}. Raw: {response_raw!r}"
            )
            raise APIError(
                f"Unexpected error during withdraw: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e

    async def get_order(self, order_id: str, symbol: str | None = None) -> Order | None:
        """Fetch a single order by its ID."""
        try:
            return await self.get_order_status(order_id=order_id, symbol=symbol)
        except APIError as e:
            if e.code == APIErrorCode.ORDER_NOT_FOUND.value:
                logger.debug(f"[{self.exchange_name}] Order {order_id} not found (get_order).")
                return None
            logger.error(f"[{self.exchange_name}] API error fetching order {order_id}: {e}")
            raise
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error fetching order {order_id}: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error fetching order {order_id}: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e

    async def cancel_all_orders(self, symbol: str | None = None) -> None:
        """Cancel all open orders, optionally filtering by symbol."""
        logger.info(
            f"[{self.exchange_name}] Attempting to cancel all orders for symbol: {symbol or 'all'}"
        )
        try:
            open_orders = await self.get_open_orders(symbol=symbol)
            if not open_orders:
                logger.info(
                    f"[{self.exchange_name}] No open orders found for {symbol or 'all'} to cancel."
                )
                return
            logger.info(f"[{self.exchange_name}] Found {len(open_orders)} open orders to cancel.")
            cancelled_count = 0
            failed_count = 0
            for order_to_cancel in open_orders:
                try:
                    if order_to_cancel.exchange_order_id and order_to_cancel.symbol:
                        logger.debug(
                            f"[{self.exchange_name}] Cancelling order {order_to_cancel.exchange_order_id} for {order_to_cancel.symbol}"
                        )
                        await self.cancel_order(
                            order_id=order_to_cancel.exchange_order_id,
                            symbol=order_to_cancel.symbol,
                        )
                        cancelled_count += 1
                        await asyncio.sleep(0.1)
                    else:
                        logger.warning(
                            f"[{self.exchange_name}] Skipping order cancel due to missing ID/symbol: {order_to_cancel}"
                        )
                        failed_count += 1
                except APIError as e_cancel:
                    logger.error(
                        f"[{self.exchange_name}] Failed to cancel order {order_to_cancel.exchange_order_id}: {e_cancel}"
                    )
                    failed_count += 1
                except Exception as e_unexp_cancel:
                    logger.error(
                        f"[{self.exchange_name}] Unexpected error cancelling order {order_to_cancel.exchange_order_id}: {e_unexp_cancel}"
                    )
                    failed_count += 1
            logger.info(
                f"[{self.exchange_name}] Cancellation summary for {symbol or 'all'}: {cancelled_count} succeeded, {failed_count} failed."
            )
        except APIError as e_get_orders:
            logger.error(
                f"[{self.exchange_name}] API Error fetching open orders to cancel: {e_get_orders}"
            )
            raise
        except Exception as e_overall:
            logger.error(
                f"[{self.exchange_name}] Unexpected error during cancel_all_orders for {symbol or 'all'}: {e_overall}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error during cancel_all_orders: {e_overall}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_overall,
            ) from e_overall

    async def get_order_history(
        self,
        symbol: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int | None = None,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> list[Order]:
        """Fetches historical orders from Hyperliquid."""
        start_time_ms = int(start_time.timestamp() * 1000) if start_time else 0
        end_time_ms = int(end_time.timestamp() * 1000) if end_time else int(time.time() * 1000)

        request_body_payload = {
            "type": "queryOrderHistory",
            "user": self._wallet_address,  # queryOrderHistory requires user
            "startTime": start_time_ms,
            "endTime": end_time_ms,
        }

        response_raw: object = None
        orders_list: list[Order] = []
        try:
            response_raw = await self._request(
                "POST",
                f"{self.INFO_URL.rstrip('/')}/info",  # Path for ExchangeAPI._request with its HttpClient should be just "/info"
                # and if HLAPI has a separate HttpClient for INFO_URL or handles full URLs.
                # For now, assume it might be passing full URL to a generic _request.
                # Or, the specific HLAPI _request was meant to handle this.
                # With refactor, self._request in HLAPI is ExchangeAPI._request.
                # ExchangeAPI._request expects a path for its _http_client.
                # Suggests HLAPI needs to manage calls to INFO_URL carefully.
                # Alt: HLAPI specific method for INFO_URL with own HttpClient.
                # This fix addresses using start_time_ms and end_time_ms.
                # URL strategy is a broader issue.
                data=request_body_payload,  # Pass the payload containing times
            )
            if not isinstance(response_raw, list):
                raise APIError(
                    f"Invalid response type for queryOrderHistory: expected list, got {type(response_raw)}",
                    code=APIErrorCode.UNKNOWN.value,
                )

            for order_data_raw in response_raw:
                try:
                    raw_order = HyperliquidRawOrder.model_validate(order_data_raw)
                    trigger_info_raw = order_data_raw.get("trigger")
                    trigger_info = (
                        HyperliquidRawTriggerInfo.model_validate(trigger_info_raw)
                        if isinstance(trigger_info_raw, dict)
                        else None
                    )
                    internal_order = HyperliquidOrderMapper.transform_raw_order_to_internal(
                        raw=raw_order, trigger=trigger_info
                    )
                    if internal_order:
                        orders_list.append(internal_order)
                except (ValidationError, ValueError) as e_item:
                    logger.warning(
                        f"[{self.exchange_name}] Skipping order history item due to validation/transform error: {e_item}. Raw: {order_data_raw}"
                    )
                    continue

            if symbol:
                orders_list = [o for o in orders_list if o.symbol == symbol]
            if order_id:
                orders_list = [o for o in orders_list if o.exchange_order_id == order_id]
            if client_order_id:
                orders_list = [o for o in orders_list if o.client_order_id == client_order_id]
            if limit is not None and limit > 0:
                orders_list = orders_list[:limit]
            return orders_list
        except APIError:
            raise
        except (ValidationError, ValueError) as e_outer:
            logger.error(
                f"[{self.exchange_name}] Error processing order history response: {e_outer}. Raw: {response_raw!r}",
                exc_info=True,
            )
            raise APIError(
                f"Failed to process order history response: {e_outer}",
                code=APIErrorCode.UNKNOWN.value,
            ) from e_outer
        except Exception as e_unexp:
            logger.error(
                f"[{self.exchange_name}] Unexpected error getting order history: {e_unexp}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error getting order history: {e_unexp}",
                code=APIErrorCode.UNKNOWN.value,
            ) from e_unexp

    async def get_trade_history(self, symbol: str | None = None, limit: int = 100) -> list[Trade]:
        """Fetch user trade history (fills)."""
        if not self._wallet_address:
            raise APIError(
                "Wallet address required for fetching trade history",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )
        response_raw: object = None
        try:
            response_raw = await self._request(
                "POST",
                f"{self.INFO_URL.rstrip('/')}/info",
            )
            if not isinstance(response_raw, list):
                logger.warning(
                    f"[{self.exchange_name}] Unexpected userFills response type: {type(response_raw)}. Expected list. Empty list."
                )
                return []
            trades: list[Trade] = []
            for fill_data_raw in response_raw:
                if not isinstance(fill_data_raw, dict):
                    logger.warning(f"Skipping non-dict item in userFills list: {fill_data_raw}")
                    continue
                try:
                    raw_fill = HyperliquidRawFill.model_validate(fill_data_raw)
                    if symbol is not None and raw_fill.coin != symbol:
                        continue
                    internal_trade = HyperliquidMapper.transform_raw_fill_to_internal(raw_fill)
                    trades.append(internal_trade)
                except (ValidationError, ValueError) as e_item:
                    logger.warning(
                        f"[{self.exchange_name}] Skipping fill due to validation/transform error: {e_item}. Data: {fill_data_raw}"
                    )
                    continue
            trades.sort(key=lambda t: t.executed_at, reverse=True)
            return trades[:limit]
        except APIError:
            raise
        except Exception as e_unexp:
            logger.error(
                f"[{self.exchange_name}] Unexpected error getting trade history: {e_unexp}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error getting trade history: {e_unexp}",
                code=APIErrorCode.UNKNOWN.value,
            ) from e_unexp

    async def get_funding_rates(self, symbols: list[str] | None = None) -> list[FundingRate]:
        """Get current funding rates."""
        try:
            response_raw: object = await self._request(
                "POST",
                f"{self.INFO_URL.rstrip('/')}/info",
            )
            if (
                not isinstance(response_raw, list)
                or len(response_raw) != 2
                or not isinstance(response_raw[0], dict)
                or not isinstance(response_raw[1], list)
            ):
                raise APIError(
                    f"Unexpected response structure for allMeta: {type(response_raw)}, len {len(response_raw) if isinstance(response_raw, list) else 'N/A'}",
                    code=APIErrorCode.UNKNOWN.value,
                )

            asset_ctx_list_raw: list[Any] = response_raw[1]
            result: list[FundingRate] = []
            for asset_ctx_raw in asset_ctx_list_raw:
                if not isinstance(asset_ctx_raw, dict):
                    logger.warning(
                        f"[{self.exchange_name}] Skipping non-dict asset_ctx in allMeta response: {asset_ctx_raw!r}"
                    )
                    continue
                try:
                    validated_asset_ctx = HyperliquidRawAssetCtx.model_validate(asset_ctx_raw)
                    market_symbol: str = validated_asset_ctx.name
                    if symbols is not None and market_symbol not in symbols:
                        continue
                    funding_rate = HyperliquidMapper.map_raw_ctx_to_funding_rate(
                        validated_asset_ctx
                    )
                    if funding_rate:
                        result.append(funding_rate)
                except ValidationError as ve_ctx:
                    logger.warning(
                        f"[{self.exchange_name}] Failed to validate asset_ctx for funding: {ve_ctx}. Data: {asset_ctx_raw!r}"
                    )
                except Exception as e_map_ctx:
                    logger.error(
                        f"[{self.exchange_name}] Error mapping asset_ctx to funding rate: {e_map_ctx}. Data: {asset_ctx_raw!r}",
                        exc_info=True,
                    )
            return result
        except APIError:
            raise
        except Exception as e_unexp:
            logger.error(
                f"[{self.exchange_name}] Unexpected error getting funding rates: {e_unexp}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error getting funding rates: {e_unexp}",
                code=APIErrorCode.SERVER_ERROR.value,
                original_exception=e_unexp,
            ) from e_unexp

    async def get_market_data(self, symbol: str, timeframe: str, limit: int = 100) -> list[Candle]:
        """Fetches historical market data (candlesticks)."""
        request_body_payload = {
            "type": "candleSnapshot",
            "req": {
                "coin": symbol.upper(),
                "interval": timeframe,
                "startTime": 0,  # Using 0 for startTime to get recent data up to endTime
                "endTime": int(time.time() * 1000),
            },
        }
        try:
            # Assuming INFO_URL might need to be handled by a specific HttpClient instance or a helper
            # that can target a different base URL than the default ExchangeAPI rest_endpoint.
            # For now, if self._request is the base ExchangeAPI._request, then `endpoint` needs to be a path.
            # If HyperliquidAPI needs to call a different base URL (INFO_URL) for this,
            # it should not use `super()._request` or `self._request` directly without ensuring
            # the correct base URL is used by HttpClient.
            # This might involve having a dedicated HttpClient for info.hyperliquid.xyz
            # or a mechanism in the base _request or HttpClient to accept a full URL
            # or a base URL override.
            # For this fix, I am focusing on passing the data payload correctly.
            # The URL part: f"{self.INFO_URL.rstrip('/')}/info" suggests the intent
            # to call the info endpoint.
            # If ExchangeAPI._request is used, and it prepends its own base URL, this would be wrong.
            # This is a structural concern noted. For the payload itself:
            raw_response = await self._request(
                method="POST",
                # This forms a full URL. HttpClient needs to handle it, or this call is wrong.
                endpoint=f"{self.INFO_URL.rstrip('/')}/info",
                data=request_body_payload,
            )
            if raw_response is None:
                raise APIError(
                    f"No response for candle snapshot {symbol} {timeframe}",
                    code=APIErrorCode.TIMEOUT.value,
                )
            if not isinstance(raw_response, dict):
                raise APIError(
                    f"Unexpected response format for candle snapshot: {type(raw_response)}",
                    code=APIErrorCode.EXCHANGE_SPECIFIC.value,
                )

            raw_snapshot = HyperliquidRawCandleSnapshot.model_validate(raw_response)
            internal_candles = HyperliquidCandleMapper.map(raw_snapshot, symbol, timeframe)
            if limit > 0 and len(internal_candles) > limit:
                internal_candles = internal_candles[-limit:]
            return internal_candles
        except ValidationError as e_val:
            logger.error(f"Pydantic validation error for {symbol} candles: {e_val}")
            raise APIError(
                "Failed to validate candle data from exchange",
                code=APIErrorCode.EXCHANGE_SPECIFIC.value,
                original_exception=e_val,
            ) from e_val
        except APIError:
            raise
        except Exception as e_unexp:
            logger.error(f"Error fetching or processing {symbol} candles: {e_unexp}", exc_info=True)
            raise APIError(
                f"An unexpected error occurred while fetching candles for {symbol}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unexp,
            ) from e_unexp

    async def place_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        time_in_force: TimeInForce,
        price: Decimal | None = None,
        stop_price: Decimal | None = None,
        client_order_id: str | None = None,
        reduce_only: bool = False,
        post_only: bool = False,
    ) -> Order:
        """Place an order on Hyperliquid."""
        asset_index = await self._get_asset_index(symbol)
        is_buy = side == OrderSide.BUY
        sz_str = str(quantity)
        underlying_hl_order_type_dict: dict[str, Any] = {}
        underlying_limit_px_str: str = "0"
        trigger_payload: dict[str, Any] | None = None
        tif_map: dict[TimeInForce, Literal["Gtc", "Ioc", "Alo"]] = {
            TimeInForce.GTC: "Gtc",
            TimeInForce.IOC: "Ioc",
            TimeInForce.ALO: "Alo",
        }
        raw_tif_str_candidate = tif_map.get(time_in_force)
        if post_only and order_type in [
            OrderType.LIMIT,
            OrderType.STOP_LIMIT,
            OrderType.TAKE_PROFIT_LIMIT,
        ]:
            raw_tif_str_candidate = "Alo"
        if raw_tif_str_candidate is None:
            logger.warning(
                f"[{self.exchange_name}] Unmapped TIF '{time_in_force}' "
                f"and post_only='{post_only}', defaulting Gtc."
            )
            raw_tif_str_candidate = "Gtc"

        effective_tif: Literal["Gtc", "Ioc", "Alo"]
        if raw_tif_str_candidate == "Gtc":
            effective_tif = "Gtc"
        elif raw_tif_str_candidate == "Ioc":
            effective_tif = "Ioc"
        elif raw_tif_str_candidate == "Alo":
            effective_tif = "Alo"
        else:
            raise AssertionError(
                f"Internal TIF logic error: unexpected raw_tif_str_candidate "
                f"'{raw_tif_str_candidate}'."
            )

        if order_type == OrderType.MARKET:
            underlying_hl_order_type_dict = {
                "market": HyperliquidRawMarketOrderTypeDetails().model_dump()
            }
            underlying_limit_px_str = "0"
        elif order_type == OrderType.LIMIT:
            if price is None:
                raise ValueError("Price is required for LIMIT orders.")
            underlying_limit_px_str = str(price)
            underlying_hl_order_type_dict = {
                "limit": HyperliquidRawLimitOrderTypeDetails(tif=effective_tif).model_dump()
            }
        elif order_type in [OrderType.STOP_MARKET, OrderType.TAKE_PROFIT_MARKET]:
            if stop_price is None:
                raise ValueError(f"stop_price is required for {order_type.value} orders.")
            underlying_limit_px_str = "0"
            trigger_details = HyperliquidRawTriggerSpec(
                triggerPx=str(stop_price),
                isMarket=True,
                tpsl="sl" if order_type == OrderType.STOP_MARKET else "tp",
            )
            trigger_payload = trigger_details.model_dump(by_alias=True)
        elif order_type in [OrderType.STOP_LIMIT, OrderType.TAKE_PROFIT_LIMIT]:
            if price is None:
                raise ValueError(f"price (for triggered limit) is required for {order_type.value}.")
            if stop_price is None:
                raise ValueError(f"stop_price is required for {order_type.value} orders.")
            underlying_limit_px_str = str(price)
            underlying_hl_order_type_dict = {
                "limit": HyperliquidRawLimitOrderTypeDetails(tif=effective_tif).model_dump()
            }
            trigger_details = HyperliquidRawTriggerSpec(
                triggerPx=str(stop_price),
                isMarket=False,
                tpsl="sl" if order_type == OrderType.STOP_LIMIT else "tp",
            )
            trigger_payload = trigger_details.model_dump(by_alias=True)
        else:
            raise NotImplementedError(f"Order type {order_type.value} is not supported.")

        action_payload: dict[str, Any] = {
            "asset": asset_index,
            "isBuy": is_buy,
            "sz": sz_str,
            "limitPx": underlying_limit_px_str,
            "orderType": underlying_hl_order_type_dict,
            "reduceOnly": reduce_only,
        }
        if client_order_id:
            action_payload["cloid"] = client_order_id
        if trigger_payload:
            action_payload["trigger"] = trigger_payload
        request_data = {"type": "order", "actions": [action_payload]}

        response_raw: object = None
        try:
            response_raw = await self._request(
                "POST", "/exchange", data=request_data, is_signed=True
            )
            validated_response = HyperliquidRawExchangeResponse.model_validate(response_raw)

            if (
                validated_response.status != "ok"
                or not validated_response.data
                or not validated_response.data.statuses
            ):
                mapped_error = self.error_mapper.map_exchange_error(
                    status_code=200,
                    error_body=str(response_raw),
                    error_data=validated_response.model_dump() if validated_response else None,
                    request_path="/exchange",
                )
                raise mapped_error

            first_status_obj_raw = validated_response.data.statuses[0]
            error_to_raise_from_status: APIError | None = None
            first_status_obj: HyperliquidRawExchangeStatusObject | None = None

            if isinstance(first_status_obj_raw, str):
                log_msg = (
                    f"[{self.exchange_name}] Order placement returned string status: "
                    f"{first_status_obj_raw}"
                )
                logger.warning(log_msg)
                err_code = APIErrorCode.ORDER_REJECTED.value
                if (
                    "LiquidationLimitOrderTooLargeError" in first_status_obj_raw
                    or "liquidation order too large" in first_status_obj_raw.lower()
                ):
                    err_code = APIErrorCode.INSUFFICIENT_FUNDS.value
                error_to_raise_from_status = APIError(
                    f"Order placement failed: {first_status_obj_raw}",
                    code=err_code,
                    exchange_message=first_status_obj_raw,
                )
            else:
                first_status_obj = first_status_obj_raw

            if error_to_raise_from_status:
                raise error_to_raise_from_status
            if first_status_obj is None:
                raise APIError(
                    "Internal error processing order status after placement.",
                    code=APIErrorCode.UNKNOWN.value,
                )

            order_id_to_fetch: int | None = None
            log_message_prefix = "Order placement reported"
            if resting_details := first_status_obj.resting:
                order_id_to_fetch, log_message_prefix = (
                    resting_details.oid,
                    f"Order OID:{resting_details.oid} resting",
                )
            elif filled_details := first_status_obj.filled:
                order_id_to_fetch, log_message_prefix = (
                    filled_details.oid,
                    (
                        f"Order OID:{filled_details.oid} filled (avgPx: {filled_details.avg_px}, "
                        f"sz: {filled_details.total_sz})"
                    ),
                )
            elif error_msg := first_status_obj.error:
                err_code_obj = APIErrorCode.ORDER_REJECTED.value
                if "LiquidationLimitOrderTooLargeError" in error_msg:
                    err_code_obj = APIErrorCode.INSUFFICIENT_FUNDS.value
                raise APIError(
                    f"Order placement failed: {error_msg}",
                    code=err_code_obj,
                    exchange_message=error_msg,
                )

            if order_id_to_fetch is not None:
                logger.info(
                    f"[{self.exchange_name}] {log_message_prefix}. Fetching canonical status."
                )
                await asyncio.sleep(0.2)
                try:
                    return await self.get_order_status(
                        order_id=str(order_id_to_fetch), symbol=symbol
                    )
                except APIError as e_fetch:
                    logger.error(
                        f"[{self.exchange_name}] {log_message_prefix}, "
                        f"but failed to fetch canonical status: {e_fetch}"
                    )
                    raise APIError(
                        f"{log_message_prefix}, but failed to retrieve final status: "
                        f"{e_fetch.message}",
                        code=APIErrorCode.UNKNOWN.value,
                        original_exception=e_fetch,
                    ) from e_fetch
            else:
                raise APIError(
                    f"Order placement status unclear, no OID/error: "
                    f"{first_status_obj.model_dump() if first_status_obj else 'N/A'}",
                    code=APIErrorCode.UNKNOWN.value,
                )
        except ValidationError as e_val_outer:
            logger.error(
                f"[{self.exchange_name}] Failed to validate order placement response: "
                f"{e_val_outer}. Raw: {response_raw!r}"
            )
            raise APIError(
                f"Invalid response after placing order: {e_val_outer}",
                code=APIErrorCode.UNKNOWN.value,
            ) from e_val_outer
        except APIError:
            raise
        except Exception as e_unexp_place:
            logger.error(
                f"[{self.exchange_name}] Unexpected error placing order: {e_unexp_place}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error placing order: {e_unexp_place}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unexp_place,
            ) from e_unexp_place

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> bool:
        """Cancel an existing order."""
        if not symbol:
            raise ValueError("Symbol is required to cancel Hyperliquid orders")
        asset_index = await self._get_asset_index(symbol)
        response_raw: object = None
        try:
            action_payload = {"asset": asset_index, "oid": int(order_id)}
            request_data = {"type": "cancel", "action": action_payload}
            response_raw = await self._request(
                "POST", "/exchange", data=request_data, is_signed=True
            )
            validated_response = HyperliquidRawExchangeResponse.model_validate(response_raw)

            if (
                validated_response.status != "ok"
                or not validated_response.data
                or not validated_response.data.statuses
            ):
                raise APIError(
                    f"Order cancel failed on exchange: "
                    f"{validated_response.model_dump() if validated_response else 'Invalid response'}",
                    code=APIErrorCode.ORDER_REJECTED.value,
                )

            if validated_response.data.statuses[0] == "canceled":
                logger.info(f"Successfully cancelled order {order_id} for asset {asset_index}")
                return True
            else:
                error_message = (
                    f"Cancellation status unclear or failed: {validated_response.data.statuses[0]}"
                )
                if (
                    isinstance(
                        validated_response.data.statuses[0], HyperliquidRawExchangeStatusObject
                    )
                    and validated_response.data.statuses[0].error
                ):
                    error_message = f"Cancel failed: {validated_response.data.statuses[0].error}"
                    raise APIError(error_message, code=APIErrorCode.ORDER_REJECTED.value)
                logger.warning(error_message)
                return True
        except (ValidationError, ValueError) as e_val_cancel:
            logger.error(
                f"[{self.exchange_name}] Failed to validate cancel response or invalid OID: "
                f"{e_val_cancel}. Raw: {response_raw!r}"
            )
            raise APIError(
                f"Invalid response/OID for cancel order {order_id}: {e_val_cancel}",
                code=APIErrorCode.UNKNOWN.value,
            ) from e_val_cancel
        except APIError:
            raise
        except Exception as e_unexp_cancel:
            logger.error(
                f"[{self.exchange_name}] Unexpected error canceling order {order_id}: "
                f"{e_unexp_cancel}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error canceling order {order_id}: {e_unexp_cancel}",
                code=APIErrorCode.UNKNOWN.value,
            ) from e_unexp_cancel

    async def get_recent_fills(
        self, symbol: str | None = None, limit: int | None = None
    ) -> list[Trade]:
        """Fetch recent fills/trades for the account."""
        effective_limit = limit if limit is not None else 100
        return await self.get_trade_history(symbol=symbol, limit=effective_limit)

    async def get_all_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Fetch all open orders, optionally filtering by symbol."""
        return await self.get_open_orders(symbol=symbol)

    async def ping_websocket(self) -> None:
        await super().ping_websocket()

    # Add implementations for ExchangeAPI abstract WS methods by calling super
    async def subscribe(self, topic: str, handler: MessageHandler) -> None:
        logger.info(
            f"[{self.exchange_name}] Subscribe called for topic: {topic}. Delegating to base."
        )
        await super().subscribe(topic, handler)

    async def _on_ws_connected(self) -> None:
        logger.info(
            f"[{self.exchange_name}] WebSocket connected. Triggering resubscription via base."
        )
        await super()._on_ws_connected()

    async def _resubscribe(self) -> None:
        logger.info(f"[{self.exchange_name}] Resubscribe called. Delegating to base.")
        await super()._resubscribe()

    def _update_rate_limit_from_headers(
        self, headers: Mapping[str, str], method: str, path: str
    ) -> None:
        """Hyperliquid does not typically provide rate limit info in headers."""
        pass

    async def get_order_status(
        self, order_id: str, symbol: str | None = None, client_order_id: str | None = None
    ) -> Order:
        """Fetches the status of a specific order."""
        if not self._wallet_address:
            raise APIError(
                "Wallet address required for fetching order status",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )
        payload = {"type": "orderStatus", "user": self._wallet_address, "oid": int(order_id)}
        try:
            response_data = await self._request(
                "POST",
                f"{self.INFO_URL.rstrip('/')}/info",
            )
            if not response_data or not isinstance(response_data, list) or not response_data:
                raise APIError(
                    f"Order not found (empty/invalid response): id={order_id}",
                    code=APIErrorCode.ORDER_NOT_FOUND.value,
                )

            status_part_raw = response_data[0]
            if isinstance(status_part_raw, str):
                if status_part_raw.lower() == "order not found":
                    raise APIError(
                        f"Order not found: id={order_id}", code=APIErrorCode.ORDER_NOT_FOUND.value
                    )
                raise APIError(
                    f"Unexpected string from get_order_status: {status_part_raw}",
                    code=APIErrorCode.EXCHANGE_SPECIFIC.value,
                )
            if not isinstance(status_part_raw, dict):
                raise APIError(
                    f"Unexpected status object format, expected dict: {status_part_raw}",
                    code=APIErrorCode.UNKNOWN.value,
                )

            validated_status_response = HyperliquidRawOrderStatusResponse.model_validate(
                status_part_raw
            )
            return HyperliquidOrderMapper.transform_raw_order_to_internal(
                raw=validated_status_response.order, trigger=None
            )
        except APIError:
            raise
        except ValidationError as e_val:
            logger.error(
                f"[{self.exchange_name}] Validation error in get_order_status: {e_val}. "
                f"Payload: {payload!r}"
            )
            raise APIError(
                "Pydantic validation error processing order status.",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_val,
            ) from e_val
        except Exception as e_unexp:
            logger.error(
                f"[{self.exchange_name}] Unexpected error in get_order_status for oid {order_id}: "
                f"{e_unexp}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error fetching order status: {e_unexp}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unexp,
            ) from e_unexp
