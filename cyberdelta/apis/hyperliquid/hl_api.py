from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from typing import Any, cast

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
from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.hl_response_handler import (
    HyperliquidResponseHandler,
    RawJsonResponse,
)
from cyberdelta.apis.hyperliquid.hl_ws_raw_message_handler import HyperliquidWsRawMessageHandler
from cyberdelta.apis.hyperliquid.models.hl_processed_exchange_responses import (
    HyperliquidSuccessfulOrderStatus,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import (
    HyperliquidRawCandleSnapshot,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeResponse,
    HyperliquidRawExchangeStatusObject,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_fill import HyperliquidRawFill
from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import (
    HyperliquidRawHistoricalOrderResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetCtx,
    HyperliquidRawMetaAndAssetCtxsResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOpenOrdersResponse,
    HyperliquidRawOrder,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import (
    HyperliquidRawL2Book as HyperliquidRawOrderBookResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import (
    HyperliquidRawPublicTrade,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_fills import (
    HyperliquidRawUserFillsResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import HyperliquidRawClearinghouseState
from cyberdelta.apis.hyperliquid.models.hl_raw_ws_events import HyperliquidRawWsTradeEvent
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models import (
    DerivativePosition,
    FundingRate,
    MarginAccountSummary,
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
                    wallet_private_key=private_key,
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
        self._hl_mapper = HyperliquidMapper()
        self._hl_order_mapper = HyperliquidOrderMapper()
        self._hl_candle_mapper = HyperliquidCandleMapper()

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
        print(
            f"[DEBUG HL_API _authenticate] Entered. Authenticator: "
            f"{self.authenticator}, type: {type(self.authenticator)}"
        )  # DEBUG PRINT
        if not self.authenticator:
            logger.error(
                f"[{self.exchange_name}] Attempt to call signed endpoint ({method} {path}) "
                "without configured HL authenticator."
            )
            raise APIError(
                "HL authenticator not initialized (e.g., missing/invalid private key).",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        current_headers = self.default_headers.copy()

        # --- DEBUG PRINT --- #
        print(
            f"[DEBUG HL_API _authenticate] About to await prepare_request. "
            f"Authenticator: {self.authenticator}",
            flush=True,
        )
        # --- END DEBUG --- #

        auth_components: AuthenticatedRequestComponents = await self.authenticator.prepare_request(
            method, path, params, data, current_headers
        )

        # --- DEBUG PRINT --- #
        print("[DEBUG HL_API _authenticate] Finished awaiting prepare_request.", flush=True)
        # --- END DEBUG --- #

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
        # build_info_request_payload returns None, which is fine for _request
        request_payload_data = HyperliquidRequestBuilder.build_info_request_payload()
        response_raw: object = await self._request(
            "POST",
            f"{self.INFO_URL.rstrip('/')}/info",
            data=request_payload_data,  # This is already None or dict, remains as is
        )

        try:
            validated_response: HyperliquidRawMetaAndAssetCtxsResponse = (
                HyperliquidResponseHandler.handle_info_meta_and_asset_ctxs_response(
                    cast(RawJsonResponse, response_raw)
                )
            )
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
                f"[{self.exchange_name}] Failed to validate metaAndAssetCtxs: {e}. "
                f"Raw: {response_raw!r}"
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

    async def _route_ws_message(self, message: dict[str, Any]) -> None:
        """
        Route incoming WebSocket messages from Hyperliquid.
        Validates raw payloads using HyperliquidWsRawMessageHandler before processing.
        """
        channel: str | None = message.get("channel")
        raw_data: Any = message.get("data")

        if not channel:
            logger.debug(f"[{self.exchange_name}] Unroutable WS message (no channel): {message}")
            return

        topic_key_for_handler = channel
        if channel in ["l2Book", "trades"]:
            if isinstance(raw_data, dict):
                raw_data_dict = cast(dict[str, Any], raw_data)
                coin_from_data_any: Any = raw_data_dict.get("coin")
                if isinstance(coin_from_data_any, str):
                    topic_key_for_handler = f"{channel}:{coin_from_data_any}"
            else:
                logger.warning(
                    f"[{self.exchange_name}] Expected dict for '{channel}' data, "
                    f"got {type(raw_data)}. Msg: {message}"
                )
                return
        elif channel == "userEvents":
            topic_key_for_handler = "userEvents"

        app_handler = self._ws_handlers.get(topic_key_for_handler)
        if not app_handler:
            generic_app_handler = self._ws_handlers.get(channel)
            if generic_app_handler:
                app_handler = generic_app_handler
            else:
                logger.debug(
                    f"[{self.exchange_name}] No WS handler for '{topic_key_for_handler}'. "
                    f"Msg: {message}"
                )
                return

        if raw_data is None and channel not in ["pong", "subscriptionResponse"]:
            logger.warning(f"[{self.exchange_name}] WS '{channel}' has no data. Msg: {message}")
            return

        try:
            if channel == "l2Book":
                if not isinstance(raw_data, dict):
                    logger.warning(
                        f"[{self.exchange_name}] l2Book data not dict: {type(raw_data)}. "
                        f"Msg: {message}"
                    )
                    return
                validated_payload = HyperliquidWsRawMessageHandler.handle_l2book_payload(
                    cast(dict[str, Any], raw_data)
                )
                await app_handler(validated_payload.model_dump(mode="json"), message)

            elif channel == "trades":  # Public trades
                if not isinstance(raw_data, list):
                    logger.warning(
                        f"[{self.exchange_name}] Trades data not list: {type(raw_data)}. "
                        f"Msg: {message}"
                    )
                    return

                typed_trades_list: list[dict[str, Any]] = []
                for item_in_trades_list_any in raw_data:  # raw_data is list[Any] here
                    if not isinstance(item_in_trades_list_any, dict):
                        logger.warning(
                            f"[{self.exchange_name}] Trades list item not dict: "
                            f"{item_in_trades_list_any}. Msg: {message}"
                        )
                        return
                    item_as_dict = cast(dict[str, Any], item_in_trades_list_any)
                    typed_trades_list.append(item_as_dict)

                if not typed_trades_list and raw_data:
                    logger.warning(
                        f"[{self.exchange_name}] All items in trades list were invalid. "
                        f"Original: {raw_data}"
                    )
                    return

                validated_trades: list[HyperliquidRawWsTradeEvent] = (
                    HyperliquidWsRawMessageHandler.handle_public_trades_payload(typed_trades_list)
                )
                for trade_event in validated_trades:
                    await app_handler(trade_event.model_dump(mode="json"), message)

            elif channel == "userEvents":
                if not isinstance(raw_data, list):
                    logger.warning(
                        f"[{self.exchange_name}] userEvents data not list: {type(raw_data)}. "
                        f"Msg: {message}"
                    )
                    return

                for event_item_any in raw_data:  # raw_data is list[Any] here
                    if not isinstance(event_item_any, dict):
                        logger.warning(
                            f"[{self.exchange_name}] userEvents item not dict: "
                            f"{event_item_any}, skipping."
                        )
                        continue

                    event_item_dict = cast(dict[str, Any], event_item_any)
                    event_type_any: Any = event_item_dict.get("type")

                    if not isinstance(event_type_any, str):
                        logger.warning(
                            f"[{self.exchange_name}] userEvent item no str type: "
                            f"{event_item_dict}, skipping."
                        )
                        continue

                    event_type_str: str = event_type_any
                    payload_for_handler: dict[str, Any] = event_item_dict

                    if event_type_str == "fill":
                        try:
                            payload_for_handler = (
                                HyperliquidWsRawMessageHandler.handle_user_fill_event_payload(
                                    event_item_dict
                                )
                            ).model_dump(mode="json")
                        except (APIError, ValidationError) as e_fill_val:
                            logger.error(
                                f"[{self.exchange_name}] Error validating user fill event: "
                                f"{e_fill_val}. Event: {event_item_dict}. "
                                f"Passing raw dict to handler."
                            )
                    elif event_type_str == "order":
                        try:
                            order_wrapper = HyperliquidWsRawMessageHandler.handle_user_order_update_wrapper_payload(\
                                event_item_dict\
                            )
                            current_order_data = order_wrapper.data
                            if isinstance(current_order_data, HyperliquidRawOrder):\
                                payload_for_handler = (
                                    HyperliquidWsRawMessageHandler.handle_user_order_event_payload(
                                        current_order_data.model_dump(mode="json")
                                    )
                                ).model_dump(mode="json")
                            elif isinstance(current_order_data, list):
                                logger.info(
                                    f"[{self.exchange_name}] User 'order' event contains "
                                    f"list of fills. Passing wrapper for now. "
                                    f"Fills: {len(current_order_data)}"
                                )
                                payload_for_handler = order_wrapper.model_dump(mode="json")
                        except (APIError, ValidationError) as e_order_val:
                            logger.error(
                                f"[{self.exchange_name}] Error validating user order event: "
                                f"{e_order_val}. Event: {event_item_dict}. "
                                f"Passing raw dict to handler."
                            )
                    elif event_type_str == "positionUpdate":
                        try:
                            payload_for_handler = (
                                HyperliquidWsRawMessageHandler.handle_user_position_update_event_payload(
                                    event_item_dict
                                )
                            ).model_dump(mode="json")
                        except (APIError, ValidationError) as e_pos_val:
                            logger.error(
                                f"[{self.exchange_name}] Error validating positionUpdate event: "
                                f"{e_pos_val}. Event: {event_item_dict}. "
                                f"Passing raw dict to handler."
                            )
                    await app_handler(payload_for_handler, message)

            elif channel == "allMids":
                if not isinstance(raw_data, dict):
                    logger.warning(
                        f"[{self.exchange_name}] Expected dict for allMids data, "
                        f"got {type(raw_data)}. Msg: {message}"
                    )
                    return
                logger.debug(
                    f"[{self.exchange_name}] Forwarding raw allMids data for '{channel}'. "
                    f"Validation TBD."
                )
                await app_handler(cast(dict[str, Any], raw_data), message)

            elif channel == "pong" or channel == "subscriptionResponse":
                logger.debug(f"[{self.exchange_name}] Control message on '{channel}': {message}")
                payload_for_control_handler = (
                    cast(dict[str, Any], raw_data) if isinstance(raw_data, dict) else {}
                )
                await app_handler(payload_for_control_handler, message)
            else:
                logger.debug(
                    f"[{self.exchange_name}] Unhandled channel '{channel}' by specific "
                    f"validation, passing raw. Msg: {message}"
                )
                payload_for_unhandled_handler = (
                    cast(dict[str, Any], raw_data) if isinstance(raw_data, dict) else {}
                )
                await app_handler(payload_for_unhandled_handler, message)

        except APIError as e:
            logger.error(
                f"[{self.exchange_name}] APIError in WS routing for {channel}: {e.message}",
                exc_info=True,
            )
        except ValidationError as e_val:
            logger.error(
                f"[{self.exchange_name}] Unexpected Pydantic ValidationErr for {channel}: {e_val}",
                exc_info=True,
            )
        except Exception as e_app:
            logger.error(
                f"[{self.exchange_name}] Error in app_handler for {channel}: {e_app}", exc_info=True
            )

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
            request_payload_data = HyperliquidRequestBuilder.build_info_request_payload()
            response: object = await self._request(
                "POST",
                f"{self.INFO_URL.rstrip('/')}/info",
                data=request_payload_data,  # This is already None or dict
            )
            validated_state: HyperliquidRawClearinghouseState = (
                HyperliquidResponseHandler.handle_info_user_state_response(
                    cast(RawJsonResponse, response), self._wallet_address
                )
            )
            return self._hl_mapper.map_raw_clearinghouse_state_to_spot_balances(validated_state)
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
            request_payload_data = HyperliquidRequestBuilder.build_info_request_payload()
            response: object = await self._request(
                "POST",
                f"{self.INFO_URL.rstrip('/')}/info",
                data=request_payload_data,  # This is already None or dict
            )
            validated_state: HyperliquidRawClearinghouseState = (
                HyperliquidResponseHandler.handle_info_user_state_response(
                    cast(RawJsonResponse, response), self._wallet_address
                )
            )
            positions_dict = self._hl_mapper.map_raw_clearinghouse_state_to_derivative_positions(
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
            request_payload_data = HyperliquidRequestBuilder.build_info_request_payload()
            response: object = await self._request(
                "POST",
                f"{self.INFO_URL.rstrip('/')}/info",
                data=request_payload_data,  # This is already None or dict
            )
            validated_response_wrapper: HyperliquidRawOpenOrdersResponse = (
                HyperliquidResponseHandler.handle_info_open_orders_response(
                    cast(RawJsonResponse, response), self._wallet_address
                )
            )
            open_orders: list[Order] = []
            for order_obj in validated_response_wrapper.items:
                order_data = order_obj.order
                trigger_info = order_obj.trigger
                order_symbol: str = order_data.asset
                if symbol is None or order_symbol == symbol:
                    mapped_order = self._hl_order_mapper.transform_raw_order_to_internal(
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
            request_payload_data = HyperliquidRequestBuilder.build_info_request_payload()
            response: object = await self._request(
                "POST",
                f"{self.INFO_URL.rstrip('/')}/info",
                data=request_payload_data,  # This is already None or dict
            )
            validated_meta_ctxs: HyperliquidRawMetaAndAssetCtxsResponse = (
                HyperliquidResponseHandler.handle_info_meta_and_asset_ctxs_response(
                    cast(RawJsonResponse, response)
                )
            )
            target_asset_ctx_raw: HyperliquidRawAssetCtx | None = None
            for asset_ctx in validated_meta_ctxs.asset_ctxs:
                if asset_ctx.name == symbol:
                    target_asset_ctx_raw = asset_ctx
                    break

            if target_asset_ctx_raw is None:
                raise APIError(
                    f"Ticker data not found for {symbol} in asset contexts",
                    code=APIErrorCode.SYMBOL_NOT_FOUND.value,
                )

            return self._hl_mapper.map_raw_ctx_to_ticker(target_asset_ctx_raw)
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
            request_payload_data = HyperliquidRequestBuilder.build_info_request_payload()
            response: object = await self._request(
                "POST",
                f"{self.INFO_URL.rstrip('/')}/info",
                data=request_payload_data,  # This is already None or dict
            )
            validated: HyperliquidRawOrderBookResponse = (
                HyperliquidResponseHandler.handle_info_l2_book_response(
                    cast(RawJsonResponse, response), symbol
                )
            )
            return self._hl_mapper.map_raw_order_book(validated, depth)
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
            request_payload_data = HyperliquidRequestBuilder.build_info_request_payload()
            response: object = await self._request(
                "POST",
                f"{self.INFO_URL.rstrip('/')}/info",
                data=request_payload_data,  # This is already None or dict
            )
            validated_trades_list: list[HyperliquidRawPublicTrade] = (
                HyperliquidResponseHandler.handle_info_recent_trades_response(
                    cast(RawJsonResponse, response), symbol
                )
            )
            return self._hl_mapper.map_raw_trades(validated_trades_list, limit)
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
            request_payload_data = HyperliquidRequestBuilder.build_info_request_payload()
            response: object = await self._request(
                "POST",
                f"{self.INFO_URL.rstrip('/')}/info",
                data=request_payload_data,  # This is already None or dict
            )
            validated_meta_ctxs: HyperliquidRawMetaAndAssetCtxsResponse = (
                HyperliquidResponseHandler.handle_info_meta_and_asset_ctxs_response(
                    cast(RawJsonResponse, response)
                )
            )
            asset_ctx = next(
                (ctx for ctx in validated_meta_ctxs.asset_ctxs if ctx.name == symbol), None
            )
            if asset_ctx is None:
                logger.warning(f"[{self.exchange_name}] No asset context found for {symbol}.")
                return None
            return self._hl_mapper.map_raw_ctx_to_funding_rate(asset_ctx)
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

        request_model = HyperliquidRequestBuilder.build_l2_usd_transfer_payload(
            destination_address=to_account, amount=amount
        )
        request_data_dict = request_model.model_dump(by_alias=True, exclude_none=True)

        response_raw: object = None
        try:
            response_raw = await self._request(
                "POST", "/exchange", data=request_data_dict, is_signed=True
            )
            validated_response: HyperliquidRawExchangeResponse = (
                HyperliquidResponseHandler.handle_exchange_response(response_raw, "L2 Transfer")
            )

            if not validated_response.data or not validated_response.data.statuses:
                logger.warning(
                    f"[{self.exchange_name}] L2 Transfer response 'ok' but data or statuses "
                    f"list is missing/empty. Raw: {response_raw!r}"
                )
                raise APIError(
                    "L2 Transfer 'ok' but no status details returned.",
                    code=APIErrorCode.UNKNOWN.value,
                )

            first_status_obj_raw = validated_response.data.statuses[0]
            error_to_raise_from_status: APIError | None = None

            # Prepare argument for the handler
            arg_for_handler: dict[str, Any] | str
            if isinstance(first_status_obj_raw, str):
                arg_for_handler = first_status_obj_raw
            elif isinstance(first_status_obj_raw, HyperliquidRawExchangeStatusObject):  # pyright: ignore[reportUnnecessaryIsInstance]
                # first_status_obj_raw is now known to be HyperliquidRawExchangeStatusObject
                # The handler expects a raw dict for validation if it's not a string
                arg_for_handler = first_status_obj_raw.model_dump(by_alias=True, exclude_none=True)
            else:
                # This path should be theoretically unreachable due to the Union type of
                # first_status_obj_raw: RawStatusStringHL | HyperliquidRawExchangeStatusObject
                # and Pydantic validation of HyperliquidRawExchangeResponse.
                # Adding this to satisfy linters about arg_for_handler potentially being unbound.
                err_msg = (  # type: ignore[unreachable]
                    f"[{self.exchange_name}] Unexpected type for first_status_obj_raw in Transfer: "
                    f"{type(first_status_obj_raw)}. Raw: {first_status_obj_raw!r}. "
                    f"Indicates flaw in Pydantic validation or unexpected API response."
                )
                logger.critical(err_msg)
                raise RuntimeError(err_msg)  # Should not happen

            processed_status = HyperliquidResponseHandler.process_first_exchange_status(
                arg_for_handler, "L2 Transfer"
            )

            if isinstance(processed_status, HyperliquidSuccessfulOrderStatus):
                # For L2 Transfer, we typically expect a generic success or specific info.
                # HyperliquidSuccessfulOrderStatus is primarily for order states.
                # If status_type is 'canceled_str', it implies the raw input was a benign string
                # like "canceled", which might be an acceptable non-error status for some actions.
                if processed_status.status_type == "canceled_str":
                    logger.info(
                        f"[{self.exchange_name}] L2 Transfer received benign status: "
                        f"'{processed_status.status_type}'. Original raw status part: "
                        f"{arg_for_handler!r}"
                    )
                    return {
                        "status": "success_with_info",
                        "data": f"Status: {processed_status.status_type}",
                    }

                # Other HyperliquidSuccessfulOrderStatus types (resting, filled, oid-based canceled)
                # are generally not expected for L2 Transfer. If process_first_exchange_status
                # mapped the raw L2 transfer success to one of these, it might be unexpected.
                # However, any non-error HyperliquidSuccessfulOrderStatus is
                # treated as success here.
                logger.info(
                    f"[{self.exchange_name}] L2 Transfer appears successful. Processed status: "
                    f"{processed_status.model_dump_json(exclude_none=True)!r}"
                )
                return {"status": "success", "data": processed_status.model_dump(exclude_none=True)}

            # If not HyperliquidSuccessfulOrderStatus, it must be HyperliquidErrorStatus
            # assuming process_first_exchange_status strictly returns one of these two types.
            else:
                # processed_status is now known to be HyperliquidErrorStatus
                logger.warning(
                    f"[{self.exchange_name}] L2 Transfer failed with error: "
                    f"{processed_status.message}"
                )
                error_to_raise_from_status = self.error_mapper.map_string_error(
                    processed_status.message,
                    http_status=200,  # Assuming 200 OK if we got this far
                )

            # If error_to_raise_from_status was set in the HyperliquidErrorStatus branch
            if error_to_raise_from_status:
                raise error_to_raise_from_status

            # Fallback: This should ideally not be reached if the logic for
            # HyperliquidSuccessfulOrderStatus and HyperliquidErrorStatus is exhaustive
            # and process_first_exchange_status is robust.
            # If it is reached, it means processed_status was neither of the expected types OR
            logger.warning(
                f"[{self.exchange_name}] L2 Transfer status unclear after processing logic. "
                f"Processed: {processed_status!r}. Raw response used for handler: "
                f"{arg_for_handler!r}"
            )
            raise APIError(
                "L2 Transfer status unclear after processing.",
                code=APIErrorCode.UNKNOWN.value,
            )

        except ValidationError as e:
            logger.error(
                f"[{self.exchange_name}] Failed to validate L2 transfer response: {e}. "
                f"Raw: {response_raw!r}"
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

        request_model = HyperliquidRequestBuilder.build_withdrawal_payload(
            asset=asset, amount=amount, destination_address=address
        )
        request_data_dict = request_model.model_dump(by_alias=True, exclude_none=True)

        response_raw: object = None
        try:
            response_raw = await self._request(
                "POST", "/exchange", data=request_data_dict, is_signed=True
            )
            validated_response: HyperliquidRawExchangeResponse = (
                HyperliquidResponseHandler.handle_exchange_response(response_raw, "Withdrawal")
            )

            if not validated_response.data or not validated_response.data.statuses:
                logger.warning(
                    f"[{self.exchange_name}] Withdraw response 'ok' but data or statuses list "
                    f"is missing/empty. Raw: {response_raw!r}"
                )
                raise APIError(
                    "Withdrawal status unclear: 'ok' but no status details provided.",
                    code=APIErrorCode.EXCHANGE_SPECIFIC.value,
                )

            first_status_obj_raw = validated_response.data.statuses[0]
            if isinstance(first_status_obj_raw, str):
                if "error" in first_status_obj_raw.lower():
                    logger.warning(
                        f"[{self.exchange_name}] Withdraw failed with status string: "
                        f"{first_status_obj_raw}"
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
                        f"[{self.exchange_name}] Withdrawal for {asset} successful. "
                        f"TxHash: {tx_hash}"
                    )
                    return {"status": "success", "tx_hash": tx_hash}
                elif status_object.success:
                    logger.info(
                        f"[{self.exchange_name}] Withdraw successful with message: "
                        f"{status_object.success}"
                    )
                    return {
                        "status": "success_with_info",
                        "message": status_object.success,
                        "tx_hash": None,
                    }
                else:
                    logger.warning(
                        f"[{self.exchange_name}] Withdraw status 'ok' but unrecognized obj "
                        f"structure: {status_object.model_dump_json()!r}. Raw: {response_raw!r}"
                    )
                    raise APIError(
                        "Withdrawal status unclear: Unrecognized success object structure.",
                        code=APIErrorCode.EXCHANGE_SPECIFIC.value,
                    )
        except ValidationError as e:
            logger.error(
                f"[{self.exchange_name}] Validation error processing withdraw response: {e}. "
                f"Raw: {response_raw!r}"
            )
            raise APIError(
                f"Failed to validate withdraw response: {e}",
                code=APIErrorCode.EXCHANGE_SPECIFIC.value,
                original_exception=e,
            ) from e
        except APIError:
            raise
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error during withdraw for {asset} "
                f"to {address}: "
                f"{e}. Raw: {response_raw!r}"
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
                            f"[{self.exchange_name}] Cancelling order "
                            f"{order_to_cancel.exchange_order_id} for {order_to_cancel.symbol}"
                        )
                        await self.cancel_order(
                            order_id=order_to_cancel.exchange_order_id,
                            symbol=order_to_cancel.symbol,
                        )
                        cancelled_count += 1
                        await asyncio.sleep(0.1)
                    else:
                        logger.warning(
                            f"[{self.exchange_name}] Skipping order cancel due to missing "
                            f"ID/symbol: {order_to_cancel}"
                        )
                        failed_count += 1
                except APIError as e_cancel:
                    logger.error(
                        f"[{self.exchange_name}] Failed to cancel order "
                        f"{order_to_cancel.exchange_order_id}: {e_cancel}"
                    )
                    failed_count += 1
                except Exception as e_unexp_cancel:
                    logger.error(
                        f"[{self.exchange_name}] Unexpected error cancelling order "
                        f"{order_to_cancel.exchange_order_id}: {e_unexp_cancel}"
                    )
                    failed_count += 1
            logger.info(
                f"[{self.exchange_name}] Cancellation summary for {symbol or 'all'}: "
                f"{cancelled_count} succeeded, {failed_count} failed."
            )
        except APIError as e_get_orders:
            logger.error(
                f"[{self.exchange_name}] API Error fetching open orders to cancel: {e_get_orders}"
            )
            raise
        except Exception as e_overall:
            logger.error(
                f"[{self.exchange_name}] Unexpected error during cancel_all_orders for "
                f"{symbol or 'all'}: {e_overall}",
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

        if self._wallet_address is None:
            raise APIError(
                "Wallet address is required for order history.",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        request_model = HyperliquidRequestBuilder.build_order_history_payload(
            wallet_address=self._wallet_address,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )
        request_data_dict = request_model.model_dump(by_alias=True, exclude_none=True)

        response_raw: object = None
        orders_list: list[Order] = []
        try:
            response_raw = await self._request(
                "POST",
                f"{self.INFO_URL.rstrip('/')}/info",
                data=request_data_dict,
            )
            if not isinstance(response_raw, list):
                raise APIError(
                    f"Invalid response type for queryOrderHistory: expected list, "
                    f"got {type(response_raw)}",
                    code=APIErrorCode.UNKNOWN.value,
                )

            validated_history_list: list[HyperliquidRawHistoricalOrderResponse] = (
                HyperliquidResponseHandler.handle_query_order_history_response(
                    response_raw, self._wallet_address
                )
            )

            for raw_status_response in validated_history_list:
                try:
                    raw_order = raw_status_response.order
                    trigger_info = None
                    internal_order = (
                        self._hl_order_mapper.transform_raw_historical_order_to_internal(
                            raw_historical_order=raw_order,
                            trigger=trigger_info,
                        )
                    )
                    if internal_order:
                        orders_list.append(internal_order)
                except (ValidationError, ValueError) as e_item:
                    logger.warning(
                        f"[{self.exchange_name}] Skipping order history item due to "
                        f"validation/transform error: {e_item}. Raw: {raw_status_response}"
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
                f"[{self.exchange_name}] Error processing order history response: {e_outer}. "
                f"Raw: {response_raw!r}",
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
            request_payload_data = HyperliquidRequestBuilder.build_info_request_payload()
            response_raw = await self._request(
                "POST",
                f"{self.INFO_URL.rstrip('/')}/info",
                data=request_payload_data,  # This is already None or dict
            )
            if not isinstance(response_raw, list):
                logger.warning(
                    f"[{self.exchange_name}] Unexpected userFills response type: "
                    f"{type(response_raw)}. Expected list. Empty list."
                )
                return []
            validated_fills_response: HyperliquidRawUserFillsResponse = (
                HyperliquidResponseHandler.handle_info_user_fills_response(
                    cast(RawJsonResponse, response_raw), self._wallet_address
                )
            )
            trades: list[Trade] = []
            for raw_fill in validated_fills_response.root:
                try:
                    if symbol is not None and raw_fill.coin != symbol:
                        continue
                    # Cast raw_fill (HyperliquidRawUserFill) to HyperliquidRawFill
                    # for the mapper
                    internal_trade = self._hl_mapper.transform_raw_fill_to_internal(
                        cast(HyperliquidRawFill, raw_fill)
                    )
                    trades.append(internal_trade)
                except (ValidationError, ValueError) as e_item:
                    logger.warning(
                        f"[{self.exchange_name}] Skipping fill due to validation/transform "
                        f"error: {e_item}. Data: {raw_fill}"
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
            request_payload_data = HyperliquidRequestBuilder.build_info_request_payload()
            response_raw: object = await self._request(
                "POST",
                f"{self.INFO_URL.rstrip('/')}/info",
                data=request_payload_data,  # This is already None or dict
            )

            # Use the consistent response handler for MetaAndAssetCtxs
            validated_meta_ctxs: HyperliquidRawMetaAndAssetCtxsResponse = (
                HyperliquidResponseHandler.handle_info_meta_and_asset_ctxs_response(
                    cast(RawJsonResponse, response_raw)
                )
            )

            result: list[FundingRate] = []

            for asset_ctx_item in validated_meta_ctxs.asset_ctxs:
                try:
                    # HyperliquidRawAssetCtx is already validated by the response handler
                    market_symbol: str = asset_ctx_item.name
                    if symbols is not None and market_symbol not in symbols:
                        continue
                    funding_rate = self._hl_mapper.map_raw_ctx_to_funding_rate(asset_ctx_item)
                    if funding_rate:
                        result.append(funding_rate)
                except ValidationError as ve_ctx:  # Should ideally not happen if handler worked
                    logger.warning(
                        f"[{self.exchange_name}] Failed to validate asset_ctx for funding rate: "
                        f"{ve_ctx}. Data: {asset_ctx_item.model_dump_json()!r}"
                    )
                except Exception as e_map_ctx:
                    logger.error(
                        f"[{self.exchange_name}] Error mapping asset_ctx to funding rate: "
                        f"{e_map_ctx}. Data: {asset_ctx_item.model_dump_json()!r}",
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
        request_model = HyperliquidRequestBuilder.build_candle_snapshot_payload(
            symbol=symbol, timeframe=timeframe, start_time_ms=0, end_time_ms=int(time.time() * 1000)
        )
        request_data_dict = request_model.model_dump(by_alias=True, exclude_none=True)
        try:
            raw_response = await self._request(
                method="POST",
                endpoint=f"{self.INFO_URL.rstrip('/')}/info",
                data=request_data_dict,
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

            raw_snapshot: HyperliquidRawCandleSnapshot = (
                HyperliquidResponseHandler.handle_info_candle_snapshot_response(
                    cast(RawJsonResponse, raw_response), symbol, timeframe
                )
            )
            # The mapper now expects the full response object (HyperliquidRawCandleSnapshot)
            internal_candles = self._hl_candle_mapper.map(raw_snapshot, symbol, timeframe)
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

        # Delegate payload construction to the request builder
        request_model = HyperliquidRequestBuilder.build_place_order_payload(
            asset_index=asset_index,
            side=side,
            order_type=order_type,
            quantity=quantity,
            time_in_force=time_in_force,
            price=price,
            stop_price=stop_price,
            client_order_id=client_order_id,
            reduce_only=reduce_only,
            post_only=post_only,
        )
        request_data_dict = request_model.model_dump(by_alias=True, exclude_none=True)

        response_raw: object = None
        try:
            response_raw = await self._request(
                "POST", "/exchange", data=request_data_dict, is_signed=True
            )

            validated_response: HyperliquidRawExchangeResponse = (
                HyperliquidResponseHandler.handle_exchange_response(
                    cast(RawJsonResponse, response_raw), "Place Order"
                )
            )

            if (
                validated_response.status != "ok"
                or not validated_response.data
                or not validated_response.data.statuses
            ):
                mapped_error = self.error_mapper.map_exchange_error(
                    status_code=200,  # Assuming 200 OK if status is 'ok' but data is missing
                    error_body=str(response_raw),  # Use raw string as body
                    error_data=validated_response.model_dump(),  # Pass validated data
                    request_path="/exchange",
                )
                raise mapped_error

            # Now statuses is known to be a list from validation
            first_status_obj_raw = validated_response.data.statuses[0]
            error_to_raise_from_status: APIError | None = None

            # Prepare argument for the handler based on its type
            arg_for_handler: dict[str, Any] | str
            if isinstance(first_status_obj_raw, str):
                arg_for_handler = first_status_obj_raw
            elif isinstance(first_status_obj_raw, HyperliquidRawExchangeStatusObject):  # pyright: ignore[reportUnnecessaryIsInstance]
                # first_status_obj_raw is now known to be HyperliquidRawExchangeStatusObject
                # The handler expects a raw dict for validation if it's not a string
                arg_for_handler = first_status_obj_raw.model_dump(by_alias=True, exclude_none=True)
            else:
                # This path should be theoretically unreachable.
                err_msg = (  # type: ignore[unreachable]
                    f"[{self.exchange_name}] Unexpected type for first_status_obj_raw in "
                    f"Place Order: {type(first_status_obj_raw)}. Raw: {first_status_obj_raw!r}. "
                    f"Indicates flaw in Pydantic validation or unexpected API response."
                )
                logger.critical(err_msg)
                raise RuntimeError(err_msg)  # Should not happen

            processed_status = HyperliquidResponseHandler.process_first_exchange_status(
                arg_for_handler, "Place Order"
            )

            order_id_to_fetch: int | None = None
            log_message_prefix = "Order placement status unclear"

            if isinstance(processed_status, HyperliquidSuccessfulOrderStatus):
                if processed_status.oid is not None:
                    if processed_status.status_type == "resting":
                        order_id_to_fetch = processed_status.oid
                        log_message_prefix = f"Order OID:{processed_status.oid} resting"
                    elif processed_status.status_type == "filled":
                        order_id_to_fetch = processed_status.oid
                        log_message_prefix = (
                            f"Order OID:{processed_status.oid} filled (avgPx: "
                            f"{processed_status.avg_px}, "
                            f"sz: {processed_status.total_sz})"
                        )
                        logger.info(f"[{self.exchange_name}] {log_message_prefix}")
                    elif processed_status.status_type == "canceled":  # Object variant
                        order_id_to_fetch = processed_status.oid
                        log_message_prefix = (
                            f"Order OID:{processed_status.oid} canceled (via object status)"
                        )
                        logger.info(f"[{self.exchange_name}] {log_message_prefix}")
                elif processed_status.status_type == "canceled_str":
                    logger.info(
                        f"[{self.exchange_name}] Order placement returned 'canceled' string status."
                    )
                    # No OID from "canceled_str", but not an error.
            else:
                # processed_status is now known to be HyperliquidErrorStatus
                logger.warning(
                    f"[{self.exchange_name}] Order placement failed with error: "
                    f"{processed_status.message}"
                )
                error_to_raise_from_status = self.error_mapper.map_string_error(
                    processed_status.message,
                    http_status=200,  # Assuming 200 OK if we got this far
                )

            # After processing with the new handler:
            if error_to_raise_from_status:
                raise error_to_raise_from_status

            if order_id_to_fetch is not None:
                logger.info(
                    f"[{self.exchange_name}] {log_message_prefix}. Fetching canonical status."
                )
                await asyncio.sleep(0.2)  # Consider making delay configurable or removing
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
                # This case means the status was a benign string without an OID, or
                # a complex object without error/resting/filled that yielded an OID.
                # This is an unclear outcome if an order was intended to be placed.
                logger.warning(
                    f"[{self.exchange_name}] Order placement status unclear. Processed status: "
                    f"{processed_status!r}. No OID found to fetch canonical status."
                )
                raise APIError(
                    f"Order placement status unclear, no OID to confirm: "
                    f"{str(processed_status)[:100]}...",  # Truncate for brevity
                    code=APIErrorCode.UNKNOWN.value,
                )
        except ValidationError as e_val_outer:
            logger.error(
                f"[{self.exchange_name}] Failed to validate order placement response: "
                f"{e_val_outer}. Raw: {response_raw!r}"
            )
            # Simplified: Raise based on validation error, avoid re-parsing raw object
            raise APIError(
                f"Invalid response structure after placing order: {e_val_outer}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_val_outer,
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
            request_model = HyperliquidRequestBuilder.build_cancel_order_payload(
                asset_index=asset_index, order_id=int(order_id)
            )
            request_data_dict = request_model.model_dump(by_alias=True, exclude_none=True)

            response_raw = await self._request(
                "POST", "/exchange", data=request_data_dict, is_signed=True
            )
            validated_response: HyperliquidRawExchangeResponse = (
                HyperliquidResponseHandler.handle_exchange_response(response_raw, "Cancel Order")
            )

            if (
                validated_response.status != "ok"
                or not validated_response.data
                or not validated_response.data.statuses
            ):
                # Use error_mapper for consistent error raising
                mapped_error = self.error_mapper.map_exchange_error(
                    status_code=200,  # Assuming 200 if we got this far
                    error_body=str(response_raw),
                    error_data=validated_response.model_dump() if validated_response else None,
                    request_path="/exchange",
                )
                raise mapped_error

            first_status_obj_raw = validated_response.data.statuses[0]
            error_to_raise_from_status: APIError | None = None

            # Prepare argument for the handler
            arg_for_handler: dict[str, Any] | str
            if isinstance(first_status_obj_raw, str):
                arg_for_handler = first_status_obj_raw
            elif isinstance(first_status_obj_raw, HyperliquidRawExchangeStatusObject):  # pyright: ignore[reportUnnecessaryIsInstance]
                arg_for_handler = first_status_obj_raw.model_dump(by_alias=True, exclude_none=True)
            else:
                # This path should be theoretically unreachable.
                err_msg = (  # type: ignore[unreachable]
                    f"[{self.exchange_name}] Unexpected type for first_status_obj_raw in "
                    f"Cancel Order: {type(first_status_obj_raw)}. Raw: {first_status_obj_raw!r}. "
                    f"Indicates flaw in Pydantic validation or unexpected API response."
                )
                logger.critical(err_msg)
                raise RuntimeError(err_msg)  # Should not happen

            processed_status = HyperliquidResponseHandler.process_first_exchange_status(
                arg_for_handler, f"Cancel Order OID:{order_id}"
            )

            if isinstance(processed_status, HyperliquidSuccessfulOrderStatus):
                # For cancel, "canceled" (obj with matching OID) or "canceled_str" are
                # primary success indicators.
                if processed_status.status_type == "canceled" and processed_status.oid == int(
                    order_id
                ):
                    logger.info(
                        f"[{self.exchange_name}] Successfully cancelled order {order_id} "
                        f"(object status: '{processed_status.status_type}')"
                    )
                    return True
                elif processed_status.status_type == "canceled_str":
                    logger.info(
                        f"[{self.exchange_name}] Successfully cancelled order {order_id} "
                        f"(string status: '{processed_status.status_type}')"
                    )
                    return True
                else:
                    # Other successful order statuses (resting, filled) or generic success objects
                    # are not typically expected for a specific cancel action
                    # but could indicate success.
                    logger.warning(
                        f"[{self.exchange_name}] Cancel order {order_id} returned unexpected "
                        f"successful status: "
                        f"{processed_status.model_dump_json(exclude_none=True)!r}. "
                        f"Assuming success."
                    )
                    # Assuming any non-error successful status means cancel likely went through
                    return True

            # If not HyperliquidSuccessfulOrderStatus, it must be HyperliquidErrorStatus
            # assuming process_first_exchange_status strictly returns one of these two types.
            else:
                # processed_status is now known to be HyperliquidErrorStatus
                logger.warning(
                    f"[{self.exchange_name}] Cancel order {order_id} failed with error: "
                    f"'{processed_status.message}'"
                )
                if "Order not found" in processed_status.message:
                    error_to_raise_from_status = APIError(
                        f"Cancel failed: Order {order_id} not found.",
                        code=APIErrorCode.ORDER_NOT_FOUND.value,
                        exchange_message=processed_status.message,
                    )
                else:
                    error_to_raise_from_status = self.error_mapper.map_string_error(
                        processed_status.message,
                        http_status=200,  # Assuming 200
                    )

            if error_to_raise_from_status:
                raise error_to_raise_from_status

            # Fallback if status was not definitively error or success by the logic above
            # This should ideally not be reached.
            logger.warning(
                f"[{self.exchange_name}] Cancel order {order_id} status unclear after processing. "
                f"Processed: {processed_status!r}. Raw arg to handler: {arg_for_handler!r}"
            )
            raise APIError(
                f"Cancel order {order_id} status unclear after processing.",
                code=APIErrorCode.UNKNOWN.value,
            )

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
                f"[{self.exchange_name}] Unexpected error canceling order "
                f"{order_id}: {e_unexp_cancel}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error canceling order {order_id}: {e_unexp_cancel}",
                code=APIErrorCode.UNKNOWN.value,
            ) from e_unexp_cancel

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
        request_model = HyperliquidRequestBuilder.build_order_status_payload(
            wallet_address=self._wallet_address, order_id=int(order_id)
        )
        request_data_dict = request_model.model_dump(by_alias=True, exclude_none=True)
        try:
            response_data_raw = await self._request(
                "POST",
                f"{self.INFO_URL.rstrip('/')}/info",
                data=request_data_dict,
            )
            response_data: Any = response_data_raw

            if not response_data or not isinstance(response_data, list) or not response_data:
                raise APIError(
                    f"Order not found (empty/invalid response): id={order_id}",
                    code=APIErrorCode.ORDER_NOT_FOUND.value,
                )

            validated_status_response: HyperliquidRawHistoricalOrderResponse = (
                HyperliquidResponseHandler.handle_info_order_status_response(
                    response_data_raw, self._wallet_address, int(order_id)
                )
            )
            # Use the correct mapper for HyperliquidRawHistoricalOrder
            return self._hl_order_mapper.transform_raw_historical_order_to_internal(
                raw_historical_order=validated_status_response.order,
                trigger=None,  # Assuming no separate trigger info here
            )
        except APIError:
            raise
        except ValidationError as e_val:
            logger.error(
                f"[{self.exchange_name}] Validation error in get_order_status: {e_val}. "
                f"Payload: {request_data_dict!r}"
            )
            raise APIError(
                "Pydantic validation error processing order status.",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_val,
            ) from e_val
        except Exception as e_unexp:
            logger.error(
                f"[{self.exchange_name}] Unhandled error fetching order status for "
                f"{order_id or client_order_id}: {e_unexp}",
                exc_info=True,
            )
            raise APIError(
                message=(
                    f"Unexpected error processing order status for "
                    f"{order_id or client_order_id}: {e_unexp}"
                ),
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unexp,
            ) from e_unexp

    async def get_account_summary(self) -> MarginAccountSummary | None:
        """Fetches user state and maps it to MarginAccountSummary."""
        if not self._wallet_address:
            logger.error(
                f"[{self.exchange_name}] Wallet address not available, cannot fetch "
                f"user state/account summary."
            )
            raise APIError(
                message=(
                    f"HLAPI: Wallet address required for get_account_summary. "
                    f"Exchange: {self.exchange_name}"
                ),
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        payload_model = HyperliquidRequestBuilder.build_user_state_payload(self._wallet_address)
        payload_dict = payload_model.model_dump(by_alias=True, exclude_none=True)
        response_raw: RawJsonResponse | None = None
        try:
            response_raw = await self._request(
                method="POST",
                endpoint=self.INFO_URL,  # User state is on the INFO_URL
                data=payload_dict,  # Use the dumped dictionary
                is_signed=False,  # User state is a public endpoint if wallet address is known
            )

            if not isinstance(response_raw, dict):
                logger.error(
                    f"[{self.exchange_name}] Unexpected user_state response format: "
                    f"{type(response_raw)}. Raw: {response_raw}"
                )
                raise APIError(
                    "Invalid user_state response format (not a dict)",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                )

            validated_user_state: HyperliquidRawClearinghouseState = (
                HyperliquidResponseHandler.handle_info_user_state_response(
                    raw_response_content=response_raw, user_address=self._wallet_address
                )
            )

            # Transform to MarginAccountSummary using the instance mapper
            margin_summary = self._hl_mapper.map_raw_clearinghouse_state_to_margin_summary(
                raw_state=validated_user_state
            )
            return margin_summary

        except APIError as e:
            logger.error(
                f"[{self.exchange_name}] API Error getting account summary (user_state): {e}"
            )
            # Depending on error, might return None or re-raise
            if (
                e.code == APIErrorCode.AUTHENTICATION_FAILED.value
            ):  # Should not happen for public endpoint
                return None
            raise e
        except ValidationError as ve:  # ADDED BLOCK
            logger.error(
                f"[{self.exchange_name}] Pydantic ValidationError in get_account_summary "
                f"(user_state): {ve}",
                exc_info=True,
            )
            raise APIError(
                f"Validation error processing account summary (user_state): {ve}",
                code=APIErrorCode.INVALID_RESPONSE.value,  # Or a more specific code
                original_exception=ve,
            ) from ve
        except Exception as e:  # Keep this for other unexpected errors
            logger.error(
                f"[{self.exchange_name}] Unexpected error in get_account_summary (user_state): {e}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error getting account summary (user_state): {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e
