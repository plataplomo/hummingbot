from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from typing import Any, cast

import aiohttp
from pydantic import HttpUrl, ValidationError

from cyberdelta.apis.base.authenticator_interface import AuthenticatedRequestComponents
from cyberdelta.apis.base.exchange_api import ExchangeAPI, MessageHandler
from cyberdelta.apis.connectivity.connectivity_models import HttpClientConfig
from cyberdelta.apis.connectivity.http_client import HttpClient
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

# Hyperliquid Raw Models
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeResponse,
    HyperliquidRawExchangeStatusObject,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_fill import HyperliquidRawFill
from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import (
    HyperliquidRawHistoricalOrderResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawMetaAndAssetCtxsResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import HyperliquidRawOpenOrdersResponse

# Import HyperliquidRawL2Book
from cyberdelta.apis.hyperliquid.models.hl_raw_user_fills import HyperliquidRawUserFillsResponse
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import HyperliquidRawClearinghouseState

# ADDED IMPORT FOR ACCOUNT SERVICE
from cyberdelta.apis.hyperliquid.services.hl_account_service import HyperliquidAccountService
from cyberdelta.apis.hyperliquid.services.hl_market_data_service import HyperliquidMarketDataService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode

# Core Domain Models
from cyberdelta.core.models import (
    DerivativePosition,
    FundingRate,
    MarginAccountSummary,
    SpotBalance,
    Ticker,  # Added Ticker
    Trade,
)
from cyberdelta.core.models.enums import (
    OrderSide,
    OrderType,
    TimeInForce,
)
from cyberdelta.core.models.market import Candle, OrderBook  # Added Candle, OrderBook
from cyberdelta.core.models.market.order import Order, OrderStatus
from cyberdelta.utils.logging_config import get_logger
from cyberdelta.utils.parsing import timeframe_to_ms  # Import the new helper

logger = get_logger(__name__)


class HyperliquidAPI(ExchangeAPI):
    """API Client for Hyperliquid DEX."""

    BASE_URL = "https://api.hyperliquid.xyz"
    INFO_URL = "https://info.hyperliquid.xyz"
    WS_URL = "wss://api.hyperliquid.xyz/ws"
    CHAIN_ID = 1337

    account_service: HyperliquidAccountService

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

        # Instantiate request builder and response handler
        self._hl_request_builder = HyperliquidRequestBuilder()
        self._hl_response_handler = HyperliquidResponseHandler()

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

        self.account_service = HyperliquidAccountService(
            http_client_requester=self._request,
            request_builder=self._hl_request_builder,
            response_handler=self._hl_response_handler,
            authenticator=self._hl_authenticator,
            rate_limiter_service=self._rate_limiter_service,
            exchange_name=self.exchange_name,
            info_url_base=self.INFO_URL,
            wallet_address=self._wallet_address,
        )

        # Create HttpClientConfig for the INFO_URL client
        info_http_client_raw_config = api_config.get("http_client", {})
        info_client_config = HttpClientConfig(
            rest_endpoint=HttpUrl(self.INFO_URL),  # Wrap with HttpUrl
            default_request_timeout=info_http_client_raw_config.get(
                "default_request_timeout", 10.0
            ),
            max_retries=info_http_client_raw_config.get("max_retries", 3),
            retry_delay_seconds=info_http_client_raw_config.get("retry_delay_seconds", 5.0),
        )

        self._info_http_client = HttpClient(
            exchange_name=f"{self.exchange_name}_info",  # Differentiate name for logging
            config=info_client_config,
            # session=None by default, so HttpClient creates its own internal session
        )

        self.market_data = HyperliquidMarketDataService(
            http_client=self._info_http_client,  # Use the INFO_URL client
            request_builder=self._hl_request_builder,  # This is self._hl_request_builder
            response_handler=self._hl_response_handler,  # This is self._hl_response_handler
            rate_limiter_service=self._rate_limiter_service,  # This is self._rate_limiter_service
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

        # Handle control messages first
        if channel in ["pong", "subscriptionResponse"]:
            logger.debug(f"[{self.exchange_name}] Control message on '{channel}': {message}")
            # For subscriptionResponse, we might eventually want to confirm active subscriptions
            return

        topic_key_for_handler = channel  # Default to base channel name
        if channel == "l2Book":
            if isinstance(raw_data, dict):
                raw_data_dict = cast(dict[str, Any], raw_data)
                coin_from_data_any: Any = raw_data_dict.get("coin")
                if isinstance(coin_from_data_any, str):
                    topic_key_for_handler = f"{channel}:{coin_from_data_any}"
            else:
                logger.warning(
                    f"[{self.exchange_name}] Expected dict for 'l2Book' data to derive topic key, "
                    f"got {type(cast(object, raw_data))}. Using base channel '{channel}' as key. "
                    f"Msg: {message}"
                )
        elif channel == "trades":
            # For trades, raw_data is typically a list of trade dicts.
            # The subscription topic (e.g., "trades:COIN") is the primary source for the coin.
            # If the message itself contains a top-level 'coin' (as in test_hl_api.py), use that.
            # Otherwise, try to get it from the first trade item if raw_data is a list.
            coin_for_topic_str: str | None = None
            # Prioritize coin from message top-level (matches test_hl_api.py trades test structure)
            if isinstance(message.get("coin"), str):
                coin_for_topic_str = message.get("coin")
            elif isinstance(raw_data, list) and raw_data:
                first_trade_item: Any = raw_data[0]
                if isinstance(first_trade_item, dict):
                    coin_from_item_any: Any = cast(dict[str, Any], first_trade_item).get("coin")
                    if isinstance(coin_from_item_any, str):
                        coin_for_topic_str = coin_from_item_any

            if coin_for_topic_str:
                topic_key_for_handler = f"{channel}:{coin_for_topic_str}"
            # If coin_for_topic_str is still None, topic_key_for_handler remains base 'trades'
        elif channel == "userEvents":
            topic_key_for_handler = "userEvents"

        app_handler = self._ws_handlers.get(topic_key_for_handler)
        if not app_handler:
            generic_app_handler = self._ws_handlers.get(channel)
            if generic_app_handler:
                app_handler = generic_app_handler
            else:
                log_parts = [f"[{self.exchange_name}] No WS handler for '{topic_key_for_handler}'"]
                if topic_key_for_handler != channel:
                    log_parts.append(f" (or base '{channel}')")
                log_parts.append(f". Msg: {message}")
                logger.debug("".join(log_parts))
                return

        if raw_data is None:
            logger.warning(f"[{self.exchange_name}] WS '{channel}' has no data. Msg: {message}")
            return

        try:
            payload_for_handler: dict[str, Any] | None = None

            if channel == "l2Book":
                if not isinstance(raw_data, dict):
                    raise APIError(
                        f"l2Book data not dict: {type(raw_data)}",  # pyright: ignore [reportUnknownArgumentType]
                        code=APIErrorCode.INVALID_RESPONSE.value,
                    )
                # raw_data is now confirmed dict
                validated_model = HyperliquidWsRawMessageHandler.handle_l2book_payload(
                    cast(dict[str, Any], raw_data)
                )
                payload_for_handler = validated_model.model_dump(mode="json")
                await app_handler(payload_for_handler, message)

            elif channel == "trades":  # Public trades
                if not isinstance(raw_data, list):
                    actual_type_name = type(raw_data).__name__  # pyright: ignore[reportUnknownArgumentType]
                    # Pyright reports actual_type_name as partially unknown due to raw_data: Any.
                    # Mypy is satisfied. Runtime check for raw_data structure (isinstance list)
                    # precedes this. Downstream Pydantic models in HyperliquidWsRawMessageHandler
                    # validate specific content. Ignoring for practical reasons given
                    # Hyperliquid's complex, nested API structure for WS data.
                    raise APIError(
                        f"Trades data not list: {actual_type_name}",
                        code=APIErrorCode.INVALID_RESPONSE.value,
                    )

                typed_trades_input_list: list[dict[str, Any]] = []
                # raw_data is list[Any] after the check above
                # Pyright reports item_loop_var as unknown type when iterating raw_data (list[Any]).
                # Mypy correctly infers item_loop_var as Any. Subsequent code uses isinstance
                # checks and casts before use. Downstream Pydantic models in
                # HyperliquidWsRawMessageHandler perform full validation.
                # Ignoring for practical reasons given Hyperliquid's complex, nested API structure.
                for item_loop_var in raw_data:  # pyright: ignore[reportUnknownVariableType]
                    item_from_any_list = cast(Any, item_loop_var)
                    if not isinstance(item_from_any_list, dict):
                        logger.warning(
                            f"[{self.exchange_name}] Trades list item not dict: "
                            f"{item_from_any_list}. Msg: {message}. Skipping item."
                        )
                        continue
                    # item_from_any_list is now confirmed dict
                    item_dict = cast(dict[str, Any], item_from_any_list)
                    typed_trades_input_list.append(item_dict)

                if (
                    not typed_trades_input_list and raw_data
                ):  # if raw_data was not empty but all items were invalid
                    logger.warning(
                        f"[{self.exchange_name}] All items in trades list were invalid. "
                        f"Original raw_data: {raw_data}"
                    )
                    return  # Nothing to process

                if typed_trades_input_list:  # Only proceed if there are valid items
                    validated_trade_models = (
                        HyperliquidWsRawMessageHandler.handle_public_trades_payload(
                            typed_trades_input_list
                        )
                    )
                    for trade_model in validated_trade_models:
                        payload_for_handler = trade_model.model_dump(mode="json")
                        await app_handler(payload_for_handler, message)
                # If typed_trades_input_list is empty after filtering, do nothing.

            elif channel == "userEvents":
                if not isinstance(raw_data, list):
                    actual_type_name = type(raw_data).__name__  # pyright: ignore[reportUnknownArgumentType]
                    # Pyright reports actual_type_name as partially unknown due to raw_data: Any.
                    # Mypy is satisfied. Runtime check for raw_data structure (isinstance list)
                    # precedes this. Downstream Pydantic models in HyperliquidWsRawMessageHandler
                    # validate specific content. Ignoring for practical reasons given
                    # Hyperliquid's complex, nested API structure for WS data.
                    raise APIError(
                        f"userEvents data not list: {actual_type_name}",
                        code=APIErrorCode.INVALID_RESPONSE.value,
                    )

                # raw_data is list[Any] after the check above
                # Pyright reports event_loop_var unknown when iterating raw_data (list[Any]).
                # Mypy correctly infers event_loop_var as Any. Subsequent code uses isinstance
                # checks and casts before use. Downstream Pydantic models in
                # HyperliquidWsRawMessageHandler perform full validation.
                # Ignoring for practical reasons given Hyperliquid's complex, nested API structure.
                for event_loop_var in raw_data:  # pyright: ignore[reportUnknownVariableType]
                    event_item_from_any_list = cast(Any, event_loop_var)
                    if not isinstance(event_item_from_any_list, dict):
                        logger.warning(
                            f"[{self.exchange_name}] userEvents item not dict: "
                            f"{event_item_from_any_list}, skipping."
                        )
                        continue

                    # event_item_from_any_list is now confirmed dict
                    event_item_dict = cast(dict[str, Any], event_item_from_any_list)
                    event_type_any = event_item_dict.get("type")

                    if not isinstance(event_type_any, str):
                        logger.warning(
                            f"[{self.exchange_name}] userEvent item has no 'type' string: "
                            f"{event_item_dict}, skipping."
                        )
                        continue

                    event_type_str: str = event_type_any
                    current_event_payload_for_handler: dict[str, Any] | None = None

                    try:
                        if event_type_str == "fill":
                            validated_fill = (
                                HyperliquidWsRawMessageHandler.handle_user_fill_event_payload(
                                    event_item_dict
                                )
                            )
                            current_event_payload_for_handler = validated_fill.model_dump(
                                mode="json"
                            )

                        elif event_type_str == "order":
                            # The event_item_dict is the wrapper for the order event.
                            # Its 'data' field contains the actual order or list of fills.
                            _handle_order_wrapper = HyperliquidWsRawMessageHandler.handle_user_order_update_wrapper_payload
                            order_update_wrapper = _handle_order_wrapper(
                                event_item_dict
                                # This is the outer dict with "type" and "data"
                            )
                            # order_update_wrapper.data is dict[str, Any] as per
                            # HyperliquidRawWsOrderUpdate
                            # This 'data' is what needs to be parsed into HyperliquidRawOrder
                            # or handled if it's a list of fills (which is not typical
                            # for this wrapper's data field)

                            # The previous logic for order_wrapper.data was:
                            # Union[HyperliquidRawOrder, list[HyperliquidRawWsFillEvent],
                            #       dict[str, Any]]
                            # However, HyperliquidRawWsOrderUpdate.data is dict[str,Any].
                            # The intention is that this 'data' dict is the *actual* order details.

                            # If order_update_wrapper.data itself is supposed to be an Order:
                            _handle_order_event = (
                                HyperliquidWsRawMessageHandler.handle_user_order_event_payload
                            )
                            validated_order_details = _handle_order_event(
                                order_update_wrapper.data  # This is dict[str, Any]
                            )
                            current_event_payload_for_handler = validated_order_details.model_dump(
                                mode="json"
                            )

                        elif event_type_str == "positionUpdate":
                            _handle_pos_update = HyperliquidWsRawMessageHandler.handle_user_position_update_event_payload
                            validated_position_update = _handle_pos_update(event_item_dict)
                            current_event_payload_for_handler = (
                                validated_position_update.model_dump(mode="json")
                            )

                        else:
                            logger.debug(
                                f"[{self.exchange_name}] Unhandled userEvent type: "
                                f"{event_type_str}. Passing raw item: {event_item_dict}"
                            )
                            current_event_payload_for_handler = event_item_dict

                        if current_event_payload_for_handler:
                            await app_handler(current_event_payload_for_handler, message)

                    except (APIError, ValidationError) as e_user_event_item:
                        logger.error(
                            f"[{self.exchange_name}] Error processing userEvent item "
                            f"(type: {event_type_str}): {e_user_event_item}. "
                            f"Item: {event_item_dict}. Skipping item."
                        )
                        continue  # Skip to next item in userEvents list

            elif channel == "allMids":
                # Ensure raw_data is a dictionary before proceeding
                if not isinstance(raw_data, dict):
                    logger.warning(
                        f"[{self.exchange_name}] 'allMids' channel data is not a dict or is None. "
                        f"Type: {type(raw_data)}. Data: {raw_data!r}. Skipping."  # pyright: ignore [reportUnknownArgumentType]
                    )
                    raise APIError(
                        f"allMids data not dict or is None: {type(raw_data)}",  # pyright: ignore [reportUnknownArgumentType]
                        code=APIErrorCode.INVALID_RESPONSE.value,
                    )

                # At this point, raw_data is confirmed to be a dict.
                # Explicitly cast for the type checker after the runtime check.
                raw_data_dict = cast(dict[str, Any], raw_data)

                # Validate the payload using the WsRawMessageHandler
                validated_all_mids = HyperliquidWsRawMessageHandler.handle_all_mids_payload(
                    raw_data_dict
                )
                # The model_dump on a RootModel returns the root type, which is dict here.
                # Explicitly cast to satisfy type checker if it struggles with
                # RootModel.model_dump()
                payload_for_handler = cast(
                    dict[str, Any], validated_all_mids.model_dump(mode="json")
                )
                await app_handler(payload_for_handler, message)

            elif channel == "pong" or channel == "subscriptionResponse":
                logger.debug(f"[{self.exchange_name}] Control message on '{channel}': {message}")
                # Control messages might have simple payloads or be None.
                # Pass raw_data if dict, or an empty dict if None/not dict.
                payload_for_control_handler = (
                    cast(dict[str, Any], raw_data) if isinstance(raw_data, dict) else {}
                )
                await app_handler(payload_for_control_handler, message)
            else:
                logger.debug(
                    f"[{self.exchange_name}] Unhandled channel '{channel}' by specific "
                    f"validation, passing raw data if dict. Msg: {message}"
                )
                payload_for_unhandled_handler = (
                    cast(dict[str, Any], raw_data) if isinstance(raw_data, dict) else {}
                )
                await app_handler(payload_for_unhandled_handler, message)

        except APIError as e:  # Catch APIErrors raised by handlers or direct checks
            logger.error(
                f"[{self.exchange_name}] APIError in WS routing for {channel}: {e.message}",
                exc_info=True,
            )
        except ValidationError as e_val:  # Catch Pydantic validation errors from direct use if any
            logger.error(
                f"[{self.exchange_name}] Unexpected Pydantic ValidationErr for {channel}: {e_val}",
                exc_info=True,
            )
        except Exception as e_app:  # Catch errors from within app_handler itself
            logger.error(
                f"[{self.exchange_name}] Error in app_handler for {channel}: {e_app}", exc_info=True
            )

    async def connect_websocket(self) -> None:
        """Establish the WebSocket connection using the base class logic."""
        await super().connect_websocket()  # Call the base method which uses WebSocketManager

    async def get_balances(self) -> dict[str, SpotBalance]:
        """Get account balances."""
        if not self._wallet_address:  # Keep pre-condition check for clarity if service relies on it
            raise APIError(
                "Wallet address required for get_balances.",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )
        try:
            # Delegate to service to get raw data
            raw_clearinghouse_state: HyperliquidRawClearinghouseState = (
                await self.account_service.get_balances_raw()
            )
            # Transform raw data to internal domain model
            return self._hl_mapper.map_raw_clearinghouse_state_to_spot_balances(
                raw_clearinghouse_state
            )
        except ValidationError as e:
            logger.error(
                f"[{self.exchange_name}] Error validating/mapping clearinghouseState for balances: {e}"
            )
            raise APIError(
                f"Failed to validate/map balance data structure: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e
        except APIError:  # Re-raise APIErrors from service or mapper
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
        if not self._wallet_address:  # Keep pre-condition check
            raise APIError(
                "Wallet address required for get_positions.",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )
        try:
            # Delegate to service to get raw data
            raw_clearinghouse_state: HyperliquidRawClearinghouseState = (
                await self.account_service.get_positions_raw()
            )
            # Transform raw data to internal domain model
            positions_dict = self._hl_mapper.map_raw_clearinghouse_state_to_derivative_positions(
                raw_clearinghouse_state
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
        except APIError:  # Re-raise APIErrors from service or mapper
            raise
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error getting positions: {e}", exc_info=True
            )
            raise APIError(
                f"Unexpected error getting positions: {e}",
                code=APIErrorCode.SERVER_ERROR.value,  # Or UNKNOWN
                original_exception=e,
            ) from e

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Get open orders."""
        if not self._wallet_address:  # Keep pre-condition check
            raise APIError(
                "Wallet address required for get_open_orders.",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )
        try:
            # Delegate to service to get raw data
            raw_open_orders_response: HyperliquidRawOpenOrdersResponse = (
                await self.account_service.get_open_orders_raw()
            )
            # Transform raw data to internal domain model
            open_orders: list[Order] = []
            for order_obj in raw_open_orders_response.items:
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
        except APIError:  # Re-raise APIErrors from service or mapper
            raise
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Error getting open orders: {e}", exc_info=True)
            raise APIError(
                f"Failed to get open orders: {e}",
                code=APIErrorCode.SERVER_ERROR.value,  # Or UNKNOWN
                original_exception=e,
            ) from e

    async def get_ticker(self, symbol: str) -> Ticker:
        """Delegates to HyperliquidMarketDataService to get ticker data, then maps to internal Ticker."""
        raw_asset_ctx = await self.market_data.get_ticker(symbol)
        if raw_asset_ctx:
            return self._hl_mapper.map_raw_ctx_to_ticker(raw_asset_ctx)
        raise APIError(
            f"Ticker data not found for symbol {symbol}", code=APIErrorCode.SYMBOL_NOT_FOUND.value
        )

    async def get_order_book(self, symbol: str, depth: int | None = None) -> OrderBook:
        """Delegates to HyperliquidMarketDataService to get L2 order book data, then maps to internal OrderBook."""
        # The 'depth' parameter is not typically used by Hyperliquid's L2Book /info endpoint,
        # so it's removed from the API client facade here.
        # The service method handles the actual request structure.
        raw_l2_book = await self.market_data.get_order_book(symbol=symbol)
        return self._hl_mapper.map_raw_order_book(raw_l2_book, depth=depth)

    async def get_recent_trades(self, symbol: str, limit: int | None = 50) -> list[Trade]:
        """Delegates to HyperliquidMarketDataService to get recent public trades, then maps to internal Trades."""
        # The 'limit' parameter is not part of HL /info request for recentTrades.
        # The service method handles the actual request structure.
        # The service will return all available, then we limit here if needed.
        raw_public_trades = await self.market_data.get_recent_trades(symbol=symbol)
        internal_trades = self._hl_mapper.map_raw_trades(raw_public_trades)
        if limit is not None and limit > 0:
            return internal_trades[:limit]
        return internal_trades

    async def get_funding_rate(self, symbol: str) -> FundingRate | None:
        """Delegates to HyperliquidMarketDataService to get funding rate related data, then maps."""
        # HL funding rate is part of the asset context
        raw_asset_ctx = await self.market_data.get_funding_rate(symbol=symbol)
        if raw_asset_ctx:
            return self._hl_mapper.map_raw_ctx_to_funding_rate(raw_asset_ctx)
        return None

    async def transfer(
        self, asset: str, amount: Decimal, from_account: str, to_account: str
    ) -> dict[str, Any]:
        """Initiates an L2 USDC transfer on Hyperliquid.
        `from_account` is ignored as Hyperliquid L2 transfers are from the main account.
        Returns the raw exchange response, which is typically a dict.
        """
        if asset.upper() != "USDC":
            raise ValueError("Hyperliquid L2 transfers are currently only supported for USDC.")
        if not to_account:
            raise ValueError(
                "Destination address (to_account) is required for Hyperliquid L2 transfer."
            )

        response_raw_model: HyperliquidRawExchangeResponse | None = None
        try:
            # Delegate to service
            response_raw_model = await self.account_service.transfer_raw(
                asset=asset, amount=amount, to_account=to_account
            )

            if (
                not response_raw_model
                or not response_raw_model.data
                or not response_raw_model.data.statuses
            ):
                logger.warning(
                    f"[{self.exchange_name}] L2 Transfer response 'ok' via service, but data or "
                    f"statuses list is missing/empty. Raw: {response_raw_model!r}"
                )
                raise APIError(
                    "L2 Transfer 'ok' but no status details returned.",
                    code=APIErrorCode.UNKNOWN.value,
                )

            first_status_obj_raw = response_raw_model.data.statuses[0]
            error_to_raise_from_status: APIError | None = None

            arg_for_handler: dict[str, Any] | str
            if isinstance(first_status_obj_raw, str):
                arg_for_handler = first_status_obj_raw
            elif isinstance(first_status_obj_raw, HyperliquidRawExchangeStatusObject):  # pyright: ignore[reportUnnecessaryIsInstance]
                arg_for_handler = first_status_obj_raw.model_dump(by_alias=True, exclude_none=True)
            else:
                err_msg = (  # type: ignore[unreachable]
                    f"[{self.exchange_name}] Unexpected type for first_status_obj_raw in Transfer: "
                    f"{type(first_status_obj_raw)}. Raw: {first_status_obj_raw!r}. "
                    f"Indicates flaw in Pydantic validation or unexpected API response."
                )
                logger.critical(err_msg)
                raise RuntimeError(err_msg)

            processed_status = HyperliquidResponseHandler.process_first_exchange_status(
                arg_for_handler, "L2 Transfer"
            )

            if isinstance(processed_status, HyperliquidSuccessfulOrderStatus):
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
                logger.info(
                    f"[{self.exchange_name}] L2 Transfer appears successful. Processed status: "
                    f"{processed_status.model_dump_json(exclude_none=True)!r}"
                )
                return {"status": "success", "data": processed_status.model_dump(exclude_none=True)}
            else:  # HyperliquidErrorStatus
                logger.warning(
                    f"[{self.exchange_name}] L2 Transfer failed with error: "
                    f"{processed_status.message}"
                )
                error_to_raise_from_status = self.error_mapper.map_string_error(
                    processed_status.message,
                    http_status=200,  # Assuming 200 OK if we got this far
                )

            if error_to_raise_from_status:
                raise error_to_raise_from_status

            # Fallback: This should ideally not be reached
            logger.warning(
                f"[{self.exchange_name}] L2 Transfer status unclear after processing logic. "
                f"Processed: {processed_status!r}. Raw response used for handler: "
                f"{arg_for_handler!r}"
            )
            raise APIError(
                "L2 Transfer status unclear after processing.",
                code=APIErrorCode.UNKNOWN.value,
            )

        except ValidationError as e:  # Should be caught by service ideally
            logger.error(
                f"[{self.exchange_name}] Failed to validate L2 transfer response (service error?): {e}. "
                f"Raw from service: {response_raw_model!r}"
            )
            raise APIError(
                f"Invalid response after L2 transfer: {e}", code=APIErrorCode.UNKNOWN.value
            ) from e
        except APIError:  # Re-raise APIErrors from service
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
        """Initiates a withdrawal to L1 on Hyperliquid.
        Returns the raw exchange response, which is typically a dict.
        `network` parameter is not used by Hyperliquid for withdrawals.
        """
        if not address:
            raise ValueError("Destination address is required for withdrawal.")

        response_raw_model: HyperliquidRawExchangeResponse | None = None
        try:
            # Delegate to service
            response_raw_model = await self.account_service.withdraw_raw(
                asset=asset, amount=amount, address=address
            )

            if (
                not response_raw_model
                or not response_raw_model.data
                or not response_raw_model.data.statuses
            ):
                logger.warning(
                    f"[{self.exchange_name}] Withdraw response 'ok' via service but data or statuses "
                    f"list is missing/empty. Raw: {response_raw_model!r}"
                )
                raise APIError(
                    "Withdrawal status unclear: 'ok' but no status details provided.",
                    code=APIErrorCode.EXCHANGE_SPECIFIC.value,
                )

            first_status_obj_raw = response_raw_model.data.statuses[0]
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
            else:  # HyperliquidRawExchangeStatusObject
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
                elif status_object.success:  # General success message
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
                        f"structure: {status_object.model_dump_json()!r}. Raw: {response_raw_model!r}"
                    )
                    raise APIError(
                        "Withdrawal status unclear: Unrecognized success object structure.",
                        code=APIErrorCode.EXCHANGE_SPECIFIC.value,
                    )
        except ValidationError as e:  # Should be caught by service ideally
            logger.error(
                f"[{self.exchange_name}] Validation error processing withdraw response (service error?): {e}. "
                f"Raw from service: {response_raw_model!r}"
            )
            raise APIError(
                f"Failed to validate withdraw response: {e}",
                code=APIErrorCode.EXCHANGE_SPECIFIC.value,  # Or UNKNOWN
                original_exception=e,
            ) from e
        except APIError:  # Re-raise APIErrors from service
            raise
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error during withdraw for {asset} "
                f"to {address}: "
                f"{e}. Raw from service: {response_raw_model!r}",
                exc_info=True,
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
        # Ensure end_time_ms is current if not provided, matching original logic
        end_time_ms = int(end_time.timestamp() * 1000) if end_time else int(time.time() * 1000)

        if self._wallet_address is None:  # Keep pre-condition check
            raise APIError(
                "Wallet address is required for order history.",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        raw_order_history_list_from_service: list[HyperliquidRawHistoricalOrderResponse] | None = (
            None  # For logging in except
        )
        orders_list: list[Order] = []
        try:
            # Delegate to service. Service is typed to return list, not Optional[list].
            raw_order_history_list_from_service = await self.account_service.get_order_history_raw(
                start_time_ms=start_time_ms, end_time_ms=end_time_ms
            )
            # Iterate directly, assuming service returns a list (empty if no history)
            for raw_status_response in raw_order_history_list_from_service:
                try:
                    raw_order = raw_status_response.order
                    # Historical orders from this endpoint might not have separate trigger info
                    trigger_info = None
                    internal_order = (
                        self._hl_order_mapper.transform_raw_historical_order_to_internal(
                            raw_historical_order=raw_order,
                            trigger=trigger_info,
                        )
                    )
                    if internal_order:
                        orders_list.append(internal_order)
                except (
                    ValidationError,
                    ValueError,
                ) as e_item:  # Catch mapping/validation errors per item
                    logger.warning(
                        f"[{self.exchange_name}] Skipping order history item due to "
                        f"validation/transform error: {e_item}. Raw: {raw_status_response!r}"
                    )
                    continue

            # Apply filters after fetching and transforming all orders from the time range
            if symbol:
                orders_list = [o for o in orders_list if o.symbol == symbol]
            if order_id:  # Filter by exchange_order_id
                orders_list = [o for o in orders_list if o.exchange_order_id == order_id]
            if client_order_id:
                orders_list = [o for o in orders_list if o.client_order_id == client_order_id]
            if limit is not None and limit > 0:
                orders_list = orders_list[:limit]  # Apply limit after other filters
            return orders_list
        except APIError:  # Re-raise APIErrors from service or mapper
            raise
        except (
            ValidationError,
            ValueError,
        ) as e_outer:  # Catch validation/value errors during service call / initial processing
            logger.error(
                f"[{self.exchange_name}] Error processing order history response (service error?): {e_outer}. "
                f"Raw from service (if available): {raw_order_history_list_from_service!r}",
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
        if not self._wallet_address:  # Keep pre-condition check
            raise APIError(
                "Wallet address required for fetching trade history",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )
        raw_fills_response: HyperliquidRawUserFillsResponse | None = None
        try:
            # Delegate to service
            raw_fills_response = await self.account_service.get_trade_history_raw()

            trades: list[Trade] = []
            # Ensure raw_fills_response and its root attribute are not None before iteration
            if raw_fills_response and raw_fills_response.root:
                for raw_fill in (
                    raw_fills_response.root
                ):  # raw_fills_response is HyperliquidRawUserFillsResponse
                    try:
                        if symbol is not None and raw_fill.coin != symbol:
                            continue
                        # Cast raw_fill (HyperliquidRawUserFill) to HyperliquidRawFill for the mapper
                        internal_trade = self._hl_mapper.transform_raw_fill_to_internal(
                            cast(
                                HyperliquidRawFill, raw_fill
                            )  # Service returns HLRawUserFillsResponse(root:list[HLRawUserFill])
                            # Mapper expects HLRawFill.
                            # They are very similar, but this cast is key.
                        )
                        trades.append(internal_trade)
                    except (ValidationError, ValueError) as e_item:
                        logger.warning(
                            f"[{self.exchange_name}] Skipping fill due to validation/transform "
                            f"error: {e_item}. Data: {raw_fill!r}"
                        )
                        continue
            else:  # Handle case where raw_fills_response or raw_fills_response.root is None
                logger.warning(
                    f"[{self.exchange_name}] No trade history data received from service or data is empty."
                )

            trades.sort(key=lambda t: t.executed_at, reverse=True)
            return trades[:limit]
        except APIError:  # Re-raise APIErrors from service or mapper
            raise
        except Exception as e_unexp:  # Catch any other unexpected error
            logger.error(
                f"[{self.exchange_name}] Unexpected error getting trade history: {e_unexp}. "
                f"Raw from service: {raw_fills_response!r}",  # Log raw data if available
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

    async def get_market_data(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 100,  # Adhere to ExchangeAPI signature
    ) -> list[Candle]:
        """Fetches historical market data (OHLCV/Kline) for a specific symbol and timeframe.

        Translates 'limit' into start_time_ms and end_time_ms for the Hyperliquid service call.
        """

        current_time_ms = int(time.time() * 1000)
        # Use the imported timeframe_to_ms helper
        timeframe_duration_ms = timeframe_to_ms(timeframe)  # Defaulting to 1 minute on parse error

        # Calculate end_time_ms (now)
        end_t_ms = current_time_ms

        # Calculate start_time_ms based on limit and timeframe duration
        # Fetches `limit` candles ending at `end_t_ms`
        start_t_ms = end_t_ms - (limit * timeframe_duration_ms)

        # The market_data service expects non-optional int for start_time_ms and end_time_ms.
        raw_candle_snapshot = await self.market_data.get_market_data(
            symbol=symbol, interval=timeframe, start_time_ms=start_t_ms, end_time_ms=end_t_ms
        )

        # The mapper also needs symbol and interval (timeframe)
        return self._hl_candle_mapper.map(
            raw_snapshot=raw_candle_snapshot, symbol=symbol, interval=timeframe
        )

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
    ) -> Order:  # Changed return type to Order, as ORDER_NOT_FOUND is an APIError
        """Fetches the status of a specific order."""
        if not self._wallet_address:  # Keep pre-condition check
            raise APIError(
                "Wallet address required for fetching order status",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )
        if not order_id:  # Ensure order_id is provided
            raise ValueError("order_id is required for get_order_status on Hyperliquid.")

        converted_order_id: int
        try:
            converted_order_id = int(order_id)
        except ValueError as e_val_int:  # Catches if int(order_id) fails
            logger.error(
                f"[{self.exchange_name}] Invalid order_id format for get_order_status: '{order_id}'. Error: {e_val_int}"
            )
            raise APIError(
                f"Invalid order_id format: '{order_id}'",
                code=APIErrorCode.INVALID_PARAMS.value,
                original_exception=e_val_int,
            ) from e_val_int

        try:
            # Delegate to service. The service expects an int for order_id.
            raw_historical_order_response: HyperliquidRawHistoricalOrderResponse = (
                await self.account_service.get_order_status_raw(order_id=converted_order_id)
            )

            # The service method get_order_status_raw handles ORDER_NOT_FOUND by raising APIError.
            # Map the raw order data to the internal Order model.
            return self._hl_order_mapper.transform_raw_historical_order_to_internal(
                raw_historical_order=raw_historical_order_response.order,
                trigger=None,  # Assuming no separate trigger info from this specific endpoint
            )
        except APIError as e:  # Re-raise APIErrors from service or mapper (incl. ORDER_NOT_FOUND)
            logger.error(
                f"[{self.exchange_name}] API error in get_order_status for "
                f"order_id '{order_id}': {e.message}",
                exc_info=(e.code not in [APIErrorCode.ORDER_NOT_FOUND.value]),
            )
            raise
        except (
            ValidationError
        ) as e_val:  # Catches Pydantic errors from service raw model validation or mapping
            logger.error(
                f"[{self.exchange_name}] Validation error in get_order_status (service/mapper): {e_val}. "
            )
            raise APIError(
                "Pydantic validation error processing order status.",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_val,
            ) from e_val
        # General ValueError should now be less likely here if int() conversion is separate
        # and ValidationError is caught. But kept for broader unexpected value issues.
        except ValueError as e_gen_val:  # Catch other ValueErrors not from int()
            logger.error(
                f"[{self.exchange_name}] Unexpected ValueError in get_order_status for "
                f"order_id '{order_id}': {e_gen_val}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected ValueError processing order status for order_id '{order_id}': {e_gen_val}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_gen_val,
            ) from e_gen_val
        except Exception as e_unexp:
            logger.error(
                f"[{self.exchange_name}] Unhandled error fetching order status for "
                f"order_id '{order_id}': {e_unexp}",
                exc_info=True,
            )
            raise APIError(
                message=(
                    f"Unexpected error processing order status for order_id '{order_id}': {e_unexp}"
                ),
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unexp,
            ) from e_unexp

    async def get_account_summary(self) -> MarginAccountSummary | None:
        """Fetches user state and maps it to MarginAccountSummary."""
        if not self._wallet_address:  # Keep pre-condition check
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

        raw_user_state_from_service: HyperliquidRawClearinghouseState | None = (
            None  # For logging in except
        )
        try:
            # Delegate to service. Service is typed to return HyperliquidRawClearinghouseState,
            # and should raise APIError on failure rather than returning None.
            raw_user_state_from_service = await self.account_service.get_account_summary_raw()

            # According to type hints, raw_user_state_from_service should not be None here if the service call succeeded.
            # The service is expected to raise an APIError if it cannot fetch the data.
            # If it somehow returns None without an error, the mapper will likely fail.

            margin_summary = self._hl_mapper.map_raw_clearinghouse_state_to_margin_summary(
                raw_state=raw_user_state_from_service  # This is HyperliquidRawClearinghouseState
            )
            return margin_summary

        except APIError as e:  # Re-raise APIErrors from service or mapper
            logger.error(f"[{self.exchange_name}] API Error getting account summary: {e.message}")
            if e.code == APIErrorCode.AUTHENTICATION_FAILED.value:
                logger.warning(
                    f"[{self.exchange_name}] Authentication failed for account summary (service reported), returning None."
                )
                return None
            raise  # Re-raise other APIErrors
        except ValidationError as ve:  # Handles errors from the mapper primarily.
            logger.error(
                f"[{self.exchange_name}] Pydantic ValidationError mapping account summary: {ve}. Raw from service: {raw_user_state_from_service!r}",
                exc_info=True,
            )
            raise APIError(
                f"Validation error mapping account summary: {ve}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=ve,
            ) from ve
        except (
            Exception
        ) as e:  # Catch any other unexpected error during mapping or other logic here.
            logger.error(
                f"[{self.exchange_name}] Unexpected error in get_account_summary: {e}. "
                f"Raw from service (if available): {raw_user_state_from_service!r}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error getting account summary: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e

    async def _handle_websocket_message(self, message: dict[str, Any]) -> None:
        """Process a single message received from the WebSocket.

        This method is called by the underlying WebSocketManager (via the
        ExchangeAPI base class) when a new message arrives. It logs the
        receipt and then delegates to the `_route_ws_message` method for
        specific parsing, validation, and handling based on message content.

        Args:
            message: The raw dictionary message received from the WebSocket.
        """
        if not message:
            logger.warning(f"[{self.exchange_name}] Received empty WebSocket message. Skipping.")
            return

        # Basic check for common error messages or unexpected top-level structure
        # For HyperLiquid, specific error channels are handled in _route_ws_message

        await self._route_ws_message(message)
