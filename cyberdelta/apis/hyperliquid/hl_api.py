from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from typing import Any, cast

import aiohttp
from pydantic import HttpUrl, ValidationError

from cyberdelta.apis.base.authenticator_interface import (
    AuthenticatedRequestComponents,
    IAuthenticator,
)
from cyberdelta.apis.base.exchange_api import ExchangeAPI, MessageHandler
from cyberdelta.apis.connectivity.connectivity_models import HttpClientConfig
from cyberdelta.apis.connectivity.http_client import HttpClient, ParsedJsonResponse
from cyberdelta.apis.connectivity.rate_limiter_service import RateLimiterService
from cyberdelta.apis.hyperliquid.hl_auth import HyperliquidEip712Authenticator
from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper
from cyberdelta.apis.hyperliquid.hl_mapper import (
    HyperliquidCandleMapper,
    HyperliquidMapper,
    HyperliquidOrderMapper,
    HyperliquidUserFillMapper,
)
from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.hl_response_handler import (
    HyperliquidResponseHandler,
    RawJsonResponse,
)
from cyberdelta.apis.hyperliquid.hl_ws_raw_message_handler import HyperliquidWsRawMessageHandler
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawMetaAndAssetCtxsResponse,
)
from cyberdelta.apis.hyperliquid.services.hl_account_service import HyperliquidAccountService
from cyberdelta.apis.hyperliquid.services.hl_market_data_service import HyperliquidMarketDataService
from cyberdelta.apis.hyperliquid.services.hl_trading_service import HyperliquidTradingService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models import (
    DerivativePosition,
    FundingRate,
    MarginAccountSummary,
    SpotBalance,
    Ticker,
    Trade,
)
from cyberdelta.core.models.enums import (
    OrderSide,
    OrderType,
    TimeInForce,
)
from cyberdelta.core.models.market import Candle, OrderBook
from cyberdelta.core.models.market.order import (
    CancelOrderResult,
    Order,
)
from cyberdelta.core.models.operations import Transfer, Withdrawal
from cyberdelta.utils.logging_config import get_logger
from cyberdelta.utils.parsing import timeframe_to_ms

logger = get_logger(__name__)


class HyperliquidAPI(ExchangeAPI):
    """API Client for Hyperliquid DEX."""

    BASE_URL = "https://api.hyperliquid.xyz"
    INFO_URL = "https://info.hyperliquid.xyz"
    WS_URL = "wss://api.hyperliquid.xyz/ws"
    CHAIN_ID = 1337

    account_service: HyperliquidAccountService
    trading_service: HyperliquidTradingService  # Declare trading_service attribute
    market_data_service: HyperliquidMarketDataService  # Added market_data_service attribute

    # Adapter method to match MarketDataHttpClientRequesterSig
    async def _market_data_requester_adapter(
        self,
        method: str,
        endpoint_path: str,
        data: dict[str, Any] | None,
        is_info_endpoint: bool | None,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        is_signed: bool = False,
    ) -> tuple[ParsedJsonResponse | None, int, Mapping[str, str]]:
        """Adapter for self._request to match MarketDataHttpClientRequesterSig."""
        request_data: dict[str, Any] | None = data

        actual_content, status, actual_headers = await self._request(
            method=method,
            endpoint=endpoint_path,
            params=params,
            data=request_data,
            headers=headers,
            is_signed=is_signed,
            is_public_info_endpoint=is_info_endpoint if is_info_endpoint is not None else False,
        )
        return actual_content, status, actual_headers

    async def _info_request_wrapper(
        self,
        method: str,
        endpoint_path: str,
        data: dict[str, Any],
        authenticator: IAuthenticator | None,
        rate_limiter_service: RateLimiterService,
        is_signed: bool,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        request_timeout: float | None = None,
    ) -> RawJsonResponse | None:
        if not hasattr(self, "_info_http_client"):
            logger.error(
                f"[{self.exchange_name}] _info_http_client not initialized when wrapper called."
            )
            return None

        (
            content_raw,
            status_code_raw,
            processed_headers_raw,
            raw_headers_raw,
        ) = await self._info_http_client.request(
            method=method,
            endpoint_path=endpoint_path,
            rate_limiter_service=rate_limiter_service,
            authenticator=authenticator,
            params=params,
            data=data,
            headers=headers,
            is_signed=is_signed,
            request_timeout=request_timeout,
        )

        logger.debug(
            f"[_info_request_wrapper] Status: {status_code_raw}, "
            f"Processed Headers: {processed_headers_raw}, Raw Headers: {raw_headers_raw}"
        )

        if content_raw is None or isinstance(content_raw, dict | list):
            return cast(RawJsonResponse | None, content_raw)
        else:
            logger.error(
                f"[{self.exchange_name}] _info_request_wrapper received unexpected string "
                f"content from HttpClient: {str(content_raw)[:100]}..."
            )
            return None

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

        self._hyperliquid_error_mapper = HyperliquidErrorMapper()

        self._hl_request_builder = HyperliquidRequestBuilder()
        self._hl_response_handler = HyperliquidResponseHandler()

        self._asset_to_index_cache: dict[str, int] = {}
        self._hl_mapper = HyperliquidMapper()
        self._hl_order_mapper = HyperliquidOrderMapper()
        self._hl_candle_mapper = HyperliquidCandleMapper()
        self._hl_user_fill_mapper = HyperliquidUserFillMapper()

        self.market_data_service = HyperliquidMarketDataService(
            http_client_requester=self._market_data_requester_adapter,
            request_builder=self._hl_request_builder,
            response_handler=self._hl_response_handler,
            exchange_name=self.exchange_name,
            info_url=self.INFO_URL,
        )

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

        http_client_raw_config = api_config.get("http_client", {})
        http_client_config = HttpClientConfig(
            rest_endpoint=HttpUrl(self.BASE_URL),
            default_request_timeout=http_client_raw_config.get("default_request_timeout", 10.0),
            max_retries=http_client_raw_config.get("max_retries", 3),
            retry_delay_seconds=http_client_raw_config.get("retry_delay_seconds", 5.0),
        )
        self._http_client = HttpClient(self.exchange_name, http_client_config)

        info_http_client_raw_config_for_info_client = api_config.get("http_client", {})
        info_client_config_obj = HttpClientConfig(
            rest_endpoint=HttpUrl(self.INFO_URL),
            default_request_timeout=info_http_client_raw_config_for_info_client.get(
                "default_request_timeout", 10.0
            ),
            max_retries=info_http_client_raw_config_for_info_client.get("max_retries", 3),
            retry_delay_seconds=info_http_client_raw_config_for_info_client.get(
                "retry_delay_seconds", 5.0
            ),
        )
        self._info_http_client = HttpClient(
            exchange_name=f"{self.exchange_name}_info",
            config=info_client_config_obj,
        )

        self.account_service = HyperliquidAccountService(
            http_client_requester=self._market_data_requester_adapter,
            request_builder=self._hl_request_builder,
            response_handler=self._hl_response_handler,
            authenticator=self._hl_authenticator,
            exchange_name=self.exchange_name,
            info_url=self.INFO_URL,
            wallet_address=self._wallet_address,
            mapper=self._hl_mapper,
            order_mapper=self._hl_order_mapper,
            user_fill_mapper=self._hl_user_fill_mapper,
        )

        self.trading_service = HyperliquidTradingService(
            exchange_http_client_requester=self._request,
            info_http_client_requester=self._info_request_wrapper,
            request_builder=self._hl_request_builder,
            response_handler=self._hl_response_handler,
            authenticator=self._hl_authenticator,
            exchange_name=self.exchange_name,
            wallet_address=self._wallet_address,
            get_asset_index_callable=self._get_asset_index,
            order_mapper=self._hl_order_mapper,
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
            f"{self._hl_authenticator}, type: {type(self._hl_authenticator)}"
        )  # DEBUG PRINT
        if not self._hl_authenticator:
            logger.error(
                f"[{self.exchange_name}] Attempt to call signed endpoint ({method} {path}) "
                "without configured HL authenticator."
            )
            raise APIError(
                "HL authenticator not initialized (e.g., missing/invalid private key).",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        current_headers = self.default_headers.copy()

        print(
            f"[DEBUG HL_API _authenticate] About to await prepare_request. "
            f"Authenticator: {self._hl_authenticator}",
            flush=True,
        )
        auth_components: AuthenticatedRequestComponents = (
            await self._hl_authenticator.prepare_request(
                method, path, params, data, current_headers
            )
        )

        print("[DEBUG HL_API _authenticate] Finished awaiting prepare_request.", flush=True)

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
        request_payload_model = HyperliquidRequestBuilder.build_info_request_payload()
        request_payload_data_dict = request_payload_model.model_dump(
            by_alias=True, exclude_none=True
        )

        try:
            response_content_raw, _, _, _ = await self._info_http_client.request(
                method="POST",
                endpoint_path="/info",
                data=request_payload_data_dict,
                rate_limiter_service=self._rate_limiter_service,
            )
        except APIError as e_api:
            logger.error(
                f"[{self.exchange_name}] API Error fetching asset index for {symbol}: {e_api}"
            )
            raise APIError(
                f"Failed to fetch asset index for symbol '{symbol}': {e_api}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_api,
            ) from e_api

        try:
            if response_content_raw is None:
                logger.error(
                    f"[{self.exchange_name}] Received None response from _info_http_client.request "
                    f"for metaAndAssetCtxs."
                )
                raise APIError(
                    "No data received for market metadata.",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                )

            validated_response: HyperliquidRawMetaAndAssetCtxsResponse = (
                HyperliquidResponseHandler.handle_info_meta_and_asset_ctxs_response(
                    cast(RawJsonResponse, response_content_raw)
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
                f"Raw: {response_content_raw!r}"
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
        """
        Handle raw WebSocket message from WebSocketManager, then route it.
        This method is called by the WebSocketManager.
        """
        # Following the pattern from BackpackAPI, directly route to _route_ws_message.
        # Add any pre-processing here if Hyperliquid requires it for common message envelopes.
        await self._route_ws_message(message)

    async def _route_ws_message(self, message: dict[str, Any]) -> None:
        """
        Route incoming WebSocket messages from Hyperliquid.
        Validates raw payloads using HyperliquidWsRawMessageHandler before processing.
        """
        channel: str | None = message.get("channel")
        raw_data_any: Any = message.get("data")  # Keep as Any initially

        if not channel:
            logger.debug(f"[{self.exchange_name}] Unroutable WS message (no channel): {message}")
            return

        if channel in ["pong", "subscriptionResponse"]:
            logger.debug(f"[{self.exchange_name}] Control message on '{channel}': {message}")
            return

        topic_key_for_handler = channel
        if channel == "l2Book":
            if isinstance(raw_data_any, dict):
                raw_data_dict = cast(dict[str, Any], raw_data_any)
                coin_from_data_any: Any = raw_data_dict.get("coin")
                if isinstance(coin_from_data_any, str):
                    topic_key_for_handler = f"{channel}:{coin_from_data_any}"
            else:
                logger.warning(
                    f"[{self.exchange_name}] Expected dict for 'l2Book' data to derive topic key, "
                    f"received other type. Msg: {message}"
                )
        elif channel == "trades":
            coin_for_topic_str: str | None = None
            if isinstance(message.get("coin"), str):
                coin_for_topic_str = message.get("coin")
            elif isinstance(raw_data_any, list):
                checked_list_for_topic_derivation: list[Any] = raw_data_any
                if checked_list_for_topic_derivation:
                    first_item_for_topic_any: Any = checked_list_for_topic_derivation[0]
                    if isinstance(first_item_for_topic_any, dict):
                        first_item_dict = cast(dict[str, Any], first_item_for_topic_any)
                        coin_from_item_any: Any = first_item_dict.get("coin")
                        if isinstance(coin_from_item_any, str):
                            coin_for_topic_str = coin_from_item_any

            if coin_for_topic_str:
                topic_key_for_handler = f"{channel}:{coin_for_topic_str}"
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

        if raw_data_any is None:
            logger.warning(f"[{self.exchange_name}] WS '{channel}' has no data. Msg: {message}")
            return

        try:
            payload_for_handler: dict[str, Any] | None = None

            if channel == "l2Book":
                if not isinstance(raw_data_any, dict):
                    raise APIError(
                        "l2Book data not dict",
                        code=APIErrorCode.INVALID_RESPONSE.value,
                    )
                validated_model = HyperliquidWsRawMessageHandler.handle_l2book_payload(
                    cast(dict[str, Any], raw_data_any)
                )
                payload_for_handler = validated_model.model_dump(mode="json")

            elif channel == "trades":
                if not isinstance(raw_data_any, list):
                    raise APIError(
                        "Trades data not list",
                        code=APIErrorCode.INVALID_RESPONSE.value,
                    )

                # Explicitly type the list after check, elements are still Any
                checked_list_of_any: list[Any] = raw_data_any
                typed_trades_input_list: list[dict[str, Any]] = []
                for item_loop_var_any in checked_list_of_any:
                    if not isinstance(item_loop_var_any, dict):
                        logger.warning(
                            f"[{self.exchange_name}] Trades list item not dict: "
                            f"{item_loop_var_any}. Msg: {message}. Skipping item."
                        )
                        continue
                    # Cast here for Pyright
                    item_dict = cast(dict[str, Any], item_loop_var_any)
                    typed_trades_input_list.append(item_dict)

                if not typed_trades_input_list and raw_data_any:
                    logger.warning(
                        f"[{self.exchange_name}] All items in trades list were invalid. "
                        f"Original raw_data: {raw_data_any}"
                    )
                    return  # payload_for_handler remains None, handled below

                if typed_trades_input_list:
                    validated_trade_models = (
                        HyperliquidWsRawMessageHandler.handle_public_trades_payload(
                            typed_trades_input_list
                        )
                    )
                    # For multiple trades, app_handler is called for each
                    # So payload_for_handler isn't used for the list itself here
                    for trade_model in validated_trade_models:
                        single_trade_payload = trade_model.model_dump(mode="json")
                        await app_handler(single_trade_payload, message)
                    return  # All handled, exit before final app_handler call

            elif channel == "userEvents":
                if not isinstance(raw_data_any, list):
                    raise APIError(
                        "userEvents data not list",
                        code=APIErrorCode.INVALID_RESPONSE.value,
                    )
                # Explicitly type the list after check, elements are still Any
                checked_list_of_any_events: list[Any] = raw_data_any
                for event_loop_var_any in checked_list_of_any_events:
                    if not isinstance(event_loop_var_any, dict):
                        logger.warning(
                            f"[{self.exchange_name}] userEvents item not dict: "
                            f"{event_loop_var_any}, skipping."
                        )
                        continue
                    # Cast here for Pyright
                    event_item_dict = cast(dict[str, Any], event_loop_var_any)
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
                            _handle_order_wrapper = HyperliquidWsRawMessageHandler.handle_user_order_update_wrapper_payload
                            order_update_wrapper = _handle_order_wrapper(event_item_dict)
                            _handle_order_event = (
                                HyperliquidWsRawMessageHandler.handle_user_order_event_payload
                            )
                            validated_order_details = _handle_order_event(order_update_wrapper.data)
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
                        continue
                return  # All user events handled, exit

            elif channel == "allMids":
                if not isinstance(raw_data_any, dict):
                    logger.warning(
                        f"[{self.exchange_name}] 'allMids' channel data is not a dict or is None. "
                        f"Data: {raw_data_any!r}. Skipping."
                    )
                    raise APIError(
                        "allMids data not dict or is None",
                        code=APIErrorCode.INVALID_RESPONSE.value,
                    )

                raw_data_dict_all_mids = cast(dict[str, Any], raw_data_any)

                validated_all_mids = HyperliquidWsRawMessageHandler.handle_all_mids_payload(
                    raw_data_dict_all_mids
                )
                payload_for_handler = validated_all_mids.model_dump(mode="json")

            elif channel == "pong" or channel == "subscriptionResponse":
                logger.debug(f"[{self.exchange_name}] Control message on '{channel}': {message}")
                payload_for_handler = (
                    cast(dict[str, Any], raw_data_any) if isinstance(raw_data_any, dict) else {}
                )
            else:
                logger.debug(
                    f"[{self.exchange_name}] Unhandled channel '{channel}' by specific "
                    f"validation, passing raw data if dict. Msg: {message}"
                )
                payload_for_handler = (
                    cast(dict[str, Any], raw_data_any) if isinstance(raw_data_any, dict) else {}
                )

            # Final handler call for channels that set payload_for_handler and don't return early
            if payload_for_handler is not None:
                await app_handler(payload_for_handler, message)
            # If payload_for_handler is None here, it means a path was taken that didn't set it
            # and didn't explicitly return (e.g. trades/userEvents handle their own calls to
            # app_handler) or an empty list for trades was encountered and returned early.

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
        await super().connect_websocket()

    async def get_balances(self) -> dict[str, SpotBalance]:
        """Get account balances."""
        if not self._wallet_address:
            raise APIError(
                "Wallet address required for get_balances.",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )
        return await self.account_service.get_balances()

    async def get_positions(self, symbol: str | None = None) -> list[DerivativePosition]:
        """Get current positions."""
        if not self._wallet_address:
            raise APIError(
                "Wallet address required for get_positions.",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )
        return await self.account_service.get_positions(symbol=symbol)

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Get open orders. Delegates to HyperliquidTradingService."""
        return await self.trading_service.get_open_orders(symbol=symbol)

    async def get_ticker(self, symbol: str) -> Ticker | None:
        """Retrieves the latest ticker information for a specific symbol."""
        return await self.market_data_service.get_ticker(symbol)

    async def get_order_book(self, symbol: str, depth: int | None = None) -> OrderBook | None:
        """Retrieves the order book for a specific symbol."""
        return await self.market_data_service.get_order_book(symbol)

    async def get_recent_trades(self, symbol: str, limit: int | None = 50) -> list[Trade]:
        """Retrieves recent public trades for a specific symbol."""
        return await self.market_data_service.get_recent_trades(symbol)

    async def get_funding_rates(self, symbols: list[str] | None = None) -> list[FundingRate]:
        """Retrieves current funding rates for specified symbols, or all if None."""
        rates: list[FundingRate] = []
        symbols_to_fetch: list[str] = []

        if symbols:
            symbols_to_fetch = symbols
        else:
            # Fetch all known symbols if cache is populated, otherwise fetch meta first
            if not self._asset_to_index_cache:
                try:
                    # Call _get_asset_index with a placeholder to populate cache.
                    # This is a bit of a hack; ideally, there'd be a "fetch_all_symbols"
                    # or "get_market_metadata" method.
                    # We need to ensure _get_asset_index is called to populate the cache.
                    # This assumes _get_asset_index correctly populates self._asset_to_index_cache
                    # by fetching metaAndAssetCtxs if the cache is empty for ANY symbol.
                    # A more robust way would be to directly fetch and parse meta here.
                    # For simplicity in this step, we'll rely on the cache being populated
                    # by a call to _get_asset_index if it was previously called, or
                    # we try to populate it.
                    # This is a temporary measure; a dedicated method to get all symbols is better.
                    await self._get_asset_index("ETH")  # Example symbol to trigger fetch if needed
                except APIError as e:
                    logger.error(
                        f"[{self.exchange_name}] Could not pre-fetch symbols for "
                        f"get_funding_rates: {e}"
                    )
                    # Depending on strictness, could return [] or raise e
            symbols_to_fetch = list(self._asset_to_index_cache.keys())
            if not symbols_to_fetch:
                logger.warning(
                    f"[{self.exchange_name}] No symbols found to fetch all funding rates."
                )
                return []

        for symbol_item in symbols_to_fetch:
            try:
                rate = await self.market_data_service.get_funding_rate(symbol_item)
                if rate:
                    rates.append(rate)
            except APIError as e:
                logger.error(
                    f"[{self.exchange_name}] APIError fetching funding rate for {symbol_item}: {e}"
                )
            except Exception as e_unex:
                logger.error(
                    f"[{self.exchange_name}] Unexpected error fetching funding rate for "
                    f"{symbol_item}: {e_unex}",
                    exc_info=True,
                )
        return rates

    async def get_market_data(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 100,
    ) -> list[Candle]:
        """Retrieves historical kline/candlestick data for a symbol and timeframe.
        Hyperliquid's candle endpoint requires startTime and endTime.
        This method needs to calculate these based on limit and timeframe if not provided directly.
        The service method get_market_data(symbol, interval, start_time_ms, end_time_ms)
        For now, this delegation assumes the caller will provide appropriate start/end times or
        that the service method can derive them if only limit is given.
        This is a simplification for delegation; original complex logic for start/end time
        calculation from limit+timeframe should be in the service or this method before delegation.

        For this refactor, we assume the service method handles the start/end time logic if needed.
        We need to define how start_time_ms and end_time_ms are derived here.
        Hyperliquid's /info for candles requires start and end times.
        Let's make a placeholder for now, as the service expects start/end ms.
        This will require more logic to be equivalent to original.
        """
        interval_ms = timeframe_to_ms(timeframe)
        if interval_ms == 0:
            raise ValueError(f"Invalid or unsupported timeframe: {timeframe}")

        current_time_ms = int(time.time() * 1000)
        end_time_ms = current_time_ms
        start_time_ms = end_time_ms - (limit * interval_ms)

        return await self.market_data_service.get_market_data(
            symbol=symbol,
            interval=timeframe,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
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
        """Place an order. Delegates to HyperliquidTradingService."""
        effective_price: Decimal
        if price is not None:
            effective_price = price
        elif order_type == OrderType.LIMIT:
            raise ValueError("Price is required for LIMIT orders on Hyperliquid.")
        else:  # MARKET or other types that might not require price upfront from user
            # The HL service's place_order method expects price: Decimal.
            # For market orders, HL uses limitPx as a slippage protection.
            # We must provide a Decimal. It's best if the caller provides an aggressive limit.
            # Raising an error if not provided for MARKET to enforce clarity for now.
            raise ValueError(
                "A limit price (for slippage protection) must be provided for MARKET orders to Hyperliquid trading service."
            )

        return await self.trading_service.place_order(
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=effective_price,
            time_in_force=time_in_force,
            stop_price=stop_price,
            client_order_id=client_order_id,
            reduce_only=reduce_only,
            post_only=post_only,
        )

    async def cancel_order(
        self, order_id: str, symbol: str | None = None
    ) -> bool:  # HL requires symbol, signature changed for Liskov
        """Cancel an order. Delegates to HyperliquidTradingService."""
        if symbol is None:
            _msg = "Symbol is required to cancel an order on Hyperliquid."
            logger.error(f"[{self.exchange_name}] {_msg}")
            raise ValueError(_msg)
        try:
            hl_order_id = int(order_id)
            return await self.trading_service.cancel_order(symbol=symbol, order_id=hl_order_id)
        except ValueError as e_val:
            _msg = (
                f"Invalid order_id format for cancel_order: {order_id}. "
                f"Must be an integer for Hyperliquid."
            )
            logger.error(f"[{self.exchange_name}] {_msg}")
            raise ValueError(_msg) from e_val

    async def cancel_all_orders(self, symbol: str | None = None) -> list[CancelOrderResult]:
        """Cancel all open orders. Delegates to HyperliquidTradingService."""
        return await self.trading_service.cancel_all_orders(symbol=symbol)

    async def get_account_summary(self) -> MarginAccountSummary | None:
        """Fetches and combines account balance and positions for Hyperliquid."""
        logger.info(f"[{self.exchange_name}] Fetching account summary.")
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
        return await self.account_service.get_account_summary()

    async def get_order_status(
        self, order_id: str, symbol: str | None = None, client_order_id: str | None = None
    ) -> Order:
        """Get order status. Fetches order via trading service and raises if not found."""
        if not symbol:
            _msg = "Symbol is required for get_order_status on Hyperliquid."
            logger.error(f"[{self.exchange_name}] {_msg}")
            raise ValueError(_msg)

        if client_order_id and not order_id:
            logger.warning(
                f"[{self.exchange_name}] get_order_status called with client_order_id='{client_order_id}' "
                f"but no exchange order_id. Hyperliquid requires exchange order_id for specific fetch."
            )

        try:
            hl_order_id = int(order_id)
        except ValueError as e_val:
            _msg = (
                f"Invalid order_id format for get_order_status: {order_id}. "
                f"Must be an integer for Hyperliquid."
            )
            logger.error(f"[{self.exchange_name}] {_msg}")
            raise ValueError(_msg) from e_val

        # Call the trading service directly to get the order.
        # The trading_service.get_order method returns Order | None.
        order = await self.trading_service.get_order(symbol=symbol, order_id=hl_order_id)

        if order is None:
            raise APIError(
                message=f"Order {order_id} for symbol {symbol} not found on {self.exchange_name}.",
                code=APIErrorCode.ORDER_NOT_FOUND.value,
            )
        return order

    async def get_order(
        self, order_id: str, symbol: str | None = None, client_order_id: str | None = None
    ) -> Order | None:  # Matches ExchangeAPI
        """Get a specific order by ID. Delegates to HyperliquidTradingService's get_order method."""
        if not symbol:
            _msg = "Symbol is required for get_order on Hyperliquid."
            logger.error(f"[{self.exchange_name}] {_msg}")
            # Return None or raise ValueError based on strictness for ExchangeAPI compliance.
            # Raising ValueError as symbol is strictly needed for the service call.
            raise ValueError(_msg)

        if client_order_id and not order_id:
            logger.warning(
                f"[{self.exchange_name}] get_order called with client_order_id='{client_order_id}' "
                f"but no exchange order_id. Hyperliquid primarily fetches by exchange order_id."
            )
            return None  # Or raise APIError(INVALID_PARAMS) if client_order_id lookup isn't supported here.

        try:
            hl_order_id = int(order_id)
            # Call the trading service's get_order method.
            retrieved_order = await self.trading_service.get_order(
                symbol=symbol, order_id=hl_order_id
            )
            return retrieved_order
        except ValueError as e_val:
            _msg = (
                f"Invalid order_id format for get_order: {order_id}. "
                f"Must be an integer for Hyperliquid."
            )
            logger.error(f"[{self.exchange_name}] {_msg}")
            # Consistent with ExchangeAPI, if params are bad leading to no possible fetch,
            # return None or raise.
            # Raising ValueError as it's a precondition for HL.
            raise ValueError(_msg) from e_val
        except APIError as e:
            # Allow trading_service to raise APIError (e.g., network issues)
            # If order specifically not found by service, it would return None, handled above.
            logger.error(
                f"[{self.exchange_name}] APIError in get_order for {order_id} ({symbol}): {e}"
            )
            raise  # Re-raise other APIErrors

    async def get_order_history(
        self,
        symbol: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int | None = None,
        order_id: str | None = None,
        client_order_id: str | None = None,
        from_id: str | None = None,
    ) -> list[Order]:
        """Retrieves historical orders."""
        if not self._wallet_address:
            raise APIError(
                "Wallet address required for get_order_history.",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )
        if limit is not None:
            logger.warning(
                f"[{self.exchange_name}] 'limit' parameter for get_order_history is not "
                f"directly supported by Hyperliquid's order history mechanism. "
                f"It will be ignored. Use start_time and end_time for filtering."
            )
        if order_id or client_order_id or from_id:
            logger.warning(
                f"[{self.exchange_name}] Parameters 'order_id', 'client_order_id', 'from_id' "
                f"for get_order_history are not directly used by the "
                f"Hyperliquid service call which primarily relies on symbol, start_time, "
                f"and end_time. These will be ignored."
            )

        return await self.account_service.get_order_history(
            symbol=symbol, start_time=start_time, end_time=end_time
        )

    async def get_trade_history(
        self,
        symbol: str | None = None,
        limit: int | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        from_id: str | None = None,
        order_id: str | None = None,
    ) -> list[Trade]:
        """Retrieves historical trades (fills)."""
        if not self._wallet_address:
            raise APIError(
                "Wallet address required for get_trade_history.",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )
        if start_time or end_time or limit or from_id or order_id:
            logger.warning(
                f"[{self.exchange_name}] Parameters 'start_time', 'end_time', 'limit', "
                f"'from_id', 'order_id' for get_trade_history are not directly supported by "
                f"Hyperliquid's user fills mechanism in the same way as other exchanges. "
                f"Filtering is primarily by symbol. These parameters will be ignored by the "
                f"direct service call."
            )
        return await self.account_service.get_trade_history(symbol=symbol)

    async def get_historical_funding_rates(
        self,
        # Parameters would be defined here if HL supported this directly
        # symbol: str,
        # start_time: datetime | None = None,
        # end_time: datetime | None = None,
        # limit: int | None = None,
    ) -> list[FundingRate]:
        """(Placeholder) Request historical funding rates.
        Hyperliquid's API for historical funding rates is not directly exposed
        or typically queried in the same way as CEXs for bulk history.
        Individual funding payments might be part of user state or transaction history.
        """
        logger.warning(
            f"[{self.exchange_name}] get_historical_funding_rates placeholder - not implemented "
            f"as Hyperliquid does not provide a dedicated bulk historical funding rate endpoint."
        )
        return []

    async def transfer(
        self,
        asset: str,
        amount: Decimal,
        from_account_type: str,  # e.g., "spot", "margin" - less relevant for HL L1<->L2
        to_account_type: str,  # e.g., "spot", "margin"
        client_transfer_id: str | None = None,
    ) -> Transfer:  # cyberdelta.core.models.operations.Transfer
        """(Not Applicable) Initiates an asset transfer between accounts.
        Hyperliquid is a DEX; transfers are typically L1 wallet deposits/withdrawals,
        not internal account-to-account transfers like on a CEX.
        """
        logger.error(
            f"[{self.exchange_name}] The 'transfer' operation as defined for CEXs "
            f"(e.g., spot to margin) is not directly applicable to Hyperliquid (DEX). "
            f"L1 deposits/withdrawals are handled differently."
        )
        raise NotImplementedError(
            f"The 'transfer' operation is not applicable to {self.exchange_name}."
        )

    async def withdraw(
        self,
        asset: str,  # For HL, this is usually implied by the L1 token
        amount: Decimal,
        address: str,  # L1 destination address
        network: str | None = None,  # L1 network, e.g., "Arbitrum"
        tag: str | None = None,  # Destination tag/memo, if applicable
        client_withdrawal_id: str | None = None,
        two_factor_token: str | None = None,
        **kwargs: dict[str, Any],  # Changed from Any
    ) -> Withdrawal:  # cyberdelta.core.models.operations.Withdrawal
        """(Not Applicable) Initiates a withdrawal of assets from the exchange.
        Hyperliquid is a DEX; withdrawals are L1 transactions signed by the user's wallet,
        not initiated via an API call in this manner.
        """
        logger.error(
            f"[{self.exchange_name}] The 'withdraw' operation via API is not applicable "
            f"to Hyperliquid (DEX). L1 withdrawals are user-signed transactions."
        )
        raise NotImplementedError(
            f"The 'withdraw' operation is not applicable to {self.exchange_name}."
        )

    async def subscribe_to_order_book(self, symbol: str) -> None:
        """Prepare subscription to order book updates for a symbol.
        Actual subscription with a handler is done via self.subscribe().
        """
        # Hyperliquid topic format: "l2Book:SYMBOL"
        topic = f"l2Book:{symbol}"
        logger.debug(
            f"[{self.exchange_name}] Preparing subscription for order book (l2Book) topic: {topic}"
        )
        # Actual subscription is initiated by the caller using self.subscribe(topic, handler)

    async def subscribe_to_ticker(self, symbol: str) -> None:
        """Prepare subscription to ticker updates for a symbol.
        Hyperliquid does not have a direct per-symbol ticker stream like 'ticker.SYMBOL'.
        It uses 'allMids' for all symbols or relies on order book/trades for ticker-like data.
        This method will log a warning. Consider subscribing to 'allMids' or 'l2Book' instead.
        """
        # Hyperliquid uses "allMids" for a combined stream.
        # Individual ticker streams like "ticker:SYMBOL" are not standard for HL.
        logger.warning(
            f"[{self.exchange_name}] Hyperliquid does not have a direct 'ticker:{symbol}' stream. "
            f"Consider subscribing to 'allMids' for all mid prices, or 'l2Book:{symbol}' "
            f"and derive ticker data."
        )
        # No direct topic construction for a non-existent stream type.

    async def subscribe_to_trades(self, symbol: str) -> None:
        """Prepare subscription to public trade updates for a symbol.
        Actual subscription with a handler is done via self.subscribe().
        """
        # Hyperliquid topic format: "trades:SYMBOL"
        topic = f"trades:{symbol}"
        logger.debug(
            f"[{self.exchange_name}] Preparing subscription for public trades topic: {topic}"
        )
        # Actual subscription is initiated by the caller using self.subscribe(topic, handler)

    async def subscribe_to_account_updates(self) -> None:
        """Prepare subscription to private account updates (fills, orders, positions).
        Actual subscription with a handler is done via self.subscribe().
        Hyperliquid uses a single 'userEvents' stream for this.
        """
        # Hyperliquid topic format for all user data: "userEvents"
        # This requires wallet_address to be known by _construct_subscription_payload
        topic = "userEvents"
        logger.debug(
            f"[{self.exchange_name}] Preparing subscription for user account updates "
            f"(userEvents) topic: {topic}"
        )
        # Actual subscription is initiated by the caller using self.subscribe(topic, handler)

    def _update_rate_limit_from_headers(
        self, headers: Mapping[str, str], method: str, path: str
    ) -> None:
        """
        Update rate limit information based on response headers.
        Hyperliquid does not typically provide rate limit info in standard headers.
        This is a placeholder implementation.
        """
        logger.debug(
            f"[{self.exchange_name}] _update_rate_limit_from_headers called "
            f"(no-op for Hyperliquid). Headers: {headers}, Method: {method}, Path: {path}"
        )
        pass

    async def subscribe(self, topic: str, handler: MessageHandler) -> None:
        """Register a handler for a WebSocket topic and send subscription via WebSocketManager."""
        logger.info(
            f"[{self.exchange_name}] Subscribing to topic: {topic}. Delegating to base ExchangeAPI."
        )
        await super().subscribe(topic, handler)

    async def _on_ws_connected(self) -> None:
        """Callback for when WebSocket connects, typically to resubscribe to topics."""
        logger.info(
            f"[{self.exchange_name}] WebSocket connected. "
            f"Triggering resubscription via base ExchangeAPI."
        )
        await super()._on_ws_connected()

    async def _resubscribe(self) -> None:
        """Resubscribe to all registered topics upon WebSocket (re)connection."""
        logger.info(
            f"[{self.exchange_name}] Resubscribing to topics. Delegating to base ExchangeAPI."
        )
        await super()._resubscribe()

    async def get_all_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Fetch all open orders, optionally filtering by symbol."""
        return await self.get_open_orders(symbol=symbol)
