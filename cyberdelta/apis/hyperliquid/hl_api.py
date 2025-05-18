from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping
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

# Attempting to fix the import path for RateLimiterService
from cyberdelta.apis.connectivity.rate_limiter_service import RateLimiterService
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
)
from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import (
    HyperliquidRawHistoricalOrder,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawMetaAndAssetCtxsResponse,
)

# Import HyperliquidRawOrder from the correct module
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOrder,
)

# Import HyperliquidRawL2Book
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import HyperliquidRawClearinghouseState

# ADDED IMPORT FOR ACCOUNT SERVICE
from cyberdelta.apis.hyperliquid.services.hl_account_service import HyperliquidAccountService
from cyberdelta.apis.hyperliquid.services.hl_market_data_service import HyperliquidMarketDataService

# IMPORT TRADING SERVICE
from cyberdelta.apis.hyperliquid.services.hl_trading_service import HyperliquidTradingService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode

# Core Domain Models
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
    OrderStatus,
    OrderType,
    TimeInForce,
)
from cyberdelta.core.models.market import Candle, OrderBook
from cyberdelta.core.models.market.order import (
    Order,
)
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
        request_data: dict[str, Any] | None
        if isinstance(data, dict) or data is None:
            request_data = data
        else:  # DEFENSIVE CHECK: Handles cases where caller violates type hint for 'data'. Mypy=[unreachable]
            logger.warning(
                f"[{self.exchange_name}] _market_data_requester_adapter received non-dict data: "
                f"{type(data)}. Passing as None."
            )
            request_data = None # Explicitly set to None if unexpected type received

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
            exchange_http_client_requester=self._request,
            info_http_client_requester=self._info_request_wrapper,
            request_builder=self._hl_request_builder,
            response_handler=self._hl_response_handler,
            authenticator=self._hl_authenticator,
            rate_limiter_service=self._rate_limiter_service,
            exchange_name=self.exchange_name,
            wallet_address=self._wallet_address,
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

        if channel in ["pong", "subscriptionResponse"]:
            logger.debug(f"[{self.exchange_name}] Control message on '{channel}': {message}")
            return

        topic_key_for_handler = channel
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
            coin_for_topic_str: str | None = None
            if isinstance(message.get("coin"), str):
                coin_for_topic_str = message.get("coin")
            elif isinstance(raw_data, list) and raw_data:
                first_trade_item_any: Any = raw_data[0]
                if isinstance(first_trade_item_any, dict):
                    first_trade_item_dict = cast(dict[str, Any], first_trade_item_any)
                    coin_from_item_any: Any = first_trade_item_dict.get("coin")
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

        if raw_data is None:
            logger.warning(f"[{self.exchange_name}] WS '{channel}' has no data. Msg: {message}")
            return

        try:
            payload_for_handler: dict[str, Any] | None = None

            if channel == "l2Book":
                if not isinstance(raw_data, dict):
                    raise APIError(
                        f"l2Book data not dict: {type(raw_data)}",
                        code=APIErrorCode.INVALID_RESPONSE.value,
                    )
                validated_model = HyperliquidWsRawMessageHandler.handle_l2book_payload(
                    cast(dict[str, Any], raw_data)
                )
                payload_for_handler = validated_model.model_dump(mode="json")
                await app_handler(payload_for_handler, message)

            elif channel == "trades":
                if not isinstance(raw_data, list):
                    actual_type_name = type(raw_data).__name__
                    raise APIError(
                        f"Trades data not list: {actual_type_name}",
                        code=APIErrorCode.INVALID_RESPONSE.value,
                    )

                typed_trades_input_list: list[dict[str, Any]] = []
                for item_loop_var in raw_data:
                    item_from_any_list = item_loop_var
                    if not isinstance(item_from_any_list, dict):
                        logger.warning(
                            f"[{self.exchange_name}] Trades list item not dict: "
                            f"{item_from_any_list}. Msg: {message}. Skipping item."
                        )
                        continue
                    item_dict = cast(dict[str, Any], item_from_any_list)
                    typed_trades_input_list.append(item_dict)

                if not typed_trades_input_list and raw_data:
                    logger.warning(
                        f"[{self.exchange_name}] All items in trades list were invalid. "
                        f"Original raw_data: {raw_data}"
                    )
                    return

                if typed_trades_input_list:
                    validated_trade_models = (
                        HyperliquidWsRawMessageHandler.handle_public_trades_payload(
                            typed_trades_input_list
                        )
                    )
                    for trade_model in validated_trade_models:
                        payload_for_handler = trade_model.model_dump(mode="json")
                        await app_handler(payload_for_handler, message)

            elif channel == "userEvents":
                if not isinstance(raw_data, list):
                    actual_type_name = type(raw_data).__name__
                    raise APIError(
                        f"userEvents data not list: {actual_type_name}",
                        code=APIErrorCode.INVALID_RESPONSE.value,
                    )

                for event_loop_var in raw_data:
                    event_item_from_any_list = event_loop_var
                    if not isinstance(event_item_from_any_list, dict):
                        logger.warning(
                            f"[{self.exchange_name}] userEvents item not dict: "
                            f"{event_item_from_any_list}, skipping."
                        )
                        continue

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
                            _handle_order_wrapper = (
                                HyperliquidWsRawMessageHandler
                                .handle_user_order_update_wrapper_payload
                            )
                            order_update_wrapper = _handle_order_wrapper(event_item_dict)
                            _handle_order_event = (
                                HyperliquidWsRawMessageHandler.handle_user_order_event_payload
                            )
                            validated_order_details = _handle_order_event(order_update_wrapper.data)
                            current_event_payload_for_handler = validated_order_details.model_dump(
                                mode="json"
                            )

                        elif event_type_str == "positionUpdate":
                            _handle_pos_update = (
                                HyperliquidWsRawMessageHandler
                                .handle_user_position_update_event_payload
                            )
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

            elif channel == "allMids":
                if not isinstance(raw_data, dict):
                    logger.warning(
                        f"[{self.exchange_name}] 'allMids' channel data is not a dict or is None. "
                        f"Type: {type(raw_data)}. Data: {raw_data!r}. Skipping."
                    )
                    raise APIError(
                        f"allMids data not dict or is None: {type(raw_data)}",
                        code=APIErrorCode.INVALID_RESPONSE.value,
                    )

                raw_data_dict = cast(dict[str, Any], raw_data)

                validated_all_mids = HyperliquidWsRawMessageHandler.handle_all_mids_payload(
                    raw_data_dict
                )
                payload_for_handler = cast(
                    dict[str, Any], validated_all_mids.model_dump(mode="json")
                )
                await app_handler(payload_for_handler, message)

            elif channel == "pong" or channel == "subscriptionResponse":
                logger.debug(f"[{self.exchange_name}] Control message on '{channel}': {message}")
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
        try:
            raw_clearinghouse_state: HyperliquidRawClearinghouseState = (
                await self.account_service.get_balances_raw()
            )
            return self._hl_mapper.map_raw_clearinghouse_state_to_spot_balances(
                raw_clearinghouse_state
            )
        except ValidationError as e:
            logger.error(
                f"[{self.exchange_name}] Error validating/mapping clearinghouseState "
                f"for balances: {e}"
            )
            raise APIError(
                f"Failed to validate/map balance data structure: {e}",
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
            raw_clearinghouse_state: HyperliquidRawClearinghouseState = (
                await self.account_service.get_positions_raw()
            )
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

        raw_orders_from_service: list[HyperliquidRawOrder]
        try:
            raw_orders_from_service = await self.trading_service.get_open_orders_raw()
        except APIError as e_service:
            logger.error(
                f"[{self.exchange_name}] APIError from trading_service.get_open_orders_raw: "
                f"{e_service.message}"
            )
            raise
        except Exception as e_unhandled_service_call:
            logger.error(
                f"[{self.exchange_name}] Unexpected error calling "
                f"trading_service.get_open_orders_raw: {e_unhandled_service_call}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error during service call for get_open_orders: "
                f"{e_unhandled_service_call}",
                APIErrorCode.UNKNOWN.value,
            ) from e_unhandled_service_call

        open_orders: list[Order] = []
        for raw_order_item in raw_orders_from_service:
            try:
                mapped_order = self._hl_order_mapper.transform_raw_order_to_internal(
                    raw=raw_order_item,
                    trigger=None,
                )
                if (
                    mapped_order
                    and mapped_order.status
                    in [
                        OrderStatus.OPEN,
                        OrderStatus.PARTIALLY_FILLED,
                        OrderStatus.NEW,
                    ]
                ):
                    if symbol is None or mapped_order.symbol == symbol:
                        open_orders.append(mapped_order)
            except (ValidationError, ValueError) as e_map:
                logger.warning(
                    f"[{self.exchange_name}] Error mapping raw open order: {e_map}. "
                    f"Raw: {raw_order_item.model_dump_json(exclude_none=True) if raw_order_item else 'None'}. "
                    f"Skipping."
                )
            except Exception as e_unexp_map:
                logger.error(
                    f"[{self.exchange_name}] Unexpected error mapping raw open order: {e_unexp_map}. "
                    f"Raw: {raw_order_item.model_dump_json(exclude_none=True) if raw_order_item else 'None'}. "
                    f"Skipping.",
                    exc_info=True,
                )
        return open_orders

    async def get_ticker(self, symbol: str) -> Ticker | None:
        """Retrieves the latest ticker information for a specific symbol."""
        return await self.market_data_service.get_ticker(symbol)

    async def get_order_book(self, symbol: str, depth: int | None = None) -> OrderBook | None:
        """Retrieves the order book for a specific symbol."""
        return await self.market_data_service.get_order_book(symbol)

    async def get_recent_trades(self, symbol: str, limit: int | None = 50) -> list[Trade]:
        """Retrieves recent public trades for a specific symbol."""
        return await self.market_data_service.get_recent_trades(symbol)

    async def get_funding_rate(self, symbol: str) -> FundingRate | None:
        """Retrieves the current funding rate for a specific symbol."""
        return await self.market_data_service.get_funding_rate(symbol)

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
        This is a simplification for delegation; original complex logic for start/end time calculation
        from limit+timeframe should be in the service or this method before delegation.

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
        """Place an order on Hyperliquid."""
        hl_time_in_force_options: dict[str, Any] = {"type": time_in_force.value}
        if stop_price is not None:
            hl_time_in_force_options["triggerPx"] = str(stop_price)
            hl_time_in_force_options["isMarket"] = True
            if order_type in [OrderType.STOP_LIMIT, OrderType.TAKE_PROFIT_LIMIT]:
                hl_time_in_force_options["isMarket"] = False

            if order_type in [OrderType.STOP_MARKET, OrderType.STOP_LIMIT]:
                hl_time_in_force_options["tpsl"] = "Sl"
            elif order_type in [OrderType.TAKE_PROFIT_MARKET, OrderType.TAKE_PROFIT_LIMIT]:
                hl_time_in_force_options["tpsl"] = "Tp"

            hl_time_in_force_options["orderTif"] = time_in_force.value

        if (
            order_type in [OrderType.LIMIT, OrderType.STOP_LIMIT, OrderType.TAKE_PROFIT_LIMIT]
            and price is None
        ):
            raise ValueError(f"Price is required for {order_type.value} orders.")

        if quantity <= Decimal("0"):
            raise ValueError("Order quantity must be positive.")

        limit_price_for_service = price
        if order_type == OrderType.MARKET and price is not None:
            logger.warning(
                f"[{self.exchange_name}] Price provided for MARKET order, it will be ignored by "
                f"Hyperliquid."
            )

        validated_response: HyperliquidRawExchangeResponse
        try:
            validated_response = await self.trading_service.place_order_raw(
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity,
                price=limit_price_for_service
                if limit_price_for_service is not None
                else Decimal("0"),
                reduce_only=reduce_only,
                time_in_force_options=hl_time_in_force_options,
                client_order_id=client_order_id,
            )
        except APIError as e_service:
            logger.error(
                f"[{self.exchange_name}] APIError from trading_service.place_order_raw: "
                f"{e_service.message}"
            )
            raise
        except Exception as e_unhandled_service_call:
            logger.error(
                f"[{self.exchange_name}] Unexpected error calling "
                f"trading_service.place_order_raw: {e_unhandled_service_call}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error during service call for order placement: "
                f"{e_unhandled_service_call}",
                APIErrorCode.UNKNOWN.value,
            ) from e_unhandled_service_call

        if (
            validated_response.status != "ok"
            or not validated_response.data
            or not validated_response.data.statuses
        ):
            mapped_error = self.error_mapper.map_exchange_error(
                status_code=200,
                error_body=str(validated_response.model_dump_json()),
                error_data=validated_response.model_dump(),
                request_path="/exchange",
            )
            raise mapped_error

        first_status_obj_raw = validated_response.data.statuses[0]
        error_to_raise_from_status: APIError | None = None

        arg_for_handler: dict[str, Any] | str
        if isinstance(first_status_obj_raw, str):
            arg_for_handler = first_status_obj_raw
        else:
            arg_for_handler = first_status_obj_raw.model_dump(by_alias=True, exclude_none=True)

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
                elif processed_status.status_type == "canceled":
                    order_id_to_fetch = processed_status.oid
                    log_message_prefix = (
                        f"Order OID:{processed_status.oid} canceled (via object status)"
                    )
                    logger.info(f"[{self.exchange_name}] {log_message_prefix}")
            elif processed_status.status_type == "canceled_str":
                logger.info(
                    f"[{self.exchange_name}] Order placement returned 'canceled' string status."
                )
                raise APIError(
                    "Order placement resulted in immediate 'canceled' status (string). "
                    "Cannot return Order object.",
                    code=APIErrorCode.ORDER_REJECTED.value,
                    exchange_message="canceled_str",
                )

        else:
            logger.warning(
                f"[{self.exchange_name}] Order placement failed with error: "
                f"{processed_status.message}"
            )
            error_to_raise_from_status = self.error_mapper.map_string_error(
                processed_status.message,
                http_status=200,
            )

        if error_to_raise_from_status:
            raise error_to_raise_from_status

        if order_id_to_fetch is not None:
            logger.info(f"[{self.exchange_name}] {log_message_prefix}. Fetching canonical status.")
            await asyncio.sleep(self._config.get("post_order_status_fetch_delay_seconds", 0.2))
            try:
                final_order_status = await self.get_order_status(
                    order_id=str(order_id_to_fetch), symbol=symbol
                )
                return final_order_status

            except APIError as e_fetch:
                logger.error(
                    f"[{self.exchange_name}] {log_message_prefix}, "
                    f"but failed to fetch canonical status: {e_fetch.message}"
                )
                raise APIError(
                    f"{log_message_prefix}, but failed to retrieve final status: {e_fetch.message}",
                    code=e_fetch.code,
                    original_exception=e_fetch,
                    exchange_message=e_fetch.exchange_message,
                ) from e_fetch
        else:
            logger.warning(
                f"[{self.exchange_name}] Order placement status unclear. Processed status: "
                f"{processed_status!r}. No OID found to fetch canonical status."
            )
            raise APIError(
                f"Order placement status unclear, no OID to confirm: "
                f"{str(processed_status)[:100]}...",
                code=APIErrorCode.UNKNOWN.value,
            )

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> bool:
        """Cancel an existing order."""
        if not symbol:
            raise ValueError("Symbol is required to cancel Hyperliquid orders")

        validated_response: HyperliquidRawExchangeResponse
        try:
            order_id_int: int
            try:
                order_id_int = int(order_id)
            except ValueError as e_val_int:
                logger.error(
                    f"[{self.exchange_name}] Invalid order_id format for cancel: '{order_id}'. "
                    f"Must be integer."
                )
                raise APIError(
                    f"Invalid order_id format for cancel: '{order_id}'. Must be integer.",
                    APIErrorCode.INVALID_PARAMS.value,
                ) from e_val_int

            validated_response = await self.trading_service.cancel_order_raw(
                symbol=symbol, order_id=order_id_int
            )
        except APIError as e_service:
            logger.error(
                f"[{self.exchange_name}] APIError from trading_service.cancel_order_raw "
                f"for OID {order_id}: {e_service.message}"
            )
            raise
        except Exception as e_unhandled_service_call:
            logger.error(
                f"[{self.exchange_name}] Unexpected error calling "
                f"trading_service.cancel_order_raw for OID {order_id}: {e_unhandled_service_call}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error during service call for cancel order {order_id}: "
                f"{e_unhandled_service_call}",
                APIErrorCode.UNKNOWN.value,
            ) from e_unhandled_service_call

        if (
            validated_response.status != "ok"
            or not validated_response.data
            or not validated_response.data.statuses
        ):
            mapped_error = self.error_mapper.map_exchange_error(
                status_code=200,
                error_body=str(validated_response.model_dump_json()),
                error_data=validated_response.model_dump(),
                request_path="/exchange (cancel_order)",
            )
            raise mapped_error

        first_status_obj_raw = validated_response.data.statuses[0]
        error_to_raise_from_status: APIError | None = None

        arg_for_handler: dict[str, Any] | str
        if isinstance(first_status_obj_raw, str):
            arg_for_handler = first_status_obj_raw
        else:
            arg_for_handler = first_status_obj_raw.model_dump(by_alias=True, exclude_none=True)

        processed_status = HyperliquidResponseHandler.process_first_exchange_status(
            arg_for_handler, f"Cancel Order OID:{order_id}"
        )

        if isinstance(processed_status, HyperliquidSuccessfulOrderStatus):
            try:
                order_id_as_int_for_check = int(order_id)
            except ValueError:
                order_id_as_int_for_check = -1

            if (
                processed_status.status_type == "canceled"
                and processed_status.oid == order_id_as_int_for_check
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
                logger.warning(
                    f"[{self.exchange_name}] Cancel order {order_id} returned unexpected "
                    f"successful status: "
                    f"{processed_status.model_dump_json(exclude_none=True)!r}. "
                    f"Assuming success."
                )
                return True
        else:
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
                    http_status=200,
                )

        if error_to_raise_from_status:
            raise error_to_raise_from_status

        logger.warning(
            f"[{self.exchange_name}] Cancel order {order_id} status unclear after processing. "
            f"Processed: {processed_status!r}. Raw arg to handler: {arg_for_handler!r}"
        )
        return False

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
        try:
            raw_user_state: HyperliquidRawClearinghouseState = (
                await self.account_service.get_account_summary_raw()
            )

            internal_summary = self._hl_mapper.map_raw_clearinghouse_state_to_margin_summary(
                raw_state=raw_user_state
            )
            return internal_summary

        except APIError as e_api:
            logger.error(
                f"[{self.exchange_name}] API Error getting account summary: {e_api}",
                exc_info=True,
            )
            raise
        except (
            ValidationError,
            ValueError,
        ) as e_map_val:
            logger.error(
                f"[{self.exchange_name}] Pydantic ValidationError or ValueError "
                f"mapping account summary: {e_map_val}",
                exc_info=True,
            )
            raise APIError(
                message=f"Failed to validate or map account summary response: {e_map_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_map_val,
            ) from e_map_val
        except Exception as e_unhandled:
            logger.error(
                f"[{self.exchange_name}] Unexpected error getting account summary: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error getting account summary: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

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

        await self._route_ws_message(message)

    async def get_order_status(
        self, order_id: str, symbol: str | None = None, client_order_id: str | None = None
    ) -> Order:
        """Fetches the status of a specific order by its orderId."""
        if not self._wallet_address:
            raise APIError(
                "Wallet address required for fetching order status",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )
        if not order_id:
            raise ValueError("order_id is required for get_order_status on Hyperliquid.")
        if not symbol:
            raise ValueError("symbol is required for get_order_status on Hyperliquid.")

        order_id_int: int
        try:
            order_id_int = int(order_id)
        except ValueError as e_val_int:
            logger.error(
                f"[{self.exchange_name}] Invalid order_id format for get_order_status: "
                f"'{order_id}'. Error: {e_val_int}"
            )
            raise APIError(
                f"Invalid order_id format: '{order_id}'",
                code=APIErrorCode.INVALID_PARAMS.value,
                original_exception=e_val_int,
            ) from e_val_int

        raw_historical_order: HyperliquidRawHistoricalOrder | None
        try:
            raw_historical_order = await self.trading_service.get_order_status_raw(
                symbol=symbol, order_id=order_id_int
            )
        except APIError as e_service:
            logger.error(
                f"[{self.exchange_name}] APIError from trading_service.get_order_status_raw "
                f"for OID {order_id}: {e_service.message}"
            )
            raise
        except Exception as e_unhandled_service_call:
            logger.error(
                f"[{self.exchange_name}] Unexpected error calling "
                f"trading_service.get_order_status_raw for OID {order_id}: {e_unhandled_service_call}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error during service call for get_order_status OID {order_id}: "
                f"{e_unhandled_service_call}",
                APIErrorCode.UNKNOWN.value,
            ) from e_unhandled_service_call

        if raw_historical_order is None:
            logger.info(
                f"[{self.exchange_name}] Order {order_id} for symbol {symbol} not found by service."
            )
            raise APIError(
                f"Order {order_id} not found for symbol {symbol}.",
                code=APIErrorCode.ORDER_NOT_FOUND.value,
            )

        try:
            internal_order = self._hl_order_mapper.transform_raw_historical_order_to_internal(
                raw_historical_order=raw_historical_order,
                trigger=None,
            )
            return internal_order
        except (ValidationError, ValueError) as e_map:
            logger.error(
                f"[{self.exchange_name}] Error mapping raw historical order for OID {order_id}: {e_map}. "
                f"Raw: {raw_historical_order.model_dump_json(exclude_none=True) if raw_historical_order else 'None'}"
            )
            raise APIError(
                f"Failed to map order status response for OID {order_id}: {e_map}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_map,
            ) from e_map
        except Exception as e_unexp_map:
            logger.error(
                f"[{self.exchange_name}] Unexpected error mapping order status for OID {order_id}: "
                f"{e_unexp_map}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error mapping order status for OID {order_id}: {e_unexp_map}",
                APIErrorCode.UNKNOWN.value,
                original_exception=e_unexp_map,
            ) from e_unexp_map

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
