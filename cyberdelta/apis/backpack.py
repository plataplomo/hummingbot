"""
CyberDeltaEngine: Backpack Exchange Integration
----------------------------------------------

This module implements the Backpack exchange adapter for CyberDeltaEngine, including:
- REST and WebSocket API client (`BackpackAPI`)
- Centralized error mapping and normalization (`BackpackErrorMapper`)
- Order and event transformation utilities (`BackpackOrderMapper`)

**Key architectural patterns:**
- All external (exchange) errors are mapped to canonical APIErrorCode values, validated and normalized via APIErrorResponse, and propagated as APIError exceptions.
- All API methods are type-safe, defensive, and log/handle edge cases robustly.
- All transformation logic is modular and testable.

**Onboarding Note:**
- When extending this module for new endpoints or error types, always use strict Pydantic validation, map all error codes, and document any non-obvious logic or edge cases.
"""

import asyncio
import hashlib
import hmac
import logging
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

from pydantic import ValidationError

from cyberdelta.apis.base import (  # Use absolute import
    APIError,
    ExchangeAPI,
    MessageHandler,
)
from cyberdelta.apis.models.api import APIErrorResponse
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.bp_api_models import BackpackRawApiError, BackpackRawOrder
from cyberdelta.core.models import (  # Use absolute import
    Balance,
    FundingRate,
    Order,
    OrderBook,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
    Ticker,
    TimeInForce,
    Trade,
)
from cyberdelta.core.models.enums import (
    OrderExpiryReason,
    OrderUpdateOrigin,
    SelfTradePrevention,
    TriggerType,
)
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value

logger = logging.getLogger(__name__)


class BackpackErrorMapper:
    """
    Maps and normalizes Backpack API errors to CyberDeltaEngine's canonical error model.

    Responsibilities:
    - Map Backpack error codes (from BackpackRawApiError) to APIErrorCode.
    - Validate and normalize all error data using APIErrorResponse.
    - Raise APIError with all validated fields for unified error propagation.
    - Log ambiguous/unmapped codes for diagnostics and future mapping improvements.

    Usage:
        raise BackpackErrorMapper.map_error_response(...)
    """

    @staticmethod
    def map_error_code(
        error_body: str, error_data: dict[str, Any] | None = None, status_code: int | None = None
    ) -> APIErrorCode:
        """
        Map Backpack error responses (body, data, status) to standardized APIErrorCode.

        - Uses BackpackRawApiError for strict code extraction if possible.
        - Falls back to heuristics if parsing fails.
        - Logs unmapped/ambiguous codes for diagnostics.

        Args:
            error_body: Raw error body as string (for fallback/logging).
            error_data: Parsed error data as dict (if available).
            status_code: HTTP status code (if available).

        Returns:
            APIErrorCode: Canonical error code for internal handling.
        """
        mapped_code = APIErrorCode.EXCHANGE_SPECIFIC
        if error_data:
            try:
                raw_api_error = BackpackRawApiError.model_validate(error_data)
                code = raw_api_error.code.upper()
                code_map = {
                    "INVALID_SIGNATURE": APIErrorCode.AUTHENTICATION_FAILED,
                    "UNAUTHORIZED": APIErrorCode.AUTHENTICATION_FAILED,
                    "FORBIDDEN": APIErrorCode.AUTHENTICATION_FAILED,
                    "TOO_MANY_REQUESTS": APIErrorCode.RATE_LIMITED,
                    "RATE_LIMIT_EXCEEDED": APIErrorCode.RATE_LIMITED,
                    "INVALID_SYMBOL": APIErrorCode.INVALID_SYMBOL,
                    "INVALID_QUANTITY": APIErrorCode.INVALID_ORDER_SIZE,
                    "INVALID_ORDER": APIErrorCode.INVALID_REQUEST,
                    "INVALID_PRICE": APIErrorCode.INVALID_REQUEST,
                    "INVALID_CLIENT_REQUEST": APIErrorCode.INVALID_REQUEST,
                    "INSUFFICIENT_FUNDS": APIErrorCode.INSUFFICIENT_FUNDS,
                    "INSUFFICIENT_MARGIN": APIErrorCode.INSUFFICIENT_FUNDS,
                    "ORDER_LIMIT": APIErrorCode.ORDER_REJECTED,
                    "POSITION_LIMIT": APIErrorCode.MAX_POSITION_EXCEEDED,
                    "SERVER_ERROR": APIErrorCode.SERVER_ERROR,
                    "MAINTENANCE": APIErrorCode.MAINTENANCE,
                    "RESOURCE_NOT_FOUND": APIErrorCode.ORDER_NOT_FOUND,
                    "INVALID_MARKET": APIErrorCode.INVALID_SYMBOL,
                    "INVALID_SOURCE": APIErrorCode.EXCHANGE_SPECIFIC,
                    "ACCOUNT_LIQUIDATING": APIErrorCode.EXCHANGE_SPECIFIC,
                    "TRADING_PAUSED": APIErrorCode.MARKET_CLOSED,
                    "INVALID_ASSET": APIErrorCode.INVALID_SYMBOL,
                    "INVALID_POSITION_ID": APIErrorCode.ORDER_NOT_FOUND,
                    "BORROW_REQUIRES_LEND_REDEEM": APIErrorCode.EXCHANGE_SPECIFIC,
                    "LEND_REQUIRES_BORROW_REPAY": APIErrorCode.EXCHANGE_SPECIFIC,
                    "INSUFFICIENT_SUPPLY": APIErrorCode.INSUFFICIENT_FUNDS,
                    "BORROW_LIMIT": APIErrorCode.MAX_POSITION_EXCEEDED,
                    "LEND_LIMIT": APIErrorCode.MAX_POSITION_EXCEEDED,
                    "MAX_LEVERAGE_REACHED": APIErrorCode.MAX_POSITION_EXCEEDED,
                    "PRECONDITION_FAILED": APIErrorCode.INVALID_REQUEST,
                    "NOT_IMPLEMENTED": APIErrorCode.EXCHANGE_SPECIFIC,
                }
                mapped_code = code_map.get(code, APIErrorCode.EXCHANGE_SPECIFIC)
                if mapped_code == APIErrorCode.EXCHANGE_SPECIFIC:
                    logger.warning(
                        f"[BackpackErrorMapper] Unmapped or ambiguous Backpack error code: {code}"
                    )
                return mapped_code
            except Exception as e:
                logger.warning(
                    f"[BackpackErrorMapper] Failed to parse error_data as "
                    f"BackpackRawApiError: {e}. Falling back to heuristics."
                )
        return mapped_code

    @staticmethod
    def map_error_response(
        status_code: int | None,
        error_body: str,
        error_data: dict[str, Any] | None = None,
        request_path: str | None = None,
        exchange_message: str | None = None,
        original_exception: Exception | None = None,
    ) -> APIError:
        """
        Map Backpack error responses to a standardized APIError, including error code
        mapping and full validation.

        - Always validates and normalizes error data using APIErrorResponse.
        - Ensures all error propagation is type-safe and consistent.
        - Handles edge cases: unmapped codes, malformed payloads, chained exceptions.

        Args:
            status_code: HTTP status code (if available).
            error_body: Raw error body as string.
            error_data: Parsed error data as dict (if available).
            request_path: API endpoint path (for diagnostics).
            exchange_message: Exchange-provided error message (if available).
            original_exception: Chained exception (if any).

        Returns:
            APIError: Exception ready to be raised or propagated.
        """
        mapped_code = BackpackErrorMapper.map_error_code(
            error_body=error_body, error_data=error_data, status_code=status_code
        )
        # Guarantee message is always a str
        msg: str = error_body
        if exchange_message is not None:
            msg = exchange_message
        elif error_data and isinstance(error_data.get("msg"), str):
            msg = error_data["msg"]
        api_error_response = APIErrorResponse.from_exchange_error(
            message=msg,
            code=mapped_code.value,
            http_status=status_code,
            exchange_code=(
                str(error_data.get("code")) if error_data and "code" in error_data else None
            ),
            exchange_message=(error_data.get("msg") if error_data else None),
            metadata={"request_path": request_path} if request_path else None,
        )
        # Convert APIErrorResponse to APIErrorModel-compatible fields
        # APIErrorModel expects code: APIErrorCode, exchange_code: str|None
        # Defensive: ensure code is valid APIErrorCode, else EXCHANGE_SPECIFIC
        try:
            code_enum = (
                APIErrorCode(api_error_response.code)
                if isinstance(api_error_response.code, int)
                else APIErrorCode.EXCHANGE_SPECIFIC
            )
        except Exception:
            code_enum = APIErrorCode.EXCHANGE_SPECIFIC
        exchange_code_str = (
            str(api_error_response.exchange_code)
            if api_error_response.exchange_code is not None
            else None
        )
        return APIError(
            message=api_error_response.message,
            code=code_enum,
            http_status=api_error_response.http_status,
            exchange_code=exchange_code_str,
            exchange_message=api_error_response.exchange_message,
            retry_after=api_error_response.retry_after,
            original_exception=original_exception,
        )


class BackpackAPI(ExchangeAPI):
    """
    Asynchronous API client for Backpack Exchange (REST + WebSocket).

    - Implements all required ExchangeAPI methods for order, market data, and account management.
    - Uses strict Pydantic validation for all responses and error payloads.
    - All error handling is routed through BackpackErrorMapper for normalization and propagation.
    - Designed for extensibility and robust, production-grade operation.

    Usage:
        api = BackpackAPI(api_config, secrets)
        await api.get_ticker("BTC_USDC")
    """

    def __init__(self, api_config: dict[str, Any], secrets: dict[str, str | None]) -> None:
        """
        Initialize the BackpackAPI client with configuration and secrets.

        Args:
            api_config: Dictionary of API configuration parameters.
            secrets: Dictionary of secret values (API key/secret).
        """
        super().__init__("backpack", api_config, secrets)
        self._api_key = secrets.get("BACKPACK_API_KEY")
        self._api_secret = secrets.get("BACKPACK_API_SECRET")
        if not self._api_key or not self._api_secret:
            logger.warning("Backpack API key/secret not provided. Signed operations will fail.")

    # --- WebSocket Implementation --- #

    async def _route_ws_message(self, message: dict[str, Any]) -> None:
        """
        Route incoming WebSocket messages to the appropriate handler.

        Args:
            message: Parsed WebSocket message as dict.
        """
        # Backpack messages typically have a 'topic' and 'data' field
        topic = message.get("topic")
        data = message.get("data")
        if not topic or not data:
            logger.debug(f"[{self.exchange_name}] Received unroutable message: {message}")
            return

        handler = self._ws_handlers.get(topic)
        if handler:
            try:
                await handler(data)
            except Exception as e:
                logger.error(
                    f"[{self.exchange_name}] Error in handler for topic {topic}: {e}",
                    exc_info=True,
                )
        else:
            logger.debug(f"[{self.exchange_name}] No handler registered for topic: {topic}")

    async def subscribe(self, topic: str, handler: MessageHandler) -> None:
        """
        Subscribe to a Backpack WebSocket topic and register a handler.

        Args:
            topic: WebSocket topic/channel name.
            handler: Async callback to handle messages for this topic.
        """
        if not self._ws_connection or not self.is_connected:
            logger.error(f"[{self.exchange_name}] Cannot subscribe, WebSocket not connected.")
            # Store handler for reconnection
            self._ws_handlers[topic] = handler
            return

        subscription_message: dict[str, Any] = {
            "op": "subscribe",
            "channel": topic,
            "args": {},  # Additional arguments if needed
        }
        try:
            await self._ws_connection.send_json(subscription_message)
            self._ws_handlers[topic] = handler
            logger.info(f"[{self.exchange_name}] Subscribed to topic: {topic}")
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Failed to subscribe to topic {topic}: {e}")

    async def _resubscribe(self) -> None:
        """
        Resubscribe to all registered WebSocket topics after reconnecting.
        """
        logger.info(
            f"[{self.exchange_name}] Resubscribing to topics: {list(self._ws_handlers.keys())}"
        )
        handlers_copy = self._ws_handlers.copy()
        for topic, handler in handlers_copy.items():
            await self.subscribe(topic, handler)
            await asyncio.sleep(0.1)  # Small delay between subscriptions

    # --- Authentication --- #

    async def _authenticate(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Authenticate and sign an API request for Backpack.

        Returns:
            Dictionary with signed headers and parameters.
        """
        return self._sign_request(method, path, params, data)

    def _sign_request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Sign a REST API request using HMAC-SHA256 as required by Backpack.

        Returns:
            Dictionary with signed headers and parameters.
        Raises:
            APIError: If API key/secret are missing.
        """
        if not self._api_key or not self._api_secret:
            raise APIError("Backpack API key and secret required for signed requests.")

        timestamp = str(int(time.time() * 1000))

        # Create signature string based on Backpack requirements
        signature_payload = timestamp
        if method == "GET" and params:
            query_string = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
            signature_payload += query_string
        elif (method == "POST" or method == "PUT" or method == "DELETE") and data:
            # For POST requests with JSON body
            import json

            signature_payload += json.dumps(data)

        # Create signature
        signature = hmac.new(
            self._api_secret.encode("utf-8"),
            signature_payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        # Return headers and potentially modified params/data
        return {
            "headers": {
                "X-API-Key": self._api_key,
                "X-Timestamp": timestamp,
                "X-Signature": signature,
            },
            "params": params,
            "data": data,
        }

    # --- Core API Implementation --- #

    async def get_ticker(self, symbol: str) -> Ticker:
        """
        Fetch the current ticker for a given symbol.

        Args:
            symbol: Trading symbol (e.g., 'BTC_USDC').
        Returns:
            Ticker: Validated ticker model.
        Raises:
            APIError: If the ticker cannot be fetched or validated.
        """
        request_path: str = f"/api/v1/ticker/{symbol}"
        try:
            response: Any = await self._request("GET", request_path)
            return Ticker.model_validate(response)
        except ValidationError as e:
            logger.error(f"[{self.exchange_name}] Ticker validation failed: {e}")
            raise APIError(
                f"Invalid ticker response for {symbol}: {e}",
                code=APIErrorCode.INVALID_REQUEST,
            ) from e
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Error getting ticker for {symbol}: {e}", exc_info=True
            )
            raise BackpackErrorMapper.map_error_response(
                status_code=getattr(e, "status", None),
                error_body=f"Error getting ticker for {symbol}: {e}",
                request_path=request_path,
            ) from e

    async def get_order_book(self, symbol: str, depth: int = 20) -> OrderBook:
        """
        Fetch the order book for a given symbol, with strict type validation.

        Args:
            symbol: Trading symbol.
            depth: Number of levels to fetch (default 20).
        Returns:
            OrderBook: Validated order book model.
        Raises:
            APIError: On error or malformed response.
        """
        request_path: str = "/api/v1/depth"

        def safe_list_of_pairs(raw: object) -> list[tuple[str, str]]:
            """
            Safely converts a list of pairs (tuples or lists of length 2) of arbitrary objects
            into a list of string pairs. Ignores any entries that are not tuples/lists of length 2.

            Args:
                raw (object): Input expected to be a list of pairs (tuples or lists).

            Returns:
                list[tuple[str, str]]: List of pairs as (str, str).
            """
            if not isinstance(raw, list):
                return []
            result: list[tuple[str, str]] = []
            entry_item: Any
            for entry_item in raw:
                item = entry_item
                if isinstance(item, tuple):
                    entry_pair = cast(Sequence[Any], item)
                elif isinstance(item, list):
                    entry_pair = cast(Sequence[Any], item)
                else:
                    continue
                if len(entry_pair) == 2:
                    a0: Any = entry_pair[0]
                    a1: Any = entry_pair[1]
                    result.append((str(a0), str(a1)))
            return result

        try:
            params: dict[str, object] = {"symbol": symbol}
            if depth:
                params["limit"] = depth

            response_raw: object = await self._request("GET", request_path, params=params)
            if not isinstance(response_raw, dict):
                raise APIError(f"Unexpected response type for order book: {type(response_raw)}")
            response: dict[str, object] = response_raw
            bids_raw = safe_list_of_pairs(response.get("bids", []) or [])
            asks_raw = safe_list_of_pairs(response.get("asks", []) or [])
            bids: list[tuple[Decimal, Decimal]] = []
            asks: list[tuple[Decimal, Decimal]] = []
            for price_raw, qty_raw in bids_raw:
                try:
                    price: Decimal = Decimal(price_raw)
                    qty: Decimal = Decimal(qty_raw)
                    bids.append((price, qty))
                except Exception:
                    continue
            for price_raw, qty_raw in asks_raw:
                try:
                    price_ask: Decimal = Decimal(price_raw)
                    qty_ask: Decimal = Decimal(qty_raw)
                    asks.append((price_ask, qty_ask))
                except Exception:
                    continue
            timestamp_raw = response.get("time", int(time.time() * 1000))
            if isinstance(timestamp_raw, int | float | str):
                timestamp = int(float(timestamp_raw))
            else:
                timestamp = int(time.time() * 1000)
            return OrderBook(
                symbol=symbol,
                bids=bids,
                asks=asks,
                timestamp=timestamp,
            )
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Error getting order book for {symbol}: {e}", exc_info=True
            )
            raise BackpackErrorMapper.map_error_response(
                status_code=getattr(e, "status", None),
                error_body=f"Error getting order book for {symbol}: {e}",
                request_path=request_path,
            ) from e

    async def get_recent_trades(self, symbol: str, limit: int | None = 50) -> list[Trade]:
        """
        Get recent trades for a symbol, mapping all required fields for the Trade model.

        Args:
            symbol: Trading symbol
            limit: Maximum number of trades to return

        Returns:
            List of Trade objects
        """
        request_path = "/api/v1/trades"
        try:
            params: dict[str, Any] = {"symbol": symbol}
            if limit is not None:
                params["limit"] = limit
            response: Any = await self._request("GET", request_path, params=params)

            if not isinstance(response, list):
                logger.warning(
                    f"[{self.exchange_name}] Unexpected trades response type: {type(response)}"
                )
                return []

            trades: list[Trade] = []
            trade_data_raw: Any
            for trade_data_raw in response:
                if not isinstance(trade_data_raw, dict):
                    logger.warning(
                        f"[{self.exchange_name}] Skipping malformed trade data: {trade_data_raw}"
                    )
                    continue
                trade_data = cast(dict[str, Any], trade_data_raw)
                try:
                    trade_id: str = str(trade_data.get("id", ""))
                    order_id: str = str(trade_data.get("orderId", ""))
                    client_order_id: str = str(trade_data.get("clientOrderId", ""))
                    try:
                        price: Decimal = Decimal(str(trade_data.get("price", "0")))
                        quantity: Decimal = Decimal(str(trade_data.get("qty", "0")))
                    except Exception as e:
                        logger.warning(
                            f"[{self.exchange_name}] Invalid price/qty in trade: {trade_data} ({e})"
                        )
                        continue
                    raw_time = trade_data.get("time")
                    if raw_time is None:
                        logger.warning(f"[{self.exchange_name}] Trade missing 'time': {trade_data}")
                        continue
                    try:
                        executed_at: datetime = datetime.fromtimestamp(float(raw_time) / 1000, UTC)
                    except Exception as e:
                        logger.warning(
                            f"[{self.exchange_name}] Invalid 'time' in trade: {trade_data} ({e})"
                        )
                        continue
                    try:
                        cost: Decimal = price * quantity
                    except Exception as e:
                        logger.warning(
                            f"[{self.exchange_name}] Error calculating cost: {trade_data} ({e})"
                        )
                        continue
                    side: OrderSide | None = None
                    if "isBuyerMaker" in trade_data:
                        side = (
                            OrderSide.BUY
                            if bool(trade_data.get("isBuyerMaker", False))
                            else OrderSide.SELL
                        )
                    else:
                        logger.warning(
                            f"[{self.exchange_name}] Trade missing 'isBuyerMaker': {trade_data}"
                        )
                        continue
                    fee: Decimal = Decimal("0")
                    fee_asset: str = ""
                    is_maker_raw = trade_data.get("isMaker", None)
                    is_maker: bool | None = bool(is_maker_raw) if is_maker_raw is not None else None
                    # Pydantic-ready: all required fields present, types correct
                    trade = Trade(
                        id=trade_id,
                        symbol=symbol,
                        executed_at=executed_at,
                        side=side,
                        order_id=order_id,
                        exchange=self.exchange_name,
                        client_order_id=client_order_id,
                        price=price,
                        quantity=quantity,
                        cost=cost,
                        fee=fee,
                        fee_asset=fee_asset,
                        is_maker=is_maker,
                        timestamp=int(float(raw_time)),
                    )
                    trades.append(trade)
                except Exception as e:
                    logger.warning(
                        f"[{self.exchange_name}] Error processing trade: {e} | Data: {trade_data}"
                    )
            return trades
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Error getting recent trades for {symbol}: {e}",
                exc_info=True,
            )
            # TODO: When refactoring for Pydantic, raise a validation error with details
            raise APIError(
                message=f"Error getting recent trades for {symbol}: {e}",
                code=APIErrorCode.UNKNOWN,
                http_status=getattr(e, "status", None),
            ) from e

    async def get_funding_rate(self, symbol: str) -> FundingRate | None:
        """
        Get funding rate information for a symbol.

        Args:
            symbol: Trading symbol

        Returns:
            FundingRate object or None if error
        """
        try:
            # Backpack Funding rate endpoint (assuming perpetuals/futures)
            # Example: Adjust path and params based on actual Backpack API
            params = {"symbol": symbol}
            response = await self._request("GET", "/api/v1/funding", params=params)

            # Check if response is a list and take the first element if needed
            if isinstance(response, list) and response:
                funding_data = response[0]
            elif isinstance(response, dict):
                funding_data = response
            else:
                logger.warning(
                    f"[{self.exchange_name}] Unexpected funding rate data format for {symbol}: "
                    f"{response}"
                )
                return None

            # Ensure fields exist before accessing
            if not all(k in funding_data for k in ["rate", "markPrice", "indexPrice", "time"]):
                logger.warning(
                    f"[{self.exchange_name}] Missing keys in funding rate data for {symbol}: "
                    f"{funding_data}"
                )
                return None

            # Create FundingRate object using Decimal and int
            funding_rate = FundingRate(
                symbol=symbol,
                funding_rate=Decimal(str(funding_data["rate"])),  # Corrected field name
                mark_price=Decimal(
                    str(funding_data["markPrice"])
                ),  # Convert string/float to Decimal
                index_price=Decimal(
                    str(funding_data["indexPrice"])
                ),  # Convert string/float to Decimal
                timestamp=int(funding_data["time"]),  # Ensure timestamp is int
            )
            return funding_rate
        except APIError as e:
            if e.code == APIErrorCode.SYMBOL_NOT_FOUND:
                logger.info(f"[{self.exchange_name}] No funding rate found for symbol {symbol}.")
                return None
            logger.error(f"[{self.exchange_name}] API error getting funding rate for {symbol}: {e}")
            return None
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Error getting funding rate for {symbol}: {e}")
            return None

    async def get_balances(self) -> dict[str, Balance]:
        """Get account balances."""
        try:
            response: Any = await self._request("GET", "/api/v1/capital")
            if not isinstance(response, dict):
                logger.warning(
                    f"[{self.exchange_name}] Unexpected response type for balances: "
                    f"{type(response)}. Returning empty balances."
                )
                return {}
            balances: dict[str, Balance] = {}
            asset_key: Any
            data_val: Any
            for asset_key, data_val in response.items():
                key = asset_key
                val = data_val
                asset_str = str(key)
                data_dict = cast(dict[str, Any], val)
                available = Decimal(str(data_dict.get("available", "0")))
                total = Decimal(str(data_dict.get("total", "0")))
                balances[asset_str] = Balance(
                    asset=asset_str,
                    available=available,
                    total=total,
                )
            return balances
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Error getting balances: {e}")
            return {}

    async def get_positions(self, symbol: str | None = None) -> list[Position]:
        """
        Get current positions.
        Currently fetches all positions, ignoring the optional symbol filter.

        Args:
            symbol: Trading symbol (optional, currently ignored).

        Returns:
            List of Position objects.
        """
        if symbol:
            logger.warning(
                f"[{self.exchange_name}] get_positions called with symbol '{symbol}', "
                "but Backpack API currently fetches all positions."
            )
        try:
            response: Any = await self._request("GET", "/api/v1/positions", signed=True)
            if not isinstance(response, list):
                logger.warning(
                    f"[backpack] Unexpected response type for positions: "
                    f"{type(response)}. Returning empty list."
                )
                return []
            positions: list[Position] = []
            pos_data_item: Any
            for pos_data_item in response:
                item = pos_data_item
                if not isinstance(item, dict):
                    logger.warning(
                        f"[backpack] Unexpected position entry type: {type(item)}. Skipping entry."
                    )
                    continue
                pos_data: dict[str, Any] = cast(dict[str, Any], item)
                symbol_from_data = pos_data.get("symbol")
                if not isinstance(symbol_from_data, str) or not symbol_from_data:
                    logger.warning(f"[backpack] Position missing or invalid 'symbol': {pos_data}")
                    continue
                try:
                    size_dec = Decimal(str(pos_data.get("positionSize", "0")))
                    entry_price_dec = Decimal(str(pos_data.get("entryPrice", "0")))
                    mark_price_dec = Decimal(str(pos_data.get("markPrice", "0")))
                    liq_price_str = pos_data.get("liquidationPrice")
                    liq_price_dec = (
                        Decimal(str(liq_price_str))
                        if liq_price_str is not None and liq_price_str != "0"
                        else Decimal("0.0")
                    )
                    pnl_dec = Decimal(str(pos_data.get("unrealizedPnl", "0")))
                    leverage_float = float(pos_data.get("leverage", "1.0"))
                    leverage_dec = Decimal(str(leverage_float))
                    position = Position(
                        symbol=symbol_from_data,
                        size=size_dec,
                        entry_price=entry_price_dec,
                        mark_price=mark_price_dec,
                        side=OrderSide.BUY if size_dec > Decimal("0") else OrderSide.SELL,
                        liquidation_price=liq_price_dec,
                        unrealized_pnl=pnl_dec,
                        leverage=leverage_dec,
                    )
                    positions.append(position)
                except Exception as e:
                    logger.warning(f"[backpack] Error processing position: {e} | Data: {pos_data}")
            return positions
        except Exception as e:
            logger.error(f"[backpack] Error getting positions: {e}")
            return []

    async def place_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        time_in_force: TimeInForce,
        price: Decimal | None = None,
        client_order_id: str | None = None,
        reduce_only: bool = False,
        post_only: bool = False,
    ) -> Order:
        """
        Place an order on Backpack Exchange. Conforms to ExchangeAPI interface.

        Args:
            symbol: Trading symbol (e.g., 'BTC_USDC')
            side: Order side (BUY or SELL)
            order_type: Order type (LIMIT, MARKET, etc.)
            quantity: Order quantity (as Decimal)
            time_in_force: Time in force (GTC, IOC, FOK). Defaults to GTC if None.
            price: Order price (required for limit orders, as Decimal)
            client_order_id: Custom client order ID
            reduce_only: Whether this is a reduce-only order (bool)
            post_only: Whether this is a post-only order (bool) - currently ignored
                if not supported by API call

        Returns:
            Order object if successful.

        Raises:
            ValueError: If price is missing for a LIMIT order.
            APIError: On API errors or if the order placement fails.
        """
        request_path = "/api/v1/order"
        try:
            order_data: dict[str, Any] = {
                "symbol": symbol,
                "side": side.value,
                "orderType": order_type.value,
                "quantity": str(quantity),
            }

            tif_value = time_in_force.value
            order_data["timeInForce"] = tif_value

            if order_type == OrderType.LIMIT:
                if price is None:
                    raise ValueError("Price is required for LIMIT orders")
                order_data["price"] = str(price)
                if post_only:
                    order_data["postOnly"] = True

            if client_order_id:
                order_data["clientId"] = client_order_id
            if reduce_only:
                order_data["reduceOnly"] = True

            response = await self._request("POST", request_path, data=order_data, signed=True)

            if not response or not isinstance(response, dict) or "id" not in response:
                raise APIError(
                    message=(
                        f"[{self.exchange_name}] Failed to place order. Invalid response: "
                        f"{response}"
                    ),
                    code=APIErrorCode.UNKNOWN,
                    http_status=None,
                )

            # The order creation logic has been moved to the _transform_raw_order_to_internal method
            return BackpackOrderMapper.transform_raw_order_to_internal(
                BackpackRawOrder.model_validate(response)
            )
        except ValueError as ve:
            raise ve
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Error placing order: {e}", exc_info=True)
            # Attempt to map the error, default to generic EXCHANGE_ERROR
            api_error = BackpackErrorMapper.map_error_response(
                status_code=getattr(e, "status", None),
                error_body=str(e),
                exchange_message=f"Error placing order for {symbol} (Path: {request_path}): {e}",
            )
            raise api_error from e

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> dict[str, Any]:
        """Cancel an existing order. Conforms to ExchangeAPI interface."""
        if symbol is None:
            # Attempt to lookup symbol from order_id if possible, or raise error
            # For now, raise error as Backpack API likely requires symbol
            raise ValueError("Symbol is required to cancel order on Backpack")

        request_path = "/api/v1/order"
        try:
            params = {"symbol": symbol, "orderId": order_id}
            # Response might be empty on success or contain order details
            response = await self._request("DELETE", request_path, params=params, signed=True)
            logger.info(f"[{self.exchange_name}] Canceled order {order_id} for {symbol}")
            # Return success dictionary as per ExchangeAPI
            return {"success": True, "orderId": order_id, "symbol": symbol, "response": response}
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Error canceling order {order_id}: {e}", exc_info=True
            )
            # Raise APIError as per ExchangeAPI
            api_error = BackpackErrorMapper.map_error_response(
                status_code=getattr(e, "status", None),
                error_body=str(e),
                exchange_message=(
                    f"Error canceling order {order_id} for {symbol} (Path: {request_path}): {e}"
                ),
            )
            raise api_error from e

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        request_path = "/api/v1/orders"
        try:
            params: dict[str, str] = {}
            if symbol:
                params["symbol"] = symbol
            response = await self._request("GET", request_path, params=params, signed=True)
            orders: list[Order] = []
            if not isinstance(response, list):
                logger.warning(
                    f"[{self.exchange_name}] Unexpected open orders response type: {type(response)}"
                )
                return []
            for order_data_item in response:
                if isinstance(order_data_item, dict):
                    try:
                        raw_order = BackpackRawOrder.model_validate(order_data_item)
                        internal_order = BackpackOrderMapper.transform_raw_order_to_internal(
                            raw_order
                        )
                        if internal_order.status in [
                            OrderStatus.NEW,
                            OrderStatus.OPEN,
                            OrderStatus.PARTIALLY_FILLED,
                        ]:
                            orders.append(internal_order)
                    except ValidationError as e:
                        msg = (
                            f"[{self.exchange_name}] Skipping order due to "
                            f"Pydantic validation error: {e}. "
                            f"Data: {order_data_item}"
                        )
                        logger.warning(msg)
                    except APIError as e:
                        msg = (
                            f"[{self.exchange_name}] Skipping order due to "
                            f"transformation error: {e}. "
                            f"Data: {order_data_item}"
                        )
                        logger.warning(msg)
                    except Exception as e:
                        logger.error(
                            f"[{self.exchange_name}] Unexpected error processing single order: {e}",
                            exc_info=True,
                        )
                else:
                    logger.warning(
                        f"[{self.exchange_name}] Skipping non-dict item in orders list: "
                        f"{order_data_item}"
                    )
            return orders
        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error getting open orders: {e}")
            raise
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error in get_open_orders: {e}", exc_info=True
            )
            raise APIError(f"Failed to get open orders: {e}", code=APIErrorCode.UNKNOWN) from e

    async def fetch_ticker(self, symbol: str) -> Ticker:
        """Fetch ticker information (ensuring non-None return)."""
        ticker = await self.get_ticker(symbol)
        return ticker

    async def fetch_order_book(self, symbol: str, depth: int | None = None) -> OrderBook:
        """Fetch order book (ensuring non-None return)."""
        order_book = await self.get_order_book(symbol, depth if depth is not None else 20)
        return order_book

    async def fetch_trades(self, symbol: str, limit: int | None = None) -> list[Trade]:
        """Fetch recent trades (ensuring non-None return)."""
        trades = await self.get_recent_trades(symbol, limit)
        return trades

    async def fetch_funding_rate(self, symbol: str) -> FundingRate:
        """Fetch funding rate for a symbol.
        Note: Base class ExchangeAPI expects get_funding_rates (plural).

        Returns:
            FundingRate object.

        Raises:
            APIError: If the funding rate cannot be fetched.
        """
        # Assuming Backpack provides funding rates via a specific endpoint
        # This endpoint might need adjustment based on actual API docs
        request_path = f"/api/v1/funding/{symbol}"
        try:
            response = await self._request("GET", request_path)
            if not isinstance(response, dict):
                logger.error(
                    f"[{self.exchange_name}] Unexpected response type for funding rate: "
                    f"{type(response)}"
                )
                raise APIError(f"Could not fetch funding rate for {symbol}")
            timestamp = int(response.get("time", int(time.time() * 1000)))
            funding_rate_dec = Decimal(str(response.get("fundingRate", "0")))
            mark_price_dec = (
                Decimal(str(response.get("markPrice", "0"))) if response.get("markPrice") else None
            )

            if mark_price_dec is None:
                logger.warning(
                    f"[{self.exchange_name}] Mark price not found in funding rate "
                    f"response for {symbol}."
                )
                # Handle missing mark price appropriately, e.g., raise or use a default
                # For now, let's create the object but log the warning.

            return FundingRate(
                symbol=symbol,
                timestamp=timestamp,
                funding_rate=funding_rate_dec,
                mark_price=mark_price_dec,
            )
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Error getting funding rate for {symbol}: {e}",
                exc_info=True,
            )
            # Use exchange_message for context in APIError call
            api_error = BackpackErrorMapper.map_error_response(
                status_code=getattr(e, "status", None),
                error_body=str(e),
                exchange_message=(
                    f"Error getting funding rate for {symbol} (Path: {request_path}): {e}"
                ),
            )
            raise api_error from e

    # --- Placeholder for required abstract method --- #
    async def get_funding_rates(self, symbols: list[str] | None = None) -> list[FundingRate]:
        """(Not Implemented) Get funding rates for one/all symbols."""
        logger.warning(
            f"[{self.exchange_name}] get_funding_rates not fully implemented. "
            f"Fetching current rate only."
        )
        rates: list[FundingRate] = []
        if symbols:
            for symbol in symbols:
                try:
                    current_rate: FundingRate = await self.fetch_funding_rate(symbol)
                    rates.append(current_rate)
                except APIError as e:
                    logger.error(
                        f"[{self.exchange_name}] Failed to fetch current funding rate for "
                        f"{symbol} within get_funding_rates: {e}"
                    )
                    raise
        else:
            logger.error(
                f"[{self.exchange_name}] get_funding_rates without a specific symbol "
                f"is not supported by Backpack API."
            )
            raise APIError(
                code=APIErrorCode.INVALID_PARAMS,
                message="Symbol is required for get_funding_rates on Backpack",
            )
        return rates

    # --- Account Management --- #
    async def get_account_info(self) -> dict[str, Any]:
        """Get general account information (Example)."""
        # This method is just an example; Backpack might not have this exact endpoint.
        request_path = "/api/v1/account"
        try:
            response = await self._request("GET", request_path, signed=True)
            if not isinstance(response, dict):
                logger.warning(
                    f"[{self.exchange_name}] Unexpected response type for account info: "
                    f"{type(response)}. Returning empty dict."
                )
                return {}
            return response
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Error getting account info: {e}")
            # Raise APIError for consistency.
            api_error = BackpackErrorMapper.map_error_response(
                status_code=getattr(e, "status", None),
                error_body=str(e),
                exchange_message=f"Error getting account info (Path: {request_path}): {e}",
            )
            raise api_error from e

    async def transfer(
        self, asset: str, amount: float, from_account: str, to_account: str
    ) -> dict[str, Any]:
        """Transfer funds between accounts."""
        logger.warning("Backpack API might not support internal transfers.")
        raise NotImplementedError("Backpack internal transfers not implemented.")

    async def withdraw(
        self, asset: str, address: str, amount: Decimal, network: str | None = None
    ) -> dict[str, Any] | None:
        """Withdraw funds."""
        logger.warning(f"[{self.exchange_name}] withdraw not implemented for Backpack.")
        return None

    async def subscribe_to_order_book(self, symbol: str) -> None:
        """Subscribe to order book updates for a symbol."""
        # Backpack topic format might be different, e.g., "depth.BTC_USDC"
        topic = f"depth.{symbol}"
        # The handler is now managed internally by the base class or needs a different approach
        # This method just needs to send the subscription command.
        if self._ws_connection and self.is_connected:
            subscription_message = {"method": "SUBSCRIBE", "params": [topic]}
            try:
                await self._ws_connection.send_json(subscription_message)
                logger.info(f"[{self.exchange_name}] Sent subscription request for topic: {topic}")
            except Exception as e:
                logger.error(
                    f"[{self.exchange_name}] Failed to send subscription for topic {topic}: {e}"
                )
        else:
            logger.warning(
                f"[{self.exchange_name}] Cannot subscribe to {topic}, WebSocket not connected."
            )

    async def subscribe_to_ticker(self, symbol: str) -> None:
        """Subscribe to ticker updates for a symbol."""
        topic = f"ticker.{symbol}"
        if self._ws_connection and self.is_connected:
            subscription_message = {"method": "SUBSCRIBE", "params": [topic]}
            try:
                await self._ws_connection.send_json(subscription_message)
                logger.info(f"[{self.exchange_name}] Sent subscription request for topic: {topic}")
            except Exception as e:
                logger.error(
                    f"[{self.exchange_name}] Failed to send subscription for topic {topic}: {e}"
                )
        else:
            logger.warning(
                f"[{self.exchange_name}] Cannot subscribe to {topic}, WebSocket not connected."
            )

    async def subscribe_to_trades(self, symbol: str) -> None:
        """Subscribe to public trade updates for a symbol."""
        topic = f"trades.{symbol}"
        if self._ws_connection and self.is_connected:
            subscription_message = {"method": "SUBSCRIBE", "params": [topic]}
            try:
                await self._ws_connection.send_json(subscription_message)
                logger.info(f"[{self.exchange_name}] Sent subscription request for topic: {topic}")
            except Exception as e:
                logger.error(
                    f"[{self.exchange_name}] Failed to send subscription for topic {topic}: {e}"
                )
        else:
            logger.warning(
                f"[{self.exchange_name}] Cannot subscribe to {topic}, WebSocket not connected."
            )

    # TODO: Implement remaining abstract methods from ExchangeAPI
    #       (e.g., get_order_status, get_recent_fills, connect_websocket, etc.)


class BackpackOrderMapper:
    """
    Utility for transforming Backpack raw order/event models to CyberDeltaEngine internal models.

    - Maps Backpack string enums to internal enums (OrderSide, OrderStatus, etc.).
    - Handles defensive parsing and validation of all fields.
    - Used by BackpackAPI for all order-related transformations.
    """

    @staticmethod
    def map_side_to_internal(bp_side: str) -> OrderSide:
        side_lower = bp_side.lower() if bp_side else ""
        if side_lower in ("buy", "bid"):
            return OrderSide.BUY
        elif side_lower in ("sell", "ask"):
            return OrderSide.SELL
        logger.warning(f"[BackpackOrderMapper] Unknown order side '{bp_side}', defaulting to BUY.")
        return OrderSide.BUY

    @staticmethod
    def map_status_to_internal(bp_status: str) -> OrderStatus:
        status_upper = (bp_status or "").upper()
        mapping = {
            "NEW": OrderStatus.NEW,
            "OPEN": OrderStatus.OPEN,
            "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
            "FILLED": OrderStatus.FILLED,
            "CANCELLED": OrderStatus.CANCELED,
            "EXPIRED": OrderStatus.EXPIRED,
            "REJECTED": OrderStatus.REJECTED,
            "TRIGGER_PENDING": OrderStatus.TRIGGER_PENDING,
            "FAILED": OrderStatus.FAILED,
        }
        if status_upper in mapping:
            return mapping[status_upper]
        logger.warning(
            f"[BackpackOrderMapper] Unknown order status '{bp_status}', mapping to UNKNOWN."
        )
        return OrderStatus.UNKNOWN

    @staticmethod
    def map_type_to_internal(bp_type: str) -> OrderType:
        type_upper = (bp_type or "").upper()
        mapping = {
            "LIMIT": OrderType.LIMIT,
            "MARKET": OrderType.MARKET,
            "STOP_MARKET": OrderType.STOP_MARKET,
            "STOP_LIMIT": OrderType.STOP_LIMIT,
            "TAKE_PROFIT_MARKET": OrderType.TAKE_PROFIT_MARKET,
            "TAKE_PROFIT_LIMIT": OrderType.TAKE_PROFIT_LIMIT,
        }
        if type_upper in mapping:
            return mapping[type_upper]
        logger.warning(
            f"[BackpackOrderMapper] Unknown order type '{bp_type}', defaulting to LIMIT."
        )
        return OrderType.LIMIT

    @staticmethod
    def map_tif_to_internal(bp_tif: str | None) -> TimeInForce:
        if not bp_tif:
            return TimeInForce.GTC
        try:
            return TimeInForce(bp_tif.upper())
        except Exception:
            logger.warning(f"[BackpackOrderMapper] Unknown TIF '{bp_tif}', defaulting to GTC.")
            return TimeInForce.GTC

    @staticmethod
    def map_trigger_by_to_internal(trigger_by: str | None) -> TriggerType | None:
        if not trigger_by:
            return None
        try:
            return TriggerType(trigger_by)
        except Exception:
            logger.warning(
                f"[BackpackOrderMapper] Unknown trigger_by '{trigger_by}', returning None."
            )
            return None

    @staticmethod
    def map_stp_to_internal(stp: str | None) -> SelfTradePrevention | None:
        if not stp:
            return None
        try:
            return SelfTradePrevention(stp)
        except Exception:
            logger.warning(
                f"[BackpackOrderMapper] Unknown self_trade_prevention '{stp}', returning None."
            )
            return None

    @staticmethod
    def map_expiry_reason_to_internal(reason: str | None) -> OrderExpiryReason | None:
        if not reason:
            return None
        try:
            return OrderExpiryReason(reason)
        except Exception:
            logger.warning(
                f"[BackpackOrderMapper] Unknown expiry_reason '{reason}', returning None."
            )
            return None

    @staticmethod
    def map_origin_to_internal(origin: str | None) -> OrderUpdateOrigin | None:
        if not origin:
            return None
        try:
            return OrderUpdateOrigin(origin)
        except Exception:
            logger.warning(f"[BackpackOrderMapper] Unknown origin '{origin}', returning None.")
            return None

    @staticmethod
    def transform_raw_order_to_internal(raw: BackpackRawOrder) -> Order:
        # Defensive: ensure required fields are present and valid
        parsed_quantity = parse_decimal_value(raw.quantity, allow_none=False)
        if parsed_quantity is None:
            raise ValueError("quantity missing/invalid in BackpackRawOrder")
        parsed_created_at = parse_datetime_utc(raw.createdAt)
        if parsed_created_at is None:
            raise ValueError("createdAt missing/invalid in BackpackRawOrder")
        # Optional fields
        parsed_quantity_filled = parse_decimal_value(raw.executedQuantity) or Decimal("0.0")
        parsed_executed_quote_quantity = parse_decimal_value(raw.executedQuoteQuantity)
        parsed_price = parse_decimal_value(raw.price)
        parsed_stop_price = parse_decimal_value(raw.triggerPrice)
        parsed_avg_fill_price = parse_decimal_value(raw.avgFillPrice)
        return Order(
            client_order_id=raw.clientId or "",
            exchange_order_id=raw.id,
            related_order_id=raw.relatedOrderId,
            exchange="backpack",
            symbol=raw.symbol,
            side=BackpackOrderMapper.map_side_to_internal(raw.side),
            order_type=BackpackOrderMapper.map_type_to_internal(raw.orderType),
            status=BackpackOrderMapper.map_status_to_internal(raw.status),
            quantity_requested=parsed_quantity,
            quantity_filled=parsed_quantity_filled,
            executed_quote_quantity=parsed_executed_quote_quantity,
            price=parsed_price,
            stop_price=parsed_stop_price,
            average_fill_price=parsed_avg_fill_price,
            trigger_by=BackpackOrderMapper.map_trigger_by_to_internal(raw.triggerBy),
            time_in_force=BackpackOrderMapper.map_tif_to_internal(raw.timeInForce),
            reduce_only=raw.reduceOnly or False,
            post_only=raw.postOnly or False,
            self_trade_prevention=BackpackOrderMapper.map_stp_to_internal(raw.selfTradePrevention),
            created_at=parsed_created_at,
            updated_at=parse_datetime_utc(raw.updatedAt),
            triggered_at=parse_datetime_utc(raw.triggeredAt),
            expiry_reason=BackpackOrderMapper.map_expiry_reason_to_internal(raw.expiryReason),
            origin=BackpackOrderMapper.map_origin_to_internal(raw.origin),
            strategy_name=raw.strategyName,
            signal_id=raw.signalId,
            trades=raw.trades or [],
        )
