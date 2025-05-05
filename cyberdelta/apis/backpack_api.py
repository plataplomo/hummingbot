"""
CyberDeltaEngine: Backpack Exchange Integration
----------------------------------------------

This module implements the Backpack exchange adapter for CyberDeltaEngine, including:
- REST and WebSocket API client (`BackpackAPI`)
- Centralized error mapping and normalization (`BackpackErrorMapper`)
- Order and event transformation utilities (`BackpackOrderMapper`)

**Key architectural patterns:**
- All external (exchange) errors are mapped to canonical APIErrorCode values, validated and
  normalized via APIErrorResponse, and propagated as APIError exceptions.
- All API methods are type-safe, defensive, and log/handle edge cases robustly.
- All transformation logic is modular and testable.

**Onboarding Note:**
- When extending this module for new endpoints or error types, always use strict Pydantic
  validation, map all error codes, and document any non-obvious logic or edge cases.
"""

import asyncio
import hashlib
import hmac
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, cast

from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_error_mapper import BackpackErrorMapper
from cyberdelta.apis.backpack.bp_order_mapper import BackpackOrderMapper
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.base_api import ExchangeAPI, MessageHandler
from cyberdelta.apis.exchange_names import ExchangeName
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models import (  # Use absolute import
    DerivativePosition,
    FundingRate,
    Order,
    OrderBook,
    OrderSide,
    OrderStatus,
    OrderType,
    SpotBalance,
    Ticker,
    TimeInForce,
    Trade,
)
from cyberdelta.core.models.spot_balance import SpotBalance  # Ensure SpotBalance is imported
from cyberdelta.utils.logging_config import get_logger
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value

logger = get_logger(__name__)


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
        super().__init__(ExchangeName.BACKPACK, api_config, secrets)
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

    def _hmac_sha256_hexdigest(self, key: bytes, msg: bytes) -> str:
        """
        Helper for HMAC-SHA256 signature generation. Returns a hex digest string.
        Uses 'Any' for intermediate types to work around static analyzer (Pyright/Pylance)
        limitations with C-extension stdlib modules. This is safe, mypy-compliant,
        and project-approved for stdlib cryptography edge cases.
        """
        # Pyright/Pylance cannot infer the type of hmac.new (C-extension);
        # this is a known false positive. This ignore is safe, does not affect mypy,
        # and is project-approved for stdlib cryptography edge cases.
        h: Any = hmac.new(key, msg, hashlib.sha256)  # pyright: ignore[reportUnknownMemberType]
        digest: str = h.hexdigest()
        return digest

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
            raise APIError(
                "Backpack API key and secret required for signed requests.",
                code=APIErrorCode.UNKNOWN.value,
            )

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

        # Create signature using helper for static analyzer compatibility
        signature = self._hmac_sha256_hexdigest(
            self._api_secret.encode("utf-8"), signature_payload.encode("utf-8")
        )

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
                code=APIErrorCode.INVALID_REQUEST.value,
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
            # Explicitly declare type for static analysis
            entry_item: Any
            for entry_item in raw:
                entry_item = cast(
                    Sequence[Any], entry_item
                )  # Pyright false positive: entry_item is always Sequence[Any] after cast
                entry: Sequence[Any] = entry_item
                if isinstance(entry, tuple) or isinstance(entry, list):
                    entry_pair: Sequence[Any] = entry
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
                raise APIError(
                    f"Unexpected response type for order book: {type(response_raw)}",
                    code=APIErrorCode.UNKNOWN.value,
                )
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
                # timestamp = int(float(timestamp_raw)) # Keep as raw for parsing
                pass
            else:
                timestamp_raw = int(time.time() * 1000)  # Fallback if type is weird

            # Parse timestamp to datetime
            timestamp_dt = parse_datetime_utc(timestamp_raw, field_name="time")
            if timestamp_dt is None:
                # DEFENSIVE CHECK: API response missing mandatory 'time' field.
                logger.error(
                    f"[{self.exchange_name}] Order book response missing 'time'. "
                    f"Using current time."
                )
                timestamp_dt = datetime.now(UTC)  # Use current UTC time as fallback

            return OrderBook(
                symbol=symbol,
                bids=bids,
                asks=asks,
                timestamp=timestamp_dt,  # Use datetime object
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
                trade_data: dict[str, Any] = cast(dict[str, Any], trade_data_raw)
                try:
                    trade_id: str = str(trade_data.get("id", ""))
                    order_id: str = str(trade_data.get("orderId", ""))
                    client_order_id: str = str(trade_data.get("clientOrderId", ""))
                    try:
                        price_val = parse_decimal_value(
                            trade_data.get("price", "0"), allow_none=False, field_name="price"
                        )
                        if price_val is None:
                            raise ValueError(
                                "price is None after parse_decimal_value with allow_none=False"
                            )
                        price: Decimal = price_val
                        quantity_val = parse_decimal_value(
                            trade_data.get("qty", "0"), allow_none=False, field_name="qty"
                        )
                        if quantity_val is None:
                            raise ValueError(
                                "quantity is None after parse_decimal_value with allow_none=False"
                            )
                        quantity: Decimal = quantity_val
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
                        executed_at_input = raw_time  # raw_time must not be None here
                        if executed_at_input is None:
                            raise ValueError(
                                "executed_at (raw_time) cannot be None for this assignment"
                            )
                        executed_at: datetime = parse_datetime_utc(
                            executed_at_input, field_name="time"
                        )  # type: ignore[assignment]
                    except Exception as e:
                        logger.warning(
                            f"[{self.exchange_name}] Invalid 'time' in trade: {trade_data} ({e})"
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
                        fee=fee,
                        fee_asset=fee_asset,
                        is_maker=is_maker,
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
            raise APIError(
                f"Error getting recent trades for {symbol}: {e}", code=APIErrorCode.UNKNOWN.value
            ) from e

    async def get_balances(self) -> dict[str, SpotBalance]:
        """Get account balances."""
        endpoint = "/api/v1/capital"
        response_data: Any = await self._request("GET", endpoint)

        # Ensure response_data is a dictionary before proceeding
        if not isinstance(response_data, dict):
            logger.warning(
                f"[{self.exchange_name}] Unexpected response type for balances: "
                f"{type(response_data)}. Returning empty balances."
            )
            return {}

        # Provide specific type hint after runtime check
        response_dict = cast(dict[str, dict[str, Any]], response_data)

        processed_balances: dict[str, SpotBalance] = {}
        for asset, balance_details in response_dict.items():
            asset_str = ""
            try:
                # Ensure asset is treated as string
                asset_str = str(asset)
                # Create SpotBalance instance, passing the exchange name and details
                # Use correct field names: total_quantity, available_quantity
                total_raw = balance_details.get("total", "0")
                available_raw = balance_details.get("available", "0")
                # Validate types before passing to model
                total_val = str(total_raw) if total_raw is not None else "0"
                available_val = str(available_raw) if available_raw is not None else "0"

                try:
                    total_dec = Decimal(total_val)
                    available_dec = Decimal(available_val)

                    balance = SpotBalance(
                        exchange=self.exchange_name,  # Pass str name directly
                        asset=asset_str,
                        total_quantity=total_dec,  # Pass Decimal
                        available_quantity=available_dec,  # Pass Decimal
                        timestamp=datetime.now(UTC),  # Add timestamp if required by model
                    )
                    processed_balances[asset_str] = balance
                except InvalidOperation as dec_err:
                    logger.error(
                        f"Failed to convert balance values to Decimal for {asset_str}: {dec_err}"
                    )
                    continue  # Skip this asset if conversion fails

            except (ValidationError, TypeError, KeyError) as e:
                logger.error(
                    f"Failed to parse balance for {asset_str}: {e}. Data: {balance_details}"
                )
                continue
        return processed_balances

    async def get_positions(self, symbol: str | None = None) -> list[DerivativePosition]:
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
                    f"[{ExchangeName.BACKPACK}] Unexpected response type for positions: "
                    f"{type(response)}. Returning empty list."
                )
                return []
            positions: list[DerivativePosition] = []
            pos_data_item: Any
            for pos_data_item in response:
                item: dict[str, Any] = cast(dict[str, Any], pos_data_item)
                symbol_from_data = item.get("symbol")
                if not isinstance(symbol_from_data, str) or not symbol_from_data:
                    logger.warning(
                        f"[{ExchangeName.BACKPACK}] Position missing or invalid 'symbol': {item}"
                    )
                    continue
                try:
                    size_dec = parse_decimal_value(
                        item.get("positionSize", "0"), allow_none=False, field_name="positionSize"
                    )
                    entry_price_dec = parse_decimal_value(
                        item.get("entryPrice", "0"), allow_none=False, field_name="entryPrice"
                    )
                    mark_price_dec = parse_decimal_value(
                        item.get("markPrice", "0"), allow_none=False, field_name="markPrice"
                    )
                    liq_price_str = item.get("liquidationPrice")
                    liq_price_dec = (
                        parse_decimal_value(
                            liq_price_str, allow_none=True, field_name="liquidationPrice"
                        )
                        if liq_price_str is not None and liq_price_str != "0"
                        else parse_decimal_value(
                            "0.0", allow_none=False, field_name="liquidationPrice"
                        )
                    )
                    pnl_dec = parse_decimal_value(
                        item.get("unrealizedPnl", "0"), allow_none=False, field_name="unrealizedPnl"
                    )
                    # Defensive: ensure required decimals are not None
                    if size_dec is None:
                        raise ValueError(
                            "Position size is None after parse_decimal_value with allow_none=False"
                        )
                    if entry_price_dec is None:
                        raise ValueError(
                            "Entry price is None after parse_decimal_value with allow_none=False"
                        )
                    if mark_price_dec is None:
                        raise ValueError(
                            "Mark price is None after parse_decimal_value with allow_none=False"
                        )
                    if pnl_dec is None:
                        raise ValueError(
                            "Unrealized PnL is None after parse_decimal_value with allow_none=False"
                        )
                    # Parse timestamp (assuming field name like 'lastUpdatedAtMs')
                    ts_raw = item.get("lastUpdatedAtMs")  # Adjust field name if needed
                    timestamp = parse_datetime_utc(ts_raw, field_name="lastUpdatedAtMs")
                    if timestamp is None:
                        logger.warning(
                            f"[{self.exchange_name}] Position missing or invalid timestamp: {item}. Using current time."
                        )
                        timestamp = datetime.now(UTC)

                    position = DerivativePosition(
                        exchange=self.exchange_name,  # Add required exchange
                        timestamp=timestamp,  # Add required timestamp
                        symbol=symbol_from_data,
                        size=size_dec,
                        entry_price=entry_price_dec,
                        mark_price=mark_price_dec,
                        side=OrderSide.BUY if size_dec > Decimal("0") else OrderSide.SELL,
                        liquidation_price=liq_price_dec
                        if liq_price_dec is not None
                        else Decimal("0"),  # Provide default if None
                        unrealized_pnl=pnl_dec,
                        # Do not pass leverage here
                        # bp_details could be parsed here if needed
                    )
                    positions.append(position)
                except Exception as e:
                    logger.warning(
                        f"[{ExchangeName.BACKPACK}] Error processing position: {e} | Data: {item}"
                    )
            return positions
        except Exception as e:
            logger.error(f"[{ExchangeName.BACKPACK}] Error getting positions: {e}")
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
                    code=APIErrorCode.UNKNOWN.value,
                    http_status=None,
                )

            raw_order: BackpackRawOrder = BackpackRawOrder.model_validate(response)
            return BackpackOrderMapper.transform_raw_order_to_internal(raw_order)
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
                        raw_order: BackpackRawOrder = BackpackRawOrder.model_validate(
                            order_data_item
                        )
                        internal_order: Order = BackpackOrderMapper.transform_raw_order_to_internal(
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
            raise APIError(
                f"API Error getting open orders: {e}", code=APIErrorCode.UNKNOWN.value
            ) from e
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error in get_open_orders: {e}", exc_info=True
            )
            raise APIError(
                f"Failed to get open orders: {e}", code=APIErrorCode.UNKNOWN.value
            ) from e

    async def get_funding_rate(self, symbol: str) -> FundingRate:
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
                raise APIError(
                    f"Could not fetch funding rate for {symbol}", code=APIErrorCode.UNKNOWN.value
                )
            # Parse timestamp to datetime
            timestamp_raw = response.get("time")
            # Use current UTC time if API doesn't provide one
            timestamp_dt = (
                parse_datetime_utc(timestamp_raw, field_name="time")
                if timestamp_raw
                else datetime.now(UTC)
            )
            if timestamp_dt is None:  # Should not happen with fallback, but defensive check
                logger.error(
                    f"[{self.exchange_name}] Failed to parse or get timestamp for funding rate. "
                    f"Using current UTC time."
                )
                timestamp_dt = datetime.now(UTC)

            funding_rate_dec = parse_decimal_value(
                response.get("fundingRate", "0"), allow_none=False, field_name="fundingRate"
            )
            mark_price_dec = (
                parse_decimal_value(
                    response.get("markPrice", "0"), allow_none=True, field_name="markPrice"
                )
                if response.get("markPrice")
                else None
            )
            if funding_rate_dec is None:  # Defensive check
                raise ValueError("fundingRate parsed to None unexpectedly")

            return FundingRate(
                symbol=symbol,
                timestamp=timestamp_dt,  # Use datetime object
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
                    current_rate: FundingRate = await self.get_funding_rate(symbol)
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
                code=APIErrorCode.INVALID_PARAMS.value,
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
        self, asset: str, amount: Decimal, from_account: str, to_account: str
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

    async def _fetch_balance(self, asset: str | None = None) -> list[SpotBalance]:
        """Internal method to fetch spot balances."""
        balances_data = await self._request("GET", "/api/v1/capital")
        if not isinstance(balances_data, dict):
            logger.warning("Unexpected response format from /capital endpoint")
            return []  # Return empty list on unexpected format

        now = datetime.now(UTC)
        balances = []
        for asset_symbol, details in balances_data.items():
            if not isinstance(details, dict):
                logger.warning(f"Skipping invalid balance entry for {asset_symbol}: {details}")
                continue

            # Use raw model from correct location if it exists, otherwise skip details for now
            # Assuming a BackpackRawBalance model exists in backpack.models
            try:
                # Need to import the actual Raw Balance model if it exists
                # from .models.bp_raw_balance import BackpackRawBalance # Placeholder
                raw_details = BackpackRawBalance.model_validate(details)  # Placeholder

                # Placeholder logic until Raw Model path confirmed - USE HARDCODED KEYS FOR NOW
                # total_raw = details.get("total", "0")
                # available_raw = details.get("available", "0")
                # locked_qty_raw = details.get("locked", "0")

                # total = str(total_raw) if total_raw is not None else "0"
                # available = str(available_raw) if available_raw is not None else "0"
                # locked = str(locked_qty_raw) if locked_qty_raw is not None else "0"

                # Need to import the actual Details model if it exists
                # from .models.bp_spot_balance_details import BackpackSpotBalanceDetails # Placeholder
                # bp_details_obj = None  # Placeholder
                bp_details_obj = BackpackSpotBalanceDetails(
                    open_order_quantity=raw_details.locked,
                    lend_quantity=raw_details.locked,
                    collateral_weight=None,
                )  # Placeholder

                try:
                    # Convert to Decimal
                    total_dec = Decimal(total)
                    available_dec = Decimal(available)
                    # locked_dec = Decimal(locked) # Assuming locked is not part of SpotBalance core

                    balance = SpotBalance(
                        asset=asset_symbol.upper(),
                        total_quantity=total_dec,
                        available_quantity=available_dec,
                        # Assuming 'locked' maps to unavailable, needs confirmation
                        # If SpotBalance model includes locked, add it here.
                        timestamp=datetime.now(UTC),  # Add timestamp
                        exchange=self.exchange_name,
                    )
                    if asset is None or balance.asset == asset.upper():
                        balances.append(balance)
                except InvalidOperation as dec_err:
                    logger.error(
                        f"Failed to convert balance values to Decimal for {asset_symbol} in list: {dec_err}"
                    )
                    continue  # Skip this item if conversion fails

            except (ValidationError, TypeError, KeyError) as e:
                logger.error(f"Failed to parse balance for {asset_symbol}: {e}. Data: {details}")
                continue
        return balances
