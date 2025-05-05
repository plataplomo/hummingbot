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
from decimal import Decimal
from typing import Any

from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_error_mapper import BackpackErrorMapper
from cyberdelta.apis.backpack.bp_order_mapper import BackpackOrderMapper
from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalance
from cyberdelta.apis.backpack.models.bp_raw_funding import BackpackRawFundingRate
from cyberdelta.apis.backpack.models.bp_raw_market import BackpackRawOrderBook, BackpackRawTicker
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPosition
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawTrade
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
from cyberdelta.utils.logging_config import get_logger

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
        response_raw: object = None  # Type as object
        try:
            response_raw = await self._request("GET", request_path)
            # Validate with Raw model first
            raw_ticker = BackpackRawTicker.model_validate(response_raw)
            # Transform to internal (if needed, Ticker might be simple enough)
            # For now, assuming direct validation is sufficient if Ticker = RawTicker structure
            # If transformation logic exists/is needed, use a Mapper.
            # Let's assume Ticker can validate the raw_ticker dictionary directly for now
            # This needs verification against the Ticker model definition.
            # A safer pattern would be:
            # internal_ticker = BackpackMarketMapper.transform_raw_ticker(raw_ticker)
            return Ticker.model_validate(raw_ticker.model_dump())
        except ValidationError as e:
            logger.error(
                f"[{self.exchange_name}] Ticker validation failed for {symbol}: {e}. "
                f"Data: {response_raw}"
            )
            raise APIError(
                f"Invalid ticker response for {symbol}: {e}",
                code=APIErrorCode.UNKNOWN.value,  # Use UNKNOWN for validation errors
            ) from e
        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error getting ticker for {symbol}: {e}")
            raise e  # Re-raise mapped APIError
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Error getting ticker for {symbol}: {e}", exc_info=True
            )
            api_error = BackpackErrorMapper.map_error_response(
                status_code=getattr(e, "status", None),
                error_body=f"Error getting ticker for {symbol}: {e}",
                request_path=request_path,
            )
            raise api_error from e

    async def get_order_book(self, symbol: str, depth: int = 20) -> OrderBook:
        """Fetch the order book for a symbol, validated via Raw model and transformed via Mapper.

        Args:
            symbol: The trading symbol (e.g., 'BTC_USDC').
            depth: Ignored for Backpack (uses default depth). Included for interface consistency.

        Returns:
            OrderBook object.

        Raises:
            APIError: If the order book cannot be fetched or validated.
        """
        endpoint = "/api/v1/depth"
        params = {"symbol": symbol}
        response_raw: Any = None  # Initialize for error handling
        try:
            response_raw = await self._request("GET", endpoint, params=params)
            # Validate raw order book data using Pydantic model
            validated_book = BackpackRawOrderBook.model_validate(response_raw)
            # Transform validated raw data to internal model
            internal_book = BackpackOrderMapper.transform_raw_orderbook_to_internal(
                symbol=symbol, raw=validated_book
            )
            # TODO: Apply depth limit if needed (Backpack provides full depth)
            # For now, return the full book as mapped
            return internal_book
        except ValidationError as e:
            logger.error(
                f"[{self.exchange_name}] Order book validation failed for {symbol}: {e}. "
                f"Data: {response_raw}"
            )
            raise APIError(
                f"Invalid order book response for {symbol}: {e}",
                code=APIErrorCode.UNKNOWN.value,
            ) from e
        except ValueError as e:  # Catches errors from transform method
            logger.error(
                f"[{self.exchange_name}] Order book transformation failed for {symbol}: {e}. "
                f"Raw Data: {response_raw}"
            )
            raise APIError(
                f"Order book transformation failed for {symbol}: {e}",
                code=APIErrorCode.UNKNOWN.value,
            ) from e
        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error getting order book for {symbol}: {e}")
            raise e
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error in get_order_book: {e}", exc_info=True
            )
            # Re-raise as a generic APIError
            raise APIError(
                f"Unexpected error fetching order book for {symbol}: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
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
            response_raw: object = await self._request("GET", request_path, params=params)

            # DEFENSIVE CHECK: Runtime check before processing
            if not isinstance(response_raw, list):
                logger.warning(
                    f"[{self.exchange_name}] Unexpected trades response type: "
                    f"{type(response_raw)}. Expected list. Returning empty list."
                )
                return []

            # No cast needed, iterate directly over the list known from isinstance
            response_list = response_raw

            trades: list[Trade] = []
            for trade_data_raw in response_list:
                # Ensure item is dict before validation
                if not isinstance(trade_data_raw, dict):
                    logger.warning(f"Skipping non-dict item in trades list: {trade_data_raw}")
                    continue
                try:
                    # Validate raw trade data using Pydantic model
                    raw_trade = BackpackRawTrade.model_validate(trade_data_raw)

                    # Transform validated raw trade to internal Trade using mapper
                    internal_trade = BackpackOrderMapper.transform_raw_trade_to_internal(
                        raw=raw_trade
                    )

                    # Append only if transformation succeeded (returned Trade, not None)
                    if internal_trade:
                        trades.append(internal_trade)

                except ValidationError as e:
                    logger.warning(
                        f"[{self.exchange_name}] Skipping trade due to validation error: {e}. "
                        f"Data: {trade_data_raw}"
                    )
                    continue
                except ValueError as e:
                    logger.warning(
                        f"[{self.exchange_name}] Skipping trade due to transformation error: {e}. "
                        f"Raw Data: {trade_data_raw}"
                    )
                    continue
                except Exception as e:
                    logger.error(
                        f"[{self.exchange_name}] Unexpected error processing trade: {e}. "
                        f"Data: {trade_data_raw}",
                        exc_info=True,
                    )
                    continue
            return trades
        except APIError as e:
            logger.error(
                f"[{self.exchange_name}] API Error getting recent trades for {symbol}: {e}"
            )
            raise e
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error in get_recent_trades: {e}", exc_info=True
            )
            api_error = BackpackErrorMapper.map_error_response(
                status_code=getattr(e, "status", None),
                error_body=str(e),
                request_path=request_path,
                exchange_message=f"Error getting recent trades for {symbol}: {e}",
            )
            raise api_error from e

    async def get_balances(self) -> dict[str, SpotBalance]:
        """Get account balances, validated via Raw models and transformed via Mapper."""
        endpoint = "/api/v1/capital"
        response_data_raw: object = await self._request("GET", endpoint)

        # DEFENSIVE CHECK: Runtime check before processing
        if not isinstance(response_data_raw, dict):
            logger.warning(
                f"[{self.exchange_name}] Unexpected response type for balances "
                f"({type(response_data_raw)}). Expected dict. Returning empty balances."
            )
            return {}

        # No cast needed, iterate directly
        response_dict = response_data_raw

        processed_balances: dict[str, SpotBalance] = {}
        # Ensure keys are strings and values are dicts before processing
        for asset_symbol_raw, balance_details_raw in response_dict.items():
            if not isinstance(balance_details_raw, dict):
                logger.warning(
                    f"Skipping balance entry with non-dict value for key "
                    f"{asset_symbol_raw}: {balance_details_raw}"
                )
                continue
            asset_symbol = str(asset_symbol_raw)
            try:
                # Validate raw balance details using Pydantic model
                raw_balance = BackpackRawBalance.model_validate(balance_details_raw)

                # Transform validated raw balance to internal SpotBalance using mapper
                internal_balance = BackpackOrderMapper.transform_raw_balance_to_internal(
                    asset_symbol=asset_symbol, raw=raw_balance
                )
                processed_balances[internal_balance.asset] = internal_balance

            except ValidationError as e:
                logger.error(
                    f"[{self.exchange_name}] Failed Pydantic validation for balance "
                    f"{asset_symbol}: {e}. Data: {balance_details_raw}"
                )
                continue  # Skip this asset if validation fails
            except ValueError as e:  # Catches errors from transform_raw_balance_to_internal
                logger.error(
                    f"[{self.exchange_name}] Failed transformation for balance "
                    f"{asset_symbol}: {e}. Raw Data: {balance_details_raw}"
                )
                continue  # Skip this asset if transformation fails
            except Exception as e:
                logger.error(
                    f"[{self.exchange_name}] Unexpected error processing balance for "
                    f"{asset_symbol}: {e}",
                    exc_info=True,
                )
                continue
        return processed_balances

    async def get_positions(self, symbol: str | None = None) -> list[DerivativePosition]:
        """
        Get current positions, validated via Raw models and transformed via Mapper.
        Currently fetches all positions, ignoring the optional symbol filter.

        Args:
            symbol: Trading symbol (optional, currently ignored).

        Returns:
            List of Position objects.
        """
        request_path = "/api/v1/positions"
        if symbol:
            logger.warning(
                f"[{self.exchange_name}] get_positions called with symbol '{symbol}', "
                "but Backpack API currently fetches all positions."
            )
        try:
            response_raw: object = await self._request("GET", request_path, signed=True)
            # DEFENSIVE CHECK: Ensure response is list
            if not isinstance(response_raw, list):
                logger.warning(
                    f"[{self.exchange_name}] Unexpected response type for positions: "
                    f"{type(response_raw)}. Returning empty list."
                )
                return []

            # No cast needed
            response_list = response_raw

            positions: list[DerivativePosition] = []
            for pos_data_raw in response_list:
                # Ensure item is dict before validation
                if not isinstance(pos_data_raw, dict):
                    logger.warning(f"Skipping non-dict item in positions list: {pos_data_raw}")
                    continue
                try:
                    # Validate raw position details using Pydantic model
                    raw_position = BackpackRawPosition.model_validate(pos_data_raw)

                    # Transform validated raw position to internal DerivativePosition
                    internal_position = BackpackOrderMapper.transform_raw_position_to_internal(
                        raw=raw_position
                    )
                    positions.append(internal_position)

                except ValidationError as e:
                    logger.warning(
                        f"[{self.exchange_name}] Skipping position due to validation error: "
                        f"{e}. Data: {pos_data_raw}"
                    )
                    continue
                except ValueError as e:  # Catches errors from transform_raw_position_to_internal
                    logger.warning(
                        f"[{self.exchange_name}] Skipping position due to transformation "
                        f"error: {e}. Raw Data: {pos_data_raw}"
                    )
                    continue
                except Exception as e:
                    logger.error(
                        f"[{self.exchange_name}] Unexpected error processing position: "
                        f"{e}. Data: {pos_data_raw}",
                        exc_info=True,
                    )
                    continue
            return positions
        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error getting positions: {e}")
            raise e
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error in get_positions: {e}", exc_info=True
            )
            api_error = BackpackErrorMapper.map_error_response(
                status_code=getattr(e, "status", None),
                error_body=str(e),
                request_path=request_path,
                exchange_message=f"Unexpected error fetching positions: {e}",
            )
            raise api_error from e

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

    async def cancel_order(
        self, order_id: str, symbol: str | None = None
    ) -> dict[str, bool | str | None]:  # More specific return type
        """Cancel an existing order. Conforms to ExchangeAPI interface."""
        if symbol is None:
            # Attempt to lookup symbol from order_id if possible, or raise error
            # For now, raise error as Backpack API likely requires symbol
            raise ValueError("Symbol is required to cancel order on Backpack")

        request_path = "/api/v1/order"
        response: object = None  # Initialize
        try:
            params = {"symbol": symbol, "orderId": order_id}
            response = await self._request("DELETE", request_path, params=params, signed=True)
            logger.info(f"[{self.exchange_name}] Canceled order {order_id} for {symbol}")
            # Return more specific success dict
            return {
                "success": True,
                "orderId": order_id,
                "symbol": symbol,
                "response_body": str(response) if response else None,
            }
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Error canceling order {order_id}: {e}", exc_info=True
            )
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
        """Fetch funding rate for a symbol, validated via Raw models and transformed via Mapper.

        Args:
            symbol: The trading symbol (e.g., 'BTC_USDC').

        Returns:
            FundingRate object.

        Raises:
            APIError: If the funding rate cannot be fetched or validated.
        """
        request_path = f"/api/v1/funding/{symbol}"
        response_raw: Any = None  # Initialize to prevent unbound error
        try:
            response_raw = await self._request("GET", request_path)

            # Validate raw funding rate data using Pydantic model
            raw_funding_rate = BackpackRawFundingRate.model_validate(response_raw)

            # Transform validated raw data to internal FundingRate using mapper
            internal_funding_rate = BackpackOrderMapper.transform_raw_funding_rate_to_internal(
                raw=raw_funding_rate
            )

            return internal_funding_rate

        except ValidationError as e:
            logger.error(
                f"[{self.exchange_name}] Funding rate validation failed for {symbol}: {e}. "
                f"Data: {response_raw}"
            )
            raise APIError(
                f"Invalid funding rate response for {symbol}: {e}",
                code=APIErrorCode.UNKNOWN.value,
            ) from e
        except ValueError as e:  # Catches errors from transform method
            logger.error(
                f"[{self.exchange_name}] Funding rate transformation failed for {symbol}: {e}. "
                f"Raw Data: {response_raw}"
            )
            raise APIError(
                f"Funding rate transformation failed for {symbol}: {e}",
                code=APIErrorCode.UNKNOWN.value,
            ) from e
        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error getting funding rate for {symbol}: {e}")
            # Reraise the mapped error from _request
            raise e
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error getting funding rate for {symbol}: {e}",
                exc_info=True,
            )
            raise BackpackErrorMapper.map_error_response(
                status_code=getattr(e, "status", None),
                error_body=str(e),
                request_path=request_path,
                exchange_message=f"Error getting funding rate for {symbol}: {e}",
            ) from e

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
    async def get_account_info(
        self,
    ) -> dict[str, object]:  # Return dict[str, object] instead of Any
        """Get general account information (Example)."""
        # This method is just an example; Backpack might not have this exact endpoint.
        request_path = "/api/v1/account"
        try:
            response: object = await self._request("GET", request_path, signed=True)
            # TODO: Define BackpackRawAccountInfo if structure is known and stable.
            # For now, perform basic type check and return as dict[str, object]
            if not isinstance(response, dict):
                logger.warning(
                    f"[{self.exchange_name}] Unexpected response type for account info: "
                    f"{type(response)}. Returning empty dict."
                )
                return {}
            return response  # Known to be dict now
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Error getting account info: {e}")
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
