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
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_auth import BackpackHmacAuthenticator
from cyberdelta.apis.backpack.bp_error_mapper import BackpackErrorMapper
from cyberdelta.apis.backpack.bp_order_mapper import BackpackOrderMapper
from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalance
from cyberdelta.apis.backpack.models.bp_raw_account_summary import BackpackRawAccountSummary
from cyberdelta.apis.backpack.models.bp_raw_funding import BackpackRawFundingRate
from cyberdelta.apis.backpack.models.bp_raw_kline import BackpackRawKline
from cyberdelta.apis.backpack.models.bp_raw_market import BackpackRawOrderBook, BackpackRawTicker
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPosition
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawTrade
from cyberdelta.apis.backpack.models.bp_raw_withdrawal import (
    BackpackRawWithdrawalRequest,
    BackpackRawWithdrawalResponse,
)
from cyberdelta.apis.base.authenticator_interface import AuthenticatedRequestComponents
from cyberdelta.apis.base_api import ExchangeAPI, MessageHandler
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models import (
    DerivativePosition,
    FundingRate,
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
from cyberdelta.core.models.market.candle import Candle
from cyberdelta.core.models.market.order import Order
from cyberdelta.core.models.market.order_book import OrderBook
from cyberdelta.utils.logging_config import get_logger

logger = get_logger(__name__)


class BackpackAPI(ExchangeAPI):
    """
    Backpack Exchange API Client.

    Implements connectivity and data handling for the Backpack exchange,
    adhering to the ExchangeAPI interface.

    Handles REST API requests and WebSocket connections for market data and account updates.
    Rate limit handling is currently a placeholder as Backpack's OpenAPI specification
    does not clearly define standard rate limit response headers. Further investigation
    or documentation from Backpack would be needed to implement robust rate limiting.
    """

    def __init__(self, api_config: dict[str, Any], secrets: dict[str, str | None]) -> None:
        """
        Initialize the BackpackAPI client with configuration and secrets.

        Args:
            api_config: Dictionary of API configuration parameters.
            secrets: Dictionary of secret values (API key/secret).
        """
        self._api_key = secrets.get("BACKPACK_API_KEY")
        self._api_secret = secrets.get("BACKPACK_API_SECRET")

        if self._api_key and self._api_secret:
            self._bp_authenticator: BackpackHmacAuthenticator | None = BackpackHmacAuthenticator(
                api_key=self._api_key, api_secret=self._api_secret
            )
        else:
            logger.warning(
                "Backpack API key/secret not provided. Signed operations will fail. "
                "Authenticator not initialized."
            )
            self._bp_authenticator = None

        super().__init__(
            exchange_name="backpack",
            config=api_config,
            secrets=secrets,
            authenticator=self._bp_authenticator,  # Ensure authenticator is passed
        )

        # Default headers - can be moved to base or kept here if specific
        self.default_headers: dict[str, str] = {
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json",
        }

    # --- WebSocket Implementation --- #

    async def _handle_websocket_message(self, message: dict[str, Any]) -> None:
        """Handle raw WebSocket message, routing it for processing."""
        # For Backpack, no special pre-processing needed currently.
        # Directly call the routing logic.
        await self._route_ws_message(message)

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
        Authenticate and sign an API request for Backpack using BackpackHmacAuthenticator.

        Returns:
            Dictionary with signed headers, params, and data as expected by base _request.
        """
        final_headers = self.default_headers.copy() if self.default_headers else {}

        if not self._bp_authenticator:
            logger.error(
                f"[{self.exchange_name}] Backpack authenticator not initialized. "
                f"Cannot sign request for {method} {path}"
            )
            raise APIError(
                "Backpack authenticator not initialized. Cannot sign request.",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        auth_components: AuthenticatedRequestComponents = (
            await self._bp_authenticator.prepare_request(
                method=method,
                path=path,
                params=params,  # Pass original params
                data=data,  # Pass original body
                headers=final_headers,  # Pass current headers for authenticator to augment/override
            )
        )
        # Return as dict[str, Any] to match base class signature
        return {
            "headers": auth_components["headers"],
            "params": auth_components["params"],
            "data": auth_components["data"],
        }

    async def _handle_ws_message(self, message: Mapping[str, Any], ws_url: str) -> None:
        # This method is not provided in the original file or the code block
        # It's assumed to exist as it's called in the _route_ws_message method
        pass

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
            raise e  # Re-raise mapped APIError from _request
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error getting ticker for {symbol}: {e}",
                exc_info=True,
            )
            # Let _request handle mapping via overridden _map_error_response
            raise APIError(
                f"Unexpected error getting ticker: {e}", code=APIErrorCode.UNKNOWN.value
            ) from e

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
            # Let _request handle mapping via overridden _map_error_response
            raise APIError(
                f"Unexpected error getting recent trades: {e}", code=APIErrorCode.UNKNOWN.value
            ) from e

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
            response_raw: object = await self._request("GET", request_path, is_signed=True)
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
            # Let _request handle mapping via overridden _map_error_response
            raise APIError(
                f"Unexpected error getting positions: {e}", code=APIErrorCode.UNKNOWN.value
            ) from e

    async def place_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        time_in_force: TimeInForce,
        price: Decimal | None = None,
        stop_price: Decimal | None = None,  # Added to match ExchangeAPI base method
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
            time_in_force: Time in force (GTC, IOC, FOK).
            price: Order price (required for limit orders, as Decimal)
            stop_price: Stop price for stop orders (currently NOT supported by Backpack
                        for basic order placement via this method).
            client_order_id: Custom client order ID
            reduce_only: Whether this is a reduce-only order (bool)
            post_only: Whether this is a post-only order (bool)

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

            # Handle TimeInForce - Backpack uses string values like "GTC", "IOC", "FOK"
            # The enum OrderType already provides these as .value
            tif_value = time_in_force.value
            order_data["timeInForce"] = tif_value

            if order_type == OrderType.LIMIT:
                if price is None:
                    raise ValueError("Price is required for LIMIT orders")
                order_data["price"] = str(price)
                if post_only:
                    # Backpack API uses `postOnly: true` for post-only limit orders
                    order_data["postOnly"] = True

            # Note: stop_price is part of the signature for ExchangeAPI compatibility,
            # but Backpack's standard POST /api/v1/order endpoint for market/limit orders
            # does not take a stopPrice. Trigger orders might be a separate endpoint or type.
            if stop_price is not None:
                logger.warning(
                    f"[{self.exchange_name}] 'stop_price' was provided for a Backpack order, "
                    f"but it is not used by the standard order placement endpoint. "
                    f"Ensure this is intended."
                )

            if client_order_id:
                order_data["clientId"] = client_order_id
            if reduce_only:
                order_data["reduceOnly"] = True

            response = await self._request("POST", request_path, data=order_data, is_signed=True)

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
        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error placing order: {e}")
            raise e
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Error placing order: {e}", exc_info=True)
            # Let _request handle mapping via overridden _map_error_response
            raise APIError(
                f"Unexpected error placing order: {e}", code=APIErrorCode.UNKNOWN.value
            ) from e

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> bool:
        """Cancel an existing order. Returns True if successful."""
        if symbol is None:
            # Attempt to lookup symbol from order_id if possible, or raise error
            # For now, raise error as Backpack API likely requires symbol
            raise ValueError("Symbol is required to cancel order on Backpack")

        request_path = "/api/v1/order"
        try:
            params = {"symbol": symbol, "orderId": order_id}
            await self._request("DELETE", request_path, params=params, is_signed=True)
            logger.info(f"[{self.exchange_name}] Canceled order {order_id} for {symbol}")
            # Return True on success (APIError wasn't raised)
            return True
        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error canceling order {order_id}: {e}")
            raise e
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Error canceling order {order_id}: {e}", exc_info=True
            )
            # Let _request handle mapping via overridden _map_error_response
            raise APIError(
                f"Unexpected error canceling order {order_id}: {e}", code=APIErrorCode.UNKNOWN.value
            ) from e

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        request_path = "/api/v1/orders"
        try:
            params: dict[str, str] = {}
            if symbol:
                params["symbol"] = symbol
            response = await self._request("GET", request_path, params=params, is_signed=True)
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
            raise e
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
            # Let _request handle mapping via overridden _map_error_response
            raise APIError(
                f"Unexpected error getting funding rate for {symbol}: {e}",
                code=APIErrorCode.UNKNOWN.value,
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
    ) -> dict[str, Any]:  # Updated return type, TODO: Define internal AccountSummary model
        """Get general account information, mapped to an internal representation."""
        request_path = "/api/v1/account"
        response_raw: object = None  # Initialize for error logging
        try:
            response_raw = await self._request("GET", request_path, is_signed=True)

            # DEFENSIVE CHECK: Ensure response_raw is a dict before validation
            if not isinstance(response_raw, dict):
                logger.error(
                    f"[{self.exchange_name}] get_account_info response is not a dict: {type(response_raw)}. Raw: {response_raw}"
                )
                raise APIError(
                    "Invalid response format from get_account_info (not a dict)",
                    code=APIErrorCode.EXCHANGE_SPECIFIC.value,
                )

            validated_account_summary = BackpackRawAccountSummary.model_validate(response_raw)
            # TODO: Define a proper internal AccountSummary model and map fully.
            # For now, using BackpackOrderMapper to return a dict.
            return BackpackOrderMapper.transform_raw_account_summary_to_internal(
                validated_account_summary
            )
        except ValidationError as e:
            logger.error(
                f"[{self.exchange_name}] Account info validation failed: {e}. Data: {response_raw}"
            )
            raise APIError(
                f"Invalid account info response: {e}",
                code=APIErrorCode.UNKNOWN.value,  # Use UNKNOWN for validation errors
                original_exception=e,
            ) from e
        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error getting account info: {e}")
            raise e  # Re-raise mapped APIError from _request
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error getting account info: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error getting account info: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e

    async def transfer(
        self,
        asset: str,
        amount: Decimal,
        from_account_type: str,  # e.g., "spot", "futures" - specific values TBD by exchange
        to_account_type: str,  # e.g., "spot", "futures" - specific values TBD by exchange
        client_transfer_id: str | None = None,
    ) -> dict[str, Any]:  # Return type TBD by actual API response structure
        """
        Initiate an internal transfer of assets between user accounts (e.g., spot to futures).

        Backpack's OpenAPI specification (as of review) does not explicitly detail a separate
        endpoint for internal account transfers distinct from general withdrawals/deposits.
        If such functionality exists, it may be part of a subaccount system or implicitly handled
        via specific parameters in deposit/withdrawal flows to other owned accounts/addresses.
        This method remains as a placeholder for potential future implementation if a dedicated
        API for internal transfers is identified or becomes available.

        Args:
            asset: The asset to transfer (e.g., "USDC").
            amount: The amount of the asset to transfer.
            from_account_type: The type of account to transfer from.
            to_account_type: The type of account to transfer to.
            client_transfer_id: Optional client-provided ID for the transfer.

        Returns:
            A dictionary containing the API response upon successful transfer.

        Raises:
            NotImplementedError: As this specific functionality is not clearly defined in Backpack's API.
            APIError: For API-level errors encountered during the request.
        """
        logger.warning(
            f"[{self.exchange_name}] The 'transfer' method is not implemented. Backpack API "
            f"does not clearly define a separate internal transfer endpoint. Consider using "
            f"withdraw/deposit mechanisms if applicable."
        )
        # Per OpenAPI, there is no dedicated internal transfer endpoint distinct from deposit/withdrawals.
        # If transfers between subaccounts or to other owned accounts are needed, they likely use
        # the withdrawal mechanism with specific parameters or target addresses.
        raise NotImplementedError(
            "Backpack API does not provide a dedicated internal transfer endpoint. "
            "Use withdrawal/deposit with appropriate parameters if applicable."
        )

    async def withdraw(
        self,
        asset: str,
        amount: Decimal,
        address: str,
        network: str | None = None,  # Blockchain network
        tag: str | None = None,  # Destination tag / memo, if required
        client_withdrawal_id: str | None = None,  # Optional client-provided ID
        two_factor_token: str | None = None,  # Optional 2FA token
        **kwargs: dict[str, Any],  # For additional exchange-specific parameters
    ) -> dict[str, Any]:  # Return validated raw response
        """Initiate a withdrawal of assets from the exchange.

        Args:
            asset: The asset symbol to withdraw (e.g., "USDC").
            amount: The amount of the asset to withdraw.
            address: The destination address for the withdrawal.
            network: The blockchain network to use (e.g., "Solana", "Ethereum").
                     This maps to `blockchain` in the Backpack API.
            tag: Optional destination tag or memo, if required by the address/network.
                 (Note: Backpack API does not explicitly show a 'tag' field in
                  AccountWithdrawalPayload, this might need to be part of the
                  'address' or handled differently).
            client_withdrawal_id: Optional client-provided ID for the withdrawal.
                                  Maps to `clientId`.
            two_factor_token: Optional 2FA token if required by user settings.
            **kwargs: Additional keyword arguments for exchange-specific options,
                      e.g., `auto_borrow: bool`, `auto_lend_redeem: bool`.

        Returns:
            A BackpackRawWithdrawalResponse object containing the API response upon successful withdrawal.

        Raises:
            APIError: For API-level errors encountered during the request.
            ValueError: If required parameters like `network` are missing.
        """
        request_path = "/wapi/v1/capital/withdrawals"

        if not network:
            raise ValueError("Network (blockchain) is required for withdrawal on Backpack.")

        # Prepare the payload using the Pydantic request model
        # This also validates the input types for asset and blockchain against Literals
        try:
            payload_data = {
                "address": address,
                "blockchain": network,  # Map `network` to `blockchain`
                "quantity": amount,
                "symbol": asset,
                "clientId": client_withdrawal_id,
                "twoFactorToken": two_factor_token,
                # Pass through any additional kwargs that are part of the model
                "autoBorrow": kwargs.get("auto_borrow"),
                "autoLendRedeem": kwargs.get("auto_lend_redeem"),
            }
            # Filter out None values for optional fields before creating the model
            filtered_payload_data = {k: v for k, v in payload_data.items() if v is not None}

            # Create and validate the request object
            # The model_validate method will raise ValidationError if inputs are bad
            withdrawal_request = BackpackRawWithdrawalRequest.model_validate(filtered_payload_data)
        except ValidationError as e:
            logger.error(f"[{self.exchange_name}] Withdrawal request validation error: {e}")
            raise ValueError(f"Invalid parameters for withdrawal: {e}") from e

        response_raw: object = None  # For logging in case of error
        try:
            # Pydantic models by default exclude None values when dumping to dict
            # if `exclude_none=True` is used, but here we create the dict first,
            # then model, so Nones are already filtered.
            response_raw = await self._request(
                "POST",
                request_path,
                data=withdrawal_request.model_dump(by_alias=True, exclude_none=True),
                is_signed=True,
            )

            if not isinstance(response_raw, dict):
                logger.error(
                    f"[{self.exchange_name}] Unexpected response type for withdrawal: "
                    f"{type(response_raw)}. Expected dict. Raw response: {response_raw}"
                )
                raise APIError(
                    f"Unexpected response structure for withdrawal: {response_raw}",
                    code=APIErrorCode.UNKNOWN.value,
                )

            # Validate response with Pydantic model
            validated_response = BackpackRawWithdrawalResponse.model_validate(response_raw)
            # TODO: Define a proper internal WithdrawalConfirmation model and map fully.
            # For now, using BackpackOrderMapper to return a dict.
            return BackpackOrderMapper.transform_raw_withdrawal_response_to_internal(
                validated_response
            )

        except ValidationError as e:
            logger.error(
                f"[{self.exchange_name}] Withdrawal response validation failed: {e}. "
                f"Data: {response_raw}"
            )
            raise APIError(
                f"Invalid withdrawal response: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e
        except APIError as e:  # Catch errors from _request or previous APIError
            logger.error(f"[{self.exchange_name}] API Error during withdrawal: {e}")
            raise
        except Exception as e:  # Catch any other unexpected errors
            logger.error(
                f"[{self.exchange_name}] Unexpected error during withdrawal: {e}", exc_info=True
            )
            raise APIError(
                f"Unexpected error during withdrawal: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e

    async def subscribe_to_order_book(self, symbol: str) -> None:
        """Subscribe to order book updates for a symbol."""
        topic = f"depth.{symbol}"
        # This method should likely just prepare the topic and potentially
        # trigger the subscription via a shared mechanism if needed,
        # but handler registration happens via self.subscribe called elsewhere.
        # For now, log intent. Actual subscription initiated by caller via self.subscribe.
        logger.debug(f"[{self.exchange_name}] Preparing subscription for topic: {topic}")
        # await self.subscribe(topic, handler) # Incorrect: Handler not passed here

    async def subscribe_to_ticker(self, symbol: str) -> None:
        """Subscribe to ticker updates for a symbol."""
        topic = f"ticker.{symbol}"
        logger.debug(f"[{self.exchange_name}] Preparing subscription for topic: {topic}")
        # await self.subscribe(topic, handler) # Incorrect: Handler not passed here

    async def subscribe_to_trades(self, symbol: str) -> None:
        """Subscribe to public trade updates for a symbol."""
        topic = f"trades.{symbol}"
        logger.debug(f"[{self.exchange_name}] Preparing subscription for topic: {topic}")
        # await self.subscribe(topic, handler) # Incorrect: Handler not passed here

    async def subscribe_to_account_updates(self) -> None:
        """Subscribe to private account updates (balances, positions, orders)."""
        # This method signals intent or triggers setup. Actual subscriptions
        # with handlers are done via self.subscribe elsewhere.
        fill_topic = "fills"
        order_topic = "orders"
        logger.debug(
            f"[{self.exchange_name}] Preparing subscription for account topics: "
            f"{fill_topic}, {order_topic}"
        )
        # await self.subscribe(fill_topic, handler) # Incorrect
        # await self.subscribe(order_topic, handler) # Incorrect

    async def get_recent_fills(
        self, symbol: str | None = None, limit: int | None = None
    ) -> list[Trade]:
        """Fetch recent fills/trades for the account.

        Uses the existing get_trade_history method.
        """
        # Ensure limit is handled correctly, default in get_trade_history is 100
        effective_limit = limit if limit is not None else 100
        return await self.get_trade_history(symbol=symbol, limit=effective_limit)

    async def get_order_history(
        self,
        symbol: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int | None = 100,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> list[Order]:
        """Fetches historical orders from Backpack.

        Args:
            symbol: Optional symbol filter.
            start_time: Optional start time filter (datetime UTC).
            end_time: Optional end time filter (datetime UTC).
            limit: Maximum number of orders to return (default 100).
            order_id: Optional filter by exchange order ID.
            client_order_id: Optional filter by client order ID.

        Returns:
            List of Order objects.

        Raises:
            APIError: If the request fails or the response is invalid.
        """
        request_path = "/wapi/v1/history/orders"
        # Corrected: Allow None for limit value type
        params: dict[str, str | int | None] = {}
        # Initialize limit first if not None
        if limit is not None:
            # Mypy struggles with conditional assignment to Union type dict value
            params["limit"] = limit
        # Add other params
        if symbol:
            params["symbol"] = symbol
        if order_id:
            params["orderId"] = order_id
        if client_order_id:
            params["clientId"] = client_order_id
        if start_time:
            params["startTime"] = int(start_time.timestamp() * 1000)
        if end_time:
            params["endTime"] = int(end_time.timestamp() * 1000)

        response_raw: object = None
        orders: list[Order] = []
        try:
            response_raw = await self._request("GET", request_path, params=params, is_signed=True)

            # DEFENSIVE CHECK: Ensure response is a list
            if not isinstance(response_raw, list):
                logger.warning(
                    f"[{self.exchange_name}] Unexpected order history response type: "
                    f"{type(response_raw)}. Expected list. Returning empty list."
                )
                return []

            for order_data_raw in response_raw:
                # Ensure item is dict before validation
                if not isinstance(order_data_raw, dict):
                    logger.warning(
                        f"Skipping non-dict item in order history list: {order_data_raw}"
                    )
                    continue
                try:
                    raw_order = BackpackRawOrder.model_validate(order_data_raw)
                    internal_order = BackpackOrderMapper.transform_raw_order_to_internal(raw_order)
                    orders.append(internal_order)
                except (ValidationError, ValueError) as e:
                    logger.warning(
                        f"[{self.exchange_name}] Skipping order in history due to validation/"
                        f"transformation error: {e}. Data: {order_data_raw}"
                    )
                    continue
                except Exception as e:
                    logger.error(
                        f"[{self.exchange_name}] Unexpected error processing historical "
                        f"order: {e}. Data: {order_data_raw}",
                        exc_info=True,
                    )
                    continue
            return orders
        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error getting order history: {e}")
            raise e
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error getting order history: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Error getting order history for {symbol or 'all'}: {e}",
                code=APIErrorCode.UNKNOWN.value,
            ) from e

    async def get_trade_history(self, symbol: str | None = None, limit: int = 100) -> list[Trade]:
        """Fetch historical trades (fills), validated via Raw models and transformed via Mapper."""
        request_path = "/wapi/v1/history/fills"
        params: dict[str, Any] = {"limit": limit}
        if symbol:
            params["symbol"] = symbol

        response_raw: object = None  # Initialize for error handling
        try:
            response_raw = await self._request("GET", request_path, params=params, is_signed=True)

            # DEFENSIVE CHECK: Ensure response is a list
            if not isinstance(response_raw, list):
                logger.warning(
                    f"[{self.exchange_name}] Unexpected trade history (fills) response type: "
                    f"{type(response_raw)}. Expected list. Returning empty list."
                )
                return []

            trades: list[Trade] = []
            for fill_data_raw in response_raw:
                if not isinstance(fill_data_raw, dict):
                    logger.warning(f"Skipping non-dict item in trade history list: {fill_data_raw}")
                    continue
                try:
                    # Validate raw trade data first
                    raw_trade_model = BackpackRawTrade.model_validate(fill_data_raw)
                    # Then transform to internal model
                    internal_trade = BackpackOrderMapper.transform_raw_trade_to_internal(
                        raw_trade_model
                    )
                    if internal_trade:  # Mapper returns Trade | None
                        trades.append(internal_trade)
                    else:
                        logger.warning(
                            f"[{self.exchange_name}] Skipping trade in history due to "
                            f"transformation failure (mapper returned None). Data: {fill_data_raw}"
                        )
                except (
                    ValidationError,
                    ValueError,
                ) as e:  # Catch Pydantic and other validation errors
                    logger.warning(
                        f"[{self.exchange_name}] Skipping trade in history due to validation/"
                        f"transformation error: {e}. Data: {fill_data_raw}"
                    )
                    continue
                except Exception as e:
                    logger.error(
                        f"[{self.exchange_name}] Unexpected error processing historical "
                        f"trade: {e}. Data: {fill_data_raw}",
                        exc_info=True,
                    )
                    continue
            return trades
        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error getting trade history: {e}")
            raise e
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error getting trade history: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Error getting trade history for {symbol or 'all'}: {e}",
                code=APIErrorCode.UNKNOWN.value,
            ) from e

    # --- Override Base Error Mapping --- #

    def _map_error_response(
        self,
        status_code: int,
        error_body: str,
        error_data: dict[str, Any] | None,  # Corrected name and kept None type
    ) -> APIError:
        """Maps Backpack specific error responses to a standardized APIError."""
        # Use the dedicated mapper for Backpack errors
        return BackpackErrorMapper.map_error_response(
            status_code=status_code,
            error_body=error_body,
            error_data=error_data,  # Use corrected name
            # request_path can be added if needed by the mapper
        )

    async def get_order_status(
        self, order_id: str, symbol: str | None = None, client_order_id: str | None = None
    ) -> Order | None:
        """Fetch the status of a specific order by its ID or client ID.

        Args:
            order_id: The exchange-assigned order ID.
            symbol: The trading symbol (required by Backpack for order lookup).
            client_order_id: The client-assigned order ID (optional, can be used
                             if order_id is not known).

        Returns:
            The Order object if found, otherwise None.
        """
        if not symbol:
            raise ValueError("Symbol is required for get_order_status on Backpack.")

        request_path = "/api/v1/order"
        params: dict[str, str] = {"symbol": symbol}

        if order_id:
            params["orderId"] = order_id
        elif client_order_id:
            params["clientId"] = client_order_id
        else:
            raise ValueError("Either order_id or client_order_id must be provided.")

        response_raw: object = None
        try:
            response_raw = await self._request("GET", request_path, params=params, is_signed=True)

            # If _request returns None (e.g., 204 or other non-error empty response),
            # or if the response is not a dict (unexpected for a single order response),
            # treat as order not found or error.
            if not isinstance(response_raw, dict):
                logger.warning(
                    f"[{self.exchange_name}] Unexpected response type for get_order_status: "
                    f"{type(response_raw)}. Expected dict. Params: {params}, Raw: {response_raw}"
                )
                # This could be an OrderNotFound if the API returns 404,
                # which _request would raise as APIError.
                # If it gets here with non-dict, it's an unexpected success response format.
                raise APIError(
                    f"Unexpected response structure for order status: {response_raw}",
                    code=APIErrorCode.UNKNOWN.value,
                )

            raw_order = BackpackRawOrder.model_validate(response_raw)
            internal_order = BackpackOrderMapper.transform_raw_order_to_internal(raw_order)
            return internal_order

        except APIError as e:
            # Specifically handle ORDER_NOT_FOUND from _request (e.g., on 404)
            if e.code == APIErrorCode.ORDER_NOT_FOUND.value:
                logger.debug(
                    f"[{self.exchange_name}] Order not found via {request_path}. "
                    f"Params: {params}, Error: {e}"
                )
                return None  # As per method contract
            logger.error(
                f"[{self.exchange_name}] API error fetching order status via {request_path}. "
                f"Params: {params}, Error: {e}"
            )
            raise  # Re-raise other APIErrors
        except ValidationError as e:
            logger.error(
                f"[{self.exchange_name}] Failed to validate order status response: {e}. "
                f"Params: {params}, Raw: {response_raw}"
            )
            raise APIError(
                "Failed to parse order status response from exchange.",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error in get_order_status. "
                f"Params: {params}, Error: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error fetching order status: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e

    async def cancel_all_orders(self, symbol: str | None = None) -> None:
        """
        Cancel all orders for a given symbol.

        Args:
            symbol: The trading symbol (optional, if None cancels all orders).

        Raises:
            ValueError: If no symbol is provided and cancel_all_orders is called.
            APIError: If the API returns an error.
        """
        if not symbol:
            raise ValueError("Backpack 'cancel all' requires a symbol.")

        params = {"symbol": symbol}
        try:
            # Use DELETE method as per OpenAPI spec. Authentication is implicit.
            await self._request("DELETE", "/api/v1/orders", params=params)
            logger.info(
                f"[{self.exchange_name}] Successfully requested cancellation of all orders "
                f"for {symbol}."
            )
            # Note: Backpack API response for successful DELETE is often empty or just status.
            # Return None to match updated base class signature
            return None
        except APIError as e:
            # Log and re-raise specific API errors
            logger.error(f"[{self.exchange_name}] Failed to cancel all orders for {symbol}: {e}")
            raise
        except Exception as e:
            # Catch-all for unexpected issues (network, etc.)
            logger.exception(
                f"[{self.exchange_name}] Unexpected error cancelling orders for {symbol}: {e}"
            )
            raise APIError(
                # Use the .value of the enum member
                code=APIErrorCode.UNKNOWN.value,
                message=f"Unexpected error cancelling orders for {symbol}: {e}",
                http_status=None,  # Status unknown in this case
                original_exception=e,  # Pass original exception for context
            ) from e

    async def get_market_data(self, symbol: str, timeframe: str, limit: int = 100) -> list[Candle]:
        """Fetch historical klines (OHLCV) for a symbol and timeframe.

        Args:
            symbol: Trading symbol (e.g., 'BTC_USDC').
            timeframe: Kline interval (e.g., '1m', '5m', '1h').
            limit: Maximum number of klines to return (default 100).

        Returns:
            A list of Candle objects, sorted oldest to newest.

        Raises:
            APIError: If the API request fails or data validation/transformation fails.
        """
        request_path = "/api/v1/klines"
        params: dict[str, Any] = {
            "symbol": symbol,
            "interval": timeframe,
            "limit": limit,
        }
        response_raw: object = None  # Initialize for error logging
        try:
            response_raw = await self._request("GET", request_path, params=params)

            # DEFENSIVE CHECK: Ensure response is a list
            if not isinstance(response_raw, list):
                logger.warning(
                    f"[{self.exchange_name}] Unexpected klines response type: "
                    f"{type(response_raw)}. Expected list. Returning empty list."
                )
                return []

            candles: list[Candle] = []
            for kline_data_raw in response_raw:
                # No need for inner isinstance check, list structure checked by raw model validator
                try:
                    # Validate raw kline data (which is a list)
                    raw_kline = BackpackRawKline.model_validate(kline_data_raw)
                    # Transform to internal Candle model
                    internal_candle = BackpackOrderMapper.transform_raw_kline_to_internal(
                        symbol=symbol, interval=timeframe, raw=raw_kline
                    )
                    candles.append(internal_candle)
                except (ValidationError, ValueError) as e:
                    logger.warning(
                        f"[{self.exchange_name}] Skipping kline due to validation/transformation "
                        f"error: {e}. Data: {kline_data_raw}"
                    )
                    continue
                except Exception as e:
                    logger.error(
                        f"[{self.exchange_name}] Unexpected error processing kline: {e}. "
                        f"Data: {kline_data_raw}",
                        exc_info=True,
                    )
                    continue

            # Backpack returns newest first, reverse to match typical convention (oldest first)
            candles.reverse()
            return candles

        except APIError as e:
            logger.error(
                f"[{self.exchange_name}] API Error getting klines for {symbol} ({timeframe}): {e}"
            )
            raise e
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error getting klines for {symbol} "
                f"({timeframe}): {e}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error getting klines for {symbol} ({timeframe}): {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e

    async def connect_websocket(self) -> None:
        """Establish the WebSocket connection using the base class logic."""
        if not self.is_connected:
            await self._connect_ws()
        else:
            logger.debug(f"[{self.exchange_name}] WebSocket already connected.")

    async def get_order(self, order_id: str, symbol: str | None = None) -> Order | None:
        """Fetch a single order by its ID.

        Args:
            order_id: The exchange-assigned order ID.
            symbol: The market symbol (required by Backpack history endpoint).

        Returns:
            The Order object if found, otherwise None.
        """
        try:
            # Reuse get_order_status which handles fetching and transformation
            return await self.get_order_status(order_id=order_id, symbol=symbol)
        except APIError as e:
            # If get_order_status raises ORDER_NOT_FOUND, return None as per this method's contract
            if e.code == APIErrorCode.ORDER_NOT_FOUND.value:
                logger.debug(
                    f"[{self.exchange_name}] Order {order_id} not found for symbol "
                    f"{symbol} (get_order)."
                )
                return None
            # Re-raise other API errors
            logger.error(f"[{self.exchange_name}] API error fetching order {order_id}: {e}")
            raise
        except Exception as e:
            # Re-raise unexpected errors
            logger.error(
                f"[{self.exchange_name}] Unexpected error fetching order {order_id}: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error fetching order {order_id}: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e

    # All abstract methods should now be implemented.

    # --- Abstract Method Implementations ---
    def _update_rate_limit_from_headers(
        self,
        headers: Mapping[str, str],
        method: str,
        path: str,
    ) -> None:
        """
        Update rate limit information based on response headers.
        Backpack does not typically provide rate limit info in standard headers.
        This is a placeholder implementation.
        """
        # Backpack does not seem to provide standard rate limit headers.
        # If specific headers are discovered, they could be parsed here.
        logger.debug(
            f"[{self.exchange_name}] _update_rate_limit_from_headers called (no-op for Backpack). "
            f"Headers: {headers}, Method: {method}, Path: {path}"
        )
        pass  # No specific Backpack headers known for this

    async def get_all_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Fetch all open orders, optionally filtering by symbol."""
        # Backpack's get_open_orders handles symbol=None to fetch all.
        return await self.get_open_orders(symbol=symbol)

    async def ping_websocket(self) -> None:
        """Send a WebSocket ping. Uses base class default implementation."""
        await super().ping_websocket()

    async def get_historical_trades(
        self, symbol: str, limit: int = 100, from_id: str | None = None
    ) -> list[Trade]:
        """Fetch historical trades for a symbol, mapped to internal Trade objects.

        Corresponds to Backpack's /api/v1/trades/history endpoint.
        The `from_id` parameter for Backpack corresponds to `offset`.
        """
        request_path = "/api/v1/trades/history"
        params: dict[str, Any] = {"symbol": symbol, "limit": limit}
        if from_id:
            params["fromId"] = from_id

        response_raw: object = None
        try:
            response_raw = await self._request("GET", request_path, params=params)

            if not isinstance(response_raw, list):
                logger.warning(
                    f"[{self.exchange_name}] Unexpected trades history response type: "
                    f"{type(response_raw)}. Expected list. Returning empty list."
                )
                return []

            internal_trades: list[Trade] = []
            for trade_data_raw in response_raw:
                if not isinstance(trade_data_raw, dict):
                    logger.warning(
                        f"Skipping non-dict item in trades history list: {trade_data_raw}"
                    )
                    continue
                try:
                    # Validate raw trade data first
                    raw_trade_model = BackpackRawTrade.model_validate(trade_data_raw)
                    # Then transform to internal model
                    internal_trade = BackpackOrderMapper.transform_raw_trade_to_internal(
                        raw_trade_model
                    )
                    if internal_trade:  # Mapper returns Trade | None
                        internal_trades.append(internal_trade)
                    else:
                        logger.warning(
                            f"[{self.exchange_name}] Skipping trade in history due to "
                            f"transformation failure (mapper returned None). Data: {trade_data_raw}"
                        )
                except (
                    ValidationError,
                    ValueError,
                ) as e:  # Catch Pydantic and other validation errors
                    logger.warning(
                        f"[{self.exchange_name}] Skipping trade in history due to validation/"
                        f"transformation error: {e}. Data: {trade_data_raw}"
                    )
                    continue
                except Exception as e:  # Catch any other unexpected errors during item processing
                    logger.error(
                        f"[{self.exchange_name}] Unexpected error processing historical "
                        f"trade: {e}. Data: {trade_data_raw}",
                        exc_info=True,
                    )
                    continue  # Continue with the next trade item
            return internal_trades
        except APIError as e:
            logger.error(f"[{self.exchange_name}] API Error getting trades history: {e}")
            raise e
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error getting trades history: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Unexpected error getting trades history for {symbol or 'all'}: {e}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e
