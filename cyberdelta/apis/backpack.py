import asyncio
import hashlib
import hmac
import logging
import time
from datetime import UTC, datetime  # Added datetime, UTC
from decimal import Decimal
from typing import Any

from cyberdelta.apis.base import (  # Use absolute import
    APIError,
    APIErrorCode,
    ExchangeAPI,
    MessageHandler,
)
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

logger = logging.getLogger(__name__)


class BackpackAPI(ExchangeAPI):
    """API Client for Backpack Exchange."""

    def __init__(self, api_config: dict[str, Any], secrets: dict[str, str | None]) -> None:
        super().__init__("backpack", api_config, secrets)
        self._api_key = secrets.get("BACKPACK_API_KEY")
        self._api_secret = secrets.get("BACKPACK_API_SECRET")
        if not self._api_key or not self._api_secret:
            logger.warning("Backpack API key/secret not provided. Signed operations will fail.")

    # --- WebSocket Implementation --- #

    async def _route_ws_message(self, message: dict[str, Any]) -> None:
        """Route incoming WebSocket messages."""
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
        """Subscribe to a Backpack WebSocket topic."""
        if not self._ws_connection or not self.is_connected:
            logger.error(f"[{self.exchange_name}] Cannot subscribe, WebSocket not connected.")
            # Store handler for reconnection
            self._ws_handlers[topic] = handler
            return

        # Construct Backpack subscription message
        subscription_message = {
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
        """Resubscribe to all registered topics upon reconnection."""
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
        """Authenticate API request for Backpack."""
        return self._sign_request(method, path, params, data)

    def _sign_request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Sign requests for Backpack using HMAC-SHA256."""
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
        Get current ticker information for a symbol.

        Args:
            symbol: Trading symbol

        Returns:
            Ticker object.

        Raises:
            APIError: If the ticker cannot be fetched.
        """
        request_path = f"/api/v1/ticker/{symbol}"
        try:
            response = await self._request("GET", request_path)

            # Process response to create Ticker object
            ticker = Ticker(
                symbol=response["symbol"],
                bid=Decimal(str(response["bidPrice"])),
                ask=Decimal(str(response["askPrice"])),
                price=Decimal(str(response["lastPrice"])),
                volume=Decimal(str(response["volume"])),
                timestamp=int(response["time"]),
            )
            return ticker
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Error getting ticker for {symbol}: {e}", exc_info=True
            )
            raise self._map_error_response(
                status_code=getattr(e, "status", None),
                error_body=f"Error getting ticker for {symbol}: {e}",
                request_path=request_path,
            ) from e

    async def get_order_book(self, symbol: str, depth: int = 20) -> OrderBook:
        """Get order book for a symbol."""
        request_path = "/api/v1/depth"
        try:
            params: dict[str, Any] = {"symbol": symbol}
            if depth:
                params["limit"] = depth

            response = await self._request("GET", request_path, params=params)

            bids = [
                (Decimal(str(price)), Decimal(str(qty))) for price, qty in response.get("bids", [])
            ]
            asks = [
                (Decimal(str(price)), Decimal(str(qty))) for price, qty in response.get("asks", [])
            ]

            return OrderBook(
                symbol=symbol,
                bids=bids,
                asks=asks,
                timestamp=int(response.get("time", int(time.time() * 1000))),
            )
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Error getting order book for {symbol}: {e}", exc_info=True
            )
            raise self._map_error_response(
                status_code=getattr(e, "status", None),
                error_body=f"Error getting order book for {symbol}: {e}",
                request_path=request_path,
            ) from e

    async def get_recent_trades(self, symbol: str, limit: int | None = 50) -> list[Trade]:
        """
        Get recent trades for a symbol.

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
            response = await self._request("GET", request_path, params=params)

            trades = []
            for trade_data in response:
                try:
                    # Map API fields to Trade dataclass fields
                    side = (
                        OrderSide.BUY if trade_data.get("isBuyerMaker", False) else OrderSide.SELL
                    )
                    # Convert string fields to required types (Decimal, int)
                    price_dec = Decimal(str(trade_data["price"]))
                    qty_dec = Decimal(str(trade_data["qty"]))
                    # Fee information might not be present, handle Optional
                    fee_dec = None  # Assuming fee is not in this endpoint
                    fee_asset = None  # Assuming fee is not in this endpoint

                    trade = Trade(
                        id=str(trade_data["id"]),
                        symbol=symbol,
                        timestamp=int(trade_data["time"]),  # Use timestamp field
                        side=side,
                        price=price_dec,
                        quantity=qty_dec,
                        fee=fee_dec,
                        fee_asset=fee_asset,
                        is_maker=trade_data.get("isMaker", None),  # Adapt if field name differs
                        # Add other fields if available in API response
                        exchange=self.exchange_name,  # Add exchange name
                    )
                    trades.append(trade)
                except KeyError as e:
                    logger.warning(
                        f"[{self.exchange_name}] Missing expected key {e} in trade data: "
                        f"{trade_data}"
                    )
                except Exception as e:
                    logger.warning(f"[{self.exchange_name}] Error processing trade: {e}")

            return trades
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Error getting recent trades for {symbol}: {e}",
                exc_info=True,
            )
            # Raise APIError for consistency
            raise self._map_error_response(
                status_code=getattr(e, "status", None),
                error_body=f"Error getting recent trades for {symbol}: {e}",
                request_path=request_path,
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
                rate=Decimal(str(funding_data["rate"])),  # Convert string/float to Decimal
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
            response = await self._request("GET", "/api/v1/capital")
            balances = {}
            for asset, data in response.items():
                balance = Balance(
                    asset=asset,
                    total=Decimal(str(data["available"])) + Decimal(str(data["locked"])),
                    available=Decimal(str(data["available"])),
                )
                balances[asset] = balance
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
            response = await self._request("GET", "/api/v1/positions", signed=True)

            positions = []  # Changed from dict to list
            for pos_data in response:
                symbol_from_data = pos_data.get("symbol")
                if not symbol_from_data:
                    continue

                # Convert to Decimal
                size_dec = Decimal(str(pos_data.get("positionSize", "0")))
                entry_price_dec = Decimal(str(pos_data.get("entryPrice", "0")))
                mark_price_dec = Decimal(str(pos_data.get("markPrice", "0")))
                liq_price_str = pos_data.get("liquidationPrice")
                liq_price_dec = (
                    Decimal(str(liq_price_str))
                    if liq_price_str is not None and liq_price_str != "0"
                    else Decimal("0.0")
                )  # Default to 0 if None
                pnl_dec = Decimal(str(pos_data.get("unrealizedPnl", "0")))
                leverage_float = float(pos_data.get("leverage", "1.0"))  # Get as float
                leverage_dec = Decimal(str(leverage_float))  # Convert to Decimal via string

                position = Position(
                    symbol=symbol_from_data,
                    size=size_dec,
                    entry_price=entry_price_dec,
                    mark_price=mark_price_dec,
                    side=OrderSide.BUY if size_dec > Decimal("0") else OrderSide.SELL,
                    liquidation_price=liq_price_dec,
                    unrealized_pnl=pnl_dec,
                    leverage=leverage_dec,  # Pass the Decimal version
                )
                positions.append(position)  # Changed from dict assignment to list append
            return positions
        except Exception as e:
            logger.error(f"[backpack] Error getting positions: {e}")
            return []  # Return empty list on error

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

            if not response or "id" not in response:
                raise APIError(
                    APIErrorCode.EXCHANGE_ERROR,
                    f"[{self.exchange_name}] Failed to place order. Invalid response: {response}",
                    http_status=None,
                    request_path=request_path,
                    response_body=str(response),
                )

            # Map API status string to OrderStatus enum
            status_str = response.get("status", "").upper()
            status = OrderStatus.UNKNOWN
            if status_str == "NEW":
                status = OrderStatus.NEW
            elif status_str == "FILLED":
                status = OrderStatus.FILLED
            elif status_str == "PARTIALLY_FILLED":
                status = OrderStatus.PARTIALLY_FILLED
            elif status_str == "CANCELLED":
                status = OrderStatus.CANCELED
            elif status_str == "EXPIRED":
                status = OrderStatus.EXPIRED
            elif status_str == "REJECTED":
                status = OrderStatus.REJECTED

            # Ensure essential fields are present before creating Order
            order_id = str(response["id"])
            order_symbol = response.get("symbol", symbol)
            order_side = OrderSide(response.get("side", side.value))
            order_type_resp = OrderType(response.get("orderType", order_type.value))
            order_quantity = Decimal(str(response.get("quantity", quantity)))

            order = Order(
                id=order_id,
                symbol=order_symbol,
                side=order_side,
                type=order_type_resp,
                price=Decimal(str(response.get("price", "0"))) if response.get("price") else price,
                quantity=order_quantity,
                filled_quantity=Decimal(str(response.get("executedQuantity", "0"))),
                status=status,
                time=int(response.get("createdAt", int(time.time() * 1000))),
                client_order_id=response.get("clientId", client_order_id),
                reduce_only=response.get("reduceOnly", reduce_only),
                avg_fill_price=Decimal(str(response.get("avgFillPrice", "0")))
                if response.get("avgFillPrice")
                else None,
            )
            return order
        except ValueError as ve:
            raise ve
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Error placing order: {e}", exc_info=True)
            # Attempt to map the error, default to generic EXCHANGE_ERROR
            api_error = self._map_error_response(
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
            api_error = self._map_error_response(
                status_code=getattr(e, "status", None),
                error_body=str(e),
                exchange_message=(
                    f"Error canceling order {order_id} for {symbol} (Path: {request_path}): {e}"
                ),
            )
            raise api_error from e

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """
        Get open orders for a specific symbol or all symbols.
        """
        request_path = "/api/v1/orders"
        try:
            params: dict[str, str] = {}
            if symbol:
                params["symbol"] = symbol

            response = await self._request("GET", request_path, params=params, signed=True)
            orders = []
            for order_data in response:
                status_str = order_data.get("status", "").upper()
                status = OrderStatus.UNKNOWN
                if status_str == "NEW":
                    status = OrderStatus.NEW
                elif status_str == "FILLED":
                    status = OrderStatus.FILLED
                elif status_str == "PARTIALLY_FILLED":
                    status = OrderStatus.PARTIALLY_FILLED
                elif status_str == "CANCELLED":
                    status = OrderStatus.CANCELED
                elif status_str == "EXPIRED":
                    status = OrderStatus.EXPIRED
                elif status_str == "REJECTED":
                    status = OrderStatus.REJECTED

                order = Order(
                    id=str(order_data["id"]),
                    symbol=order_data["symbol"],
                    side=OrderSide(order_data["side"]),
                    type=OrderType(order_data["orderType"]),
                    price=Decimal(str(order_data.get("price", "0")))
                    if order_data.get("price")
                    else None,
                    quantity=Decimal(str(order_data["quantity"])),
                    filled_quantity=Decimal(str(order_data.get("executedQuantity", "0"))),
                    status=status,
                    time=int(order_data.get("createdAt", int(time.time() * 1000))),
                    client_order_id=order_data.get("clientId", ""),
                    reduce_only=order_data.get("reduceOnly", False),
                    avg_fill_price=Decimal(str(order_data.get("avgFillPrice", "0")))
                    if order_data.get("avgFillPrice")
                    else None,
                )
                if status in [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED, OrderStatus.OPEN]:
                    orders.append(order)
            return orders
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Error getting open orders: {e}", exc_info=True)
            # Raise APIError as per ExchangeAPI
            api_error = self._map_error_response(
                status_code=getattr(e, "status", None),
                error_body=str(e),
                exchange_message=(
                    f"Error getting open orders for {symbol or 'all'} (Path: {request_path}): {e}"
                ),
            )
            raise api_error from e

    async def fetch_ticker(self, symbol: str) -> Ticker:
        """Fetch ticker information (ensuring non-None return)."""
        ticker = await self.get_ticker(symbol)
        if ticker is None:
            raise APIError(f"Could not fetch ticker for {symbol}")
        return ticker

    async def fetch_order_book(self, symbol: str, depth: int | None = None) -> OrderBook:
        """Fetch order book (ensuring non-None return)."""
        order_book = await self.get_order_book(symbol, depth)
        if order_book is None:
            raise APIError(f"Could not fetch order book for {symbol}")
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

            timestamp = int(response.get("time", int(time.time() * 1000)))
            funding_rate_dec = Decimal(str(response.get("fundingRate", "0")))
            mark_price_dec = (
                Decimal(str(response.get("markPrice", "0"))) if response.get("markPrice") else None
            )

            if mark_price_dec is None:
                logger.warning(
                    f"[{self.exchange_name}] Mark price not found in funding rate response for {symbol}."
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
            api_error = self._map_error_response(
                status_code=getattr(e, "status", None),
                error_body=str(e),
                exchange_message=f"Error getting funding rate for {symbol} (Path: {request_path}): {e}",
            )
            raise api_error from e

    # --- Placeholder for required abstract method --- #
    async def get_funding_rates(self, symbol: str | None = None) -> list[FundingRate]:
        """(Not Implemented) Get funding rates for one/all symbols."""
        # Backpack might only provide the current rate per symbol.
        # This needs a proper implementation if historical/multiple rates are needed.
        logger.warning(
            f"[{self.exchange_name}] get_funding_rates not fully implemented. "
            f"Fetching current rate only."
        )
        rates = []
        target_symbol = symbol  # Assume fetching for a single symbol for now
        if target_symbol:
            try:
                current_rate = await self.fetch_funding_rate(target_symbol)
                rates.append(current_rate)
            except APIError as e:
                logger.error(
                    f"[{self.exchange_name}] Failed to fetch current funding rate for {target_symbol} within get_funding_rates: {e}"
                )
                # Optionally re-raise or return empty list based on desired behavior
                raise  # Re-raise the APIError
        else:
            logger.error(
                f"[{self.exchange_name}] get_funding_rates without a specific symbol "
                f"is not supported by Backpack API."
            )
            # Raise error or return empty list
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
            # The response type here is inherently uncertain without API docs,
            # so dict[str, Any] is a reasonable starting point.
            response: dict[str, Any] = await self._request("GET", request_path, signed=True)
            return response
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Error getting account info: {e}")
            # Raise APIError for consistency.
            api_error = self._map_error_response(
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

    def _map_error_response(
        self,
        status_code: int | None,
        error_body: str,
        error_data: dict[str, Any] | None = None,
        request_path: str | None = None,
        exchange_message: str | None = None,
    ) -> APIError:
        """Map Backpack error responses to generic APIErrorCode."""
        # Convert body to lower for case-insensitive matching
        error_body_lower = error_body.lower()
        mapped_code = APIErrorCode.UNKNOWN  # Changed Default

        # --- Educated Guesses for Backpack Error Mappings ---
        # Authentication / Signature Errors
        if (
            "invalid signature" in error_body_lower
            or "authentication failed" in error_body_lower
            or "invalid api key" in error_body_lower
        ):
            mapped_code = APIErrorCode.AUTHENTICATION_FAILED  # Use Enum member
        # Rate Limits
        elif "rate limit exceeded" in error_body_lower or "too many requests" in error_body_lower:
            mapped_code = APIErrorCode.RATE_LIMITED  # Use Enum member
        # Invalid Parameters / Bad Request
        elif "invalid symbol" in error_body_lower:
            mapped_code = APIErrorCode.INVALID_SYMBOL  # Use Enum member
        elif "invalid quantity" in error_body_lower or "invalid size" in error_body_lower:
            # Consider mapping to QUANTITY_OUT_OF_RANGE if more specific
            mapped_code = APIErrorCode.INVALID_ORDER_SIZE  # Use Enum member
        elif "invalid parameter" in error_body_lower or "bad request" in error_body_lower:
            mapped_code = (
                APIErrorCode.INVALID_REQUEST
            )  # Use Enum member (more specific than BAD_REQUEST)
        # Order specific errors
        elif "order not found" in error_body_lower:
            mapped_code = APIErrorCode.ORDER_NOT_FOUND  # Use Enum member
        elif "insufficient balance" in error_body_lower or "insufficient funds" in error_body_lower:
            mapped_code = APIErrorCode.INSUFFICIENT_FUNDS  # Use Enum member
        # Server / Availability Errors
        elif (
            "service unavailable" in error_body_lower or "internal server error" in error_body_lower
        ):
            mapped_code = APIErrorCode.SERVICE_UNAVAILABLE  # Use Enum member
        # --- End Guesses ---

        # Log the original error for debugging
        log_message = (
            f"Mapping Backpack error (Path: {request_path}, Status: {status_code}, "
            f"Body: '{error_body}') to APIErrorCode.{mapped_code.name}"
        )
        logger.warning(log_message)

        # Construct the final error message for the exception
        final_message = exchange_message or error_body  # Use specific message if provided

        return APIError(
            message=final_message,  # Pass refined message
            code=mapped_code,  # Pass the mapped enum code
            http_status=status_code,
            # Pass original error details if available (e.g., from parsed JSON error data)
            exchange_code=str(error_data.get("code")) if error_data else None,
            exchange_message=error_data.get("msg") if error_data else None,
            original_exception=None,  # Can pass original exception if caught earlier
        )

    # --- Data Parsing (Implementation for Base Class Abstract Method) --- #

    def parse_order(self, data: dict[str, Any]) -> Order:
        """Parse raw order data from Backpack into an Order object."""
        # TODO: Implement actual Backpack order parsing logic based on API V1 docs
        # Example structure (needs verification with actual API response)
        try:
            status_str = data.get("status", "").upper()
            order_status = (
                OrderStatus[status_str]
                if status_str in OrderStatus.__members__
                else OrderStatus.UNKNOWN
            )

            # Handle potential None or invalid Decimal values safely
            price = data.get("price")
            quantity = data.get("quantity")
            filled_quantity = data.get("filledQuantity")
            avg_fill_price = data.get("avgFillPrice")
            # Backpack uses integer ms timestamps
            order_time_ms = data.get("time") or data.get("createdAt")

            return Order(
                symbol=data["symbol"],
                id=str(data["id"]),
                side=OrderSide(data["side"]),
                type=OrderType(data["orderType"]),
                quantity=Decimal(str(quantity)) if quantity is not None else Decimal("0"),
                status=order_status,
                client_order_id=data.get("clientId"),
                price=Decimal(str(price)) if price is not None else None,
                avg_fill_price=Decimal(str(avg_fill_price)) if avg_fill_price is not None else None,
                filled_quantity=Decimal(str(filled_quantity))
                if filled_quantity is not None
                else Decimal("0"),
                # remaining_quantity calculation might be needed
                # Assuming order_time_ms is already an int representing milliseconds
                time=datetime.fromtimestamp(order_time_ms / 1000, UTC) if order_time_ms else None,
                time_in_force=TimeInForce(data["timeInForce"]) if "timeInForce" in data else None,
                post_only=data.get("postOnly", False),
                reduce_only=data.get("reduceOnly", False),
                # Add other fields like leverage, metadata if available
            )
        except KeyError as e:
            logger.error(f"[{self.exchange_name}] Missing key {e} in order data: {data}")
            raise APIError(
                f"Missing key {e} in order data", code=APIErrorCode.INVALID_PARAMS
            ) from e
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Error parsing order data: {e}", exc_info=True)
            raise APIError(
                f"Error parsing order data: {e}", code=APIErrorCode.INVALID_PARAMS
            ) from e

    # --- WebSocket Subscriptions (Implementations for Base Class Abstract Methods) ---

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
