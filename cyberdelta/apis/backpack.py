import asyncio
import hashlib
import hmac
import logging
import time
from decimal import Decimal
from typing import Any

from ..core.models import (
    Balance,
    FundingRate,
    Order,
    OrderBook,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
    Ticker,
    Trade,
)
from .base import APIError, APIErrorCode, ExchangeAPI, MessageHandler

logger = logging.getLogger(__name__)


class BackpackAPI(ExchangeAPI):
    """API Client for Backpack Exchange."""

    def __init__(self, api_config: dict[str, Any], secrets: dict[str, str | None]):
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

    async def get_ticker(self, symbol: str) -> Ticker | None:
        """
        Get current ticker information for a symbol.

        Args:
            symbol: Trading symbol

        Returns:
            Ticker object or None if error
        """
        try:
            response = await self._request("GET", f"/api/v1/ticker/{symbol}")

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
            logger.error(f"[backpack] Error getting ticker for {symbol}: {e}")
            return None

    async def get_order_book(self, symbol: str, depth: int | None = None) -> OrderBook | None:
        """Get order book for a symbol."""
        try:
            params = {"symbol": symbol}
            if depth:
                params["limit"] = depth

            response = await self._request("GET", "/api/v1/depth", params=params)

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
            logger.error(f"[{self.exchange_name}] Error getting order book for {symbol}: {e}")
            return None

    async def get_recent_trades(self, symbol: str, limit: int | None = 50) -> list[Trade]:
        """
        Get recent trades for a symbol.

        Args:
            symbol: Trading symbol
            limit: Maximum number of trades to return

        Returns:
            List of Trade objects
        """
        try:
            params = {"symbol": symbol}
            if limit is not None:
                params["limit"] = limit
            response = await self._request("GET", "/api/v1/trades", params=params)

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
                        f"[backpack] Missing expected key {e} in trade data: {trade_data}"
                    )
                except Exception as e:
                    logger.warning(f"[backpack] Error parsing trade data: {e} - Data: {trade_data}")

            return trades
        except APIError as e:
            logger.error(f"[{self.exchange_name}] Error getting recent trades for {symbol}: {e}")
            return []
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error getting recent trades for {symbol}: {e}",
                exc_info=True,
            )
            return []

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
                    f"[{self.exchange_name}] Unexpected funding rate data format for {symbol}: {response}"
                )
                return None

            # Ensure fields exist before accessing
            if not all(k in funding_data for k in ["rate", "markPrice", "indexPrice", "time"]):
                logger.warning(
                    f"[{self.exchange_name}] Missing keys in funding rate data for {symbol}: {funding_data}"
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

    async def get_positions(self) -> dict[str, Position]:
        """
        Get current positions.

        Returns:
            Dictionary of positions by symbol
        """
        try:
            response = await self._request("GET", "/api/v1/positions", signed=True)

            positions = {}
            for pos_data in response:
                symbol = pos_data.get("symbol")
                if not symbol:
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
                leverage = float(pos_data.get("leverage", "1.0"))  # Keep leverage as float

                position = Position(
                    symbol=symbol,
                    exchange=self.exchange_name,
                    size=size_dec,
                    entry_price=entry_price_dec,
                    mark_price=mark_price_dec,
                    side=OrderSide.BUY if size_dec > Decimal("0") else OrderSide.SELL,
                    liquidation_price=liq_price_dec,
                    unrealized_pnl=pnl_dec,
                    leverage=leverage,
                )
                positions[symbol] = position
            return positions
        except Exception as e:
            logger.error(f"[backpack] Error getting positions: {e}")
            return {}

    async def place_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        price: Decimal | None = None,
        time_in_force: str | None = None,  # e.g., GTC, IOC, FOK
        client_order_id: str | None = None,
        reduce_only: bool = False,
    ) -> Order | None:
        """
        Place an order on Backpack Exchange.

        Args:
            symbol: Trading symbol (e.g., 'BTC_USDC')
            side: Order side (BUY or SELL)
            order_type: Order type (LIMIT, MARKET, etc.)
            quantity: Order quantity
            price: Order price (required for limit orders)
            client_order_id: Custom client order ID
            time_in_force: Time in force for the order (default 'GTC' - Good Till Cancel)
            **kwargs: Additional exchange-specific parameters like:
                - reduce_only: Whether this is a reduce-only order (bool)
                - post_only: Whether this is a post-only order (bool)

        Returns:
            Order object if successful, None otherwise

        Raises:
            APIError: On API errors or parameter validation failures
        """
        try:
            path = "/api/v1/order"
            order_data: dict[str, Any] = {
                "symbol": symbol,
                "side": side.value,  # Use enum value ('BUY' or 'SELL')
                "orderType": order_type.value,  # Use enum value ('LIMIT', 'MARKET')
                "quantity": str(quantity),  # Send quantity as string
            }
            if order_type == OrderType.LIMIT:
                if price is None:
                    raise ValueError("Price is required for LIMIT orders")
                order_data["price"] = str(price)  # Send as string
                if time_in_force:
                    # Backpack uses specific strings like 'GTC', 'IOC', 'FOK'
                    order_data["timeInForce"] = time_in_force

            if client_order_id:
                order_data["clientId"] = client_order_id
            if reduce_only:
                order_data["reduceOnly"] = True

            response = await self._request("POST", path, data=order_data)

            # Parse response to create Order object
            # Backpack order creation response might differ, adapt parsing logic
            # Map API status string to OrderStatus enum
            status_str = response.get("status", "").upper()
            status = OrderStatus.NEW  # Default or map based on response
            if status_str == "FILLED":
                status = OrderStatus.FILLED
            elif status_str == "PARTIALLY_FILLED":
                status = OrderStatus.PARTIALLY_FILLED
            elif status_str == "CANCELED":
                status = OrderStatus.CANCELED
            elif status_str == "EXPIRED":
                status = OrderStatus.EXPIRED  # Or map to CANCELED if appropriate
            # Add more mappings as needed

            order = Order(
                id=str(response["id"]),
                symbol=response["symbol"],
                side=OrderSide(response["side"]),
                type=OrderType(response["orderType"]),
                price=Decimal(str(response.get("price", "0")))
                if response.get("price")
                else None,  # Handle optional price
                quantity=Decimal(str(response["quantity"])),
                filled_quantity=Decimal(str(response.get("executedQuantity", "0"))),
                status=status.value,  # Use enum value
                time=int(response["createdAt"]),  # Ensure timestamp is int
                client_order_id=response.get("clientId", ""),
                reduce_only=response.get("reduceOnly", False),
                avg_fill_price=Decimal(str(response.get("avgFillPrice", "0")))
                if response.get("avgFillPrice")
                else None,
            )
            return order
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Error placing order: {e}")
            return None

    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        """Cancel an existing order."""
        try:
            path = "/api/v1/order"
            params = {"symbol": symbol, "orderId": order_id}
            await self._request("DELETE", path, params=params)
            logger.info(f"[{self.exchange_name}] Canceled order {order_id} for {symbol}")
            return True
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Error canceling order {order_id}: {e}")
            return False

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """
        Get open orders for a specific symbol or all symbols.

        Args:
            symbol: Trading symbol (optional)

        Returns:
            List of open Order objects
        """
        try:
            path = "/api/v1/orders"
            params: dict[str, str] = {}
            if symbol:
                params["symbol"] = symbol

            response = await self._request("GET", path, params=params)
            orders = []
            for order_data in response:
                # Map API status string to OrderStatus enum
                status_str = order_data.get("status", "").upper()
                status = OrderStatus.NEW  # Default or map based on response
                if status_str == "FILLED":
                    status = OrderStatus.FILLED
                elif status_str == "PARTIALLY_FILLED":
                    status = OrderStatus.PARTIALLY_FILLED
                elif status_str == "CANCELED":
                    status = OrderStatus.CANCELED
                elif status_str == "EXPIRED":
                    status = OrderStatus.EXPIRED  # Or map to CANCELED if appropriate
                # Add more mappings as needed

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
                    status=status.value,  # Use enum value
                    time=int(order_data["createdAt"]),  # Convert string timestamp to int
                    client_order_id=order_data.get("clientId", ""),
                    reduce_only=order_data.get("reduceOnly", False),
                    avg_fill_price=Decimal(str(order_data.get("avgFillPrice", "0")))
                    if order_data.get("avgFillPrice")
                    else None,  # Added avg_fill_price
                )
                # Filter for open orders based on status (adjust as needed)
                if status in [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED]:
                    orders.append(order)
            return orders
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Error getting open orders: {e}")
            return []

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
        # get_recent_trades already returns [] on error
        return trades

    async def fetch_funding_rate(self, symbol: str) -> FundingRate:
        """Fetch funding rate (ensuring non-None return)."""
        funding_rate = await self.get_funding_rate(symbol)
        if funding_rate is None:
            raise APIError(f"Could not fetch funding rate for {symbol}")
        return funding_rate

    # --- Account Management --- #
    async def get_account_info(self) -> dict[str, Any] | None:
        """Get general account information (if available)."""
        try:
            # Example: Backpack might have a specific account endpoint
            response = await self._request("GET", "/api/v1/account")
            return response
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Error getting account info: {e}")
            return None

    async def transfer(
        self, asset: str, amount: float, from_account: str, to_account: str
    ) -> dict[str, Any]:
        """Transfer funds between accounts."""
        # Backpack might not support internal transfers via API, this is a placeholder
        logger.warning("Backpack API might not support internal transfers.")
        raise NotImplementedError("Backpack internal transfers not implemented.")
        # Example structure if it existed:
        # data = {
        #    "asset": asset,
        #    "amount": str(amount),
        #    "fromAccountType": from_account,
        #    "toAccountType": to_account
        # }
        # response = await self._request("POST", "/api/v1/transfer", data=data, signed=True)
        # return response

    async def withdraw(
        self, asset: str, address: str, amount: Decimal, network: str | None = None
    ) -> dict[str, Any] | None:
        """Withdraw funds."""
        # Implementation depends on Backpack API for withdrawals
        logger.warning(f"[{self.exchange_name}] withdraw not implemented for Backpack.")
        return None  # Placeholder

    def _map_error_response(
        self,
        status_code: int,
        error_body: str,
        error_data: dict[str, Any] | None = None,
    ) -> APIError:
        """Map Backpack error responses to standard APIError."""
        # Default error
        error_code = APIErrorCode.UNKNOWN
        message = error_body

        # Try to parse specific error details from Backpack response
        if error_data:
            message = error_data.get("msg", error_body)
            code = error_data.get("code")

            # Map Backpack error codes to APIErrorCode
            if (
                code == -1021
            ):  # Example: Timestamp for this request was 1000ms ahead of the server time
                error_code = APIErrorCode.INVALID_TIMESTAMP  
            elif code == -2014:  # Example: API-key format invalid.
                error_code = APIErrorCode.INVALID_API_KEY  
            elif code == -2015:  # Example: Invalid API-key, IP, or permissions for action.
                error_code = APIErrorCode.AUTHENTICATION_ERROR  
            elif code == -1121:  # Example: Invalid symbol.
                error_code = APIErrorCode.INVALID_SYMBOL  
            elif code == -1013:  # Example: Filter failure: LOT_SIZE
                error_code = APIErrorCode.INVALID_ORDER_SIZE  
            elif code == -2010:  # Example: New order rejected.
                error_code = APIErrorCode.ORDER_REJECTED
            elif code == -2011:  # Example: Cancel order failed.
                error_code = APIErrorCode.ORDER_NOT_FOUND  # Or other specific cancel error
            elif code == -1022:  # Signature for this request is not valid.
                error_code = APIErrorCode.INVALID_SIGNATURE  
            elif status_code == 429:  # Rate limit exceeded
                error_code = APIErrorCode.RATE_LIMIT_EXCEEDED  
            elif status_code == 401:  # Unauthorized
                error_code = APIErrorCode.AUTHENTICATION_ERROR  
            elif status_code == 400:  # Bad request
                error_code = APIErrorCode.BAD_REQUEST
            elif status_code == 500:  # Internal server error
                error_code = APIErrorCode.EXCHANGE_ERROR  
            elif status_code == 503:  # Service unavailable
                error_code = APIErrorCode.SERVICE_UNAVAILABLE 

        elif status_code == 429:
            error_code = APIErrorCode.RATE_LIMIT_EXCEEDED  
            message = "Rate limit exceeded"
        elif status_code == 401:
            error_code = APIErrorCode.AUTHENTICATION_ERROR  
            message = "Authentication failed"
        elif status_code == 500:
            error_code = APIErrorCode.EXCHANGE_ERROR  
            message = "Exchange internal error"
        elif status_code == 503:
            error_code = APIErrorCode.SERVICE_UNAVAILABLE  
            message = "Exchange service unavailable"

        return APIError(
            message=message,
            code=error_code,
            http_status=status_code,
        )
