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
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.bp_api_models import BackpackRawOrder
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
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value

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
        """Subscribe to a Backpack WebSocket topic with explicit type safety and validation."""
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
        Get current ticker information for a symbol, with strict type validation.

        Args:
            symbol: Trading symbol

        Returns:
            Ticker object.

        Raises:
            APIError: If the ticker cannot be fetched or data is malformed.
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
            raise self._map_error_response(
                status_code=getattr(e, "status", None),
                error_body=f"Error getting ticker for {symbol}: {e}",
                request_path=request_path,
            ) from e

    async def get_order_book(self, symbol: str, depth: int = 20) -> OrderBook:
        """Get order book for a symbol, with strict type validation."""
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
            for entry_item in raw:  # type: ignore[reportUnknownVariableType]  # entry_item is from dynamic/external data (e.g., JSON, API response); static analysis cannot infer its type. All usage is guarded by explicit type checks/casts. This ignore is required to silence the linter and is safe in this context.
                # so we use cast(Any, ...) at the point of use
                item = cast(Any, entry_item)
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
            raise self._map_error_response(
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
            for asset_key, data_val in response.items():  # type: ignore[reportUnknownVariableType]  # asset_key and data_val are from dynamic/external data (e.g., JSON, API response); static analysis cannot infer their types. All usage is guarded by explicit type checks/casts. This ignore is required to silence the linter and is safe in this context.
                key = cast(Any, asset_key)
                val = cast(Any, data_val)
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
            for pos_data_item in response:  # type: ignore[reportUnknownVariableType]  # pos_data_item is from dynamic/external data (e.g., JSON, API response); static analysis cannot infer its type. All usage is guarded by explicit type checks/casts. This ignore is required to silence the linter and is safe in this context.
                item = cast(Any, pos_data_item)
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
            order = Order(
                client_order_id=response.get("clientId", client_order_id or ""),
                exchange_order_id=str(response.get("id", "")),
                symbol=response.get("symbol", symbol),
                side=OrderSide(response.get("side", side.value)),
                order_type=OrderType(response.get("orderType", order_type.value)),
                status=status,
                quantity_requested=Decimal(str(response.get("quantity", quantity))),
                quantity_filled=Decimal(str(response.get("executedQuantity", "0"))),
                price=Decimal(str(response["price"]))
                if response.get("price") is not None
                else price,
                average_fill_price=Decimal(str(response["avgFillPrice"]))
                if response.get("avgFillPrice") is not None
                else None,
                created_at=datetime.fromtimestamp(
                    int(response.get("createdAt", int(time.time() * 1000))) / 1000, UTC
                ),
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
            orders: list[Order] = []
            if not isinstance(response, list):
                logger.warning(
                    f"[{self.exchange_name}] Unexpected open orders response type: {type(response)}"
                )
                return []
            for order_data_item in response:
                status_val: Any = order_data_item.get("status", "")
                status_str: str = str(status_val).upper()
                status: OrderStatus = OrderStatus.UNKNOWN
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

                client_order_id_val: Any = order_data_item.get("clientId", "")
                exchange_order_id_val: Any = order_data_item.get("id", "")
                symbol_val: Any = order_data_item.get("symbol", symbol or "")
                side_val: Any = order_data_item.get("side", "buy")
                order_type_val: Any = order_data_item.get("orderType", "limit")
                quantity_requested_val: Any = order_data_item.get("quantity", "0")
                quantity_filled_val: Any = order_data_item.get("executedQuantity", "0")
                price_val: Any = order_data_item.get("price")
                avg_fill_price_val: Any = order_data_item.get("avgFillPrice")
                created_at_val: Any = order_data_item.get("createdAt", int(time.time() * 1000))

                order = Order(
                    client_order_id=str(client_order_id_val),
                    exchange_order_id=str(exchange_order_id_val),
                    symbol=str(symbol_val),
                    side=OrderSide(str(side_val)),
                    order_type=OrderType(str(order_type_val)),
                    status=status,
                    quantity_requested=Decimal(str(quantity_requested_val)),
                    quantity_filled=Decimal(str(quantity_filled_val)),
                    price=Decimal(str(price_val)) if price_val is not None else None,
                    average_fill_price=Decimal(str(avg_fill_price_val))
                    if avg_fill_price_val is not None
                    else None,
                    created_at=datetime.fromtimestamp(int(created_at_val) / 1000, UTC),
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
            api_error = self._map_error_response(
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

    # --- Data Parsing (Refactored to backpack/ submodules) --- #
    # The following parser methods have been refactored into standalone modules:
    #   - bp_parse_order:      backpack/bp_parse_order.py
    #   - bp_parse_trade:      backpack/bp_parse_trade.py
    #   - bp_parse_position:   backpack/bp_parse_position.py
    #   - bp_parse_balance:    backpack/bp_parse_balance.py
    #   - bp_parse_funding_rate: backpack/bp_parse_funding_rate.py
    #   - bp_parse_order_book: backpack/bp_parse_order_book.py
    # Import and use these functions from their respective modules.
    #
    # Example usage:
    #   from cyberdelta.apis.backpack.bp_parse_order import bp_parse_order
    #   order = bp_parse_order(raw_order_data)
    #

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


def _parse_api_order_to_internal_order(raw_data: dict[str, object]) -> Order:
    """
    Parses a raw order dictionary from Backpack API into the internal Order model.
    1. Validates raw data against BackpackRawOrder schema.
    2. Transforms validated raw data into the CyberDeltaEngine Order model.

    Args:
        raw_data: Raw order data from Backpack API (dict)
    Returns:
        Order: Standardized Order object for CyberDeltaEngine
    Raises:
        APIError: If validation or transformation fails
    """
    try:
        # 1. VALIDATE against the raw API structure model
        raw_order = BackpackRawOrder.model_validate(raw_data)

        # 2. TRANSFORM validated raw data into your INTERNAL Order model
        # Map status
        try:
            order_status_enum = OrderStatus[raw_order.status.upper()]
        except KeyError:
            logger.warning(
                f"Unknown Backpack order status '{raw_order.status}', mapping to UNKNOWN."
            )
            order_status_enum = OrderStatus.UNKNOWN

        # Map side (Backpack: 'Bid'/'Ask', 'buy'/'sell', etc.)
        side_val = raw_order.side.lower()
        if side_val in ("buy", "bid"):
            order_side_enum = OrderSide.BUY
        elif side_val in ("sell", "ask"):
            order_side_enum = OrderSide.SELL
        else:
            logger.warning(f"Unknown Backpack order side '{raw_order.side}', defaulting to BUY.")
            order_side_enum = OrderSide.BUY

        # Map order type (Backpack: 'LIMIT', 'MARKET', etc.)
        order_type_val = raw_order.order_type.lower()
        try:
            order_type_enum = OrderType(order_type_val)
        except ValueError:
            logger.warning(
                f"Unknown Backpack order type '{raw_order.order_type}', defaulting to LIMIT."
            )
            order_type_enum = OrderType.LIMIT

        parsed_price = parse_decimal_value(raw_order.price)
        parsed_avg_fill_price = parse_decimal_value(raw_order.average_fill_price)
        parsed_created_at = parse_datetime_utc(raw_order.created_at)
        if parsed_created_at is None:
            raise ValueError("Failed to parse created_at timestamp from raw order")

        parsed_quantity = parse_decimal_value(raw_order.quantity, allow_none=False)
        parsed_quantity_filled = parse_decimal_value(raw_order.quantity_filled, allow_none=False)
        if parsed_quantity is None or parsed_quantity_filled is None:
            raise ValueError("Failed parsing required quantity fields")

        internal_order = Order(
            client_order_id=raw_order.client_order_id or "",
            exchange_order_id=raw_order.exchange_order_id,
            symbol=raw_order.symbol,
            side=order_side_enum,
            order_type=order_type_enum,
            status=order_status_enum,
            quantity_requested=parsed_quantity,
            quantity_filled=parsed_quantity_filled,
            price=parsed_price,
            average_fill_price=parsed_avg_fill_price,
            created_at=parsed_created_at,
            updated_at=parse_datetime_utc(raw_order.updated_at) if raw_order.updated_at else None,
        )
        return internal_order

    except ValidationError as e:
        logger.error(f"Pydantic validation failed for raw order data: {e}. Data: {raw_data}")
        raise APIError(
            f"Invalid raw order data structure from Backpack: {e}", code=APIErrorCode.INVALID_PARAMS
        ) from e
    except (ValueError, TypeError, KeyError) as e:
        logger.error(f"Error transforming raw Backpack order data: {e}. Data: {raw_data}")
        raise APIError(
            f"Error processing order data fields: {e}", code=APIErrorCode.INVALID_PARAMS
        ) from e
    except Exception as e:
        logger.error(
            f"Unexpected error parsing Backpack order: {e}. Data: {raw_data}", exc_info=True
        )
        raise APIError(f"Unexpected error parsing order: {e}", code=APIErrorCode.UNKNOWN) from e
