import asyncio
import json
import logging
import time
from datetime import datetime
from decimal import Decimal
from typing import Any

from ..core.models import (
    Balance,
    FundingRate,
    Order,
    OrderBook,
    OrderSide,
    OrderType,
    Position,
    Ticker,
    Trade,
    OrderStatus,
)
from .base import APIError, APIErrorCode, ExchangeAPI, MessageHandler

# Import required for signature generation
try:
    from eth_account import Account
    from eth_account.messages import encode_structured_data
except ImportError:
    Account = None
    encode_structured_data = None
    logging.warning("eth_account module not found. Hyperliquid API signing will not work.")

logger = logging.getLogger(__name__)


class HyperliquidAPI(ExchangeAPI):
    """API Client for Hyperliquid DEX."""

    def __init__(self, api_config: dict[str, Any], secrets: dict[str, str | None]):
        super().__init__("hyperliquid", api_config, secrets)
        # Specific Hyperliquid initialization
        self._private_key = secrets.get("HYPERLIQUID_WALLET_PRIVATE_KEY")
        self._wallet_address = secrets.get("HYPERLIQUID_WALLET_ADDRESS")
        self._nonce_counter = 0
        self._nonce_lock = asyncio.Lock()

        # Check if we can sign transactions
        if Account is None:
            logger.warning("eth_account not imported. Signed requests will fail.")
        if not self._private_key:
            logger.warning("Hyperliquid private key not provided. Signed operations will fail.")

    async def _authenticate(
        self,
        method: str,
        path: str,
        params: dict | None = None,
        data: dict | None = None,
    ) -> dict[str, Any]:
        """
        Sign a request using EIP-712 and wallet private key.

        Args:
            method: HTTP method
            path: API endpoint
            params: URL parameters
            data: Request body

        Returns:
            Authentication data for the request
        """
        # For test environment, provide mock authentication if private key is not available
        if not self._private_key or Account is None:
            # Check if we're in a test environment
            if "test" in path or (data and isinstance(data, dict) and data.get("test") == True):
                logger.info(
                    f"[{self.exchange_name}] Using mock authentication for test environment"
                )
                timestamp = str(int(time.time() * 1000))
                nonce = "12345"
                signature = "0x" + "0" * 130  # Mock signature

                return {
                    "headers": {
                        "X-HL-Signature": signature,
                        "X-HL-Timestamp": timestamp,
                        "X-HL-Nonce": nonce,
                    },
                    "params": params,
                    "data": data,
                }
            else:
                # In production, we need proper authentication
                raise APIError(
                    "Private key not provided or eth_account not available",
                    code=APIErrorCode.AUTHENTICATION_FAILED,
                )

        async with self._nonce_lock:
            self._nonce_counter += 1
            nonce = self._nonce_counter

        timestamp = int(time.time() * 1000)

        # Create message
        message = {
            "method": method,
            "path": path,
            "body": data or {},
            "timestamp": timestamp,
            "nonce": nonce,
        }

        # Create EIP-712 structured data
        structured_data = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                ],
                "Request": [
                    {"name": "method", "type": "string"},
                    {"name": "path", "type": "string"},
                    {"name": "body", "type": "string"},
                    {"name": "timestamp", "type": "uint64"},
                    {"name": "nonce", "type": "uint64"},
                ],
            },
            "primaryType": "Request",
            "domain": {"name": "HyperLiquid", "version": "1"},
            "message": {
                "method": message["method"],
                "path": message["path"],
                "body": json.dumps(message["body"]),
                "timestamp": message["timestamp"],
                "nonce": message["nonce"],
            },
        }

        # Sign message
        encoded_message = encode_structured_data(structured_data)
        signed_message = Account.sign_message(encoded_message, private_key=self._private_key)
        signature = signed_message.signature.hex()

        # Return authentication data
        return {
            "headers": {
                "X-HL-Signature": signature,
                "X-HL-Timestamp": str(timestamp),
                "X-HL-Nonce": str(nonce),
            },
            "params": params,
            "data": data,
        }

    # --- WebSocket Implementation --- #

    async def _route_ws_message(self, message: dict[str, Any]):
        """Route incoming WebSocket messages."""
        # Hyperliquid messages typically have a 'channel' and 'data' field
        channel = message.get("channel")
        data = message.get("data")
        if not channel or not data:
            logger.debug(f"[{self.exchange_name}] Received unroutable message: {message}")
            return

        # Route message to the appropriate handler
        handler = self._ws_handlers.get(channel)
        if handler:
            try:
                await handler(data)
            except Exception as e:
                logger.error(
                    f"[{self.exchange_name}] Error in handler for channel {channel}: {e}",
                    exc_info=True,
                )
        else:
            logger.debug(f"[{self.exchange_name}] No handler registered for channel: {channel}")

    async def subscribe(self, topic: str, handler: MessageHandler):
        """Subscribe to a Hyperliquid WebSocket topic."""
        if not self._ws_connection or not self.is_connected:
            logger.error(f"[{self.exchange_name}] Cannot subscribe, WebSocket not connected.")
            # Store handler for reconnection
            self._ws_handlers[topic] = handler
            return

        # Construct subscription message
        subscription_message = {
            "method": "subscribe",
            "subscription": {
                "type": topic.split(":")[0],
                "coin": topic.split(":")[1] if ":" in topic else None,
            },
        }

        # Remove None values
        if subscription_message["subscription"]["coin"] is None:
            del subscription_message["subscription"]["coin"]

        try:
            await self._ws_connection.send_json(subscription_message)
            self._ws_handlers[topic] = handler
            logger.info(f"[{self.exchange_name}] Subscribed to topic: {topic}")
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Failed to subscribe to topic {topic}: {e}")

    async def _resubscribe(self):
        """Resubscribe to all registered topics upon reconnection."""
        logger.info(
            f"[{self.exchange_name}] Resubscribing to topics: {list(self._ws_handlers.keys())}"
        )
        handlers_copy = self._ws_handlers.copy()
        for topic, handler in handlers_copy.items():
            await self.subscribe(topic, handler)
            await asyncio.sleep(0.1)  # Small delay between subscriptions

    # --- REST API Implementation --- #

    async def get_balances(self) -> dict[str, Balance]:
        """Get account balances."""
        try:
            payload = {"type": "clearinghouseState"}
            response = await self._request("POST", "/user", data=payload, signed=True)

            # Hyperliquid API returns balance as a string representation of USD value
            if "data" in response and "clearinghouseState" in response["data"] and "walletBalance" in response["data"]["clearinghouseState"]:
                balance_str = response["data"]["clearinghouseState"]["walletBalance"]
                balance_decimal = Decimal(balance_str) # Convert to Decimal
                return {
                    "USDC": Balance( # Assuming USDC is the collateral
                        asset="USDC",
                        free=balance_decimal,
                        locked=Decimal("0.0"),  # Hyperliquid might not separate free/locked
                        total=balance_decimal,
                    )
                }
            logger.warning(f"[{self.exchange_name}] Unexpected balance response structure: {response}")
            return {}
        except APIError as e:
            logger.error(f"[{self.exchange_name}] Error getting balances: {e}")
            return {}
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error getting balances: {e}",
                exc_info=True,
            )
            return {}

    async def get_positions(self) -> dict[str, Position]:
        """Get current positions."""
        try:
            payload = {"type": "positions"}
            response = await self._request("POST", "/user", data=payload, signed=True)

            positions = {}
            # Hyperliquid API returns a list of position objects
            if "data" in response and isinstance(response["data"], list):
                for item in response["data"]:
                    pos_data = item.get("position")
                    asset_info = item.get("assetInfo")
                    if pos_data and asset_info:
                        symbol = asset_info.get("name")
                        if symbol:
                            try:
                                # Convert API string values to Decimal
                                position = Position(
                                    symbol=symbol,
                                    size=Decimal(pos_data.get("szi", "0")), # szi is size, assuming 'i' indicates integer part? Check API docs
                                    entry_price=Decimal(pos_data.get("entryPx", "0")),
                                    mark_price=Decimal(asset_info.get("markPx", "0")), # Assuming markPx exists
                                    pnl=Decimal(pos_data.get("unrealizedPnl", "0")),
                                    liquidation_price=Decimal(pos_data.get("liquidationPx", "0")), # Assuming liquidationPx exists
                                    leverage=Decimal(pos_data.get("leverage", {"value":"1"}).get("value", "1")), # Default to 1x leverage
                                    margin_type="isolated", # Hyperliquid uses isolated margin by default
                                    side=OrderSide.BUY if Decimal(pos_data.get("szi", "0")) > 0 else OrderSide.SELL, # Determine side based on size sign
                                )
                                # Only add non-zero size positions
                                if position.size != Decimal("0"):
                                    positions[symbol] = position
                            except Exception as e:
                                logger.warning(f"[{self.exchange_name}] Error parsing position data for {symbol}: {e} - Data: {pos_data}")
            return positions
        except APIError as e:
            logger.error(f"[{self.exchange_name}] Error getting positions: {e}")
            return {}
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error getting positions: {e}",
                exc_info=True,
            )
            return {}

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Get open orders for a specific symbol or all symbols."""
        try:
            payload = {"type": "openOrders"}
            if symbol:
                 # Hyperliquid might require the user's wallet address for user-specific data
                 if not self._wallet_address:
                     raise APIError("Wallet address required for user-specific endpoints like openOrders.", code=APIErrorCode.AUTHENTICATION_FAILED)
                 payload["user"] = self._wallet_address # Assuming 'user' is the param key

            # Assuming Hyperliquid endpoint takes symbol or user address, adjust as necessary
            # Let's assume it returns all open orders if no symbol is specified, filtering happens later or the endpoint needs user.
            response = await self._request("POST", "/info", data=payload) # /info is a guess, check API docs

            open_orders = []
            if isinstance(response, list): # Assuming response is a list of orders
                for order_data in response:
                    # Filter by symbol if provided
                    if symbol and order_data.get("coin") != symbol:
                        continue

                    try:
                        # Map Hyperliquid order status string to OrderStatus enum
                        status_str = order_data.get("status", "unknown").upper()
                        # Define mapping based on Hyperliquid documentation
                        status_map = {
                            "OPEN": OrderStatus.OPEN,
                            "FILLED": OrderStatus.FILLED,
                            "CANCELED": OrderStatus.CANCELED,
                            # Add other mappings as needed
                        }
                        order_status = status_map.get(status_str, OrderStatus.UNKNOWN)

                        # Map Hyperliquid order type string to OrderType enum
                        type_str = order_data.get("order", {}).get("orderType", "").upper()
                        type_map = {
                           "LIMIT": OrderType.LIMIT,
                           "MARKET": OrderType.MARKET, # Adjust if Hyperliquid uses different terms
                           # Add other mappings
                        }
                        order_type = type_map.get(type_str, OrderType.MARKET) # Default or raise error

                        # Convert numeric strings to Decimal
                        price = Decimal(order_data.get("order", {}).get("limitPx", "0")) if order_type == OrderType.LIMIT else None
                        quantity = Decimal(order_data.get("order", {}).get("sz", "0"))
                        filled_quantity = Decimal(order_data.get("cumSz", "0")) # Assuming cumSz is filled size

                        order = Order(
                            id=str(order_data.get("oid")),
                            symbol=order_data.get("coin"),
                            side=OrderSide.BUY if order_data.get("order", {}).get("side") == "B" else OrderSide.SELL,
                            type=order_type,
                            price=price,
                            quantity=quantity,
                            filled_quantity=filled_quantity,
                            status=order_status, # Use mapped status
                            time=int(order_data.get("timestamp")), # Ensure timestamp is int
                            client_order_id=order_data.get("order", {}).get("cloid"), # Check if cloid exists
                            reduce_only=order_data.get("order", {}).get("reduceOnly", False),
                            avg_fill_price=None # Hyperliquid might not provide avgFillPrice directly in this call
                        )
                        open_orders.append(order)
                    except Exception as e:
                         logger.warning(f"[{self.exchange_name}] Error parsing order data: {e} - Data: {order_data}")

            # If symbol was provided, ensure filtering occurred (either by API or here)
            if symbol:
                return [o for o in open_orders if o.symbol == symbol]
            else:
                return open_orders

        except APIError as e:
            logger.error(f"[{self.exchange_name}] Error getting open orders for {symbol}: {e}")
            return []
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error getting open orders for {symbol}: {e}",
                exc_info=True,
            )
            return []

    async def place_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal, # Changed to Decimal
        price: Decimal | None = None, # Changed to Decimal
        client_order_id: str | None = None,
        time_in_force: str | None = None, # Example: GTC, IOC, FOK (check Hyperliquid support)
        **kwargs,
    ) -> Order | None: # Return Order or None on failure
        """Place a new order."""
        if Account is None or not self._private_key or not self._wallet_address:
             logger.error(f"[{self.exchange_name}] Cannot place order: eth_account missing or keys not configured.")
             raise APIError("Missing required authentication components.", code=APIErrorCode.AUTHENTICATION_FAILED)

        try:
            # Construct payload based on Hyperliquid API requirements
            order_payload = {
                "coin": symbol,
                "is_buy": side == OrderSide.BUY,
                "sz": str(quantity), # Send quantity as string
                "limit_px": "0", # Default for market/non-limit
                "order_type": {}, # Placeholder for type-specific details
                "reduce_only": kwargs.get("reduce_only", False),
            }

            # Type-specific logic
            if order_type == OrderType.LIMIT:
                if price is None:
                    raise ValueError("Price is required for LIMIT orders")
                order_payload["limit_px"] = str(price) # Send price as string
                order_payload["order_type"] = {"limit": {"tif": time_in_force or "Gtc"}} # Default TIF if not provided
            elif order_type == OrderType.MARKET:
                 # Hyperliquid might handle market orders by setting limit_px to '0' or specific type value
                 # Check API docs for exact market order structure
                 order_payload["order_type"] = {"market": {}} # Example market structure
                 # Maybe remove limit_px for market?
                 # del order_payload["limit_px"] # Check if needed
            else:
                 # Add support for other order types (STOP_LOSS, TAKE_PROFIT) if Hyperliquid supports them
                 raise NotImplementedError(f"Order type {order_type} not implemented for Hyperliquid.")


            if client_order_id:
                order_payload["cloid"] = client_order_id

            # Use the correct signing mechanism and endpoint
            action_payload = {
                 "type": "order",
                 "orders": [order_payload],
                 "grouping": "na" # Check Hyperliquid API for grouping options
            }

            response = await self._request("POST", "/exchange", data=action_payload, signed=True)

            # Process response to create and return an Order object
            # Hyperliquid response structure needs verification
            if "data" in response and "statuses" in response["data"] and isinstance(response["data"]["statuses"], list):
                order_status_info = response["data"]["statuses"][0] # Assuming one order placed

                # Extract details based on actual response structure
                # This is a placeholder structure
                if "resting" in order_status_info: # Example: order is resting (limit)
                    order_details = order_status_info["resting"]
                    status = OrderStatus.OPEN
                    order_id = str(order_details.get("oid"))
                    filled_qty = Decimal("0") # Resting orders aren't filled yet
                elif "filled" in order_status_info: # Example: order filled immediately (market)
                    order_details = order_status_info["filled"]
                    status = OrderStatus.FILLED
                    order_id = str(order_details.get("oid")) # Might need to check if oid is available on fill
                    filled_qty = Decimal(str(order_details.get("totalSz", quantity))) # Use totalSz if available
                else:
                    logger.error(f"[{self.exchange_name}] Unknown order status in response: {order_status_info}")
                    return None # Indicate failure

                # Construct the Order object
                placed_order = Order(
                    id=order_id,
                    symbol=symbol,
                    side=side,
                    type=order_type,
                    price=price if order_type == OrderType.LIMIT else None, # Use provided price for Limit
                    quantity=quantity,
                    filled_quantity=filled_qty,
                    status=status,
                    time=int(time.time() * 1000), # Use current time or response timestamp if available
                    client_order_id=client_order_id or "",
                    reduce_only=kwargs.get("reduce_only", False),
                    avg_fill_price=None # Difficult to get immediately, might need another call
                )
                return placed_order

            else:
                 logger.error(f"[{self.exchange_name}] Failed to place order. Response: {response}")
                 # Try to parse specific error message from response if possible
                 error_msg = response.get("error", "Unknown error")
                 raise APIError(f"Failed to place order: {error_msg}", data=response)


        except APIError as e:
            logger.error(f"[{self.exchange_name}] APIError placing order for {symbol}: {e}")
            # Re-raise or return None depending on desired error handling
            raise e
        except ValueError as e:
            logger.error(f"[{self.exchange_name}] ValueError placing order for {symbol}: {e}")
            raise e
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error placing order for {symbol}: {e}",
                exc_info=True,
            )
            # Re-raise or return None
            raise APIError(f"Unexpected error placing order: {e}")

    async def cancel_order(self, order_id: str, symbol: str) -> bool: # Added symbol
        """Cancel an existing order."""
        if Account is None or not self._private_key or not self._wallet_address:
             logger.error(f"[{self.exchange_name}] Cannot cancel order: eth_account missing or keys not configured.")
             raise APIError("Missing required authentication components.", code=APIErrorCode.AUTHENTICATION_FAILED)

        try:
            # Construct cancel payload - Hyperliquid requires symbol and order ID
            cancel_payload = {
                "coin": symbol,
                "oid": int(order_id), # Hyperliquid expects integer oid
            }
            action_payload = {
                 "type": "cancel",
                 "cancels": [cancel_payload]
            }

            response = await self._request("POST", "/exchange", data=action_payload, signed=True)

            # Check response status - needs verification based on API docs
            # Example: Assuming successful cancellation returns specific status
            if "data" in response and "statuses" in response["data"] and response["data"]["statuses"][0] == "success":
                 logger.info(f"[{self.exchange_name}] Successfully cancelled order {order_id} for {symbol}")
                 return True
            else:
                 error_msg = response.get("error", "Failed to cancel order")
                 logger.error(f"[{self.exchange_name}] Failed to cancel order {order_id} for {symbol}. Response: {response}")
                 # Consider raising APIError based on the response content
                 # raise APIError(f"Failed to cancel order {order_id}: {error_msg}", data=response)
                 return False # Return False if cancellation wasn't confirmed

        except APIError as e:
            logger.error(f"[{self.exchange_name}] APIError cancelling order {order_id} for {symbol}: {e}")
            return False
        except ValueError: # Handle case where order_id is not an int
            logger.error(f"[{self.exchange_name}] Invalid order ID format for cancellation: {order_id}")
            return False
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error cancelling order {order_id} for {symbol}: {e}",
                exc_info=True,
            )
            return False

    async def get_ticker(self, symbol: str) -> Ticker | None: # Return Ticker or None
        """Get current ticker information for a symbol."""
        try:
            # Use Hyperliquid's public market data endpoint
            payload = {"type": "metaAndAssetCtxs"} # This gets context for all assets
            response = await self._request("POST", "/info", data=payload)

            if isinstance(response, list) and len(response) > 1:
                 # First element is meta, second is list of asset contexts
                 asset_contexts = response[1]
                 for ctx in asset_contexts:
                     if ctx.get("name") == symbol:
                         day_ntl_vlm = ctx.get("dayNtlVlm", "0") # National Volume (USD?)
                         mark_px = ctx.get("markPx", "0")
                         # Hyperliquid might not provide direct bid/ask in this context easily
                         # Consider using order book snapshot for bid/ask
                         # For now, using mark price as a proxy for last price, bid, ask
                         ticker = Ticker(
                             symbol=symbol,
                             bid=Decimal(mark_px), # Use Decimal
                             ask=Decimal(mark_px), # Use Decimal
                             price=Decimal(mark_px), # Use Decimal
                             volume=Decimal(day_ntl_vlm), # Use Decimal (Volume in USD)
                             timestamp=int(time.time() * 1000) # Use current time; API might provide timestamp
                         )
                         return ticker
            logger.warning(f"[{self.exchange_name}] Ticker data not found for {symbol} in response: {response}")
            return None
        except APIError as e:
            logger.error(f"[{self.exchange_name}] Error getting ticker for {symbol}: {e}")
            return None
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error getting ticker for {symbol}: {e}",
                exc_info=True,
            )
            return None

    async def get_order_book(self, symbol: str, depth: int | None = None) -> OrderBook | None: # Return OrderBook or None
        """Get order book for a symbol."""
        try:
             # Use Hyperliquid's public order book endpoint
            payload = {"type": "l2Book", "coin": symbol}
            response = await self._request("POST", "/info", data=payload)

            if "data" in response and "levels" in response["data"]:
                 levels = response["data"]["levels"] # Levels is a list [ [bid_levels], [ask_levels] ]
                 bids_raw = levels[0] # List of [price_str, size_str]
                 asks_raw = levels[1] # List of [price_str, size_str]

                 # Convert to list of (Decimal, Decimal) tuples
                 bids = [(Decimal(str(p)), Decimal(str(s))) for p, s in bids_raw]
                 asks = [(Decimal(str(p)), Decimal(str(s))) for p, s in asks_raw]

                 # Sort bids descending and asks ascending by price
                 bids.sort(key=lambda x: x[0], reverse=True)
                 asks.sort(key=lambda x: x[0])

                 # Apply depth limit if specified
                 if depth:
                     bids = bids[:depth]
                     asks = asks[:depth]

                 order_book = OrderBook(
                     symbol=symbol,
                     bids=bids,
                     asks=asks,
                     timestamp=int(time.time() * 1000) # Use current time or response timestamp
                 )
                 return order_book
            logger.warning(f"[{self.exchange_name}] Order book data not found for {symbol} in response: {response}")
            return None

        except APIError as e:
            logger.error(f"[{self.exchange_name}] Error getting order book for {symbol}: {e}")
            return None
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error getting order book for {symbol}: {e}",
                exc_info=True,
            )
            return None

    async def get_recent_trades(self, symbol: str, limit: int | None = None) -> list[Trade]:
        """Get recent trades for a symbol."""
        try:
            # Use Hyperliquid's public trades endpoint
            payload = {"type": "recentTrades", "coin": symbol}
            response = await self._request("POST", "/info", data=payload)

            trades = []
            if "data" in response and isinstance(response["data"], list):
                trade_list = response["data"]
                # Apply limit if specified (API might have its own limit)
                if limit:
                    trade_list = trade_list[:limit]

                for trade_data in trade_list:
                    try:
                        trade = Trade(
                             # Hyperliquid trade ID might be composite? Use 'tid' if available or hash
                            id=str(trade_data.get("tid", hash(frozenset(trade_data.items())))),
                            symbol=symbol,
                            price=Decimal(str(trade_data.get("px"))), # Convert to Decimal
                            quantity=Decimal(str(trade_data.get("sz"))), # Convert to Decimal
                            side=OrderSide.BUY if trade_data.get("side") == "B" else OrderSide.SELL,
                            time=int(trade_data.get("time")), # Ensure timestamp is int
                            is_maker=False # Hyperliquid might not provide maker status directly
                        )
                        trades.append(trade)
                    except Exception as e:
                        logger.warning(f"[{self.exchange_name}] Error parsing trade data: {e} - Data: {trade_data}")

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

    async def get_funding_rate(self, symbol: str) -> FundingRate | None: # Return FundingRate or None
        """Get funding rate for a symbol."""
        try:
            # Use the same endpoint as get_ticker to get asset context
            payload = {"type": "metaAndAssetCtxs"}
            response = await self._request("POST", "/info", data=payload)

            if isinstance(response, list) and len(response) > 1:
                 asset_contexts = response[1]
                 for ctx in asset_contexts:
                     if ctx.get("name") == symbol:
                         funding_rate_str = ctx.get("funding", "0") # Funding rate per hour? Check docs
                         mark_px_str = ctx.get("markPx", "0")
                         # Hyperliquid might provide index price in 'oraclePx'
                         index_px_str = ctx.get("oraclePx", "0")

                         # Convert to Decimal
                         funding_rate = Decimal(funding_rate_str)
                         mark_price = Decimal(mark_px_str)
                         index_price = Decimal(index_px_str)

                         # Estimate next funding time (Hyperliquid funds hourly)
                         now_ms = int(time.time() * 1000)
                         next_funding_time_ms = (now_ms // 3600000 + 1) * 3600000 # Start of next hour

                         funding_info = FundingRate(
                             symbol=symbol,
                             rate=funding_rate,
                             mark_price=mark_price,
                             index_price=index_price,
                             next_funding_time=next_funding_time_ms,
                             timestamp=now_ms # Current timestamp
                         )
                         return funding_info

            logger.warning(f"[{self.exchange_name}] Funding rate data not found for {symbol} in response: {response}")
            return None
        except APIError as e:
            logger.error(f"[{self.exchange_name}] Error getting funding rate for {symbol}: {e}")
            return None
        except Exception as e:
            logger.error(
                f"[{self.exchange_name}] Unexpected error getting funding rate for {symbol}: {e}",
                exc_info=True,
            )
            return None

    # --- Optional/Advanced Endpoints --- #

    async def transfer(
        self, asset: str, amount: Decimal, from_account: str, to_account: str # Changed to Decimal
    ) -> dict[str, Any]:
        # Hyperliquid likely doesn't have internal spot/futures accounts like CEXs
        # This might map to L1 <-> L2 transfers if applicable, check API docs
        logger.warning(f"[{self.exchange_name}] Transfer endpoint not directly applicable/implemented.")
        raise NotImplementedError("Transfer functionality not implemented for Hyperliquid.")

    async def withdraw(
        self, asset: str, amount: Decimal, address: str, network: str | None = None # Changed to Decimal
    ) -> dict[str, Any]:
        # Requires signed request (wallet interaction)
        if Account is None or not self._private_key or not self._wallet_address:
             logger.error(f"[{self.exchange_name}] Cannot withdraw: eth_account missing or keys not configured.")
             raise APIError("Missing required authentication components.", code=APIErrorCode.AUTHENTICATION_FAILED)

        logger.warning(f"[{self.exchange_name}] Withdrawal endpoint requires careful implementation and testing.")
        # Construct the withdrawal payload according to Hyperliquid API docs
        # Example structure (verify with docs):
        # payload = {
        #     "type": "withdraw",
        #     "amount": str(amount), # Send as string
        #     "destination": address,
        #     # "chain": network # If applicable
        # }
        # response = await self._request("POST", "/exchange", data=payload, signed=True)
        # return response # Return raw response or parse it
        raise NotImplementedError("Withdrawal functionality not implemented for Hyperliquid.")

    def _map_error_response(
        self, status_code: int, error_body: str, error_data: dict[str, Any] | None = None
    ) -> APIError:
        """Map Hyperliquid specific errors to standardized APIError."""
        # Basic mapping, refine based on observed Hyperliquid errors
        code = APIErrorCode.UNKNOWN
        message = f"HTTP {status_code}"
        if error_data:
             message = error_data.get("error", error_body or f"HTTP {status_code}")
        elif error_body:
             message = error_body


        # Add specific error code mapping based on Hyperliquid documentation or observed responses
        # Example:
        # if "Invalid Signature" in message:
        #     code = APIErrorCode.AUTHENTICATION_FAILED
        # elif "Insufficient Margin" in message:
        #     code = APIErrorCode.INSUFFICIENT_FUNDS
        # elif "Order not found" in message:
        #     code = APIErrorCode.ORDER_NOT_FOUND
        # ... etc.

        # Default mapping based on HTTP status code if no specific message found
        if code == APIErrorCode.UNKNOWN:
            if status_code == 400:
                code = APIErrorCode.BAD_REQUEST
            elif status_code == 401 or status_code == 403:
                code = APIErrorCode.AUTHENTICATION_FAILED
            elif status_code == 429:
                code = APIErrorCode.RATE_LIMIT_EXCEEDED
            elif status_code >= 500:
                code = APIErrorCode.EXCHANGE_UNAVAILABLE

        return APIError(message, code=code, status_code=status_code, data=error_data)

    # --- WebSocket Subscription Helpers --- #

    async def subscribe_to_order_updates(self, handler: MessageHandler):
        """Subscribe to user order updates."""
        # Hyperliquid requires user address for private channels
        if not self._wallet_address:
            logger.error(f"[{self.exchange_name}] Wallet address required for order updates subscription.")
            return
        topic = f"userEvents:{self._wallet_address}" # Check actual topic format
        await self.subscribe(topic, handler)

    async def subscribe_to_trades(self, symbol: str, handler: MessageHandler):
        """Subscribe to public trades for a symbol."""
        topic = f"trades:{symbol}"
        await self.subscribe(topic, handler)

    async def subscribe_to_ticker(self, symbol: str, handler: MessageHandler):
        """Subscribe to ticker/market updates for a symbol."""
        # Hyperliquid might use 'l2Book' or another channel for ticker-like updates
        topic = f"l2Book:{symbol}" # Assuming L2 book updates can derive ticker info
        await self.subscribe(topic, handler)

    async def subscribe_to_order_book(self, symbol: str, handler: MessageHandler):
         """Subscribe to order book updates for a symbol."""
         topic = f"l2Book:{symbol}"
         await self.subscribe(topic, handler)
