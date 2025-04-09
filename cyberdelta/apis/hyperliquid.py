import asyncio
import base64
import json
import logging
import time
from typing import Any, Dict, List, Optional, Callable, Coroutine, Tuple
from datetime import datetime

from .base import ExchangeAPI, APIError, APIErrorCode, MessageHandler
from ..core.models import (
    Order, OrderBook, Ticker, Trade, Position, Balance, FundingRate, OrderType, OrderSide
)

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

    def __init__(self, api_config: Dict[str, Any], secrets: Dict[str, Optional[str]]):
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

    async def _authenticate(self, method: str, path: str, params: Optional[Dict] = None, data: Optional[Dict] = None) -> Dict[str, Any]:
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
        if not self._private_key or Account is None:
            raise APIError(
                "Private key not provided or eth_account not available",
                code=APIErrorCode.AUTHENTICATION_FAILED
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
            "nonce": nonce
        }
        
        # Create EIP-712 structured data
        structured_data = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"}
                ],
                "Request": [
                    {"name": "method", "type": "string"},
                    {"name": "path", "type": "string"},
                    {"name": "body", "type": "string"},
                    {"name": "timestamp", "type": "uint64"},
                    {"name": "nonce", "type": "uint64"}
                ]
            },
            "primaryType": "Request",
            "domain": {
                "name": "HyperLiquid",
                "version": "1"
            },
            "message": {
                "method": message["method"],
                "path": message["path"],
                "body": json.dumps(message["body"]),
                "timestamp": message["timestamp"],
                "nonce": message["nonce"]
            }
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
                "X-HL-Nonce": str(nonce)
            },
            "params": params,
            "data": data
        }

    # --- WebSocket Implementation --- #

    async def _route_ws_message(self, message: Dict[str, Any]):
        """Route incoming WebSocket messages."""
        # Hyperliquid messages typically have a 'channel' and 'data' field
        channel = message.get('channel')
        data = message.get('data')
        if not channel or not data:
            logger.debug(f"[{self.exchange_name}] Received unroutable message: {message}")
            return

        # Route message to the appropriate handler
        handler = self._ws_handlers.get(channel)
        if handler:
            try:
                await handler(data)
            except Exception as e:
                logger.error(f"[{self.exchange_name}] Error in handler for channel {channel}: {e}", exc_info=True)
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
                "type": topic.split(':')[0],
                "coin": topic.split(':')[1] if ':' in topic else None
            }
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
        logger.info(f"[{self.exchange_name}] Resubscribing to topics: {list(self._ws_handlers.keys())}")
        handlers_copy = self._ws_handlers.copy()
        for topic, handler in handlers_copy.items():
            await self.subscribe(topic, handler)
            await asyncio.sleep(0.1)  # Small delay between subscriptions

    # --- REST API Implementation --- #

    async def get_balances(self) -> Dict[str, Balance]:
        """Get account balances."""
        try:
            payload = {"type": "clearinghouseState"}
            response = await self._request("POST", "/user", data=payload, signed=True)
            
            if "data" in response and "walletBalanceUsd" in response["data"]:
                balance_usd = float(response["data"]["walletBalanceUsd"])
                return {
                    "USDC": Balance(
                        asset="USDC",
                        free=balance_usd,
                        locked=0.0,  # Hyperliquid doesn't separate free/locked
                        total=balance_usd
                    )
                }
            
            return {}
        except APIError as e:
            logger.error(f"[{self.exchange_name}] Error getting balances: {e}")
            return {}
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Unexpected error getting balances: {e}", exc_info=True)
            return {}

    async def get_positions(self) -> Dict[str, Position]:
        """Get current positions."""
        try:
            payload = {"type": "positions"}
            response = await self._request("POST", "/user", data=payload, signed=True)
            
            positions = {}
            if "data" in response:
                for position in response["data"]:
                    symbol = position.get("coin")
                    size = float(position.get("szi", 0))
                    entry_price = float(position.get("entryPx", 0))
                    mark_price = float(position.get("markPx", 0))
                    leverage = float(position.get("leverage", 1))
                    side = OrderSide.BUY if size > 0 else OrderSide.SELL
                    
                    # Calculate unrealized PnL
                    unrealized_pnl = 0.0
                    if "unrealizedPnl" in position:
                        unrealized_pnl = float(position["unrealizedPnl"])
                    
                    if size != 0:  # Only include non-zero positions
                        positions[symbol] = Position(
                            symbol=symbol,
                            size=abs(size),
                            entry_price=entry_price,
                            mark_price=mark_price,
                            leverage=leverage,
                            side=side,
                            unrealized_pnl=unrealized_pnl
                        )
            
            return positions
        except APIError as e:
            logger.error(f"[{self.exchange_name}] Error getting positions: {e}")
            return {}
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Unexpected error getting positions: {e}", exc_info=True)
            return {}

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """Get all open orders, optionally filtered by symbol."""
        try:
            payload = {"type": "openOrders"}
            response = await self._request("POST", "/user", data=payload, signed=True)
            
            orders = []
            if "data" in response:
                for order_data in response["data"]:
                    order_symbol = order_data.get("coin")
                    
                    # Filter by symbol if provided
                    if symbol and order_symbol != symbol:
                        continue
                    
                    side = OrderSide.BUY if order_data.get("side") == "B" else OrderSide.SELL
                    order_type = OrderType.LIMIT  # Hyperliquid mainly supports limit orders
                    
                    orders.append(Order(
                        id=str(order_data.get("oid", "")),
                        symbol=order_symbol,
                        side=side,
                        type=order_type,
                        price=float(order_data.get("limitPx", 0)),
                        quantity=float(order_data.get("sz", 0)),
                        filled_quantity=0.0,  # Not provided in open order data
                        status="OPEN",
                        time=int(order_data.get("time", 0)) if "time" in order_data else 0,
                        client_order_id=order_data.get("cloid", ""),
                        reduce_only=order_data.get("reduceOnly", False)
                    ))
            
            return orders
        except APIError as e:
            logger.error(f"[{self.exchange_name}] Error getting open orders: {e}")
            return []
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Unexpected error getting open orders: {e}", exc_info=True)
            return []

    async def place_order(self, symbol: str, side: OrderSide, order_type: OrderType,
                        quantity: float, price: Optional[float] = None,
                        client_order_id: Optional[str] = None, time_in_force: Optional[str] = None) -> Order:
        """Place a new order."""
        try:
            # Validate required parameters
            if order_type == OrderType.LIMIT and price is None:
                raise APIError("Price is required for limit orders", code=APIErrorCode.INVALID_PARAMS)
            
            # Construct order payload
            side_code = "B" if side == OrderSide.BUY else "A"
            order_data = {
                "coin": symbol,
                "side": side_code,
                "sz": str(quantity),
                "reduceOnly": False,
                "cloid": client_order_id or str(int(time.time() * 1000))  # Use timestamp as default client ID
            }
            
            if order_type == OrderType.LIMIT:
                order_data["limitPx"] = str(price)
                order_data["tif"] = time_in_force.lower() if time_in_force else "gtc"
            
            payload = {
                "type": "order",
                "order": order_data
            }
            
            # Send order
            response = await self._request("POST", "/exchange", data=payload, signed=True)
            
            # Extract order ID and check for success
            if "data" in response and "statuses" in response["data"]:
                status = response["data"]["statuses"][0]
                
                if "error" in status and status["error"]:
                    raise APIError(f"Order error: {status['error']}", code=APIErrorCode.INVALID_PARAMS)
                
                order_id = str(status.get("oid", ""))
                is_resting = status.get("resting", False)
                
                # Create Order object
                order = Order(
                    id=order_id,
                    client_order_id=order_data["cloid"],
                    symbol=symbol,
                    side=side,
                    type=order_type,
                    price=price or 0.0,
                    quantity=quantity,
                    filled_quantity=0.0 if is_resting else quantity,  # Assume filled if not resting
                    status="OPEN" if is_resting else "FILLED",
                    time=int(time.time() * 1000)
                )
                
                logger.info(f"[{self.exchange_name}] Order placed: {order.id} for {symbol}")
                return order
            
            raise APIError("Unexpected response format", code=APIErrorCode.SERVER_ERROR)
        except APIError:
            raise
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Error placing order: {e}", exc_info=True)
            raise APIError(f"Error placing order: {e}", code=APIErrorCode.UNKNOWN)

    async def cancel_order(self, order_id: str, symbol: Optional[str] = None) -> bool:
        """Cancel an existing order."""
        try:
            if not symbol:
                raise APIError("Symbol is required for Hyperliquid cancel orders", code=APIErrorCode.INVALID_PARAMS)
            
            payload = {
                "type": "cancel",
                "cancel": {
                    "coin": symbol,
                    "oid": int(order_id)
                }
            }
            
            response = await self._request("POST", "/exchange", data=payload, signed=True)
            
            if "data" in response and "statuses" in response["data"]:
                status = response["data"]["statuses"][0]
                success = not status.get("err")
                
                if success:
                    logger.info(f"[{self.exchange_name}] Order {order_id} cancelled successfully")
                    return True
                else:
                    logger.warning(f"[{self.exchange_name}] Failed to cancel order {order_id}: {status.get('err')}")
                    return False
            
            return False
        except APIError as e:
            logger.error(f"[{self.exchange_name}] Error cancelling order: {e}")
            return False
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Unexpected error cancelling order: {e}", exc_info=True)
            return False

    async def get_ticker(self, symbol: str) -> Ticker:
        """Get ticker information for a symbol."""
        try:
            payload = {"type": "indexPrice", "coin": symbol}
            response = await self._request("POST", "/info", data=payload)
            
            if "data" in response and "price" in response["data"]:
                price = float(response["data"]["price"])
                
                # Get additional market data for more ticker info
                market_payload = {"type": "metaAndAssetCtxs"}
                market_response = await self._request("POST", "/info", data=market_payload)
                
                # Find the asset in the response
                asset_ctx = None
                if "data" in market_response and "assetCtxs" in market_response["data"]:
                    for asset in market_response["data"]["assetCtxs"]:
                        if asset.get("name") == symbol:
                            asset_ctx = asset
                            break
                
                bid = price
                ask = price
                volume = 0.0
                
                # Extract additional data if available
                if asset_ctx:
                    if "midPrice" in asset_ctx:
                        # Use midPrice from asset context
                        price = float(asset_ctx["midPrice"])
                    
                    # Some exchanges provide bid/ask spread
                    if "bidPrice" in asset_ctx:
                        bid = float(asset_ctx["bidPrice"])
                    if "askPrice" in asset_ctx:
                        ask = float(asset_ctx["askPrice"])
                    
                    # Extract volume if available
                    if "volume24H" in asset_ctx:
                        volume = float(asset_ctx["volume24H"])
                
                return Ticker(
                    symbol=symbol,
                    price=price,
                    bid=bid,
                    ask=ask,
                    volume=volume,
                    timestamp=int(time.time() * 1000)
                )
            
            raise APIError(f"Failed to get ticker for {symbol}", code=APIErrorCode.SERVER_ERROR)
        except APIError:
            raise
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Error getting ticker: {e}", exc_info=True)
            raise APIError(f"Error getting ticker: {e}", code=APIErrorCode.UNKNOWN)

    async def get_order_book(self, symbol: str, depth: Optional[int] = None) -> OrderBook:
        """Get order book for a symbol."""
        try:
            payload = {"type": "l2Book", "coin": symbol}
            response = await self._request("POST", "/info", data=payload)
            
            if "data" in response and "levels" in response["data"]:
                levels = response["data"]["levels"]
                bids = []
                asks = []
                
                for level in levels:
                    price = float(level.get("px", 0))
                    size = float(level.get("sz", 0))
                    
                    if level.get("isBid"):
                        bids.append((price, size))
                    else:
                        asks.append((price, size))
                
                # Sort and limit depth if specified
                bids.sort(key=lambda x: -x[0])  # Sort bids descending by price
                asks.sort(key=lambda x: x[0])   # Sort asks ascending by price
                
                if depth:
                    bids = bids[:depth]
                    asks = asks[:depth]
                
                return OrderBook(
                    symbol=symbol,
                    bids=bids,
                    asks=asks,
                    timestamp=int(time.time() * 1000)
                )
            
            raise APIError(f"Failed to get order book for {symbol}", code=APIErrorCode.SERVER_ERROR)
        except APIError:
            raise
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Error getting order book: {e}", exc_info=True)
            raise APIError(f"Error getting order book: {e}", code=APIErrorCode.UNKNOWN)

    async def get_recent_trades(self, symbol: str, limit: Optional[int] = None) -> List[Trade]:
        """Get recent trades for a symbol."""
        try:
            payload = {"type": "recentTrades", "coin": symbol}
            response = await self._request("POST", "/info", data=payload)
            
            trades = []
            if "data" in response and "trades" in response["data"]:
                for trade_data in response["data"]["trades"]:
                    side = OrderSide.BUY if trade_data.get("side") == "B" else OrderSide.SELL
                    trade_id = trade_data.get("tid", "")
                    
                    trades.append(Trade(
                        id=str(trade_id),
                        symbol=symbol,
                        price=float(trade_data.get("px", 0)),
                        quantity=float(trade_data.get("sz", 0)),
                        time=int(trade_data.get("time", 0)),
                        side=side
                    ))
            
            # Apply limit if specified
            if limit and len(trades) > limit:
                trades = trades[:limit]
            
            return trades
        except APIError as e:
            logger.error(f"[{self.exchange_name}] Error getting recent trades: {e}")
            return []
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Unexpected error getting recent trades: {e}", exc_info=True)
            return []

    async def get_funding_rate(self, symbol: str) -> FundingRate:
        """Get current funding rate for a perpetual contract."""
        try:
            # Get funding rate data
            funding_payload = {"type": "fundingRate", "coin": symbol}
            funding_response = await self._request("POST", "/info", data=funding_payload)
            
            # Get mark price and index price
            price_payload = {"type": "metaAndAssetCtxs"}
            price_response = await self._request("POST", "/info", data=price_payload)
            
            funding_rate = 0.0
            predicted_rate = 0.0
            mark_price = 0.0
            index_price = 0.0
            next_funding_time = 0
            
            # Extract funding rate
            if "data" in funding_response and "fundingRate" in funding_response["data"]:
                funding_rate = float(funding_response["data"]["fundingRate"])
                predicted_rate = funding_rate  # Use current rate as prediction if no specific prediction is available
            
            # Find asset in meta response to get prices
            if "data" in price_response and "assetCtxs" in price_response["data"]:
                for asset in price_response["data"]["assetCtxs"]:
                    if asset.get("name") == symbol:
                        # Mark price
                        if "markPx" in asset:
                            mark_price = float(asset["markPx"])
                        
                        # Index/Oracle price
                        if "oraclePx" in asset:
                            index_price = float(asset["oraclePx"])
                        
                        # Find next funding time if available
                        if "nextFundingTime" in asset:
                            next_funding_time = int(asset["nextFundingTime"])
                        else:
                            # Calculate approximate next funding time (hourly funding)
                            current_time = int(time.time() * 1000)
                            minutes_to_hour = 60 - (datetime.fromtimestamp(current_time / 1000).minute)
                            next_funding_time = current_time + (minutes_to_hour * 60 * 1000)
                        
                        break
            
            # Create FundingRate object
            return FundingRate(
                symbol=symbol,
                funding_rate=funding_rate,
                predicted_rate=predicted_rate,
                mark_price=mark_price,
                index_price=index_price,
                next_funding_time=next_funding_time
            )
        except APIError as e:
            logger.error(f"[{self.exchange_name}] Error getting funding rate: {e}")
            raise
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Unexpected error getting funding rate: {e}", exc_info=True)
            raise APIError(f"Unexpected error getting funding rate: {e}", code=APIErrorCode.UNKNOWN)

    async def transfer(self, asset: str, amount: float, from_account: str, to_account: str) -> Dict[str, Any]:
        # Hyperliquid likely doesn't have internal spot/futures accounts like CEXs
        logger.warning(f"[{self.exchange_name}] internal transfer may not be applicable.")
        raise NotImplementedError("Internal transfers may not apply to Hyperliquid.")

    async def withdraw(self, asset: str, amount: float, address: str, network: Optional[str] = None) -> Dict[str, Any]:
        # Requires signed request (wallet interaction)
        logger.warning(f"[{self.exchange_name}] withdraw not implemented.")
        raise NotImplementedError

    # --- Helper Methods Specific to Hyperliquid --- #
    # e.g., methods for wallet interaction/signing if not using a library

    def _map_error_response(
        self, 
        status_code: int, 
        error_body: str,
        error_data: Dict[str, Any]
    ) -> APIError:
        """
        Map Hyperliquid-specific error responses to standardized APIError.
        
        Args:
            status_code: HTTP status code
            error_body: Raw error response body
            error_data: Parsed error data (if JSON)
            
        Returns:
            Standardized APIError
        """
        # Start with default mapping from base class
        code = APIErrorCode.UNKNOWN
        message = "Unknown error"
        exchange_code = None
        
        # Hyperliquid errors are typically nested in data->statuses->[0]->err or error field
        if isinstance(error_data, dict):
            # Case 1: Exchange endpoint statuses format
            if "data" in error_data and "statuses" in error_data["data"] and error_data["data"]["statuses"]:
                status = error_data["data"]["statuses"][0]
                err_message = status.get("err") or status.get("error")
                if err_message:
                    message = err_message
                    exchange_code = "STATUS_ERROR"
                    
                    # Map common Hyperliquid errors to standardized codes
                    if "insufficient margin" in err_message.lower() or "insufficient balance" in err_message.lower():
                        code = APIErrorCode.INSUFFICIENT_FUNDS
                    elif "position does not exist" in err_message.lower():
                        code = APIErrorCode.ORDER_NOT_FOUND
                    elif "coin not found" in err_message.lower() or "invalid coin" in err_message.lower():
                        code = APIErrorCode.SYMBOL_NOT_FOUND
                    elif "invalid order id" in err_message.lower() or "order not found" in err_message.lower():
                        code = APIErrorCode.ORDER_NOT_FOUND
                    elif "price out of range" in err_message.lower():
                        code = APIErrorCode.PRICE_OUT_OF_RANGE
                    elif "size too small" in err_message.lower() or "size too large" in err_message.lower():
                        code = APIErrorCode.QUANTITY_OUT_OF_RANGE
                    elif "liquidation" in err_message.lower():
                        code = APIErrorCode.LIQUIDATION_IN_PROGRESS
                    elif "precision" in err_message.lower():
                        code = APIErrorCode.PRECISION_ERROR
                    elif "max position" in err_message.lower() or "position limit" in err_message.lower():
                        code = APIErrorCode.MAX_POSITION_EXCEEDED
                    elif "already exists" in err_message.lower():
                        code = APIErrorCode.DUPLICATE_ORDER
                    else:
                        code = APIErrorCode.INVALID_PARAMS
            
            # Case 2: Direct error message
            elif "error" in error_data or "message" in error_data:
                message = error_data.get("error") or error_data.get("message", "Unknown error")
                exchange_code = error_data.get("code")
                
                # Map common Hyperliquid errors
                if "rate limit" in message.lower():
                    code = APIErrorCode.RATE_LIMITED
                elif "authentication" in message.lower() or "signature" in message.lower():
                    code = APIErrorCode.AUTHENTICATION_FAILED
                elif "maintenance" in message.lower():
                    code = APIErrorCode.MAINTENANCE
                else:
                    code = APIErrorCode.SERVER_ERROR
                
        # Default HTTP status code based mapping as fallback
        if code == APIErrorCode.UNKNOWN:
            if status_code == 401 or status_code == 403:
                code = APIErrorCode.AUTHENTICATION_FAILED
            elif status_code == 429:
                code = APIErrorCode.RATE_LIMITED
            elif status_code == 400:
                code = APIErrorCode.INVALID_PARAMS
            elif status_code >= 500:
                code = APIErrorCode.SERVER_ERROR
        
        return APIError(
            message=f"Hyperliquid API error: {message}",
            code=code,
            http_status=status_code,
            exchange_code=exchange_code,
            exchange_message=message
        )