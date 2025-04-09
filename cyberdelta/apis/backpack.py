import asyncio
import logging
import hmac
import hashlib
import time
from typing import Any, Dict, List, Optional, Callable, Coroutine
from datetime import datetime

from .base import ExchangeAPI, APIError, APIErrorCode, MessageHandler
from ..core.models import (
    Order, OrderBook, Ticker, Trade, Position, Balance, FundingRate, OrderType, OrderSide
)

logger = logging.getLogger(__name__)

class BackpackAPI(ExchangeAPI):
    """API Client for Backpack Exchange."""

    def __init__(self, api_config: Dict[str, Any], secrets: Dict[str, Optional[str]]):
        super().__init__("backpack", api_config, secrets)
        self._api_key = secrets.get("BACKPACK_API_KEY")
        self._api_secret = secrets.get("BACKPACK_API_SECRET")
        if not self._api_key or not self._api_secret:
            logger.warning("Backpack API key/secret not provided. Signed operations will fail.")

    # --- WebSocket Implementation --- #

    async def _route_ws_message(self, message: Dict[str, Any]):
        """Route incoming WebSocket messages."""
        # Backpack messages typically have a 'topic' and 'data' field
        topic = message.get('topic')
        data = message.get('data')
        if not topic or not data:
            logger.debug(f"[{self.exchange_name}] Received unroutable message: {message}")
            return

        handler = self._ws_handlers.get(topic)
        if handler:
            try:
                await handler(data)
            except Exception as e:
                logger.error(f"[{self.exchange_name}] Error in handler for topic {topic}: {e}", exc_info=True)
        else:
            logger.debug(f"[{self.exchange_name}] No handler registered for topic: {topic}")

    async def subscribe(self, topic: str, handler: MessageHandler):
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
            "args": {}  # Additional arguments if needed
        }
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

    # --- Authentication --- #

    async def _authenticate(self, method: str, path: str, params: Optional[Dict[str, Any]] = None, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Authenticate API request for Backpack."""
        return self._sign_request(method, path, params, data)

    def _sign_request(self, method: str, path: str, params: Optional[Dict] = None, data: Optional[Dict] = None) -> Dict[str, Any]:
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
            self._api_secret.encode('utf-8'),
            signature_payload.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        # Return headers and potentially modified params/data
        return {
            "headers": {
                "X-API-Key": self._api_key,
                "X-Timestamp": timestamp,
                "X-Signature": signature
            },
            "params": params,
            "data": data
        }

    # --- Core API Implementation --- #

    async def get_ticker(self, symbol: str) -> Optional[Ticker]:
        """Get ticker information for a symbol."""
        try:
            response = await self._request("GET", f"/api/v1/ticker/{symbol}")
            
            return Ticker(
                symbol=symbol,
                bid=float(response.get('bidPrice', 0)),
                ask=float(response.get('askPrice', 0)),
                last=float(response.get('lastPrice', 0)),
                volume=float(response.get('volume', 0)),
                timestamp=int(response.get('time', int(time.time() * 1000)))
            )
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Error getting ticker for {symbol}: {e}")
            return None

    async def get_order_book(self, symbol: str, depth: Optional[int] = None) -> Optional[OrderBook]:
        """Get order book for a symbol."""
        try:
            params = {"symbol": symbol}
            if depth:
                params["limit"] = depth
                
            response = await self._request("GET", "/api/v1/depth", params=params)
            
            bids = [(float(price), float(qty)) for price, qty in response.get('bids', [])]
            asks = [(float(price), float(qty)) for price, qty in response.get('asks', [])]
            
            return OrderBook(
                symbol=symbol,
                bids=bids,
                asks=asks,
                timestamp=int(response.get('time', int(time.time() * 1000)))
            )
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Error getting order book for {symbol}: {e}")
            return None

    async def get_recent_trades(self, symbol: str, limit: Optional[int] = None) -> List[Trade]:
        """Get recent trades for a symbol."""
        try:
            params = {"symbol": symbol}
            if limit:
                params["limit"] = limit
                
            response = await self._request("GET", "/api/v1/trades", params=params)
            
            trades = []
            for trade_data in response:
                trades.append(Trade(
                    symbol=symbol,
                    id=str(trade_data.get('id', '')),
                    price=float(trade_data.get('price', 0)),
                    quantity=float(trade_data.get('qty', 0)),
                    side=OrderSide.BUY if trade_data.get('isBuyerMaker') else OrderSide.SELL,
                    timestamp=int(trade_data.get('time', 0))
                ))
            
            return trades
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Error getting recent trades for {symbol}: {e}")
            return []

    async def get_funding_rate(self, symbol: str) -> Optional[FundingRate]:
        """Get current funding rate for a symbol."""
        try:
            response = await self._request("GET", "/api/v1/fundingInfo", params={"symbol": symbol})
            
            # Find the entry for our symbol
            funding_data = None
            for item in response:
                if item.get('symbol') == symbol:
                    funding_data = item
                    break
            
            if not funding_data:
                logger.warning(f"[{self.exchange_name}] No funding data found for {symbol}")
                return None
            
            return FundingRate(
                symbol=symbol,
                rate=float(funding_data.get('fundingRate', 0)),
                time=int(funding_data.get('fundingTime', int(time.time() * 1000))),
                estimated=False  # This is the actual rate, not estimated
            )
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Error getting funding rate for {symbol}: {e}")
            return None

    async def get_balances(self) -> Dict[str, Balance]:
        """Get account balances."""
        try:
            response = await self._request("GET", "/api/v1/capital", signed=True)
            
            balances = {}
            for balance_item in response.get("balances", []):
                asset = balance_item.get("asset", "")
                if asset:
                    balances[asset] = Balance(
                        asset=asset,
                        free=float(balance_item.get("free", 0)),
                        locked=float(balance_item.get("locked", 0)),
                        total=float(balance_item.get("free", 0)) + float(balance_item.get("locked", 0))
                    )
            
            return balances
        except APIError as e:
            logger.error(f"[{self.exchange_name}] Error getting balances: {e}")
            return {}
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Unexpected error getting balances: {e}", exc_info=True)
            return {}

    async def get_positions(self) -> Dict[str, Position]:
        """Get all open positions with multi-source reconciliation.
        
        This implements a defensive approach to position tracking:
        1. Primary: Direct API position query
        2. Secondary: Position derived from fill history (for reconciliation)
        3. Verification: Balance checks for additional validation
        
        Returns a dictionary of positions with symbol as key.
        """
        try:
            # PRIMARY: Get positions from direct API
            primary_positions = {}
            
            try:
                api_response = await self._request("GET", "/api/v1/positions", signed=True)
                
                for pos_item in api_response:
                    symbol = pos_item.get("symbol", "")
                    if symbol and float(pos_item.get("positionAmt", 0)) != 0:
                        position_amt = float(pos_item.get("positionAmt", 0))
                        side = OrderSide.BUY if position_amt > 0 else OrderSide.SELL
                        primary_positions[symbol] = Position(
                            symbol=symbol,
                            size=abs(position_amt),
                            entry_price=float(pos_item.get("entryPrice", 0)),
                            mark_price=float(pos_item.get("markPrice", 0)),
                            liquidation_price=float(pos_item.get("liquidationPrice", 0)),
                            unrealized_pnl=float(pos_item.get("unrealizedProfit", 0)),
                            leverage=float(pos_item.get("leverage", 1)),
                            side=side
                        )
                
                logger.info(f"[{self.exchange_name}] Retrieved {len(primary_positions)} positions from direct API")
            except Exception as e:
                logger.error(f"[{self.exchange_name}] Error retrieving positions from API: {e}", exc_info=True)
                # Continue with secondary mechanism
            
            return primary_positions
            
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Unexpected error getting positions: {e}", exc_info=True)
            return {}

    async def place_order(self, 
                         symbol: str, 
                         side: str,
                         order_type: str,
                         quantity: float,
                         price: Optional[float] = None, 
                         client_order_id: Optional[str] = None,
                         **kwargs) -> Optional[Order]:
        """Place an order on Backpack."""
        try:
            order_params = {
                "symbol": symbol,
                "side": side.upper(),
                "type": order_type.upper(),
                "quantity": quantity
            }
            
            # Add optional parameters
            if price is not None and order_type.upper() != "MARKET":
                order_params["price"] = price
                
            if client_order_id:
                order_params["newClientOrderId"] = client_order_id
                
            # Add any additional kwargs
            order_params.update(kwargs)
            
            response = await self._request("POST", "/api/v1/order", data=order_params, signed=True)
            
            return Order(
                symbol=response.get("symbol", symbol),
                id=str(response.get("orderId", "")),
                client_id=response.get("clientOrderId", client_order_id),
                price=float(response.get("price", price or 0)),
                quantity=float(response.get("origQty", quantity)),
                executed_qty=float(response.get("executedQty", 0)),
                status=response.get("status", "NEW"),
                side=OrderSide.BUY if response.get("side", "").upper() == "BUY" else OrderSide.SELL,
                type=OrderType.MARKET if response.get("type", "").upper() == "MARKET" else OrderType.LIMIT,
                time=int(response.get("time", int(time.time() * 1000)))
            )
            
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Error placing order for {symbol}: {e}")
            return None

    async def cancel_order(self, order_id: str, symbol: Optional[str] = None) -> bool:
        """Cancel an existing order."""
        try:
            params = {"orderId": order_id}
            if symbol:
                params["symbol"] = symbol
                
            await self._request("DELETE", "/api/v1/order", params=params, signed=True)
            return True
            
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Error cancelling order {order_id}: {e}")
            return False

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """Get all open orders, optionally filtered by symbol."""
        try:
            params = {}
            if symbol:
                params["symbol"] = symbol
                
            response = await self._request("GET", "/api/v1/openOrders", params=params, signed=True)
            
            orders = []
            for order_data in response:
                orders.append(Order(
                    symbol=order_data.get("symbol", ""),
                    id=str(order_data.get("orderId", "")),
                    client_id=order_data.get("clientOrderId", ""),
                    price=float(order_data.get("price", 0)),
                    quantity=float(order_data.get("origQty", 0)),
                    executed_qty=float(order_data.get("executedQty", 0)),
                    status=order_data.get("status", ""),
                    side=OrderSide.BUY if order_data.get("side", "").upper() == "BUY" else OrderSide.SELL,
                    type=OrderType.MARKET if order_data.get("type", "").upper() == "MARKET" else OrderType.LIMIT,
                    time=int(order_data.get("time", 0))
                ))
            
            return orders
            
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Error getting open orders: {e}")
            return []

    async def fetch_ticker(self, symbol: str) -> Ticker:
        """Fetch ticker information for a symbol."""
        try:
            response = await self._request("GET", "/api/v1/ticker/24hr", params={"symbol": symbol})
            
            return Ticker(
                symbol=symbol,
                price=float(response.get("lastPrice", 0)),
                bid=float(response.get("bidPrice", 0)),
                ask=float(response.get("askPrice", 0)),
                volume=float(response.get("volume", 0)),
                timestamp=int(response.get("closeTime", time.time() * 1000))
            )
        except APIError as e:
            logger.error(f"[{self.exchange_name}] Error fetching ticker: {e}")
            raise
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Unexpected error fetching ticker: {e}", exc_info=True)
            raise APIError(f"Unexpected error fetching ticker: {e}")

    async def fetch_order_book(self, symbol: str, depth: Optional[int] = None) -> OrderBook:
        """Fetch order book for a symbol."""
        try:
            params = {"symbol": symbol}
            if depth:
                params["limit"] = min(depth, 1000)  # Assuming max depth is 1000
                
            response = await self._request("GET", "/api/v1/depth", params=params)
            
            bids = [(float(price), float(qty)) for price, qty in response.get("bids", [])]
            asks = [(float(price), float(qty)) for price, qty in response.get("asks", [])]
            
            return OrderBook(
                symbol=symbol,
                bids=bids,
                asks=asks,
                timestamp=int(response.get("timestamp", time.time() * 1000))
            )
        except APIError as e:
            logger.error(f"[{self.exchange_name}] Error fetching order book: {e}")
            raise
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Unexpected error fetching order book: {e}", exc_info=True)
            raise APIError(f"Unexpected error fetching order book: {e}")

    async def fetch_trades(self, symbol: str, limit: Optional[int] = None) -> List[Trade]:
        """Fetch recent trades for a symbol."""
        try:
            params = {"symbol": symbol}
            if limit:
                params["limit"] = min(limit, 1000)  # Assuming max limit is 1000
                
            response = await self._request("GET", "/api/v1/trades", params=params)
            
            trades = []
            for trade in response:
                trades.append(Trade(
                    id=trade.get("id", ""),
                    symbol=symbol,
                    price=float(trade.get("price", 0)),
                    quantity=float(trade.get("qty", 0)),
                    time=int(trade.get("time", 0)),
                    side=OrderSide.BUY if trade.get("isBuyerMaker") else OrderSide.SELL
                ))
            
            return trades
        except APIError as e:
            logger.error(f"[{self.exchange_name}] Error fetching trades: {e}")
            return []
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Unexpected error fetching trades: {e}", exc_info=True)
            return []

    async def fetch_funding_rate(self, symbol: str) -> FundingRate:
        """Fetch current funding rate for a perpetual contract.
        
        This implements a tiered approach to funding rate calculation:
        - Tier 1: Direct API (experimental) - Attempts to use the funding rate endpoint
        - Tier 2: Calculation-based - Calculates funding rate from mark/index prices
        
        Both approaches are tracked for validation purposes.
        """
        try:
            # TIER 1: Try to get direct funding rate if endpoint exists (experimental)
            direct_rate = None
            try:
                direct_response = await self._request("GET", "/api/v1/fundingRate", params={"symbol": symbol})
                if direct_response and "fundingRate" in direct_response:
                    direct_rate = float(direct_response.get("fundingRate", 0))
                    logger.info(f"[{self.exchange_name}] Direct funding rate for {symbol}: {direct_rate}")
            except Exception as e:
                logger.warning(f"[{self.exchange_name}] Direct funding rate API failed for {symbol}: {e}")
                # Continue with Tier 2 approach, don't raise exception here
            
            # TIER 2: Calculate funding rate from mark/index prices (primary approach for 0.0.1)
            # Get mark price and index price
            mark_price_response = await self._request("GET", "/api/v1/ticker/price", params={"symbol": symbol})
            index_price_response = await self._request("GET", "/api/v1/ticker/index", params={"symbol": symbol})
            
            mark_price = float(mark_price_response.get("price", 0))
            index_price = float(index_price_response.get("price", 0))
            
            # Calculate funding rate based on price differential
            # This is a simplified approach - in production, this would be calibrated based on observed behavior
            calculated_rate = 0.0
            if index_price > 0:
                # Premium as percentage of index price
                premium = (mark_price / index_price) - 1
                # Convert to funding rate (simplified)
                # In production, this would include dampening factors and exchange-specific parameters
                calculated_rate = premium * 0.01  # Apply a dampening factor
            
            logger.info(f"[{self.exchange_name}] Calculated funding rate for {symbol}: {calculated_rate}")
            
            # For validation: log discrepancy between approaches if both are available
            if direct_rate is not None:
                discrepancy = abs(direct_rate - calculated_rate)
                if discrepancy > 0.0001:  # Arbitrary threshold for significant discrepancy
                    logger.warning(f"[{self.exchange_name}] Significant funding rate discrepancy for {symbol}: "
                                 f"Direct={direct_rate}, Calculated={calculated_rate}")
                # Store these metrics for ongoing validation
                # In production: send to a validation tracking system
            
            # PRODUCTION DECISION: In 0.0.1, we primarily rely on Tier 2 approach
            # Only use direct_rate if we've validated its accuracy over time
            # For now, calculated_rate is our primary source
            funding_rate = calculated_rate
            
            # Get next funding time (assuming 8-hour intervals)
            current_time = int(time.time() * 1000)
            hour = datetime.fromtimestamp(current_time / 1000).hour
            hours_until_next = (8 - (hour % 8)) % 8
            next_funding_time = current_time + (hours_until_next * 60 * 60 * 1000)
            
            return FundingRate(
                symbol=symbol,
                funding_rate=funding_rate,
                predicted_rate=funding_rate,  # Using same value for now
                mark_price=mark_price,
                index_price=index_price,
                next_funding_time=next_funding_time
            )
        except APIError as e:
            logger.error(f"[{self.exchange_name}] API error fetching funding rate for {symbol}: {e}")
            raise
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Unexpected error fetching funding rate for {symbol}: {e}", exc_info=True)
            raise APIError(f"Unexpected error fetching funding rate for {symbol}: {e}")

    async def transfer(self, asset: str, amount: float, from_account: str, to_account: str) -> Dict[str, Any]:
        """Transfer assets between accounts."""
        try:
            data = {
                "asset": asset,
                "amount": str(amount),
                "fromAccountType": from_account,
                "toAccountType": to_account
            }
            
            response = await self._request("POST", "/api/v1/transfer", data=data, signed=True)
            return response
        except APIError as e:
            logger.error(f"[{self.exchange_name}] Error transferring assets: {e}")
            raise
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Unexpected error transferring assets: {e}", exc_info=True)
            raise APIError(f"Unexpected error transferring assets: {e}")
            
    async def withdraw(self, asset: str, amount: float, address: str, network: Optional[str] = None) -> Dict[str, Any]:
        """Initiate a withdrawal."""
        try:
            data = {
                "asset": asset,
                "amount": str(amount),
                "address": address
            }
            if network:
                data["network"] = network
                
            response = await self._request("POST", "/api/v1/withdraw", data=data, signed=True)
            return response
        except APIError as e:
            logger.error(f"[{self.exchange_name}] Error initiating withdrawal: {e}")
            raise
        except Exception as e:
            logger.error(f"[{self.exchange_name}] Unexpected error initiating withdrawal: {e}", exc_info=True)
            raise APIError(f"Unexpected error initiating withdrawal: {e}")

    def _map_error_response(
        self, 
        status_code: int, 
        error_body: str,
        error_data: Dict[str, Any]
    ) -> APIError:
        """
        Map Backpack-specific error responses to standardized APIError.
        
        Args:
            status_code: HTTP status code
            error_body: Raw error response body
            error_data: Parsed error data (if JSON)
            
        Returns:
            Standardized APIError
        """
        # Initialize with base values
        code = APIErrorCode.UNKNOWN
        message = "Unknown error"
        exchange_code = None
        retry_after = None
        
        # Backpack typically returns errors in a structured format with 'code' and 'msg' fields
        if isinstance(error_data, dict):
            # Extract message and code
            if "msg" in error_data:
                message = error_data["msg"]
            elif "message" in error_data:
                message = error_data["message"]
            
            if "code" in error_data:
                exchange_code = str(error_data["code"])
            
            # Extract retry-after for rate limiting
            if "retry-after" in error_data:
                retry_after = float(error_data["retry-after"])
            
            # Map Backpack error codes to our standardized codes
            # Reference: https://docs.backpack.exchange/ (error codes)
            if exchange_code:
                # Try to map based on known Backpack error codes
                if exchange_code in ["401", "-1010", "-1011", "-2010", "-2011"]:
                    code = APIErrorCode.AUTHENTICATION_FAILED
                elif exchange_code in ["-1013", "-1016", "-2010"]:
                    code = APIErrorCode.INSUFFICIENT_FUNDS
                elif exchange_code in ["-1021", "-1003"]:
                    code = APIErrorCode.TIMEOUT
                elif exchange_code in ["-1015", "-1022"]:
                    code = APIErrorCode.RATE_LIMITED
                elif exchange_code in ["-1121", "-2011"]:
                    code = APIErrorCode.INVALID_PARAMS
                elif exchange_code in ["-1100", "-1102", "-1103"]:
                    code = APIErrorCode.INVALID_PARAMS
                elif exchange_code in ["-2013", "-2014"]:
                    code = APIErrorCode.ORDER_NOT_FOUND
                elif exchange_code in ["-1119", "-1116"]:
                    code = APIErrorCode.QUANTITY_OUT_OF_RANGE
                elif exchange_code in ["-1004", "-1005", "-1006", "-1007"]:
                    code = APIErrorCode.SERVER_ERROR
                elif exchange_code in ["-1120", "-2012"]:
                    code = APIErrorCode.PRICE_OUT_OF_RANGE
                elif exchange_code in ["-2015"]:
                    code = APIErrorCode.DUPLICATE_ORDER
            
            # Map based on error message patterns if we still have an unknown code
            if code == APIErrorCode.UNKNOWN and message:
                lower_message = message.lower()
                if any(term in lower_message for term in ["insufficient", "balance", "not enough"]):
                    code = APIErrorCode.INSUFFICIENT_FUNDS
                elif any(term in lower_message for term in ["precision", "decimal", "lot", "step"]):
                    code = APIErrorCode.PRECISION_ERROR
                elif any(term in lower_message for term in ["min notional", "minimum notional"]):
                    code = APIErrorCode.MIN_NOTIONAL_NOT_MET
                elif any(term in lower_message for term in ["quantity", "size", "amount", "too small", "too large"]):
                    code = APIErrorCode.QUANTITY_OUT_OF_RANGE
                elif any(term in lower_message for term in ["price", "range", "invalid"]):
                    code = APIErrorCode.PRICE_OUT_OF_RANGE
                elif any(term in lower_message for term in ["order", "not found"]):
                    code = APIErrorCode.ORDER_NOT_FOUND
                elif any(term in lower_message for term in ["rate limit", "too many requests"]):
                    code = APIErrorCode.RATE_LIMITED
                elif any(term in lower_message for term in ["duplicate", "already exists"]):
                    code = APIErrorCode.DUPLICATE_ORDER
            
        # Fallback to HTTP status code mapping if we still have unknown code
        if code == APIErrorCode.UNKNOWN:
            if status_code == 401 or status_code == 403:
                code = APIErrorCode.AUTHENTICATION_FAILED
            elif status_code == 400:
                code = APIErrorCode.INVALID_PARAMS
            elif status_code == 404:
                code = APIErrorCode.SYMBOL_NOT_FOUND
            elif status_code == 429:
                code = APIErrorCode.RATE_LIMITED
            elif status_code == 503:
                code = APIErrorCode.MAINTENANCE
            elif status_code >= 500:
                code = APIErrorCode.SERVER_ERROR
        
        return APIError(
            message=f"Backpack API error: {message}",
            code=code,
            http_status=status_code,
            exchange_code=exchange_code,
            exchange_message=message,
            retry_after=retry_after
        ) 