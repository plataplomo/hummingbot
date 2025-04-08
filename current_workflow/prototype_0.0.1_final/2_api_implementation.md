# API Implementation - Exchange Integration

This document outlines the essential API implementations for Hyperliquid and Backpack exchanges in the CyberDeltaEngine Prototype 0.0.1.

## 1. Base Exchange API Interface

Both exchange implementations will inherit from a common abstract base class that defines the standard interface:

```python
from abc import ABC, abstractmethod
import asyncio
import aiohttp
import time
import json
import logging
from typing import Dict, List, Optional, Any, Tuple

class ExchangeAPI(ABC):
    """Abstract base class for exchange API clients."""
    
    def __init__(self, api_config: dict, secrets: dict):
        """Initialize the API client with configuration and secrets."""
        self.base_url = api_config.get("base_url")
        self.ws_url = api_config.get("ws_url")
        self.session = None
        self.ws_connections = {}
        self.request_timestamps = {}
        self.logger = logging.getLogger(f"{self.__class__.__name__}")
        self.lock = asyncio.Lock()
        
    @abstractmethod
    async def connect(self) -> bool:
        """Establish connections to the exchange."""
        pass
        
    @abstractmethod
    async def close(self) -> bool:
        """Close all connections to the exchange."""
        pass
    
    @abstractmethod
    async def _authenticate_request(self, *args, **kwargs) -> dict:
        """Authenticate a request using exchange-specific mechanism."""
        pass
    
    @abstractmethod
    async def get_balances(self) -> dict:
        """Get current balances for all assets."""
        pass
    
    @abstractmethod
    async def get_positions(self) -> dict:
        """Get current open positions."""
        pass
    
    @abstractmethod
    async def get_funding_rates(self, symbols: List[str] = None) -> dict:
        """Get current funding rates for the specified symbols."""
        pass
    
    @abstractmethod
    async def get_open_orders(self, symbol: str = None) -> List[dict]:
        """Get all open orders, optionally filtered by symbol."""
        pass
    
    @abstractmethod
    async def place_order(self, symbol: str, side: str, quantity: float,
                        order_type: str, price: float = None,
                        time_in_force: str = None) -> dict:
        """Place an order on the exchange."""
        pass
    
    @abstractmethod
    async def cancel_order(self, order_id: str, symbol: str = None) -> bool:
        """Cancel an existing order."""
        pass
    
    @abstractmethod
    async def get_ticker(self, symbol: str) -> dict:
        """Get ticker information for a symbol."""
        pass
    
    @abstractmethod
    async def get_orderbook(self, symbol: str, depth: int = None) -> dict:
        """Get orderbook for a symbol with specified depth."""
        pass
    
    async def _request(self, method: str, endpoint: str, data: dict = None, 
                     authenticated: bool = False) -> dict:
        """Make a request to the exchange API with error handling."""
        if self.session is None:
            self.session = aiohttp.ClientSession()
            
        url = f"{self.base_url}{endpoint}"
        headers = {}
        
        if authenticated:
            auth_data = await self._authenticate_request(method, endpoint, data)
            headers.update(auth_data.get("headers", {}))
            if auth_data.get("params"):
                data = auth_data.get("params")
                
        try:
            # Record request time for rate limiting
            async with self.lock:
                endpoint_key = f"{method}:{endpoint}"
                self.request_timestamps[endpoint_key] = time.time()
            
            # Make the actual request
            async with self.session.request(method, url, headers=headers, 
                                         json=data if method != "GET" else None,
                                         params=data if method == "GET" else None) as response:
                response_data = await response.json()
                
                if response.status >= 400:
                    error_message = f"API error: {response.status} - {response_data}"
                    self.logger.error(error_message)
                    raise APIError(error_message)
                    
                return response_data
        except aiohttp.ClientError as e:
            error_message = f"Connection error: {str(e)}"
            self.logger.error(error_message)
            raise ConnectionError(error_message)
```

## 2. Hyperliquid API Implementation

The Hyperliquid API client implements blockchain-based EIP-712 authentication and connects to Hyperliquid's REST and WebSocket endpoints.

### 2.1 Authentication and Connection

```python
class HyperliquidAPI(ExchangeAPI):
    """Hyperliquid exchange API client."""
    
    def __init__(self, api_config: dict, secrets: dict):
        """Initialize the Hyperliquid API client."""
        super().__init__(api_config, secrets)
        self.base_url = api_config.get("base_url", "https://api.hyperliquid.xyz")
        self.ws_url = api_config.get("ws_url", "wss://api.hyperliquid.xyz/ws")
        self.private_key = secrets.get("HYPERLIQUID_WALLET_PRIVATE_KEY")
        self.nonce_counter = 0
        self.ws_handlers = {}
        
    async def connect(self) -> bool:
        """Establish connections to Hyperliquid."""
        self.session = aiohttp.ClientSession()
        # Test connection with a simple request
        try:
            await self.get_ticker("BTC-PERP")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to Hyperliquid: {e}")
            return False
            
    async def close(self) -> bool:
        """Close all connections to Hyperliquid."""
        if self.session:
            for ws in self.ws_connections.values():
                if not ws.closed:
                    await ws.close()
            await self.session.close()
            self.session = None
            return True
        return False
    
    async def _authenticate_request(self, method: str, endpoint: str, data: dict = None) -> dict:
        """Sign a request using EIP-712 and wallet private key."""
        if not self.private_key:
            raise AuthenticationError("Private key not provided")
        
        async with self.lock:
            self.nonce_counter += 1
            nonce = self.nonce_counter
        
        timestamp = int(time.time() * 1000)
        
        # Create EIP-712 compatible message
        message = {
            "method": method,
            "path": endpoint,
            "body": data or {},
            "timestamp": timestamp,
            "nonce": nonce
        }
        
        # Sign message using eth_account
        from eth_account.messages import encode_structured_data
        from eth_account import Account
        
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
        
        encoded_message = encode_structured_data(structured_data)
        signed_message = Account.sign_message(encoded_message, private_key=self.private_key)
        
        # Return headers and updated data
        return {
            "headers": {
                "X-HL-Signature": signed_message.signature.hex(),
                "X-HL-Timestamp": str(timestamp),
                "X-HL-Nonce": str(nonce)
            }
        }
```

### 2.2 Core API Methods

```python
    async def get_funding_rates(self, symbols: List[str] = None) -> dict:
        """Get current funding rates for symbols."""
        if not symbols:
            # Get funding rates for all assets
            response = await self._request("POST", "/info", {"type": "metaAndAssetCtxs"})
            assets = response.get("data", {}).get("assetCtxs", [])
            funding_rates = {}
            for asset in assets:
                asset_name = asset.get("name")
                funding_rate = asset.get("funding", {}).get("rate", 0.0)
                funding_rates[asset_name] = float(funding_rate)
            return funding_rates
        else:
            # Get funding rates for specific symbols
            funding_rates = {}
            for symbol in symbols:
                payload = {"type": "fundingRate", "asset": symbol}
                response = await self._request("POST", "/info", data=payload)
                if "data" in response and "fundingRate" in response["data"]:
                    funding_rates[symbol] = float(response["data"]["fundingRate"])
                else:
                    funding_rates[symbol] = 0.0
            return funding_rates
            
    async def get_balances(self) -> dict:
        """Get current balances."""
        payload = {"type": "clearinghouseState"}
        response = await self._request("POST", "/user", data=payload, authenticated=True)
        
        if "data" in response and "walletBalanceUsd" in response["data"]:
            balance_usd = float(response["data"]["walletBalanceUsd"])
            return {"USDC": balance_usd}
        
        return {}
        
    async def get_positions(self) -> dict:
        """Get current positions."""
        payload = {"type": "positions"}
        response = await self._request("POST", "/user", data=payload, authenticated=True)
        
        positions = {}
        if "data" in response:
            for position in response["data"]:
                symbol = position.get("coin")
                size = float(position.get("szi", 0))
                entry_price = float(position.get("entryPx", 0))
                leverage = float(position.get("leverage", 1))
                positions[symbol] = {
                    "symbol": symbol,
                    "size": size,
                    "entry_price": entry_price,
                    "leverage": leverage
                }
        
        return positions
        
    async def place_order(self, symbol: str, side: str, quantity: float,
                        order_type: str = "limit", price: float = None,
                        time_in_force: str = "gtc") -> dict:
        """Place an order on Hyperliquid."""
        if order_type.lower() == "limit" and price is None:
            raise ValueError("Price is required for limit orders")
            
        # Normalize order details
        normalized_side = "B" if side.upper() in ["BUY", "B", "LONG"] else "A"
        normalized_order_type = "limit" if order_type.lower() == "limit" else "market"
        
        # Prepare order payload
        order = {
            "asset": symbol,
            "side": normalized_side,
            "size": str(quantity),
            "reduceOnly": False,
            "cloid": str(int(time.time() * 1000))  # Client order ID
        }
        
        if normalized_order_type == "limit":
            order["limitPx"] = str(price)
            order["tif"] = time_in_force.lower()
        
        payload = {
            "type": "order",
            "order": order
        }
        
        # Send the order
        response = await self._request("POST", "/exchange", data=payload, authenticated=True)
        
        # Process the response
        if "data" in response and "statuses" in response["data"]:
            status = response["data"]["statuses"][0]
            if status.get("resting"):
                # Order is resting on the book
                return {
                    "order_id": status.get("oid"),
                    "client_order_id": order["cloid"],
                    "symbol": symbol,
                    "side": side,
                    "type": order_type,
                    "quantity": quantity,
                    "price": price,
                    "status": "OPEN"
                }
            else:
                # Order was filled immediately or rejected
                return {
                    "order_id": status.get("oid"),
                    "client_order_id": order["cloid"],
                    "symbol": symbol,
                    "side": side,
                    "type": order_type,
                    "quantity": quantity,
                    "price": price,
                    "status": "FILLED" if not status.get("err") else "REJECTED",
                    "error": status.get("err")
                }
                
        raise APIError(f"Unexpected response format: {response}")
        
    async def cancel_order(self, order_id: str, symbol: str = None) -> bool:
        """Cancel an existing order."""
        if not symbol:
            raise ValueError("Symbol is required for Hyperliquid cancel orders")
            
        payload = {
            "type": "cancel",
            "cancel": {
                "asset": symbol,
                "oid": int(order_id)
            }
        }
        
        response = await self._request("POST", "/exchange", data=payload, authenticated=True)
        
        if "data" in response and "statuses" in response["data"]:
            status = response["data"]["statuses"][0]
            if not status.get("err"):
                return True
                
        return False
        
    async def get_open_orders(self, symbol: str = None) -> List[dict]:
        """Get all open orders, optionally filtered by symbol."""
        payload = {"type": "openOrders"}
        response = await self._request("POST", "/user", data=payload, authenticated=True)
        
        orders = []
        if "data" in response:
            for order in response["data"]:
                if symbol and order.get("asset") != symbol:
                    continue
                    
                orders.append({
                    "order_id": order.get("oid"),
                    "symbol": order.get("asset"),
                    "side": "BUY" if order.get("side") == "B" else "SELL",
                    "quantity": float(order.get("sz", 0)),
                    "price": float(order.get("limitPx", 0)),
                    "type": "LIMIT" if "limitPx" in order else "MARKET",
                    "time_in_force": order.get("tif", "gtc").upper(),
                    "reduce_only": order.get("reduceOnly", False)
                })
                
        return orders
        
    async def get_ticker(self, symbol: str) -> dict:
        """Get ticker information for a symbol."""
        payload = {"type": "indexPrice", "asset": symbol}
        response = await self._request("POST", "/info", data=payload)
        
        if "data" in response and "price" in response["data"]:
            price = float(response["data"]["price"])
            return {
                "symbol": symbol,
                "price": price
            }
            
        raise APIError(f"Failed to get ticker for {symbol}")
        
    async def get_orderbook(self, symbol: str, depth: int = None) -> dict:
        """Get orderbook for a symbol."""
        payload = {"type": "l2Book", "asset": symbol}
        response = await self._request("POST", "/info", data=payload)
        
        if "data" in response and "levels" in response["data"]:
            levels = response["data"]["levels"]
            bids = []
            asks = []
            
            for level in levels:
                if level["isBid"]:
                    bids.append([float(level["px"]), float(level["sz"])])
                else:
                    asks.append([float(level["px"]), float(level["sz"])])
            
            # Sort and limit depth if specified
            bids.sort(key=lambda x: -x[0])  # Sort bids descending by price
            asks.sort(key=lambda x: x[0])   # Sort asks ascending by price
            
            if depth:
                bids = bids[:depth]
                asks = asks[:depth]
                
            return {
                "symbol": symbol,
                "bids": bids,
                "asks": asks,
                "timestamp": int(time.time() * 1000)
            }
            
        raise APIError(f"Failed to get orderbook for {symbol}")
```

## 3. Backpack API Implementation

The Backpack API client implements ED25519 cryptographic authentication and connects to Backpack's REST and WebSocket endpoints.

### 3.1 Authentication and Connection

```python
class BackpackAPI(ExchangeAPI):
    """Backpack exchange API client."""
    
    def __init__(self, api_config: dict, secrets: dict):
        """Initialize the Backpack API client."""
        super().__init__(api_config, secrets)
        self.base_url = api_config.get("base_url", "https://api.backpack.exchange")
        self.ws_url = api_config.get("ws_url", "wss://ws.backpack.exchange")
        self.api_key = secrets.get("BACKPACK_API_KEY")
        self.api_secret = secrets.get("BACKPACK_API_SECRET")
        self.window = 5000  # Default time window in milliseconds
        self.ws_handlers = {}
        
    async def connect(self) -> bool:
        """Establish connections to Backpack."""
        self.session = aiohttp.ClientSession()
        # Test connection with a simple request
        try:
            await self.get_ticker("BTC_USDC")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to Backpack: {e}")
            return False
            
    async def close(self) -> bool:
        """Close all connections to Backpack."""
        if self.session:
            for ws in self.ws_connections.values():
                if not ws.closed:
                    await ws.close()
            await self.session.close()
            self.session = None
            return True
        return False
        
    async def _authenticate_request(self, method: str, endpoint: str, data: dict = None) -> dict:
        """Create authentication headers for Backpack."""
        import base64
        import hashlib
        import hmac
        from urllib.parse import urlencode
        
        if not self.api_key or not self.api_secret:
            raise AuthenticationError("API key and secret required")
            
        timestamp = int(time.time() * 1000)
        
        # Determine instruction type based on endpoint
        instruction_map = {
            "/api/v1/capital": "balanceQuery",
            "/api/v1/capital/deposits": "depositQueryAll",
            "/api/v1/capital/withdrawals": "withdrawalQueryAll",
            "/api/v1/capital/address": "depositAddressQuery",
            "/api/v1/capital/withdraw": "withdraw",
            "/api/v1/order": "orderExecute" if method == "POST" else "orderQuery",
            "/api/v1/orders": "orderQueryAll",
            "/api/v1/history/orders": "orderHistoryQueryAll",
            "/api/v1/history/fills": "fillHistoryQueryAll"
        }
        instruction = instruction_map.get(endpoint)
        
        if instruction is None:
            # Try to match by prefix
            for key, value in instruction_map.items():
                if endpoint.startswith(key):
                    instruction = value
                    break
                    
        if instruction is None:
            self.logger.warning(f"Could not determine instruction type for {endpoint}")
            instruction = "unknown"
            
        # Prepare signing string
        if method == "GET" and data:
            # Sort query parameters
            sorted_params = []
            for key in sorted(data.keys()):
                sorted_params.append(f"{key}={data[key]}")
            query_string = "&".join(sorted_params)
        elif method == "POST" and data:
            # Sort body parameters
            sorted_params = []
            for key in sorted(data.keys()):
                sorted_params.append(f"{key}={data[key]}")
            query_string = "&".join(sorted_params)
        else:
            query_string = ""
            
        # Append instruction, timestamp and window
        if query_string:
            signing_string = f"instruction={instruction}&{query_string}&timestamp={timestamp}&window={self.window}"
        else:
            signing_string = f"instruction={instruction}&timestamp={timestamp}&window={self.window}"
            
        # Sign the string
        signature = base64.b64encode(
            hmac.new(
                base64.b64decode(self.api_secret),
                signing_string.encode("utf-8"),
                hashlib.sha256
            ).digest()
        ).decode("utf-8")
        
        # Return headers
        return {
            "headers": {
                "X-API-Key": self.api_key,
                "X-Timestamp": str(timestamp),
                "X-Window": str(self.window),
                "X-Signature": signature
            }
        }
```

### 3.2 Core API Methods

```python
    async def get_funding_rates(self, symbols: List[str] = None) -> dict:
        """Get current funding rates for symbols on Backpack.
        
        Note: Backpack doesn't have a direct funding rate endpoint, so we'll need to
        calculate the funding rate from the mark price and index price.
        Funding rate is typically calculated as: (mark_price - index_price) / index_price
        and is expressed as a percentage for the funding period.
        """
        funding_rates = {}
        
        if not symbols:
            # Get all available perpetual symbols
            all_markets = await self.get_markets()
            symbols = [m["symbol"] for m in all_markets if m.get("type") == "perp"]
            
        for symbol in symbols:
            try:
                # Get ticker for mark price
                ticker = await self.get_ticker(symbol)
                mark_price = ticker["price"]
                
                # Get index price (will need to be added to Backpack implementation)
                # For Backpack, we need to find the underlying index from docs/API
                index_data = await self._request("GET", f"/api/v1/ticker/index/{symbol}")
                
                if "data" in index_data and "price" in index_data["data"]:
                    index_price = float(index_data["data"]["price"])
                    
                    # Calculate estimated funding rate
                    # Note: This calculation may need adjustment based on Backpack's specific formula
                    premium = (mark_price - index_price) / index_price
                    # Convert to 8-hour rate if needed (based on Backpack's funding periods)
                    funding_rate = premium * (8 / 24)  # Assuming 8-hour funding periods
                    
                    funding_rates[symbol] = funding_rate
                else:
                    self.logger.error(f"Could not get index price for {symbol}. Funding rate calculation failed.")
                    funding_rates[symbol] = None
            except Exception as e:
                self.logger.error(f"Error calculating funding rate for {symbol}: {e}")
                funding_rates[symbol] = None
                
        return funding_rates
        
    async def get_markets(self) -> List[dict]:
        """Get available markets on Backpack."""
        response = await self._request("GET", "/api/v1/markets")
        return response.get("data", [])
            
    async def get_balances(self) -> dict:
        """Get current balances."""
        response = await self._request("GET", "/api/v1/capital", authenticated=True)
        
        balances = {}
        if "data" in response:
            for balance in response["data"]:
                asset = balance.get("asset")
                available = float(balance.get("available", 0))
                locked = float(balance.get("locked", 0))
                balances[asset] = {
                    "available": available,
                    "locked": locked,
                    "total": available + locked
                }
                
        return balances
        
    async def get_positions(self) -> dict:
        """
        Get current positions on Backpack by analyzing fill history.
        
        Since Backpack doesn't provide a direct position endpoint for perpetuals,
        we calculate positions based on trade history.
        """
        positions = {}
        
        try:
            # Get recent fill history with sufficient depth to capture all open positions
            # Using a 30-day lookback as a reasonable default
            current_time = int(time.time() * 1000)
            thirty_days_ago = current_time - (30 * 24 * 60 * 60 * 1000)
            
            fills_payload = {
                "from": thirty_days_ago,
                "to": current_time,
                "limit": 1000  # Maximum allowed limit
            }
            
            fills_response = await self._request(
                "GET", 
                "/api/v1/history/fills", 
                data=fills_payload, 
                authenticated=True
            )
            
            if "data" not in fills_response:
                self.logger.error("Failed to retrieve fill history from Backpack")
                return positions
                
            # Get all perpetual symbols
            markets = await self.get_markets()
            perp_symbols = [m["symbol"] for m in markets if m.get("type") == "perp"]
            
            # Process fills to calculate positions
            for symbol in perp_symbols:
                symbol_fills = [f for f in fills_response["data"] if f.get("symbol") == symbol]
                
                # Skip if no fills for this symbol
                if not symbol_fills:
                    continue
                    
                # Calculate net position
                net_size = 0.0
                avg_entry_price = 0.0
                total_cost = 0.0
                
                for fill in symbol_fills:
                    side = fill.get("side", "").upper()
                    quantity = float(fill.get("quantity", 0))
                    price = float(fill.get("price", 0))
                    
                    # Buy increases position, sell decreases
                    fill_size = quantity if side == "BUY" else -quantity
                    net_size += fill_size
                    
                    # Track for average entry price calculation
                    if (net_size > 0 and fill_size > 0) or (net_size < 0 and fill_size < 0):
                        total_cost += abs(fill_size) * price
                
                # Skip if no position
                if abs(net_size) < 0.00001:
                    continue
                    
                # Calculate average entry price
                if net_size != 0:
                    avg_entry_price = total_cost / abs(net_size)
                    
                # Get current price for mark-to-market
                ticker = await self.get_ticker(symbol)
                current_price = ticker["price"]
                
                # Calculate unrealized PnL
                unrealized_pnl = net_size * (current_price - avg_entry_price) if net_size > 0 else net_size * (avg_entry_price - current_price)
                
                positions[symbol] = {
                    "symbol": symbol,
                    "size": net_size,
                    "entry_price": avg_entry_price,
                    "mark_price": current_price,
                    "unrealized_pnl": unrealized_pnl,
                    "position_value": abs(net_size * current_price),
                    "note": "Calculated from fill history - may not reflect exchange's internal accounting"
                }
            
            self.logger.info(f"Calculated {len(positions)} positions from Backpack fill history")
            return positions
            
        except Exception as e:
            self.logger.error(f"Error calculating Backpack positions: {e}")
            return {}
        
    async def place_order(self, symbol: str, side: str, quantity: float,
                        order_type: str = "limit", price: float = None,
                        time_in_force: str = "GTC") -> dict:
        """Place an order on Backpack."""
        if order_type.lower() == "limit" and price is None:
            raise ValueError("Price is required for limit orders")
            
        # Prepare order payload
        payload = {
            "symbol": symbol,
            "side": side.upper(),
            "quantity": str(quantity),
            "type": order_type.upper()
        }
        
        if order_type.lower() == "limit":
            payload["price"] = str(price)
            payload["timeInForce"] = time_in_force.upper()
            
        # Send the order
        response = await self._request("POST", "/api/v1/order", data=payload, authenticated=True)
        
        if "data" in response:
            order_data = response["data"]
            return {
                "order_id": order_data.get("orderId"),
                "client_order_id": order_data.get("clientOrderId"),
                "symbol": symbol,
                "side": side,
                "type": order_type,
                "quantity": quantity,
                "price": price,
                "status": order_data.get("status")
            }
            
        raise APIError(f"Unexpected response format: {response}")
        
    async def cancel_order(self, order_id: str, symbol: str = None) -> bool:
        """Cancel an existing order."""
        if not symbol:
            raise ValueError("Symbol is required for Backpack cancel orders")
            
        payload = {
            "symbol": symbol,
            "orderId": order_id
        }
        
        response = await self._request("DELETE", "/api/v1/order", data=payload, authenticated=True)
        
        if response.get("success", False):
            return True
            
        return False
        
    async def get_open_orders(self, symbol: str = None) -> List[dict]:
        """Get all open orders, optionally filtered by symbol."""
        payload = {}
        if symbol:
            payload["symbol"] = symbol
            
        response = await self._request("GET", "/api/v1/orders", data=payload, authenticated=True)
        
        orders = []
        if "data" in response:
            for order in response["data"]:
                orders.append({
                    "order_id": order.get("orderId"),
                    "client_order_id": order.get("clientOrderId"),
                    "symbol": order.get("symbol"),
                    "side": order.get("side"),
                    "quantity": float(order.get("quantity", 0)),
                    "price": float(order.get("price", 0)),
                    "type": order.get("type"),
                    "time_in_force": order.get("timeInForce"),
                    "status": order.get("status")
                })
                
        return orders
        
    async def get_ticker(self, symbol: str) -> dict:
        """Get ticker information for a symbol."""
        payload = {"symbol": symbol}
        response = await self._request("GET", "/api/v1/ticker", data=payload)
        
        if "data" in response:
            ticker = response["data"]
            return {
                "symbol": symbol,
                "price": float(ticker.get("lastPrice", 0)),
                "bid": float(ticker.get("bidPrice", 0)),
                "ask": float(ticker.get("askPrice", 0)),
                "volume": float(ticker.get("volume", 0)),
                "timestamp": int(ticker.get("time", 0))
            }
            
        raise APIError(f"Failed to get ticker for {symbol}")
        
    async def get_orderbook(self, symbol: str, depth: int = None) -> dict:
        """Get orderbook for a symbol."""
        payload = {"symbol": symbol}
        if depth:
            payload["limit"] = depth
            
        response = await self._request("GET", "/api/v1/depth", data=payload)
        
        if "data" in response:
            book = response["data"]
            return {
                "symbol": symbol,
                "bids": [[float(b[0]), float(b[1])] for b in book.get("bids", [])],
                "asks": [[float(a[0]), float(a[1])] for a in book.get("asks", [])],
                "timestamp": int(book.get("time", 0))
            }
            
        raise APIError(f"Failed to get orderbook for {symbol}")
```

## 4. Error Handling

Both API implementations include robust error handling to manage common API issues:

```python
class APIError(Exception):
    """Exception raised for API errors."""
    pass

class AuthenticationError(Exception):
    """Exception raised for authentication errors."""
    pass

class ConnectionError(Exception):
    """Exception raised for connection errors."""
    pass

class DataError(Exception):
    """Exception raised for data format errors."""
    pass

class RateLimitError(Exception):
    """Exception raised for rate limit errors."""
    pass
```

## 5. WebSocket Implementation

Both API clients include WebSocket support for real-time data. The WebSocket implementations handle:

```python
async def _connect_websocket(self, stream_name: str, handler: Callable, is_private: bool = False) -> None:
    """Establish a WebSocket connection and register a handler."""
    url = f"{self.ws_url}"
    
    if stream_name in self.ws_connections and not self.ws_connections[stream_name].closed:
        self.logger.debug(f"WebSocket for {stream_name} already connected")
        return
        
    try:
        session = aiohttp.ClientSession()
        ws = await session.ws_connect(url)
        self.ws_connections[stream_name] = ws
        
        # Authenticate if this is a private stream
        if is_private:
            auth_message = await self._create_ws_auth_message()
            await ws.send_json(auth_message)
            auth_response = await ws.receive_json()
            if not self._validate_ws_auth_response(auth_response):
                raise AuthenticationError(f"WebSocket authentication failed: {auth_response}")
                
        # Subscribe to the stream
        subscribe_message = {
            "method": "SUBSCRIBE",
            "params": [stream_name],
            "id": int(time.time() * 1000)
        }
        await ws.send_json(subscribe_message)
        
        # Handle subscription response
        sub_response = await ws.receive_json()
        if not self._validate_ws_subscription(sub_response):
            raise ConnectionError(f"WebSocket subscription failed: {sub_response}")
            
        # Start message handler
        asyncio.create_task(self._handle_ws_messages(ws, stream_name, handler))
        
        self.logger.info(f"Successfully connected to {stream_name} WebSocket")
        
    except Exception as e:
        self.logger.error(f"WebSocket connection error for {stream_name}: {e}")
        if stream_name in self.ws_connections:
            del self.ws_connections[stream_name]
        raise ConnectionError(f"WebSocket connection failed: {e}")
        
async def _handle_ws_messages(self, ws: aiohttp.ClientWebSocketResponse, 
                            stream_name: str, handler: Callable) -> None:
    """Handle incoming WebSocket messages."""
    try:
        async for message in ws:
            if message.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(message.data)
                    if self._is_keepalive_message(data):
                        await self._handle_keepalive(ws, data)
                    else:
                        # Process the message
                        await handler(data)
                except json.JSONDecodeError:
                    self.logger.error(f"Invalid JSON in WebSocket message: {message.data}")
            elif message.type in (aiohttp.WSMsgType.CLOSE, 
                               aiohttp.WSMsgType.CLOSING, 
                               aiohttp.WSMsgType.CLOSED):
                self.logger.info(f"WebSocket connection for {stream_name} is closing")
                break
            elif message.type == aiohttp.WSMsgType.ERROR:
                self.logger.error(f"WebSocket error for {stream_name}: {message.data}")
                break
    except Exception as e:
        self.logger.error(f"Error in WebSocket handler for {stream_name}: {e}")
    finally:
        # Cleanup and schedule reconnection
        if not ws.closed:
            await ws.close()
        if stream_name in self.ws_connections:
            del self.ws_connections[stream_name]
        self.logger.info(f"Scheduling reconnection for {stream_name} in 5 seconds")
        await asyncio.sleep(5)
        asyncio.create_task(self._connect_websocket(stream_name, handler, is_private))
```

This implementation includes:
1. Connection establishment and automatic reconnection
2. Authentication for private streams
3. Subscription management
4. Structured message handling with error recovery
5. Keepalive mechanism to maintain the connection
6. Cleanup on connection close or error

## 6. Rate Limiting

Both API implementations include comprehensive rate limit handling:

```python
class RateLimiter:
    """Rate limiter for API requests using token bucket algorithm."""
    
    def __init__(self, rate: float = 1.0, bucket_size: int = 1):
        """Initialize the rate limiter.
        
        Args:
            rate: Tokens per second to add to the bucket
            bucket_size: Maximum number of tokens in the bucket
        """
        self.rate = rate
        self.bucket_size = bucket_size
        self.tokens = bucket_size
        self.updated_at = time.monotonic()
        self.lock = asyncio.Lock()
        
    async def acquire(self) -> float:
        """Acquire a token from the bucket.
        
        Returns:
            The wait time in seconds before the token can be acquired.
        """
        async with self.lock:
            now = time.monotonic()
            time_elapsed = now - self.updated_at
            
            # Add tokens that should have been added during elapsed time
            self.tokens = min(self.bucket_size, self.tokens + time_elapsed * self.rate)
            self.updated_at = now
            
            # If we have at least one token, consume it immediately
            if self.tokens >= 1:
                self.tokens -= 1
                return 0
                
            # Calculate wait time until a token is available
            wait_time = (1 - self.tokens) / self.rate
            return wait_time
            
class RateLimitManager:
    """Manage rate limits for multiple endpoints."""
    
    def __init__(self, default_rate: float = 1.0, default_bucket: int = 1):
        """Initialize the rate limit manager."""
        self.limiters = {}
        self.default_rate = default_rate
        self.default_bucket = default_bucket
        
    def get_limiter(self, endpoint: str) -> RateLimiter:
        """Get or create a rate limiter for an endpoint."""
        if endpoint not in self.limiters:
            self.limiters[endpoint] = RateLimiter(self.default_rate, self.default_bucket)
        return self.limiters[endpoint]
        
    def configure_endpoint(self, endpoint: str, rate: float, bucket_size: int) -> None:
        """Configure rate limits for a specific endpoint."""
        self.limiters[endpoint] = RateLimiter(rate, bucket_size)
        
    async def acquire(self, endpoint: str) -> None:
        """Acquire a token for an endpoint, waiting if necessary."""
        limiter = self.get_limiter(endpoint)
        wait_time = await limiter.acquire()
        
        if wait_time > 0:
            await asyncio.sleep(wait_time)
```

Implementation in the API clients:

```python
# In ExchangeAPI class initialization
self.rate_limiter = RateLimitManager()

# Configure specific endpoints (example for Hyperliquid)
self.rate_limiter.configure_endpoint("POST:/exchange", 5.0, 10)  # 5 requests/sec with burst of 10
self.rate_limiter.configure_endpoint("POST:/info", 10.0, 20)     # 10 requests/sec with burst of 20

# In the _request method
async def _request(self, method: str, endpoint: str, data: dict = None, 
                 authenticated: bool = False) -> dict:
    """Make a request to the exchange API with error handling and rate limiting."""
    if self.session is None:
        self.session = aiohttp.ClientSession()
        
    # Apply rate limiting
    endpoint_key = f"{method}:{endpoint}"
    await self.rate_limiter.acquire(endpoint_key)
    
    # Continue with normal request logic
    # ...
```

This implementation:
1. Uses token bucket algorithm for flexible rate limiting
2. Supports different limits for different endpoints
3. Handles bursts of requests within the limit
4. Waits when rate limits would be exceeded
5. Properly tracks and limits usage across multiple asynchronous calls

## 7. Implementation Priority

For Prototype 0.0.1, the implementation priority is:

1. Core API endpoints required for funding rate strategy:
   - Funding rate retrieval (implemented for both exchanges)
   - Order placement and cancellation
   - Position and balance retrieval
   - Market data access (tickers, orderbooks)

2. Robust error handling with specific error code handling
   - Map HTTP error codes to appropriate actions
   - Implement retry logic with exponential backoff
   - Add circuit breakers for critical failures

3. WebSocket connectivity with reconnection logic
   - Implement keep-alive mechanism
   - Detect and handle disconnections
   - Buffer recent messages to prevent data loss

4. Authentication mechanisms with proper security
   - Secure storage of API keys
   - Proper signing of requests
   - Validation of responses

## 8. Backpack Funding Rate Strategy: Validation and Fallback

### 8.1 Funding Rate Calculation Validation

The funding rate calculation for Backpack is **highly speculative** and requires rigorous validation before being used in production:

1. **Initial Implementation Approach**:
   - Calculate estimated funding rates using mark/index price data
   - Log all calculations with timestamps for comparison
   - Store all prediction values with actual observed funding payments
   - Calculate error metrics (RMSE, MAE) to assess accuracy
   - Immediately implement validation logic that compares predicted vs. actual

2. **Live Validation**:
   ```python
   def validate_funding_calculation(predicted_rate, actual_payment, position_size, mark_price):
       """
       Validate funding rate calculation against actual payments.
       Returns False if prediction error exceeds threshold.
       """
       if position_size == 0 or mark_price == 0:
           return True  # No position to validate
           
       expected_payment = position_size * mark_price * predicted_rate
       payment_error = abs(expected_payment - actual_payment)
       relative_error = payment_error / abs(actual_payment) if actual_payment != 0 else float('inf')
       
       # Log validation results
       logger.info(f"Funding validation: predicted={predicted_rate:.6f}, " 
                  f"implied_actual={actual_payment/(position_size*mark_price):.6f}, "
                  f"relative_error={relative_error:.2%}")
       
       # Use tighter threshold in production
       ERROR_THRESHOLD = 0.25  # 25% error margin for prototype, tighten later
       return relative_error <= ERROR_THRESHOLD
   ```

3. **Validation Schedule**:
   - Immediately after each funding payment
   - Daily validation report for all funding pairs
   - Weekly calibration of calculation parameters (dampening factor)

### 8.2 Fallback Implementation (REQUIRED)

Due to the high uncertainty in funding rate calculations for Backpack, the fallback strategy **MUST** be fully implemented in Prototype 0.0.1:

1. **Primary Strategy**: Use only when funding rate validation consistently shows < 15% error
2. **Fallback Strategy**: Spot vs. Perpetual arbitrage between:
   - Backpack spot markets (high certainty)
   - Hyperliquid perpetual markets (reliable funding data)

3. **Fallback Activation Triggers**:
   - Funding rate validation fails for 3 consecutive periods
   - Calculated funding rates show high volatility (> 3 std dev)
   - Any evidence of data issues in mark/index prices

4. **Fallback Strategy Implementation**:
   ```python
   # Pseudocode for fallback strategy determination
   def determine_active_strategy():
       validation_failures = get_recent_validation_failures()
       if validation_failures >= 3:
           return "FALLBACK_SPOT_VS_PERP"
       
       funding_volatility = calculate_funding_volatility()
       if funding_volatility > 3 * historical_std_dev:
           return "FALLBACK_SPOT_VS_PERP"
       
       return "PRIMARY_FUNDING_ARBITRAGE"
   ```

The prototype MUST include both strategies fully implemented, with clear metrics for determining which is active. We will **NOT** rely on the speculative funding rate calculation without confirming its accuracy through real-world validation.

## 9. Exchange-Specific Funding Rate Mechanics

Understanding the exact funding rate calculation methods for each exchange is critical for accurate arbitrage opportunity detection.

### 9.1 Hyperliquid Funding Rate Mechanics

Hyperliquid's 8-hour funding rate typically incorporates:

```python
# Hyperliquid funding rate calculation (conceptual)
def calculate_hyperliquid_funding_rate(premium_index, interest_component):
    """
    Calculate Hyperliquid funding rate based on documentation and observed behavior.
    
    Parameters:
    - premium_index: The premium/discount of mark price to oracle price
    - interest_component: Interest rate component (typically small)
    
    Returns:
    - 8-hour funding rate, clamped to prevent excessive values
    """
    # Calculate raw funding rate
    funding_rate = premium_index + interest_component
    
    # Apply clamping (standard limit is ±0.75% per 8h period)
    max_rate = 0.0075  # 0.75%
    min_rate = -0.0075  # -0.75%
    clamped_rate = max(min(funding_rate, max_rate), min_rate)
    
    return clamped_rate
```

For Hyperliquid's funding rate implementation:
- The funding is paid hourly at 1/8th of the 8-hour rate
- The premium index is based on the difference between market price and reference price (typically oracle price)
- Actual values are paid/received every hour based on your position
- The hourly payment is position_size * mark_price * (hourly_funding_rate)

### 9.2 Backpack Funding Rate Mechanics

For Backpack, since direct funding rate data might be less accessible, we'll derive it:

```python
# Backpack funding rate estimation (conceptual)
def estimate_backpack_funding_rate(mark_price, index_price, funding_interval=8):
    """
    Estimate Backpack funding rate based on mark-index price difference.
    
    Parameters:
    - mark_price: Current mark price
    - index_price: Current index (reference) price
    - funding_interval: Hours between funding payments (typically 8)
    
    Returns:
    - Estimated funding rate for the specified interval
    """
    # Calculate price premium/discount
    premium = (mark_price - index_price) / index_price
    
    # Convert to funding rate (typical dampening factor of ~0.75)
    dampening = 0.75
    estimated_rate = premium * dampening
    
    return estimated_rate
```

For Backpack's implementation:
- The calculation is an estimation; actual calculations may vary
- The API endpoint `/api/v1/ticker/index/{symbol}` (or equivalent) will be needed for index prices
- Validation against actual funding payments will be necessary to refine the model
- Alternate data sources may be required if this approach proves unreliable

### 9.3 Implementation Approach for Prototype 0.0.1

For the initial prototype, we'll focus on:

1. **Reliable Data Collection**: Prioritize accurate collection of Hyperliquid funding rates from their API
2. **Simple Estimation Model**: Use the basic premium/discount approach for Backpack with appropriate safeguards
3. **Verification Loop**: Compare estimated rates with actual funding payments for validation
4. **Conservative Thresholds**: Apply wider safety margins for estimated funding rates
5. **Fallback Ready**: Keep spot vs perpetual strategy prepared as a fallback

Subsequent versions can implement more sophisticated funding rate models as we gather data and better understand exchange-specific mechanics.