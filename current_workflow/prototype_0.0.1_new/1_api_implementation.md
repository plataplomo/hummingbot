# API Implementation - Exchange Integration

This document provides concrete specifications for implementing the exchange API clients, focusing on actual endpoints, authentication methods, and required functionality for both Hyperliquid and Backpack exchanges.

## 1. HyperLiquid API Implementation

### 1.1 HyperLiquid API Overview

HyperLiquid's API consists of:
- REST API endpoints served at `https://api.hyperliquid.xyz`
- WebSocket endpoints at `wss://api.hyperliquid.xyz/ws`
- Authentication via EIP-712 message signing for private endpoints
- Leverages custom HyperBFT consensus optimized for latency

### 1.2 Key Endpoints

| Endpoint | Method | Purpose | Authentication |
|----------|--------|---------|----------------|
| `/info` | POST | Market data, funding rates, indices | No |
| `/exchange` | POST | Order placement, cancellation | Yes |
| `/user` | POST | Account data, positions, balances | Yes |
| `/ticker` | WebSocket | Real-time ticker updates | No |
| `/orderbook` | WebSocket | Orderbook updates | No |
| `/trades` | WebSocket | Market trades | No |
| `/user` | WebSocket | User account updates | Yes |

### 1.3 Authentication Mechanism

HyperLiquid uses EIP-712 message signing with Ethereum wallets:

1. Create payload containing method, path, body, timestamp, and nonce
2. Sign using EIP-712 with wallet's private key using `eth_account` library
3. Include signature in request header

### 1.4 Performance Characteristics

- End-to-end latency: 0.2 seconds median, 0.9 seconds at 99th percentile
- Throughput: Approximately 100k orders/sec
- Optimized for low-latency trading with consistent order execution

### 1.5 Special Features

- Support for "Hyperps" (HyperLiquid-only perps) that use internal 8-hour EWMA for funding
- Open interest caps may apply to newer markets (e.g., $1,000,000 for newer tokens)
- Native L1 chain with order book directly in L1 state

## 2. Backpack API Implementation

### 2.1 Backpack API Overview

Backpack's API consists of:
- REST API endpoints served at `https://api.backpack.exchange`
- WebSocket endpoints at `wss://ws.backpack.exchange`
- Authentication via ED25519 keypair signing
- Time window authentication for request validity

### 2.2 Key Endpoints

| Category | Endpoint | Method | Purpose | Authentication |
|----------|----------|--------|---------|----------------|
| **Markets** | `/api/v1/assets` | GET | Get assets | No |
| **Markets** | `/api/v1/markets` | GET | List markets | No |
| **Markets** | `/api/v1/ticker` | GET | Get ticker | No |
| **Markets** | `/api/v1/depth` | GET | Get orderbook depth | No |
| **Markets** | `/api/v1/klines` | GET | Get K-line data | No |
| **Capital** | `/api/v1/capital` | GET | Get balances | Yes |
| **Capital** | `/api/v1/capital/deposits` | GET | Get deposits | Yes |
| **Capital** | `/api/v1/capital/address` | GET | Get deposit address | Yes |
| **Capital** | `/api/v1/capital/withdrawals` | GET | Get withdrawals | Yes |
| **Capital** | `/api/v1/capital/withdraw` | POST | Request withdrawal | Yes |
| **Order** | `/api/v1/order` | GET | Get open order | Yes |
| **Order** | `/api/v1/order` | POST | Execute order | Yes |
| **Order** | `/api/v1/order` | DELETE | Cancel open order | Yes |
| **Order** | `/api/v1/orders` | GET | Get open orders | Yes |
| **Order** | `/api/v1/orders` | DELETE | Cancel open orders | Yes |
| **History** | `/api/v1/history/orders` | GET | Get order history | Yes |
| **History** | `/api/v1/history/fills` | GET | Get fill history | Yes |

### 2.3 Authentication Mechanism

Backpack uses ED25519 keypair signing:

1. Required headers:
   - `X-Timestamp` - Unix time in milliseconds
   - `X-Window` - Time window in milliseconds (default 5000, max 60000)
   - `X-API-Key` - Base64 encoded verifying key
   - `X-Signature` - Base64 encoded signature

2. Signature generation process:
   - Sort request parameters alphabetically
   - Format as query string
   - Append timestamp and window
   - Prefix with instruction type (e.g., `orderExecute`)
   - Sign with ED25519 private key
   - Encode signature in Base64

### 2.4 WebSocket Integration

Backpack WebSocket streams include:
- Public streams: K-Line, Ticker, Trade, Depth, Book Ticker
- Private streams: Order update

WebSocket requires:
- Subscription message format
- Keep-alive pings every 30 seconds
- Proper reconnection handling

## 3. Critical Implementation Components

### 3.1 Base Exchange API Class

```python
class ExchangeAPI(ABC):
    def __init__(self, api_config: dict, secrets: dict):
        self.base_url = api_config.get("base_url")
        self.ws_url = api_config.get("ws_url")
        self.session = None
        self.ws_connections = {}
        self.ws_subscriptions = {}
        self.request_timestamps = {}
        self.lock = asyncio.Lock()
        
    @abstractmethod
    async def _authenticate_request(self, *args, **kwargs):
        """Authenticate a request using exchange-specific mechanism."""
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
            # Implement rate limiting and error handling
            async with self.session.request(method, url, headers=headers, 
                                           json=data if method != "GET" else None,
                                           params=data if method == "GET" else None) as response:
                response_data = await response.json()
                
                if response.status >= 400:
                    raise APIError(f"API error: {response.status} - {response_data}")
                    
                return response_data
        except aiohttp.ClientError as e:
            raise ConnectionError(f"Connection error: {str(e)}")
```

### 3.2 HyperLiquid API Client

```python
class HyperliquidAPI(ExchangeAPI):
    def __init__(self, api_config: dict, secrets: dict):
        super().__init__(api_config, secrets)
        self.base_url = api_config.get("base_url", "https://api.hyperliquid.xyz")
        self.ws_url = api_config.get("ws_url", "wss://api.hyperliquid.xyz/ws")
        self.private_key = secrets.get("HYPERLIQUID_WALLET_PRIVATE_KEY")
        self.nonce_counter = 0
        
    async def _authenticate_request(self, method: str, endpoint: str, data: dict) -> dict:
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
        
    async def fetch_funding_rate(self, symbol: str) -> float:
        """Fetch the current funding rate for a symbol."""
        payload = {"type": "fundingRate", "asset": symbol}
        response = await self._request("POST", "/info", data=payload)
        
        if "data" in response and "fundingRate" in response["data"]:
            return float(response["data"]["fundingRate"])
        
        raise DataError("Invalid funding rate response")
    
    async def place_order(self, symbol: str, side: str, quantity: float, 
                        order_type: str = "limit", price: float = None,
                        time_in_force: str = "gtc") -> dict:
        """Place an order on HyperLiquid."""
        # Validate parameters
        if order_type == "limit" and price is None:
            raise ValueError("Price is required for limit orders")
            
        payload = {
            "type": "order",
            "asset": symbol,
            "side": side.lower(),
            "size": str(quantity),
            "orderType": order_type.lower()
        }
        
        if price is not None:
            payload["price"] = str(price)
            
        if time_in_force:
            payload["timeInForce"] = time_in_force.lower()
            
        response = await self._request("POST", "/exchange", data=payload, authenticated=True)
        return response
```

### 3.3 Backpack API Client

```python
class BackpackAPI(ExchangeAPI):
    def __init__(self, api_config: dict, secrets: dict):
        super().__init__(api_config, secrets)
        self.base_url = api_config.get("base_url", "https://api.backpack.exchange")
        self.ws_url = api_config.get("ws_url", "wss://ws.backpack.exchange")
        self.api_key = secrets.get("BACKPACK_API_KEY")
        self.private_key = secrets.get("BACKPACK_PRIVATE_KEY")
        self.window = api_config.get("window", 5000)  # Default 5 seconds
        
    async def _authenticate_request(self, method: str, endpoint: str, data: dict) -> dict:
        """Sign a request using ED25519 keypair."""
        if not self.api_key or not self.private_key:
            raise AuthenticationError("API key or private key not provided")
            
        timestamp = int(time.time() * 1000)
        
        # Determine instruction type based on endpoint and method
        instruction = self._get_instruction_type(method, endpoint)
        
        # Sort parameters alphabetically and format as query string
        if data:
            sorted_params = "&".join([f"{k}={v}" for k, v in sorted(data.items())])
        else:
            sorted_params = ""
            
        # Create signing string
        signing_string = f"instruction={instruction}&{sorted_params}timestamp={timestamp}&window={self.window}"
        
        # Sign using ED25519
        import nacl.signing
        
        # Decode private key from base64 if needed
        import base64
        if isinstance(self.private_key, str):
            key_bytes = base64.b64decode(self.private_key)
        else:
            key_bytes = self.private_key
            
        signing_key = nacl.signing.SigningKey(key_bytes)
        signature = signing_key.sign(signing_string.encode()).signature
        signature_b64 = base64.b64encode(signature).decode()
        
        # Return headers and data unchanged
        return {
            "headers": {
                "X-Timestamp": str(timestamp),
                "X-Window": str(self.window),
                "X-API-Key": self.api_key,
                "X-Signature": signature_b64
            }
        }
        
    def _get_instruction_type(self, method: str, endpoint: str) -> str:
        """Determine the instruction type based on endpoint and method."""
        endpoint_path = endpoint.rstrip("/").split("/")[-1]
        
        # Map endpoints to instruction types
        instruction_map = {
            ("GET", "capital"): "balanceQuery",
            ("GET", "capital/deposits"): "depositQueryAll",
            ("GET", "capital/address"): "depositAddressQuery",
            ("GET", "capital/withdrawals"): "withdrawalQueryAll",
            ("POST", "capital/withdraw"): "withdraw",
            ("GET", "order"): "orderQuery",
            ("POST", "order"): "orderExecute",
            ("DELETE", "order"): "orderCancel",
            ("GET", "orders"): "orderQueryAll",
            ("DELETE", "orders"): "orderCancelAll",
            ("GET", "history/orders"): "orderHistoryQueryAll",
            ("GET", "history/fills"): "fillHistoryQueryAll"
        }
        
        key = (method, endpoint_path)
        if key in instruction_map:
            return instruction_map[key]
            
        raise ValueError(f"Unknown instruction type for {method} {endpoint}")
            
    async def fetch_funding_rate(self, symbol: str) -> float:
        """Fetch the current funding rate for a symbol."""
        response = await self._request("GET", "/api/v1/ticker", data={"symbol": symbol})
        
        # Extract funding rate from ticker if available
        if response and "fundingRate" in response:
            return float(response["fundingRate"])
        
        raise DataError("Funding rate not available in response")
    
    async def place_order(self, symbol: str, side: str, quantity: float, 
                        order_type: str = "LIMIT", price: float = None,
                        time_in_force: str = "GTC") -> dict:
        """Place an order on Backpack."""
        # Validate parameters
        if order_type == "LIMIT" and price is None:
            raise ValueError("Price is required for limit orders")
            
        payload = {
            "symbol": symbol,
            "side": side.upper(),
            "quantity": str(quantity),
            "type": order_type.upper()
        }
        
        if price is not None:
            payload["price"] = str(price)
            
        if time_in_force:
            payload["timeInForce"] = time_in_force.upper()
            
        response = await self._request("POST", "/api/v1/order", data=payload, authenticated=True)
        return response
```

### 3.4 WebSocket Management

```python
class WebSocketManager:
    def __init__(self, api_client):
        self.api_client = api_client
        self.connections = {}
        self.callbacks = {}
        self.running_tasks = {}
        self.lock = asyncio.Lock()
        
    async def connect(self, stream: str, is_private: bool = False) -> bool:
        """Establish a WebSocket connection for a specific stream."""
        async with self.lock:
            if stream in self.connections and not self.connections[stream].closed:
                return True
                
            # Build WebSocket URL
            if isinstance(self.api_client, HyperliquidAPI):
                ws_url = f"{self.api_client.ws_url}/{stream}"
                
                # Add authentication if needed
                if is_private:
                    auth_params = await self.api_client._authenticate_request("GET", f"/ws/{stream}", {})
                    # Append auth params to URL
                    
            elif isinstance(self.api_client, BackpackAPI):
                ws_url = self.api_client.ws_url
                # Backpack uses message-based subscription
                
            try:
                self.connections[stream] = await websockets.connect(ws_url)
                
                # After connection, subscribe/authenticate if needed
                if isinstance(self.api_client, BackpackAPI):
                    # Backpack requires explicit subscription message
                    subscribe_msg = {
                        "method": "SUBSCRIBE",
                        "params": [stream],
                        "id": int(time.time() * 1000)
                    }
                    await self.connections[stream].send(json.dumps(subscribe_msg))
                
                # Start message handling task
                self.running_tasks[stream] = asyncio.create_task(self._message_handler(stream))
                return True
                
            except Exception as e:
                logger.error(f"WebSocket connection error: {str(e)}")
                return False
                
    async def _message_handler(self, stream: str):
        """Handle incoming WebSocket messages."""
        try:
            while True:
                if stream not in self.connections or self.connections[stream].closed:
                    break
                    
                ws = self.connections[stream]
                
                try:
                    message = await ws.recv()
                    data = json.loads(message)
                    
                    # Process the message
                    if stream in self.callbacks:
                        for callback in self.callbacks.get(stream, []):
                            try:
                                await callback(data)
                            except Exception as e:
                                logger.error(f"Callback error: {str(e)}")
                                
                except websockets.ConnectionClosed:
                    logger.warning(f"WebSocket connection closed: {stream}")
                    break
                    
                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON in WebSocket message: {message}")
                    continue
                    
        finally:
            # Clean up connection if handler exits
            await self._cleanup_connection(stream)
                
    async def _cleanup_connection(self, stream: str):
        """Clean up WebSocket connection resources."""
        async with self.lock:
            if stream in self.connections and not self.connections[stream].closed:
                try:
                    await self.connections[stream].close()
                except Exception:
                    pass
                    
            if stream in self.running_tasks:
                task = self.running_tasks.pop(stream)
                if not task.done():
                    task.cancel()
                    
            self.connections.pop(stream, None)
            
    async def subscribe(self, stream: str, callback, is_private: bool = False):
        """Subscribe to a WebSocket stream with a callback."""
        async with self.lock:
            if stream not in self.callbacks:
                self.callbacks[stream] = []
                
            self.callbacks[stream].append(callback)
            
            # Ensure connection is established
            if stream not in self.connections or self.connections[stream].closed:
                await self.connect(stream, is_private)
                
    async def unsubscribe(self, stream: str, callback = None):
        """Unsubscribe from a WebSocket stream."""
        async with self.lock:
            if stream in self.callbacks:
                if callback:
                    self.callbacks[stream] = [cb for cb in self.callbacks[stream] if cb != callback]
                else:
                    self.callbacks[stream] = []
                    
            # Close connection if no more callbacks
            if not self.callbacks.get(stream):
                await self._cleanup_connection(stream)
                
    async def close_all(self):
        """Close all WebSocket connections."""
        streams = list(self.connections.keys())
        for stream in streams:
            await self._cleanup_connection(stream)
```

## 4. Implementation Recommendations

### 4.1 HyperLiquid-Specific Considerations

1. **Performance Optimization**
   - Take advantage of HyperLiquid's low latency (median 0.2s)
   - Implement connection co-location where possible
   - Optimize WebSocket message processing for minimal latency

2. **Special Features**
   - Handle Hyperps (HyperLiquid-only perps) properly in funding calculations
   - Account for open interest caps on newer markets
   - Implement proper handling for the 8-hour EWMA oracle price mechanism

3. **Authentication Robustness**
   - Implement proper EIP-712 signing with robust error handling
   - Handle nonce management correctly to prevent transaction failures
   - Securely store and access Ethereum private keys

### 4.2 Backpack-Specific Considerations

1. **Timing and Windows**
   - Properly handle the timestamp and window parameters
   - Ensure system clock is accurately synchronized
   - Consider variable window sizes based on operation criticality

2. **ED25519 Integration**
   - Implement correct ED25519 key handling and signing
   - Use appropriate libraries (e.g., PyNaCl) for cryptographic operations
   - Securely generate and store key pairs

3. **WebSocket Management**
   - Implement 30-second keepalive pings
   - Handle subscription confirmation messages
   - Properly manage reconnection with exponential backoff

### 4.3 General API Improvements

1. **Error Handling**
   - Implement typed error classes for different error scenarios
   - Add appropriate retry logic with backoff for transient errors
   - Log detailed error information for troubleshooting

2. **Rate Limiting**
   - Track request timestamps to avoid exceeding rate limits
   - Implement token bucket algorithm for precise rate control
   - Add adaptive rate limiting based on observed API behavior

3. **Testing Improvements**
   - Create comprehensive test fixtures based on actual API responses
   - Implement integration tests against exchange testnet environments
   - Add performance tests to validate latency requirements

## 5. Implementation Timeline

| Week | HyperLiquid Tasks | Backpack Tasks | Shared Tasks |
|------|------------------|---------------|-------------|
| 1 | EIP-712 signing, REST endpoints | ED25519 signing, API research | Base abstraction, error handling |
| 2 | WebSocket integration, order execution | Basic REST endpoints, authorization | WebSocket manager, data parsing |
| 3 | Full order management, funding data | Order execution, WebSocket integration | Integration testing, monitoring |
| 4 | Performance optimization, error handling | Full order management, funding data | End-to-end testing, documentation |
| 5 | Extended features, thorough testing | Extended features, thorough testing | System integration, deployment prep | 