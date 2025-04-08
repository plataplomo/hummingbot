# API Implementation - Hyperliquid

This document provides concrete specifications for implementing the Hyperliquid API client, focusing on actual endpoints, authentication methods, and required functionality.

## 1. Research Findings

### 1.1 Hyperliquid API Overview

Hyperliquid's API consists of:
- REST API endpoints served at `https://api.hyperliquid.xyz`
- WebSocket endpoints at `wss://api.hyperliquid.xyz/ws`
- Authentication via EIP-712 message signing for private endpoints
- Rate limits (documented per endpoint below)

### 1.2 Key Endpoints

| Endpoint | Method | Purpose | Rate Limit | Authentication |
|----------|--------|---------|------------|----------------|
| `/info` | POST | Market data, funding rates, indices | 100/min | No |
| `/exchange` | POST | Order placement, cancellation | 60/min | Yes |
| `/user` | POST | Account data, positions, balances | 60/min | Yes |
| `/ticker` | WebSocket | Real-time ticker updates | N/A | No |
| `/orderbook` | WebSocket | Orderbook updates | N/A | No |
| `/trades` | WebSocket | Market trades | N/A | No |
| `/user` | WebSocket | User account updates | N/A | Yes |

### 1.3 Authentication Mechanism

Hyperliquid uses EIP-712 message signing with Ethereum wallets:

1. Create payload containing method, path, body, timestamp, and nonce
2. Sign using EIP-712 with wallet's private key using `eth_account` library
3. Include signature in request header (`X-HL-Signature`)

## 2. Critical Implementation Components

### 2.1 Base Client Class

```python
class HyperliquidAPI(ExchangeAPI):
    def __init__(self, api_config: dict, secrets: dict):
        self.base_url = api_config.get("base_url", "https://api.hyperliquid.xyz")
        self.ws_url = api_config.get("ws_url", "wss://api.hyperliquid.xyz/ws")
        self.private_key = secrets.get("HYPERLIQUID_WALLET_PRIVATE_KEY")
        self.session = None
        self.ws_connections = {}
        self.ws_subscriptions = {}
        self.request_timestamps = {}  # For rate limiting
        self.nonce_counter = 0  # For signature nonce
        self.lock = asyncio.Lock()  # Thread safety
```

### 2.2 Authentication Method

```python
async def _sign_request(self, method: str, path: str, body: dict) -> dict:
    """Sign a request using EIP-712 and wallet private key."""
    if not self.private_key:
        raise ValueError("Private key not provided")
    
    async with self.lock:
        self.nonce_counter += 1
        nonce = self.nonce_counter
    
    timestamp = int(time.time() * 1000)
    
    # Create message to sign (method, path, body, timestamp, nonce)
    # Format as EIP-712 compatible structure
    # Use eth_account.Account.sign_message() to generate signature
    
    # Return signature, timestamp, nonce and original body
```

### 2.3 Rate Limiting

```python
async def _handle_rate_limit(self, endpoint: str, limit: int, window: int = 60):
    """Handle rate limiting with exponential backoff."""
    # Track request timestamps per endpoint
    # Check if current request would exceed limit
    # If rate-limited, implement exponential backoff with jitter
    # Raise exception after max retries
```

### 2.4 HTTP Request Method

```python
async def _request(self, method: str, endpoint: str, data: dict = None, 
                 authenticated: bool = False, rate_limit: int = 100) -> dict:
    """Make a request to the Hyperliquid API with error handling."""
    # Initialize session if needed
    # Handle rate limiting
    # Add authentication if required
    # Make the request with proper error handling
    # Parse response and check for API-level errors
    # Return response data
```

### 2.5 WebSocket Management

```python
async def _connect_websocket(self, topic: str) -> None:
    """Connect to a WebSocket feed and set up message handler."""
    # Create WebSocket connection
    # Set up message handler task
    # Implement reconnection logic

async def _ws_message_handler(self, topic: str) -> None:
    """Handle messages from a WebSocket connection."""
    # Process incoming messages
    # Parse JSON data
    # Route to appropriate callbacks
    # Handle errors and reconnection

async def subscribe(self, topic: str, callback) -> None:
    """Subscribe to a WebSocket topic with a callback."""
    # Register callback for topic
    # Connect to WebSocket if needed

async def unsubscribe(self, topic: str, callback=None) -> None:
    """Unsubscribe from a WebSocket topic."""
    # Remove callback(s)
    # Close connection if no remaining subscriptions
```

## 3. Core Functionality Implementation

### 3.1 Market Data Methods

```python
async def fetch_funding_rate(self, symbol: str) -> float:
    """Fetch the current funding rate for a symbol."""
    payload = {"type": "funding", "asset": symbol}
    response = await self._request("POST", "/info", data=payload)
    # Extract and return funding rate

async def fetch_ticker(self, symbol: str) -> dict:
    """Fetch ticker data for a symbol."""
    payload = {"type": "ticker", "asset": symbol}
    response = await self._request("POST", "/info", data=payload)
    # Parse and return standardized ticker object

async def fetch_orderbook(self, symbol: str, depth: int = 10) -> dict:
    """Fetch orderbook data for a symbol."""
    payload = {"type": "orderbook", "asset": symbol, "depth": depth}
    response = await self._request("POST", "/info", data=payload)
    # Parse and return standardized orderbook object
```

### 3.2 Account Methods

```python
async def get_balances(self) -> dict:
    """Fetch account balances."""
    payload = {"type": "balances"}
    response = await self._request("POST", "/user", data=payload, 
                                authenticated=True, rate_limit=60)
    # Parse and return standardized balance objects

async def get_positions(self) -> list:
    """Fetch current positions."""
    payload = {"type": "positions"}
    response = await self._request("POST", "/user", data=payload, 
                                authenticated=True, rate_limit=60)
    # Parse and return standardized position objects
```

### 3.3 Order Methods

```python
async def place_order(self, symbol: str, side: str, quantity: float, 
                    order_type: str = "limit", price: float = None,
                    time_in_force: str = "gtc") -> dict:
    """Place an order on Hyperliquid."""
    # Validate parameters
    # Create order payload
    # Send authenticated request to /exchange
    # Parse and return order details

async def cancel_order(self, symbol: str, order_id: str) -> bool:
    """Cancel an order on Hyperliquid."""
    payload = {"type": "cancel_order", "symbol": symbol, "orderId": order_id}
    response = await self._request("POST", "/exchange", data=payload, 
                                authenticated=True, rate_limit=60)
    # Return success status

async def get_order_status(self, symbol: str, order_id: str) -> dict:
    """Check the status of an order."""
    payload = {"type": "order_status", "symbol": symbol, "orderId": order_id}
    response = await self._request("POST", "/exchange", data=payload, 
                                authenticated=True, rate_limit=60)
    # Parse and return order status
```

## 4. Error Handling

Key error types to implement:
- `APIError`: Base class for API errors
- `ConnectionError`: Connection issues
- `AuthenticationError`: Auth failures
- `RateLimitExceeded`: Rate limit reached
- `ServerError`: 5xx responses
- `OrderError`: Order placement/cancellation failures

Implementation will include specific error codes, retry logic, and meaningful error messages.

## 5. Testing Strategy

### 5.1 Unit Tests

Focus on testing:
- Request signing with known inputs and outputs
- Response parsing with mocked data
- Rate limiting logic
- Error handling for various scenarios

### 5.2 Integration Tests

With mock server:
- Authentication flow
- WebSocket connection and message handling
- Complete request/response cycles

### 5.3 Live Tests

With testnet:
- Market data retrieval
- Order placement and cancellation (small size)
- WebSocket subscription and event handling

## 6. Implementation Timeline

| Task | Days | Details |
|------|------|---------|
| Authentication | 1 | Implement EIP-712 signing |
| Rate Limiting | 1 | Add rate tracking and backoff |
| Market Data Endpoints | 2 | Implement funding, ticker, orderbook |
| Account Endpoints | 1 | Implement balance, positions |
| Order Endpoints | 2 | Implement place, cancel, status |
| WebSocket Handling | 2 | Add connection, subscription management |
| Error Handling | 1 | Implement comprehensive error types |
| Testing | 3 | Unit and integration tests |
| Documentation | 1 | Create usage examples |

## 7. Security Considerations

- Use environment variables (.env) for API keys
- Never log private keys or signatures
- Implement nonce tracking to prevent replay attacks
- Use testnet accounts during development
- Audit signing code carefully before production use
- Isolate signing logic for easier security review 