# Implementation Strategy - Leveraging CyberDelta Knowledge

## Overview
This document outlines how to leverage knowledge from CyberDelta's Backpack implementation to build a clean Hummingbot connector, while avoiding architectural incompatibilities.

## Key Insights from CyberDelta's Implementation

### 1. Authentication - Ed25519 Signing
CyberDelta shows Backpack uses Ed25519 signatures. We can reference this logic:

```python
# From CyberDelta bp_auth.py insights:
# - Uses Ed25519 private key signing
# - Headers: X-API-Key, X-Timestamp, X-Signature, X-Window
# - Signature payload: timestamp + method + path + body
# - WebSocket auth uses similar pattern

# Clean implementation for Hummingbot:
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519
import base64

class BackpackAuth(AuthBase):
    def _sign_payload(self, payload: str) -> str:
        # Load private key from base64 API secret
        private_key_bytes = base64.b64decode(self.api_secret)
        private_key = ed25519.Ed25519PrivateKey.from_private_bytes(private_key_bytes)

        # Sign the payload
        signature = private_key.sign(payload.encode())

        # Return base64 encoded signature
        return base64.b64encode(signature).decode()
```

### 2. WebSocket Subscription Format
From CyberDelta's bp_ws_router.py and models, we know:

```python
# Backpack WebSocket subscription format:
{
    "method": "subscribe",
    "params": {
        "subscriptions": ["depth@BTC_USDC", "trades@BTC_USDC"]
    }
}

# Private channels require authentication:
# - "account.orders"
# - "account.balances"
# - "account.positions"
# - "account.transactions"
```

### 3. API Endpoints and Structure
From openapi_backpack.json and CyberDelta constants:

```python
# Key endpoints discovered:
BASE_URL = "https://api.backpack.exchange/"

# Public endpoints:
GET  /api/v1/capital        # Exchange info and trading rules
GET  /api/v1/ticker         # Price ticker
GET  /api/v1/depth          # Order book
GET  /api/v1/klines         # Candlesticks
GET  /api/v1/trades         # Recent trades
GET  /api/v1/time           # Server time

# Private endpoints (require auth):
POST /api/v1/order          # Place order
DELETE /api/v1/order        # Cancel order
GET  /api/v1/orders         # Open orders
GET  /api/v1/orderHistory   # Order history
GET  /api/v1/fills          # Trade fills
GET  /api/v1/balances       # Account balances
```

### 4. Order and Market Structure
From CyberDelta mappers and models:

```python
# Order placement request:
{
    "symbol": "BTC_USDC",      # Note: underscore format
    "side": "Buy" | "Sell",    # Capitalized
    "orderType": "Limit" | "Market",
    "quantity": "0.001",        # String format
    "price": "45000.00",        # String format for Limit
    "clientId": "HBOT-12345",   # Optional client order ID
    "postOnly": False,          # Optional
    "timeInForce": "GTC"        # GTC, IOC, FOK
}

# Order response:
{
    "orderId": "abc123",
    "clientId": "HBOT-12345",
    "status": "New",
    "symbol": "BTC_USDC",
    "side": "Buy",
    "orderType": "Limit",
    "quantity": "0.001",
    "price": "45000.00",
    "executedQuantity": "0",
    "createdAt": 1234567890000
}
```

### 5. Trading Rules Structure
From CyberDelta's market data mappers:

```python
# Market info contains filters:
{
    "symbol": "BTC_USDC",
    "baseAsset": "BTC",
    "quoteAsset": "USDC",
    "status": "TRADING",
    "filters": {
        "minQty": "0.0001",      # Min order size
        "maxQty": "100",         # Max order size
        "stepSize": "0.0001",    # Size increment
        "tickSize": "0.01",      # Price increment
        "minNotional": "10",     # Min order value
        "pricePrecision": 2,     # Price decimal places
        "quantityPrecision": 4   # Quantity decimal places
    }
}
```

### 6. WebSocket Message Formats
From CyberDelta's WebSocket context and mappers:

```python
# Order update message:
{
    "type": "orderUpdate",
    "data": {
        "orderId": "abc123",
        "clientId": "HBOT-12345",
        "status": "Filled",
        "executedQuantity": "0.001",
        "remainingQuantity": "0",
        "lastTradePrice": "45000.00",
        "lastTradeQuantity": "0.001"
    }
}

# Balance update:
{
    "type": "balanceUpdate",
    "data": {
        "asset": "USDC",
        "free": "10000.00",
        "locked": "500.00",
        "total": "10500.00"
    }
}

# Trade update:
{
    "type": "trade",
    "data": {
        "tradeId": "xyz789",
        "orderId": "abc123",
        "symbol": "BTC_USDC",
        "price": "45000.00",
        "quantity": "0.001",
        "fee": "0.045",
        "feeAsset": "USDC",
        "side": "Buy",
        "timestamp": 1234567890000
    }
}
```

## Implementation Strategy

### Phase 1: Foundation (Day 1-2)
Start with minimal structure that can connect and authenticate:

```python
# 1. Create constants file with discovered endpoints
# 2. Implement Ed25519 auth based on CyberDelta pattern
# 3. Create basic exchange class with properties
# 4. Implement network check and time sync
```

### Phase 2: Trading Core (Day 3-4)
Implement order lifecycle:

```python
# 1. _place_order() using discovered format
# 2. _place_cancel() with DELETE method
# 3. _update_balances() parsing balance format
# 4. _update_trading_rules() from /capital endpoint
```

### Phase 3: WebSocket Integration (Day 5-6)
Connect data streams:

```python
# 1. Order book data source with depth@symbol subscription
# 2. User stream with account.* subscriptions
# 3. Message parsers for each event type
# 4. Reconnection logic
```

### Phase 4: Testing & Polish (Day 7-8)
Ensure robustness:

```python
# 1. Unit tests for each component
# 2. Integration tests with mock responses
# 3. Error handling and edge cases
# 4. Documentation
```

## Key Differences from CyberDelta

### What NOT to Copy:
1. **Service-oriented architecture** - Use direct API calls
2. **Pydantic models** - Use dictionaries
3. **Complex WebSocket routing** - Use simple message handling
4. **Multiple mapper layers** - Parse directly in handlers
5. **Circuit breakers** - Use simple retry logic
6. **Python 3.13 features** - Stick to Python 3.10

### What to Reference:
1. **API endpoints and parameters**
2. **Authentication logic** (adapt for Python 3.10)
3. **WebSocket subscription formats**
4. **Message structures**
5. **Error codes and handling**
6. **Rate limit values**

## Code Adaptation Examples

### CyberDelta Pattern (Don't Copy):
```python
# Complex service layer
class BackpackTradingService:
    def __init__(self,
                 request_builder: RequestBuilder,
                 order_mapper: BackpackOrderMapper,
                 validator: OrderValidator):
        # Multiple dependencies

    async def place_order(self, args: PlaceOrderArgs) -> Order:
        validated = self.validator.validate(args)
        request = self.request_builder.build(validated)
        response = await self.http_client.post(request)
        return self.order_mapper.map_to_domain(response)
```

### Hummingbot Pattern (Do This):
```python
# Direct implementation
async def _place_order(self, order_id, trading_pair, amount, trade_type, order_type, price=None):
    # Build request directly
    params = {
        "symbol": convert_to_exchange_trading_pair(trading_pair),
        "side": "Buy" if trade_type == TradeType.BUY else "Sell",
        "orderType": order_type.name.capitalize(),
        "quantity": str(amount),
        "clientId": order_id,
    }
    if price:
        params["price"] = str(price)

    # Single API call
    response = await self._api_post("/api/v1/order", params)
    return response["orderId"], self.current_timestamp
```

## Error Handling Strategy

Based on CyberDelta's error mapper insights:

```python
# Common Backpack errors to handle:
ERROR_CODES = {
    "INSUFFICIENT_BALANCE": "Insufficient balance",
    "ORDER_NOT_FOUND": "Order does not exist",
    "INVALID_SYMBOL": "Invalid symbol",
    "MIN_NOTIONAL": "Order value below minimum",
    "RATE_LIMIT": "Rate limit exceeded",
    "INVALID_SIGNATURE": "Invalid API signature",
    "EXPIRED_TIMESTAMP": "Request timestamp expired",
}

# Simple error handler:
def _handle_error_response(self, response: Dict[str, Any]):
    error_code = response.get("code")
    error_msg = response.get("msg", "Unknown error")

    if error_code == "INSUFFICIENT_BALANCE":
        raise InsufficientBalanceError(error_msg)
    elif error_code == "ORDER_NOT_FOUND":
        # Order already cancelled, not an error
        return
    else:
        raise IOError(f"API Error {error_code}: {error_msg}")
```

## Performance Optimizations

Learn from CyberDelta but simplify:

1. **Rate Limiting**: Use AsyncThrottler with discovered limits
2. **WebSocket Reconnection**: Simple exponential backoff
3. **Order Tracking**: Use ClientOrderTracker as-is
4. **Caching**: Cache trading rules for 30 minutes
5. **Batching**: Don't over-engineer, Hummingbot handles this

## Testing Strategy

Create focused tests based on discovered message formats:

```python
# Mock WebSocket messages from CyberDelta insights
MOCK_ORDER_UPDATE = {
    "type": "orderUpdate",
    "data": {
        "orderId": "test123",
        "clientId": "HBOT-test",
        "status": "Filled",
        "executedQuantity": "1.0"
    }
}

# Test with real message structures
async def test_process_order_update(self):
    self.exchange._process_order_message(MOCK_ORDER_UPDATE)
    # Assert order state updated correctly
```

## Development Checklist

- [ ] Review CyberDelta's bp_auth.py for Ed25519 implementation
- [ ] Extract API endpoints from openapi_backpack.json
- [ ] Note WebSocket subscription formats from bp_ws_router.py
- [ ] Identify error codes from bp_error_mapper.py
- [ ] Map order statuses from mapper classes
- [ ] Document rate limits from bp_rate_limit_strategy.py
- [ ] Create simple Python 3.10 compatible implementations
- [ ] Test with mock data based on real message formats
- [ ] Verify no Python 3.13 features used
- [ ] Ensure no Pydantic v2 dependencies

## Summary

By leveraging CyberDelta's deep knowledge of the Backpack API while avoiding its complex architecture, we can build a clean, maintainable connector in ~8 days that:

1. Uses proven API formats and endpoints
2. Implements correct Ed25519 authentication
3. Handles all message types properly
4. Follows Hummingbot's simple patterns
5. Remains Python 3.10 compatible
6. Avoids circular dependencies
7. Maintains high performance with minimal complexity

The key is to extract the "what" (API specs) from CyberDelta while ignoring the "how" (complex implementation patterns).
