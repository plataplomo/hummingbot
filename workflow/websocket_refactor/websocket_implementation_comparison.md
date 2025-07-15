# WebSocket Implementation Analysis: Backpack vs Hyperliquid

## Executive Summary

This document provides a detailed comparison of WebSocket implementations between Backpack and Hyperliquid exchanges in the CyberDeltaEngine. Both exchanges implement WebSocket streaming for real-time market data and account updates, but with significantly different architectural approaches, authentication methods, and message structures.

## 1. Message Type Handling

### Backpack Exchange

**Message Categories:**
- **Market Data:**
  - `depth.*` - Order book updates (e.g., `depth.SOL_USDC`)
  - `ticker.*` - Ticker updates (e.g., `ticker.BTC_USDC`)
- **Private Account Data:**
  - `fills` - Trade executions
  - `orders` - Order updates
  - `positionUpdate` - Position changes

**Message Structure:**
```json
{
  "topic": "depth.SOL_USDC",
  "data": {
    "lastUpdateId": "123456",
    "b": [["100.5", "10.0"], ["100.4", "20.0"]],
    "a": [["100.6", "15.0"], ["100.7", "25.0"]]
  }
}
```

### Hyperliquid Exchange

**Message Categories:**
- **Market Data:**
  - `l2Book` - Order book snapshots
  - `trades` - Public trade stream
  - `allMids` - All mid-prices
  - `candle` - Kline/candle data
- **Private Account Data:**
  - `userEvents` - Unified channel for fills, orders, and positions

**Message Structure:**
```json
{
  "channel": "l2Book",
  "data": {
    "coin": "ETH",
    "levels": [[{"px": "2500.0", "sz": "10.0", "n": 5}], [{"px": "2501.0", "sz": "15.0", "n": 3}]],
    "time": 1699999999999
  }
}
```

## 2. Pydantic Models

### Backpack Models

**Subscription Payload:**
```python
class BackpackRawWsSubscriptionRequest(BaseModel):
    method: Literal["SUBSCRIBE", "UNSUBSCRIBE"]
    params: list[RawBpNonEmptyStringMax128]  # Stream names
    signature: tuple[str, str, str, str] | None  # For private streams
```

**Event Models:**
- `BackpackRawDepthUpdateEvent` - Order book updates
- `BackpackRawTickerEvent` - Ticker data
- `BackpackRawPublicTradeEvent` - Trade events
- `BackpackRawOrderUpdate` - Order status changes
- `BackpackRawPositionUpdate` - Position updates

### Hyperliquid Models

**Subscription Payloads:**
```python
class HyperliquidRawWsSubscribeRequest(BaseModel):
    method: Literal["subscribe", "unsubscribe"]
    subscription: Union[
        HyperliquidRawWsL2BookSubscriptionPayload,
        HyperliquidRawWsTradesSubscriptionPayload,
        HyperliquidRawWsUserEventsSubscriptionPayload,
        HyperliquidRawWsCandleSubscriptionPayload,
        HyperliquidRawWsAllMidsSubscriptionPayload
    ]
```

**Event Models:**
- `HyperliquidRawWsBookUpdate` - Order book snapshots
- `HyperliquidRawWsTradeEvent` - Public trades
- `HyperliquidRawWsFillEvent` - User fills
- `HyperliquidRawWsOrderUpdate` - Order updates
- `HyperliquidRawWsPositionUpdateEvent` - Position changes

## 3. Subscription Payload Construction

### Backpack

**Public Streams:**
```json
{
  "method": "SUBSCRIBE",
  "params": ["depth.SOL_USDC"]
}
```

**Private Streams (with ED25519 signature):**
```json
{
  "method": "SUBSCRIBE",
  "params": ["account"],
  "signature": ["<api_key>", "<signature>", "<timestamp>", "<window>"]
}
```

**Authentication Process:**
1. Generate timestamp and window (5000ms)
2. Create string to sign: `stream={type}&timestamp={ts}&window={window}`
3. Sign with ED25519 private key
4. Base64 encode signature

### Hyperliquid

**Public Streams:**
```json
{
  "method": "subscribe",
  "subscription": {
    "type": "l2Book",
    "coin": "ETH"
  }
}
```

**Private Streams (wallet address required):**
```json
{
  "method": "subscribe",
  "subscription": {
    "type": "userEvents",
    "user": "0x1234...abcd"
  }
}
```

**Key Difference:** Hyperliquid uses wallet address for user identification instead of API key authentication.

## 4. Differences and Commonalities

### Key Differences

1. **Authentication:**
   - Backpack: ED25519 signature-based authentication
   - Hyperliquid: Wallet address-based identification (no signature for WS)

2. **Message Routing:**
   - Backpack: Topic-based routing (e.g., `depth.SYMBOL`)
   - Hyperliquid: Channel-based routing with embedded symbol data

3. **User Events:**
   - Backpack: Separate channels for fills, orders, positions
   - Hyperliquid: Unified `userEvents` channel with event type discrimination

4. **Method Casing:**
   - Backpack: Uppercase methods (`SUBSCRIBE`, `UNSUBSCRIBE`)
   - Hyperliquid: Lowercase methods (`subscribe`, `unsubscribe`)

5. **Order Book Format:**
   - Backpack: Array of [price, quantity] tuples
   - Hyperliquid: Array of objects with `px`, `sz`, `n` fields

### Commonalities

1. **Pydantic Validation:** Both use strict Pydantic models for type safety
2. **Message Router Pattern:** Both implement a router to handle incoming messages
3. **Raw Message Handlers:** Both have dedicated handlers for validating raw payloads
4. **Transformation Pipeline:** Both transform raw models to internal domain models
5. **Error Handling:** Both use custom APIError exceptions for validation failures

## 5. Error Handling Approaches

### Backpack

- Comprehensive error code enum (`BackpackAPIErrorCode`)
- Specific error codes for different scenarios
- Error suppression for repeated unroutable messages
- Structured error responses with codes

### Hyperliquid

- String-based error messages (no official error codes)
- Pattern matching on error strings
- Fallback to generic errors for unknown messages
- More defensive approach due to lack of formal error specification

## 6. Architectural Recommendations

### Strengths of Current Implementation

1. **Type Safety:** Strong Pydantic validation at boundaries
2. **Separation of Concerns:** Clear separation between raw and internal models
3. **Extensibility:** Router pattern allows easy addition of new message types
4. **Error Resilience:** Comprehensive error handling and logging

### Areas for Improvement

1. **Unified Interface:** Consider abstracting common WebSocket operations
2. **Reconnection Logic:** Implement robust reconnection with exponential backoff
3. **Message Buffering:** Add buffering for high-frequency updates
4. **Performance Monitoring:** Add metrics for latency and message processing time
5. **Rate Limiting:** Implement client-side rate limiting for subscriptions

### Security Considerations

1. **Authentication:** Backpack's signature-based auth is more secure for WS
2. **Input Validation:** Both exchanges properly validate all inputs
3. **Sensitive Data:** Proper use of SecretStr for keys and credentials
4. **Logging:** Careful to avoid logging sensitive information

## Conclusion

Both implementations follow solid architectural patterns with strong type safety and validation. The main differences stem from the exchanges' different API designs rather than implementation choices. The current architecture provides a good foundation for reliable, secure WebSocket communication with both exchanges.
