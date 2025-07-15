# WebSocket API Contract Documentation

This document provides comprehensive documentation of the current WebSocket API contract for CyberDeltaEngine before removing backwards compatibility. This serves as a reference for understanding the legacy formats and validation patterns that will be deprecated.

## Table of Contents

1. [Overview](#overview)
2. [Backpack WebSocket API](#backpack-websocket-api)
3. [Hyperliquid WebSocket API](#hyperliquid-websocket-api)
4. [Base WebSocket Router](#base-websocket-router)
5. [Format Detection Functions](#format-detection-functions)
6. [Context Extraction Methods](#context-extraction-methods)
7. [Union Types](#union-types)
8. [Test Patterns](#test-patterns)
9. [Migration Notes](#migration-notes)

## Overview

The WebSocket API currently supports multiple envelope formats for backward compatibility:

### Backpack Exchange
- **Current format** (post 2024-01-16): `{"stream": "<stream>", "data": "<payload>"}`
- **Legacy topic format** (pre 2024-01-16): `{"topic": "<topic>", "data": "<payload>"}`
- **Legacy flat format** (very old): `{"type": "<type>", ...fields...}`

### Hyperliquid Exchange
- **Standard format**: `{"channel": "<channel>", "data": "<payload>"}`
- **User events format**: `{"channel": "userEvents", "data": {"fills": [], "orders": [], ...}}`

## Backpack WebSocket API

### File: `cyberdelta/apis/backpack/models/bp_ws_envelope.py`

#### 1. Legacy Envelope Models

##### BackpackRawWebSocketEnvelope (Current Format)
```python
class BackpackRawWebSocketEnvelope(BaseModel):
    """Current API format (post 2024-01-16)"""
    stream: RawBpNonEmptyStringMax128  # "depth.SOL_USDC"
    data: dict[str, Any] | list[Any]   # Stream-specific payload
```

**Stream Format Patterns:**
- Public streams: `"depth.<symbol>"`, `"ticker.<symbol>"`, `"trade.<symbol>"`
- K-line streams: `"kline.<interval>.<symbol>"`
- Private streams: `"account.orderUpdate"`, `"account.positionUpdate.<symbol>"`
- Global streams: `"liquidation"`

**Validation Features:**
- Stream normalization (case, whitespace, format)
- Cross-field validation (stream type vs data structure)
- Performance monitoring with 1ms threshold
- Size constraints (max 1000 dict items, 10000 list items)

##### BackpackLegacyTopicEnvelope (Legacy Format)
```python
class BackpackLegacyTopicEnvelope(BaseModel):
    """Legacy topic format (pre 2024-01-16)"""
    topic: RawBpNonEmptyStringMax128   # "depth.SOL_USDC"
    data: dict[str, Any]               # Topic-specific payload
```

**Usage:** Supports old format during migration period.

##### BackpackLegacyTypeEnvelope (Very Old Format)
```python
class BackpackLegacyTypeEnvelope(BaseModel):
    """Legacy flat format (very old)"""
    type: RawBpNonEmptyStringMax128    # "fills", "orders", "positionUpdate"
    # Additional fields allowed directly in root
```

**Valid Types:** `{"fills", "orders", "positionUpdate", "orderUpdate"}`
**Configuration:** `extra="allow"` for flat structure

#### 2. Format Detection Function

```python
def detect_envelope_format(message: dict[str, Any]) -> str:
    """Detect which envelope format a message uses.

    Returns:
        One of: "stream", "topic", "type", "unknown"
    """
    if "stream" in message and "data" in message:
        return "stream"  # Current API format
    if "topic" in message and "data" in message:
        return "topic"   # Legacy format
    if "type" in message:
        return "type"    # Very old flat format
    return "unknown"     # Malformed message
```

#### 3. Validation Function

```python
def validate_backpack_envelope(message: dict[str, Any]) -> BackpackWebSocketMessage:
    """Validate and parse a Backpack WebSocket message envelope.

    Returns:
        Validated envelope model instance

    Raises:
        ValidationError: If message doesn't match any valid envelope format
        ValueError: If message format is unrecognized
    """
```

**Validation Flow:**
1. Detect format type
2. Try appropriate model validation
3. Fallback to current format
4. Raise descriptive error with expected formats

#### 4. Union Type

```python
BackpackWebSocketMessage = (
    BackpackRawWebSocketEnvelope |
    BackpackLegacyTopicEnvelope |
    BackpackLegacyTypeEnvelope
)
```

### File: `cyberdelta/apis/backpack/bp_ws_router.py`

#### 1. Routing Key Extraction

```python
def _extract_routing_key_from_envelope(self, envelope: BackpackWebSocketMessage) -> str | None:
    """Extract routing key from validated envelope."""
```

**Logic:**
- **Legacy Type**: Use `type` field directly (`"fills"`, `"orders"`, `"positionUpdate"`)
- **Stream/Topic**: Parse identifier and extract type using `ExchangeSpecificValidators.validate_backpack_topic()`

#### 2. Payload Extraction

```python
def _extract_payload_from_envelope(self, envelope: BackpackWebSocketMessage) -> dict[str, Any]:
    """Extract payload from validated envelope."""
```

**Logic:**
- **Legacy Type**: Return all fields except `"type"` (flat format)
- **Stream/Topic**: Return `data` field, wrap lists in `{"items": data}`

#### 3. Context Extraction Methods

```python
def get_symbol_from_context(self, context: dict[str, Any]) -> str | None:
    """Extract symbol from processing context."""
```

**Extraction Sources:**
1. **Envelope-based**: Extract from `validated_envelope.stream` or `validated_envelope.topic`
2. **Legacy fallback**: Extract from `original_message["topic"]`

**Symbol Extraction Flow:**
```python
def _extract_symbol_from_envelope(self, context: dict[str, Any]) -> str | None:
    envelope = context.get("validated_envelope")
    if isinstance(envelope, BackpackRawWebSocketEnvelope):
        stream = envelope.stream
    elif isinstance(envelope, BackpackLegacyTopicEnvelope):
        stream = envelope.topic
    # Parse using ExchangeSpecificValidators.validate_backpack_topic()
```

#### 4. Subscription Payload Construction

```python
def construct_subscription_payload(
    self,
    topic: str,
    signature_components: BackpackWsSignatureComponents | None = None,
) -> BackpackRawWsSubscriptionRequest:
    """Construct subscription payload for Backpack."""
```

**Format:**
- Public: `{"method": "SUBSCRIBE", "params": ["topic"]}`
- Private: `{"method": "SUBSCRIBE", "params": ["topic"], "signature": (key, sig, ts, window)}`

#### 5. Processor Setup

```python
def _setup_processors(self) -> None:
    """Setup Backpack-specific message processors."""
```

**Processors:**
- `"depth"` → `BackpackRawDepthUpdateEvent`
- `"ticker"` → `BackpackRawTickerEvent`
- `"trades"` → `BackpackRawPublicTradeEvent`
- `"orders"` → `BackpackRawOrderUpdate`
- `"positionUpdate"` → `BackpackRawPositionUpdate`
- `"fills"` → `BackpackRawFill`

## Hyperliquid WebSocket API

### File: `cyberdelta/apis/hyperliquid/models/hl_ws_envelope.py`

#### 1. Envelope Models

##### HyperliquidRawWebSocketEnvelope (Standard Format)
```python
class HyperliquidRawWebSocketEnvelope(BaseModel):
    """Standard WebSocket message envelope"""
    channel: str                       # "l2Book", "trades", etc.
    data: dict[str, Any] | list[Any]   # Channel-specific payload
```

**Channel Types:**
```python
class HyperliquidChannelType(StrEnum):
    L2_BOOK = "l2Book"
    TRADES = "trades"
    USER_EVENTS = "userEvents"
    ALL_MIDS = "allMids"
    NOTIFICATION = "notification"
    WEB_DATA2 = "webData2"
    ORDERS = "orders"      # Routed from userEvents
    FILLS = "fills"        # Routed from userEvents
```

**Validation Features:**
- Channel normalization (case variations)
- Cross-field validation (channel type vs data structure)
- Channel-specific field validation (`coin` required for market data)
- Performance monitoring

##### HyperliquidUserEventEnvelope (Specialized Format)
```python
class HyperliquidUserEventEnvelope(HyperliquidRawWebSocketEnvelope):
    """Specialized envelope for userEvents channel"""
    # Inherits fields but adds userEvents-specific validation
```

**Data Structure Validation:**
- Must be dict (not list)
- Cannot be empty
- Validates known event types: `{"fills", "orders", "positions", "ledgerUpdates"}`

#### 2. Format Detection Function

```python
def detect_hyperliquid_envelope_type(message: dict[str, Any]) -> str:
    """Detect Hyperliquid envelope type.

    Returns:
        One of: "standard", "userEvents", "unknown"
    """
    channel = message.get("channel")
    if not channel:
        return "unknown"
    if channel == "userEvents":
        return "userEvents"
    return "standard"
```

#### 3. Validation Function

```python
def validate_hyperliquid_envelope(message: dict[str, Any]) -> HyperliquidWebSocketMessage:
    """Validate and parse a Hyperliquid WebSocket message envelope."""
```

**Validation Flow:**
1. Check required fields (`channel`, `data`)
2. Detect envelope type
3. Use appropriate model (standard vs userEvents)
4. Re-raise with enhanced error context

#### 4. Union Type

```python
HyperliquidWebSocketMessage = (
    HyperliquidRawWebSocketEnvelope |
    HyperliquidUserEventEnvelope
)
```

### File: `cyberdelta/apis/hyperliquid/hl_ws_router.py`

#### 1. Routing Key Extraction

```python
def _extract_routing_key_from_envelope(self, envelope: HyperliquidWebSocketMessage) -> str | None:
    """Extract routing key from validated envelope."""
```

**Logic:**
- Direct channel mapping for most channels
- Special handling for `userEvents` → routes to `"userEvents"` processor
- Unknown channels logged as warnings but returned as routing key

#### 2. Payload Extraction

```python
def _extract_payload_from_envelope(self, envelope: HyperliquidWebSocketMessage) -> dict[str, Any] | list[Any]:
    """Extract payload from validated envelope."""
    return envelope.data  # Direct access (already validated)
```

#### 3. Context Extraction Methods

```python
def get_coin_from_context(self, context: dict[str, Any]) -> str | None:
    """Extract coin from processing context."""
```

**Extraction Sources:**
1. **Envelope data**: `validated_envelope.data["coin"]`
2. **Legacy fallback**: `original_message["subscription"]["coin"]` or `original_message["data"]["coin"]`

#### 4. Subscription Payload Construction

Multiple specialized methods:

```python
def construct_l2book_subscription_payload(self, coin: str) -> HyperliquidRawWsSubscribeRequest
def construct_trades_subscription_payload(self, coin: str) -> HyperliquidRawWsSubscribeRequest
def construct_user_events_subscription_payload(self, user_address: str) -> HyperliquidRawWsSubscribeRequest
def construct_candle_subscription_payload(self, coin: str, interval: str) -> HyperliquidRawWsSubscribeRequest
def construct_all_mids_subscription_payload(self) -> HyperliquidRawWsSubscribeRequest
```

**Generic Method:**
```python
def construct_subscription_payload(self, topic: str, wallet_address: str | None = None) -> HyperliquidRawWsSubscribeRequest:
    """Generic subscription method with topic parsing."""
```

**Supported Topic Formats:**
- `"allMids"` → All mids subscription
- `"l2Book:BTC"` → L2 book for BTC
- `"trades:ETH"` → Trades for ETH
- `"userEvents:0x..."` → User events for address
- `"candle:BTC:1h"` → Candles for BTC 1h interval

#### 5. Processor Setup

```python
def _setup_processors(self) -> None:
    """Setup Hyperliquid-specific message processors."""
```

**Processors:**
- `"l2Book"` → `HyperliquidRawWsBookUpdate`
- `"trades"` → `HyperliquidRawWsTradeEvent`
- `"userEvents"` → `HyperliquidRawWsPositionUpdateEvent`
- `"orders"` → `HyperliquidRawWsOrderUpdate` (with special transformation)
- `"fills"` → `HyperliquidRawWsFillEvent`

## Base WebSocket Router

### File: `cyberdelta/apis/base/ws_router.py`

#### 1. Enhanced Router Architecture

```python
class BaseWebSocketRouter[EnvelopeType](ABC):
    """Abstract base class for WebSocket message routing with type safety."""
```

**Key Features:**
- Generic type parameter `EnvelopeType` for type safety
- Envelope-based routing (new) vs legacy routing (deprecated)
- Centralized error handling
- Exchange-agnostic abstractions

#### 2. Routing Methods

##### New Envelope-Based Routing
```python
async def _route_with_envelope_validation(self, message: dict[str, Any], handlers: dict[str, MessageHandler]) -> None:
    """Enhanced routing with built-in envelope validation."""
```

**Flow:**
1. Validate envelope structure (`envelope_validator(message)`)
2. Extract routing key from validated envelope
3. Get appropriate handler
4. Extract payload from validated envelope
5. Create enhanced context with validated envelope
6. Allow exchanges to enhance context
7. Process with appropriate processor

##### Legacy Routing (Deprecated)
```python
async def _route_legacy(self, message: dict[str, Any], handlers: dict[str, MessageHandler]) -> None:
    """Legacy routing method for backward compatibility."""
```

**Flow:**
1. Validate message structure
2. Extract routing key using legacy method
3. Find processor and handler
4. Extract payload using legacy method
5. Process with basic context

#### 3. Context Enhancement

```python
def _create_enhanced_context(
    self,
    message: dict[str, Any],
    envelope: EnvelopeType,
    routing_key: str,
) -> dict[str, Any]:
    """Create standardized processing context with envelope."""
    return {
        "original_message": message,
        "validated_envelope": envelope,
        "envelope_type": type(envelope).__name__,
        "routing_key": routing_key,
        "exchange": self.exchange_name,
    }
```

```python
async def _enhance_context(
    self,
    context: dict[str, Any],
    routing_key: str,
) -> dict[str, Any]:
    """Allow exchanges to enhance context with exchange-specific data."""
    # Override in exchange implementations
    return context
```

#### 4. Abstract Methods

**Required Implementations:**
```python
@abstractmethod
def _setup_processors(self) -> None:
    """Setup exchange-specific message processors."""

@abstractmethod
def _extract_routing_key_from_envelope(self, envelope: EnvelopeType) -> str | None:
    """Extract routing key from validated envelope."""

@abstractmethod
def _extract_payload_from_envelope(self, envelope: EnvelopeType) -> dict[str, Any] | list[Any]:
    """Extract payload data from validated envelope."""
```

**Deprecated Methods:**
```python
def _extract_routing_key(self, message: dict[str, Any]) -> str | None:
    """DEPRECATED: Use _extract_routing_key_from_envelope instead."""

def _extract_payload(self, message: dict[str, Any]) -> dict[str, Any] | list[Any]:
    """DEPRECATED: Use _extract_payload_from_envelope instead."""
```

## Format Detection Functions

### Backpack Format Detection
```python
def detect_envelope_format(message: dict[str, Any]) -> str:
    """Detect Backpack envelope format."""
    if "stream" in message and "data" in message:
        return "stream"    # Current format
    if "topic" in message and "data" in message:
        return "topic"     # Legacy format
    if "type" in message:
        return "type"      # Very old format
    return "unknown"       # Malformed
```

### Hyperliquid Format Detection
```python
def detect_hyperliquid_envelope_type(message: dict[str, Any]) -> str:
    """Detect Hyperliquid envelope type."""
    channel = message.get("channel")
    if not channel:
        return "unknown"
    if channel == "userEvents":
        return "userEvents"  # Specialized envelope
    return "standard"        # Standard envelope
```

### Exchange-Specific Validators
```python
class ExchangeSpecificValidators:
    @staticmethod
    def validate_backpack_topic(topic: str) -> tuple[str, str]:
        """Parse 'type.symbol' format."""
        # Returns (topic_type, symbol)

    @staticmethod
    def validate_hyperliquid_channel(channel: str) -> str:
        """Validate Hyperliquid channel format."""
        # Logs warnings for unknown channels but doesn't fail
```

## Context Extraction Methods

### Backpack Context Extraction

#### Symbol Extraction
```python
def get_symbol_from_context(self, context: dict[str, Any]) -> str | None:
    """Extract symbol from context (envelope-based + legacy fallback)."""

def _extract_symbol_from_envelope(self, context: dict[str, Any]) -> str | None:
    """Extract symbol from validated envelope."""

def _extract_symbol_from_legacy_message(self, context: dict[str, Any]) -> str | None:
    """Extract symbol from legacy message format."""
```

#### Context Enhancement
```python
async def _enhance_context(self, context: dict[str, Any], routing_key: str) -> dict[str, Any]:
    """Add Backpack-specific context enhancements."""
    if routing_key == "depth":
        symbol = self.get_symbol_from_context(context)
        if symbol:
            context["symbol"] = symbol
    return context
```

### Hyperliquid Context Extraction

#### Coin Extraction
```python
def get_coin_from_context(self, context: dict[str, Any]) -> str | None:
    """Extract coin from context (envelope-based + legacy fallback)."""
```

**Sources (in order):**
1. `validated_envelope.data["coin"]`
2. `original_message["subscription"]["coin"]`
3. `original_message["data"]["coin"]`

#### Context Enhancement
```python
async def _enhance_context(self, context: dict[str, Any], routing_key: str) -> dict[str, Any]:
    """Add Hyperliquid-specific context enhancements."""
    # Add channel information
    if validated_envelope and hasattr(validated_envelope, "channel"):
        context["channel"] = validated_envelope.channel

    # For market data, extract coin
    if routing_key in {"l2Book", "trades"}:
        coin = self.get_coin_from_context(context)
        if coin:
            context["coin"] = coin
    return context
```

## Union Types

### Backpack Union Types
```python
# Primary union for all Backpack envelope formats
BackpackWebSocketMessage = (
    BackpackRawWebSocketEnvelope |      # Current format
    BackpackLegacyTopicEnvelope |       # Legacy topic format
    BackpackLegacyTypeEnvelope          # Legacy flat format
)
```

### Hyperliquid Union Types
```python
# Primary union for all Hyperliquid envelope formats
HyperliquidWebSocketMessage = (
    HyperliquidRawWebSocketEnvelope |   # Standard format
    HyperliquidUserEventEnvelope        # Specialized user events
)
```

### Base Router Generic Types
```python
# Generic envelope type parameter
BaseWebSocketRouter[EnvelopeType]

# Message handler type
MessageHandler = Callable[[dict[str, Any], dict[str, Any]], Awaitable[None]]
```

## Test Patterns

### File: `tests/unit/apis/backpack/test_bp_ws_router.py`

#### 1. Legacy Format Testing

**Topic-Based Messages:**
```python
def test_extract_routing_key_topic_based(self, router: BackpackWebSocketRouter) -> None:
    envelope = BackpackLegacyTopicEnvelope(topic="depth.BTC_USDC", data={})
    routing_key = router._extract_routing_key_from_envelope(envelope)
    assert routing_key == "depth"
```

**Type-Based Messages:**
```python
def test_extract_routing_key_type_based(self, router: BackpackWebSocketRouter) -> None:
    envelope = BackpackLegacyTypeEnvelope.model_validate({"type": "fills", "orderId": "123"})
    routing_key = router._extract_routing_key_from_envelope(envelope)
    assert routing_key == "fills"
```

**Stream-Based Messages:**
```python
def test_extract_routing_key_stream_based(self, router: BackpackWebSocketRouter) -> None:
    envelope = BackpackRawWebSocketEnvelope(stream="depth.BTC_USDC", data={})
    routing_key = router._extract_routing_key_from_envelope(envelope)
    assert routing_key == "depth"
```

#### 2. Payload Extraction Testing

**Legacy Type Payload:**
```python
def test_extract_payload_type_based(self, router: BackpackWebSocketRouter) -> None:
    envelope = BackpackLegacyTypeEnvelope.model_validate({
        "type": "fills",
        "orderId": "123",
        "quantity": "10.0",
    })
    payload = router._extract_payload_from_envelope(envelope)
    expected = {"orderId": "123", "quantity": "10.0"}  # Type field removed
    assert payload == expected
```

#### 3. Context Extraction Testing

**Symbol from Legacy:**
```python
def test_get_symbol_from_context(self, router: BackpackWebSocketRouter) -> None:
    context = {
        "original_message": {"topic": "depth.BTC_USDC", "data": {}},
        "routing_key": "depth",
    }
    symbol = router.get_symbol_from_context(context)
    assert symbol == "BTC_USDC"
```

#### 4. Integration Testing

**Full Message Routing:**
```python
@pytest.mark.asyncio
async def test_route_message_success(self, router: BackpackWebSocketRouter, error_handler: AsyncMock) -> None:
    handler = AsyncMock()
    ws_handlers = {"depth": handler}
    message = {"topic": "depth.BTC_USDC", "data": {"bids": [], "asks": []}}

    mock_processor = AsyncMock()
    router.processors["depth"] = mock_processor

    await router.route_message(message, ws_handlers)

    mock_processor.process.assert_called_once()
    call_args = mock_processor.process.call_args
    payload, handler_arg, context = call_args[0]

    assert payload == {"bids": [], "asks": []}
    assert handler_arg == handler
    assert context["routing_key"] == "depth"
    assert context["symbol"] == "BTC_USDC"
```

### File: `tests/unit/apis/hyperliquid/test_hl_ws_router.py`

#### 1. Envelope Testing

**Standard Envelope:**
```python
def test_extract_routing_key_valid_channel(self, router: HyperliquidWebSocketRouter) -> None:
    envelope = HyperliquidRawWebSocketEnvelope(channel="l2Book", data={"coin": "BTC"})
    routing_key = router._extract_routing_key_from_envelope(envelope)
    assert routing_key == "l2Book"
```

**User Events Envelope:**
```python
def test_extract_routing_key_valid_channel(self, router: HyperliquidWebSocketRouter) -> None:
    envelope = HyperliquidUserEventEnvelope(channel="userEvents", data={"user": "0x123"})
    routing_key = router._extract_routing_key_from_envelope(envelope)
    assert routing_key == "userEvents"
```

#### 2. Subscription Testing

**Topic-Based Subscriptions:**
```python
def test_construct_subscription_payload(self, router: HyperliquidWebSocketRouter) -> None:
    # Test various topic formats
    # "l2Book:BTC", "trades:ETH", "userEvents:0x...", "candle:BTC:1h"
```

#### 3. Context Extraction Testing

**Coin from Multiple Sources:**
```python
def test_get_coin_from_context(self, router: HyperliquidWebSocketRouter) -> None:
    # From subscription data
    context = {"original_message": {"subscription": {"coin": "BTC"}, "data": {}}}
    coin = router.get_coin_from_context(context)
    assert coin == "BTC"

    # From data payload
    context = {"original_message": {"data": {"coin": "ETH"}}}
    coin = router.get_coin_from_context(context)
    assert coin == "ETH"
```

### File: `tests/unit/apis/base/test_ws_validators.py`

#### 1. Backpack Topic Validation Testing

```python
@pytest.mark.parametrize(
    ("topic", "expected_type", "expected_symbol"),
    [
        ("depth.BTC_USDC", "depth", "BTC_USDC"),
        ("ticker.SOL-PERP", "ticker", "SOL-PERP"),
        ("trades.ETH_USD", "trades", "ETH_USD"),
    ],
)
def test_validate_backpack_topic_success(self, topic: str, expected_type: str, expected_symbol: str) -> None:
    topic_type, symbol = ExchangeSpecificValidators.validate_backpack_topic(topic)
    assert topic_type == expected_type
    assert symbol == expected_symbol
```

#### 2. Hyperliquid Channel Validation Testing

```python
def test_validate_hyperliquid_channel_unknown(self) -> None:
    # Should not raise exception, just log warning
    result = ExchangeSpecificValidators.validate_hyperliquid_channel("unknown_channel")
    assert result == "unknown_channel"
```

## Migration Notes

### Current Backwards Compatibility Support

1. **Backpack Exchange:**
   - Supports 3 envelope formats simultaneously
   - Format detection handles graceful fallbacks
   - Legacy payload extraction maintains flat structure for type-based messages
   - Context extraction has envelope-based + legacy fallback paths

2. **Hyperliquid Exchange:**
   - Supports 2 envelope formats (standard + userEvents)
   - Less complexity due to more recent API design
   - Context extraction has envelope-based + legacy fallback paths

3. **Base Router:**
   - Dual routing paths (envelope-based vs legacy)
   - Enhanced context creation with full type safety
   - Graceful fallbacks for missing components

### Areas for Cleanup

When removing backwards compatibility:

1. **Remove Legacy Envelope Models:**
   - `BackpackLegacyTopicEnvelope`
   - `BackpackLegacyTypeEnvelope`

2. **Simplify Union Types:**
   - `BackpackWebSocketMessage` → only `BackpackRawWebSocketEnvelope`
   - Keep `HyperliquidWebSocketMessage` as-is (minimal legacy)

3. **Remove Format Detection:**
   - `detect_envelope_format()` function
   - Fallback validation logic in `validate_backpack_envelope()`

4. **Simplify Context Extraction:**
   - Remove legacy fallback methods
   - Remove `_extract_symbol_from_legacy_message()`
   - Simplify `get_symbol_from_context()` and `get_coin_from_context()`

5. **Remove Legacy Router Methods:**
   - `_route_legacy()` method
   - `_extract_routing_key()` (deprecated)
   - `_extract_payload()` (deprecated)

6. **Update Tests:**
   - Remove legacy format test cases
   - Remove fallback testing
   - Simplify routing tests to only use current formats

7. **Simplify Payload Extraction:**
   - No more type field removal for flat formats
   - No more list wrapping for envelope compatibility

This documentation serves as the complete reference for understanding the current state before migration and identifies exactly what needs to be removed to eliminate backwards compatibility.
