# WebSocket API Contract Documentation - Before Backwards Compatibility Removal

## Executive Summary

This document captures the complete WebSocket API contract before removing backwards compatibility. It serves as a reference for understanding all legacy formats, detection logic, routing methods, and context extraction patterns that will be removed or simplified during the backwards compatibility removal process.

**Document Status**: Baseline documentation created before backwards compatibility removal
**Date**: 2025-07-04
**Purpose**: Reference for safe removal of legacy support

## Table of Contents

1. [Envelope Models](#envelope-models)
2. [Format Detection Functions](#format-detection-functions)
3. [Routing Methods](#routing-methods)
4. [Context Extraction](#context-extraction)
5. [Union Types](#union-types)
6. [Test Patterns](#test-patterns)
7. [Migration Impact](#migration-impact)

## Envelope Models

### Backpack Exchange Models

#### 1. Current Format - BackpackRawWebSocketEnvelope

**File**: `cyberdelta/apis/backpack/models/bp_ws_envelope.py` (lines 20-85)

```python
class BackpackRawWebSocketEnvelope(BaseModel):
    """Current Backpack WebSocket envelope format (post-2024-01-16)."""

    stream: RawBpNonEmptyStringMax128 = Field(
        ...,
        description="Stream identifier (e.g., 'depth.SOL_USDC', 'ticker.BTC_USDC')"
    )
    data: dict[str, Any] | list[Any] = Field(
        ...,
        description="Message payload - structure varies by stream type"
    )

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        validate_assignment=True,
    )

    @field_validator("stream", mode="before")
    @classmethod
    def normalize_and_validate_stream(cls, v: str | dict[str, Any]) -> str:
        """Handle legacy topic format conversion."""
        if isinstance(v, dict) and "topic" in v:
            v = str(v["topic"])
        if isinstance(v, str):
            v = v.strip().lower()
            return v
        raise ValueError(f"Invalid stream format: {v}")
```

**Message Format**:
```json
{
    "stream": "depth.BTC_USDC",
    "data": {
        "bids": [[50000, 1.5]],
        "asks": [[51000, 2.0]]
    }
}
```

#### 2. Legacy Topic Format - BackpackLegacyTopicEnvelope

**File**: `cyberdelta/apis/backpack/models/bp_ws_envelope.py` (lines 378-443)

```python
class BackpackLegacyTopicEnvelope(BaseModel):
    """Legacy envelope model for topic-based messages (pre-2024-01-16)."""

    topic: RawBpNonEmptyStringMax128 = Field(
        ...,
        description="Legacy topic field (replaced by 'stream' in current format)"
    )
    data: dict[str, Any] = Field(
        ...,
        description="Message payload"
    )

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        validate_assignment=True,
    )

    @field_validator("topic")
    @classmethod
    def validate_topic_format(cls, v: str) -> str:
        """Validate legacy topic format."""
        if not v or not isinstance(v, str):
            raise ValueError("Topic must be a non-empty string")

        # Validate topic patterns
        valid_patterns = [
            r"^depth\.[A-Z_]+$",
            r"^ticker\.[A-Z_]+$",
            r"^trades\.[A-Z_]+$",
            r"^kline\.\d+[mhd]\.[A-Z_]+$"
        ]

        if not any(re.match(pattern, v) for pattern in valid_patterns):
            raise ValueError(f"Invalid topic format: {v}")

        return v
```

**Message Format**:
```json
{
    "topic": "depth.BTC_USDC",
    "data": {
        "bids": [[50000, 1.5]],
        "asks": [[51000, 2.0]]
    }
}
```

#### 3. Legacy Flat Format - BackpackLegacyTypeEnvelope

**File**: `cyberdelta/apis/backpack/models/bp_ws_envelope.py` (lines 446-525)

```python
class BackpackLegacyTypeEnvelope(BaseModel):
    """Legacy envelope model for type-based account messages (very old format)."""

    type: RawBpNonEmptyStringMax128 = Field(
        ...,
        description="Message type (e.g., 'fills', 'orders', 'balances')"
    )

    # Dynamic fields based on type
    model_config = ConfigDict(
        extra="allow",  # Allow additional fields for flat format
        frozen=True,
        validate_assignment=True,
    )

    @field_validator("type")
    @classmethod
    def validate_type_field(cls, v: str) -> str:
        """Validate message type field."""
        valid_types = {
            "fills", "orders", "balances", "positions",
            "orderUpdate", "positionUpdate", "balanceUpdate"
        }

        if v not in valid_types:
            raise ValueError(f"Invalid message type: {v}")

        return v
```

**Message Format**:
```json
{
    "type": "fills",
    "orderId": "123456",
    "symbol": "BTC_USDC",
    "side": "buy",
    "quantity": "1.5",
    "price": "50000.0",
    "timestamp": 1704067200000
}
```

### Hyperliquid Exchange Models

#### 1. Standard Format - HyperliquidRawWebSocketEnvelope

**File**: `cyberdelta/apis/hyperliquid/models/hl_ws_envelope.py` (lines 15-75)

```python
class HyperliquidRawWebSocketEnvelope(BaseModel):
    """Standard Hyperliquid WebSocket envelope format."""

    channel: str = Field(
        ...,
        description="Channel name (e.g., 'l2Book', 'trades', 'allMids')"
    )
    data: dict[str, Any] | list[Any] = Field(
        ...,
        description="Channel-specific data payload"
    )

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        validate_assignment=True,
    )

    @computed_field
    @property
    def validated_coin(self) -> str | None:
        """Extract coin from data with type safety."""
        if isinstance(self.data, dict) and 'coin' in self.data:
            coin = self.data['coin']
            return coin if isinstance(coin, str) else None
        return None
```

**Message Format**:
```json
{
    "channel": "l2Book",
    "data": {
        "coin": "BTC",
        "levels": [
            [{"px": "50000", "sz": "1.5", "n": 1}],
            [{"px": "51000", "sz": "2.0", "n": 1}]
        ],
        "time": 1704067200000
    }
}
```

#### 2. User Events Format - HyperliquidUserEventEnvelope

**File**: `cyberdelta/apis/hyperliquid/models/hl_ws_envelope.py` (lines 78-125)

```python
class HyperliquidUserEventEnvelope(BaseModel):
    """Specialized envelope for Hyperliquid user events."""

    channel: Literal["userEvents"] = Field(
        ...,
        description="Fixed channel name for user events"
    )
    data: dict[str, Any] = Field(
        ...,
        description="User event data with user ID"
    )

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        validate_assignment=True,
    )

    @field_validator("data")
    @classmethod
    def validate_user_data(cls, v: dict[str, Any]) -> dict[str, Any]:
        """Validate user event data structure."""
        if "user" not in v:
            raise ValueError("User events must contain 'user' field")

        return v
```

## Format Detection Functions

### Backpack Format Detection

**File**: `cyberdelta/apis/backpack/models/bp_ws_envelope.py` (lines 528-549)

```python
def detect_envelope_format(message: dict[str, Any]) -> str:
    """Detect which envelope format a message uses.

    Returns:
        - "stream": Current format (post 2024-01-16)
        - "topic": Legacy topic format (pre 2024-01-16)
        - "type": Legacy flat format (very old)
        - "unknown": Unrecognized format
    """
    # Priority order: check for most specific fields first
    if "stream" in message and "data" in message:
        return "stream"  # Current API format
    if "topic" in message and "data" in message:
        return "topic"  # Legacy format
    if "type" in message:
        return "type"  # Very old flat format
    return "unknown"

def validate_backpack_envelope(message: dict[str, Any]) -> BackpackWebSocketMessage:
    """Unified validation function for all Backpack envelope formats.

    Args:
        message: Raw message dictionary

    Returns:
        Validated envelope of appropriate type

    Raises:
        ValueError: If message format is unrecognized or invalid
    """
    format_type = detect_envelope_format(message)

    if format_type == "stream":
        return BackpackRawWebSocketEnvelope.model_validate(message)
    elif format_type == "topic":
        return BackpackLegacyTopicEnvelope.model_validate(message)
    elif format_type == "type":
        return BackpackLegacyTypeEnvelope.model_validate(message)
    else:
        raise ValueError(f"Unknown Backpack message format: {message}")
```

### Hyperliquid Format Detection

**File**: `cyberdelta/apis/hyperliquid/models/hl_ws_envelope.py` (lines 128-149)

```python
def detect_hyperliquid_envelope_type(message: dict[str, Any]) -> str:
    """Detect Hyperliquid envelope type.

    Returns:
        - "userEvents": User-specific events
        - "standard": Standard market data
        - "unknown": Unrecognized format
    """
    if not isinstance(message, dict):
        return "unknown"

    channel = message.get("channel")
    if channel == "userEvents":
        return "userEvents"
    elif isinstance(channel, str):
        return "standard"
    else:
        return "unknown"

def validate_hyperliquid_envelope(message: dict[str, Any]) -> HyperliquidWebSocketMessage:
    """Unified validation for Hyperliquid envelopes."""
    envelope_type = detect_hyperliquid_envelope_type(message)

    if envelope_type == "userEvents":
        return HyperliquidUserEventEnvelope.model_validate(message)
    elif envelope_type == "standard":
        return HyperliquidRawWebSocketEnvelope.model_validate(message)
    else:
        raise ValueError(f"Unknown Hyperliquid message format: {message}")
```

## Routing Methods

### Base Router - Dual Routing Logic

**File**: `cyberdelta/apis/base/ws_router.py` (lines 293-320)

```python
async def route_message(
    self,
    message: dict[str, Any],
    handlers: dict[str, MessageHandler],
) -> None:
    """Route WebSocket message to appropriate processor and handler.

    Supports both envelope-based routing (recommended) and legacy routing.
    """
    try:
        # Check if envelope validator is available
        if self.envelope_validator is not None:
            # Enhanced envelope-based routing (preferred)
            await self._route_with_envelope_validation(message, handlers)
        else:
            # Legacy routing for backward compatibility
            await self._route_legacy(message, handlers)

    except Exception as e:
        await self._handle_routing_error(message, e)

async def _route_with_envelope_validation(
    self,
    message: dict[str, Any],
    handlers: dict[str, MessageHandler],
) -> None:
    """Enhanced routing with envelope validation."""
    # Validate envelope first
    envelope = self.envelope_validator(message)

    # Extract routing key from validated envelope
    routing_key = self.extract_routing_key_from_envelope(envelope)

    # Create enhanced context
    context = self._create_enhanced_context(message, envelope, routing_key)

    # Route to appropriate handler
    await self._route_to_handler(routing_key, context, handlers)

async def _route_legacy(
    self,
    message: dict[str, Any],
    handlers: dict[str, MessageHandler],
) -> None:
    """Legacy routing without envelope validation."""
    # Extract routing key directly from message
    routing_key = self.extract_routing_key(message)

    # Create basic context
    context = self._create_basic_context(message, routing_key)

    # Route to appropriate handler
    await self._route_to_handler(routing_key, context, handlers)
```

### Context Creation - Enhanced vs Basic

**File**: `cyberdelta/apis/base/ws_router.py` (lines 189-230)

```python
def _create_enhanced_context(
    self,
    message: dict[str, Any],
    envelope: EnvelopeType,
    routing_key: str,
) -> dict[str, Any]:
    """Create standardized processing context with envelope."""
    return {
        "original_message": message,           # Preserved for backwards compatibility
        "validated_envelope": envelope,        # Validated envelope object
        "envelope_type": type(envelope).__name__,
        "routing_key": routing_key,
        "exchange": self.exchange_name,
        "timestamp": datetime.now(timezone.utc),
        "message_id": str(uuid.uuid4()),
        "connection_id": getattr(self, 'connection_id', 'unknown'),
    }

def _create_basic_context(
    self,
    message: dict[str, Any],
    routing_key: str,
) -> dict[str, Any]:
    """Create basic processing context without envelope."""
    return {
        "original_message": message,           # Primary data source for legacy
        "routing_key": routing_key,
        "exchange": self.exchange_name,
        "timestamp": datetime.now(timezone.utc),
        "message_id": str(uuid.uuid4()),
        "connection_id": getattr(self, 'connection_id', 'unknown'),
    }
```

## Context Extraction

### Backpack Symbol Extraction

**File**: `cyberdelta/apis/backpack/bp_ws_router.py` (lines 360-415)

```python
def get_symbol_from_context(self, context: dict[str, Any]) -> str | None:
    """Extract symbol from processing context with fallback logic."""
    # Try new envelope-based approach first
    symbol = self._extract_symbol_from_envelope(context)
    if symbol is not None:
        return symbol

    # Fallback to legacy approach for backward compatibility
    return self._extract_symbol_from_legacy_message(context)

def _extract_symbol_from_envelope(self, context: dict[str, Any]) -> str | None:
    """Extract symbol from validated envelope."""
    envelope = context.get("validated_envelope")
    if envelope is None:
        return None

    # Handle different envelope types
    if hasattr(envelope, "stream"):
        # Current format
        try:
            _, symbol = ExchangeSpecificValidators.validate_backpack_topic(envelope.stream)
            return symbol
        except ValueError:
            return None
    elif hasattr(envelope, "topic"):
        # Legacy topic format
        try:
            _, symbol = ExchangeSpecificValidators.validate_backpack_topic(envelope.topic)
            return symbol
        except ValueError:
            return None

    return None

def _extract_symbol_from_legacy_message(self, context: dict[str, Any]) -> str | None:
    """Extract symbol from legacy message format."""
    original_message = context.get("original_message")
    if not isinstance(original_message, dict):
        return None

    # Check for topic field (legacy format)
    topic = original_message.get("topic")
    if topic is not None and isinstance(topic, str):
        try:
            _, symbol = ExchangeSpecificValidators.validate_backpack_topic(topic)
            return symbol
        except ValueError:
            pass

    # Check for stream field (current format)
    stream = original_message.get("stream")
    if stream is not None and isinstance(stream, str):
        try:
            _, symbol = ExchangeSpecificValidators.validate_backpack_topic(stream)
            return symbol
        except ValueError:
            pass

    # Check for symbol field (flat format)
    symbol = original_message.get("symbol")
    if symbol is not None and isinstance(symbol, str):
        return symbol

    return None
```

### Hyperliquid Coin Extraction

**File**: `cyberdelta/apis/hyperliquid/hl_ws_router.py` (lines 491-527)

```python
def get_coin_from_context(self, context: dict[str, Any]) -> str | None:
    """Extract coin from processing context with envelope fallback."""
    # Try to extract from validated envelope data (preferred)
    envelope = context.get("validated_envelope")
    if envelope is not None and hasattr(envelope, "data"):
        envelope_data = getattr(envelope, "data", None)
        if isinstance(envelope_data, dict) and "coin" in envelope_data:
            coin_value: Any = envelope_data["coin"]
            if isinstance(coin_value, str):
                return coin_value

    # Try computed field if available
    if envelope is not None and hasattr(envelope, "validated_coin"):
        return envelope.validated_coin

    # Fallback to original message for backward compatibility
    original_message = context.get("original_message")
    if isinstance(original_message, dict):
        # Try direct coin field
        coin = original_message.get("coin")
        if isinstance(coin, str):
            return coin

        # Try subscription data
        subscription_data: Any = original_message.get("subscription")
        if isinstance(subscription_data, dict) and "coin" in subscription_data:
            sub_coin: Any = subscription_data["coin"]
            if isinstance(sub_coin, str):
                return sub_coin

        # Try data.coin field
        data = original_message.get("data")
        if isinstance(data, dict) and "coin" in data:
            data_coin: Any = data["coin"]
            if isinstance(data_coin, str):
                return data_coin

    return None
```

## Union Types

### Backpack Union Type

**File**: `cyberdelta/apis/backpack/models/bp_ws_envelope.py` (lines 576-581)

```python
BackpackWebSocketMessage = Union[
    BackpackRawWebSocketEnvelope,     # Current format
    BackpackLegacyTopicEnvelope,      # Legacy topic format
    BackpackLegacyTypeEnvelope,       # Legacy flat format
]
```

### Hyperliquid Union Type

**File**: `cyberdelta/apis/hyperliquid/models/hl_ws_envelope.py` (lines 152-156)

```python
HyperliquidWebSocketMessage = Union[
    HyperliquidRawWebSocketEnvelope,  # Standard format
    HyperliquidUserEventEnvelope,     # User events format
]
```

## Test Patterns

### Legacy Format Testing

**File**: `tests/unit/apis/backpack/test_bp_ws_router.py`

```python
def test_extract_routing_key_topic_based():
    """Test routing key extraction from legacy topic format."""
    router = BackpackWebSocketRouter()

    # Legacy topic format
    message = {
        "topic": "depth.BTC_USDC",
        "data": {"bids": [], "asks": []}
    }

    routing_key = router.extract_routing_key(message)
    assert routing_key == "depth"

def test_extract_routing_key_type_based():
    """Test routing key extraction from legacy flat format."""
    router = BackpackWebSocketRouter()

    # Legacy flat format
    message = {
        "type": "fills",
        "orderId": "123456",
        "symbol": "BTC_USDC"
    }

    routing_key = router.extract_routing_key(message)
    assert routing_key == "account"

def test_symbol_extraction_fallback():
    """Test symbol extraction with fallback to legacy methods."""
    router = BackpackWebSocketRouter()

    # Context without validated_envelope (legacy scenario)
    context = {
        "original_message": {
            "topic": "depth.SOL_USDC",
            "data": {}
        }
    }

    symbol = router.get_symbol_from_context(context)
    assert symbol == "SOL_USDC"
```

### Envelope Validation Testing

```python
def test_envelope_format_detection():
    """Test format detection for all envelope types."""

    # Current format
    current_msg = {"stream": "depth.BTC_USDC", "data": {}}
    assert detect_envelope_format(current_msg) == "stream"

    # Legacy topic format
    legacy_topic = {"topic": "depth.BTC_USDC", "data": {}}
    assert detect_envelope_format(legacy_topic) == "topic"

    # Legacy flat format
    legacy_flat = {"type": "fills", "orderId": "123"}
    assert detect_envelope_format(legacy_flat) == "type"

def test_unified_envelope_validation():
    """Test unified validation function."""

    # Test all supported formats
    messages = [
        {"stream": "depth.BTC_USDC", "data": {}},
        {"topic": "depth.BTC_USDC", "data": {}},
        {"type": "fills", "orderId": "123"}
    ]

    for msg in messages:
        envelope = validate_backpack_envelope(msg)
        assert envelope is not None
        assert isinstance(envelope, BackpackWebSocketMessage)
```

## Migration Impact

### Files to be Modified

1. **Remove Legacy Models**:
   - `BackpackLegacyTopicEnvelope` (lines 378-443)
   - `BackpackLegacyTypeEnvelope` (lines 446-525)

2. **Remove Detection Functions**:
   - `detect_envelope_format()` (lines 528-549)
   - `validate_backpack_envelope()` (lines 552-573)

3. **Simplify Context Extraction**:
   - Remove `_extract_symbol_from_legacy_message()`
   - Simplify `get_symbol_from_context()`
   - Remove fallback logic in `get_coin_from_context()`

4. **Update Union Types**:
   - Replace `BackpackWebSocketMessage` with `BackpackRawWebSocketEnvelope`
   - Simplify type hints throughout codebase

5. **Remove Legacy Tests**:
   - All tests using legacy envelope formats
   - Fallback extraction tests
   - Format detection tests

### Breaking Changes

1. **Message Format Support**:
   - No longer accepts legacy topic format (`{"topic": "...", "data": {...}}`)
   - No longer accepts legacy flat format (`{"type": "...", "field": "..."}`)
   - Only accepts current stream format (`{"stream": "...", "data": {...}}`)

2. **Context Structure**:
   - `original_message` field removed from context
   - Simplified context with only `validated_envelope`

3. **Router Behavior**:
   - Only envelope-based routing supported
   - Legacy routing methods removed
   - Envelope validator required

4. **Type System**:
   - Union types simplified to single envelope type
   - All legacy model references removed

### Performance Improvements Expected

- **Validation**: 5-10% faster without format detection
- **Routing**: 2-5% faster with single code path
- **Memory**: 1-2% reduction from simplified context
- **Overall**: 8-17% performance improvement

---

*Documentation completed on 2025-07-04*
*Serves as baseline before backwards compatibility removal*
