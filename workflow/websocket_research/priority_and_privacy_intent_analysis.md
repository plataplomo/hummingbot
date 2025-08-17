# Intent Analysis: Processing Priority and Private Message Detection

## 1. Processing Priority Intent

### Original Intent
The `processing_priority` method was designed to prioritize WebSocket messages for processing:
- **Priority 1 (Highest)**: Trades and user events - time-critical for trading decisions
- **Priority 2**: Order book depth updates - important for market state
- **Priority 3**: Tickers and statistics - less time-sensitive
- **Priority 4 (Lowest)**: Everything else

### Why It's Not Used
The WebSocket module processes messages **synchronously** in the order they arrive. There's no priority queue or async task scheduler that would use these priorities.

### Where It SHOULD Live

**Option A: Message Queue Handler** (If async processing is implemented)
```python
# cyberdelta/apis/websocket/processing/message_queue_handler.py
class WebSocketMessageQueueHandler:
    """Handles prioritized async message processing."""

    def __init__(self):
        self.priority_queue = asyncio.PriorityQueue()

    def get_message_priority(self, routing_key: str) -> int:
        """Determine processing priority for a message."""
        # High priority for trades and user events
        if "trades" in routing_key or "userEvents" in routing_key:
            return 1
        # Medium priority for order book updates
        if "depth" in routing_key or "l2Book" in routing_key:
            return 2
        # Lower priority for tickers and statistics
        if "ticker" in routing_key or "stats" in routing_key:
            return 3
        # Lowest priority for everything else
        return 4

    async def enqueue_message(self, message: dict, context: WebSocketContextProtocol):
        """Add message to priority queue."""
        priority = self.get_message_priority(context.routing_key)
        await self.priority_queue.put((priority, message, context))
```

**Option B: Router Configuration** (For immediate use)
```python
# cyberdelta/apis/websocket/ws_message_router.py
class WebSocketMessageRouter:
    # Add as class constant
    MESSAGE_PRIORITIES = {
        "trades": 1,
        "userEvents": 1,
        "depth": 2,
        "l2Book": 2,
        "ticker": 3,
        "stats": 3,
    }

    def get_handler_priority(self, routing_key: str) -> int:
        """Get priority for handler execution order."""
        for pattern, priority in self.MESSAGE_PRIORITIES.items():
            if pattern in routing_key:
                return priority
        return 4  # Default lowest priority
```

## 2. Private Message Detection Intent

### Original Intent
The `is_private_message` method identifies messages containing sensitive account data:
- Private: account, user, balance, orders, fills
- Public: market data, tickers, order books

### Why It's Not Used
The module doesn't currently implement different security/permission checks for private vs public messages.

### Where It SHOULD Live

**Option A: Security Validator** (Most appropriate)
```python
# cyberdelta/apis/websocket/security/channel_classifier.py
class ChannelClassifier:
    """Classifies WebSocket channels for security purposes."""

    PRIVATE_PATTERNS = {"account", "user", "balance", "orders", "fills", "positions"}

    @classmethod
    def is_private_channel(cls, routing_key: str) -> bool:
        """Determine if channel contains private/sensitive data."""
        routing_key_lower = routing_key.lower()
        return any(pattern in routing_key_lower for pattern in cls.PRIVATE_PATTERNS)

    @classmethod
    def requires_authentication(cls, topic: str) -> bool:
        """Check if topic requires authentication."""
        return cls.is_private_channel(topic)

    @classmethod
    def get_channel_security_level(cls, routing_key: str) -> str:
        """Get security classification for monitoring/logging."""
        if cls.is_private_channel(routing_key):
            return "PRIVATE"
        return "PUBLIC"
```

**Option B: Subscription Manager** (For subscription validation)
```python
# cyberdelta/apis/websocket/subscription/subscription_validator.py
class SubscriptionValidator:
    """Validates subscription requests."""

    def validate_subscription(
        self,
        topic: str,
        signature_components: Any | None
    ) -> None:
        """Validate subscription has proper auth for private channels."""
        if self._is_private_topic(topic) and not signature_components:
            raise WebSocketSubscriptionError(
                f"Private topic '{topic}' requires authentication"
            )

    def _is_private_topic(self, topic: str) -> bool:
        """Check if topic requires authentication."""
        private_patterns = {"account", "user", "balance", "orders", "fills"}
        return any(pattern in topic.lower() for pattern in private_patterns)
```

## 3. Implementation Plan

### Step 1: Delete from Context Model
```python
# DELETE these from ws_context.py:
- @computed_field def processing_priority(self) -> int
- @computed_field def is_private_message(self) -> bool
- def get_transformer_params(self) -> dict[str, str]
- def get_symbol_param(self) -> dict[str, str] | None
- def get_coin_param(self) -> dict[str, str] | None
```

### Step 2: Create Channel Classifier
```python
# NEW FILE: cyberdelta/apis/websocket/security/channel_classifier.py
from typing import Final

class ChannelClassifier:
    """Security classification for WebSocket channels."""

    PRIVATE_PATTERNS: Final[set[str]] = {
        "account", "user", "balance", "orders",
        "fills", "positions", "wallet", "portfolio"
    }

    PUBLIC_PATTERNS: Final[set[str]] = {
        "ticker", "depth", "trades", "stats",
        "kline", "orderbook", "market"
    }

    @classmethod
    def classify(cls, routing_key: str) -> str:
        """Classify channel as PUBLIC or PRIVATE."""
        routing_key_lower = routing_key.lower()

        # Check for private patterns
        if any(p in routing_key_lower for p in cls.PRIVATE_PATTERNS):
            return "PRIVATE"

        # Default to public for market data
        return "PUBLIC"

    @classmethod
    def requires_auth(cls, routing_key: str) -> bool:
        """Check if channel requires authentication."""
        return cls.classify(routing_key) == "PRIVATE"
```

### Step 3: Add Priority Constants (for future use)
```python
# In cyberdelta/apis/websocket/constants.py (or create if doesn't exist)
from enum import IntEnum

class MessagePriority(IntEnum):
    """WebSocket message processing priorities."""
    CRITICAL = 1  # Trades, fills, user events
    HIGH = 2      # Order book updates
    NORMAL = 3    # Tickers, statistics
    LOW = 4       # Everything else

MESSAGE_ROUTING_PRIORITIES = {
    "trades": MessagePriority.CRITICAL,
    "fills": MessagePriority.CRITICAL,
    "userEvents": MessagePriority.CRITICAL,
    "depth": MessagePriority.HIGH,
    "l2Book": MessagePriority.HIGH,
    "orderbook": MessagePriority.HIGH,
    "ticker": MessagePriority.NORMAL,
    "stats": MessagePriority.NORMAL,
    # Default is LOW for unspecified
}
```

### Step 4: Use in Router (Example)
```python
# In bp_ws_router.py construct_subscription_payload
from cyberdelta.apis.websocket.security.channel_classifier import ChannelClassifier

def construct_subscription_payload(self, topic: str, signature_components: ...):
    # Existing validation...

    # NEW: Validate auth for private channels
    if ChannelClassifier.requires_auth(topic) and not signature_components:
        raise WebSocketSubscriptionError(
            f"Private channel '{topic}' requires authentication",
            code=WebSocketErrorCode.AUTHENTICATION_REQUIRED
        )
```

## Benefits of This Refactoring

1. **Proper Separation of Concerns**
   - Context: Pure data
   - Security: Channel classification
   - Processing: Priority handling

2. **Reusability**
   - Channel classifier can be used by multiple components
   - Priority constants available for future async processing

3. **Clear Intent**
   - Channel classification is explicitly about security
   - Priority is explicitly about processing order

4. **Extensibility**
   - Easy to add new private/public patterns
   - Easy to adjust priorities
   - Ready for async priority queue if needed

## Conclusion

The original methods had good intent but were placed in the wrong location and never connected to actual functionality. By moving them to dedicated security and processing modules, we:
- Make the intent explicit
- Enable actual usage
- Maintain clean architecture
- Prepare for future enhancements (async processing, enhanced security)
