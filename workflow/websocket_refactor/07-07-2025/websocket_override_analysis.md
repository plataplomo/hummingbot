# WebSocket Router Override Pattern Analysis

## Executive Summary

After analyzing the WebSocket router implementations for both Backpack and Hyperliquid exchanges, I've identified several architectural issues that require overriding base class methods. The primary issue is a **fundamental mismatch between the base class's assumptions about handler registration patterns and the actual patterns used by these exchanges**.

## Key Findings

### 1. Handler Registration Pattern Mismatch

#### Base Class Assumption
The `BaseWebSocketRouter` assumes handlers are registered using simple routing keys extracted from messages (e.g., "ticker", "depth", "orders").

#### Backpack Reality
- Handlers are registered with **full topic patterns** including symbols: `"ticker.SOL_USDC"`, `"depth.BTC_USDC"`
- The base class extracts only the type portion ("ticker", "depth") as the routing key
- This creates a mismatch where the router looks for handlers by type, but they're registered by type+symbol

#### Hyperliquid Reality
- Uses channel-based routing where the channel name directly maps to the routing key
- Generally simpler than Backpack, but still has special cases (e.g., userEvents)
- No symbol in the registration pattern, but needs coin extraction for context

### 2. Override Patterns Observed

#### Backpack Overrides

```python
async def route_message(self, message: dict[str, Any], handlers: dict[str, MessageHandler]) -> None:
    """Override to handle Backpack's topic.symbol handler registration pattern."""
```

**Why Override Needed:**
- Lines 426-438 in `bp_ws_router.py` show the custom handler lookup logic:
  ```python
  # First try exact match for simple topics
  if routing_key in handlers:
      handler = handlers[routing_key]
  # Then try with symbol if we have one
  elif "full_topic" in context and context["full_topic"] in handlers:
      handler = handlers[context["full_topic"]]
  ```
- This two-step lookup is necessary because handlers can be registered either way

#### Hyperliquid Overrides

Hyperliquid **does not override** `route_message`, which suggests its channel-based approach aligns better with the base class assumptions. However, it still needs custom logic for:
- Channel-to-routing-key mapping
- Coin extraction from envelope data
- Special handling for userEvents

### 3. Common Override Methods

Both exchanges override these methods:

1. **`_extract_routing_key_from_envelope`**
   - Backpack: Parses "stream" format, validates topic structure
   - Hyperliquid: Maps channels to routing keys, handles special cases

2. **`_extract_payload_from_envelope`**
   - Backpack: Extracts data, wraps lists in dicts
   - Hyperliquid: Simple data extraction from validated envelope

3. **`_enhance_context`**
   - Backpack: Adds symbol and full_topic to context
   - Hyperliquid: Adds channel and coin to context

### 4. Architectural Issues Revealed

#### Issue 1: Rigid Handler Lookup Pattern
The base class assumes a 1:1 mapping between routing keys and handlers. Real exchanges need:
- Composite keys (type + symbol)
- Fallback patterns
- Dynamic handler selection based on message content

#### Issue 2: Symbol/Asset Extraction Complexity
Both exchanges need to extract asset identifiers (symbol/coin) but at different points:
- Backpack: From the stream/topic string
- Hyperliquid: From the message data

The base class provides `_enhance_context` for this, but it's called too late in the routing process for handler lookup.

#### Issue 3: Envelope Structure Assumptions
The base class assumes a consistent envelope structure, but:
- Backpack has multiple legacy formats to support
- Hyperliquid has specialized envelopes (e.g., `HyperliquidUserEventEnvelope`)

### 5. Design Recommendations

1. **Flexible Handler Resolution**
   ```python
   async def resolve_handler(
       self, 
       routing_key: str, 
       context: dict[str, Any], 
       handlers: dict[str, MessageHandler]
   ) -> MessageHandler | None:
       """Allow exchanges to implement custom handler resolution logic."""
       return handlers.get(routing_key)
   ```

2. **Early Context Enhancement**
   Move context enhancement before handler lookup to allow symbol-based routing decisions.

3. **Handler Registration Patterns**
   Support multiple registration patterns:
   - Simple key: `"ticker"`
   - Composite key: `"ticker.SOL_USDC"`
   - Pattern matching: `"ticker.*"`

4. **Exchange-Specific Router Interface**
   ```python
   class ExchangeWebSocketRouter(Protocol):
       def get_handler_key(self, routing_key: str, context: dict[str, Any]) -> str:
           """Generate the key used to lookup handlers."""
       
       def extract_asset_identifier(self, envelope: Any) -> str | None:
           """Extract asset identifier (symbol/coin) from envelope."""
   ```

## Conclusion

The current architecture forces exchanges to override core routing logic because the base class makes assumptions that don't match real-world exchange APIs. The main issue is the **handler registration pattern mismatch** - the base assumes simple keys while exchanges use composite keys.

These overrides indicate that the abstraction needs to be more flexible to accommodate different exchange patterns without requiring method overrides. The base class should provide extension points for custom handler resolution rather than forcing overrides of the entire routing logic.