# Authentication Tracking Solution

## The Problem

In `ws_context.py`, the `create_error_context()` method needs to populate `is_authenticated` in the `StreamErrorContext`, but currently just hardcodes it to `False`:

```python
is_authenticated=False,  # Would need auth tracking
```

Previously it was incorrectly using:
```python
is_authenticated=bool(self.is_private_message),  # Wrong assumption
```

This assumed that private channels = authenticated, which is incorrect logic.

## Understanding Authentication in WebSocket

### What Authentication Actually Means

1. **Connection Level**: WebSocket connection has been authenticated with API key/signature
2. **Channel Level**: Subscription to private channels requires authentication
3. **Message Level**: Messages from private channels come from authenticated subscriptions

### Current Architecture Gap

The `WebSocketMessageContext` doesn't track authentication state because:
- Authentication happens at the **connection/subscription level**
- Context is created **per message**
- No connection state is passed to the context

## Better Solutions

### Solution 1: Add Authentication Tracking to Context (Recommended)

Add an optional field to track if the message came from an authenticated channel:

```python
# In ws_context.py
class WebSocketMessageContext[EnvelopeType: "BaseModel"](BaseModel):
    # ... existing fields ...

    # Add this field
    is_authenticated_channel: bool = Field(
        default=False,
        description="Whether this message came from an authenticated channel"
    )
```

Then use our new `ChannelClassifier`:

```python
# In create_error_context method
from cyberdelta.apis.websocket.security.channel_classifier import ChannelClassifier

def create_error_context(self, ...) -> StreamErrorContext:
    # Determine if this is an authenticated channel
    is_authenticated = (
        self.is_authenticated_channel  # Explicitly set
        or ChannelClassifier.requires_authentication(self.routing_key)  # Inferred
    )

    return StreamErrorContext(
        # ...
        is_authenticated=is_authenticated,
        # ...
    )
```

### Solution 2: Pass Authentication State from Router

The router knows which subscriptions are authenticated. It should pass this info:

```python
# In router when creating context
typed_context = self._create_typed_context(
    validated_envelope,
    routing_key,
    message_id,
    is_authenticated=self._is_authenticated_subscription(routing_key)
)
```

### Solution 3: Track at Connection Level (Most Correct)

Create a connection state tracker:

```python
# New file: cyberdelta/apis/websocket/connection/state_tracker.py
class WebSocketConnectionState:
    """Tracks WebSocket connection state."""

    def __init__(self, connection_id: str):
        self.connection_id = connection_id
        self.is_authenticated = False
        self.authenticated_channels: set[str] = set()

    def mark_authenticated(self, channel: str | None = None):
        """Mark connection or channel as authenticated."""
        if channel:
            self.authenticated_channels.add(channel)
        else:
            self.is_authenticated = True

    def is_channel_authenticated(self, channel: str) -> bool:
        """Check if a specific channel is authenticated."""
        return (
            self.is_authenticated  # Whole connection authenticated
            or channel in self.authenticated_channels  # Specific channel
        )
```

## Recommended Implementation

### Step 1: Update Context Model

```python
# In ws_context.py
class WebSocketMessageContext[EnvelopeType: "BaseModel"](BaseModel):
    # ... existing fields ...

    # Add authentication tracking
    is_authenticated_channel: bool = Field(
        default=False,
        description="Whether message is from authenticated channel"
    )
```

### Step 2: Update create_error_context

```python
# In ws_context.py
from cyberdelta.apis.websocket.security.channel_classifier import ChannelClassifier

def create_error_context(self, ...) -> StreamErrorContext:
    # Better authentication detection
    is_authenticated = self.is_authenticated_channel

    # If not explicitly set, infer from channel type
    if not is_authenticated and self.routing_key:
        is_authenticated = ChannelClassifier.requires_authentication(self.routing_key)

    return StreamErrorContext(
        # ...
        is_authenticated=is_authenticated,
        # ...
    )
```

### Step 3: Router Sets Authentication

When the router creates a context for a message from an authenticated subscription:

```python
# In router
if signature_components:  # This subscription is authenticated
    context.is_authenticated_channel = True
```

## Why This Is Better

1. **Explicit Tracking**: Authentication is explicitly tracked, not guessed
2. **Separation of Concerns**: Channel privacy ≠ authentication
3. **Flexibility**: Can handle both authenticated and unauthenticated private channels
4. **Correctness**: Reflects actual authentication state, not assumptions

## Implementation Priority

**Quick Fix (5 minutes):**
Use ChannelClassifier to infer authentication from routing key

**Proper Fix (30 minutes):**
Add `is_authenticated_channel` field and have router set it correctly

**Best Fix (2 hours):**
Implement connection state tracking for comprehensive auth management
