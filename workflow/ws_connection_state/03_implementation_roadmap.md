# WebSocket Connection State Implementation Roadmap

## Overview

This document provides a concrete implementation plan for the WebSocket connection state refactoring, breaking down the work into manageable phases with clear deliverables.

## Phase 1: Core Infrastructure (Foundation)

### 1.1 Connection State Models Enhancement
**File**: `cyberdelta/apis/websocket/connection/state_tracker.py`

```python
@dataclass
class WebSocketConnectionState:
    # Add new fields
    ws_url: str  # Track which endpoint we're connected to
    is_public_connection: bool = False  # Track if this is a public stream

    def is_public_stream(self) -> bool:
        """Check if this connection is for public data only."""
        return self.is_public_connection and len(self.authenticated_channels) == 0

    def is_private_stream(self) -> bool:
        """Check if this connection has any authenticated channels."""
        return len(self.authenticated_channels) > 0
```

### 1.2 Connection Registry
**New File**: `cyberdelta/apis/websocket/connection/registry.py`

```python
from typing import Dict, List, Optional
from cyberdelta.apis.websocket.connection.state_tracker import WebSocketConnectionState
from cyberdelta.enums import ExchangeName

@dataclass
class ConnectionStateRegistry:
    """Central registry for all WebSocket connections."""

    connections: Dict[str, WebSocketConnectionState] = Field(default_factory=dict)

    def create_connection(
        self,
        connection_id: str,
        ws_url: str,
        exchange: ExchangeName,
        is_public: bool = False
    ) -> WebSocketConnectionState:
        """Create and register a new connection state."""
        state = WebSocketConnectionState(
            connection_id=connection_id,
            ws_url=ws_url,
            exchange=exchange,
            is_public_connection=is_public
        )
        self.connections[connection_id] = state
        return state

    def get_connection(self, connection_id: str) -> Optional[WebSocketConnectionState]:
        """Get connection state by ID."""
        return self.connections.get(connection_id)

    def remove_connection(self, connection_id: str) -> bool:
        """Remove a connection from registry."""
        if connection_id in self.connections:
            del self.connections[connection_id]
            return True
        return False

    def get_connections_by_exchange(
        self,
        exchange: ExchangeName
    ) -> List[WebSocketConnectionState]:
        """Get all connections for a specific exchange."""
        return [
            conn for conn in self.connections.values()
            if conn.exchange == exchange
        ]
```

### 1.3 Stream Classifier Protocol
**New File**: `cyberdelta/apis/websocket/security/stream_classifier.py`

```python
from typing import Protocol
from abc import abstractmethod

class StreamClassifier(Protocol):
    """Protocol for classifying WebSocket streams as public or private."""

    @abstractmethod
    def classify_connection(self, ws_url: str, headers: dict) -> bool:
        """Determine if a connection is for public streams."""
        ...

    @abstractmethod
    def classify_channel(self, channel: str) -> bool:
        """Determine if a specific channel is public."""
        ...

class HyperliquidStreamClassifier:
    """Hyperliquid-specific stream classification logic."""

    def classify_connection(self, ws_url: str, headers: dict) -> bool:
        # Hyperliquid uses same endpoint for both public and private
        return True  # Can be either

    def classify_channel(self, channel: str) -> bool:
        """Check if channel is public for Hyperliquid."""
        public_channels = {
            "l2Book", "trades", "allMids", "candle",
            "notification", "webData2"
        }
        # Check if channel starts with any public prefix
        return any(channel.startswith(p) for p in public_channels)

class BackpackStreamClassifier:
    """Backpack-specific stream classification logic."""

    def classify_connection(self, ws_url: str, headers: dict) -> bool:
        # Backpack uses same endpoint
        return True

    def classify_channel(self, channel: str) -> bool:
        """Check if channel is public for Backpack."""
        # Private channels start with "account."
        return not channel.startswith("account.")
```

## Phase 2: WebSocketManager Integration

### 2.1 Update WebSocketManager
**File**: `cyberdelta/apis/connectivity/ws_manager.py`

```python
class WebSocketManager:
    def __init__(self, ...):
        # Add connection state
        self.connection_state: WebSocketConnectionState | None = None
        self.connection_id: str = str(uuid.uuid4())

    async def _establish_connection(self) -> None:
        """Establishes and maintains the WebSocket connection."""
        # ... existing code ...

        # After successful connection, create state
        if self._ws_connection:
            self.connection_state = WebSocketConnectionState(
                connection_id=self.connection_id,
                ws_url=self._ws_url,
                exchange=ExchangeName(self._exchange_name),
                is_public_connection=self._determine_if_public()
            )
            self.connection_state.mark_connected()
            self._record_connection_success()

    def _determine_if_public(self) -> bool:
        """Determine if this is a public connection."""
        # Can be enhanced with classifier later
        return True  # Default to public for safety

    async def _handle_text_message(self, msg: aiohttp.WSMessage) -> None:
        """Handle TEXT type WebSocket messages."""
        try:
            # ... existing parsing code ...

            # Add connection context to message
            if self.connection_state:
                data['_connection_context'] = {
                    'connection_id': self.connection_state.connection_id,
                    'ws_url': self.connection_state.ws_url,
                    'is_public': self.connection_state.is_public_connection
                }

            await self._message_handler(data)
        except Exception as e:
            # ... existing error handling ...
```

## Phase 3: ExchangeAPI Layer Updates

### 3.1 Update Base ExchangeAPI
**File**: `cyberdelta/apis/base/exchange_api.py`

```python
class ExchangeAPI(ABC):
    def __init__(self, ...):
        # Add connection registry
        self.connection_registry = ConnectionStateRegistry()
        # ... existing init code ...

    async def _handle_websocket_message(self, message: dict[str, Any]) -> None:
        """Handle raw WebSocket message from WebSocketManager."""
        # Extract connection context if present
        conn_context = message.pop('_connection_context', None)

        if conn_context and self._ws_manager:
            # Get or create connection state
            conn_id = conn_context['connection_id']
            conn_state = self.connection_registry.get_connection(conn_id)

            if not conn_state:
                # Create new state from context
                conn_state = self.connection_registry.create_connection(
                    connection_id=conn_id,
                    ws_url=conn_context['ws_url'],
                    exchange=self.exchange_name,
                    is_public=conn_context.get('is_public', True)
                )
                conn_state.mark_connected()
        else:
            # Fallback for backwards compatibility
            conn_state = self._get_fallback_connection_state()

        # Route with connection state
        await self._route_ws_message_with_state(message, conn_state)

    async def _route_ws_message_with_state(
        self,
        message: dict[str, Any],
        connection_state: WebSocketConnectionState
    ) -> None:
        """Route message with connection state."""
        await self._route_ws_message(message, connection_state)

    @abstractmethod
    async def _route_ws_message(
        self,
        message: dict[str, Any],
        connection_state: WebSocketConnectionState | None = None
    ) -> None:
        """Route WebSocket message to handlers."""
        raise NotImplementedError
```

### 3.2 Update Backpack API
**File**: `cyberdelta/apis/backpack/bp_api.py`

```python
class BackpackAPI(ExchangeAPI):
    async def _route_ws_message(
        self,
        message: dict[str, Any],
        connection_state: WebSocketConnectionState | None = None
    ) -> None:
        """Delegate WebSocket message routing to the WebSocket router."""
        if connection_state:
            await self._bp_ws_router.route_message(
                message,
                self._ws_handlers,
                connection_state
            )
        else:
            # Backwards compatibility
            await self._bp_ws_router.route_message(message, self._ws_handlers)
```

### 3.3 Update Hyperliquid API
**File**: `cyberdelta/apis/hyperliquid/hl_api.py`

```python
class HyperliquidAPI(ExchangeAPI):
    async def _route_ws_message(
        self,
        message: dict[str, Any],
        connection_state: WebSocketConnectionState | None = None
    ) -> None:
        """Delegate WebSocket message routing to the WebSocket router."""
        if connection_state:
            await self._hl_ws_router.route_message(
                message,
                self._ws_handlers,
                connection_state
            )
        else:
            # Backwards compatibility
            await self._hl_ws_router.route_message(message, self._ws_handlers)
```

## Phase 4: Router Updates

### 4.1 Update Base Router
**File**: `cyberdelta/apis/websocket/ws_message_router.py`

```python
class WebSocketMessageRouter(ABC):
    def __init__(self, ...):
        # Remove local connection state management
        # self.conn_manager = ConnectionStateManager()  # REMOVE
        # self.conn_state = ...  # REMOVE
        pass

    async def route_message(
        self,
        message: dict[str, Any],
        handlers: dict[str, MessageHandler],
        connection_state: WebSocketConnectionState | None = None,  # Phase 1: Optional
    ) -> None:
        """Route WebSocket message to appropriate processor and handler."""
        # Phase 2: Make connection_state required
        if connection_state is None:
            # Temporary backwards compatibility
            connection_state = self._create_temporary_connection_state()
            logger.warning(
                "route_message_without_connection_state",
                message="Route message called without connection state (deprecated)"
            )

        # Use provided connection state throughout
        # ... rest of routing logic using connection_state ...
```

### 4.2 Update Hyperliquid Router
**File**: `cyberdelta/apis/hyperliquid/hl_ws_router.py`

```python
class HyperliquidWebSocketRouter(WebSocketMessageRouter):
    def _extract_routing_key_from_envelope(
        self,
        envelope: HyperliquidWebSocketMessage,
        connection_state: WebSocketConnectionState | None = None,
    ) -> str | None:
        """Extract routing key and update authentication state."""
        channel = envelope.channel

        # Handle subscription responses for authentication
        if channel == "subscriptionResponse" and connection_state:
            if isinstance(envelope, HyperliquidSubscriptionResponse):
                if envelope.is_successful and envelope.subscription_type == "userEvents":
                    # NOW we know userEvents is authenticated
                    connection_state.mark_channel_authenticated("userEvents")
                    logger.info(
                        "hyperliquid_userevents_authenticated",
                        connection_id=connection_state.connection_id,
                        message="UserEvents channel authenticated via subscription response"
                    )

        # ... rest of routing key extraction ...
```

### 4.3 Update Backpack Router
**File**: `cyberdelta/apis/backpack/bp_ws_router.py`

```python
class BackpackWebSocketRouter(WebSocketMessageRouter):
    def construct_subscription_payload(
        self,
        topic: str,
        signature_components: dict[str, Any] | None = None,
        connection_state: WebSocketConnectionState | None = None,
    ) -> BackpackRawWsSubscriptionRequest:
        """Construct subscription and track authentication."""
        # ... existing construction logic ...

        # Track authentication if signature provided
        if signature_components and connection_state:
            # Signature presence indicates channel authentication for Backpack
            connection_state.mark_channel_authenticated(topic)
            logger.info(
                "backpack_channel_authenticated",
                connection_id=connection_state.connection_id,
                channel=topic,
                message="Channel authenticated via signature"
            )

        return subscription_request
```

## Phase 5: Testing Implementation

### 5.1 Create Base Test Fixtures
**File**: `tests/integration/websocket/conftest.py`

```python
@pytest.fixture
async def ws_connection_with_state(exchange_api):
    """Provide WebSocket connection with proper state tracking."""
    await exchange_api.connect_websocket()

    # Verify connection state exists
    assert exchange_api._ws_manager is not None
    assert exchange_api._ws_manager.connection_state is not None

    yield exchange_api

    await exchange_api.close()

@pytest.fixture
def verify_connection_state():
    """Helper to verify connection state properties."""
    def _verify(api, **expected):
        conn_state = api._ws_manager.connection_state

        for key, value in expected.items():
            actual = getattr(conn_state, key)
            assert actual == value, f"Expected {key}={value}, got {actual}"

        return conn_state
    return _verify
```

### 5.2 Create Integration Tests
**File**: `tests/integration/websocket/test_connection_state.py`

```python
@pytest.mark.integration
class TestWebSocketConnectionState:
    """Integration tests for WebSocket connection state management."""

    async def test_connection_state_lifecycle(self, ws_connection_with_state):
        """Test full connection state lifecycle."""
        api = ws_connection_with_state
        conn_state = api._ws_manager.connection_state

        # Verify initial state
        assert conn_state.is_connected
        assert conn_state.connection_id
        assert conn_state.ws_url
        assert conn_state.exchange == api.exchange_name

    async def test_subscription_tracking(self, ws_connection_with_state):
        """Test that subscriptions are properly tracked."""
        api = ws_connection_with_state

        # Get real symbol
        markets = await api.get_markets(GetMarketsArgs())
        symbol = markets[0].symbol

        # Subscribe to public channel
        topic = f"ticker.{symbol.value}"
        await api.subscribe(topic, lambda ctx: None)

        # Verify subscription tracked
        conn_state = api._ws_manager.connection_state
        assert topic in conn_state.subscriptions
```

## Phase 6: Migration and Rollout

### 6.1 Backwards Compatibility Phase
- Make connection_state optional in all route_message methods
- Add deprecation warnings when called without connection_state
- Update all existing tests to not break

### 6.2 Migration Phase
- Update all callers to provide connection_state
- Update integration tests to use new pattern
- Monitor for any deprecated usage

### 6.3 Enforcement Phase
- Make connection_state required in route_message
- Remove all fallback code
- Remove temporary connection state creation

## Timeline

| Phase | Duration | Key Deliverables |
|-------|----------|-----------------|
| Phase 1 | 2 days | Core models, registry, classifiers |
| Phase 2 | 1 day | WebSocketManager integration |
| Phase 3 | 2 days | ExchangeAPI layer updates |
| Phase 4 | 2 days | Router updates for both exchanges |
| Phase 5 | 3 days | Comprehensive integration tests |
| Phase 6 | 2 days | Migration and cleanup |

**Total: ~12 days**

## Success Criteria

1. **No Global State**: All connection state passed explicitly
2. **Type Safety**: All connection_state parameters properly typed
3. **Test Coverage**: 100% coverage of new connection state code
4. **Authentication Accuracy**: Correct tracking of authenticated channels
5. **Performance**: No measurable performance degradation
6. **Backwards Compatibility**: Phased migration without breaking changes

## Risk Mitigation

1. **Risk**: Breaking existing functionality
   - **Mitigation**: Phased approach with optional parameters first

2. **Risk**: Performance impact from state tracking
   - **Mitigation**: Use efficient data structures, profile before/after

3. **Risk**: Exchange-specific edge cases
   - **Mitigation**: Comprehensive integration tests for each exchange

4. **Risk**: Concurrent connection management
   - **Mitigation**: Use connection registry with proper locking

## Monitoring and Validation

```python
class ConnectionStateMonitor:
    """Monitor connection state health and consistency."""

    async def validate_state_consistency(self, api):
        """Validate connection state is consistent."""
        checks = {
            "has_connection_state": api._ws_manager.connection_state is not None,
            "connection_id_set": bool(api._ws_manager.connection_state.connection_id),
            "exchange_matches": api._ws_manager.connection_state.exchange == api.exchange_name,
            "subscriptions_valid": all(
                sub.channel in api._ws_handlers
                for sub in api._ws_manager.connection_state.subscriptions.values()
            )
        }

        failed = [k for k, v in checks.items() if not v]
        if failed:
            logger.error(
                "connection_state_validation_failed",
                failed_checks=failed,
                connection_id=api._ws_manager.connection_state.connection_id
            )

        return len(failed) == 0
```
