# Event Architecture Implementation Guide (Reality-Based)

**Updated:** 2025-01-18
**Approach:** Incremental improvements, not complete rewrite

## Key Principle: Pydantic Models Stay Pydantic!

### Clear Separation of Concerns:

| Layer | Model Type | Why |
|-------|------------|-----|
| **Domain Models** | 100% Pydantic | Rich validation, business logic, computed fields |
| **Configuration** | 100% Pydantic | Complex validation, one-time parse |
| **API Models** | 100% Pydantic | Industry standard, schema generation |
| **Events Only** | msgspec.Struct | High-frequency, simple data transfer |
| **Serialization** | Hybrid | Pydantic → dict → msgspec/orjson |

### What We Keep:
- **ALL Pydantic models** unchanged (PortfolioState, Order, Position, etc.)
- **Existing hybrid serialization** (perfect as-is)
- **Current EventBus** (already uses msgspec for events)

### What We Add (Minimal):
- **Simple state change events** (msgspec)
- **Event emission in state managers** (small addition)
- **StateCoordinator** (using Pydantic models)

## Directory Structure (Minimal Changes)

- `models/events/` - Event definitions (msgspec.Struct) - **ALREADY EXISTS**
- `domain/*/event_handlers.py` - Simple adapters between events and services
- `infrastructure/event_bus/` - Existing EventBus - **NO CHANGES NEEDED**
- `models/` - **KEEP ALL PYDANTIC MODELS UNCHANGED**

## Step 1: Add State Change Events (Simple)

### models/events/state_events.py (NEW - Small Addition)

```python
import msgspec
from datetime import datetime
from typing import Any

# Simple state change notification
class StateChanged(msgspec.Struct):
    """Emitted when any state manager updates."""
    component: str  # "portfolio", "safety", etc.
    key: str  # What changed
    old_value: Any  # Previous value (serialized)
    new_value: Any  # New value (serialized)
    timestamp: datetime = msgspec.field(default_factory=datetime.now)

# 2. Order Event
class OrderEvent(msgspec.Struct, tag="order"):
    """ALL order lifecycle events"""
    order_id: str
    exchange: ExchangeName  # Enum works directly
    symbol: str  # String for zero overhead
    event_type: Literal[
        "placed", "filled", "partially_filled",
        "cancelled", "rejected", "expired", "amended"
    ]
    price: Decimal | None = None
    quantity: Decimal | None = None
    fill_price: Decimal | None = None
    fill_quantity: Decimal | None = None
    remaining_quantity: Decimal | None = None
    commission: Decimal | None = None
    reason: str | None = None
    error_code: str | None = None
    timestamp: float = msgspec.field(default_factory=time.time)

# 3. Position Event
class PositionEvent(msgspec.Struct, tag="position"):
    """ALL position changes"""
    position_id: str
    symbol: str
    exchange: str
    event_type: Literal["opened", "updated", "closed", "liquidated"]
    size: Decimal
    average_price: Decimal
    realized_pnl: Decimal | None = None
    unrealized_pnl: Decimal | None = None
    close_price: Decimal | None = None
    timestamp: float = msgspec.field(default_factory=time.time)

# 4. Signal Event
class SignalEvent(msgspec.Struct, tag="signal"):
    """Trading signals"""
    signal_id: str
    strategy_name: str
    symbol: str
    action: Literal["buy", "sell", "hold", "close"]
    confidence: float
    target_price: Decimal | None = None
    target_quantity: Decimal | None = None
    timestamp: float = msgspec.field(default_factory=time.time)

# 5. Risk Event
class RiskEvent(msgspec.Struct, tag="risk"):
    """Risk management events"""
    risk_type: Literal["limit_breach", "drawdown", "exposure", "margin_call"]
    severity: Literal["info", "warning", "critical", "emergency"]
    current_value: Decimal
    limit_value: Decimal
    symbol: str | None = None
    exchange: str | None = None
    message: str
    timestamp: float = msgspec.field(default_factory=time.time)

# 6. Balance Event
class BalanceEvent(msgspec.Struct, tag="balance"):
    """Account balance updates"""
    account_id: str
    exchange: str
    currency: str
    event_type: Literal["updated", "locked", "unlocked", "settled"]
    old_balance: Decimal
    new_balance: Decimal
    locked_amount: Decimal | None = None
    timestamp: float = msgspec.field(default_factory=time.time)

# 7. System Event
class SystemEvent(msgspec.Struct, tag="system"):
    """System-level events"""
    component: str
    event_type: Literal["started", "stopped", "error", "warning", "health_check"]
    status: Literal["healthy", "degraded", "failed"]
    message: str
    metadata: dict[str, str] | None = None
    timestamp: float = msgspec.field(default_factory=time.time)
```

## Step 2: Use Existing Event Bus (Already Has msgspec!)

### infrastructure/event_bus/bus.py (ALREADY EXISTS)

```python
import msgspec
from typing import Type, TypeVar, Callable, Optional
import asyncio
from collections import defaultdict
from enum import Enum
import uuid

T = TypeVar('T', bound=msgspec.Struct)

class HandlerPriority(Enum):
    """Priority levels for event handlers"""
    CRITICAL = 0  # Risk checks, circuit breakers
    HIGH = 1      # Order validation
    NORMAL = 2    # Regular processing
    LOW = 3       # Logging, metrics

# Your existing EventBus already supports msgspec!
# From line 26: T = TypeVar("T", bound=msgspec.Struct)

# No changes needed - it already works with msgspec events
class EventBus:

    def __init__(self):
        self._handlers: dict[type, list[Callable]] = defaultdict(list)
        self._priority_handlers: dict[type, list[tuple[int, Callable]]] = defaultdict(list)
        self._decoders: dict[str, msgspec.json.Decoder] = {}
        self._encoder = msgspec.json.Encoder()
        self._pending_requests: dict[str, asyncio.Future] = {}

    async def publish(self, event: msgspec.Struct) -> None:
        """Publish typed msgspec event with priority support"""
        event_type = type(event)

        # Combine regular and priority handlers
        handlers = list(self._handlers.get(event_type, []))

        # Add priority handlers in order
        priority_handlers = sorted(self._priority_handlers.get(event_type, []), key=lambda x: x[0])
        for _, handler in priority_handlers:
            handlers.insert(0, handler)  # Higher priority runs first

        if handlers:
            # Execute concurrently for performance
            results = await asyncio.gather(
                *[handler(event) for handler in handlers],
                return_exceptions=True
            )

            # Log any handler errors
            for result in results:
                if isinstance(result, Exception):
                    # Log but don't fail
                    print(f"Handler error: {result}")

    def subscribe(self, event_type: Type[T], handler: Callable[[T], Awaitable[None]], priority: HandlerPriority = HandlerPriority.NORMAL) -> None:
        """Subscribe to specific event type with optional priority"""
        if priority == HandlerPriority.NORMAL:
            self._handlers[event_type].append(handler)
        else:
            self._priority_handlers[event_type].append((priority.value, handler))

        # Pre-compile decoder for performance
        if event_type.__name__ not in self._decoders:
            self._decoders[event_type.__name__] = msgspec.json.Decoder(event_type)

    async def request(self, request: msgspec.Struct, timeout: float = 5.0) -> Optional[msgspec.Struct]:
        """Send request and wait for response (Nautilus pattern)"""
        request_id = str(uuid.uuid4())
        future = asyncio.get_event_loop().create_future()
        self._pending_requests[request_id] = future

        # Add request ID if the struct has the field
        if hasattr(request, 'request_id'):
            request.request_id = request_id

        await self.publish(request)

        try:
            response = await asyncio.wait_for(future, timeout)
            return response
        except asyncio.TimeoutError:
            return None
        finally:
            self._pending_requests.pop(request_id, None)

    async def respond(self, request_id: str, response: msgspec.Struct) -> None:
        """Send response to a pending request"""
        future = self._pending_requests.get(request_id)
        if future and not future.done():
            future.set_result(response)

    async def publish_raw(self, raw_bytes: bytes, event_type: Type[T]) -> None:
        """Ultra-fast path for WebSocket data"""
        decoder = self._decoders.get(event_type.__name__)
        if not decoder:
            decoder = msgspec.json.Decoder(event_type)
            self._decoders[event_type.__name__] = decoder

        event = decoder.decode(raw_bytes)
        await self.publish(event)
```

## Important: Architecture Principles

### 1. Pydantic Models Are NOT Converted
```python
# KEEP AS-IS (Pydantic):
class PortfolioState(BaseModel):  # ✅ KEEP
    balances: dict[str, SpotBalance]
    positions: dict[str, DerivativePosition]

class Order(BaseModel):  # ✅ KEEP
    order_id: str
    symbol: Symbol
    quantity: Decimal

# ONLY Events use msgspec:
class StateChanged(msgspec.Struct):  # Events only!
    component: str
    key: str
    old_value: str  # JSON serialized Pydantic
    new_value: str  # JSON serialized Pydantic
```

### 2. Hybrid Serialization Works Perfect
```python
# Your existing approach - DON'T CHANGE:
def serialize(state: PortfolioState) -> bytes:
    # Step 1: Pydantic validation
    data = state.model_dump_json()  # Rich validation
    # Step 2: Fast serialization
    return msgspec.encode(data)  # Speed
```

```python
class AnyEventHandler:
    def __init__(self, symbol_service: SymbolService):
        self.symbol_service = symbol_service
        self._symbol_cache = {}  # Cache for performance

    async def handle_event(self, event: MarketData):
        # ALWAYS create Symbol at the boundary
        cache_key = f"{event.symbol}:{event.exchange.value}"
        symbol = self._symbol_cache.get(cache_key)
        if not symbol:
            symbol = self.symbol_service.create_symbol(
                event.symbol,  # String from event
                event.exchange  # Enum from event
            )
            self._symbol_cache[cache_key] = symbol

        # Now use Symbol object throughout handler
        await self.process_with_symbol(symbol)
```

## Step 3: Simple Event Handlers (Not Over-Engineered)

### domain/portfolio/event_handlers.py (Simple Adapter)

```python
from cyberdelta.models.events.state_events import StateChanged
from cyberdelta.domain.portfolio.state_manager import PortfolioStateManager
import logging

logger = logging.getLogger(__name__)

class PortfolioEventHandler:
    """Simple adapter between events and portfolio service."""

    def __init__(self, portfolio_manager: PortfolioStateManager, event_bus):
        self.portfolio_manager = portfolio_manager
        self.event_bus = event_bus

        # Subscribe to relevant events
        event_bus.subscribe(StateChanged, self.handle_state_change)

    async def handle_state_change(self, event: StateChanged):
        """React to state changes from other components."""
        if event.component == "safety" and "circuit_breaker" in event.key:
            # Safety system changed state, might affect portfolio
            logger.info(f"Circuit breaker state changed: {event.new_value}")
            # Could trigger portfolio actions if needed

    def __init__(self, handler_id: str, event_bus: MsgspecEventBus):
        self.handler_id = handler_id
        self.event_bus = event_bus
        self._state = ComponentState.PRE_INITIALIZED
        self._error_count = 0
        self._metrics = {}
        self._cache = {}  # Handler-level cache

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(ConnectionError),
        before_sleep=lambda retry_state: logger.warning(
            f"Retrying handler start, attempt {retry_state.attempt_number}"
        )
    )
    async def start(self) -> None:
        """Start the handler with initialization and retry logic"""
        if self._state != ComponentState.PRE_INITIALIZED:
            return

        await self.on_start()
        self._state = ComponentState.RUNNING

    async def stop(self) -> None:
        """Stop the handler with cleanup"""
        if self._state not in [ComponentState.RUNNING, ComponentState.DEGRADED]:
            return

        await self.on_stop()
        self._state = ComponentState.STOPPED

    async def degrade(self) -> None:
        """Enter degraded mode (reduced functionality)"""
        if self._state != ComponentState.RUNNING:
            return

        self._state = ComponentState.DEGRADED
        await self.on_degrade()

    async def fault(self) -> None:
        """Enter faulted state (non-operational)"""
        self._state = ComponentState.FAULTED
        await self.on_fault()

    # Lifecycle hooks to override
    @abstractmethod
    async def on_start(self) -> None:
        """Initialize handler resources"""
        pass

    @abstractmethod
    async def on_stop(self) -> None:
        """Cleanup handler resources"""
        pass

    async def on_degrade(self) -> None:
        """Handle degraded mode"""
        pass

    async def on_fault(self) -> None:
        """Handle fault state"""
        pass

    # Error handling with auto-degradation and retry
    @retry(
        stop=stop_after_attempt(2),
        wait=wait_exponential(multiplier=0.5, min=1, max=5),
        retry=retry_if_exception_type((ConnectionError, TimeoutError))
    )
    async def handle_with_degradation(self, event: msgspec.Struct) -> None:
        """Handle event with automatic degradation on errors and tenacity retry"""
        try:
            await self.handle_event(event)
            self._error_count = 0  # Reset on success
        except (ConnectionError, TimeoutError):
            # Let tenacity retry these
            raise
        except Exception as e:
            self._error_count += 1
            if self._error_count > 10:
                await self.degrade()
            elif self._error_count > 20:
                await self.fault()
            raise

    @abstractmethod
    async def handle_event(self, event: msgspec.Struct) -> None:
        """Main event handling method"""
        pass
```

## Step 4: Enhance Existing State Managers (Incremental)

### domain/portfolio/state_manager.py (Keep ALL Pydantic)

```python
from cyberdelta.models.portfolio.state import PortfolioState  # PYDANTIC!
from pydantic import BaseModel

class PortfolioStateManager:
    """Existing manager - ALL models stay Pydantic."""

    def __init__(self, config, storage, event_bus=None):
        # Pydantic model - NO CHANGE
        self._cached_state: PortfolioState | None = None
        self._event_bus = event_bus

    async def update_from_fill(self, fill: Fill) -> None:
        """Update state - Pydantic models throughout."""
        # fill is Pydantic, _cached_state is Pydantic
        old_state = self._cached_state.model_copy() if self._cached_state else None

        # ... existing update logic with Pydantic models ...

        # NEW: Emit event (but models stay Pydantic)
        if self._event_bus and old_state:
            await self._event_bus.publish(StateChanged(
                component="portfolio",
                key="fill_processed",
                # Serialize Pydantic for event
                old_value=old_state.model_dump_json(),
                new_value=self._cached_state.model_dump_json()
            ))

    def get_checksum(self) -> str:
        """Use Pydantic's serialization."""
        data = self._cached_state.model_dump_json()
        return hashlib.sha256(data.encode()).hexdigest()

    def __init__(self, trading_service, symbol_service, event_bus: MsgspecEventBus):
        super().__init__("trading_handler", event_bus)
        self.trading_service = trading_service
        self.symbol_service = symbol_service

        # Performance optimization caches
        self._order_cache: Dict[str, Order] = {}
        self._symbol_cache: Dict[str, Symbol] = {}

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((ConnectionError, TimeoutError))
    )
    async def on_start(self) -> None:
        """Initialize handler and warm caches with retry logic"""
        # Subscribe to events with priorities
        self.event_bus.subscribe(OrderEvent, self.handle_order_event, HandlerPriority.HIGH)
        self.event_bus.subscribe(PositionEvent, self.handle_position_event, HandlerPriority.NORMAL)

        # Warm order cache with open orders (with retry)
        try:
            open_orders = await self.trading_service.get_open_orders()
            for order in open_orders:
                self._order_cache[order.order_id] = order
        except (ConnectionError, TimeoutError):
            # Let tenacity retry these
            raise
        except Exception as e:
            # Log but don't fail startup for other errors
            logger.warning(f"Failed to warm order cache: {e}")

    async def on_stop(self) -> None:
        """Cleanup and persist state"""
        # Persist any pending updates
        for order_id, order in self._order_cache.items():
            if order.is_dirty:  # Assuming we track dirty state
                await self.trading_service.save_order(order)

    async def on_degrade(self) -> None:
        """Enter degraded mode - only allow cancellations"""
        await self.event_bus.publish(SystemEvent(
            component="trading_handler",
            event_type="warning",
            status="degraded",
            message="Trading handler degraded - only cancellations allowed"
        ))

    async def handle_event(self, event: msgspec.Struct) -> None:
        """Main event handling with hierarchical routing"""
        if self._state == ComponentState.DEGRADED:
            # In degraded mode, only handle cancellations
            if isinstance(event, OrderEvent) and event.event_type == "cancelled":
                await self.handle_order_event(event)
            return

        # Normal routing
        if isinstance(event, OrderEvent):
            await self.handle_order_event(event)
        elif isinstance(event, PositionEvent):
            await self.handle_position_event(event)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.5, min=1, max=5),
        retry=retry_if_exception_type((ConnectionError, TimeoutError)),
        before_sleep=lambda retry_state: logger.info(
            f"Retrying order event handling for {retry_state.args[1].order_id}"
        )
    )
    async def handle_order_event(self, event: OrderEvent) -> None:
        """Handle order events with caching, hierarchical routing, and tenacity retry"""
        # Check cache first for performance
        order = self._order_cache.get(event.order_id)
        if not order:
            order = await self.trading_service.get_order(event.order_id)
            if not order and event.event_type == "placed":
                # New order from exchange
                order = Order(
                    order_id=event.order_id,
                    exchange=event.exchange,
                    symbol=event.symbol,
                    quantity=event.quantity or Decimal("0"),
                    price=event.price
                )
            if order:
                self._order_cache[event.order_id] = order
            else:
                return

        # Hierarchical routing - try specific handler first
        handler_method = f"on_order_{event.event_type}"
        if hasattr(self, handler_method):
            await getattr(self, handler_method)(event, order)
        else:
            # Fallback to generic handling
            match event.event_type:
                case "filled":
                    order.fill(
                        fill_price=event.fill_price or Decimal("0"),
                        fill_quantity=event.fill_quantity or Decimal("0"),
                        commission=event.commission or Decimal("0")
                    )

            case "partially_filled":
                order.partial_fill(
                    fill_price=event.fill_price or Decimal("0"),
                    fill_quantity=event.fill_quantity or Decimal("0"),
                    remaining=event.remaining_quantity or Decimal("0")
                )

            case "cancelled":
                order.cancel(reason=event.reason or "User requested")

            case "rejected":
                order.reject(
                    reason=event.reason or "Unknown",
                    error_code=event.error_code
                )

            case "expired":
                order.expire()

            case "amended":
                if event.price:
                    order.amend_price(event.price)
                if event.quantity:
                    order.amend_quantity(event.quantity)

        # Update cache and save
        self._order_cache[order.order_id] = order
        await self.trading_service.save_order(order)

        # Emit derived events if needed
        if event.event_type == "filled" and order.is_fully_filled():
            await self.event_bus.publish(SystemEvent(
                component="trading_handler",
                event_type="info",
                status="healthy",
                message=f"Order {event.order_id} fully filled"
            ))

    # Specific handlers for better organization (Nautilus pattern)
    async def on_order_filled(self, event: OrderEvent, order: Order) -> None:
        """Specific handler for filled orders"""
        order.fill(
            fill_price=event.fill_price or Decimal("0"),
            fill_quantity=event.fill_quantity or Decimal("0"),
            commission=event.commission or Decimal("0")
        )
        # Additional fill-specific logic
        await self._update_position_from_fill(order)

    async def on_order_cancelled(self, event: OrderEvent, order: Order) -> None:
        """Specific handler for cancelled orders"""
        order.cancel(reason=event.reason or "User requested")
        # Release any reserved capital
        await self._release_order_capital(order)
```

### domain/portfolio/portfolio_event_handlers.py

```python
from cyberdelta.models.events.core import PositionEvent, BalanceEvent
from cyberdelta.domain.portfolio.portfolio_service import PortfolioService
from decimal import Decimal

class PortfolioEventHandler:
    """Adapts events to portfolio domain operations"""

    def __init__(self, portfolio_service: PortfolioService, event_bus: MsgspecEventBus):
        self.portfolio_service = portfolio_service
        self.event_bus = event_bus

        event_bus.subscribe(PositionEvent, self.handle_position_event)
        event_bus.subscribe(BalanceEvent, self.handle_balance_event)

    async def handle_position_event(self, event: PositionEvent) -> None:
        """Translate position event to domain operations"""
        position = await self.position_repo.get(event.position_id)

        match event.event_type:
            case "opened":
                if not position:
                    position = Position(
                        position_id=event.position_id,
                        symbol=event.symbol,
                        exchange=event.exchange,
                        size=event.size,
                        average_price=event.average_price
                    )
                else:
                    position.size = event.size
                    position.average_price = event.average_price

            case "updated":
                if position:
                    size_delta = event.size - position.size
                    if size_delta > 0:
                        position.increase(size_delta, event.average_price)
                    elif size_delta < 0:
                        pnl = position.decrease(-size_delta, event.average_price)
                        # Could track PnL here

            case "closed":
                if position:
                    pnl = position.close(event.close_price or event.average_price)
                    # Emit PnL event
                    risk_event = RiskEvent(
                        risk_type="exposure",
                        severity="info",
                        current_value=Decimal("0"),
                        limit_value=Decimal("0"),
                        message=f"Position closed with PnL: {pnl}"
                    )
                    await self.event_bus.publish(risk_event)

            case "liquidated":
                if position:
                    position.liquidate(event.close_price or event.average_price)

        if position:
            await self.position_repo.save(position)
```

## Step 5: Keep Existing WebSocket Approach

### WebSocket Already Optimized

```python
# Your existing WebSocket handlers likely already use fast parsing
# If they use Pydantic models for exchange data, that's fine!
# The hybrid approach (Pydantic models + msgspec serialization) works well

# No changes needed to WebSocket handling
```

## Step 6: Workflows (Only If Needed)

### Keep It Simple

```python
# Workflows are optional - only add if you have complex multi-step processes
# For most cases, direct service calls are simpler and clearer

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((ConnectionError, TimeoutError))
    )
    async def perform_risk_check_with_retry(self, check: str) -> bool:
        """Perform risk check with retry logic"""
        return await self.perform_risk_check(check)

    async def execute(self):
        # Step 1: Risk checks with retry
        for check in self.risk_checks:
            if not await self.perform_risk_check_with_retry(check):
                raise ValueError(f"Risk check failed: {check}")

        # Step 2: Place order with retry
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=2, max=10),
            retry=retry_if_exception_type((ConnectionError, TimeoutError))
        ):
            with attempt:
                order_event = OrderEvent(
                    order_id=self.order_id,
                    exchange="backpack",  # Could be from config
                    symbol=self.symbol,
                    event_type="placed",
                    price=self.price,
                    quantity=self.quantity
                )

                # Publish to msgspec bus
                await self.event_bus.publish(order_event)

        # Step 3: Wait for fill with retry
        try:
            # Wait for fill confirmation
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(2),
                wait=wait_exponential(multiplier=2, min=5, max=30)
            ):
                with attempt:
                    fill_event = await self.event_bus.expect(
                        OrderEvent,
                        lambda e: e.order_id == self.order_id and e.event_type == "filled",
                        timeout=30
                    )
                    return fill_event.order_id
        except TimeoutError:
            # Handle timeout - cancel order with retry
            await self.cancel_order_with_retry()
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.5, min=1, max=5)
    )
    async def cancel_order_with_retry(self):
        """Cancel order with retry logic"""
        await self.cancel_order()

class RebalanceWorkflow(BaseEvent[bool]):
    """Portfolio rebalancing"""
    target_allocations: dict[str, Decimal]
    max_slippage: Decimal = Decimal("0.01")

    async def execute(self):
        # Complex rebalancing logic
        pass

class EmergencyLiquidation(BaseEvent):
    """Emergency position closing"""
    reason: str
    positions: list[str]
    max_loss: Decimal

    async def execute(self):
        # Urgent liquidation logic
        pass
```

## Step 7: Minimal Bootstrap Changes

### application/bootstrap.py (Minimal Changes)

```python
# In your existing bootstrap, just pass event_bus to state managers:

class Application:
    async def initialize(self):
        # Your existing event bus
        self.event_bus = EventBus()  # Already exists

        # Pass event_bus to state managers (small change)
        self.portfolio_manager = PortfolioStateManager(
            config=self.config,
            storage=portfolio_storage,
            event_bus=self.event_bus  # NEW: Pass event bus
        )

        # That's it! State managers can now emit events

    async def initialize(self):
        """Initialize and start all components"""
        # Create enhanced event bus
        self.event_bus = MsgspecEventBus()

        # Create repositories (existing)
        order_repo = OrderRepository()
        position_repo = PositionRepository()
        balance_repo = BalanceRepository()

        # Create services
        trading_service = TradingService(order_repo)
        portfolio_service = PortfolioService(position_repo)
        risk_service = RiskService()
        market_service = MarketService()
        symbol_service = SymbolService()

        # Create handlers with lifecycle support
        self.handlers = [
            TradingEventHandler(trading_service, symbol_service, self.event_bus),
            PortfolioEventHandler(portfolio_service, self.event_bus),
            RiskEventHandler(risk_service, self.event_bus),
            MarketEventHandler(market_service, self.event_bus),
        ]

        # Start all handlers (initializes resources, subscribes to events)
        for handler in self.handlers:
            await handler.start()
            print(f"Started {handler.handler_id}: {handler._state}")

        # Create WebSocket processor
        self.ws_processor = WebSocketProcessor(self.event_bus)

        # Create orchestrator for workflows
        self.orchestrator = BubusOrchestrator()

        return self

    async def shutdown(self):
        """Graceful shutdown of all components"""
        # Stop handlers in reverse order
        for handler in reversed(self.handlers):
            try:
                await handler.stop()
                print(f"Stopped {handler.handler_id}: {handler._state}")
            except Exception as e:
                print(f"Error stopping {handler.handler_id}: {e}")

    async def check_health(self) -> dict:
        """Check health of all components"""
        health = {}
        for handler in self.handlers:
            health[handler.handler_id] = {
                "state": handler._state.value,
                "error_count": handler._error_count,
                "metrics": handler._metrics
            }
        return health

# Usage
async def main():
    manager = EventSystemManager()

    try:
        # Initialize and start
        await manager.initialize()

        # Check health
        health = await manager.check_health()
        print(f"System health: {health}")

        # Run your trading logic here
        # ...

    finally:
        # Ensure clean shutdown
        await manager.shutdown()
```

## Testing Strategy (Unchanged)

### Domain Models Stay the Same

```python
def test_portfolio_state():
    """Pydantic models work exactly as before."""
    state = PortfolioState(
        balances={"USDC": SpotBalance(...)},
        positions={}
    )

    # All validation still works
    assert state.balances["USDC"].available > 0
```

### Unit Tests - Event Handlers

```python
async def test_trading_event_adapter():
    """Test adapter translation"""
    # Mock service
    mock_service = Mock()
    mock_symbol_service = Mock()

    # Create adapter
    event_bus = MsgspecEventBus()
    adapter = TradingEventHandler(mock_service, mock_symbol_service, event_bus)

    # Create event
    event = OrderEvent(
        order_id="123",
        event_type="filled",
        fill_price=Decimal("100"),
        fill_quantity=Decimal("10")
    )

    # Handle event
    await adapter.handle_order_event(event)

    # Verify service method was called
    mock_service.process_fill.assert_called_once_with(
        order_id="123",
        fill_price=Decimal("100"),
        fill_quantity=Decimal("10")
    )
```

## Summary: Pydantic-First Architecture

### Final Architecture Decision

```python
# This is our approach - FINAL:

class PortfolioState(BaseModel):  # ✅ PYDANTIC
    """Domain model with rich validation."""
    balances: dict[str, SpotBalance]

    @field_validator('balances')
    def validate_balances(cls, v):
        # Complex business logic
        return v

class StateChanged(msgspec.Struct):  # ✅ msgspec for events ONLY
    """Simple event for notifications."""
    component: str
    old_value: str
    new_value: str

# Serialization stays hybrid:
def save_state(state: PortfolioState):
    # Pydantic validation + msgspec speed
    data = state.model_dump_json()  # Pydantic
    binary = msgspec.encode(data)   # Fast
    await storage.save(binary)
```

### What We're KEEPING (No Changes):
- ✅ **ALL Pydantic domain models** (PortfolioState, Order, Position, Fill, etc.)
- ✅ **ALL Pydantic config models** (AppSettings, ExchangeConfig, etc.)
- ✅ **ALL Pydantic API models** (request/response models)
- ✅ **Hybrid serialization** (already optimal)
- ✅ **EventBus with msgspec** (already done)

### What We're ADDING (Minimal):
- ✅ **Checksums to portfolio manager** (using Pydantic's model_dump_json)
- ✅ **Persistence to safety manager** (using Pydantic models)
- ✅ **Simple StateChanged events** (msgspec, for notifications only)
- ✅ **StateCoordinator** (coordinates Pydantic models)

### What We're NOT Doing:
- ❌ **NO conversion of ANY Pydantic model to msgspec**
- ❌ **NO new state module from scratch**
- ❌ **NO replacement of existing serialization**
- ❌ **NO Redis or other complexity**

**Timeline:** 1-2 weeks of incremental work
**Risk:** Very low - we're keeping all existing models
**Benefit:** Better coordination and observability without rewrites
