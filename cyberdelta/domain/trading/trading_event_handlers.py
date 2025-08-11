"""Trading Domain Event Handlers.

High-performance event handlers for trading-related events (order, position) with
proper Symbol boundary conversion, configuration-driven behavior, and caching.

Features:
- Order event handling with order cache warming
- Position event handling with PnL tracking
- Symbol service integration with caching
- Degraded mode (cancellations only)
- Configuration-driven retry and error thresholds
"""

from decimal import Decimal
from typing import TYPE_CHECKING, Any, Protocol, assert_never, runtime_checkable

import msgspec

from cyberdelta.config.models.event_system_config import EventHandlerConfig
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.domain.base_event_handler import EventHandlerActor
from cyberdelta.enums.component_state import ComponentState
from cyberdelta.enums.event_bus import HandlerPriority
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.enums.trading import OrderEventType, PositionEventType
from cyberdelta.models.events.core import OrderEvent, PositionEvent
from cyberdelta.utils.retry_utils import create_retryer


# Forward declarations for type checking
Symbol = Any  # Will be replaced with proper Symbol import when available

if TYPE_CHECKING:
    from cyberdelta.infrastructure.event_bus import EventBus


@runtime_checkable
class TradingEventHandlerProtocol(Protocol):
    """Protocol defining expected interface for trading service integration.

    This protocol defines the methods that trading event handlers expect
    from the trading service. This is a forward-looking interface that
    will be implemented in future steps.
    """

    async def get_active_orders(self) -> list[Any]:
        """Get list of active orders for cache warming."""
        ...

    async def update_order_status(self, order_id: str, status: str, symbol: Symbol) -> None:
        """Update order status from order placed event."""
        ...

    async def process_order_fill(
        self,
        order_id: str,
        fill_price: Decimal | None,
        fill_quantity: Decimal | None,
        commission: Decimal | None,
        symbol: Symbol,
    ) -> None:
        """Process order fill from order fill event."""
        ...

    async def complete_order(
        self,
        order_id: str,
        event_type: str,
        symbol: Symbol,
        error_code: str | None = None,
        reason: str | None = None,
    ) -> None:
        """Complete order from cancellation/rejection event."""
        ...

    async def update_order_amendment(
        self, order_id: str, price: Decimal | None, quantity: Decimal | None, symbol: Symbol
    ) -> None:
        """Update order from amendment event."""
        ...

    async def track_new_position(
        self, position_id: str, symbol: Symbol, size: Decimal, average_price: Decimal
    ) -> None:
        """Track new position from position opened event."""
        ...

    async def update_position(
        self,
        position_id: str,
        symbol: Symbol,
        size: Decimal,
        average_price: Decimal,
        unrealized_pnl: Decimal | None,
    ) -> None:
        """Update existing position from position updated event."""
        ...

    async def close_position(
        self,
        position_id: str,
        symbol: Symbol,
        close_price: Decimal | None,
        realized_pnl: Decimal | None,
    ) -> None:
        """Close position from position closed event."""
        ...

    async def handle_liquidation(
        self,
        position_id: str,
        symbol: Symbol,
        liquidation_price: Decimal | None,
        realized_pnl: Decimal | None,
    ) -> None:
        """Handle position liquidation event."""
        ...


@runtime_checkable
class SymbolServiceProtocol(Protocol):
    """Protocol defining expected interface for symbol service."""

    def create_symbol(self, symbol_str: str, exchange: ExchangeName) -> Symbol:
        """Create Symbol object from string and exchange."""
        ...


logger = get_logger(__name__)


class TradingOrderEventHandler(EventHandlerActor):
    """Handles order lifecycle events with caching and degraded mode support.

    Features:
    - Order cache warming on startup
    - High-priority order event processing
    - Symbol boundary conversion with caching
    - Degraded mode: cancellations only
    - Configuration-driven error thresholds and retries
    """

    def __init__(
        self,
        event_bus: "EventBus",
        trading_service: TradingEventHandlerProtocol,
        symbol_service: SymbolServiceProtocol,
        config: EventHandlerConfig,
    ) -> None:
        """Initialize trading order event handler.

        Args:
            event_bus: Event bus for subscription and publishing
            trading_service: Trading service protocol for order operations
            symbol_service: Symbol service protocol for Symbol creation
            config: Handler configuration (no hardcoded values)
        """
        super().__init__(handler_id="trading_order_handler", event_bus=event_bus, config=config)
        self.trading_service = trading_service
        self.symbol_service = symbol_service

        # Cache for Symbol objects - key: "symbol_str:exchange"
        self._symbol_cache: dict[str, Symbol] = {}

    async def on_start(self) -> None:
        """Initialize handler with order cache warming.

        Sets up event subscriptions and warms Symbol cache with active orders.
        """
        await super().on_start()

        # Subscribe to order events with HIGH priority
        await self.subscribe_to_event(OrderEvent, HandlerPriority.HIGH)

        # Warm order cache on startup
        await self._warm_order_cache()

        logger.info(
            "trading_order_handler_started",
            handler_id=self.handler_id,
            cached_symbols=len(self._symbol_cache),
        )

    async def on_degrade(self) -> None:
        """Enter degraded mode: cancellations only."""
        await super().on_degrade()
        logger.warning(
            "trading_order_handler_degraded", handler_id=self.handler_id, mode="cancellations_only"
        )

    async def handle_event(self, event: msgspec.Struct) -> None:
        """Handle order events with Symbol boundary conversion.

        Args:
            event: OrderEvent to process
        """
        if not isinstance(event, OrderEvent):
            logger.warning(
                "unexpected_event_type", handler_id=self.handler_id, event_type=type(event).__name__
            )
            return

        # Symbol boundary conversion with caching (ALWAYS create Symbol at boundary)
        symbol = self._get_or_create_symbol(event.symbol, event.exchange)

        # Process event based on state
        if self._state == ComponentState.DEGRADED:
            # Degraded mode: only process cancellations
            if event.event_type in {
                OrderEventType.CANCELLED,
                OrderEventType.REJECTED,
                OrderEventType.EXPIRED,
            }:
                await self._handle_order_completion(event, symbol)
            else:
                logger.debug(
                    "order_event_skipped_degraded_mode",
                    handler_id=self.handler_id,
                    event_type=event.event_type,
                    order_id=event.order_id,
                )
        else:
            # Normal mode: process all order events
            await self._process_order_event(event, symbol)

    async def _process_order_event(self, event: OrderEvent, symbol: Symbol) -> None:
        """Process order event in normal mode.

        Args:
            event: Order event to process
            symbol: Symbol object (converted at boundary)
        """
        if event.event_type == OrderEventType.PLACED:
            await self._handle_order_placed(event, symbol)
        elif event.event_type in {OrderEventType.FILLED, OrderEventType.PARTIALLY_FILLED}:
            await self._handle_order_fill(event, symbol)
        elif event.event_type in {
            OrderEventType.CANCELLED,
            OrderEventType.REJECTED,
            OrderEventType.EXPIRED,
        }:
            await self._handle_order_completion(event, symbol)
        elif event.event_type == OrderEventType.AMENDED:
            await self._handle_order_amendment(event, symbol)
        else:
            logger.warning(
                "unknown_order_event_type",
                handler_id=self.handler_id,
                event_type=event.event_type,
                order_id=event.order_id,
            )

    async def _handle_order_placed(self, event: OrderEvent, symbol: Symbol) -> None:
        """Handle order placed event.

        Args:
            event: Order placed event
            symbol: Symbol object
        """
        logger.info(
            "order_placed",
            handler_id=self.handler_id,
            order_id=event.order_id,
            symbol=str(symbol),
            exchange=event.exchange.value,
            price=str(event.price) if event.price else None,
            quantity=str(event.quantity) if event.quantity else None,
        )

        # Update internal order tracking
        await self.trading_service.update_order_status(event.order_id, "placed", symbol)

    async def _handle_order_fill(self, event: OrderEvent, symbol: Symbol) -> None:
        """Handle order fill events.

        Args:
            event: Order fill event
            symbol: Symbol object
        """
        logger.info(
            "order_fill",
            handler_id=self.handler_id,
            order_id=event.order_id,
            event_type=event.event_type,
            symbol=str(symbol),
            fill_price=str(event.fill_price) if event.fill_price else None,
            fill_quantity=str(event.fill_quantity) if event.fill_quantity else None,
            commission=str(event.commission) if event.commission else None,
        )

        # Update order with fill information
        await self.trading_service.process_order_fill(
            event.order_id, event.fill_price, event.fill_quantity, event.commission, symbol
        )

    async def _handle_order_completion(self, event: OrderEvent, symbol: Symbol) -> None:
        """Handle order completion events (cancelled, rejected, expired).

        Args:
            event: Order completion event
            symbol: Symbol object
        """
        logger.info(
            "order_completed",
            handler_id=self.handler_id,
            order_id=event.order_id,
            event_type=event.event_type,
            symbol=str(symbol),
            reason=event.reason,
            error_code=event.error_code,
        )

        # Clean up order from tracking
        await self.trading_service.complete_order(
            event.order_id,
            event.event_type.value,
            symbol,
            error_code=event.error_code,
            reason=event.reason,
        )

    async def _handle_order_amendment(self, event: OrderEvent, symbol: Symbol) -> None:
        """Handle order amendment events.

        Args:
            event: Order amendment event
            symbol: Symbol object
        """
        logger.info(
            "order_amended",
            handler_id=self.handler_id,
            order_id=event.order_id,
            symbol=str(symbol),
            new_price=str(event.price) if event.price else None,
            new_quantity=str(event.quantity) if event.quantity else None,
        )

        # Update order with new parameters
        await self.trading_service.update_order_amendment(
            event.order_id, event.price, event.quantity, symbol
        )

    def _get_or_create_symbol(self, symbol_str: str, exchange: ExchangeName) -> Symbol:
        """Get Symbol from cache or create new one (boundary conversion).

        Args:
            symbol_str: Symbol string from event
            exchange: ExchangeName from event

        Returns:
            Symbol object for business logic
        """
        cache_key = f"{symbol_str}:{exchange.value}"

        # Check cache first
        if cache_key in self._symbol_cache:
            self._metrics["cache_hits"] += 1
            return self._symbol_cache[cache_key]

        # Create new Symbol at boundary
        self._metrics["cache_misses"] += 1
        symbol = self.symbol_service.create_symbol(symbol_str, exchange)
        self._symbol_cache[cache_key] = symbol

        logger.debug(
            "symbol_created_and_cached",
            handler_id=self.handler_id,
            symbol=symbol_str,
            exchange=exchange.value,
            cache_key=cache_key,
        )

        return symbol

    async def _warm_order_cache(self) -> None:
        """Warm Symbol cache with active orders.

        Retrieves active orders and pre-populates Symbol cache for performance.
        Uses create_retryer helper for configuration-driven retry with zero hardcoded values.
        """
        # Execute cache warming with configuration-driven retry
        retryer = create_retryer(self.config.retry_config)
        await retryer(self._do_cache_warming)

    async def _do_cache_warming(self) -> None:
        """Actual cache warming implementation.

        Raises:
            ConnectionError: When connection issues occur (retryable)
        """
        try:
            # Get active orders from trading service
            active_orders = await self.trading_service.get_active_orders()

            for order in active_orders:
                # Pre-populate Symbol cache
                cache_key = f"{order.symbol}:{order.exchange.value}"
                if cache_key not in self._symbol_cache:
                    symbol = self.symbol_service.create_symbol(order.symbol, order.exchange)
                    self._symbol_cache[cache_key] = symbol

            logger.info(
                "order_cache_warmed",
                handler_id=self.handler_id,
                active_orders=len(active_orders),
                cached_symbols=len(self._symbol_cache),
            )

        except ConnectionError:
            # Let tenacity retry
            raise
        except Exception:
            logger.exception("order_cache_warming_failed", handler_id=self.handler_id)
            # Don't raise - cache warming is performance optimization, not critical


class TradingPositionEventHandler(EventHandlerActor):
    """Handles position lifecycle events with PnL tracking.

    Features:
    - Position event processing with high priority
    - PnL calculation and tracking
    - Symbol boundary conversion with caching
    - Configuration-driven behavior
    """

    def __init__(
        self,
        event_bus: "EventBus",
        trading_service: TradingEventHandlerProtocol,
        symbol_service: SymbolServiceProtocol,
        config: EventHandlerConfig,
    ) -> None:
        """Initialize trading position event handler.

        Args:
            event_bus: Event bus for subscription
            trading_service: Trading service protocol for position operations
            symbol_service: Symbol service protocol for Symbol creation
            config: Handler configuration (no hardcoded values)
        """
        super().__init__(handler_id="trading_position_handler", event_bus=event_bus, config=config)
        self.trading_service = trading_service
        self.symbol_service = symbol_service

        # Cache for Symbol objects
        self._symbol_cache: dict[str, Symbol] = {}

    async def on_start(self) -> None:
        """Initialize handler and subscribe to position events."""
        await super().on_start()

        # Subscribe to position events with HIGH priority
        await self.subscribe_to_event(PositionEvent, HandlerPriority.HIGH)

        logger.info("trading_position_handler_started", handler_id=self.handler_id)

    async def handle_event(self, event: msgspec.Struct) -> None:
        """Handle position events with Symbol boundary conversion.

        Args:
            event: PositionEvent to process
        """
        if not isinstance(event, PositionEvent):
            logger.warning(
                "unexpected_event_type", handler_id=self.handler_id, event_type=type(event).__name__
            )
            return

        # Symbol boundary conversion (ALWAYS create Symbol at boundary)
        symbol = self._get_or_create_symbol(event.symbol, event.exchange)

        # Process position event
        await self._process_position_event(event, symbol)

    async def _process_position_event(self, event: PositionEvent, symbol: Symbol) -> None:
        """Process position event based on type.

        Args:
            event: Position event to process
            symbol: Symbol object (converted at boundary)
        """
        if event.event_type == PositionEventType.OPENED:
            await self._handle_position_opened(event, symbol)
        elif event.event_type == PositionEventType.UPDATED:
            await self._handle_position_updated(event, symbol)
        elif event.event_type == PositionEventType.CLOSED:
            await self._handle_position_closed(event, symbol)
        elif event.event_type == PositionEventType.LIQUIDATED:
            await self._handle_position_liquidated(event, symbol)
        else:
            # Satisfy mypy exhaustiveness checking - this should never execute
            assert_never(event.event_type)

    async def _handle_position_opened(self, event: PositionEvent, symbol: Symbol) -> None:
        """Handle position opened event.

        Args:
            event: Position opened event
            symbol: Symbol object
        """
        logger.info(
            "position_opened",
            handler_id=self.handler_id,
            position_id=event.position_id,
            symbol=str(symbol),
            size=str(event.size),
            average_price=str(event.average_price),
        )

        # Update position tracking
        await self.trading_service.track_new_position(
            event.position_id, symbol, event.size, event.average_price
        )

    async def _handle_position_updated(self, event: PositionEvent, symbol: Symbol) -> None:
        """Handle position update event.

        Args:
            event: Position updated event
            symbol: Symbol object
        """
        logger.info(
            "position_updated",
            handler_id=self.handler_id,
            position_id=event.position_id,
            symbol=str(symbol),
            size=str(event.size),
            average_price=str(event.average_price),
            unrealized_pnl=str(event.unrealized_pnl) if event.unrealized_pnl else None,
        )

        # Update position with new values
        await self.trading_service.update_position(
            event.position_id, symbol, event.size, event.average_price, event.unrealized_pnl
        )

    async def _handle_position_closed(self, event: PositionEvent, symbol: Symbol) -> None:
        """Handle position closed event.

        Args:
            event: Position closed event
            symbol: Symbol object
        """
        logger.info(
            "position_closed",
            handler_id=self.handler_id,
            position_id=event.position_id,
            symbol=str(symbol),
            close_price=str(event.close_price) if event.close_price else None,
            realized_pnl=str(event.realized_pnl) if event.realized_pnl else None,
        )

        # Close position and record PnL
        await self.trading_service.close_position(
            event.position_id, symbol, event.close_price, event.realized_pnl
        )

    async def _handle_position_liquidated(self, event: PositionEvent, symbol: Symbol) -> None:
        """Handle position liquidation event.

        Args:
            event: Position liquidated event
            symbol: Symbol object
        """
        logger.error(
            "position_liquidated",
            handler_id=self.handler_id,
            position_id=event.position_id,
            symbol=str(symbol),
            liquidation_price=str(event.close_price) if event.close_price else None,
            realized_pnl=str(event.realized_pnl) if event.realized_pnl else None,
        )

        # Handle liquidation (critical event)
        await self.trading_service.handle_liquidation(
            event.position_id, symbol, event.close_price, event.realized_pnl
        )

    def _get_or_create_symbol(self, symbol_str: str, exchange: ExchangeName) -> Symbol:
        """Get Symbol from cache or create new one (boundary conversion).

        Args:
            symbol_str: Symbol string from event
            exchange: ExchangeName from event

        Returns:
            Symbol object for business logic
        """
        cache_key = f"{symbol_str}:{exchange.value}"

        # Check cache first
        if cache_key in self._symbol_cache:
            self._metrics["cache_hits"] += 1
            return self._symbol_cache[cache_key]

        # Create new Symbol at boundary
        self._metrics["cache_misses"] += 1
        symbol = self.symbol_service.create_symbol(symbol_str, exchange)
        self._symbol_cache[cache_key] = symbol

        return symbol
