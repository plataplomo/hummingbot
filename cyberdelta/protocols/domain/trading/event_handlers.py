"""Trading event handler protocols."""

from decimal import Decimal
from typing import Any, Protocol, runtime_checkable

from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.enums.trading import OrderEventType


# Forward declarations for type checking
Symbol = Any  # Will be replaced with proper Symbol import when available


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

    async def update_order_status(
        self, order_id: str, status: OrderEventType, symbol: Symbol
    ) -> None:
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
        event_type: OrderEventType,
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
