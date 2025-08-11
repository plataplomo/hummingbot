"""Service protocols for CyberDeltaEngine event system."""

from typing import Protocol, runtime_checkable

from cyberdelta.models.derivative_position import DerivativePosition
from cyberdelta.models.events.workflow_context import WorkflowContextModel
from cyberdelta.models.portfolio.state import PortfolioState


@runtime_checkable
class RiskService(Protocol):
    """Protocol for risk management service."""

    async def check_risk_limits(self, context: WorkflowContextModel) -> bool:
        """Check if risk limits are satisfied.

        Args:
            context: Workflow context with order details

        Returns:
            True if risk checks pass, False otherwise
        """
        ...


@runtime_checkable
class TradingService(Protocol):
    """Protocol for trading execution service."""

    async def place_order(self, context: WorkflowContextModel) -> str:
        """Place an order based on context.

        Args:
            context: Workflow context with order details

        Returns:
            Order ID from exchange
        """
        ...

    async def cancel_all_orders(self) -> int:
        """Cancel all open orders.

        Returns:
            Number of orders cancelled
        """
        ...

    async def get_pending_orders(self) -> list[str]:
        """Get list of pending order IDs.

        Returns:
            List of pending order IDs
        """
        ...


@runtime_checkable
class PortfolioService(Protocol):
    """Protocol for portfolio management service."""

    async def get_positions(self) -> dict[str, DerivativePosition]:
        """Get current portfolio positions.

        Returns:
            Dictionary of positions by symbol
        """
        ...

    async def update_positions(self, context: WorkflowContextModel) -> None:
        """Update portfolio positions.

        Args:
            context: Workflow context with position updates
        """
        ...


@runtime_checkable
class AlertService(Protocol):
    """Protocol for alert notification service."""

    async def send_critical_alert(self, message: str) -> None:
        """Send critical alert to all channels.

        Args:
            message: Alert message to send
        """
        ...


@runtime_checkable
class StateService(Protocol):
    """Protocol for state persistence service."""

    async def persist_state(self) -> None:
        """Persist current application state to storage."""
        ...

    async def load_state(self) -> PortfolioState:
        """Load application state from storage.

        Returns:
            Saved portfolio state
        """
        ...
