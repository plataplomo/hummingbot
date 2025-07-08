"""Testable class wrappers that expose protected methods for testing.

This module provides test-specific subclasses that expose protected methods
as public test methods, following the recommendations from DETAILED_TEST_ANALYSIS.md.
"""

import asyncio
from decimal import Decimal

from cyberdelta.core.execution_handler import ExecutionHandler
from cyberdelta.core.models import Order, OrderSide, OrderType, TimeInForce
from cyberdelta.core.models.execution import TradeExecution
from cyberdelta.core.services.interfaces import OrderRequest


class TestableExecutionHandler(ExecutionHandler):
    """ExecutionHandler with exposed internals for testing.

    This class exposes protected methods as public test methods to avoid
    direct access to protected members in tests, as recommended in the
    test analysis documents.

    Note: This is not a pytest test class despite the name prefix.
    """

    __test__ = False  # Tell pytest this is not a test class

    async def expose_place_order_with_retry(
        self,
        execution: TradeExecution,
        exchange_id: str,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        order_type: OrderType,
        price: Decimal | None = None,
        time_in_force: TimeInForce = TimeInForce.GTC,
        reduce_only: bool = False,
        post_only: bool = False,
        is_long_leg: bool = True,
    ) -> Order | None:
        """Wrapper that exposes _place_order_with_retry for testing."""
        # The refactored ExecutionHandler uses services now
        if hasattr(self.services, "order_service"):
            request = OrderRequest(
                exchange_id=exchange_id,
                symbol=symbol,
                side=side,
                quantity=quantity,
                order_type=order_type,
                price=price,
                time_in_force=time_in_force,
                reduce_only=reduce_only,
                post_only=post_only,
            )
            result = await self.services.order_service.place_order_with_retry(request)
            return result.data if result.success else None
        return None

    async def expose_get_order_status(
        self,
        execution: TradeExecution,
        exchange_id: str,
        order_id: str,
        symbol: str | None = None,
        client_order_id: str | None = None,
    ) -> Order | None:
        """Wrapper that exposes _get_order_status for testing."""
        # The refactored ExecutionHandler uses services now
        if hasattr(self.services, "order_service"):
            result = await self.services.order_service.get_order_status(order_id, exchange_id)
            return result.data if result.success else None
        return None

    async def test_compensate_position(
        self,
        execution: TradeExecution,
        exchange_id: str,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
    ) -> bool:
        """Test wrapper for _compensate_position."""
        # The refactored ExecutionHandler uses services now
        if hasattr(self.services, "compensation_service"):
            result = await self.services.compensation_service.compensate_position(
                execution, "test_leg", quantity
            )
            return result.success
        return False

    def test_add_to_history(self, execution: TradeExecution) -> None:
        """Test wrapper for _add_to_history.

        Args:
            execution: Trade execution to add to history.
        """
        # The refactored ExecutionHandler uses services now
        if hasattr(self.services, "state_manager"):
            try:
                loop = asyncio.get_event_loop()
                loop.run_until_complete(
                    self.services.state_manager.finalize_execution(execution.id)
                )
            except RuntimeError:
                pass

    @property
    def test_execution_history(self) -> list[TradeExecution]:
        """Test accessor for execution history.

        Returns:
            Copy of the execution history list.
        """
        return list(self.get_active_executions())  # Return a copy for safety
