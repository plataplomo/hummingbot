"""
Testable class wrappers that expose protected methods for testing.

This module provides test-specific subclasses that expose protected methods
as public test methods, following the recommendations from DETAILED_TEST_ANALYSIS.md.
"""

from decimal import Decimal

from cyberdelta.core.execution_handler import ExecutionHandler, TradeExecution
from cyberdelta.core.models import Order, OrderSide, OrderType, TimeInForce


class TestableExecutionHandler(ExecutionHandler):
    """ExecutionHandler with exposed internals for testing.

    This class exposes protected methods as public test methods to avoid
    direct access to protected members in tests, as recommended in the
    test analysis documents.

    Note: This is not a pytest test class despite the name prefix.
    """
    __test__ = False  # Tell pytest this is not a test class

    async def test_place_order_with_retry(
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
        """Test wrapper for _place_order_with_retry."""
        return await self._place_order_with_retry(
            execution, exchange_id, symbol, side, quantity, order_type,
            price, time_in_force, reduce_only, post_only, is_long_leg,
        )

    async def test_get_order_status(
        self,
        execution: TradeExecution,
        exchange_id: str,
        order_id: str,
        symbol: str | None = None,
        client_order_id: str | None = None,
    ) -> Order | None:
        """Test wrapper for _get_order_status."""
        return await self._get_order_status(
            execution, exchange_id, order_id, symbol, client_order_id,
        )

    async def test_compensate_position(
        self,
        execution: TradeExecution,
        exchange_id: str,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
    ) -> bool:
        """Test wrapper for _compensate_position."""
        return await self._compensate_position(execution, exchange_id, symbol, side, quantity)

    def test_add_to_history(self, execution: TradeExecution) -> None:
        """Test wrapper for _add_to_history."""
        return self._add_to_history(execution)

    @property
    def test_execution_history(self) -> list[TradeExecution]:
        """Test accessor for execution history."""
        return list(self.executions)  # Return a copy for safety
