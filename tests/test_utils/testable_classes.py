"""
Testable class wrappers that expose protected methods for testing.

This module provides test-specific subclasses that expose protected methods
as public test methods, following the recommendations from DETAILED_TEST_ANALYSIS.md.
"""

from typing import Any
from cyberdelta.core.execution_handler import ExecutionHandler, TradeExecution


class TestableExecutionHandler(ExecutionHandler):  # type: ignore[misc]
    """ExecutionHandler with exposed internals for testing.
    
    This class exposes protected methods as public test methods to avoid
    direct access to protected members in tests, as recommended in the
    test analysis documents.
    
    Note: This is not a pytest test class despite the name prefix.
    """
    __test__ = False  # Tell pytest this is not a test class
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    async def test_place_order_with_retry(self, *args, **kwargs):
        """Test wrapper for _place_order_with_retry."""
        return await self._place_order_with_retry(*args, **kwargs)
    
    async def test_get_order_status(self, *args, **kwargs):
        """Test wrapper for _get_order_status."""
        return await self._get_order_status(*args, **kwargs)
    
    async def test_compensate_position(self, *args, **kwargs):
        """Test wrapper for _compensate_position."""
        return await self._compensate_position(*args, **kwargs)
    
    def test_add_to_history(self, execution: TradeExecution) -> None:
        """Test wrapper for _add_to_history."""
        return self._add_to_history(execution)
    
    @property
    def test_execution_history(self) -> list[TradeExecution]:
        """Test accessor for execution history."""
        return list(self.execution_history)  # Return a copy for safety