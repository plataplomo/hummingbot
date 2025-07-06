"""Unit tests for the SynchronizedOrderSubmission component.

Tests core functionality of synchronized order submission with simplified mocking.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases.
"""

import asyncio
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest

from cyberdelta.core.execution.synchronized_order_submission import (
    ExecutionResult,
    ExecutionStatus,
    OrderVerifier,
    SynchronizedOrderSubmissionService,
)
from cyberdelta.core.models import (
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from cyberdelta.validation.funding_data import ArbitrageOpportunity


pytestmark = pytest.mark.timing


def create_test_opportunity() -> ArbitrageOpportunity:
    """Create a test ArbitrageOpportunity."""
    return ArbitrageOpportunity(
        symbol="BTC-PERP",
        long_exchange="hyperliquid",
        short_exchange="backpack",
        long_price=Decimal("50000.0"),
        short_price=Decimal("50100.0"),
        optimal_size=Decimal("1.0"),
        long_funding_rate=Decimal("0.001"),
        short_funding_rate=Decimal("0.0005"),
        net_funding_differential=Decimal("0.0005"),
        timestamp=datetime.now(UTC),
    )


def create_test_order() -> Mock:
    """Create a test Order with to_dict method."""
    order = Order(
        client_order_id="test_order_123",
        exchange_order_id="exchange_123",
        symbol="BTC-PERP",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        time_in_force=TimeInForce.IOC,
        quantity_requested=Decimal("1.0"),
        price=Decimal("50000.0"),
        status=OrderStatus.NEW,
        exchange="test_exchange",
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
        quantity_filled=Decimal(0),
        average_fill_price=None,
        trades=[],
        triggered_at=None,
        strategy_name=None,
        signal_id=None,
    )

    # Create a mock that wraps the order and adds the to_dict method
    mock_order = Mock(wraps=order)
    mock_order.to_dict = order.model_dump

    # Copy specific model fields directly instead of using deprecated dir() approach
    model_fields = Order.model_fields.keys()
    for field_name in model_fields:
        if hasattr(order, field_name):
            setattr(mock_order, field_name, getattr(order, field_name))

    return mock_order


class TestExecutionResult:
    """Test suite for ExecutionResult class with success, edge, and failure cases."""

    # SUCCESS CASES
    def test_execution_result_success_creation(self) -> None:
        """Test successful creation of ExecutionResult."""
        # Arrange & Act
        result = ExecutionResult(
            execution_id="test_exec_123",
            status=ExecutionStatus.COMPLETED,
            timestamp=int(time.time() * 1000),
        )

        # Assert
        assert result.execution_id == "test_exec_123"
        assert result.status == ExecutionStatus.COMPLETED
        assert result.timestamp > 0
        assert result.details is None
        assert result.error is None

    def test_execution_result_success_to_dict(self) -> None:
        """Test successful conversion to dictionary."""
        # Arrange
        result = ExecutionResult(
            execution_id="test_exec_123",
            status=ExecutionStatus.COMPLETED,
            timestamp=1234567890,
            details={"test": "data"},
            error="test error",
        )

        # Act
        result_dict = result.to_dict()

        # Assert
        assert result_dict["execution_id"] == "test_exec_123"
        assert result_dict["status"] == "COMPLETED"
        assert result_dict["timestamp"] == 1234567890
        assert result_dict["details"] == {"test": "data"}
        assert result_dict["error"] == "test error"

    def test_execution_result_success_update(self) -> None:
        """Test successful update of ExecutionResult."""
        # Arrange
        result = ExecutionResult(
            execution_id="test_exec_123",
            status=ExecutionStatus.PENDING,
            timestamp=1234567890,
        )
        update_data = {
            "status": ExecutionStatus.COMPLETED,
            "error": "updated error",
            "details": {"updated": "data"},
        }

        # Act
        result.update(update_data)

        # Assert
        assert result.status == ExecutionStatus.COMPLETED
        assert result.error == "updated error"
        assert result.details == {"updated": "data"}

    # EDGE CASES
    def test_execution_result_edge_minimal_creation(self) -> None:
        """Test creation with minimal required fields."""
        # Arrange & Act
        result = ExecutionResult(
            execution_id="min_exec",
            status=ExecutionStatus.PENDING,
            timestamp=0,
        )

        # Assert
        assert result.execution_id == "min_exec"
        assert result.status == ExecutionStatus.PENDING
        assert result.timestamp == 0
        assert result.details is None

    def test_execution_result_edge_update_nonexistent_attribute(self) -> None:
        """Test update with non-existent attributes."""
        # Arrange
        result = ExecutionResult(
            execution_id="test_exec",
            status=ExecutionStatus.PENDING,
            timestamp=1234567890,
        )

        # Act
        result.update({"nonexistent_field": "value"})

        # Assert - should not crash, non-existent fields ignored
        assert not hasattr(result, "nonexistent_field")

    # FAILURE CASES
    def test_execution_result_failure_invalid_update_dict(self) -> None:
        """Test update with invalid dictionary keys."""
        # Arrange
        result = ExecutionResult(
            execution_id="test_exec",
            status=ExecutionStatus.PENDING,
            timestamp=1234567890,
        )

        # Act & Assert - should not crash with invalid keys
        result.update({"invalid_key": "value", "another_invalid": 123})
        assert result.execution_id == "test_exec"  # Original values preserved


class TestOrderVerifierSimple:
    """Simplified test suite for OrderVerifier functionality."""

    # SUCCESS CASES
    @pytest.mark.asyncio
    async def test_verify_order_placement_success(self) -> None:
        """Test successful order placement verification."""
        # Arrange
        config: dict[str, Any] = {}
        portfolio_tracker = Mock()

        # Create mock API client
        mock_api_client = AsyncMock()
        exchange_adapters: dict[str, Any] = {"test_exchange": mock_api_client}

        test_order = create_test_order()
        portfolio_tracker.get_order_by_id.return_value = test_order
        mock_api_client.get_order.return_value = test_order

        verifier = OrderVerifier(config, portfolio_tracker, exchange_adapters)

        expected_details = {
            "symbol": "BTC-PERP",
            "side": OrderSide.BUY,
            "order_type": OrderType.MARKET,
        }

        # Act
        result = await verifier.verify_order_placement(
            "test_exchange",
            "test_order_123",
            expected_details,
        )

        # Assert
        assert result["success"] is True
        assert result["error"] is None
        assert "details" in result

    @pytest.mark.asyncio
    async def test_verify_market_conditions_success(self) -> None:
        """Test successful market conditions verification."""
        # Arrange
        config: dict[str, Any] = {}
        portfolio_tracker = Mock()
        exchange_adapters: dict[str, Any] = {}
        circuit_breaker = Mock()
        position_reconciliation = Mock()

        circuit_breaker.can_execute.return_value = (True, None)

        service = SynchronizedOrderSubmissionService(
            config,
            exchange_adapters,
            circuit_breaker,
            position_reconciliation,
            portfolio_tracker,
        )

        opportunity = create_test_opportunity()

        # Act
        result = await service.verify_market_conditions(opportunity)

        # Assert
        assert result["verified"] is True
        assert result["error"] is None
        assert "details" in result

    # EDGE CASES
    @pytest.mark.asyncio
    async def test_verify_order_placement_edge_no_local_order(self) -> None:
        """Test order placement verification when local order not found."""
        # Arrange
        config: dict[str, Any] = {}
        portfolio_tracker = Mock()
        exchange_adapters: dict[str, Any] = {}

        portfolio_tracker.get_order_by_id.return_value = None

        verifier = OrderVerifier(config, portfolio_tracker, exchange_adapters)

        # Act
        result = await verifier.verify_order_placement(
            "test_exchange",
            "test-order-nonexistent",
            {"symbol": "BTC-PERP"},
        )

        # Assert
        assert result["success"] is False
        assert result["error"] == "Local order not found"

    @pytest.mark.asyncio
    async def test_verify_balances_edge_empty_opportunity(self) -> None:
        """Test balance verification with minimal opportunity data."""
        # Arrange
        config: dict[str, Any] = {}
        portfolio_tracker = Mock()
        exchange_adapters: dict[str, Any] = {}
        circuit_breaker = Mock()
        position_reconciliation = Mock()

        service = SynchronizedOrderSubmissionService(
            config,
            exchange_adapters,
            circuit_breaker,
            position_reconciliation,
            portfolio_tracker,
        )

        opportunity = create_test_opportunity()

        # Act
        result = await service.verify_balances(opportunity)

        # Assert
        assert result["verified"] is True  # Placeholder implementation returns True
        assert "details" in result

    # FAILURE CASES
    @pytest.mark.asyncio
    async def test_verify_order_placement_failure_order_mismatch(self) -> None:
        """Test order placement verification with property mismatch."""
        # Arrange
        config: dict[str, Any] = {}
        portfolio_tracker = Mock()
        exchange_adapters: dict[str, Any] = {}

        test_order = create_test_order()
        test_order.symbol = "ETH-PERP"  # Wrong symbol
        portfolio_tracker.get_order_by_id.return_value = test_order

        verifier = OrderVerifier(config, portfolio_tracker, exchange_adapters)

        expected_details = {"symbol": "BTC-PERP"}  # Expected different symbol

        # Act
        result = await verifier.verify_order_placement(
            "test_exchange",
            "test_order_123",
            expected_details,
        )

        # Assert
        assert result["success"] is False
        assert result["error"] is not None
        assert "mismatch" in result["error"]


class TestSynchronizedOrderSubmissionServiceSimple:
    """Simplified test suite for SynchronizedOrderSubmissionService."""

    @pytest.fixture
    def service(self) -> SynchronizedOrderSubmissionService:
        """Create SynchronizedOrderSubmissionService instance."""
        config: dict[str, Any] = {
            "execution.verification_timeout": 10.0,
            "execution.verification_retries": 3,
            "execution.verification_interval": 1.0,
            "execution.context_retention_seconds": 3600,
            "execution.wait_for_first_fill": True,
            "execution.wait_for_second_fill": True,
        }

        # Create mock exchange adapters (without spec to allow method assignment)
        mock_hyperliquid = AsyncMock()
        mock_backpack = AsyncMock()

        # Set default return values for place_order and get_order
        mock_hyperliquid.place_order.return_value = create_test_order()
        mock_backpack.place_order.return_value = create_test_order()
        mock_hyperliquid.get_order.return_value = create_test_order()
        mock_backpack.get_order.return_value = create_test_order()

        exchange_adapters: dict[str, Any] = {
            "hyperliquid": mock_hyperliquid,
            "backpack": mock_backpack,
        }

        circuit_breaker = Mock()
        circuit_breaker.can_execute.return_value = (True, None)
        position_reconciliation = Mock()
        portfolio_tracker = Mock()

        return SynchronizedOrderSubmissionService(
            config,
            exchange_adapters,
            circuit_breaker,
            position_reconciliation,
            portfolio_tracker,
        )

    # SUCCESS CASES
    @pytest.mark.asyncio
    async def test_verify_pre_execution_success(
        self, service: SynchronizedOrderSubmissionService
    ) -> None:
        """Test successful pre-execution verification."""
        # Arrange
        opportunity = create_test_opportunity()

        with patch.object(service.execution_coordinator, "start_execution") as mock_start:
            mock_context = Mock()
            mock_context.execution_id = "test_exec"
            mock_start.return_value = mock_context

            # Act
            result = await service.verify_pre_execution(mock_context, opportunity)

        # Assert
        assert result["verified"] is True
        assert result["error"] is None or not result["error"].strip()

    @pytest.mark.asyncio
    async def test_execution_id_generation_through_submit(
        self, service: SynchronizedOrderSubmissionService
    ) -> None:
        """Test execution ID is properly generated during order submission."""
        # Arrange
        opportunity = create_test_opportunity()

        # Act - The service fixture already has proper AsyncMock setup
        # Submit orders and check that execution ID is generated correctly
        execution = await service.submit_orders(opportunity)

        # Assert - execution should have a properly formatted ID
        assert execution.execution_id.startswith("exec_")
        assert len(execution.execution_id.split("_")) == 3  # exec_timestamp_hash

    @pytest.mark.asyncio
    async def test_submit_orders_with_unknown_strategy(
        self, service: SynchronizedOrderSubmissionService
    ) -> None:
        """Test submission behavior with an unknown execution strategy."""
        # Arrange
        opportunity = create_test_opportunity()

        # Act - Test with an unknown strategy name
        execution = await service.submit_orders(opportunity, "unknown_strategy")

        # Assert - execution should fail due to unknown strategy
        assert execution.status == ExecutionStatus.FAILED
        assert execution.error is not None
        assert "unknown" in execution.error.lower() or "strategy" in execution.error.lower()

    # EDGE CASES
    @pytest.mark.asyncio
    async def test_verify_pre_execution_edge_circuit_breaker_failed(
        self, service: SynchronizedOrderSubmissionService
    ) -> None:
        """Test pre-execution verification with circuit breaker failure."""
        # Arrange
        opportunity = create_test_opportunity()
        with patch.object(
            service.circuit_breaker_system,
            "can_execute",
            return_value=(False, "Circuit breaker tripped"),
        ):
            mock_context = Mock()
            mock_context.execution_id = "test_exec"

            # Act
            result = await service.verify_pre_execution(mock_context, opportunity)

            # Assert
            assert result["verified"] is False
            assert result["error"] is not None

    @pytest.mark.asyncio
    async def test_order_preparation_with_default_strategy(
        self, service: SynchronizedOrderSubmissionService
    ) -> None:
        """Test order preparation during submission with default strategy."""
        # Arrange
        opportunity = create_test_opportunity()

        # Act - Use default strategy (no strategy parameter)
        execution = await service.submit_orders(opportunity)

        # Assert - execution should complete successfully or fail gracefully
        assert execution.execution_id is not None
        assert execution.status in [
            ExecutionStatus.COMPLETED,
            ExecutionStatus.FAILED,
            ExecutionStatus.PARTIALLY_COMPLETED,
        ]

    @pytest.mark.asyncio
    async def test_minimal_opportunity_processing(
        self, service: SynchronizedOrderSubmissionService
    ) -> None:
        """Test order submission with minimal opportunity data."""
        # Arrange
        # Create minimal opportunity
        opportunity = ArbitrageOpportunity(
            symbol="BTC-PERP",
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_price=Decimal(50000),
            short_price=Decimal(50100),
            optimal_size=Decimal("1.0"),
            long_funding_rate=Decimal("0.001"),
            short_funding_rate=Decimal("-0.001"),
            net_funding_differential=Decimal("0.002"),
            timestamp=datetime.now(UTC),
            expected_profit=Decimal(100),
        )

        # Act - Test with minimal opportunity data
        execution = await service.submit_orders(opportunity)

        # Assert - execution should complete or fail gracefully
        assert execution.execution_id is not None
        assert execution.status in [
            ExecutionStatus.COMPLETED,
            ExecutionStatus.FAILED,
            ExecutionStatus.PARTIALLY_COMPLETED,
        ]

    # FAILURE CASES
    @pytest.mark.asyncio
    async def test_order_preparation_failure_edge_case_handling(
        self, service: SynchronizedOrderSubmissionService
    ) -> None:
        """Test order preparation handles edge cases properly."""
        # Arrange
        # Create opportunity with edge case values - very small size
        opportunity = ArbitrageOpportunity(
            symbol="BTC",
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_price=Decimal(50000),
            short_price=Decimal(50100),
            optimal_size=Decimal("0.00000001"),  # Edge case: extremely small size
            long_funding_rate=Decimal("0.001"),
            short_funding_rate=Decimal("-0.001"),
            net_funding_differential=Decimal("0.002"),
            timestamp=datetime.now(UTC),
            expected_profit=Decimal(100),
        )

        # Act
        execution = await service.submit_orders(opportunity)

        # Assert - should fail due to invalid size
        assert execution.status == ExecutionStatus.FAILED

    @pytest.mark.asyncio
    async def test_order_preparation_failure_zero_optimal_size(
        self, service: SynchronizedOrderSubmissionService
    ) -> None:
        """Test order preparation rejects opportunities without optimal_size."""
        # Arrange
        # Create opportunity without optimal_size set (None)
        opportunity = ArbitrageOpportunity(
            symbol="BTC",
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_price=Decimal(50000),
            short_price=Decimal(50100),
            optimal_size=None,  # Edge case: None size (not set)
            long_funding_rate=Decimal("0.001"),
            short_funding_rate=Decimal("-0.001"),
            net_funding_differential=Decimal("0.002"),
            timestamp=datetime.now(UTC),
            expected_profit=Decimal(100),
        )

        # Act
        execution = await service.submit_orders(opportunity)

        # Assert - should reject or handle None size explicitly
        assert execution.status in [ExecutionStatus.FAILED, ExecutionStatus.REJECTED]
        if execution.error:
            # Should contain some indication of missing or invalid size/quantity
            keywords = ["size", "none", "invalid", "missing", "required", "quantity"]
            assert any(keyword in execution.error.lower() for keyword in keywords)

    @pytest.mark.asyncio
    async def test_order_validation_failure_ensures_safety(
        self, service: SynchronizedOrderSubmissionService
    ) -> None:
        """Test order validation ensures safety by rejecting invalid orders."""
        # Arrange
        opportunity = create_test_opportunity()

        # Test edge case - invalid symbol format
        opportunity.symbol = ""  # Edge case: empty symbol

        # Act
        execution = await service.submit_orders(opportunity)

        # Assert - should fail due to invalid order type
        assert execution.status == ExecutionStatus.FAILED
        assert execution.error is not None
        # Check for various error messages that indicate validation failure
        # The error message should contain information about the failure
        error_lower = execution.error.lower()
        validation_msg = "validation" in error_lower
        invalid_msg = "invalid" in error_lower
        missing_msg = "missing required" in error_lower
        not_order_msg = "not an order" in error_lower
        empty_string_msg = "emptystringerror" in error_lower or "empty" in error_lower
        symbol_msg = "symbol" in error_lower
        assert (
            validation_msg
            or invalid_msg
            or missing_msg
            or not_order_msg
            or empty_string_msg
            or symbol_msg
        )

    @pytest.mark.asyncio
    async def test_verify_positions_placeholder(
        self, service: SynchronizedOrderSubmissionService
    ) -> None:
        """Test positions verification placeholder implementation."""
        # Arrange
        opportunity = create_test_opportunity()
        result = ExecutionResult(
            execution_id="test_exec",
            status=ExecutionStatus.COMPLETED,
            timestamp=int(time.time() * 1000),
        )

        # Act
        verification_result = await service.verify_positions(opportunity, result)

        # Assert
        assert "checked" in verification_result


# Additional focused tests for critical paths
class TestExecutionWorkflows:
    """Test critical execution workflows."""

    @pytest.mark.asyncio
    async def test_full_verification_workflow(self) -> None:
        """Test complete verification workflow."""
        # Arrange
        config: dict[str, Any] = {}
        portfolio_tracker = Mock()
        exchange_adapters: dict[str, Any] = {}
        circuit_breaker = Mock()
        position_reconciliation = Mock()

        circuit_breaker.can_execute.return_value = (True, None)

        service = SynchronizedOrderSubmissionService(
            config,
            exchange_adapters,
            circuit_breaker,
            position_reconciliation,
            portfolio_tracker,
        )

        opportunity = create_test_opportunity()

        # Act
        market_result = await service.verify_market_conditions(opportunity)
        balance_result = await service.verify_balances(opportunity)

        # Assert
        assert market_result["verified"] is True
        assert balance_result["verified"] is True

    @pytest.mark.asyncio
    async def test_execution_id_uniqueness_through_multiple_submissions(self) -> None:
        """Test execution ID generation produces unique IDs for each submission."""
        # Arrange
        config: dict[str, Any] = {
            "execution.verification_timeout": 10.0,
            "execution.verification_retries": 3,
            "execution.verification_interval": 1.0,
            "execution.context_retention_seconds": 3600,
            "execution.wait_for_first_fill": True,
            "execution.wait_for_second_fill": True,
        }

        # Create mock exchange adapters
        mock_hyperliquid = AsyncMock()
        mock_backpack = AsyncMock()

        # Set default return values for place_order and get_order
        mock_hyperliquid.place_order.return_value = create_test_order()
        mock_backpack.place_order.return_value = create_test_order()
        mock_hyperliquid.get_order.return_value = create_test_order()
        mock_backpack.get_order.return_value = create_test_order()

        exchange_adapters: dict[str, Any] = {
            "hyperliquid": mock_hyperliquid,
            "backpack": mock_backpack,
        }

        circuit_breaker = Mock()
        circuit_breaker.can_execute.return_value = (True, None)
        position_reconciliation = Mock()
        portfolio_tracker = Mock()

        # Mock portfolio tracker methods to prevent AttributeError
        portfolio_tracker.start_order_tracking = Mock()
        portfolio_tracker.add_order = Mock()
        portfolio_tracker.get_order_by_id = Mock(return_value=create_test_order())

        service = SynchronizedOrderSubmissionService(
            config,
            exchange_adapters,
            circuit_breaker,
            position_reconciliation,
            portfolio_tracker,
        )

        opportunity = create_test_opportunity()

        # Act - submit twice to get two execution IDs
        execution1 = await service.submit_orders(opportunity)
        await asyncio.sleep(0.001)  # Small delay to ensure different timestamp
        execution2 = await service.submit_orders(opportunity)

        # Assert
        assert execution1.execution_id != execution2.execution_id  # Should be unique
        assert execution1.execution_id.startswith("exec_")
        assert execution2.execution_id.startswith("exec_")
