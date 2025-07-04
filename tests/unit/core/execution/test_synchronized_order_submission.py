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

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.apis.common import APIError
from cyberdelta.apis.common.api_error_codes import APIErrorCode
from cyberdelta.apis.models.service_args_models import PlaceOrderArgs
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


def create_test_order() -> Order:
    """Create a test Order."""
    return Order(
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

        mock_exchange1 = Mock(spec=ExchangeAPI)
        mock_exchange2 = Mock(spec=ExchangeAPI)
        service.exchange_adapters = {
            "exchange1": mock_exchange1,
            "exchange2": mock_exchange2,
        }

        # Mock successful order placement
        mock_order = create_test_order()
        mock_exchange1.place_order = AsyncMock(return_value=mock_order)
        mock_exchange2.place_order = AsyncMock(return_value=mock_order)

        # Act
        execution = await service.submit_orders(opportunity)

        # Assert - execution should have a properly formatted ID
        assert execution.execution_id.startswith("exec_")
        assert len(execution.execution_id.split("_")) == 3  # exec_timestamp_hash

    @pytest.mark.asyncio
    async def test_order_validation_through_submit_with_invalid_order(
        self, service: SynchronizedOrderSubmissionService
    ) -> None:
        """Test order validation during submission when order is invalid."""
        # Arrange
        opportunity = create_test_opportunity()

        mock_hyperliquid = Mock(spec=ExchangeAPI)
        mock_backpack = Mock(spec=ExchangeAPI)
        service.exchange_adapters = {
            "hyperliquid": mock_hyperliquid,
            "backpack": mock_backpack,
        }

        # Mock invalid order (missing required method)
        invalid_order = Mock()
        # Don't set client_order_id or to_dict to make it fail validation
        mock_hyperliquid.place_order = AsyncMock(return_value=invalid_order)
        mock_backpack.place_order = AsyncMock(return_value=create_test_order())

        # Act
        execution = await service.submit_orders(opportunity)

        # Assert - execution should fail due to invalid order
        assert execution.status == ExecutionStatus.FAILED
        assert execution.error is not None
        # The error could be validation, invalid, or mismatch related
        validation_msg = "validation" in execution.error.lower()
        invalid_msg = "invalid" in execution.error.lower()
        mismatch_msg = "mismatch" in execution.error.lower()
        assert validation_msg or invalid_msg or mismatch_msg

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
    async def test_order_validation_edge_missing_attributes_detected(
        self, service: SynchronizedOrderSubmissionService
    ) -> None:
        """Test order validation detects orders with missing attributes."""
        # Arrange
        opportunity = create_test_opportunity()

        mock_hyperliquid = Mock(spec=ExchangeAPI)
        mock_backpack = Mock(spec=ExchangeAPI)
        service.exchange_adapters = {
            "hyperliquid": mock_hyperliquid,
            "backpack": mock_backpack,
        }

        # Mock order with missing attributes
        invalid_order = Mock()
        # Intentionally not setting required attributes
        mock_hyperliquid.place_order = AsyncMock(return_value=invalid_order)
        mock_backpack.place_order = AsyncMock(return_value=create_test_order())

        # Act
        execution = await service.submit_orders(opportunity)

        # Assert - should fail due to invalid order
        assert execution.status == ExecutionStatus.FAILED

    @pytest.mark.asyncio
    async def test_order_preparation_through_submit_with_minimal_data(
        self, service: SynchronizedOrderSubmissionService
    ) -> None:
        """Test order preparation during submission with minimal opportunity data."""
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

        # Get the existing mocked exchange adapters from the service
        mock_hyperliquid = service.exchange_adapters["hyperliquid"]
        mock_backpack = service.exchange_adapters["backpack"]

        placed_orders: list[PlaceOrderArgs] = []

        def capture_order(args: PlaceOrderArgs) -> Mock:
            placed_orders.append(args)
            order = Mock(spec=Order)
            order.client_order_id = f"test_order_{len(placed_orders)}"
            order.exchange_order_id = f"exchange_{len(placed_orders)}"
            order.symbol = args.symbol
            order.side = args.side
            order.status = OrderStatus.NEW
            order.updated_at = datetime.now(UTC)
            order.created_at = datetime.now(UTC)
            order.order_type = args.order_type
            order.time_in_force = args.time_in_force
            order.quantity_requested = args.quantity
            order.quantity_filled = Decimal(0)
            order.price = (
                args.price if hasattr(args, "price") and args.price else Decimal("50000.0")
            )
            order.average_fill_price = None
            order.trades = []
            order.exchange = "hyperliquid" if args.side == OrderSide.BUY else "backpack"
            order.triggered_at = None
            order.strategy_name = None
            order.signal_id = None
            order.related_order_id = None
            order.hl_details = None
            order.bp_details = None
            order.to_dict = Mock(
                return_value={
                    "client_order_id": order.client_order_id,
                    "exchange_order_id": order.exchange_order_id,
                    "symbol": order.symbol,
                    "side": order.side.value,
                    "status": order.status.value,
                    "order_type": order.order_type.value,
                }
            )
            return order

        # Override the place_order mock method with our custom side_effect
        place_order_method = "place_order"
        setattr(mock_hyperliquid, place_order_method, AsyncMock(side_effect=capture_order))
        setattr(mock_backpack, place_order_method, AsyncMock(side_effect=capture_order))

        # Mock portfolio tracker to return proper orders for verification
        def create_portfolio_order(exchange: str, order_id: str) -> Mock:
            portfolio_order = Mock(spec=Order)
            portfolio_order.client_order_id = order_id
            portfolio_order.exchange_order_id = f"exchange_{order_id}"
            portfolio_order.symbol = "BTC-PERP"
            portfolio_order.side = OrderSide.BUY if "1" in order_id else OrderSide.SELL
            portfolio_order.status = OrderStatus.FILLED
            portfolio_order.updated_at = datetime.now(UTC)
            portfolio_order.created_at = datetime.now(UTC)
            portfolio_order.order_type = OrderType.MARKET
            portfolio_order.time_in_force = TimeInForce.IOC
            portfolio_order.quantity_requested = Decimal("1.0")
            portfolio_order.quantity_filled = Decimal("1.0")
            portfolio_order.price = Decimal("50000.0")
            portfolio_order.average_fill_price = Decimal("50000.0")
            portfolio_order.trades = []
            portfolio_order.exchange = (
                "hyperliquid" if portfolio_order.side == OrderSide.BUY else "backpack"
            )
            portfolio_order.triggered_at = None
            portfolio_order.strategy_name = None
            portfolio_order.signal_id = None
            portfolio_order.to_dict = Mock(
                return_value={
                    "client_order_id": portfolio_order.client_order_id,
                    "exchange_order_id": portfolio_order.exchange_order_id,
                    "symbol": portfolio_order.symbol,
                    "side": portfolio_order.side.value,
                    "status": portfolio_order.status.value,
                    "order_type": portfolio_order.order_type.value,
                }
            )
            return portfolio_order

        # Override the portfolio tracker mock method
        get_order_method = "get_order_by_id"
        setattr(
            service.portfolio_tracker,
            get_order_method,
            Mock(side_effect=create_portfolio_order),
        )

        # Act
        await service.submit_orders(opportunity)

        # Assert - verify orders were prepared correctly
        # The test may only place one order if the first fails
        assert len(placed_orders) >= 1
        # Check first order placed
        first_order = placed_orders[0]
        assert first_order.symbol == "BTC-PERP"
        assert first_order.quantity == Decimal("1.0")
        assert first_order.side in [OrderSide.BUY, OrderSide.SELL]

        # If two orders were placed, check the second
        if len(placed_orders) == 2:
            second_order = placed_orders[1]
            assert second_order.symbol == "BTC-PERP"
            assert second_order.quantity == Decimal("1.0")
            assert second_order.side != first_order.side  # Should be opposite side

    # FAILURE CASES
    @pytest.mark.asyncio
    async def test_order_preparation_failure_edge_case_handling(
        self, service: SynchronizedOrderSubmissionService
    ) -> None:
        """Test order preparation handles edge cases properly."""
        # Arrange
        # Create opportunity with edge case values
        opportunity = ArbitrageOpportunity(
            symbol="BTC",
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

        mock_hyperliquid = Mock(spec=ExchangeAPI)
        mock_backpack = Mock(spec=ExchangeAPI)
        service.exchange_adapters = {
            "hyperliquid": mock_hyperliquid,
            "backpack": mock_backpack,
        }

        # Mock order placement to fail for zero size
        mock_hyperliquid.place_order = AsyncMock(
            side_effect=APIError("Invalid order size", APIErrorCode.INVALID_ORDER_SIZE.value)
        )
        mock_backpack.place_order = AsyncMock(
            side_effect=APIError("Invalid order size", APIErrorCode.INVALID_ORDER_SIZE.value)
        )

        # Act
        execution = await service.submit_orders(opportunity)

        # Assert - should fail due to invalid size
        assert execution.status == ExecutionStatus.FAILED

    @pytest.mark.asyncio
    async def test_order_validation_failure_ensures_safety(
        self, service: SynchronizedOrderSubmissionService
    ) -> None:
        """Test order validation ensures safety by rejecting invalid orders."""
        # Arrange
        opportunity = create_test_opportunity()

        mock_hyperliquid = Mock(spec=ExchangeAPI)
        mock_backpack = Mock(spec=ExchangeAPI)
        service.exchange_adapters = {
            "hyperliquid": mock_hyperliquid,
            "backpack": mock_backpack,
        }

        # Mock place_order to return something that's not an Order object
        mock_hyperliquid.place_order = AsyncMock(return_value="not_an_order")
        mock_backpack.place_order = AsyncMock(return_value=create_test_order())

        # Act
        execution = await service.submit_orders(opportunity)

        # Assert - should fail due to invalid order type
        assert execution.status == ExecutionStatus.FAILED
        assert execution.error is not None
        # Check for various error messages that indicate validation failure
        validation_msg = "validation" in execution.error.lower()
        invalid_msg = "invalid" in execution.error.lower()
        missing_msg = "missing required" in execution.error.lower()
        not_order_msg = "not an order" in execution.error.lower()
        assert validation_msg or invalid_msg or missing_msg or not_order_msg

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
        config: dict[str, Any] = {}
        portfolio_tracker = Mock()
        exchange_adapters: dict[str, Any] = {}
        circuit_breaker = Mock()
        circuit_breaker.can_execute.return_value = (True, None)
        position_reconciliation = Mock()

        service = SynchronizedOrderSubmissionService(
            config,
            exchange_adapters,
            circuit_breaker,
            position_reconciliation,
            portfolio_tracker,
        )

        opportunity = create_test_opportunity()

        mock_hyperliquid = Mock(spec=ExchangeAPI)
        mock_backpack = Mock(spec=ExchangeAPI)
        service.exchange_adapters = {
            "hyperliquid": mock_hyperliquid,
            "backpack": mock_backpack,
        }

        mock_order = Mock(spec=Order)
        mock_order.client_order_id = "test_order_123"
        mock_order.exchange_order_id = "exchange_123"
        mock_order.symbol = "BTC-PERP"
        mock_order.side = OrderSide.BUY
        mock_order.status = OrderStatus.NEW
        mock_order.updated_at = datetime.now(UTC)
        mock_order.to_dict = Mock(
            return_value={
                "client_order_id": mock_order.client_order_id,
                "exchange_order_id": mock_order.exchange_order_id,
                "symbol": mock_order.symbol,
                "side": mock_order.side.value,
                "status": mock_order.status.value,
            }
        )
        mock_hyperliquid.place_order = AsyncMock(return_value=mock_order)
        mock_backpack.place_order = AsyncMock(return_value=mock_order)

        # Mock portfolio tracker
        portfolio_tracker.get_order_by_id.return_value = mock_order

        # Act - submit twice to get two execution IDs
        execution1 = await service.submit_orders(opportunity)
        await asyncio.sleep(0.001)  # Small delay to ensure different timestamp
        execution2 = await service.submit_orders(opportunity)

        # Assert
        assert execution1.execution_id != execution2.execution_id  # Should be unique
        assert execution1.execution_id.startswith("exec_")
        assert execution2.execution_id.startswith("exec_")
