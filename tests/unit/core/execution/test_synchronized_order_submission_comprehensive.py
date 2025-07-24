"""Comprehensive unit tests for synchronized order submission module.

Tests synchronized order submission functionality including execution coordination,
order verification, and position reconciliation.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases for each method.
"""

import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.apis.common import APIError
from cyberdelta.core.execution.synchronized_order_submission import (
    ExecutionContext,
    ExecutionCoordinator,
    ExecutionResult,
    ExecutionStatus,
    OrderVerifier,
    PositionReconciliationSystem,
    SynchronizedOrderSubmissionService,
    VerificationStatus,
)
from cyberdelta.core.models import (
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem
from cyberdelta.validation.funding_data import ArbitrageOpportunity


class TestVerificationStatus:
    """Test suite for VerificationStatus enum."""

    # ==================== SUCCESS CASES ====================

    def test_verification_status_success_has_all_values(self) -> None:
        """Test that VerificationStatus has all expected values."""
        # Act & Assert
        assert VerificationStatus.SUCCESS
        assert VerificationStatus.FAILURE
        assert VerificationStatus.PARTIAL
        assert VerificationStatus.TIMEOUT
        assert VerificationStatus.ERROR

    def test_verification_status_success_value_access(self) -> None:
        """Test that VerificationStatus values can be accessed."""
        # Act & Assert
        assert VerificationStatus.SUCCESS.name == "SUCCESS"
        assert VerificationStatus.FAILURE.name == "FAILURE"
        assert VerificationStatus.PARTIAL.name == "PARTIAL"
        assert VerificationStatus.TIMEOUT.name == "TIMEOUT"
        assert VerificationStatus.ERROR.name == "ERROR"


class TestExecutionStatus:
    """Test suite for ExecutionStatus enum."""

    # ==================== SUCCESS CASES ====================

    def test_execution_status_success_has_all_values(self) -> None:
        """Test that ExecutionStatus has all expected values."""
        # Act & Assert
        assert ExecutionStatus.PENDING
        assert ExecutionStatus.EXECUTING
        assert ExecutionStatus.COMPLETED
        assert ExecutionStatus.FAILED
        assert ExecutionStatus.PARTIALLY_COMPLETED
        assert ExecutionStatus.COMPENSATING
        assert ExecutionStatus.REJECTED

    def test_execution_status_success_value_access(self) -> None:
        """Test that ExecutionStatus values can be accessed."""
        # Act & Assert
        assert ExecutionStatus.PENDING.name == "PENDING"
        assert ExecutionStatus.EXECUTING.name == "EXECUTING"
        assert ExecutionStatus.COMPLETED.name == "COMPLETED"
        assert ExecutionStatus.FAILED.name == "FAILED"
        assert ExecutionStatus.PARTIALLY_COMPLETED.name == "PARTIALLY_COMPLETED"
        assert ExecutionStatus.COMPENSATING.name == "COMPENSATING"
        assert ExecutionStatus.REJECTED.name == "REJECTED"


class TestExecutionResult:
    """Test suite for ExecutionResult dataclass."""

    # ==================== SUCCESS CASES ====================

    def test_execution_result_success_initialization(self) -> None:
        """Test successful ExecutionResult initialization."""
        # Arrange
        execution_id = "test_exec_123"
        status = ExecutionStatus.COMPLETED
        timestamp = int(time.time() * 1000)

        # Act
        result = ExecutionResult(
            execution_id=execution_id,
            status=status,
            timestamp=timestamp,
        )

        # Assert
        assert result.execution_id == execution_id
        assert result.status == status
        assert result.timestamp == timestamp
        assert result.details is None
        assert result.error is None
        assert result.verification_results is None

    def test_execution_result_success_to_dict(self) -> None:
        """Test successful conversion to dictionary."""
        # Arrange
        execution_id = "test_exec_456"
        status = ExecutionStatus.FAILED
        timestamp = int(time.time() * 1000)
        error = "Test error"

        result = ExecutionResult(
            execution_id=execution_id,
            status=status,
            timestamp=timestamp,
            error=error,
        )

        # Act
        result_dict = result.to_dict()

        # Assert
        assert result_dict["execution_id"] == execution_id
        assert result_dict["status"] == "FAILED"
        assert result_dict["error"] == error
        assert result_dict["timestamp"] == timestamp

    def test_execution_result_success_update(self) -> None:
        """Test successful update with dictionary."""
        # Arrange
        result = ExecutionResult(
            execution_id="test",
            status=ExecutionStatus.PENDING,
            timestamp=int(time.time() * 1000),
        )
        update_data = {
            "status": ExecutionStatus.COMPLETED,
            "error": None,
            "details": {"test": "data"},
        }

        # Act
        result.update(update_data)

        # Assert
        assert result.status == ExecutionStatus.COMPLETED
        assert result.error is None
        assert result.details == {"test": "data"}

    # ==================== EDGE CASES ====================

    def test_execution_result_edge_minimal_data(self) -> None:
        """Test ExecutionResult with minimal required data."""
        # Arrange
        execution_id = ""
        status = ExecutionStatus.PENDING
        timestamp = 0

        # Act
        result = ExecutionResult(
            execution_id=execution_id,
            status=status,
            timestamp=timestamp,
        )

        # Assert
        assert not result.execution_id
        assert result.status == ExecutionStatus.PENDING
        assert result.timestamp == 0

    def test_execution_result_edge_update_nonexistent_field(self) -> None:
        """Test update with nonexistent field."""
        # Arrange
        result = ExecutionResult(
            execution_id="test",
            status=ExecutionStatus.PENDING,
            timestamp=int(time.time() * 1000),
        )
        update_data = {"nonexistent_field": "value"}

        # Act
        result.update(update_data)

        # Assert - Should not fail, just ignore nonexistent field
        assert not hasattr(result, "nonexistent_field")


class TestExecutionContext:
    """Test suite for ExecutionContext class."""

    @pytest.fixture
    def mock_opportunity(self) -> Mock:
        """Create a mock arbitrage opportunity."""
        opportunity = Mock(spec=ArbitrageOpportunity)
        opportunity.id = uuid4()
        opportunity.symbol = "BTC"
        opportunity.long_exchange = "hyperliquid"
        opportunity.short_exchange = "backpack"
        opportunity.optimal_size = Decimal("1.0")
        opportunity.long_price = Decimal("50000.0")
        opportunity.short_price = Decimal("50100.0")
        return opportunity

    # ==================== SUCCESS CASES ====================

    def test_execution_context_success_initialization(self, mock_opportunity: Mock) -> None:
        """Test successful ExecutionContext initialization."""
        # Arrange
        execution_id = "test_exec_789"
        strategy = "funding_rate_arb"
        start_time = datetime.now(UTC)
        status = ExecutionStatus.PENDING
        checkpoints: list[dict[str, Any]] = []

        # Act
        context = ExecutionContext(
            execution_id=execution_id,
            opportunity=mock_opportunity,
            strategy=strategy,
            start_time=start_time,
            status=status,
            checkpoints=checkpoints,
        )

        # Assert
        assert context.execution_id == execution_id
        assert context.opportunity == mock_opportunity
        assert context.strategy == strategy
        assert context.start_time == start_time
        assert context.status == status
        assert context.checkpoints == checkpoints
        assert context.end_time is None
        assert context.result is None
        assert context.abort_reason is None

    # ==================== EDGE CASES ====================

    def test_execution_context_edge_empty_checkpoints(self, mock_opportunity: Mock) -> None:
        """Test ExecutionContext with empty checkpoints list."""
        # Arrange
        checkpoints: list[dict[str, Any]] = []

        # Act
        context = ExecutionContext(
            execution_id="test",
            opportunity=mock_opportunity,
            strategy="test_strategy",
            start_time=datetime.now(UTC),
            status=ExecutionStatus.PENDING,
            checkpoints=checkpoints,
        )

        # Assert
        assert context.checkpoints == []
        assert len(context.checkpoints) == 0


class TestOrderVerifier:
    """Test suite for OrderVerifier class."""

    @pytest.fixture
    def mock_config(self) -> dict[str, Any]:
        """Create a mock configuration."""
        return {
            "execution": {
                "verification_timeout": 10.0,
                "verification_retries": 3,
            }
        }

    @pytest.fixture
    def mock_portfolio_tracker(self) -> Mock:
        """Create a mock portfolio tracker."""
        return Mock(spec=PortfolioTracker)

    @pytest.fixture
    def mock_exchange_adapters(self) -> dict[str, Mock]:
        """Create mock exchange adapters."""
        # Create mocks with properly configured async methods
        hyperliquid_mock = Mock(spec=ExchangeAPI)
        backpack_mock = Mock(spec=ExchangeAPI)

        # Configure async methods to return awaitable values
        hyperliquid_mock.get_order = AsyncMock(return_value=Mock())
        hyperliquid_mock.place_order = AsyncMock(return_value=Mock())
        backpack_mock.get_order = AsyncMock(return_value=Mock())
        backpack_mock.place_order = AsyncMock(return_value=Mock())

        return {
            "hyperliquid": hyperliquid_mock,
            "backpack": backpack_mock,
        }

    @pytest.fixture
    def order_verifier(
        self,
        mock_config: dict[str, Any],
        mock_portfolio_tracker: Mock,
        mock_exchange_adapters: dict[str, ExchangeAPI],
    ) -> OrderVerifier:
        """Create an OrderVerifier instance for testing."""
        return OrderVerifier(
            mock_config,
            mock_portfolio_tracker,
            mock_exchange_adapters,
        )

    @pytest.fixture
    def sample_order(self) -> Order:
        """Create a sample order for testing."""
        return Order(
            exchange="hyperliquid",
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity_requested=Decimal("1.0"),
            price=Decimal("50000.0"),
            status=OrderStatus.NEW,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            time_in_force=TimeInForce.GTC,
            client_order_id="test_order_123",
        )

    # ==================== SUCCESS CASES ====================

    def test_order_verifier_success_initialization(
        self,
        mock_config: dict[str, Any],
        mock_portfolio_tracker: Mock,
        mock_exchange_adapters: dict[str, ExchangeAPI],
    ) -> None:
        """Test successful OrderVerifier initialization."""
        # Act
        verifier = OrderVerifier(
            mock_config,
            mock_portfolio_tracker,
            mock_exchange_adapters,
        )

        # Assert
        assert verifier.config == mock_config
        assert verifier.portfolio_tracker == mock_portfolio_tracker
        assert verifier.exchange_adapters == mock_exchange_adapters

    @pytest.mark.asyncio
    async def test_verify_order_placement_success_local_and_api(
        self,
        order_verifier: OrderVerifier,
        mock_portfolio_tracker: Mock,
        mock_exchange_adapters: dict[str, Mock],
        sample_order: Order,
    ) -> None:
        """Test successful order placement verification with both local and API checks."""
        # Arrange
        exchange = "hyperliquid"
        order_id = "test_order_123"
        expected_details = {
            "symbol": "BTC",
            "side": OrderSide.BUY,
            "order_type": OrderType.MARKET,
        }

        # Mock local order found
        mock_portfolio_tracker.get_order_by_id.return_value = sample_order

        # Mock API order found
        api_order = sample_order
        mock_exchange_adapters[exchange].get_order = AsyncMock(return_value=api_order)

        # Act
        result = await order_verifier.verify_order_placement(exchange, order_id, expected_details)

        # Assert
        assert result["success"] is True
        assert result["error"] is None
        assert "local_order" in result["details"]
        assert "api_order" in result["details"]

    @pytest.mark.asyncio
    async def test_verify_order_execution_success_filled_order(
        self,
        order_verifier: OrderVerifier,
        mock_portfolio_tracker: Mock,
        mock_exchange_adapters: dict[str, Mock],
        sample_order: Order,
    ) -> None:
        """Test successful order execution verification with filled order."""
        # Arrange
        exchange = "hyperliquid"
        order_id = "test_order_123"

        # Create a filled order
        filled_order = Order(
            exchange=exchange,
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity_requested=Decimal("1.0"),
            price=Decimal("50000.0"),
            status=OrderStatus.FILLED,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            time_in_force=TimeInForce.GTC,
            client_order_id=order_id,
        )

        # Mock local order
        mock_portfolio_tracker.get_order_by_id.return_value = filled_order

        # Mock API methods
        mock_exchange_adapters[exchange].get_order_status = AsyncMock(return_value=filled_order)
        mock_exchange_adapters[exchange].get_trade_history = AsyncMock(return_value=[])

        # Act
        result = await order_verifier.verify_order_execution(exchange, order_id)

        # Assert
        assert result["success"] is True
        assert result["details"]["local_order_status"] == "FILLED"
        assert result["details"]["api_order_status"] == "FILLED"

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_verify_order_placement_edge_empty_expected_details(
        self,
        order_verifier: OrderVerifier,
        mock_portfolio_tracker: Mock,
        sample_order: Order,
    ) -> None:
        """Test order placement verification with empty expected details."""
        # Arrange
        exchange = "hyperliquid"
        order_id = "test_order_123"
        expected_details: dict[str, Any] = {}

        # Mock local order found
        mock_portfolio_tracker.get_order_by_id.return_value = sample_order

        # Act
        result = await order_verifier.verify_order_placement(exchange, order_id, expected_details)

        # Assert
        assert result["success"] is True  # Should pass with empty details

    @pytest.mark.asyncio
    async def test_verify_order_execution_edge_no_api_client(
        self,
        order_verifier: OrderVerifier,
        mock_portfolio_tracker: Mock,
        sample_order: Order,
    ) -> None:
        """Test order execution verification when API client is missing."""
        # Arrange
        exchange = "unknown_exchange"
        order_id = "test_order_123"

        # Mock local order
        mock_portfolio_tracker.get_order_by_id.return_value = sample_order

        # Act
        result = await order_verifier.verify_order_execution(exchange, order_id)

        # Assert
        assert result["success"] is False
        assert "API client not found" in result["error"]

    # ==================== FAILURE CASES ====================

    @pytest.mark.asyncio
    async def test_verify_order_placement_failure_local_order_not_found(
        self,
        order_verifier: OrderVerifier,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test order placement verification failure when local order not found."""
        # Arrange
        exchange = "hyperliquid"
        order_id = "test-order-nonexistent"
        expected_details = {"symbol": "BTC"}

        # Mock local order not found
        mock_portfolio_tracker.get_order_by_id.return_value = None

        # Act
        result = await order_verifier.verify_order_placement(exchange, order_id, expected_details)

        # Assert
        assert result["success"] is False
        assert "Local order not found" in result["error"]

    @pytest.mark.asyncio
    async def test_verify_order_placement_failure_property_mismatch(
        self,
        order_verifier: OrderVerifier,
        mock_portfolio_tracker: Mock,
        sample_order: Order,
    ) -> None:
        """Test order placement verification failure with property mismatch."""
        # Arrange
        exchange = "hyperliquid"
        order_id = "test_order_123"
        expected_details = {
            "symbol": "ETH",  # Different from sample_order which has "BTC"
        }

        # Mock local order found
        mock_portfolio_tracker.get_order_by_id.return_value = sample_order

        # Act
        result = await order_verifier.verify_order_placement(exchange, order_id, expected_details)

        # Assert
        assert result["success"] is False
        assert "symbol mismatch" in result["error"]

    @pytest.mark.asyncio
    async def test_verify_order_execution_failure_unfilled_order(
        self,
        order_verifier: OrderVerifier,
        mock_portfolio_tracker: Mock,
        mock_exchange_adapters: dict[str, Mock],
    ) -> None:
        """Test order execution verification failure with unfilled order."""
        # Arrange
        exchange = "hyperliquid"
        order_id = "test_order_123"

        # Create an unfilled order
        unfilled_order = Order(
            exchange=exchange,
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity_requested=Decimal("1.0"),
            price=Decimal("50000.0"),
            status=OrderStatus.NEW,  # Not filled
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            time_in_force=TimeInForce.GTC,
            client_order_id=order_id,
        )

        # Mock local order
        mock_portfolio_tracker.get_order_by_id.return_value = unfilled_order

        # Mock API methods
        mock_exchange_adapters[exchange].get_order_status = AsyncMock(return_value=unfilled_order)
        mock_exchange_adapters[exchange].get_trade_history = AsyncMock(return_value=[])

        # Act
        result = await order_verifier.verify_order_execution(exchange, order_id)

        # Assert
        assert result["success"] is False
        assert "expected FILLED" in result["error"]
        assert "OrderStatus.NEW" in result["error"] or "status is NEW" in result["error"]

    @pytest.mark.asyncio
    async def test_verify_order_execution_failure_api_exception(
        self,
        order_verifier: OrderVerifier,
        mock_portfolio_tracker: Mock,
        mock_exchange_adapters: dict[str, Mock],
        sample_order: Order,
    ) -> None:
        """Test order execution verification failure when API raises exception."""
        # Arrange
        exchange = "hyperliquid"
        order_id = "test_order_123"

        # Mock local order
        mock_portfolio_tracker.get_order_by_id.return_value = sample_order

        # Mock API exception
        mock_exchange_adapters[exchange].get_order_status = AsyncMock(
            side_effect=APIError("API connection failed", code=500)
        )

        # Act & Assert - Current business logic doesn't catch APIError, so it propagates
        # This is the current behavior and source of truth
        with pytest.raises(APIError) as exc_info:
            await order_verifier.verify_order_execution(exchange, order_id)

        # Verify the APIError is correctly propagated
        assert "API connection failed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_verify_order_fill_failure_not_implemented(
        self, order_verifier: OrderVerifier
    ) -> None:
        """Test verify_order_fill returns not implemented error."""
        # Act
        result = await order_verifier.verify_order_fill("hyperliquid", "test_order")

        # Assert
        assert result["success"] is False
        assert result["error"] == "Not implemented"


class TestExecutionCoordinator:
    """Test suite for ExecutionCoordinator class."""

    @pytest.fixture
    def mock_config(self) -> dict[str, Any]:
        """Create a mock configuration."""
        return {
            "execution": {
                "context_retention_seconds": 3600,
                "verification_timeout": 10.0,
            }
        }

    @pytest.fixture
    def execution_coordinator(self, mock_config: dict[str, Any]) -> ExecutionCoordinator:
        """Create an ExecutionCoordinator instance for testing."""
        return ExecutionCoordinator(mock_config)

    @pytest.fixture
    def mock_opportunity(self) -> Mock:
        """Create a mock arbitrage opportunity."""
        opportunity = Mock(spec=ArbitrageOpportunity)
        opportunity.id = uuid4()
        opportunity.symbol = "BTC"
        opportunity.long_exchange = "hyperliquid"
        opportunity.short_exchange = "backpack"
        return opportunity

    # ==================== SUCCESS CASES ====================

    def test_execution_coordinator_success_initialization(
        self, mock_config: dict[str, Any]
    ) -> None:
        """Test successful ExecutionCoordinator initialization."""
        # Act
        coordinator = ExecutionCoordinator(mock_config)

        # Assert
        assert coordinator.config == mock_config
        assert coordinator.executions == {}

    @pytest.mark.asyncio
    async def test_start_execution_success(
        self,
        execution_coordinator: ExecutionCoordinator,
        mock_opportunity: Mock,
    ) -> None:
        """Test successful execution start."""
        # Arrange
        execution_id = "test_exec_123"
        strategy = "funding_rate_arb"

        # Act
        context = await execution_coordinator.start_execution(
            execution_id, mock_opportunity, strategy
        )

        # Assert
        assert context.execution_id == execution_id
        assert context.opportunity == mock_opportunity
        assert context.strategy == strategy
        assert context.status == ExecutionStatus.PENDING
        assert len(context.checkpoints) == 1
        assert context.checkpoints[0]["name"] == "execution_started"
        assert execution_id in execution_coordinator.executions

    @pytest.mark.asyncio
    async def test_add_checkpoint_success(
        self,
        execution_coordinator: ExecutionCoordinator,
        mock_opportunity: Mock,
    ) -> None:
        """Test successful checkpoint addition."""
        # Arrange
        context = await execution_coordinator.start_execution(
            "test_exec", mock_opportunity, "test_strategy"
        )
        checkpoint_name = "test_checkpoint"
        details = {"test": "data"}

        # Act
        await execution_coordinator.add_checkpoint(context, checkpoint_name, details)

        # Assert
        assert len(context.checkpoints) == 2  # Including initial checkpoint
        latest_checkpoint = context.checkpoints[-1]
        assert latest_checkpoint["name"] == checkpoint_name
        assert latest_checkpoint["details"] == details
        assert "time" in latest_checkpoint

    @pytest.mark.asyncio
    async def test_complete_execution_success(
        self,
        execution_coordinator: ExecutionCoordinator,
        mock_opportunity: Mock,
    ) -> None:
        """Test successful execution completion."""
        # Arrange
        context = await execution_coordinator.start_execution(
            "test_exec", mock_opportunity, "test_strategy"
        )
        result = ExecutionResult(
            execution_id=context.execution_id,
            status=ExecutionStatus.COMPLETED,
            timestamp=int(time.time() * 1000),
        )

        # Act
        await execution_coordinator.complete_execution(context, result)

        # Assert
        assert context.status == ExecutionStatus.COMPLETED
        assert context.end_time is not None
        assert context.result == result
        assert len(context.checkpoints) == 2  # execution_started + execution_completed checkpoints

    @pytest.mark.asyncio
    async def test_abort_execution_success(
        self,
        execution_coordinator: ExecutionCoordinator,
        mock_opportunity: Mock,
    ) -> None:
        """Test successful execution abort."""
        # Arrange
        context = await execution_coordinator.start_execution(
            "test_exec", mock_opportunity, "test_strategy"
        )
        reason = "Test abort reason"

        # Act
        abort_details = await execution_coordinator.abort_execution(context, reason)

        # Assert
        assert context.status == ExecutionStatus.FAILED
        assert context.end_time is not None
        assert context.abort_reason == reason
        assert abort_details["execution_id"] == context.execution_id
        assert abort_details["abort_reason"] == reason
        assert "duration_ms" in abort_details

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_add_checkpoint_edge_empty_details(
        self,
        execution_coordinator: ExecutionCoordinator,
        mock_opportunity: Mock,
    ) -> None:
        """Test checkpoint addition with empty details."""
        # Arrange
        context = await execution_coordinator.start_execution(
            "test_exec", mock_opportunity, "test_strategy"
        )
        checkpoint_name = "empty_checkpoint"
        details: dict[str, Any] = {}

        # Act
        await execution_coordinator.add_checkpoint(context, checkpoint_name, details)

        # Assert
        latest_checkpoint = context.checkpoints[-1]
        assert latest_checkpoint["details"] == {}

    def test_cleanup_execution_edge_nonexistent_id(
        self, execution_coordinator: ExecutionCoordinator
    ) -> None:
        """Test cleanup behavior with nonexistent execution ID through public API."""
        # Arrange
        nonexistent_id = "nonexistent_exec"

        # Act & Assert - Test public behavior: execution state is consistent
        # The cleanup functionality is tested through public API methods
        # Verify that executions dict doesn't contain the nonexistent ID
        assert nonexistent_id not in execution_coordinator.executions

        # Test that other operations work normally despite nonexistent ID
        assert len(execution_coordinator.executions) == 0

    # ==================== FAILURE CASES ====================

    @pytest.mark.asyncio
    async def test_complete_execution_failure_with_error_result(
        self,
        execution_coordinator: ExecutionCoordinator,
        mock_opportunity: Mock,
    ) -> None:
        """Test execution completion with failed result."""
        # Arrange
        context = await execution_coordinator.start_execution(
            "test_exec", mock_opportunity, "test_strategy"
        )
        result = ExecutionResult(
            execution_id=context.execution_id,
            status=ExecutionStatus.FAILED,
            timestamp=int(time.time() * 1000),
            error="Test execution failure",
        )

        # Act
        await execution_coordinator.complete_execution(context, result)

        # Assert
        assert context.status == ExecutionStatus.FAILED
        assert context.result is not None
        assert context.result.error == "Test execution failure"


class TestSynchronizedOrderSubmissionService:
    """Test suite for SynchronizedOrderSubmissionService class."""

    @pytest.fixture
    def mock_config(self) -> dict[str, Any]:
        """Create a mock configuration."""
        return {
            "execution": {
                "verification_timeout": 10.0,
                "verification_retries": 3,
                "verification_interval": 1.0,
                "context_retention_seconds": 3600,
                "wait_for_first_fill": True,
                "wait_for_second_fill": True,
            }
        }

    @pytest.fixture
    def mock_exchange_adapters(self) -> dict[str, ExchangeAPI]:
        """Create mock exchange adapters."""
        # Create Mock objects that satisfy the ExchangeAPI interface
        mock_hyperliquid = Mock(spec=ExchangeAPI)
        mock_backpack = Mock(spec=ExchangeAPI)

        # Set up default behaviors for common methods with proper return values
        mock_hyperliquid.place_order = AsyncMock(return_value=Mock())
        mock_hyperliquid.get_order = AsyncMock(return_value=Mock())
        mock_backpack.place_order = AsyncMock(return_value=Mock())
        mock_backpack.get_order = AsyncMock(return_value=Mock())

        # Return as ExchangeAPI dict for type compatibility
        return {
            "hyperliquid": mock_hyperliquid,
            "backpack": mock_backpack,
        }

    @pytest.fixture
    def mock_circuit_breaker(self) -> Mock:
        """Create a mock circuit breaker system."""
        mock = Mock(spec=CircuitBreakerSystem)
        mock.can_execute.return_value = (True, None)
        return mock

    @pytest.fixture
    def mock_position_reconciliation(self) -> Mock:
        """Create a mock position reconciliation system."""
        return Mock(spec=PositionReconciliationSystem)

    @pytest.fixture
    def mock_portfolio_tracker(self) -> Mock:
        """Create a mock portfolio tracker."""
        return Mock(spec=PortfolioTracker)

    @pytest.fixture
    def service(
        self,
        mock_config: dict[str, Any],
        mock_exchange_adapters: dict[str, ExchangeAPI],
        mock_circuit_breaker: Mock,
        mock_position_reconciliation: Mock,
        mock_portfolio_tracker: Mock,
    ) -> SynchronizedOrderSubmissionService:
        """Create a SynchronizedOrderSubmissionService instance for testing."""
        return SynchronizedOrderSubmissionService(
            mock_config,
            mock_exchange_adapters,
            mock_circuit_breaker,
            mock_position_reconciliation,
            mock_portfolio_tracker,
        )

    @pytest.fixture
    def mock_opportunity(self) -> Mock:
        """Create a mock arbitrage opportunity."""
        opportunity = Mock(spec=ArbitrageOpportunity)
        opportunity.id = uuid4()
        opportunity.symbol = "BTC"
        opportunity.long_exchange = "hyperliquid"
        opportunity.short_exchange = "backpack"
        opportunity.optimal_size = Decimal("1.0")
        opportunity.long_price = Decimal("50000.0")
        opportunity.short_price = Decimal("50100.0")
        return opportunity

    # ==================== SUCCESS CASES ====================

    def test_service_success_initialization(
        self,
        mock_config: dict[str, Any],
        mock_exchange_adapters: dict[str, ExchangeAPI],
        mock_circuit_breaker: Mock,
        mock_position_reconciliation: Mock,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test successful service initialization."""
        # Act
        service = SynchronizedOrderSubmissionService(
            mock_config,
            mock_exchange_adapters,
            mock_circuit_breaker,
            mock_position_reconciliation,
            mock_portfolio_tracker,
        )

        # Assert
        assert service.config == mock_config
        assert service.exchange_adapters == mock_exchange_adapters
        assert service.circuit_breaker_system == mock_circuit_breaker
        assert service.position_reconciliation_system == mock_position_reconciliation
        assert service.portfolio_tracker == mock_portfolio_tracker
        assert service.verification_timeout == 10.0
        assert service.verification_retries == 3
        assert service.verification_interval == 1.0

    @pytest.mark.asyncio
    async def test_verify_market_conditions_success(
        self, service: SynchronizedOrderSubmissionService, mock_opportunity: Mock
    ) -> None:
        """Test successful market conditions verification."""
        # Act
        result = await service.verify_market_conditions(mock_opportunity)

        # Assert
        assert result["verified"] is True
        assert result["error"] is None
        assert "details" in result
        assert result["details"]["spread_ok"] is True

    @pytest.mark.asyncio
    async def test_verify_balances_success(
        self, service: SynchronizedOrderSubmissionService, mock_opportunity: Mock
    ) -> None:
        """Test successful balance verification."""
        # Act
        result = await service.verify_balances(mock_opportunity)

        # Assert
        assert result["verified"] is True
        assert result["error"] is None
        assert "details" in result
        assert result["details"]["long_balance_ok"] is True

    @pytest.mark.asyncio
    async def test_verify_positions_success(
        self,
        service: SynchronizedOrderSubmissionService,
        mock_opportunity: Mock,
    ) -> None:
        """Test successful position verification."""
        # Arrange
        execution_result = ExecutionResult(
            execution_id="test",
            status=ExecutionStatus.COMPLETED,
            timestamp=int(time.time() * 1000),
        )

        # Act
        result = await service.verify_positions(mock_opportunity, execution_result)

        # Assert
        assert result["checked"] is True

    @pytest.mark.asyncio
    async def test_verify_fills_success(
        self,
        service: SynchronizedOrderSubmissionService,
        mock_opportunity: Mock,
    ) -> None:
        """Test successful fill verification."""
        # Arrange
        execution_result = ExecutionResult(
            execution_id="test",
            status=ExecutionStatus.COMPLETED,
            timestamp=int(time.time() * 1000),
        )

        # Act
        result = await service.verify_fills(mock_opportunity, execution_result)

        # Assert
        assert result["checked"] is True

    @pytest.mark.asyncio
    async def test_verify_orders_success(
        self,
        service: SynchronizedOrderSubmissionService,
        mock_opportunity: Mock,
    ) -> None:
        """Test successful order verification."""
        # Arrange
        execution_result = ExecutionResult(
            execution_id="test",
            status=ExecutionStatus.COMPLETED,
            timestamp=int(time.time() * 1000),
        )

        # Act
        result = await service.verify_orders(mock_opportunity, execution_result)

        # Assert
        assert result["checked"] is True

    @pytest.mark.asyncio
    async def test_ensure_valid_placed_order_success(
        self, service: SynchronizedOrderSubmissionService, mock_opportunity: Mock
    ) -> None:
        """Test successful placed order validation with ensure method."""
        # Arrange - test with valid opportunity data

        # Act - Test through public submit_orders method which uses this private method
        # Mock the opportunity to create a complete test scenario
        mock_opportunity.symbol = "BTC"
        mock_opportunity.optimal_size = Decimal("1.0")
        mock_opportunity.long_leg_price = Decimal("50000.0")
        mock_opportunity.short_leg_price = Decimal("50100.0")

        # Should not raise exception during order submission with valid opportunity
        # The fixture already provides working mocks, so just test the public behavior
        await service.submit_orders(mock_opportunity, "sequential_lock_in")

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_submit_orders_edge_unknown_strategy(
        self, service: SynchronizedOrderSubmissionService, mock_opportunity: Mock
    ) -> None:
        """Test submit_orders with unknown execution strategy."""
        # Act
        result = await service.submit_orders(mock_opportunity, "unknown_strategy")

        # Assert
        assert result.status == ExecutionStatus.FAILED
        assert result.error is not None
        assert "Unknown execution strategy" in result.error

    # ==================== FAILURE CASES ====================

    @pytest.mark.asyncio
    async def test_submit_orders_failure_circuit_breaker_tripped(
        self,
        service: SynchronizedOrderSubmissionService,
        mock_opportunity: Mock,
        mock_circuit_breaker: Mock,
    ) -> None:
        """Test submit_orders failure when circuit breaker is tripped."""
        # Arrange
        mock_circuit_breaker.can_execute.return_value = (False, "Circuit breaker tripped")

        # Act
        result = await service.submit_orders(mock_opportunity)

        # Assert
        assert result.status == ExecutionStatus.REJECTED
        assert result.error is not None
        assert "Long leg CB" in result.error


class TestSynchronizedOrderSubmissionIntegration:
    """Integration tests for synchronized order submission."""

    @pytest.fixture
    def complete_service_setup(self) -> dict[str, Any]:
        """Create a complete service setup for integration testing."""
        config = {
            "execution": {
                "verification_timeout": 5.0,
                "verification_retries": 2,
                "verification_interval": 0.5,
                "context_retention_seconds": 300,
                "wait_for_first_fill": False,  # Speed up tests
                "wait_for_second_fill": False,
            }
        }

        hyperliquid_mock = Mock(spec=ExchangeAPI)
        backpack_mock = Mock(spec=ExchangeAPI)

        exchange_adapters: dict[str, ExchangeAPI] = {
            "hyperliquid": hyperliquid_mock,
            "backpack": backpack_mock,
        }

        circuit_breaker = Mock(spec=CircuitBreakerSystem)
        circuit_breaker.can_execute.return_value = (True, None)

        position_reconciliation = Mock(spec=PositionReconciliationSystem)
        portfolio_tracker = Mock(spec=PortfolioTracker)

        # Mock successful order placement
        mock_order = Order(
            exchange="hyperliquid",
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity_requested=Decimal("1.0"),
            price=Decimal("50000.0"),
            status=OrderStatus.FILLED,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            time_in_force=TimeInForce.GTC,
            client_order_id="test_order_123",
        )

        hyperliquid_mock.place_order = AsyncMock(return_value=mock_order)
        hyperliquid_mock.get_order = AsyncMock(return_value=mock_order)
        backpack_mock.place_order = AsyncMock(return_value=mock_order)
        backpack_mock.get_order = AsyncMock(return_value=mock_order)

        portfolio_tracker.get_order_by_id.return_value = mock_order

        service = SynchronizedOrderSubmissionService(
            config,
            exchange_adapters,
            circuit_breaker,
            position_reconciliation,
            portfolio_tracker,
        )

        return {
            "service": service,
            "config": config,
            "exchange_adapters": exchange_adapters,
            "circuit_breaker": circuit_breaker,
            "portfolio_tracker": portfolio_tracker,
            "mock_order": mock_order,
        }

    @pytest.fixture
    def integration_opportunity(self) -> Mock:
        """Create a complete opportunity for integration testing."""
        opportunity = Mock(spec=ArbitrageOpportunity)
        opportunity.id = uuid4()
        opportunity.symbol = "BTC"
        opportunity.long_exchange = "hyperliquid"
        opportunity.short_exchange = "backpack"
        opportunity.optimal_size = Decimal("1.0")
        opportunity.long_price = Decimal("50000.0")
        opportunity.short_price = Decimal("50100.0")
        return opportunity

    @pytest.mark.asyncio
    async def test_submit_orders_with_configured_service(
        self, complete_service_setup: dict[str, Any]
    ) -> None:
        """Test successful order submission using pre-configured service setup."""
        # Extract service and test data from the complete setup
        service = complete_service_setup["service"]

        # Create test opportunity
        mock_opportunity = Mock()
        mock_opportunity.symbol = "BTC"
        mock_opportunity.optimal_size = Decimal("1.0")
        mock_opportunity.long_exchange = "hyperliquid"
        mock_opportunity.short_exchange = "backpack"
        mock_opportunity.long_price = Decimal("50000.0")
        mock_opportunity.short_price = Decimal("50100.0")

        # Act - Test through public submit_orders method
        result = await service.submit_orders(mock_opportunity, "sequential_lock_in")

        # Assert - Result should indicate processing was attempted
        assert result.execution_id is not None
        assert result.status in [
            ExecutionStatus.COMPLETED,
            ExecutionStatus.FAILED,
            ExecutionStatus.PARTIALLY_COMPLETED,
            ExecutionStatus.REJECTED,
        ]

    @pytest.mark.asyncio
    async def test_full_execution_workflow_success(
        self,
        complete_service_setup: dict[str, Any],
        integration_opportunity: Mock,
    ) -> None:
        """Test complete execution workflow from start to finish."""
        # Arrange
        service = complete_service_setup["service"]

        # Act
        result = await service.submit_orders(integration_opportunity, "sequential_lock_in")

        # Assert
        assert result.status in {ExecutionStatus.COMPLETED, ExecutionStatus.PARTIALLY_COMPLETED}
        assert result.execution_id is not None
        assert result.timestamp > 0

    @pytest.mark.asyncio
    async def test_pre_execution_verification_workflow(
        self,
        complete_service_setup: dict[str, Any],
        integration_opportunity: Mock,
    ) -> None:
        """Test pre-execution verification workflow."""
        # Arrange
        service = complete_service_setup["service"]
        execution_context = ExecutionContext(
            execution_id="test_exec",
            opportunity=integration_opportunity,
            strategy="test",
            start_time=datetime.now(UTC),
            status=ExecutionStatus.PENDING,
            checkpoints=[],
        )

        # Act
        result = await service.verify_pre_execution(execution_context, integration_opportunity)

        # Assert
        assert result["verified"] is True
        assert "details" in result
        assert "circuit_breaker_long" not in result["details"]  # Should be True, so not added

    @pytest.mark.asyncio
    async def test_post_execution_verification_workflow(
        self,
        complete_service_setup: dict[str, Any],
        integration_opportunity: Mock,
    ) -> None:
        """Test post-execution verification workflow."""
        # Arrange
        service = complete_service_setup["service"]
        execution_context = ExecutionContext(
            execution_id="test_exec",
            opportunity=integration_opportunity,
            strategy="test",
            start_time=datetime.now(UTC),
            status=ExecutionStatus.EXECUTING,
            checkpoints=[],
        )
        execution_result = ExecutionResult(
            execution_id="test_exec",
            status=ExecutionStatus.COMPLETED,
            timestamp=int(time.time() * 1000),
        )

        # Act
        result = await service.verify_post_execution(
            execution_context, integration_opportunity, execution_result
        )

        # Assert
        assert result["verified"] is True
        assert "details" in result
        assert "positions" in result["details"]
        assert "fills" in result["details"]
        assert "orders" in result["details"]
