"""
Tests for the synchronize order submission module.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, NamedTuple
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.core.execution.synchronized_order_submission import (
    ExecutionCoordinator,
    ExecutionResult,
    ExecutionStatus,
    OrderVerifier,
    SynchronizedOrderSubmissionService,
)
from cyberdelta.core.models import Order, OrderSide, OrderStatus, OrderType, TimeInForce
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem
from cyberdelta.validation.funding_data import ArbitrageOpportunity

# Configure logger
logger = logging.getLogger(__name__)


class MockOpportunity(NamedTuple):
    """Mock arbitrage opportunity for testing."""

    symbol: str = "BTC-PERP"
    long_exchange: str = "hyperliquid"
    short_exchange: str = "backpack"
    long_price: Decimal = Decimal("50000.0")
    short_price: Decimal = Decimal("50001.0")
    quantity: Decimal = Decimal("1.0")
    long_size: Decimal = Decimal("1000.0")
    short_size: Decimal = Decimal("1000.0")
    expected_profit: Decimal = Decimal("1.0")

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "long_exchange": self.long_exchange,
            "short_exchange": self.short_exchange,
            "long_price": str(self.long_price),
            "short_price": str(self.short_price),
            "quantity": str(self.quantity),
            "long_size": str(self.long_size),
            "short_size": str(self.short_size),
            "expected_profit": str(self.expected_profit),
        }

    def __str__(self) -> str:
        return f"MockOpportunity({self.symbol}, {self.long_exchange}, {self.short_exchange})"

    def __hash__(self) -> int:
        return hash((self.symbol, self.long_exchange, self.short_exchange))


class TestOrderVerifier:
    """Test suite for the OrderVerifier class."""

    @pytest.fixture
    def portfolio_tracker(self) -> MagicMock:
        """Create a mock portfolio tracker."""
        mock_tracker = MagicMock()
        # Define a sample filled order for mocking
        sample_filled_order = Order(
            client_order_id="test-order-1",
            exchange="mock_exchange",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.GTC,
            price=Decimal("50000.0"),
            quantity_requested=Decimal("1.0"),
            status=OrderStatus.FILLED,
            quantity_filled=Decimal("1.0"),
            average_fill_price=Decimal("50000.0"),
            created_at=datetime.now(UTC),
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )
        mock_tracker.get_order.return_value = sample_filled_order

        mock_api_order = Order(
            client_order_id="exchange-order-id-1",
            exchange="mock_exchange",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.GTC,
            quantity_requested=Decimal("1.0"),
            price=Decimal("50000"),
            status=OrderStatus.FILLED,
            quantity_filled=Decimal("1.0"),
            average_fill_price=Decimal("50000"),
            created_at=datetime.now(UTC),
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )

        # Mock API client for exchange
        mock_api_client = AsyncMock(
            spec=ExchangeAPI,
            get_order=AsyncMock(return_value=mock_api_order),  # Added and configured
            get_order_status=AsyncMock(return_value=mock_api_order),
            get_recent_fills=AsyncMock(
                return_value=[
                    MagicMock(
                        order_id="exchange-order-id-1",
                        symbol="BTC-PERP",
                        side=OrderSide.BUY,
                        price=Decimal("50000.0"),
                        quantity=Decimal("1.0"),
                        timestamp=int(datetime.now(UTC).timestamp() * 1000),
                    )
                ]
            ),
        )

        mock_tracker.get_api_client.return_value = mock_api_client

        return mock_tracker

    @pytest.mark.asyncio
    async def test_verify_order_placement(self, portfolio_tracker: MagicMock) -> None:
        """Test verifying order placement via OrderVerifier."""
        config: dict[str, Any] = {}
        verifier = OrderVerifier(config, portfolio_tracker)

        # Test successful verification
        expected_details: dict[str, Any] = {
            "symbol": "BTC-PERP",
            "side": OrderSide.BUY,
            "order_type": OrderType.LIMIT,
        }

        # Ensure the mock returns the Order object defined in fixture
        mock_local_order = Order(
            client_order_id="test-order-1",
            exchange="mock_exchange",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.GTC,
            status=OrderStatus.OPEN,
            price=Decimal("50000.0"),
            quantity_requested=Decimal("1.0"),
            created_at=datetime.now(UTC),
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )
        # Directly populate the internal dict instead of mocking get_order
        exchange_id = "hyperliquid"
        order_id = "test-order-1"
        if exchange_id not in portfolio_tracker._orders:
            portfolio_tracker._orders[exchange_id] = {}
        portfolio_tracker._orders[exchange_id][order_id] = mock_local_order
        # portfolio_tracker.get_order.return_value = mock_local_order # Remove ineffective mock

        result_dict = await verifier.verify_order_placement(exchange_id, order_id, expected_details)

        assert result_dict["success"] is True

        # Test failed verification with missing order
        portfolio_tracker.get_order.return_value = None

        result_fail = await verifier.verify_order_placement(
            "hyperliquid", "missing-order", expected_details
        )

        assert result_fail.get("success") is False
        error_msg = result_fail.get("error")
        assert error_msg is not None and "not found in local state" in error_msg
        details = result_fail.get("details")
        assert details is not None and details.get("local_order") is None

    @pytest.mark.asyncio
    async def test_verify_order_execution(self, portfolio_tracker: MagicMock) -> None:
        """Test verifying order execution via OrderVerifier."""
        config: dict[str, Any] = {}
        verifier = OrderVerifier(config, portfolio_tracker)

        # Test successful verification
        # Corrected based on mypy error: Add 'type', use 'id', remove timestamp/order_type kwarg
        local_order_mock = Order(
            client_order_id="exchange-order-id-1",
            exchange="mock_exchange",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.GTC,
            status=OrderStatus.FILLED,
            price=Decimal("50000.0"),
            quantity_requested=Decimal("1.0"),
            quantity_filled=Decimal("1.0"),
            average_fill_price=Decimal("50000.0"),  # Added for validation
            created_at=datetime.now(UTC),
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )
        portfolio_tracker.get_order.return_value = local_order_mock

        # Mock the API call within verify_order_execution
        mock_api = portfolio_tracker.get_api_client()
        api_order_response = Order(
            client_order_id="exchange-order-id-1",
            exchange="mock_exchange",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.GTC,
            quantity_requested=Decimal("1.0"),
            price=Decimal("50000"),
            status=OrderStatus.FILLED,
            quantity_filled=Decimal("1.0"),
            average_fill_price=Decimal("50000"),  # Added for validation
            created_at=datetime.now(UTC),
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )
        # Mypy fix: avg_fill_price is not a direct attribute
        # api_order_response.avg_fill_price=Decimal("50000")

        # Ensure get_order_status method is mocked correctly on the mock_api instance
        # Fix [method-assign]: Assign to method attribute
        mock_api.get_order_status = AsyncMock(return_value=api_order_response)
        # >>> ADD MOCK FOR get_order <<<
        mock_api.get_order = AsyncMock(
            return_value=api_order_response
        )  # Assuming get_order returns similar obj
        # >>> ADD MOCK FOR get_recent_fills <<<
        mock_api.get_recent_fills = AsyncMock(
            return_value=[]
        )  # Mock as async returning empty list for this test

        result_dict = await verifier.verify_order_execution(
            "hyperliquid", "exchange-order-id-1"
        )  # Use correct ID

        assert result_dict.get("success") is True
        assert result_dict.get("error") is None
        # Mypy fix: Check for None before using `in` [operator] error 227
        if result_dict and "details" in result_dict and result_dict["details"]:
            assert "api_order" in result_dict["details"]

        # Test failed verification (e.g., order not filled on exchange)
        api_order_response_open = Order(
            client_order_id="exchange-order-id-1",
            exchange="mock_exchange",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.GTC,
            quantity_requested=Decimal("1.0"),
            price=Decimal("50000"),
            status=OrderStatus.OPEN,
            # quantity_filled=Decimal("0"), # Not needed, defaults to 0 for OPEN
            created_at=datetime.now(UTC),
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )
        mock_api.get_order_status.return_value = api_order_response_open  # Update return value
        result_fail = await verifier.verify_order_execution("hyperliquid", "exchange-order-id-1")
        assert result_fail.get("success") is False
        assert result_fail.get("error") is not None
        assert "Order status mismatch" in result_fail.get("error", "")


class TestExecutionCoordinator:
    """Test suite for the ExecutionCoordinator class."""

    @pytest.fixture
    def coordinator(self) -> ExecutionCoordinator:
        """Create an execution coordinator."""
        config: dict[str, Any] = {
            "execution.context_retention_seconds": 1  # Short retention for testing
        }
        return ExecutionCoordinator(config)

    @pytest.mark.asyncio
    async def test_start_execution(self, coordinator: ExecutionCoordinator) -> None:
        """Test starting an execution context."""
        # Revert to using ArbitrageOpportunity as expected by the coordinator
        mock_opportunity = ArbitrageOpportunity(
            symbol="BTC-PERP",
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_price=Decimal("50000.0"),
            short_price=Decimal("50001.0"),
            long_funding_rate=Decimal("0.0001"),
            short_funding_rate=Decimal("-0.0001"),
            net_funding_differential=Decimal("0.0002"),
            timestamp=datetime.now(UTC),
            expected_profit=Decimal("1.0"),
            utility_score=0.8,
            basis_volatility=0.001,
        )
        strategy = "sequential_lock_in"

        result = await coordinator.start_execution("test-execution-1", mock_opportunity, strategy)

        # Restore original assertions for context verification
        assert "test-execution-1" in coordinator.executions
        context = coordinator.executions["test-execution-1"]
        assert context.execution_id == "test-execution-1"
        # Compare relevant fields, model_dump might be needed if comparing full objects
        assert context.opportunity["symbol"] == mock_opportunity.symbol
        assert context.strategy == strategy
        assert context.status == ExecutionStatus.PENDING
        assert len(context.checkpoints) == 1
        assert context.checkpoints[0]["name"] == "execution_started"

    @pytest.mark.asyncio
    async def test_add_checkpoint(self, coordinator: ExecutionCoordinator) -> None:
        """Test adding a checkpoint."""
        mock_opportunity = ArbitrageOpportunity(
            symbol="BTC-PERP",
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_price=Decimal("50000.0"),
            short_price=Decimal("50001.0"),
            long_funding_rate=Decimal("0.0001"),
            short_funding_rate=Decimal("-0.0001"),
            net_funding_differential=Decimal("0.0002"),
            timestamp=datetime.now(UTC),
            expected_profit=Decimal("1.0"),
            utility_score=0.8,
            basis_volatility=0.001,
        )
        strategy = "sequential_lock_in"

        await coordinator.start_execution("test-execution-2", mock_opportunity, strategy)
        context = coordinator.executions["test-execution-2"]

        # Add a checkpoint
        await coordinator.add_checkpoint(context, "test-checkpoint", {"test": "data"})

        assert len(context.checkpoints) == 2
        assert context.checkpoints[1]["name"] == "test-checkpoint"
        assert context.checkpoints[1]["details"]["test"] == "data"

    @pytest.mark.asyncio
    async def test_complete_execution(self, coordinator: ExecutionCoordinator) -> None:
        """Test completing an execution."""
        mock_opportunity = ArbitrageOpportunity(
            symbol="BTC-PERP",
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_price=Decimal("50000.0"),
            short_price=Decimal("50001.0"),
            long_funding_rate=Decimal("0.0001"),
            short_funding_rate=Decimal("-0.0001"),
            net_funding_differential=Decimal("0.0002"),
            timestamp=datetime.now(UTC),
            expected_profit=Decimal("1.0"),
            utility_score=0.8,
            basis_volatility=0.001,
        )
        strategy = "sequential_lock_in"

        await coordinator.start_execution("test-execution-3", mock_opportunity, strategy)
        context = coordinator.executions["test-execution-3"]

        # Complete the execution
        result = ExecutionResult(
            execution_id="test-execution-3",
            status=ExecutionStatus.COMPLETED,  # Mypy fix: Add mandatory status
            timestamp=int(datetime.now(UTC).timestamp() * 1000),  # Keep timestamp logic
        )

        await coordinator.complete_execution(context, result)

        assert context.status == ExecutionStatus.COMPLETED
        assert context.result == result
        assert context.end_time is not None
        assert len(context.checkpoints) == 2
        assert context.checkpoints[1]["name"] == "execution_completed"

    @pytest.mark.asyncio
    async def test_abort_execution(self, coordinator: ExecutionCoordinator) -> None:
        """Test aborting an execution."""
        mock_opportunity = ArbitrageOpportunity(
            symbol="BTC-PERP",
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_price=Decimal("50000.0"),
            short_price=Decimal("50001.0"),
            long_funding_rate=Decimal("0.0001"),
            short_funding_rate=Decimal("-0.0001"),
            net_funding_differential=Decimal("0.0002"),
            timestamp=datetime.now(UTC),
            expected_profit=Decimal("1.0"),
            utility_score=0.8,
            basis_volatility=0.001,
        )
        strategy = "sequential_lock_in"

        await coordinator.start_execution("test-execution-4", mock_opportunity, strategy)
        context = coordinator.executions["test-execution-4"]

        # Abort the execution
        abort_details_dict = await coordinator.abort_execution(context, "Test abort reason")

        assert context.status == ExecutionStatus.FAILED
        assert context.abort_reason == "Test abort reason"
        assert context.end_time is not None
        assert len(context.checkpoints) == 2
        assert context.checkpoints[1]["name"] == "execution_aborted"

        # Check dict returned by abort_execution
        assert abort_details_dict["execution_id"] == "test-execution-4"
        assert abort_details_dict["abort_reason"] == "Test abort reason"
        assert abort_details_dict["checkpoint_count"] == 2


class TestSynchronizedOrderSubmissionService:
    """Test suite for the SynchronizedOrderSubmissionService class."""

    @pytest.fixture
    def service(
        self,
    ) -> tuple[SynchronizedOrderSubmissionService, dict[str, Any], MagicMock, MagicMock]:
        """Create a service instance with mocked dependencies using dependency injection."""
        config: dict[str, Any] = {
            "execution.order_placement_type": "sequential",
            "execution.verification_timeout_seconds": 0.1,
            "execution.max_slippage_bps": 10,
            "execution.context_retention_seconds": 60,
        }

        # 1. Instantiate Mocks for Dependencies
        mock_hl_api = AsyncMock(spec=ExchangeAPI)
        mock_bp_api = AsyncMock(spec=ExchangeAPI)
        mock_exchange_adapters: dict[str, ExchangeAPI] = {
            "hyperliquid": mock_hl_api,
            "backpack": mock_bp_api,
        }
        # Use spec for better validation of mocked methods/attributes
        mock_cb_system = MagicMock(spec=CircuitBreakerSystem)
        # Assuming async behavior for recon system, adjust spec if available
        mock_pos_recon_system = AsyncMock()
        mock_portfolio_tracker = MagicMock(spec=PortfolioTracker)

        # Mocks for components potentially used internally or returned for test assertions
        # These are not directly injected via __init__ based on previous fixture structure,
        # but tests might expect them. Tests using @patch will override these anyway.
        mock_order_verifier = MagicMock(spec=OrderVerifier)
        mock_coordinator = MagicMock(spec=ExecutionCoordinator)

        # 2. Configure Default Mock Methods (Optional - keep minimal in fixture)
        # Example: Default CB allows execution
        mock_cb_system.can_execute.return_value = (True, None)
        # Other specific configurations are likely handled by @patch in individual tests

        # 3. Instantiate Service with Mocked Dependencies
        service_instance = SynchronizedOrderSubmissionService(
            config=config,
            exchange_adapters=mock_exchange_adapters,
            circuit_breaker_system=mock_cb_system,
            position_reconciliation_system=mock_pos_recon_system,
            portfolio_tracker=mock_portfolio_tracker,
            # Assuming OrderVerifier and ExecutionCoordinator are instantiated internally
            # or patched by tests, not injected via __init__ directly.
        )

        # --- REMOVED direct mocking of internal service methods --- #
        # The following lines caused [method-assign] errors and are replaced
        # by dependency injection and test-specific @patch decorators.
        # service._verify_pre_execution = AsyncMock(...) # REMOVED
        # service._verify_market_conditions = AsyncMock(...) # REMOVED
        # service._verify_balances = AsyncMock(...) # REMOVED
        # service._verify_post_execution = AsyncMock(...) # REMOVED
        # service._compensate_verification_failure = AsyncMock(...) # REMOVED
        # service._execute_sequential_with_verification = AsyncMock(...) # REMOVED
        # service._execute_simultaneous_with_verification = AsyncMock(...) # REMOVED

        # 4. Return service instance and necessary mocks
        # Returning mocks needed for assertions or further configuration in tests.
        # Keep returning mock_order_verifier and mock_coordinator for signature compatibility,
        # although tests using @patch might not use these specific instances.
        return service_instance, config, mock_order_verifier, mock_coordinator

    @pytest.mark.asyncio
    @patch.object(
        SynchronizedOrderSubmissionService, "_verify_pre_execution", new_callable=AsyncMock
    )
    @patch.object(
        SynchronizedOrderSubmissionService,
        "_execute_sequential_with_verification",
        new_callable=AsyncMock,
    )
    @patch.object(
        SynchronizedOrderSubmissionService, "_verify_post_execution", new_callable=AsyncMock
    )
    async def test_submit_orders_sequential(
        self,
        mock_verify_post: AsyncMock,
        mock_execute_seq: AsyncMock,
        mock_verify_pre: AsyncMock,
        service: tuple[SynchronizedOrderSubmissionService, dict[str, Any], MagicMock, MagicMock],
    ) -> None:
        """Test submitting orders with sequential strategy."""
        service_instance, _, _, _ = service
        mock_opportunity = ArbitrageOpportunity(
            symbol="BTC-PERP",
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_price=Decimal("50000.0"),
            short_price=Decimal("50001.0"),
            long_funding_rate=Decimal("0.0001"),
            short_funding_rate=Decimal("-0.0001"),
            net_funding_differential=Decimal("0.0002"),
            timestamp=datetime.now(UTC),
            expected_profit=Decimal("1.0"),
            utility_score=0.8,
            basis_volatility=0.001,
        )

        # Configure patched mocks (they replace the instance methods)
        mock_verify_pre.return_value = {"success": True, "error": None, "details": {}}
        mock_execute_seq.return_value = ExecutionResult(
            execution_id="test-seq-exec",
            status=ExecutionStatus.COMPLETED,
            timestamp=int(time.time() * 1000),
        )
        mock_verify_post.return_value = {"success": True, "error": None, "details": {}}

        # Call the SUT
        result_obj: ExecutionResult = await service_instance.submit_orders(
            mock_opportunity, "sequential_lock_in"
        )

        # Assert based on the expected dictionary structure returned by submit_orders
        assert (
            result_obj.status == ExecutionStatus.COMPLETED
        )  # submit_orders returns ExecutionResult
        # Check patched mocks were called
        assert isinstance(mock_verify_pre, AsyncMock)
        mock_verify_pre.assert_awaited_once()
        assert isinstance(mock_execute_seq, AsyncMock)
        mock_execute_seq.assert_awaited_once()
        assert isinstance(mock_verify_post, AsyncMock)
        mock_verify_post.assert_awaited_once()

    @pytest.mark.asyncio
    @patch.object(
        SynchronizedOrderSubmissionService, "_verify_pre_execution", new_callable=AsyncMock
    )
    @patch.object(
        SynchronizedOrderSubmissionService,
        "_execute_simultaneous_with_verification",
        new_callable=AsyncMock,
    )
    @patch.object(
        SynchronizedOrderSubmissionService, "_verify_post_execution", new_callable=AsyncMock
    )
    async def test_submit_orders_simultaneous(
        self,
        mock_verify_post: AsyncMock,
        mock_execute_sim: AsyncMock,
        mock_verify_pre: AsyncMock,
        service: tuple[SynchronizedOrderSubmissionService, dict[str, Any], MagicMock, MagicMock],
    ) -> None:
        """Test submitting orders with simultaneous strategy."""
        service_instance, _, _, _ = service
        mock_opportunity = ArbitrageOpportunity(
            symbol="BTC-PERP",
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_price=Decimal("50000.0"),
            short_price=Decimal("50001.0"),
            long_funding_rate=Decimal("0.0001"),
            short_funding_rate=Decimal("-0.0001"),
            net_funding_differential=Decimal("0.0002"),
            timestamp=datetime.now(UTC),
            expected_profit=Decimal("1.0"),
            utility_score=0.8,
            basis_volatility=0.001,
        )

        # Configure patched mocks
        mock_verify_pre.return_value = {"success": True, "error": None, "details": {}}
        mock_execute_sim.return_value = {  # Returns dict
            "success": True,
            "error": None,
            "timestamp": int(time.time() * 1000),
            "details": {},
        }
        mock_verify_post.return_value = {"success": True, "error": None, "details": {}}

        # Call the SUT
        result_obj: ExecutionResult = await service_instance.submit_orders(
            mock_opportunity, "simultaneous"
        )

        # Assert based on the expected dictionary structure
        assert (
            result_obj.status == ExecutionStatus.COMPLETED
        )  # submit_orders returns ExecutionResult
        # Check patched mocks were called
        assert isinstance(mock_verify_pre, AsyncMock)
        mock_verify_pre.assert_awaited_once()
        assert isinstance(mock_execute_sim, AsyncMock)
        mock_execute_sim.assert_awaited_once()
        assert isinstance(mock_verify_post, AsyncMock)
        mock_verify_post.assert_awaited_once()

    @pytest.mark.asyncio
    @patch.object(
        SynchronizedOrderSubmissionService, "_verify_pre_execution", new_callable=AsyncMock
    )
    @patch.object(
        SynchronizedOrderSubmissionService,
        "_execute_sequential_with_verification",
        new_callable=AsyncMock,
    )
    @patch.object(
        SynchronizedOrderSubmissionService, "_verify_post_execution", new_callable=AsyncMock
    )
    @patch.object(
        ExecutionCoordinator, "start_execution", new_callable=AsyncMock
    )  # Mock coordinator interactions
    @patch.object(ExecutionCoordinator, "add_checkpoint", new_callable=AsyncMock)
    @patch.object(ExecutionCoordinator, "complete_execution", new_callable=AsyncMock)
    async def test_submit_orders_pre_execution_failure(
        self,
        mock_complete_execution: AsyncMock,
        mock_add_checkpoint: AsyncMock,
        mock_start_execution: AsyncMock,
        mock_verify_post: AsyncMock,
        mock_execute_seq: AsyncMock,
        mock_verify_pre: AsyncMock,
        service: tuple[SynchronizedOrderSubmissionService, dict[str, Any], MagicMock, MagicMock],
    ) -> None:
        """Test failure during pre-execution checks."""
        service_instance, _, _, _ = service
        # Use patched coordinator mocks from parameters, not fixture
        service_instance.execution_coordinator = MagicMock(spec=ExecutionCoordinator)
        service_instance.execution_coordinator.start_execution = mock_start_execution
        service_instance.execution_coordinator.add_checkpoint = mock_add_checkpoint
        service_instance.execution_coordinator.complete_execution = mock_complete_execution

        mock_opportunity = ArbitrageOpportunity(
            symbol="BTC-PERP",
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_price=Decimal("50000.0"),
            short_price=Decimal("50001.0"),
            long_funding_rate=Decimal("0.0001"),
            short_funding_rate=Decimal("-0.0001"),
            net_funding_differential=Decimal("0.0002"),
            timestamp=datetime.now(UTC),
            expected_profit=Decimal("1.0"),
            utility_score=0.8,
            basis_volatility=0.001,
        )
        strategy = "sequential_lock_in"

        # Configure the mock for _verify_pre_execution to fail
        mock_verify_pre.return_value = {
            "success": False,
            "error": "Pre-execution check failed (mock)",
            "details": {},
        }

        # Mock coordinator methods as submit_orders calls them
        mock_context = MagicMock()
        mock_start_execution.return_value = mock_context

        # Mypy fix [no-untyped-def] error 471: Add type annotation
        async def mock_complete(ctx: object, res: ExecutionResult) -> None:
            assert isinstance(res, ExecutionResult)
            assert res.status == ExecutionStatus.REJECTED

        # Use the actual mock instance from the test parameters
        mock_complete_execution.side_effect = mock_complete

        # Submit orders
        result_obj: ExecutionResult = await service_instance.submit_orders(
            mock_opportunity, strategy
        )

        # Verify status is REJECTED and error is propagated correctly in ExecutionResult
        assert result_obj.status == ExecutionStatus.REJECTED
        assert (
            result_obj.error == "Pre-execution check failed (mock)"
        )  # Error should be set on ExecutionResult

        # Verify the pre-execution check was called, but not execution or post-check
        mock_verify_pre.assert_awaited_once()
        mock_execute_seq.assert_not_awaited()
        mock_verify_post.assert_not_awaited()
        # Verify coordinator interactions
        mock_start_execution.assert_awaited_once()
        # Checkpoints called for start and pre-execution checks
        assert mock_add_checkpoint.call_count >= 2
        mock_complete_execution.assert_awaited_once()

    @pytest.mark.asyncio
    @patch.object(
        SynchronizedOrderSubmissionService, "_verify_pre_execution", new_callable=AsyncMock
    )
    @patch.object(
        SynchronizedOrderSubmissionService,
        "_execute_sequential_with_verification",
        new_callable=AsyncMock,
    )
    @patch.object(
        SynchronizedOrderSubmissionService, "_verify_post_execution", new_callable=AsyncMock
    )
    @patch.object(
        SynchronizedOrderSubmissionService,
        "_compensate_verification_failure",
        new_callable=AsyncMock,
    )  # Mock compensation
    @patch.object(
        ExecutionCoordinator, "start_execution", new_callable=AsyncMock
    )  # Mock coordinator interactions
    @patch.object(ExecutionCoordinator, "add_checkpoint", new_callable=AsyncMock)
    @patch.object(ExecutionCoordinator, "complete_execution", new_callable=AsyncMock)
    async def test_submit_orders_post_execution_failure(
        self,
        mock_complete_execution: AsyncMock,
        mock_add_checkpoint: AsyncMock,
        mock_start_execution: AsyncMock,
        mock_compensate: AsyncMock,  # Add mock compensation
        mock_verify_post: AsyncMock,
        mock_execute_seq: AsyncMock,
        mock_verify_pre: AsyncMock,
        service: tuple[SynchronizedOrderSubmissionService, dict[str, Any], MagicMock, MagicMock],
    ) -> None:
        """Test failure during post-execution verification."""
        service_instance, _, _, _ = service
        # Use patched coordinator mocks from parameters
        service_instance.execution_coordinator = MagicMock(spec=ExecutionCoordinator)
        service_instance.execution_coordinator.start_execution = mock_start_execution
        service_instance.execution_coordinator.add_checkpoint = mock_add_checkpoint
        service_instance.execution_coordinator.complete_execution = mock_complete_execution

        mock_opportunity = ArbitrageOpportunity(
            symbol="BTC-PERP",
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_price=Decimal("50000.0"),
            short_price=Decimal("50001.0"),
            long_funding_rate=Decimal("0.0001"),
            short_funding_rate=Decimal("-0.0001"),
            net_funding_differential=Decimal("0.0002"),
            timestamp=datetime.now(UTC),
            expected_profit=Decimal("1.0"),
            utility_score=0.8,
            basis_volatility=0.001,
        )
        strategy = "sequential_lock_in"

        # Mock pre-execution to pass
        mock_verify_pre.return_value = {"success": True, "error": None, "details": {}}

        # Mock coordinator methods
        mock_context = MagicMock()
        mock_start_execution.return_value = mock_context

        # Mock _execute_sequential_* (patched mock) to return a successful ExecutionResult
        mock_exec_result = ExecutionResult(
            execution_id="test-exec-post-fail",
            status=ExecutionStatus.COMPLETED,  # Assume execution itself *was* ok initially
            timestamp=int(datetime.now(UTC).timestamp() * 1000),
        )
        mock_execute_seq.return_value = mock_exec_result

        # Mock post-execution verification to fail
        mock_verify_post.return_value = {
            "success": False,
            "error": "Post-execution check failed (mock)",
            "details": {},
        }

        # Mock compensation result
        mock_compensate.return_value = {"compensated": True, "details": "Mock compensation details"}

        # Submit orders
        result_obj: ExecutionResult = await service_instance.submit_orders(
            mock_opportunity, strategy
        )

        # Verify status is PARTIALLY_COMPLETED after compensation attempt
        assert (
            result_obj.status == ExecutionStatus.PARTIALLY_COMPLETED
        )  # Status after compensation attempt
        assert (
            result_obj.error is None
        )  # Error from verification failure is handled, not set on final result
        assert result_obj.compensation_result is not None  # Check compensation result exists
        assert result_obj.compensation_result["compensated"] is True

        # Check internal methods were called
        mock_verify_pre.assert_awaited_once()
        mock_execute_seq.assert_awaited_once()
        mock_verify_post.assert_awaited_once()
        mock_compensate.assert_awaited_once()  # Ensure compensation was called
        # Verify coordinator interactions
        mock_start_execution.assert_awaited_once()
        # Checkpoints for start, pre-exec, market, balance, execution, post-exec
        assert mock_add_checkpoint.call_count >= 6
        mock_complete_execution.assert_awaited_once()

    @pytest.mark.asyncio
    @patch.object(
        SynchronizedOrderSubmissionService, "_verify_market_conditions", new_callable=AsyncMock
    )
    @patch.object(SynchronizedOrderSubmissionService, "_verify_balances", new_callable=AsyncMock)
    @patch(
        "cyberdelta.core.execution.synchronized_order_submission.ExecutionCoordinator",
        autospec=True,
    )  # Use autospec for coordinator mock
    async def test_internal_verify_pre_execution(
        self,
        MockExecutionCoordinator: MagicMock,  # Patched class
        mock_verify_balances: AsyncMock,
        mock_verify_market_conditions: AsyncMock,
        service: tuple[SynchronizedOrderSubmissionService, dict[str, Any], MagicMock, MagicMock],
    ) -> None:
        """Test the internal _verify_pre_execution method logic."""
        service_instance, _, _, _ = service  # Use service_instance
        mock_opportunity = ArbitrageOpportunity(
            symbol="BTC-PERP",
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_price=Decimal("50000.0"),
            short_price=Decimal("50001.0"),
            long_funding_rate=Decimal("0.0001"),
            short_funding_rate=Decimal("-0.0001"),
            net_funding_differential=Decimal("0.0002"),
            timestamp=datetime.now(UTC),
            expected_profit=Decimal("1.0"),
            utility_score=0.8,
            basis_volatility=0.001,
        )
        # execution_id = service_instance._generate_execution_id(...) # ID generated internally

        # Instantiate the coordinator mock for interaction (though the class is patched)
        mock_coordinator_instance = MockExecutionCoordinator.return_value
        service_instance.execution_coordinator = mock_coordinator_instance  # Assign instance

        # --- Test successful verification ---
        # Mypy fix [attr-defined] error 597 etc: Mock the can_execute method on the system mock
        mock_circuit_breaker_system = MagicMock(spec=CircuitBreakerSystem)
        mock_circuit_breaker_system.can_execute = AsyncMock(return_value=(True, None))
        service_instance.circuit_breaker_system = mock_circuit_breaker_system

        mock_market_result_dict: dict[str, Any] = {
            "success": True,
            "error": None,
            "details": {"checked": True},
        }
        mock_verify_market_conditions.return_value = mock_market_result_dict
        mock_balance_result_dict: dict[str, Any] = {
            "success": True,
            "error": None,
            "details": {"checked": True},
        }
        mock_verify_balances.return_value = mock_balance_result_dict

        # Call the actual method on the service instance
        result_dict = await service_instance._verify_pre_execution(mock_opportunity)

        assert result_dict.get("success") is True
        # Check mocked methods on dependencies were called
        assert mock_circuit_breaker_system.can_execute.call_count == 2
        mock_verify_market_conditions.assert_awaited_once()
        mock_verify_balances.assert_awaited_once()
        # Check coordinator interactions (add_checkpoint is called internally)
        assert mock_coordinator_instance.add_checkpoint.call_count > 0

        # --- Test circuit breaker failure ---
        mock_circuit_breaker_system.can_execute.reset_mock()
        mock_verify_market_conditions.reset_mock()
        mock_verify_balances.reset_mock()
        mock_coordinator_instance.add_checkpoint.reset_mock()
        # Configure CB mock to fail
        mock_circuit_breaker_system.can_execute.return_value = (False, "CB Tripped")

        result_cb_fail = await service_instance._verify_pre_execution(mock_opportunity)
        assert result_cb_fail.get("success") is False
        assert "CB Tripped" in result_cb_fail.get("error", "")
        # Check mocked methods
        assert mock_circuit_breaker_system.can_execute.call_count == 1  # Stops after first failure
        mock_verify_market_conditions.assert_not_awaited()
        mock_verify_balances.assert_not_awaited()
        assert mock_coordinator_instance.add_checkpoint.call_count > 0  # Checkpoint for CB failure

        # --- Test market conditions failure ---
        mock_circuit_breaker_system.can_execute.reset_mock()
        mock_verify_market_conditions.reset_mock()
        mock_verify_balances.reset_mock()
        mock_coordinator_instance.add_checkpoint.reset_mock()
        # Configure CB mock to pass, market mock to fail
        mock_circuit_breaker_system.can_execute.return_value = (True, None)
        mock_market_fail_dict: dict[str, Any] = {
            "success": False,
            "error": "Bad Market",
            "details": {},
        }
        mock_verify_market_conditions.return_value = mock_market_fail_dict
        mock_verify_balances.return_value = mock_balance_result_dict  # Ensure balance mock passes

        result_market_fail = await service_instance._verify_pre_execution(mock_opportunity)
        assert result_market_fail.get("success") is False
        assert "Bad Market" in result_market_fail.get("error", "")
        # Check mocked methods
        assert mock_circuit_breaker_system.can_execute.call_count == 2  # Both CBs checked
        mock_verify_market_conditions.assert_awaited_once()
        mock_verify_balances.assert_not_awaited()
        assert mock_coordinator_instance.add_checkpoint.call_count > 0

        # --- Test balance verification failure ---
        mock_circuit_breaker_system.can_execute.reset_mock()
        mock_verify_market_conditions.reset_mock()
        mock_verify_balances.reset_mock()
        mock_coordinator_instance.add_checkpoint.reset_mock()
        # Configure CB and market to pass, balance mock to fail
        mock_circuit_breaker_system.can_execute.return_value = (True, None)
        mock_verify_market_conditions.return_value = mock_market_result_dict  # Market passes
        mock_balance_fail_dict: dict[str, Any] = {
            "success": False,
            "error": "Low Balance",
            "details": {},
        }
        mock_verify_balances.return_value = mock_balance_fail_dict

        result_balance_fail = await service_instance._verify_pre_execution(mock_opportunity)
        assert result_balance_fail.get("success") is False
        assert "Low Balance" in result_balance_fail.get("error", "")
        # Check mocked methods
        assert mock_circuit_breaker_system.can_execute.call_count == 2  # CBs checked
        mock_verify_market_conditions.assert_awaited_once()
        mock_verify_balances.assert_awaited_once()
        assert mock_coordinator_instance.add_checkpoint.call_count > 0

    @pytest.mark.asyncio
    @patch.object(SynchronizedOrderSubmissionService, "_verify_positions", new_callable=AsyncMock)
    @patch.object(SynchronizedOrderSubmissionService, "_verify_fills", new_callable=AsyncMock)
    @patch.object(SynchronizedOrderSubmissionService, "_verify_orders", new_callable=AsyncMock)
    @patch(
        "cyberdelta.core.execution.synchronized_order_submission.ExecutionCoordinator",
        autospec=True,
    )  # Use autospec
    async def test_verify_post_execution(
        self,
        MockExecutionCoordinator: MagicMock,  # Patched class
        mock_verify_orders: AsyncMock,
        mock_verify_fills: AsyncMock,
        mock_verify_positions: AsyncMock,
        service: tuple[SynchronizedOrderSubmissionService, dict[str, Any], MagicMock, MagicMock],
    ) -> None:
        """Test the internal _verify_post_execution method logic."""
        service_instance, _, _, _ = service
        # Mock context is passed in, but we need coordinator instance for checkpoint calls
        mock_coordinator_instance = MockExecutionCoordinator.return_value
        service_instance.execution_coordinator = mock_coordinator_instance  # Assign instance

        # mock_context = MagicMock() # Mock context passed - Not needed as it's not used
        mock_opportunity = ArbitrageOpportunity(
            symbol="BTC-PERP",
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_price=Decimal("50000.0"),
            short_price=Decimal("50001.0"),
            long_funding_rate=Decimal("0.0001"),
            short_funding_rate=Decimal("-0.0001"),
            net_funding_differential=Decimal("0.0002"),
            timestamp=datetime.now(UTC),
            expected_profit=Decimal("1.0"),
            utility_score=0.8,
            basis_volatility=0.001,
        )
        mock_execution_result = ExecutionResult(
            execution_id="test-execution",
            status=ExecutionStatus.COMPLETED,
            timestamp=int(datetime.now(UTC).timestamp() * 1000),
        )

        # --- Test successful verification ---
        mock_pos_result: dict[str, Any] = {"success": True, "error": None, "details": {}}
        mock_verify_positions.return_value = mock_pos_result
        mock_fill_result: dict[str, Any] = {"success": True, "error": None, "details": {}}
        mock_verify_fills.return_value = mock_fill_result
        mock_order_result: dict[str, Any] = {"success": True, "error": None, "details": {}}
        mock_verify_orders.return_value = mock_order_result

        final_result_dict = await service_instance._verify_post_execution(
            mock_opportunity,
            mock_execution_result,
        )

        assert final_result_dict.get("success") is True
        mock_verify_positions.assert_awaited_once()
        mock_verify_fills.assert_awaited_once()
        mock_verify_orders.assert_awaited_once()
        # Checkpoints are internal to _verify_post_execution helper methods, not called directly
        assert (
            mock_coordinator_instance.add_checkpoint.call_count > 0
        )  # Checkpoints are called internally

        # --- Test position verification failure ---
        mock_verify_positions.reset_mock()
        mock_verify_fills.reset_mock()
        mock_verify_orders.reset_mock()
        mock_coordinator_instance.add_checkpoint.reset_mock()
        mock_pos_fail_result: dict[str, Any] = {
            "success": False,
            "error": "Pos Fail",
            "details": {"pos_fail": True},
        }
        mock_verify_positions.return_value = mock_pos_fail_result
        mock_verify_fills.return_value = mock_fill_result  # Ensure others pass
        mock_verify_orders.return_value = mock_order_result  # Ensure others pass

        final_result_pos_fail = await service_instance._verify_post_execution(
            mock_opportunity,
            mock_execution_result,
        )

        assert final_result_pos_fail.get("success") is False
        assert "Pos Fail" in final_result_pos_fail.get("error", "")
        mock_verify_positions.assert_awaited_once()
        # Logic might still call other checks even if one fails, depending on implementation
        mock_verify_fills.assert_awaited_once()
        mock_verify_orders.assert_awaited_once()
        assert (
            mock_coordinator_instance.add_checkpoint.call_count > 0
        )  # Checkpoints called internally

        # --- Test fill verification failure ---
        mock_verify_positions.reset_mock()
        mock_verify_fills.reset_mock()
        mock_verify_orders.reset_mock()
        mock_coordinator_instance.add_checkpoint.reset_mock()
        mock_verify_positions.return_value = mock_pos_result  # Ensure pos passes
        mock_fill_fail_result: dict[str, Any] = {
            "success": False,
            "error": "Fill Fail",
            "details": {"fill_fail": True},
        }
        mock_verify_fills.return_value = mock_fill_fail_result
        mock_verify_orders.return_value = mock_order_result  # Ensure orders passes

        final_result_fill_fail = await service_instance._verify_post_execution(
            mock_opportunity,
            mock_execution_result,
        )

        assert final_result_fill_fail.get("success") is False
        assert "Fill Fail" in final_result_fill_fail.get("error", "")
        mock_verify_positions.assert_awaited_once()
        mock_verify_fills.assert_awaited_once()
        mock_verify_orders.assert_awaited_once()  # Called even if fills fail
        assert mock_coordinator_instance.add_checkpoint.call_count > 0

        # --- Test order verification failure ---
        mock_verify_positions.reset_mock()
        mock_verify_fills.reset_mock()
        mock_verify_orders.reset_mock()
        mock_coordinator_instance.add_checkpoint.reset_mock()
        mock_verify_positions.return_value = mock_pos_result  # Ensure pos passes
        mock_verify_fills.return_value = mock_fill_result  # Ensure fills passes
        mock_order_fail_result: dict[str, Any] = {
            "success": False,
            "error": "Order Fail",
            "details": {"order_fail": True},
        }
        mock_verify_orders.return_value = mock_order_fail_result

        final_result_order_fail = await service_instance._verify_post_execution(
            mock_opportunity,
            mock_execution_result,
        )

        assert final_result_order_fail.get("success") is False
        assert "Order Fail" in final_result_order_fail.get("error", "")
        mock_verify_positions.assert_awaited_once()
        mock_verify_fills.assert_awaited_once()
        mock_verify_orders.assert_awaited_once()
        assert mock_coordinator_instance.add_checkpoint.call_count > 0
