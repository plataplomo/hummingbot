"""Tests for the synchronize order submission module."""

from __future__ import annotations

import time
from collections import defaultdict
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, NamedTuple, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.execution.synchronized_order_submission import (
    ExecutionContext,
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


pytestmark = pytest.mark.timing

# Configure logger
logger = get_logger(__name__)


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
        """Convert mock opportunity to dictionary representation for testing.

        Returns:
            Dictionary containing all opportunity fields as strings for JSON serialization.
        """
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
        """Return string representation of the mock opportunity for debugging."""
        return f"MockOpportunity({self.symbol}, {self.long_exchange}, {self.short_exchange})"

    def __hash__(self) -> int:
        """Return hash of the mock opportunity for dictionary and set operations."""
        return hash((self.symbol, self.long_exchange, self.short_exchange))


class TestOrderVerifier:
    """Test suite for the OrderVerifier class."""

    @pytest.fixture
    def portfolio_tracker(self) -> MagicMock:
        """Create a mock portfolio tracker.

        Returns:
            Mock PortfolioTracker instance configured for testing.
        """
        mock_tracker = MagicMock(spec=PortfolioTracker)

        # Initialize orders ; type is inferred from spec=PortfolioTracker
        mock_tracker.orders = defaultdict(
            lambda: dict[str, Order](),
        )  # More precise default factory

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

        mock_api_order = Order(
            client_order_id="exchange-order-id-1",
            exchange="mock_exchange",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.GTC,
            quantity_requested=Decimal("1.0"),
            price=Decimal(50000),
            status=OrderStatus.FILLED,
            quantity_filled=Decimal("1.0"),
            average_fill_price=Decimal(50000),
            created_at=datetime.now(UTC),
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )

        # Mock API client for exchange
        mock_api_client_hyperliquid = AsyncMock(
            spec=ExchangeAPI,
            get_order=AsyncMock(return_value=mock_api_order),
            get_order_status=AsyncMock(return_value=mock_api_order),
            get_trade_history=AsyncMock(
                return_value=[
                    MagicMock(
                        order_id="exchange-order-id-1",
                        symbol="BTC-PERP",
                        side=OrderSide.BUY,
                        price=Decimal("50000.0"),
                        quantity=Decimal("1.0"),
                        timestamp=int(datetime.now(UTC).timestamp() * 1000),
                    ),
                ],
            ),
        )

        # Correctly mock the api_clients dictionary attribute
        mock_tracker.api_clients = {
            "hyperliquid": mock_api_client_hyperliquid,
            # Add other exchanges if needed for tests, e.g., mock_exchange for some tests
            "mock_exchange": AsyncMock(
                spec=ExchangeAPI,
                get_order=AsyncMock(return_value=mock_api_order),
                get_order_status=AsyncMock(return_value=mock_api_order),
                get_trade_history=AsyncMock(return_value=[]),  # Example empty fills
            ),
        }

        # Mock the get_order_by_id method used by OrderVerifier
        # This needs to be flexible based on what OrderVerifier tries to fetch
        def mock_get_order_by_id(exchange_id: str, order_id_param: str) -> Order | None:
            """Return mock get order by id for testing."""
            # Simulate fetching the sample_filled_order if ids match
            # This part of the mock might need to be more sophisticated if tests
            # rely on different orders being returned.
            if order_id_param == "test-order-1" or order_id_param == "exchange-order-id-1":
                # For test_verify_order_placement, it uses "test-order-1"
                # For test_verify_order_execution, it uses "exchange-order-id-1"
                return sample_filled_order
            return None

        mock_tracker.get_order_by_id = MagicMock(side_effect=mock_get_order_by_id)

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
        # Ensure portfolio_tracker.orders behaves like a defaultdict(dict) and type it for Pyright
        # This check might be redundant if the fixture guarantees the type, but casting helps.
        typed_orders: defaultdict[str, dict[str, Order]]
        # Directly cast the attribute from the mock, assuming the fixture sets it up correctly.
        typed_orders = cast("defaultdict[str, dict[str, Order]]", portfolio_tracker.orders)

        if exchange_id not in typed_orders:
            typed_orders[exchange_id] = {}
        typed_orders[exchange_id][order_id] = mock_local_order

        result_dict = await verifier.verify_order_placement(exchange_id, order_id, expected_details)

        logger.info(f"Verification result: {result_dict}")

        assert result_dict["success"] is True
        assert result_dict.get("error") is None

        # Test failed verification with missing order
        # has no get_order
        # Instead, rely on the mock_get_order_by_id logic in the fixture for missing orders

        result_fail = await verifier.verify_order_placement(
            "hyperliquid",
            "missing-order",
            expected_details,
        )

        assert result_fail.get("success") is False
        error_msg = result_fail.get("error")
        assert error_msg is not None and "not found in local portfolio" in error_msg
        details = result_fail.get("details")
        assert details is not None and details.get("local_order") is None

    @pytest.mark.asyncio
    async def test_verify_order_execution(self, portfolio_tracker: MagicMock) -> None:
        """Test verifying order execution via OrderVerifier."""
        config: dict[str, Any] = {}
        verifier = OrderVerifier(config, portfolio_tracker)

        # Test successful verification
        # Ensure we use a client from the mocked api_clients dict
        # For the success part, the test calls with "hyperliquid", so configure that client.
        success_mock_api_client = portfolio_tracker.api_clients["hyperliquid"]

        api_order_response_filled = Order(
            client_order_id="exchange-order-id-1",
            exchange="hyperliquid",  # Match the exchange used in the call
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.GTC,
            quantity_requested=Decimal("1.0"),
            price=Decimal(50000),
            status=OrderStatus.FILLED,
            quantity_filled=Decimal("1.0"),
            average_fill_price=Decimal(50000),
            created_at=datetime.now(UTC),
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )
        success_mock_api_client.get_order_status = AsyncMock(return_value=api_order_response_filled)
        success_mock_api_client.get_order = AsyncMock(return_value=api_order_response_filled)
        success_mock_api_client.get_trade_history = AsyncMock(return_value=[])

        result_dict = await verifier.verify_order_execution("hyperliquid", "exchange-order-id-1")

        assert result_dict.get("success") is True
        assert result_dict.get("error") is None
        if result_dict and "details" in result_dict and result_dict["details"]:
            assert "api_order_details" in result_dict["details"]
            assert "local_order_details" in result_dict["details"]

        # Test failed verification (e.g., order not filled on exchange)
        # Ensure this mock is applied to the correct client ("hyperliquid")
        failure_mock_api_client = portfolio_tracker.api_clients["hyperliquid"]

        api_order_response_open = Order(
            client_order_id="exchange-order-id-1",
            exchange="hyperliquid",  # Match the exchange used in the call
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.GTC,
            quantity_requested=Decimal("1.0"),
            price=Decimal(50000),
            status=OrderStatus.OPEN,
            created_at=datetime.now(UTC),
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )
        # Configure the get_order_status for the failure case on the correct mock client
        failure_mock_api_client.get_order_status = AsyncMock(return_value=api_order_response_open)
        # Also mock get_order for fallback, though get_order_status should be hit first if available
        failure_mock_api_client.get_order = AsyncMock(return_value=api_order_response_open)

        result_fail = await verifier.verify_order_execution("hyperliquid", "exchange-order-id-1")
        assert result_fail.get("success") is False
        assert result_fail.get("error") is not None
        assert "Order status mismatch" in result_fail.get("error", "")


class TestExecutionCoordinator:
    """Test suite for the ExecutionCoordinator class."""

    @pytest.fixture
    def coordinator(self) -> ExecutionCoordinator:
        """Create an execution coordinator.

        Returns:
            ExecutionCoordinator instance configured for testing.
        """
        config: dict[str, Any] = {
            "execution.context_retention_seconds": 1,  # Short retention for testing
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

        _ = await coordinator.start_execution("test-execution-1", mock_opportunity, strategy)

        # Restore original assertions for context verification
        assert "test-execution-1" in coordinator.executions
        context = coordinator.executions["test-execution-1"]
        assert context.execution_id == "test-execution-1"
        # Compare relevant fields, model_dump might be needed if comparing full objects
        assert context.opportunity.symbol == mock_opportunity.symbol
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
    ) -> tuple[
        SynchronizedOrderSubmissionService,
        dict[str, Any],
        MagicMock,  # mock_cb_system
        MagicMock,  # mock_portfolio_tracker
    ]:
        """Create a service instance with mocked dependencies using dependency injection.

        Returns:
            Tuple containing service instance, config dictionary, and mock dependencies.
        """
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
        mock_pos_recon_system = (
            AsyncMock()
        )  # This is not returned by fixture, tests should patch if needed
        mock_portfolio_tracker = MagicMock(spec=PortfolioTracker)

        # Mocks for components potentially used internally or returned for test assertions
        # These are not directly injected via __init__ based on previous fixture structure,
        # but tests might expect them. Tests using @patch will override these anyway.

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

        # 4. Return service instance and necessary mocks
        # Returning mocks needed for assertions or further configuration in tests.
        # Keep returning mock_order_verifier and mock_coordinator for signature compatibility,
        # although tests using @patch might not use these specific instances.
        return service_instance, config, mock_cb_system, mock_portfolio_tracker

    @pytest.mark.asyncio
    @patch.object(
        SynchronizedOrderSubmissionService,
        "verify_pre_execution",
        new_callable=AsyncMock,
    )
    @patch.object(
        SynchronizedOrderSubmissionService,
        "_execute_sequential_with_verification",
        new_callable=AsyncMock,
    )
    @patch.object(
        SynchronizedOrderSubmissionService,
        "verify_post_execution",
        new_callable=AsyncMock,
    )
    async def test_submit_orders_sequential(
        self,
        mock_verify_post: AsyncMock,
        mock_execute_seq: AsyncMock,
        mock_verify_pre: AsyncMock,
        service: tuple[
            SynchronizedOrderSubmissionService,
            dict[str, Any],
            MagicMock,  # mock_cb_system
            MagicMock,  # mock_portfolio_tracker
        ],
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
        mock_verify_pre.return_value = {"verified": True, "error": None, "details": {}}
        mock_execute_seq.return_value = ExecutionResult(
            execution_id="test-seq-exec",
            status=ExecutionStatus.COMPLETED,
            timestamp=int(time.time() * 1000),
        )
        mock_verify_post.return_value = {"verified": True, "error": None, "details": {}}

        # Call the SUT
        result_obj: ExecutionResult = await service_instance.submit_orders(
            mock_opportunity,
            "sequential_lock_in",
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
        SynchronizedOrderSubmissionService,
        "verify_pre_execution",
        new_callable=AsyncMock,
    )
    @patch.object(
        SynchronizedOrderSubmissionService,
        "_execute_simultaneous_with_verification",
        new_callable=AsyncMock,
    )
    @patch.object(
        SynchronizedOrderSubmissionService,
        "verify_post_execution",
        new_callable=AsyncMock,
    )
    async def test_submit_orders_simultaneous(
        self,
        mock_verify_post: AsyncMock,
        mock_execute_sim: AsyncMock,
        mock_verify_pre: AsyncMock,
        service: tuple[
            SynchronizedOrderSubmissionService,
            dict[str, Any],
            MagicMock,  # mock_cb_system
            MagicMock,  # mock_portfolio_tracker
        ],
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
        mock_verify_pre.return_value = {"verified": True, "error": None, "details": {}}
        mock_execute_sim.return_value = {  # Returns dict
            "verified": True,
            "error": None,
            "timestamp": int(time.time() * 1000),
            "details": {},
        }
        mock_verify_post.return_value = {"verified": True, "error": None, "details": {}}

        # Call the SUT
        result_obj: ExecutionResult = await service_instance.submit_orders(
            mock_opportunity,
            "simultaneous",
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
        SynchronizedOrderSubmissionService,
        "verify_pre_execution",
        new_callable=AsyncMock,
    )
    @patch.object(
        SynchronizedOrderSubmissionService,
        "_execute_sequential_with_verification",
        new_callable=AsyncMock,
    )
    @patch.object(
        SynchronizedOrderSubmissionService,
        "verify_post_execution",
        new_callable=AsyncMock,
    )
    @patch.object(
        SynchronizedOrderSubmissionService,
        "_compensate_verification_failure",
        new_callable=AsyncMock,
    )
    @patch.object(ExecutionCoordinator, "start_execution", new_callable=AsyncMock)
    @patch.object(ExecutionCoordinator, "add_checkpoint", new_callable=AsyncMock)
    @patch.object(ExecutionCoordinator, "complete_execution", new_callable=AsyncMock)
    async def test_submit_orders_pre_execution_failure(
        self,
        mock_ec_complete_exec: AsyncMock,  # Corresponds to ExecutionCoordinator.complete_execution
        mock_ec_add_checkpoint: AsyncMock,  # Corresponds to ExecutionCoordinator.add_checkpoint
        mock_ec_start_exec: AsyncMock,  # Corresponds to ExecutionCoordinator.start_execution
        mock_sos_compensate: AsyncMock,  # Corresponds to SynchronizedOrderSubmissionService
        # ._compensate_verification_failure
        mock_sos_verify_post: AsyncMock,  # Corresponds to SynchronizedOrderSubmissionService
        # .verify_post_execution
        mock_sos_exec_seq: AsyncMock,  # Corresponds to SynchronizedOrderSubmissionService
        # ._execute_sequential_with_verification
        mock_sos_verify_pre: AsyncMock,  # Corresponds to SynchronizedOrderSubmissionService
        # .verify_pre_execution
        service: tuple[  # This should now be the fixture
            SynchronizedOrderSubmissionService,
            dict[str, Any],
            MagicMock,  # mock_cb_system
            MagicMock,  # mock_portfolio_tracker
        ],
    ) -> None:
        """Test failure during pre-execution checks."""
        service_instance, _, _, _ = service  # Now 'service' is the fixture tuple
        # Use patched coordinator mocks from parameters, not fixture
        # Assign them to the service_instance's coordinator if necessary,
        # or ensure the patches correctly target the instance's coordinator.
        # For now, we assume patches correctly mock the methods on the instance.
        # However, the coordinator itself is instantiated within SOS.
        # So we should mock the *class* ExecutionCoordinator, and then its methods.
        # The current patches on ExecutionCoordinator methods should be fine if they
        # are applied globally to the class for the test's duration.

        # service_instance.execution_coordinator is created internally.
        # To control its methods, the patches on ExecutionCoordinator class methods are used.
        # mock_ec_start_exec, mock_ec_add_checkpoint, mock_ec_complete_exec will act on it.

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

        # Configure the mock for SynchronizedOrderSubmissionService.verify_pre_execution to fail
        mock_sos_verify_pre.return_value = {  # Use the correctly named mock
            "verified": False,
            "error": "Pre-execution check failed (mock)",
            "details": {},
        }

        # Mock coordinator methods
        mock_context = MagicMock(spec=ExecutionContext)  # Use spec for better mock
        mock_ec_start_exec.return_value = mock_context

        def mock_complete_side_effect(ctx: ExecutionContext, res: ExecutionResult) -> None:
            assert isinstance(res, ExecutionResult)
            assert res.status == ExecutionStatus.REJECTED
            # ensure ctx is the same mock_context if needed
            assert ctx == mock_context

        mock_ec_complete_exec.side_effect = mock_complete_side_effect

        # Submit orders
        result_obj: ExecutionResult = await service_instance.submit_orders(
            mock_opportunity,
            strategy,
        )

        # Verify status is REJECTED and error is propagated correctly in ExecutionResult
        assert result_obj.status == ExecutionStatus.REJECTED
        assert result_obj.error == "Pre-execution check failed (mock)"

        # Verify the pre-execution check was called, but not execution or post-check
        mock_sos_verify_pre.assert_awaited_once()
        mock_sos_exec_seq.assert_not_awaited()
        # This was mock_verify_post, now mock_sos_verify_post
        mock_sos_verify_post.assert_not_awaited()

        # Verify coordinator interactions
        mock_ec_start_exec.assert_awaited_once()
        # Checkpoints called for start and pre-execution checks
        # With complete_execution's side_effect bypassing original add_checkpoint,
        # only start_execution's add_checkpoint is counted.
        assert mock_ec_add_checkpoint.call_count == 1  # Expect 1 call from start_execution
        mock_ec_complete_exec.assert_awaited_once_with(mock_context, result_obj)

    @pytest.mark.asyncio
    @patch.object(
        SynchronizedOrderSubmissionService,
        "verify_pre_execution",
        new_callable=AsyncMock,
    )
    @patch.object(
        SynchronizedOrderSubmissionService,
        "_execute_sequential_with_verification",
        new_callable=AsyncMock,
    )
    @patch.object(
        SynchronizedOrderSubmissionService,
        "verify_post_execution",
        new_callable=AsyncMock,
    )
    @patch.object(
        SynchronizedOrderSubmissionService,
        "_compensate_verification_failure",
        new_callable=AsyncMock,
    )
    @patch.object(ExecutionCoordinator, "start_execution", new_callable=AsyncMock)
    @patch.object(ExecutionCoordinator, "add_checkpoint", new_callable=AsyncMock)
    @patch.object(ExecutionCoordinator, "complete_execution", new_callable=AsyncMock)
    async def test_submit_orders_post_execution_failure(
        self,
        mock_ec_complete_exec: AsyncMock,
        mock_ec_add_checkpoint: AsyncMock,
        mock_ec_start_exec: AsyncMock,
        mock_sos_compensate: AsyncMock,
        mock_sos_verify_post: AsyncMock,
        mock_sos_exec_seq: AsyncMock,
        mock_sos_verify_pre: AsyncMock,
        service: tuple[
            SynchronizedOrderSubmissionService,
            dict[str, Any],
            MagicMock,  # mock_cb_system
            MagicMock,  # mock_portfolio_tracker
        ],
    ) -> None:
        """Test order submission with post-execution verification failure."""
        service_instance, _, _, _ = service  # Unpack fixture
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
            id="opp-post-exec-fail-test",
            optimal_size=Decimal("0.1"),
            utility_score=0.8,
            basis_volatility=0.001,
        )

        # Mock pre-execution verification to succeed
        mock_sos_verify_pre.return_value = {"verified": True}

        # Mock execution to succeed
        mock_execution_result = ExecutionResult(
            execution_id="test_exec_id_post_fail",
            status=ExecutionStatus.COMPLETED,
            timestamp=int(time.time() * 1000),
            details=None,  # Explicitly provide details as None initially
        )
        mock_sos_exec_seq.return_value = mock_execution_result

        # Mock post-execution verification to fail
        mock_sos_verify_post.return_value = {
            "verified": False,
            "error": "Post-execution check failed",
        }

        # Mock compensation to succeed
        mock_sos_compensate.return_value = {
            "compensated": True,
            "details": "Compensation successful",
        }

        # Mock ExecutionCoordinator methods
        mock_exec_context = ExecutionContext(
            execution_id="test_exec_id_post_fail",
            opportunity=mock_opportunity,
            strategy="sequential_lock_in",  # Default strategy from config if not specified
            start_time=datetime.now(UTC),
            status=ExecutionStatus.PENDING,
            checkpoints=[],
        )
        mock_ec_start_exec.return_value = mock_exec_context

        def mock_complete_side_effect(ctx: ExecutionContext, res: ExecutionResult) -> None:
            assert isinstance(res, ExecutionResult)
            assert res.status == ExecutionStatus.PARTIALLY_COMPLETED
            assert ctx == mock_exec_context

        mock_ec_complete_exec.side_effect = mock_complete_side_effect

        # Call the method using the default strategy from config (or specify if different)
        result = await service_instance.submit_orders(mock_opportunity)

        # Assertions
        assert result.status == ExecutionStatus.PARTIALLY_COMPLETED
        assert result.error is None
        assert result.details is not None
        assert result.details.get("compensation_attempted") is True
        compensation_res = result.details.get("compensation_result")
        assert compensation_res is not None
        assert compensation_res.get("compensated") is True
        assert "Compensation successful" in compensation_res.get("details", "")

        # Verify calls
        mock_sos_verify_pre.assert_awaited_once()
        mock_sos_exec_seq.assert_awaited_once()
        mock_sos_verify_post.assert_awaited_once()
        mock_sos_compensate.assert_awaited_once()
        mock_ec_start_exec.assert_awaited_once()
        mock_ec_complete_exec.assert_awaited_once_with(mock_exec_context, result)
        assert mock_ec_add_checkpoint.call_count >= 4

    @pytest.mark.asyncio
    @patch.object(
        SynchronizedOrderSubmissionService,
        "verify_market_conditions",
        new_callable=AsyncMock,
    )
    @patch.object(SynchronizedOrderSubmissionService, "verify_balances", new_callable=AsyncMock)
    @patch(
        "cyberdelta.core.execution.synchronized_order_submission.ExecutionCoordinator",
        autospec=True,
    )  # Use autospec for coordinator mock
    async def test_internal_verify_pre_execution(
        self,
        MockExecutionCoordinator: MagicMock,  # Patched class
        mock_verify_balances: AsyncMock,
        mock_verify_market_conditions: AsyncMock,
        service: tuple[
            SynchronizedOrderSubmissionService,
            dict[str, Any],
            MagicMock,  # mock_cb_system
            MagicMock,  # mock_portfolio_tracker
        ],
    ) -> None:
        """Test the internal verify_pre_execution method's logic paths."""
        service_instance, _, mock_cb_system, _ = service

        # --- Setup for ExecutionCoordinator interaction ---
        # Instantiate the mock ExecutionCoordinator
        mock_coordinator_instance = MockExecutionCoordinator.return_value
        # Explicitly assign to ensure the service instance uses this specific mock
        service_instance.execution_coordinator = mock_coordinator_instance

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
            id="test-opp-verify-pre-exec",
            optimal_size=Decimal("1.0"),
            utility_score=0.8,
            basis_volatility=0.001,
        )

        # Create a mock ExecutionContext instance
        mock_exec_context = ExecutionContext(
            execution_id="test_exec_context_id",
            opportunity=mock_opportunity,
            strategy="test_strategy",
            start_time=datetime.now(UTC),
            status=ExecutionStatus.PENDING,
            checkpoints=[],
        )

        # --- Test all pass ---
        mock_cb_system.can_execute.return_value = (True, None)
        mock_verify_market_conditions.return_value = {
            "verified": True,
            "error": None,
            "details": {},
        }
        mock_verify_balances.return_value = {"verified": True, "error": None, "details": {}}

        result_dict_all_pass = await service_instance.verify_pre_execution(
            mock_exec_context,
            mock_opportunity,
        )
        assert result_dict_all_pass.get("verified") is True
        assert result_dict_all_pass.get("error") is None
        mock_cb_system.can_execute.assert_any_call(
            mock_opportunity.long_exchange,
            mock_opportunity.symbol,
        )
        mock_cb_system.can_execute.assert_any_call(
            mock_opportunity.short_exchange,
            mock_opportunity.symbol,
        )
        mock_verify_market_conditions.assert_awaited_once_with(mock_opportunity)
        mock_verify_balances.assert_awaited_once_with(mock_opportunity)
        assert mock_coordinator_instance.add_checkpoint.call_count > 0  # Check for some calls

        # Reset mocks for next scenario
        mock_cb_system.reset_mock()
        mock_cb_system.can_execute.side_effect = (
            None  # Explicitly clear side_effect for the attribute mock
        )
        mock_verify_market_conditions.reset_mock()
        mock_verify_balances.reset_mock()
        mock_coordinator_instance.add_checkpoint.reset_mock()

        # --- Test long leg CB failure ---
        mock_cb_system.can_execute.side_effect = [
            (False, "Long leg CB tripped"),
            (True, None),  # Should not be called for short leg
        ]
        mock_verify_market_conditions.return_value = {
            "verified": True,
            "error": None,
            "details": {},
        }
        mock_verify_balances.return_value = {"verified": True, "error": None, "details": {}}

        result_dict_fail_long_cb = await service_instance.verify_pre_execution(
            mock_exec_context,
            mock_opportunity,
        )
        assert result_dict_fail_long_cb.get("verified") is False
        assert "Long leg CB tripped" in result_dict_fail_long_cb.get("error", "")
        mock_cb_system.can_execute.assert_called_once_with(
            mock_opportunity.long_exchange,
            mock_opportunity.symbol,
        )  # Only long check
        mock_verify_market_conditions.assert_not_awaited()
        mock_verify_balances.assert_not_awaited()
        assert mock_coordinator_instance.add_checkpoint.call_count > 0

        # Reset mocks
        mock_cb_system.reset_mock()
        mock_cb_system.can_execute.side_effect = (
            None  # Explicitly clear side_effect for the attribute mock
        )
        mock_verify_market_conditions.reset_mock()
        mock_verify_balances.reset_mock()
        mock_coordinator_instance.add_checkpoint.reset_mock()

        # --- Test short leg CB failure ---
        mock_cb_system.can_execute.side_effect = [
            (True, None),
            (False, "Short leg CB tripped"),
        ]
        mock_verify_market_conditions.return_value = {
            "verified": True,
            "error": None,
            "details": {},
        }
        mock_verify_balances.return_value = {"verified": True, "error": None, "details": {}}

        result_dict_fail_short_cb = await service_instance.verify_pre_execution(
            mock_exec_context,
            mock_opportunity,
        )
        assert result_dict_fail_short_cb.get("verified") is False
        assert "Short leg CB tripped" in result_dict_fail_short_cb.get("error", "")
        # Market/balance checks might not be called if short CB fails.
        # For now, assume they are not called after a CB failure in verify_pre_execution
        assert mock_cb_system.can_execute.call_count == 2
        assert mock_coordinator_instance.add_checkpoint.call_count > 0

        # Reset mocks
        mock_cb_system.reset_mock()
        mock_cb_system.can_execute.side_effect = (
            None  # Explicitly clear side_effect for the attribute mock
        )
        mock_verify_market_conditions.reset_mock()
        mock_verify_balances.reset_mock()
        mock_coordinator_instance.add_checkpoint.reset_mock()

        # --- Test market condition failure ---
        mock_cb_system.can_execute.return_value = (True, None)
        mock_verify_market_conditions.return_value = {
            "verified": False,
            "error": "Market spread too wide",
            "details": {},
        }
        mock_verify_balances.return_value = {"verified": True, "error": None, "details": {}}

        result_dict_fail_market = await service_instance.verify_pre_execution(
            mock_exec_context,
            mock_opportunity,
        )
        assert result_dict_fail_market.get("verified") is False
        assert "Market spread too wide" in result_dict_fail_market.get("error", "")
        mock_verify_balances.assert_not_awaited()  # Should not be called if market fails
        assert mock_coordinator_instance.add_checkpoint.call_count > 0

        # Reset mocks
        mock_cb_system.reset_mock()
        mock_cb_system.can_execute.side_effect = (
            None  # Explicitly clear side_effect for the attribute mock
        )
        mock_verify_market_conditions.reset_mock()
        mock_verify_balances.reset_mock()
        mock_coordinator_instance.add_checkpoint.reset_mock()

        # --- Test balance verification failure ---
        mock_circuit_breaker_system_balance_fail = MagicMock(spec=CircuitBreakerSystem)
        mock_circuit_breaker_system_balance_fail.can_execute.return_value = (True, None)
        mock_cb_system.can_execute.side_effect = (
            None  # Clear side effect before setting return_value
        )
        mock_cb_system.can_execute.return_value = (True, None)

        mock_verify_market_conditions.return_value = {
            "verified": True,
            "error": None,
            "details": {},
        }
        mock_verify_balances.return_value = {
            "verified": False,
            "error": "Insufficient balance on long leg",
            "details": {},
        }

        result_dict_fail_balance = await service_instance.verify_pre_execution(
            mock_exec_context,
            mock_opportunity,
        )
        assert result_dict_fail_balance.get("verified") is False
        assert "Insufficient balance on long leg" in result_dict_fail_balance.get("error", "")
        assert mock_coordinator_instance.add_checkpoint.call_count > 0

    @pytest.mark.asyncio
    @patch.object(SynchronizedOrderSubmissionService, "verify_positions", new_callable=AsyncMock)
    @patch.object(SynchronizedOrderSubmissionService, "verify_fills", new_callable=AsyncMock)
    @patch.object(SynchronizedOrderSubmissionService, "verify_orders", new_callable=AsyncMock)
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
        service: tuple[
            SynchronizedOrderSubmissionService,
            dict[str, Any],
            MagicMock,  # mock_cb_system
            MagicMock,  # mock_portfolio_tracker
        ],
    ) -> None:
        """Test the verify_post_execution method."""
        service_instance, _, _, _ = service

        # --- Setup for ExecutionCoordinator interaction ---
        mock_coordinator_instance = MockExecutionCoordinator.return_value
        service_instance.execution_coordinator = (
            mock_coordinator_instance  # CRITICAL: Ensure this assignment happens
        )

        mock_opportunity = ArbitrageOpportunity(
            symbol="ETH-PERP",
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_price=Decimal("3000.0"),
            short_price=Decimal("3001.0"),
            long_funding_rate=Decimal("0.0001"),
            short_funding_rate=Decimal("-0.0001"),
            net_funding_differential=Decimal("0.0002"),
            timestamp=datetime.now(UTC),
            expected_profit=Decimal("0.5"),
            id="test-opp-verify-post-exec",
            optimal_size=Decimal("0.5"),
            utility_score=0.7,
            basis_volatility=0.002,
        )
        mock_execution_result = ExecutionResult(
            execution_id="test_exec_id_verify_post",
            status=ExecutionStatus.COMPLETED,
            timestamp=int(time.time() * 1000),
        )
        mock_exec_context = ExecutionContext(
            execution_id="test_exec_id_verify_post",
            opportunity=mock_opportunity,
            strategy="test_strategy",
            start_time=datetime.now(UTC),
            status=ExecutionStatus.COMPLETED,
            checkpoints=[],
        )

        # Scenario 1: All verifications pass
        mock_verify_positions.return_value = {"verified": True, "details": "Positions OK"}
        mock_verify_fills.return_value = {"verified": True, "details": "Fills OK"}
        mock_verify_orders.return_value = {"verified": True, "details": "Orders OK"}

        result_pass = await service_instance.verify_post_execution(
            mock_exec_context,
            mock_opportunity,
            mock_execution_result,
        )
        assert result_pass["verified"] is True
        mock_verify_positions.assert_awaited_once_with(mock_opportunity, mock_execution_result)
        mock_verify_fills.assert_awaited_once_with(mock_opportunity, mock_execution_result)
        mock_verify_orders.assert_awaited_once_with(mock_opportunity, mock_execution_result)
        # Check for success checkpoint
        checkpoint_found = False
        expected_checkpoint_name = "post_execution_verification_success"
        expected_details = result_pass["details"]
        for call_args in mock_coordinator_instance.add_checkpoint.call_args_list:
            args, _kwargs = call_args
            if (
                len(args) == 3
                and args[0] == mock_exec_context
                and args[1] == expected_checkpoint_name
                and args[2] == expected_details
            ):
                checkpoint_found = True
                break
        assert checkpoint_found, (
            f"Checkpoint {expected_checkpoint_name} with details {expected_details} "
            f"not found in calls to add_checkpoint. "
            f"Calls: {mock_coordinator_instance.add_checkpoint.call_args_list}"
        )

        # Reset mocks for next scenario
        mock_verify_positions.reset_mock()
        mock_verify_fills.reset_mock()
        mock_verify_orders.reset_mock()
        mock_coordinator_instance.add_checkpoint.reset_mock()

        # Scenario 2: Position verification fails
        mock_verify_positions.return_value = {
            "verified": False,
            "error": "Position mismatch",
            "details": {},
        }
        mock_verify_fills.return_value = {"verified": True}  # Fills and orders pass
        mock_verify_orders.return_value = {"verified": True}

        result_fail_pos = await service_instance.verify_post_execution(
            mock_exec_context,
            mock_opportunity,
            mock_execution_result,
        )
        assert result_fail_pos["verified"] is False
        checkpoint_found_fail_pos = False
        expected_checkpoint_name_fail_pos = "position_verification_failed"
        expected_details_fail_pos: dict[str, Any] = {"error": "Position mismatch", "details": {}}
        for call_args in mock_coordinator_instance.add_checkpoint.call_args_list:
            args, _kwargs = call_args
            if (
                len(args) == 3
                and args[0] == mock_exec_context
                and args[1] == expected_checkpoint_name_fail_pos
                and args[2] == expected_details_fail_pos
            ):
                checkpoint_found_fail_pos = True
                break
        assert checkpoint_found_fail_pos, (
            f"Checkpoint {expected_checkpoint_name_fail_pos} with details "
            f"{expected_details_fail_pos} "
            f"not found in calls to add_checkpoint. "
            f"Calls: {mock_coordinator_instance.add_checkpoint.call_args_list}"
        )

        # Reset mocks
        mock_verify_positions.reset_mock()
        mock_verify_fills.reset_mock()
        mock_verify_orders.reset_mock()
        mock_coordinator_instance.add_checkpoint.reset_mock()

        # Scenario 3: Fill verification fails
        mock_verify_positions.return_value = {"verified": True}
        mock_verify_fills.return_value = {"verified": False, "error": "Fill quantity incorrect"}
        mock_verify_orders.return_value = {"verified": True}

        result_fail_fill = await service_instance.verify_post_execution(
            mock_exec_context,
            mock_opportunity,
            mock_execution_result,
        )
        assert result_fail_fill["verified"] is False
        checkpoint_found_fail_fill = False
        expected_checkpoint_name_fail_fill = "fill_verification_failed"
        expected_details_fail_fill: dict[str, Any | None] = {
            "error": "Fill quantity incorrect",
            "details": None,
        }
        for call_args in mock_coordinator_instance.add_checkpoint.call_args_list:
            args, _kwargs = call_args
            if (
                len(args) == 3
                and args[0] == mock_exec_context
                and args[1] == expected_checkpoint_name_fail_fill
                and args[2] == expected_details_fail_fill
            ):
                checkpoint_found_fail_fill = True
                break
        assert checkpoint_found_fail_fill, (
            f"Checkpoint {expected_checkpoint_name_fail_fill} with details "
            f"{expected_details_fail_fill} "
            f"not found in calls to add_checkpoint. "
            f"Calls: {mock_coordinator_instance.add_checkpoint.call_args_list}"
        )
