"""
Tests for the synchronize order submission module.
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from cyberdelta.core.execution.synchronized_order_submission import (
    SynchronizedOrderSubmissionService,
    OrderVerifier,
    ExecutionCoordinator,
    ExecutionStatus,
    VerificationResult,
    ExecutionResult,
    ExecutionContext
)
from cyberdelta.core.models import OrderStatus, Order, OrderSide, OrderType


class MockOpportunity:
    """Mock arbitrage opportunity for testing."""
    
    def __init__(self, symbol="BTC-PERP", long_exchange="hyperliquid", short_exchange="backpack"):
        self.symbol = symbol
        self.long_exchange = long_exchange
        self.short_exchange = short_exchange
        
    def to_dict(self):
        return {
            "symbol": self.symbol,
            "long_exchange": self.long_exchange,
            "short_exchange": self.short_exchange
        }
        
    def __str__(self):
        return f"MockOpportunity({self.symbol}, {self.long_exchange}, {self.short_exchange})"
        
    def __hash__(self):
        return hash((self.symbol, self.long_exchange, self.short_exchange))


class TestOrderVerifier:
    """Test suite for the OrderVerifier class."""
    
    @pytest.fixture
    def portfolio_tracker(self):
        """Create a mock portfolio tracker."""
        mock_tracker = MagicMock()
        mock_tracker.get_order.return_value = Order(
            id="test-order-1",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            type=OrderType.LIMIT,
            price=50000.0,
            quantity=1.0,
            filled_quantity=1.0,
            status=OrderStatus.FILLED.value,
            time=int(datetime.now().timestamp() * 1000),
            client_order_id="client-order-1"
        )
        
        # Mock API client for exchange
        mock_api_client = AsyncMock()
        mock_api_client.get_order.return_value = {
            "id": "test-order-1",
            "symbol": "BTC-PERP",
            "side": "BUY",
            "type": "LIMIT",
            "price": 50000.0,
            "quantity": 1.0,
            "filled_quantity": 1.0,
            "status": "FILLED"
        }
        mock_api_client.get_recent_fills.return_value = [
            {
                "order_id": "test-order-1",
                "symbol": "BTC-PERP",
                "side": "BUY",
                "price": 50000.0,
                "quantity": 1.0,
                "timestamp": int(datetime.now().timestamp() * 1000)
            }
        ]
        
        mock_tracker.get_api_client.return_value = mock_api_client
        
        return mock_tracker
    
    @pytest.mark.asyncio
    async def test_verify_order_placement(self, portfolio_tracker):
        """Test verifying order placement."""
        config = {}
        verifier = OrderVerifier(config, portfolio_tracker)
        
        # Test successful verification
        expected_details = {
            "symbol": "BTC-PERP",
            "side": OrderSide.BUY,
            "type": OrderType.LIMIT
        }
        
        result = await verifier.verify_order_placement("hyperliquid", "test-order-1", expected_details)
        
        assert result.success is True
        assert result.error is None
        assert "local_order" in result.details
        assert "api_order" in result.details
        
        # Test failed verification with missing order
        portfolio_tracker.get_order.return_value = None
        
        result = await verifier.verify_order_placement("hyperliquid", "missing-order", expected_details)
        
        assert result.success is False
        assert "not found in local state" in result.error
        assert result.details["local_order"] is None
        
    @pytest.mark.asyncio
    async def test_verify_order_execution(self, portfolio_tracker):
        """Test verifying order execution."""
        config = {}
        verifier = OrderVerifier(config, portfolio_tracker)
        
        # Test successful verification
        result = await verifier.verify_order_execution("hyperliquid", "test-order-1")
        
        assert result.success is True
        assert result.error is None
        assert "local_order" in result.details
        assert "api_order" in result.details
        assert "matching_fills" in result.details
        assert len(result.details["matching_fills"]) > 0
        
        # Test failed verification with unfilled order
        order = portfolio_tracker.get_order.return_value
        order.status = OrderStatus.NEW.value
        
        result = await verifier.verify_order_execution("hyperliquid", "test-order-1")
        
        assert result.success is False
        assert "not filled in local state" in result.error
        
        # Test failed verification with no fills
        order.status = OrderStatus.FILLED.value
        portfolio_tracker.get_api_client.return_value.get_recent_fills.return_value = []
        
        result = await verifier.verify_order_execution("hyperliquid", "test-order-1")
        
        assert result.success is False
        assert "No fills found for order" in result.error


class TestExecutionCoordinator:
    """Test suite for the ExecutionCoordinator class."""
    
    @pytest.fixture
    def coordinator(self):
        """Create an execution coordinator."""
        config = {
            "execution.context_retention_seconds": 1  # Short retention for testing
        }
        return ExecutionCoordinator(config)
    
    @pytest.mark.asyncio
    async def test_start_execution(self, coordinator):
        """Test starting an execution."""
        opportunity = MockOpportunity()
        strategy = "sequential_lock_in"
        
        context = await coordinator.start_execution("test-execution-1", opportunity, strategy)
        
        assert context.execution_id == "test-execution-1"
        assert context.opportunity == opportunity
        assert context.strategy == strategy
        assert context.status == ExecutionStatus.PENDING
        assert len(context.checkpoints) == 1
        assert context.checkpoints[0]["name"] == "execution_started"
        
        # Verify the execution was stored
        assert "test-execution-1" in coordinator.executions
        
    @pytest.mark.asyncio
    async def test_add_checkpoint(self, coordinator):
        """Test adding a checkpoint."""
        opportunity = MockOpportunity()
        strategy = "sequential_lock_in"
        
        context = await coordinator.start_execution("test-execution-2", opportunity, strategy)
        
        # Add a checkpoint
        await coordinator.add_checkpoint(context, "test-checkpoint", {"test": "data"})
        
        assert len(context.checkpoints) == 2
        assert context.checkpoints[1]["name"] == "test-checkpoint"
        assert context.checkpoints[1]["details"]["test"] == "data"
        
    @pytest.mark.asyncio
    async def test_complete_execution(self, coordinator):
        """Test completing an execution."""
        opportunity = MockOpportunity()
        strategy = "sequential_lock_in"
        
        context = await coordinator.start_execution("test-execution-3", opportunity, strategy)
        
        # Complete the execution
        result = ExecutionResult(
            execution_id="test-execution-3",
            status=ExecutionStatus.COMPLETED
        )
        
        await coordinator.complete_execution(context, result)
        
        assert context.status == ExecutionStatus.COMPLETED
        assert context.result == result
        assert context.end_time is not None
        assert len(context.checkpoints) == 2
        assert context.checkpoints[1]["name"] == "execution_completed"
        
    @pytest.mark.asyncio
    async def test_abort_execution(self, coordinator):
        """Test aborting an execution."""
        opportunity = MockOpportunity()
        strategy = "sequential_lock_in"
        
        context = await coordinator.start_execution("test-execution-4", opportunity, strategy)
        
        # Abort the execution
        abort_result = await coordinator.abort_execution(context, "Test abort reason")
        
        assert context.status == ExecutionStatus.FAILED
        assert context.abort_reason == "Test abort reason"
        assert context.end_time is not None
        assert len(context.checkpoints) == 2
        assert context.checkpoints[1]["name"] == "execution_aborted"
        
        # Check abort result
        assert abort_result["execution_id"] == "test-execution-4"
        assert abort_result["abort_reason"] == "Test abort reason"
        assert abort_result["checkpoint_count"] == 2


class TestSynchronizedOrderSubmissionService:
    """Test suite for the SynchronizedOrderSubmissionService class."""
    
    @pytest.fixture
    def service(self):
        """Create a service instance with mocked dependencies."""
        config = {}
        
        # Mock exchange adapters
        exchange_adapters = {
            "hyperliquid": AsyncMock(),
            "backpack": AsyncMock()
        }
        
        # Mock circuit breaker system
        circuit_breaker_system = MagicMock()
        circuit_breaker_result = MagicMock()
        circuit_breaker_result.all_ok = True
        circuit_breaker_result.tripped_breakers = []
        circuit_breaker_system.check_all.return_value = circuit_breaker_result
        
        # Mock position reconciliation system
        position_reconciliation_system = AsyncMock()
        
        # Mock portfolio tracker
        portfolio_tracker = MagicMock()
        
        # Create service with mocked dependencies
        service = SynchronizedOrderSubmissionService(
            config=config,
            exchange_adapters=exchange_adapters,
            circuit_breaker_system=circuit_breaker_system,
            position_reconciliation_system=position_reconciliation_system,
            portfolio_tracker=portfolio_tracker
        )
        
        # Mock service methods
        service._verify_market_conditions = AsyncMock(return_value=VerificationResult(
            timestamp=int(datetime.now().timestamp() * 1000),
            success=True,
            details={"checked": True}
        ))
        
        service._verify_balances = AsyncMock(return_value=VerificationResult(
            timestamp=int(datetime.now().timestamp() * 1000),
            success=True,
            details={"checked": True}
        ))
        
        service._verify_positions = AsyncMock(return_value=VerificationResult(
            timestamp=int(datetime.now().timestamp() * 1000),
            success=True,
            details={"checked": True}
        ))
        
        service._verify_fills = AsyncMock(return_value=VerificationResult(
            timestamp=int(datetime.now().timestamp() * 1000),
            success=True,
            details={"checked": True}
        ))
        
        service._verify_orders = AsyncMock(return_value=VerificationResult(
            timestamp=int(datetime.now().timestamp() * 1000),
            success=True,
            details={"checked": True}
        ))
        
        service._execute_sequential_with_verification = AsyncMock(return_value=ExecutionResult(
            execution_id="test-execution",
            status=ExecutionStatus.COMPLETED
        ))
        
        service._execute_simultaneous_with_verification = AsyncMock(return_value=ExecutionResult(
            execution_id="test-execution",
            status=ExecutionStatus.COMPLETED
        ))
        
        return service
    
    @pytest.mark.asyncio
    async def test_submit_orders_sequential(self, service):
        """Test submitting orders with sequential strategy."""
        opportunity = MockOpportunity()
        
        result = await service.submit_orders(opportunity, "sequential_lock_in")
        
        assert result.status == ExecutionStatus.COMPLETED
        assert service._verify_pre_execution.called
        assert service._execute_sequential_with_verification.called
        assert service._verify_post_execution.called
        
    @pytest.mark.asyncio
    async def test_submit_orders_simultaneous(self, service):
        """Test submitting orders with simultaneous strategy."""
        opportunity = MockOpportunity()
        
        result = await service.submit_orders(opportunity, "simultaneous")
        
        assert result.status == ExecutionStatus.COMPLETED
        assert service._verify_pre_execution.called
        assert service._execute_simultaneous_with_verification.called
        assert service._verify_post_execution.called
        
    @pytest.mark.asyncio
    async def test_submit_orders_pre_execution_failure(self, service):
        """Test submitting orders with pre-execution verification failure."""
        opportunity = MockOpportunity()
        
        # Mock pre-execution verification failure
        service._verify_pre_execution = AsyncMock(return_value=VerificationResult(
            timestamp=int(datetime.now().timestamp() * 1000),
            success=False,
            error="Test verification failure",
            details={"failed": True}
        ))
        
        result = await service.submit_orders(opportunity)
        
        assert result.status == ExecutionStatus.REJECTED
        assert result.error == "Test verification failure"
        assert service._verify_pre_execution.called
        assert not service._execute_sequential_with_verification.called
        assert not service._verify_post_execution.called
        
    @pytest.mark.asyncio
    async def test_submit_orders_post_execution_failure(self, service):
        """Test submitting orders with post-execution verification failure."""
        opportunity = MockOpportunity()
        
        # Mock post-execution verification failure
        service._verify_post_execution = AsyncMock(return_value=VerificationResult(
            timestamp=int(datetime.now().timestamp() * 1000),
            success=False,
            error="Test post-verification failure",
            details={"failed": True}
        ))
        
        # Mock compensation for verification failure
        service._compensate_verification_failure = AsyncMock(return_value={"compensated": True})
        
        result = await service.submit_orders(opportunity)
        
        assert result.status == ExecutionStatus.PARTIALLY_COMPLETED
        assert result.error == "Test post-verification failure"
        assert service._verify_pre_execution.called
        assert service._execute_sequential_with_verification.called
        assert service._verify_post_execution.called
        assert service._compensate_verification_failure.called
        assert result.compensation_result == {"compensated": True}
        
    @pytest.mark.asyncio
    async def test_verify_pre_execution(self, service):
        """Test pre-execution verification."""
        opportunity = MockOpportunity()
        
        # Test successful verification
        result = await service._verify_pre_execution(opportunity)
        
        assert result.success is True
        assert result.error is None
        assert "circuit_breakers" in result.details
        assert "market_conditions" in result.details
        assert "balances" in result.details
        
        # Test circuit breaker failure
        service.circuit_breaker_system.check_all.return_value.all_ok = False
        service.circuit_breaker_system.check_all.return_value.tripped_breakers = ["exchange:hyperliquid"]
        
        result = await service._verify_pre_execution(opportunity)
        
        assert result.success is False
        assert "Circuit breakers tripped" in result.error
        
        # Reset circuit breaker mock
        service.circuit_breaker_system.check_all.return_value.all_ok = True
        service.circuit_breaker_system.check_all.return_value.tripped_breakers = []
        
        # Test market conditions failure
        service._verify_market_conditions = AsyncMock(return_value=VerificationResult(
            timestamp=int(datetime.now().timestamp() * 1000),
            success=False,
            error="Market condition failure",
            details={"failed": True}
        ))
        
        result = await service._verify_pre_execution(opportunity)
        
        assert result.success is False
        assert "Market condition verification failed" in result.error
        
        # Test balance verification failure
        service._verify_market_conditions = AsyncMock(return_value=VerificationResult(
            timestamp=int(datetime.now().timestamp() * 1000),
            success=True,
            details={"checked": True}
        ))
        
        service._verify_balances = AsyncMock(return_value=VerificationResult(
            timestamp=int(datetime.now().timestamp() * 1000),
            success=False,
            error="Insufficient balance",
            details={"failed": True}
        ))
        
        result = await service._verify_pre_execution(opportunity)
        
        assert result.success is False
        assert "Insufficient balance" in result.error
        
    @pytest.mark.asyncio
    async def test_verify_post_execution(self, service):
        """Test post-execution verification."""
        opportunity = MockOpportunity()
        execution_result = ExecutionResult(
            execution_id="test-execution",
            status=ExecutionStatus.COMPLETED
        )
        
        # Test successful verification
        result = await service._verify_post_execution(opportunity, execution_result)
        
        assert result.success is True
        assert result.error is None
        assert "positions" in result.details
        assert "fills" in result.details
        assert "orders" in result.details
        
        # Test position verification failure
        service._verify_positions = AsyncMock(return_value=VerificationResult(
            timestamp=int(datetime.now().timestamp() * 1000),
            success=False,
            error="Position verification failed",
            details={"failed": True}
        ))
        
        result = await service._verify_post_execution(opportunity, execution_result)
        
        assert result.success is False
        assert "Position verification failed" in result.error
        
        # Test fill verification failure
        service._verify_fills = AsyncMock(return_value=VerificationResult(
            timestamp=int(datetime.now().timestamp() * 1000),
            success=False,
            error="Fill verification failed",
            details={"failed": True}
        ))
        
        result = await service._verify_post_execution(opportunity, execution_result)
        
        assert result.success is False
        assert "Fill verification failed" in result.error
        
        # Test order verification failure
        service._verify_orders = AsyncMock(return_value=VerificationResult(
            timestamp=int(datetime.now().timestamp() * 1000),
            success=False,
            error="Order verification failed",
            details={"failed": True}
        ))
        
        result = await service._verify_post_execution(opportunity, execution_result)
        
        assert result.success is False
        assert "Order verification failed" in result.error
