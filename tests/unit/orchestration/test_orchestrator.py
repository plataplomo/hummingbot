"""Real unit tests for workflow orchestrator that test actual business logic."""

import asyncio
import contextlib
from decimal import Decimal

import pytest

from cyberdelta.config.models.event_system_config import (
    EventRetryConfig,
    EventWorkflowConfig,
)
from cyberdelta.enums import WorkflowStatus
from cyberdelta.enums.trading import OrderSide, OrderType
from cyberdelta.exceptions import ServiceValidationError
from cyberdelta.models.events.workflow import BaseWorkflowEvent, PlaceOrderWorkflowEvent
from cyberdelta.models.events.workflow_context import WorkflowContextModel
from cyberdelta.orchestration.orchestrator import WorkflowOrchestrator
from cyberdelta.orchestration.workflows import PlaceOrderWorkflowHandler


def create_real_config() -> EventWorkflowConfig:
    """Create a real EventWorkflowConfig for testing.

    Returns:
        EventWorkflowConfig: Configuration for testing workflows
    """
    return EventWorkflowConfig(
        workflow_timeout_sec=60.0,
        step_timeout_sec=30.0,
        retry_config=EventRetryConfig(
            max_attempts=2,
            initial_delay_sec=0.1,
            max_delay_sec=1.0,
        ),
        workflow_retry_attempts_factor=1.0,
        place_order_risk_checks=["position_limit", "exposure"],
        emergency_alert_channels=["email"],
    )


class RealWorkflowHandler:
    """Real workflow handler that actually does validation."""

    def __init__(self, should_fail: bool = False) -> None:
        """Initialize real workflow handler.

        Args:
            should_fail: Whether handler should fail during execution
        """
        self.should_fail = should_fail
        self.executed = False

    async def execute(self, event: BaseWorkflowEvent) -> WorkflowContextModel:
        """Execute with real validation.

        Args:
            event: Workflow event to execute

        Returns:
            WorkflowContextModel: Context with execution results

        Raises:
            RuntimeError: If should_fail is True
        """
        self.executed = True

        if self.should_fail:
            raise RuntimeError("Handler intentionally failed")

        # Create real context with proper validation
        return WorkflowContextModel(
            workflow_id="real-workflow-123",
            workflow_type="RealWorkflow",
            timeout=event.timeout,
        )


class TestWorkflowOrchestratorReal:
    """Test WorkflowOrchestrator with real business logic."""

    def test_orchestrator_initialization_with_real_config(self) -> None:
        """Test orchestrator initializes with real configuration."""
        config = create_real_config()
        orchestrator = WorkflowOrchestrator(config)

        # Test actual public interface
        assert orchestrator.get_registered_handlers() == []
        assert orchestrator.get_active_workflows() == []

    def test_register_and_retrieve_handler(self) -> None:
        """Test handler registration through public interface."""
        config = create_real_config()
        orchestrator = WorkflowOrchestrator(config)
        handler = RealWorkflowHandler()

        # Register through public interface
        orchestrator.register_handler("test_event", handler)

        # Verify through public interface
        registered_types = orchestrator.get_registered_handlers()
        assert "test_event" in registered_types

    @pytest.mark.asyncio
    async def test_execute_workflow_with_real_handler(self) -> None:
        """Test workflow execution with real handler logic."""
        config = create_real_config()
        orchestrator = WorkflowOrchestrator(config)
        handler = RealWorkflowHandler()

        orchestrator.register_handler("real_workflow", handler)

        event = BaseWorkflowEvent(
            event_type="real_workflow",
            timeout=30.0,
        )

        # Execute and test real results
        result = await orchestrator.execute_workflow(event)

        # Verify real execution happened
        assert handler.executed is True
        assert result.workflow_id == "real-workflow-123"
        assert result.workflow_type == "RealWorkflow"
        assert result.timeout == event.timeout

        # Verify event state was really updated
        assert event.status == WorkflowStatus.COMPLETED
        assert event.started_at is not None
        assert event.completed_at is not None

    @pytest.mark.asyncio
    async def test_execute_workflow_no_handler_real_error(self) -> None:
        """Test real error when no handler is registered."""
        config = create_real_config()
        orchestrator = WorkflowOrchestrator(config)

        event = BaseWorkflowEvent(
            event_type="nonexistent_workflow",
            timeout=30.0,
        )

        # Test real ServiceValidationError behavior
        with pytest.raises(ServiceValidationError) as exc_info:
            await orchestrator.execute_workflow(event)

        # Verify real exception properties
        assert exc_info.value.field_name == "event_type"
        assert exc_info.value.source_value == "nonexistent_workflow"
        assert "Handler" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_execute_workflow_handler_failure_propagation(self) -> None:
        """Test real error propagation from handler."""
        config = create_real_config()
        orchestrator = WorkflowOrchestrator(config)
        failing_handler = RealWorkflowHandler(should_fail=True)

        orchestrator.register_handler("failing_workflow", failing_handler)

        event = BaseWorkflowEvent(
            event_type="failing_workflow",
            timeout=30.0,
        )

        # Test real error propagation
        with pytest.raises(RuntimeError, match="Handler intentionally failed"):
            await orchestrator.execute_workflow(event)

        # Verify handler actually executed
        assert failing_handler.executed is True

        # Verify event status reflects real failure
        assert event.status == WorkflowStatus.FAILED
        assert event.error == "Handler intentionally failed"
        assert event.completed_at is not None

    @pytest.mark.asyncio
    async def test_active_workflows_real_tracking(self) -> None:
        """Test real active workflow tracking during execution."""
        config = create_real_config()
        orchestrator = WorkflowOrchestrator(config)

        # Handler that allows us to inspect active workflows during execution
        class InspectingHandler:
            def __init__(self) -> None:
                self.active_count_during_execution: int = 0

            async def execute(self, event: BaseWorkflowEvent) -> WorkflowContextModel:
                # Check active workflows during real execution
                active = orchestrator.get_active_workflows()
                self.active_count_during_execution = len(active)

                return WorkflowContextModel(
                    workflow_id="inspect-123",
                    workflow_type="InspectWorkflow",
                    timeout=event.timeout,
                )

        handler = InspectingHandler()
        orchestrator.register_handler("inspect_workflow", handler)

        event = BaseWorkflowEvent(
            event_type="inspect_workflow",
            timeout=30.0,
        )

        # Execute and verify real tracking
        await orchestrator.execute_workflow(event)

        # Verify workflow was tracked during execution
        assert handler.active_count_during_execution == 1

        # Verify cleanup after completion
        assert len(orchestrator.get_active_workflows()) == 0

    @pytest.mark.timing
    @pytest.mark.asyncio
    async def test_cancel_workflow_real_behavior(self) -> None:
        """Test real workflow cancellation."""
        config = create_real_config()
        orchestrator = WorkflowOrchestrator(config)

        # Handler that will be slow so we can cancel it
        class SlowHandler:
            async def execute(self, event: BaseWorkflowEvent) -> WorkflowContextModel:
                await asyncio.sleep(0.1)  # Simulate work
                return WorkflowContextModel(
                    workflow_id="slow-123",
                    workflow_type="SlowWorkflow",
                    timeout=event.timeout,
                )

        handler = SlowHandler()
        orchestrator.register_handler("slow_workflow", handler)

        event = BaseWorkflowEvent(
            event_type="slow_workflow",
            timeout=60.0,
        )

        # Start execution and cancel during execution
        execution_task = asyncio.create_task(orchestrator.execute_workflow(event))

        # Give it time to start
        await asyncio.sleep(0.01)

        # Cancel the workflow
        was_cancelled = await orchestrator.cancel_workflow(event.event_id)

        # Verify real cancellation happened
        assert was_cancelled is True
        assert event.status == WorkflowStatus.CANCELLED
        assert event.error == "Cancelled by request"
        assert event.completed_at is not None

        # Clean up
        execution_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await execution_task

    @pytest.mark.timing
    @pytest.mark.asyncio
    async def test_shutdown_with_real_active_workflows(self) -> None:
        """Test real shutdown behavior with active workflows."""
        config = create_real_config()
        orchestrator = WorkflowOrchestrator(config)

        # Create handlers that will run long enough to be cancelled
        class LongRunningHandler:
            async def execute(self, event: BaseWorkflowEvent) -> WorkflowContextModel:
                # Simulate long-running work that can be interrupted
                await asyncio.sleep(10.0)  # Long enough to be cancelled
                return WorkflowContextModel(
                    workflow_id="long-running-123",
                    workflow_type="LongRunning",
                    timeout=event.timeout,
                )

        handler1 = LongRunningHandler()
        handler2 = LongRunningHandler()

        orchestrator.register_handler("workflow1", handler1)
        orchestrator.register_handler("workflow2", handler2)

        # Create events
        event1 = BaseWorkflowEvent(event_type="workflow1", timeout=60.0)
        event2 = BaseWorkflowEvent(event_type="workflow2", timeout=60.0)

        # Start workflows without waiting for completion to simulate active state
        task1 = asyncio.create_task(orchestrator.execute_workflow(event1))
        task2 = asyncio.create_task(orchestrator.execute_workflow(event2))

        # Give time for workflows to start and register as active
        await asyncio.sleep(0.05)

        # Verify they're tracked as active
        active_workflows = orchestrator.get_active_workflows()
        assert len(active_workflows) == 2

        # Test real shutdown
        await orchestrator.shutdown()

        # Verify real cancellation occurred
        assert event1.status == WorkflowStatus.CANCELLED
        assert event2.status == WorkflowStatus.CANCELLED
        assert event1.error == "Cancelled by request"
        assert event2.error == "Cancelled by request"

        # Verify registry cleared
        assert orchestrator.get_registered_handlers() == []

        # Clean up tasks (they should already be cancelled by shutdown)
        with contextlib.suppress(asyncio.CancelledError):
            await task1
        with contextlib.suppress(asyncio.CancelledError):
            await task2


class TestRealPlaceOrderWorkflowHandler:
    """Test PlaceOrderWorkflowHandler with real validation logic."""

    def test_handler_initialization_with_real_config(self) -> None:
        """Test handler initializes with real configuration."""
        config = create_real_config()
        handler = PlaceOrderWorkflowHandler(config)

        # Verify real initialization (through public interface testing)
        assert handler is not None

    @pytest.mark.asyncio
    async def test_execute_with_real_validation_success(self) -> None:
        """Test successful execution with real validation."""
        config = create_real_config()
        handler = PlaceOrderWorkflowHandler(config)

        event = PlaceOrderWorkflowEvent(
            event_type="PlaceOrderWorkflow",
            timeout=30.0,
            symbol="BTC",
            side=OrderSide.BUY,
            quantity=Decimal("1.0"),
            price=Decimal("50000.00"),
            order_type=OrderType.LIMIT,
        )

        # Execute and test real validation
        result = await handler.execute(event)

        # Verify real context was created with proper data
        assert result.workflow_type == "PlaceOrder"
        assert result.symbol == event.symbol
        assert result.side == event.side
        assert result.quantity == event.quantity
        assert result.price == event.price
        assert result.order_type == event.order_type

        # Verify real audit trail was created
        assert len(result.audit_trail) > 0
        assert any("workflow_complete" in entry.step for entry in result.audit_trail)

    @pytest.mark.asyncio
    async def test_execute_with_real_empty_symbol_validation(self) -> None:
        """Test real validation failure for empty symbol."""
        config = create_real_config()
        handler = PlaceOrderWorkflowHandler(config)

        event = PlaceOrderWorkflowEvent(
            event_type="PlaceOrderWorkflow",
            timeout=30.0,
            symbol="",  # Empty symbol should fail
            side=OrderSide.BUY,
            quantity=Decimal("1.0"),
            price=Decimal("50000.00"),
            order_type=OrderType.LIMIT,
        )

        # Test real validation error
        with pytest.raises(Exception) as exc_info:
            await handler.execute(event)

        # Should be caught and re-raised from handler logic
        assert "symbol" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_execute_with_real_negative_quantity_validation(self) -> None:
        """Test real validation failure for negative quantity."""
        config = create_real_config()
        handler = PlaceOrderWorkflowHandler(config)

        event = PlaceOrderWorkflowEvent(
            event_type="PlaceOrderWorkflow",
            timeout=30.0,
            symbol="BTC",
            side=OrderSide.BUY,
            quantity=Decimal("-1.0"),  # Negative quantity should fail
            price=Decimal("50000.00"),
            order_type=OrderType.LIMIT,
        )

        # Test real validation error propagates
        with pytest.raises(Exception) as exc_info:
            await handler.execute(event)

        # Should be caught and re-raised from handler logic
        assert "quantity" in str(exc_info.value).lower()
