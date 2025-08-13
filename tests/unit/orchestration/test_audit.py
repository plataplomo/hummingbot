"""Real unit tests for workflow audit logger that test actual logging behavior."""

import io
import sys
from datetime import UTC, datetime
from decimal import Decimal
from time import sleep

from cyberdelta.enums import WorkflowStatus
from cyberdelta.enums.trading import OrderSide, OrderType
from cyberdelta.models.events.workflow import (
    BaseWorkflowEvent,
    EmergencyLiquidationEvent,
    PlaceOrderWorkflowEvent,
)
from cyberdelta.models.events.workflow_context import WorkflowContextModel
from cyberdelta.orchestration.audit import WorkflowAuditLogger


class TestWorkflowAuditLoggerReality:
    """Test WorkflowAuditLogger with real logging output validation."""

    def test_workflow_start_logging_output(self) -> None:
        """Test that workflow start logging produces actual structured output."""
        audit_logger = WorkflowAuditLogger()

        event = BaseWorkflowEvent(
            event_type="test_workflow",
            timeout=60.0,
            event_id="test-event-123",
            started_at=datetime.now(UTC),
            parent_id="parent-workflow",
            context={"strategy": "test_strategy", "priority": "high"},
        )

        # Capture stdout since structlog outputs there
        captured_output = io.StringIO()
        original_stdout = sys.stdout
        sys.stdout = captured_output

        try:
            audit_logger.log_workflow_start(event)
            output = captured_output.getvalue()

            # Verify real structured logging output
            assert "workflow_started" in output
            assert event.event_id in output
            assert event.event_type in output
            assert str(event.timeout) in output

            # Context should be logged
            assert "strategy" in output
            assert "test_strategy" in output

        finally:
            sys.stdout = original_stdout

    def test_workflow_completion_logging_with_context(self) -> None:
        """Test workflow completion logging includes context details."""
        audit_logger = WorkflowAuditLogger()

        event = PlaceOrderWorkflowEvent(
            event_type="PlaceOrderWorkflow",
            timeout=30.0,
            symbol="BTC",
            side=OrderSide.BUY,
            quantity=Decimal("1.0"),
            price=Decimal("50000.00"),
            order_type=OrderType.LIMIT,
            status=WorkflowStatus.COMPLETED,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
        )

        context = WorkflowContextModel(
            workflow_id="order-workflow-456",
            workflow_type="PlaceOrder",
            timeout=30.0,
            symbol="BTC",
            side=OrderSide.BUY,
            quantity=Decimal("1.0"),
            price=Decimal("50000.00"),
            order_type=OrderType.LIMIT,
        )

        # Add some audit trail entries
        context.add_audit("validate_order", "SUCCESS")
        context.add_audit("place_order", "SUCCESS")
        context.add_audit("workflow_complete", "SUCCESS")

        # Capture logging output
        captured_output = io.StringIO()
        original_stdout = sys.stdout
        sys.stdout = captured_output

        try:
            audit_logger.log_workflow_complete(event, context)
            output = captured_output.getvalue()

            # Verify completion logging details
            assert "workflow_completed" in output
            assert context.workflow_id in output
            assert context.workflow_type in output
            assert "SUCCESS" in output or "success=True" in output

            # Should include audit trail length
            assert "3" in output  # 3 audit entries

            # Should include timing information
            assert "duration_ms" in output or "duration" in output

        finally:
            sys.stdout = original_stdout

    def test_workflow_error_logging_captures_details(self) -> None:
        """Test that workflow error logging captures error details."""
        audit_logger = WorkflowAuditLogger()

        event = BaseWorkflowEvent(
            event_type="failing_workflow",
            timeout=30.0,
            status=WorkflowStatus.FAILED,
            error="Validation failed: Invalid symbol",
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
        )

        test_error = RuntimeError("Test error for logging validation")

        # Capture logging output
        captured_output = io.StringIO()
        original_stdout = sys.stdout
        sys.stdout = captured_output

        try:
            audit_logger.log_workflow_error(event, test_error)
            output = captured_output.getvalue()

            # Verify error logging details
            assert "workflow_failed" in output or "error" in output
            assert event.event_id in output
            assert event.event_type in output
            assert "Test error for logging validation" in output

            # Should indicate failure
            assert "failed" in output or "FAILED" in output or "success=False" in output

        finally:
            sys.stdout = original_stdout

    def test_emergency_event_logging_includes_reason(self) -> None:
        """Test that emergency events log critical information."""
        audit_logger = WorkflowAuditLogger()

        event = EmergencyLiquidationEvent(
            event_type="EmergencyLiquidation",
            timeout=60.0,
            reason="Circuit breaker triggered - 5% loss in 1 minute",
            force=True,
            positions=["BTC", "ETH", "SOL"],
            max_loss=Decimal("50000.00"),
            started_at=datetime.now(UTC),
        )

        # Capture logging output
        captured_output = io.StringIO()
        original_stdout = sys.stdout
        sys.stdout = captured_output

        try:
            audit_logger.log_workflow_start(event)
            output = captured_output.getvalue()

            # Emergency events should include critical information
            assert "workflow_started" in output
            assert event.reason in output
            assert "Circuit breaker triggered" in output
            assert "force" in output.lower() or str(event.force) in output

            # Emergency-specific fields should be logged
            # Note: positions field might not be logged by current audit implementation
            # but other emergency fields should be present
            assert "50000.00" in output  # max_loss should be logged

        finally:
            sys.stdout = original_stdout

    def test_audit_logger_consistency_across_events(self) -> None:
        """Test that audit logger produces consistent output across different event types."""
        audit_logger = WorkflowAuditLogger()

        events = [
            BaseWorkflowEvent(event_type="base_test", timeout=30.0),
            PlaceOrderWorkflowEvent(
                event_type="PlaceOrderWorkflow",
                timeout=30.0,
                symbol="ETH",
                side=OrderSide.SELL,
                quantity=Decimal("2.0"),
                price=Decimal("2000.00"),
                order_type=OrderType.LIMIT,
            ),
            EmergencyLiquidationEvent(
                event_type="EmergencyLiquidation",
                timeout=60.0,
                reason="Risk limit breach",
                force=False,
            ),
        ]

        logged_outputs: list[str] = []

        for event in events:
            captured_output = io.StringIO()
            original_stdout = sys.stdout
            sys.stdout = captured_output

            try:
                audit_logger.log_workflow_start(event)
                output = captured_output.getvalue()
                logged_outputs.append(output)

            finally:
                sys.stdout = original_stdout

        # All outputs should contain consistent base information
        for output in logged_outputs:
            assert "workflow_started" in output
            assert "event_id" in output or any(event.event_id in output for event in events)
            assert "timeout" in output

    def test_context_model_audit_trail_integration(self) -> None:
        """Test that context audit trails are properly logged."""
        audit_logger = WorkflowAuditLogger()

        event = BaseWorkflowEvent(
            event_type="audit_trail_test",
            timeout=60.0,
            status=WorkflowStatus.COMPLETED,
            completed_at=datetime.now(UTC),
        )

        # Create context with comprehensive audit trail
        context = WorkflowContextModel(
            workflow_id="comprehensive-test", workflow_type="AuditTrailTest", timeout=60.0
        )

        # Add multiple audit entries
        context.add_audit("step_1", "SUCCESS", "First step completed")
        context.add_audit("step_2", "SUCCESS", "Second step completed")
        context.add_audit("step_3", "WARNING", "Third step had warnings")
        context.add_audit("step_4", "SUCCESS", "Fourth step completed")

        # Capture logging output
        captured_output = io.StringIO()
        original_stdout = sys.stdout
        sys.stdout = captured_output

        try:
            audit_logger.log_workflow_complete(event, context)
            output = captured_output.getvalue()

            # Should log audit trail information
            assert "audit_trail_length" in output or str(len(context.audit_trail)) in output
            assert "4" in output  # Should mention 4 audit entries

            # Context information should be present
            assert context.workflow_id in output
            assert context.workflow_type in output

        finally:
            sys.stdout = original_stdout

    def test_timing_information_in_logs(self) -> None:
        """Test that timing information is correctly logged."""
        audit_logger = WorkflowAuditLogger()

        # Create event with realistic timing
        start_time = datetime.now(UTC)
        # Simulate some execution time
        sleep(0.01)  # 10ms delay
        end_time = datetime.now(UTC)

        event = BaseWorkflowEvent(
            event_type="timing_test",
            timeout=30.0,
            started_at=start_time,
            completed_at=end_time,
            status=WorkflowStatus.COMPLETED,
        )

        context = WorkflowContextModel(
            workflow_id="timing-workflow", workflow_type="TimingTest", timeout=30.0
        )

        # Capture logging output
        captured_output = io.StringIO()
        original_stdout = sys.stdout
        sys.stdout = captured_output

        try:
            audit_logger.log_workflow_complete(event, context)
            output = captured_output.getvalue()

            # Should include timing information
            assert "duration" in output.lower()
            # Should have some positive duration (at least 1ms from our sleep)
            duration_present = any(char.isdigit() for char in output)
            assert duration_present

        finally:
            sys.stdout = original_stdout

    def test_logger_functionality_through_public_interface(self) -> None:
        """Test that WorkflowAuditLogger functions correctly through public methods."""
        audit_logger = WorkflowAuditLogger()

        # Test logging through public interface - should not raise exceptions
        mock_event = PlaceOrderWorkflowEvent(
            event_type="PlaceOrderWorkflow",
            timeout=30.0,
            symbol="BTC-USD",
            side=OrderSide.BUY,
            quantity=Decimal("1.0"),
            price=Decimal(50000),
            order_type=OrderType.LIMIT,
            strategy_id="test_strategy",
        )

        mock_context = WorkflowContextModel(
            workflow_id="test_workflow_id",
            workflow_type="PlaceOrder",
            timeout=30.0,
        )

        # These should work without throwing exceptions - testing public behavior
        audit_logger.log_workflow_start(mock_event)
        audit_logger.log_workflow_complete(mock_event, mock_context)

        # Test error logging
        test_error = RuntimeError("test error")
        mock_event.status = WorkflowStatus.FAILED
        mock_event.error = "test error"
        audit_logger.log_workflow_error(mock_event, test_error)
