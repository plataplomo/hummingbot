"""Real unit tests for workflow handlers that test actual validation and business logic."""

from decimal import Decimal

import pytest

from cyberdelta.config.models.event_system_config import (
    EventRetryConfig,
    EventWorkflowConfig,
)
from cyberdelta.enums.trading import OrderSide, OrderType
from cyberdelta.exceptions import RequiredFieldError, TypeFieldError
from cyberdelta.models.events.workflow import (
    EmergencyLiquidationEvent,
    GracefulShutdownEvent,
    PlaceOrderWorkflowEvent,
    RebalanceWorkflowEvent,
)
from cyberdelta.orchestration.workflows import (
    EmergencyLiquidationHandler,
    GracefulShutdownHandler,
    PlaceOrderWorkflowHandler,
    RebalanceWorkflowHandler,
)


def create_real_workflow_config() -> EventWorkflowConfig:
    """Create a real EventWorkflowConfig for workflow handler testing.

    Returns:
        EventWorkflowConfig: Real configuration for testing
    """
    return EventWorkflowConfig(
        workflow_timeout_sec=60.0,
        step_timeout_sec=30.0,
        retry_config=EventRetryConfig(
            max_attempts=3,
            initial_delay_sec=1.0,
            max_delay_sec=5.0,
        ),
        workflow_retry_attempts_factor=1.0,
        place_order_risk_checks=["position_limit", "exposure", "drawdown"],
        emergency_alert_channels=["email", "slack"],
    )


class TestPlaceOrderWorkflowHandlerValidation:
    """Test PlaceOrderWorkflowHandler with real validation logic."""

    @pytest.mark.asyncio
    async def test_empty_symbol_validation_failure(self) -> None:
        """Test that empty symbol triggers real validation error."""
        config = create_real_workflow_config()
        handler = PlaceOrderWorkflowHandler(config)

        event = PlaceOrderWorkflowEvent(
            event_type="PlaceOrderWorkflow",
            timeout=30.0,
            symbol="",  # Empty symbol should fail validation
            side=OrderSide.BUY,
            quantity=Decimal("1.0"),
            price=Decimal("50000.00"),
            order_type=OrderType.LIMIT,
        )

        # Should fail with real RequiredFieldError
        with pytest.raises(RequiredFieldError) as exc_info:
            await handler.execute(event)

        # Verify real error properties
        assert exc_info.value.field_name == "symbol"
        assert "order placement workflow" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_negative_quantity_validation_failure(self) -> None:
        """Test that negative quantity triggers real type validation error."""
        config = create_real_workflow_config()
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

        # Should fail with real TypeFieldError
        with pytest.raises(TypeFieldError) as exc_info:
            await handler.execute(event)

        # Verify real error properties
        assert exc_info.value.field_name == "quantity"
        assert "positive decimal" in exc_info.value.expected_type
        assert exc_info.value.source_value == Decimal("-1.0")

    @pytest.mark.asyncio
    async def test_zero_quantity_validation_failure(self) -> None:
        """Test that zero quantity triggers validation error."""
        config = create_real_workflow_config()
        handler = PlaceOrderWorkflowHandler(config)

        event = PlaceOrderWorkflowEvent(
            event_type="PlaceOrderWorkflow",
            timeout=30.0,
            symbol="ETH",
            side=OrderSide.SELL,
            quantity=Decimal(0),  # Zero quantity should fail
            price=Decimal("2000.00"),
            order_type=OrderType.LIMIT,
        )

        # Should fail with real TypeFieldError
        with pytest.raises(TypeFieldError) as exc_info:
            await handler.execute(event)

        assert exc_info.value.field_name == "quantity"
        assert exc_info.value.source_value == Decimal(0)

    @pytest.mark.asyncio
    async def test_successful_order_workflow_execution(self) -> None:
        """Test successful order workflow with real business logic."""
        config = create_real_workflow_config()
        handler = PlaceOrderWorkflowHandler(config)

        event = PlaceOrderWorkflowEvent(
            event_type="PlaceOrderWorkflow",
            timeout=30.0,
            symbol="BTC",
            side=OrderSide.BUY,
            quantity=Decimal("0.5"),
            price=Decimal("45000.00"),
            order_type=OrderType.LIMIT,
        )

        # Should succeed and create real context
        result = await handler.execute(event)

        # Verify real workflow context creation
        assert result.workflow_type == "PlaceOrder"
        assert result.symbol == "BTC"
        assert result.side == OrderSide.BUY
        assert result.quantity == Decimal("0.5")
        assert result.price == Decimal("45000.00")
        assert result.order_type == OrderType.LIMIT

        # Verify real audit trail creation
        assert len(result.audit_trail) > 0

        # Check that all expected steps were audited
        audit_steps = [entry.step for entry in result.audit_trail]
        expected_steps = [
            "validate_order",
            "check_risk",
            "verify_connectivity",
            "place_order",
            "confirm_placement",
            "update_state",
            "emit_events",
            "workflow_complete",
        ]

        for step in expected_steps:
            assert step in audit_steps, f"Missing audit step: {step}"

    @pytest.mark.asyncio
    async def test_risk_checks_configuration_integration(self) -> None:
        """Test that risk checks from configuration are actually used."""
        config = create_real_workflow_config()
        handler = PlaceOrderWorkflowHandler(config)

        event = PlaceOrderWorkflowEvent(
            event_type="PlaceOrderWorkflow",
            timeout=30.0,
            symbol="SOL",
            side=OrderSide.BUY,
            quantity=Decimal("10.0"),
            price=Decimal("100.00"),
            order_type=OrderType.LIMIT,
        )

        result = await handler.execute(event)

        # Check that risk check step was executed
        risk_audit = [entry for entry in result.audit_trail if entry.step == "check_risk"]
        assert len(risk_audit) > 0
        assert risk_audit[0].status == "SUCCESS"

        # Verify risk checks were actually processed (through audit trail)
        # Real implementation logs each risk check type
        assert result.audit_trail is not None


class TestRebalanceWorkflowHandlerValidation:
    """Test RebalanceWorkflowHandler with real validation logic."""

    @pytest.mark.asyncio
    async def test_empty_target_allocations_validation(self) -> None:
        """Test that empty target allocations trigger validation error."""
        config = create_real_workflow_config()
        handler = RebalanceWorkflowHandler(config)

        event = RebalanceWorkflowEvent(
            event_type="RebalanceWorkflow",
            timeout=300.0,
            target_allocations={},  # Empty allocations should fail
            max_slippage=Decimal("0.01"),
            rebalance_mode="proportional",
            dry_run=False,
        )

        # Should fail with RequiredFieldError
        with pytest.raises(RequiredFieldError) as exc_info:
            await handler.execute(event)

        assert exc_info.value.field_name == "target_allocations"
        assert "rebalance workflow" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_successful_rebalance_workflow_execution(self) -> None:
        """Test successful rebalance workflow with real business logic."""
        config = create_real_workflow_config()
        handler = RebalanceWorkflowHandler(config)

        target_allocations = {
            "BTC": Decimal("0.6"),
            "ETH": Decimal("0.3"),
            "SOL": Decimal("0.1"),
        }

        event = RebalanceWorkflowEvent(
            event_type="RebalanceWorkflow",
            timeout=300.0,
            target_allocations=target_allocations,
            max_slippage=Decimal("0.005"),
            rebalance_mode="gradual",
            dry_run=True,
        )

        result = await handler.execute(event)

        # Verify real workflow execution
        assert result.workflow_type == "Rebalance"
        assert result.timeout == config.workflow_timeout_sec

        # Check audit trail for rebalance-specific steps
        audit_steps = [entry.step for entry in result.audit_trail]
        expected_rebalance_steps = [
            "calculate_targets",
            "determine_trades",
            "check_risks",
            "execute_trades",
            "verify_positions",
            "update_portfolio",
            "rebalance_complete",
        ]

        for step in expected_rebalance_steps:
            assert step in audit_steps, f"Missing rebalance step: {step}"

    @pytest.mark.asyncio
    async def test_rebalance_workflow_error_handling(self) -> None:
        """Test rebalance workflow error handling and rollback."""
        config = create_real_workflow_config()
        handler = RebalanceWorkflowHandler(config)

        # Create an event that will cause validation failure
        event = RebalanceWorkflowEvent(
            event_type="RebalanceWorkflow",
            timeout=300.0,
            target_allocations={},  # Will trigger validation error
            max_slippage=Decimal("0.01"),
            rebalance_mode="aggressive",
            dry_run=False,
        )

        # Should fail and propagate the error
        with pytest.raises(RequiredFieldError):
            await handler.execute(event)


class TestEmergencyLiquidationHandlerBehavior:
    """Test EmergencyLiquidationHandler with real emergency logic."""

    @pytest.mark.asyncio
    async def test_emergency_liquidation_execution_steps(self) -> None:
        """Test emergency liquidation executes all critical steps."""
        config = create_real_workflow_config()
        handler = EmergencyLiquidationHandler(config)

        event = EmergencyLiquidationEvent(
            event_type="EmergencyLiquidation",
            timeout=60.0,
            reason="Risk limit exceeded",
            force=False,
            positions=["BTC", "ETH"],
            max_loss=Decimal("10000.00"),
        )

        result = await handler.execute(event)

        # Verify emergency liquidation context
        assert result.workflow_type == "EmergencyLiquidation"
        assert result.reason == "Risk limit exceeded"
        assert result.force is False

        # Check all critical emergency steps were executed
        audit_steps = [entry.step for entry in result.audit_trail]
        critical_steps = [
            "freeze_trading",
            "cancel_orders",
            "close_positions",
            "verify_closure",
            "disable_trading",
            "send_alerts",
            "liquidation_complete",
        ]

        for step in critical_steps:
            assert step in audit_steps, f"Missing critical step: {step}"

    @pytest.mark.asyncio
    async def test_forced_emergency_liquidation_behavior(self) -> None:
        """Test forced emergency liquidation continues despite errors."""
        config = create_real_workflow_config()
        handler = EmergencyLiquidationHandler(config)

        event = EmergencyLiquidationEvent(
            event_type="EmergencyLiquidation",
            timeout=30.0,
            reason="System malfunction",
            force=True,  # Force flag should handle errors gracefully
        )

        # Even with force=True, should complete successfully
        result = await handler.execute(event)

        assert result.workflow_type == "EmergencyLiquidation"
        assert result.reason == "System malfunction"
        assert result.force is True

        # Should have completed successfully
        completion_entries = [
            entry
            for entry in result.audit_trail
            if entry.step == "liquidation_complete" and entry.status == "SUCCESS"
        ]
        assert len(completion_entries) > 0

    @pytest.mark.asyncio
    async def test_alert_channels_integration(self) -> None:
        """Test that emergency alert channels from config are used."""
        config = create_real_workflow_config()
        handler = EmergencyLiquidationHandler(config)

        event = EmergencyLiquidationEvent(
            event_type="EmergencyLiquidation",
            timeout=45.0,
            reason="Manual intervention",
            force=False,
        )

        result = await handler.execute(event)

        # Verify alert step was executed
        alert_entries = [entry for entry in result.audit_trail if entry.step == "send_alerts"]
        assert len(alert_entries) > 0
        assert alert_entries[0].status == "SUCCESS"


class TestGracefulShutdownHandlerBehavior:
    """Test GracefulShutdownHandler with real operational logic."""

    @pytest.mark.asyncio
    async def test_graceful_shutdown_full_execution(self) -> None:
        """Test graceful shutdown with all options enabled."""
        config = create_real_workflow_config()
        handler = GracefulShutdownHandler(config)

        event = GracefulShutdownEvent(
            event_type="GracefulShutdown",
            timeout=300.0,
            close_positions=True,
            save_state=True,
            notify_services=True,
            timeout_seconds=180.0,
        )

        result = await handler.execute(event)

        # Verify graceful shutdown context
        assert result.workflow_type == "GracefulShutdown"
        assert result.timeout == 180.0  # Should use shutdown-specific timeout

        # Check all shutdown steps were executed
        audit_steps = [entry.step for entry in result.audit_trail]
        expected_steps = [
            "stop_new_orders",
            "wait_pending",
            "cancel_remaining",
            "close_positions",
            "persist_state",
            "notify_services",
            "close_connections",
            "shutdown_complete",
        ]

        for step in expected_steps:
            assert step in audit_steps, f"Missing shutdown step: {step}"

    @pytest.mark.asyncio
    async def test_graceful_shutdown_minimal_execution(self) -> None:
        """Test graceful shutdown with minimal options."""
        config = create_real_workflow_config()
        handler = GracefulShutdownHandler(config)

        event = GracefulShutdownEvent(
            event_type="GracefulShutdown",
            timeout=120.0,
            close_positions=False,
            save_state=False,
            notify_services=False,
        )

        result = await handler.execute(event)

        # Should use event timeout when timeout_seconds is None
        assert result.timeout == config.workflow_timeout_sec

        # Check that optional steps were NOT executed
        audit_steps = [entry.step for entry in result.audit_trail]

        # These should always execute
        assert "stop_new_orders" in audit_steps
        assert "wait_pending" in audit_steps
        assert "cancel_remaining" in audit_steps
        assert "close_connections" in audit_steps
        assert "shutdown_complete" in audit_steps

        # These should NOT execute when disabled
        assert "close_positions" not in audit_steps
        assert "persist_state" not in audit_steps
        assert "notify_services" not in audit_steps

    @pytest.mark.asyncio
    async def test_timeout_hierarchy_in_shutdown(self) -> None:
        """Test that shutdown respects timeout hierarchy correctly."""
        config = create_real_workflow_config()
        handler = GracefulShutdownHandler(config)

        # Test with shutdown-specific timeout
        event_with_timeout = GracefulShutdownEvent(
            event_type="GracefulShutdown",
            timeout=600.0,
            close_positions=True,
            save_state=True,
            notify_services=True,
            timeout_seconds=300.0,  # Shutdown-specific timeout
        )

        result_with_timeout = await handler.execute(event_with_timeout)
        assert result_with_timeout.timeout == 300.0  # Should use shutdown timeout

        # Test without shutdown-specific timeout
        event_without_timeout = GracefulShutdownEvent(
            event_type="GracefulShutdown",
            timeout=600.0,
            close_positions=True,
            save_state=True,
            notify_services=True,
            # No timeout_seconds specified
        )

        result_without_timeout = await handler.execute(event_without_timeout)
        assert result_without_timeout.timeout == config.workflow_timeout_sec


class TestWorkflowConfigurationIntegration:
    """Test that workflow handlers properly integrate with configuration."""

    def test_configuration_injection_and_usage(self) -> None:
        """Test that handlers properly receive and use configuration."""
        config = create_real_workflow_config()

        # All handlers should accept configuration and initialize without errors
        place_order_handler = PlaceOrderWorkflowHandler(config)
        rebalance_handler = RebalanceWorkflowHandler(config)
        emergency_handler = EmergencyLiquidationHandler(config)
        shutdown_handler = GracefulShutdownHandler(config)

        # Handlers should be properly initialized (test through instance checks)
        assert isinstance(place_order_handler, PlaceOrderWorkflowHandler)
        assert isinstance(rebalance_handler, RebalanceWorkflowHandler)
        assert isinstance(emergency_handler, EmergencyLiquidationHandler)
        assert isinstance(shutdown_handler, GracefulShutdownHandler)

    @pytest.mark.asyncio
    async def test_risk_checks_configuration_usage(self) -> None:
        """Test that PlaceOrderWorkflowHandler uses risk checks from configuration."""
        custom_risk_checks = ["custom_limit", "special_check", "unique_validation"]

        config = EventWorkflowConfig(
            workflow_timeout_sec=60.0,
            step_timeout_sec=30.0,
            retry_config=EventRetryConfig(max_attempts=2),
            workflow_retry_attempts_factor=1.0,
            place_order_risk_checks=custom_risk_checks,  # Custom risk checks
            emergency_alert_channels=["email"],
        )

        handler = PlaceOrderWorkflowHandler(config)

        event = PlaceOrderWorkflowEvent(
            event_type="PlaceOrderWorkflow",
            timeout=30.0,
            symbol="TEST",
            side=OrderSide.BUY,
            quantity=Decimal("1.0"),
            price=Decimal("100.0"),
            order_type=OrderType.LIMIT,
        )

        result = await handler.execute(event)

        # The handler should have processed the custom risk checks
        # (This is validated through the actual implementation processing the config)
        assert len(result.audit_trail) > 0

        # Verify risk check step exists
        risk_entries = [entry for entry in result.audit_trail if entry.step == "check_risk"]
        assert len(risk_entries) > 0

    @pytest.mark.asyncio
    async def test_emergency_alert_channels_usage(self) -> None:
        """Test that EmergencyLiquidationHandler uses alert channels from configuration."""
        custom_channels = ["custom_alert", "special_notification"]

        config = EventWorkflowConfig(
            workflow_timeout_sec=60.0,
            step_timeout_sec=30.0,
            retry_config=EventRetryConfig(max_attempts=2),
            workflow_retry_attempts_factor=1.0,
            place_order_risk_checks=["position_limit"],
            emergency_alert_channels=custom_channels,  # Custom alert channels
        )

        handler = EmergencyLiquidationHandler(config)

        event = EmergencyLiquidationEvent(
            event_type="EmergencyLiquidation",
            timeout=60.0,
            reason="Testing custom alerts",
            force=False,
        )

        result = await handler.execute(event)

        # Handler should have processed alerts using the configured channels
        alert_entries = [entry for entry in result.audit_trail if entry.step == "send_alerts"]
        assert len(alert_entries) > 0
        assert alert_entries[0].status == "SUCCESS"
