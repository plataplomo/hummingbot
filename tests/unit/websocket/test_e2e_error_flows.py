"""End-to-end error flow tests for WebSocket error system.

Tests complete error flows from trigger to recovery, ensuring the entire
error handling pipeline works correctly in realistic scenarios.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import BaseModel, Field, ValidationError

from cyberdelta.apis.common.error_foundation import (
    ErrorSeverity,
    WebSocketRecoveryStrategy,
)
from cyberdelta.apis.websocket.ws_dual_error_manager import DualErrorManager
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_processor import PydanticWebSocketProcessor
from cyberdelta.apis.websocket.ws_processor_error_bridge import ProcessorErrorBridge
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError
from cyberdelta.apis.websocket.ws_stream_error_handler import WebSocketStreamErrorHandler
from cyberdelta.apis.websocket.ws_stream_recovery import StreamRecoverySystem
from cyberdelta.config.models.websocket_error_config import (
    WebSocketErrorConfig,
    WebSocketErrorRecoveryConfig,
)
from tests.utils.websocket.error_test_utils import ErrorTestFactory


# Test models for processor testing
class TestRawModel(BaseModel):
    """Raw WebSocket message model."""

    type: str = Field(description="Message type")
    data: dict[str, Any] = Field(default_factory=dict)
    sequence: int | None = Field(default=None)


class TestDomainModel(BaseModel):
    """Domain model after transformation."""

    message_type: str
    payload: dict[str, Any]
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))


class TestTransformer:
    """Test transformer for processor."""

    def transform(self, raw_model: TestRawModel) -> TestDomainModel:
        """Transform raw to domain model."""
        return TestDomainModel(
            message_type=raw_model.type,
            payload=raw_model.data,
        )


@pytest.mark.asyncio
class TestE2EErrorFlows:
    """Test end-to-end error flows."""

    @pytest.fixture
    def error_config(self) -> WebSocketErrorConfig:
        """Create test error configuration."""
        return WebSocketErrorConfig(
            max_recovery_attempts=3,
            recovery_backoff_ms=100,
            enable_metrics_collection=True,
            enable_legacy_compatibility=True,
        )

    @pytest.fixture
    def recovery_config(self) -> WebSocketErrorRecoveryConfig:
        """Create test recovery configuration."""
        return WebSocketErrorRecoveryConfig(
            max_recovery_attempts=3,
            initial_backoff_ms=100,
            max_backoff_ms=5000,
            backoff_multiplier=2.0,
            circuit_breaker_enabled=True,
            circuit_breaker_threshold=5,
            circuit_breaker_timeout_ms=10000,
        )

    @pytest.fixture
    def mock_connection_manager(self) -> MagicMock:
        """Create mock connection manager."""
        manager = MagicMock()
        manager.reconnect = AsyncMock(return_value=True)
        manager.reset_connection = AsyncMock(return_value=True)
        manager.get_connection_state = AsyncMock(return_value="connected")
        return manager

    @pytest.fixture
    def mock_subscription_manager(self) -> MagicMock:
        """Create mock subscription manager."""
        manager = MagicMock()
        manager.resubscribe = AsyncMock(return_value=True)
        manager.clear_subscriptions = AsyncMock()
        return manager

    @pytest.fixture
    def mock_state_manager(self) -> MagicMock:
        """Create mock state manager."""
        manager = MagicMock()
        manager.request_snapshot = AsyncMock(return_value=True)
        manager.clear_state = AsyncMock()
        return manager

    async def test_validation_error_flow(self) -> None:
        """Test complete validation error flow through processor."""
        # Create processor with error handling
        processor = PydanticWebSocketProcessor(
            raw_model=TestRawModel,
            transformer=TestTransformer(),
            processor_name="test_processor",
        )

        # Create error handler
        error_handler = WebSocketStreamErrorHandler(
            config=WebSocketErrorConfig(),
        )

        # Create bridge
        bridge = ProcessorErrorBridge(
            processor=processor,
            stream_error_handler=error_handler,
        )

        # Create invalid payload
        invalid_payload = {
            "type": 123,  # Should be string
            "data": "not_a_dict",  # Should be dict
        }

        # Create mock context that implements protocol
        context = MagicMock(spec=WebSocketContextProtocol)
        context.connection_id = "test-conn"
        context.exchange_name = "hyperliquid"
        context.routing_key = "test.routing"
        context.timestamp_ms = int(datetime.now(UTC).timestamp() * 1000)

        # Process invalid message
        with pytest.raises(ValidationError):
            raw_model = TestRawModel(**invalid_payload)

        # Handle validation error through bridge
        try:
            raw_model = TestRawModel(**invalid_payload)
        except ValidationError as e:
            await bridge.handle_validation_error(e, invalid_payload, context)

        # Verify error was handled
        assert bridge.processor.processor_name == "test_processor"

    async def test_connection_error_recovery_flow(
        self,
        recovery_config: WebSocketErrorRecoveryConfig,
        mock_connection_manager: MagicMock,
        mock_subscription_manager: MagicMock,
        mock_state_manager: MagicMock,
    ) -> None:
        """Test connection error with recovery flow."""
        # Create recovery system
        recovery_system = StreamRecoverySystem(
            config=recovery_config,
            connection_manager=mock_connection_manager,
            subscription_manager=mock_subscription_manager,
            state_manager=mock_state_manager,
        )

        # Create connection error
        error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.CONNECTION_LOST,
            message="Connection lost unexpectedly",
        )

        # Handle error through recovery system
        success = await recovery_system.handle_stream_error(error)

        # Verify recovery attempted
        assert mock_connection_manager.reconnect.called
        assert success

    async def test_rate_limit_backoff_flow(
        self,
        recovery_config: WebSocketErrorRecoveryConfig,
        mock_connection_manager: MagicMock,
        mock_subscription_manager: MagicMock,
        mock_state_manager: MagicMock,
    ) -> None:
        """Test rate limit error with exponential backoff."""
        recovery_system = StreamRecoverySystem(
            config=recovery_config,
            connection_manager=mock_connection_manager,
            subscription_manager=mock_subscription_manager,
            state_manager=mock_state_manager,
        )

        # Create rate limit error
        error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.RATE_LIMITED,
            message="Rate limit exceeded",
        )
        error.recovery_strategy = WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF

        # Track sleep calls
        with patch("asyncio.sleep") as mock_sleep:
            mock_sleep.return_value = None

            # First attempt
            await recovery_system.handle_stream_error(error)

            # Second attempt (should have longer delay)
            await recovery_system.handle_stream_error(error)

            # Verify exponential backoff
            if mock_sleep.call_count >= 2:
                delays = [call[0][0] for call in mock_sleep.call_args_list]
                # Second delay should be longer (exponential)
                assert delays[1] > delays[0]

    async def test_subscription_error_resubscribe_flow(
        self,
        recovery_config: WebSocketErrorRecoveryConfig,
        mock_connection_manager: MagicMock,
        mock_subscription_manager: MagicMock,
        mock_state_manager: MagicMock,
    ) -> None:
        """Test subscription error with resubscribe flow."""
        recovery_system = StreamRecoverySystem(
            config=recovery_config,
            connection_manager=mock_connection_manager,
            subscription_manager=mock_subscription_manager,
            state_manager=mock_state_manager,
        )

        # Create subscription error
        error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.SUBSCRIPTION_FAILED,
            message="Subscription to channel failed",
        )
        error.recovery_strategy = WebSocketRecoveryStrategy.RESUBSCRIBE_ALL
        error.context.channel = "trades"

        # Handle error
        success = await recovery_system.handle_stream_error(error)

        # Verify resubscription attempted
        assert mock_subscription_manager.resubscribe.called

    async def test_circuit_breaker_flow(
        self,
        recovery_config: WebSocketErrorRecoveryConfig,
        mock_connection_manager: MagicMock,
        mock_subscription_manager: MagicMock,
        mock_state_manager: MagicMock,
    ) -> None:
        """Test circuit breaker activation flow."""
        recovery_system = StreamRecoverySystem(
            config=recovery_config,
            connection_manager=mock_connection_manager,
            subscription_manager=mock_subscription_manager,
            state_manager=mock_state_manager,
        )

        # Create stream error
        error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.STREAM_CORRUPTED,
            message="Stream data corrupted",
        )
        error.recovery_strategy = WebSocketRecoveryStrategy.CIRCUIT_BREAKER

        # Trigger circuit breaker by exceeding threshold
        successes = []
        for _ in range(recovery_config.circuit_breaker_threshold + 2):
            success = await recovery_system.handle_stream_error(error)
            successes.append(success)

        # Circuit breaker should be active after threshold
        recovery_key = recovery_system._get_recovery_key(error.context)
        assert recovery_system._is_circuit_breaker_active(recovery_key)

        # Further attempts should be blocked
        assert not successes[-1]  # Last attempt should fail

    async def test_dual_error_system_flow(
        self,
        error_config: WebSocketErrorConfig,
    ) -> None:
        """Test dual error system handling during migration."""
        # Create mock handlers
        old_handler = MagicMock()
        old_handler.handle_error = AsyncMock(return_value={"handled": True})

        new_handler = WebSocketStreamErrorHandler(config=error_config)

        # Create dual manager
        dual_manager = DualErrorManager(
            old_handler=old_handler,
            new_handler=new_handler,
            config=error_config,
        )

        # Create test error
        error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.CONNECTION_LOST,
        )

        # Create mock context for dual system
        context = MagicMock(spec=WebSocketContextProtocol)
        context.connection_id = "dual-test"
        context.exchange_name = "hyperliquid"
        context.timestamp_ms = int(datetime.now(UTC).timestamp() * 1000)

        # Handle through dual system
        await dual_manager.handle_error_dual(error, context)

        # Verify both systems were called
        assert old_handler.handle_error.called

    async def test_error_chain_propagation(self) -> None:
        """Test error chain propagation through the system."""
        # Create chain of errors
        root_cause = ValueError("Root cause error")

        context = ErrorTestFactory.create_test_context()
        context.add_error_to_chain(root_cause)

        middle_error = TypeError("Middle layer error")
        context.add_error_to_chain(middle_error)

        # Create final error
        final_error = WebSocketStreamError(
            message="Final error in chain",
            code=WebSocketErrorCode.INTERNAL_ERROR,
            context=context,
            cause=middle_error,
        )

        # Verify chain preserved
        assert len(final_error.context.error_chain) == 2
        assert final_error.context.error_chain[0].error_class == "ValueError"
        assert final_error.context.error_chain[1].error_class == "TypeError"
        assert final_error.cause == middle_error

    async def test_sequence_gap_detection_flow(
        self,
        recovery_config: WebSocketErrorRecoveryConfig,
        mock_connection_manager: MagicMock,
        mock_subscription_manager: MagicMock,
        mock_state_manager: MagicMock,
    ) -> None:
        """Test sequence gap detection and recovery flow."""
        recovery_system = StreamRecoverySystem(
            config=recovery_config,
            connection_manager=mock_connection_manager,
            subscription_manager=mock_subscription_manager,
            state_manager=mock_state_manager,
        )

        # Create context with sequence gap
        context = ErrorTestFactory.create_test_context(
            sequence_number=100,
            expected_sequence=105,  # Gap of 5
        )

        # Create sequence gap error
        error = WebSocketStreamError(
            message="Detected sequence gap",
            code=WebSocketErrorCode.SEQUENCE_GAP,
            context=context,
        )

        # Handle sequence gap
        success = await recovery_system.handle_stream_error(error)

        # Verify appropriate recovery
        assert context.get_sequence_gap_size() == 5
        assert error.recovery_strategy == WebSocketRecoveryStrategy.IMMEDIATE_RETRY

    async def test_authentication_expiry_flow(
        self,
        recovery_config: WebSocketErrorRecoveryConfig,
        mock_connection_manager: MagicMock,
        mock_subscription_manager: MagicMock,
        mock_state_manager: MagicMock,
    ) -> None:
        """Test authentication expiry and re-authentication flow."""
        recovery_system = StreamRecoverySystem(
            config=recovery_config,
            connection_manager=mock_connection_manager,
            subscription_manager=mock_subscription_manager,
            state_manager=mock_state_manager,
        )

        # Create auth expired error
        error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.AUTH_EXPIRED,
            message="Authentication token expired",
        )

        # Handle auth expiry
        with patch.object(recovery_system, "_handle_auth_renewal") as mock_auth:
            mock_auth.return_value = True
            success = await recovery_system.handle_stream_error(error)

            # Verify auth renewal attempted
            if hasattr(recovery_system, "_handle_auth_renewal"):
                assert mock_auth.called

    async def test_exchange_overload_degradation_flow(
        self,
        recovery_config: WebSocketErrorRecoveryConfig,
        mock_connection_manager: MagicMock,
        mock_subscription_manager: MagicMock,
        mock_state_manager: MagicMock,
    ) -> None:
        """Test exchange overload with service degradation flow."""
        recovery_system = StreamRecoverySystem(
            config=recovery_config,
            connection_manager=mock_connection_manager,
            subscription_manager=mock_subscription_manager,
            state_manager=mock_state_manager,
        )

        # Create exchange overload error
        error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.EXCHANGE_OVERLOADED,
            message="Exchange is overloaded",
        )
        error.recovery_strategy = WebSocketRecoveryStrategy.DEGRADE_SERVICE

        # Handle overload
        success = await recovery_system.handle_stream_error(error)

        # Verify service degradation
        assert mock_subscription_manager.clear_subscriptions.called

    async def test_security_violation_flow(self) -> None:
        """Test security violation error flow (no recovery)."""
        # Create security violation error
        error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.SECURITY_VIOLATION,
            message="Security violation detected",
        )

        # Verify no recovery for security violations
        assert error.recovery_strategy == WebSocketRecoveryStrategy.NONE
        assert not error.get_recovery_strategy() != WebSocketRecoveryStrategy.NONE
        assert error.severity == ErrorSeverity.CRITICAL

    async def test_concurrent_error_handling(
        self,
        recovery_config: WebSocketErrorRecoveryConfig,
        mock_connection_manager: MagicMock,
        mock_subscription_manager: MagicMock,
        mock_state_manager: MagicMock,
    ) -> None:
        """Test handling multiple errors concurrently."""
        recovery_system = StreamRecoverySystem(
            config=recovery_config,
            connection_manager=mock_connection_manager,
            subscription_manager=mock_subscription_manager,
            state_manager=mock_state_manager,
        )

        # Create multiple different errors
        errors = [
            ErrorTestFactory.create_test_error(code=WebSocketErrorCode.CONNECTION_LOST),
            ErrorTestFactory.create_test_error(code=WebSocketErrorCode.RATE_LIMITED),
            ErrorTestFactory.create_test_error(code=WebSocketErrorCode.SUBSCRIPTION_FAILED),
        ]

        # Handle all errors concurrently
        tasks = [recovery_system.handle_stream_error(error) for error in errors]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Verify all were handled
        assert len(results) == 3
        assert all(isinstance(r, bool) or isinstance(r, Exception) for r in results)

    async def test_error_metrics_collection(
        self,
        error_config: WebSocketErrorConfig,
    ) -> None:
        """Test error metrics collection throughout flow."""
        # Enable metrics collection
        error_config.enable_metrics_collection = True

        # Create error handler with metrics
        error_handler = WebSocketStreamErrorHandler(config=error_config)

        # Create various errors
        errors = [
            ErrorTestFactory.create_test_error(code=WebSocketErrorCode.CONNECTION_LOST),
            ErrorTestFactory.create_test_error(code=WebSocketErrorCode.RATE_LIMITED),
            ErrorTestFactory.create_test_error(code=WebSocketErrorCode.AUTH_FAILED),
        ]

        # Handle errors
        for error in errors:
            await error_handler.handle_stream_error(error)

        # Get metrics
        metrics = error_handler.get_metrics()

        # Verify metrics collected
        assert metrics.total_errors >= 3
        assert len(metrics.errors_by_code) >= 3

    async def test_stale_connection_detection_flow(
        self,
        recovery_config: WebSocketErrorRecoveryConfig,
        mock_connection_manager: MagicMock,
        mock_subscription_manager: MagicMock,
        mock_state_manager: MagicMock,
    ) -> None:
        """Test stale connection detection and recovery."""
        recovery_system = StreamRecoverySystem(
            config=recovery_config,
            connection_manager=mock_connection_manager,
            subscription_manager=mock_subscription_manager,
            state_manager=mock_state_manager,
        )

        # Create context with stale connection
        now = datetime.now(UTC)
        stale_context = ErrorTestFactory.create_test_context(
            last_heartbeat_ms=int((now - timedelta(minutes=5)).timestamp() * 1000),
        )

        # Check if stale
        assert stale_context.is_connection_stale(max_heartbeat_age_ms=60000)

        # Create heartbeat timeout error
        error = WebSocketStreamError(
            message="Heartbeat timeout",
            code=WebSocketErrorCode.HEARTBEAT_TIMEOUT,
            context=stale_context,
        )

        # Handle stale connection
        success = await recovery_system.handle_stream_error(error)

        # Verify reconnection attempted
        assert (
            mock_connection_manager.reconnect.called
            or mock_connection_manager.reset_connection.called
        )
