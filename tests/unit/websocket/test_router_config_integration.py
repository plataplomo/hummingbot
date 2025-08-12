"""Test router configuration integration for typed WebSocket error handling.

This test validates Step 42: Router Configuration Updates.
"""

from __future__ import annotations

from unittest.mock import Mock

import pytest
from pydantic import BaseModel

from cyberdelta.apis.websocket.ws_context import ExchangeType
from cyberdelta.apis.websocket.ws_router import BaseWebSocketRouter
from cyberdelta.apis.websocket.ws_router_config_integration import (
    RouterConfigurationError,
    RouterConfigurationValidator,
    WebSocketRouterConfigurator,
)
from cyberdelta.apis.websocket.ws_stream_error_handler import WebSocketStreamErrorHandler
from cyberdelta.apis.websocket.ws_typed_processor import TypeSafeWebSocketProcessor
from cyberdelta.config.models.websocket_error_config import (
    WebSocketErrorConfig,
)


class TestEnvelopeModel(BaseModel):
    """Test envelope model."""

    stream: str
    data: dict[str, object]


class TestRouterImpl(BaseWebSocketRouter[TestEnvelopeModel]):
    """Test router implementation."""

    def _setup_processors(self) -> None:
        """Setup test processors."""

    def _extract_routing_key_from_envelope(self, envelope: TestEnvelopeModel) -> str | None:
        """Extract routing key."""
        return envelope.stream

    def _extract_payload_from_envelope(self, envelope: TestEnvelopeModel) -> dict[str, object]:
        """Extract payload."""
        return envelope.data


class TestWebSocketRouterConfigurator:
    """Test router configurator functionality."""

    @pytest.fixture
    def mock_legacy_error_handler(self) -> Mock:
        """Create mock legacy error handler."""
        return Mock()

    @pytest.fixture
    def mock_stream_error_handler(self) -> Mock:
        """Create mock stream error handler."""
        return Mock(spec=WebSocketStreamErrorHandler)

    @pytest.fixture
    def mock_typed_processor(self) -> Mock:
        """Create mock typed processor."""
        return Mock(spec=TypeSafeWebSocketProcessor)

    @pytest.fixture
    def default_config(self) -> WebSocketErrorConfig:
        """Create default WebSocket error configuration."""
        return WebSocketErrorConfig()

    @pytest.fixture
    def test_router(
        self, mock_legacy_error_handler: Mock, mock_typed_processor: Mock
    ) -> TestRouterImpl:
        """Create test router."""
        return TestRouterImpl(
            exchange_name="hyperliquid",
            exchange_type=ExchangeType.HYPERLIQUID,
            error_handler=mock_legacy_error_handler,
            typed_processor=mock_typed_processor,
        )

    @pytest.fixture
    def configurator(
        self, default_config: WebSocketErrorConfig
    ) -> WebSocketRouterConfigurator[TestEnvelopeModel]:
        """Create router configurator."""
        return WebSocketRouterConfigurator(default_config)

    def test_configurator_initialization(
        self,
        configurator: WebSocketRouterConfigurator[TestEnvelopeModel],
        default_config: WebSocketErrorConfig,
    ) -> None:
        """Test configurator initialization."""
        assert configurator.config == default_config
        assert configurator.router_config == default_config.router
        assert configurator.logger is not None

    def test_configure_router_error_handling_with_stream_handler(
        self,
        configurator: WebSocketRouterConfigurator[TestEnvelopeModel],
        test_router: TestRouterImpl,
        mock_legacy_error_handler: Mock,
        mock_stream_error_handler: Mock,
    ) -> None:
        """Test router configuration with available stream handler."""
        # Set stream handler on router
        test_router.stream_error_handler = mock_stream_error_handler

        # Configure router
        bridge = configurator.configure_router_error_handling(
            test_router, mock_legacy_error_handler
        )

        # Verify bridge was created
        assert bridge is not None
        assert bridge.is_available()

        # Verify bridge configuration
        bridge_info = bridge.get_bridge_info()
        assert bridge_info["exchange"] == "hyperliquid"
        assert bridge_info["router_type"] == "TestRouterImpl"
        assert bridge_info["stream_handler_available"] is True
        assert bridge_info["legacy_handler_available"] is True

    def test_configure_router_error_handling_without_stream_handler_fallback_enabled(
        self,
        configurator: WebSocketRouterConfigurator[TestEnvelopeModel],
        test_router: TestRouterImpl,
        mock_legacy_error_handler: Mock,
    ) -> None:
        """Test router configuration without stream handler with fallback enabled."""
        # Ensure no stream handler
        test_router.stream_error_handler = None

        # Configure router (should return None due to fallback)
        bridge = configurator.configure_router_error_handling(
            test_router, mock_legacy_error_handler
        )

        # Should return None when stream handler not available and fallback enabled
        assert bridge is None

    def test_configure_router_error_handling_without_stream_handler_fallback_disabled(
        self,
        test_router: TestRouterImpl,
        mock_legacy_error_handler: Mock,
    ) -> None:
        """Test router configuration without stream handler with fallback disabled."""
        # Create config with fallback disabled
        config = WebSocketErrorConfig()
        config.router.bridge_fallback_to_legacy = False
        configurator = WebSocketRouterConfigurator(config)

        # Ensure no stream handler
        test_router.stream_error_handler = None

        # Should raise error when stream handler required but not available
        with pytest.raises(RouterConfigurationError) as exc_info:
            configurator.configure_router_error_handling(test_router, mock_legacy_error_handler)

        assert "Stream error handler required but not available" in str(exc_info.value)
        assert exc_info.value.config_field == "stream_error_handler"

    def test_configure_router_error_handling_disabled(
        self,
        test_router: TestRouterImpl,
        mock_legacy_error_handler: Mock,
    ) -> None:
        """Test router configuration when error bridge is disabled."""
        # Create config with bridge disabled
        config = WebSocketErrorConfig()
        config.router.enable_error_bridge = False
        configurator = WebSocketRouterConfigurator(config)

        # Configure router
        bridge = configurator.configure_router_error_handling(
            test_router, mock_legacy_error_handler
        )

        # Should return None when disabled
        assert bridge is None

    def test_get_envelope_validation_config(
        self, configurator: WebSocketRouterConfigurator[TestEnvelopeModel]
    ) -> None:
        """Test envelope validation configuration retrieval."""
        config = configurator.get_envelope_validation_config()

        assert "strict_validation" in config
        assert "log_failures" in config
        assert "timeout_ms" in config
        assert config["strict_validation"] is True
        assert config["log_failures"] is True
        assert config["timeout_ms"] == 500

    def test_get_routing_key_config(
        self, configurator: WebSocketRouterConfigurator[TestEnvelopeModel]
    ) -> None:
        """Test routing key configuration retrieval."""
        config = configurator.get_routing_key_config()

        assert "allow_empty" in config
        assert "max_length" in config
        assert "log_missing" in config
        assert config["allow_empty"] is False
        assert config["max_length"] == 100
        assert config["log_missing"] is True

    def test_get_processor_config(
        self, configurator: WebSocketRouterConfigurator[TestEnvelopeModel]
    ) -> None:
        """Test processor configuration retrieval."""
        config = configurator.get_processor_config()

        assert "log_missing_processors" in config
        assert "log_missing_handlers" in config
        assert "processor_timeout_ms" in config
        assert "handler_timeout_ms" in config
        assert config["log_missing_processors"] is True
        assert config["log_missing_handlers"] is True
        assert config["processor_timeout_ms"] == 100
        assert config["handler_timeout_ms"] == 100

    def test_get_message_send_config(
        self, configurator: WebSocketRouterConfigurator[TestEnvelopeModel]
    ) -> None:
        """Test message send configuration retrieval."""
        config = configurator.get_message_send_config()

        assert "enable_tracking" in config
        assert "timeout_ms" in config
        assert "retry_attempts" in config
        assert config["enable_tracking"] is True
        assert config["timeout_ms"] == 5000
        assert config["retry_attempts"] == 2

    def test_get_performance_config(
        self, configurator: WebSocketRouterConfigurator[TestEnvelopeModel]
    ) -> None:
        """Test performance configuration retrieval."""
        config = configurator.get_performance_config()

        assert "enable_tracking" in config
        assert "warning_threshold_ms" in config
        assert "max_concurrent" in config
        assert config["enable_tracking"] is True
        assert config["warning_threshold_ms"] == 100
        assert config["max_concurrent"] == 100

    def test_get_context_config(
        self, configurator: WebSocketRouterConfigurator[TestEnvelopeModel]
    ) -> None:
        """Test context configuration retrieval."""
        config = configurator.get_context_config()

        assert "enhanced_contexts" in config
        assert "include_metadata" in config
        assert "timeout_ms" in config
        assert "max_extra_data_bytes" in config
        assert config["enhanced_contexts"] is True
        assert config["include_metadata"] is True
        assert config["timeout_ms"] == 200
        assert config["max_extra_data_bytes"] == 10240

    def test_should_use_typed_error_handling(
        self, configurator: WebSocketRouterConfigurator[TestEnvelopeModel]
    ) -> None:
        """Test typed error handling enablement check."""
        # Default config should enable typed error handling
        assert configurator.should_use_typed_error_handling() is True

        # Test with error system disabled
        configurator.config.enabled = False
        assert configurator.should_use_typed_error_handling() is False

        # Reset and test with bridge disabled
        configurator.config.enabled = True
        configurator.router_config.enable_error_bridge = False
        assert configurator.should_use_typed_error_handling() is False

    def test_should_fallback_to_legacy(
        self, configurator: WebSocketRouterConfigurator[TestEnvelopeModel]
    ) -> None:
        """Test legacy fallback enablement check."""
        # Default config should enable fallback
        assert configurator.should_fallback_to_legacy() is True

        # Test with fallback disabled
        configurator.router_config.bridge_fallback_to_legacy = False
        assert configurator.should_fallback_to_legacy() is False

    def test_get_bridge_timeout_ms(
        self, configurator: WebSocketRouterConfigurator[TestEnvelopeModel]
    ) -> None:
        """Test bridge timeout retrieval."""
        timeout = configurator.get_bridge_timeout_ms()
        assert timeout == 1000  # Default value

    def test_create_router_config_summary(
        self,
        configurator: WebSocketRouterConfigurator[TestEnvelopeModel],
        test_router: TestRouterImpl,
    ) -> None:
        """Test router configuration summary creation."""
        summary = configurator.create_router_config_summary(test_router)

        # Verify summary structure
        assert "exchange" in summary
        assert "router_type" in summary
        assert "typed_error_handling_enabled" in summary
        assert "bridge_fallback_enabled" in summary
        assert "envelope_validation" in summary
        assert "routing_key" in summary
        assert "processor" in summary
        assert "message_send" in summary
        assert "performance" in summary
        assert "context" in summary
        assert "bridge_timeout_ms" in summary

        # Verify values
        assert summary["exchange"] == "hyperliquid"
        assert summary["router_type"] == "TestRouterImpl"
        assert summary["typed_error_handling_enabled"] is True
        assert summary["bridge_fallback_enabled"] is True
        assert summary["bridge_timeout_ms"] == 1000


class TestRouterConfigurationValidator:
    """Test router configuration validator functionality."""

    @pytest.fixture
    def mock_legacy_error_handler(self) -> Mock:
        """Create mock legacy error handler."""
        return Mock()

    @pytest.fixture
    def mock_typed_processor(self) -> Mock:
        """Create mock typed processor."""
        return Mock(spec=TypeSafeWebSocketProcessor)

    def test_validate_valid_config(self) -> None:
        """Test validation of valid configuration."""
        config = WebSocketErrorConfig()
        errors = RouterConfigurationValidator.validate_config(config)
        assert len(errors) == 0

    def test_validate_timeout_inconsistency(self) -> None:
        """Test validation of timeout inconsistencies."""
        config = WebSocketErrorConfig()

        # Set bridge timeout < envelope validation timeout (invalid)
        config.router.bridge_error_timeout_ms = 100
        config.router.envelope_validation_timeout_ms = 200

        errors = RouterConfigurationValidator.validate_config(config)
        assert len(errors) > 0
        assert any(
            "Bridge timeout must be >= envelope validation timeout" in error for error in errors
        )

    def test_validate_context_timeout_inconsistency(self) -> None:
        """Test validation of context timeout inconsistencies."""
        config = WebSocketErrorConfig()

        # Set context creation timeout > bridge timeout (invalid)
        config.router.context_creation_timeout_ms = 2000
        config.router.bridge_error_timeout_ms = 1000

        errors = RouterConfigurationValidator.validate_config(config)
        assert len(errors) > 0
        assert any(
            "Context creation timeout must be <= bridge timeout" in error for error in errors
        )

    def test_validate_invalid_performance_settings(self) -> None:
        """Test validation of invalid performance settings."""
        config = WebSocketErrorConfig()

        # Set invalid values
        config.router.max_concurrent_routing_operations = 0
        config.router.routing_performance_warning_threshold_ms = 0
        config.router.max_context_extra_data_size_bytes = 512  # Too small
        config.router.routing_key_max_length = 0
        config.router.message_send_retry_attempts = -1

        errors = RouterConfigurationValidator.validate_config(config)
        assert len(errors) == 5  # All invalid settings should be caught

    def test_validate_router_compatibility_success(
        self, mock_legacy_error_handler: Mock, mock_typed_processor: Mock
    ) -> None:
        """Test router compatibility validation success."""
        config = WebSocketErrorConfig()
        router = TestRouterImpl(
            exchange_name="hyperliquid",
            exchange_type=ExchangeType.HYPERLIQUID,
            error_handler=mock_legacy_error_handler,
            typed_processor=mock_typed_processor,
        )

        issues = RouterConfigurationValidator.validate_router_compatibility(router, config)
        assert len(issues) == 0

    def test_validate_router_compatibility_missing_features(
        self, mock_legacy_error_handler: Mock, mock_typed_processor: Mock
    ) -> None:
        """Test router compatibility validation with missing features."""
        config = WebSocketErrorConfig()

        # Create minimal router without optional features
        router = TestRouterImpl(
            exchange_name="hyperliquid",
            exchange_type=ExchangeType.HYPERLIQUID,
            error_handler=mock_legacy_error_handler,
            typed_processor=mock_typed_processor,
        )

        # Remove stream_error_handler attribute to simulate missing feature
        if hasattr(router, "stream_error_handler"):
            delattr(router, "stream_error_handler")

        issues = RouterConfigurationValidator.validate_router_compatibility(router, config)

        # Should have compatibility issues
        assert len(issues) > 0
        assert any("does not support stream error handler" in issue for issue in issues)
