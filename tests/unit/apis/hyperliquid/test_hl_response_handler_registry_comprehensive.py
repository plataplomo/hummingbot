"""Comprehensive unit tests for HyperliquidResponseHandlerRegistry.

Tests registry functionality for managing response handlers,
focusing only on public API behavior with comprehensive success, edge, and failure cases.
"""

from unittest.mock import MagicMock, patch

import pytest

from cyberdelta.apis.hyperliquid.response_handlers.hl_response_handler_registry import (
    HyperliquidResponseHandlerRegistry,
    IResponseHandler,
)
from cyberdelta.utils.typing import ParsedJsonResponse


class TestHyperliquidResponseHandlerRegistry:
    """Test HyperliquidResponseHandlerRegistry public API functionality."""

    @pytest.fixture
    def registry(self) -> HyperliquidResponseHandlerRegistry:
        """Create a fresh registry instance for testing."""
        return HyperliquidResponseHandlerRegistry()

    @pytest.fixture
    def mock_handler(self) -> MagicMock:
        """Create a mock response handler that implements IResponseHandler protocol."""
        mock = MagicMock(spec=IResponseHandler)
        mock.handle_response.return_value = {"processed": "response"}
        return mock

    @pytest.fixture
    def sample_response_data(self) -> tuple[ParsedJsonResponse, int, dict[str, str], str]:
        """Create sample response data for testing."""
        response: ParsedJsonResponse = {"data": "test"}
        status_code = 200
        headers = {"Content-Type": "application/json"}
        context = "test_context"
        return response, status_code, headers, context

    def test_registry_initialization(self, registry: HyperliquidResponseHandlerRegistry) -> None:
        """Test that registry initializes correctly."""
        assert registry is not None
        assert isinstance(registry, HyperliquidResponseHandlerRegistry)
        # Test initialization state through public behavior
        assert len(registry) == 0

    def test_initialize_default_handlers_success(
        self, registry: HyperliquidResponseHandlerRegistry
    ) -> None:
        """Test successful initialization of default handlers."""
        with (
            patch.object(registry, "_register_trading_handlers") as mock_trading,
            patch.object(registry, "_register_account_handlers") as mock_account,
            patch.object(registry, "_register_market_data_handlers") as mock_market,
        ):
            registry.initialize_default_handlers()

            # Verify all registration methods were called
            mock_trading.assert_called_once()
            mock_account.assert_called_once()
            mock_market.assert_called_once()

    def test_initialize_default_handlers_already_initialized(
        self, registry: HyperliquidResponseHandlerRegistry
    ) -> None:
        """Test that re-initialization is prevented and logged."""
        # First initialization
        with patch.object(registry, "_register_trading_handlers"):
            registry.initialize_default_handlers()

        # Second attempt should be prevented
        with patch.object(registry, "_register_trading_handlers") as mock_trading:
            registry.initialize_default_handlers()

            # Should not be called again
            mock_trading.assert_not_called()

    def test_register_trading_handler(
        self, registry: HyperliquidResponseHandlerRegistry, mock_handler: MagicMock
    ) -> None:
        """Test registering a trading response handler."""
        operation = "place_order"

        registry.register_trading_handler(operation, mock_handler)

        # Verify registration
        assert registry.is_registered(f"trading.{operation}")
        retrieved_handler = registry.get(f"trading.{operation}")
        assert retrieved_handler is mock_handler

    def test_register_account_handler(
        self, registry: HyperliquidResponseHandlerRegistry, mock_handler: MagicMock
    ) -> None:
        """Test registering an account response handler."""
        operation = "get_balance"

        registry.register_account_handler(operation, mock_handler)

        # Verify registration
        assert registry.is_registered(f"account.{operation}")
        retrieved_handler = registry.get(f"account.{operation}")
        assert retrieved_handler is mock_handler

    def test_register_market_data_handler(
        self, registry: HyperliquidResponseHandlerRegistry, mock_handler: MagicMock
    ) -> None:
        """Test registering a market data response handler."""
        operation = "get_ticker"

        registry.register_market_data_handler(operation, mock_handler)

        # Verify registration
        assert registry.is_registered(f"market_data.{operation}")
        retrieved_handler = registry.get(f"market_data.{operation}")
        assert retrieved_handler is mock_handler

    def test_get_trading_handler(
        self, registry: HyperliquidResponseHandlerRegistry, mock_handler: MagicMock
    ) -> None:
        """Test retrieving a trading response handler."""
        operation = "cancel_order"
        registry.register_trading_handler(operation, mock_handler)

        retrieved_handler = registry.get_trading_handler(operation)

        assert retrieved_handler is mock_handler

    def test_get_account_handler(
        self, registry: HyperliquidResponseHandlerRegistry, mock_handler: MagicMock
    ) -> None:
        """Test retrieving an account response handler."""
        operation = "get_positions"
        registry.register_account_handler(operation, mock_handler)

        retrieved_handler = registry.get_account_handler(operation)

        assert retrieved_handler is mock_handler

    def test_get_market_data_handler(
        self, registry: HyperliquidResponseHandlerRegistry, mock_handler: MagicMock
    ) -> None:
        """Test retrieving a market data response handler."""
        operation = "get_orderbook"
        registry.register_market_data_handler(operation, mock_handler)

        retrieved_handler = registry.get_market_data_handler(operation)

        assert retrieved_handler is mock_handler

    @pytest.mark.parametrize(
        ("domain", "operations"),
        [
            ("trading", ["place_order", "cancel_order", "get_order"]),
            ("account", ["get_balance", "get_positions", "get_trades"]),
            ("market_data", ["get_ticker", "get_orderbook", "get_candles"]),
        ],
    )
    def test_get_handlers_by_domain(
        self, registry: HyperliquidResponseHandlerRegistry, domain: str, operations: list[str]
    ) -> None:
        """Test retrieving handlers by domain."""
        handlers = {}

        # Register handlers for the domain
        for operation in operations:
            mock_handler = MagicMock(spec=IResponseHandler)
            getattr(registry, f"register_{domain}_handler")(operation, mock_handler)
            handlers[operation] = mock_handler

        # Retrieve handlers by domain
        domain_handlers = registry.get_handlers_by_domain(domain)

        # Verify all handlers are returned
        assert len(domain_handlers) == len(operations)
        for operation in operations:
            assert operation in domain_handlers
            assert domain_handlers[operation] is handlers[operation]

    def test_get_handlers_by_domain_empty(
        self, registry: HyperliquidResponseHandlerRegistry
    ) -> None:
        """Test retrieving handlers by domain when none are registered."""
        domain_handlers = registry.get_handlers_by_domain("trading")

        assert domain_handlers == {}

    def test_handle_response_by_operation_success(
        self,
        registry: HyperliquidResponseHandlerRegistry,
        mock_handler: MagicMock,
        sample_response_data: tuple[ParsedJsonResponse, int, dict[str, str], str],
    ) -> None:
        """Test successful response handling by operation."""
        domain = "trading"
        operation = "place_order"
        response, status_code, headers, context = sample_response_data

        # Register handler
        registry.register_trading_handler(operation, mock_handler)

        # Handle response
        result = registry.handle_response_by_operation(
            domain, operation, response, status_code, headers, context
        )

        # Verify handler was called correctly
        mock_handler.handle_response.assert_called_once_with(
            response, status_code, headers, context
        )
        assert result == {"processed": "response"}

    def test_handle_response_by_operation_default_context(
        self,
        registry: HyperliquidResponseHandlerRegistry,
        mock_handler: MagicMock,
        sample_response_data: tuple[ParsedJsonResponse, int, dict[str, str], str],
    ) -> None:
        """Test response handling with default context generation."""
        domain = "account"
        operation = "get_balance"
        response, status_code, headers, _ = sample_response_data

        # Register handler
        registry.register_account_handler(operation, mock_handler)

        # Handle response without explicit context
        result = registry.handle_response_by_operation(
            domain, operation, response, status_code, headers
        )

        # Verify handler was called with generated context
        expected_context = f"{domain}.{operation}"
        mock_handler.handle_response.assert_called_once_with(
            response, status_code, headers, expected_context
        )
        assert result == {"processed": "response"}

    def test_validate_registry_all_present(
        self, registry: HyperliquidResponseHandlerRegistry
    ) -> None:
        """Test registry validation when all expected handlers are present."""
        # Register all expected handlers
        expected_handlers = [
            ("trading", ["place_order", "cancel_order", "get_order", "batch_orders"]),
            ("account", ["get_balance", "get_positions", "get_account_summary"]),
            ("market_data", ["get_ticker", "get_orderbook", "get_trades"]),
        ]

        for domain, operations in expected_handlers:
            for operation in operations:
                mock_handler = MagicMock(spec=IResponseHandler)
                getattr(registry, f"register_{domain}_handler")(operation, mock_handler)

        issues = registry.validate_registry()

        assert issues == []  # No issues should be found

    def test_validate_registry_missing_handlers(
        self, registry: HyperliquidResponseHandlerRegistry
    ) -> None:
        """Test registry validation when some handlers are missing."""
        # Register only some handlers
        trading_handler = MagicMock(spec=IResponseHandler)
        registry.register_trading_handler("place_order", trading_handler)

        issues = registry.validate_registry()

        # Should report missing handlers
        assert len(issues) > 0
        assert any("Missing handler: trading.cancel_order" in issue for issue in issues)
        assert any("Missing handler: account.get_balance" in issue for issue in issues)
        assert any("Missing handler: market_data.get_ticker" in issue for issue in issues)

    def test_handler_protocol_compliance(
        self,
        registry: HyperliquidResponseHandlerRegistry,
        sample_response_data: tuple[ParsedJsonResponse, int, dict[str, str], str],
    ) -> None:
        """Test that registered handlers must implement IResponseHandler protocol."""
        # Create a proper mock that implements the protocol
        mock_handler = MagicMock(spec=IResponseHandler)
        mock_handler.handle_response.return_value = {"handled": "data"}

        registry.register_trading_handler("test_op", mock_handler)
        retrieved_handler = registry.get_trading_handler("test_op")

        # Should be able to call protocol methods
        response, status_code, headers, context = sample_response_data
        result = retrieved_handler.handle_response(response, status_code, headers, context)

        assert result == {"handled": "data"}
        mock_handler.handle_response.assert_called_once_with(
            response, status_code, headers, context
        )

    def test_create_error_handler_wrapper_success(
        self,
        registry: HyperliquidResponseHandlerRegistry,
        sample_response_data: tuple[ParsedJsonResponse, int, dict[str, str], str],
    ) -> None:
        """Test creating error handler wrapper that handles responses normally."""
        base_handler = MagicMock(spec=IResponseHandler)
        base_handler.handle_response.return_value = {"success": "response"}
        error_mapper = MagicMock()

        # Create wrapper
        wrapped_handler = registry.create_error_handler_wrapper(base_handler, error_mapper)

        # Test normal response handling
        response, status_code, headers, context = sample_response_data
        result = wrapped_handler.handle_response(response, status_code, headers, context)

        # Should pass through to base handler
        base_handler.handle_response.assert_called_once_with(
            response, status_code, headers, context
        )
        assert result == {"success": "response"}

        # Error mapper should not be called on success
        assert not error_mapper.map_response_error.called

    def test_create_error_handler_wrapper_with_error_mapping(
        self,
        registry: HyperliquidResponseHandlerRegistry,
        sample_response_data: tuple[ParsedJsonResponse, int, dict[str, str], str],
    ) -> None:
        """Test error handler wrapper that maps errors."""
        base_handler = MagicMock(spec=IResponseHandler)
        original_error = ValueError("Original error")
        base_handler.handle_response.side_effect = original_error

        error_mapper = MagicMock()
        mapped_error = RuntimeError("Mapped error")
        error_mapper.map_response_error.return_value = mapped_error

        # Create wrapper
        wrapped_handler = registry.create_error_handler_wrapper(base_handler, error_mapper)

        # Test error handling
        response, status_code, headers, context = sample_response_data

        with pytest.raises(RuntimeError) as exc_info:
            wrapped_handler.handle_response(response, status_code, headers, context)

        # Should have called error mapper
        error_mapper.map_response_error.assert_called_once_with(
            original_error, status_code, context
        )
        assert exc_info.value is mapped_error

    def test_create_error_handler_wrapper_no_error_mapper(
        self,
        registry: HyperliquidResponseHandlerRegistry,
        sample_response_data: tuple[ParsedJsonResponse, int, dict[str, str], str],
    ) -> None:
        """Test error handler wrapper when error mapper doesn't have map_response_error method."""
        base_handler = MagicMock(spec=IResponseHandler)
        original_error = ValueError("Original error")
        base_handler.handle_response.side_effect = original_error

        # Error mapper without map_response_error method
        error_mapper = MagicMock()
        del error_mapper.map_response_error  # Remove the method

        # Create wrapper
        wrapped_handler = registry.create_error_handler_wrapper(base_handler, error_mapper)

        # Test error handling
        response, status_code, headers, context = sample_response_data

        with pytest.raises(ValueError) as exc_info:
            wrapped_handler.handle_response(response, status_code, headers, context)

        # Should re-raise original error
        assert exc_info.value is original_error

    @pytest.mark.parametrize("domain", ["trading", "account", "market_data"])
    def test_domain_specific_registration_and_retrieval(
        self, registry: HyperliquidResponseHandlerRegistry, domain: str
    ) -> None:
        """Test domain-specific registration and retrieval methods."""
        operation = "test_operation"
        mock_handler = MagicMock(spec=IResponseHandler)

        # Register using domain-specific method
        register_method = getattr(registry, f"register_{domain}_handler")
        register_method(operation, mock_handler)

        # Retrieve using domain-specific method
        get_method = getattr(registry, f"get_{domain}_handler")
        retrieved_handler = get_method(operation)

        assert retrieved_handler is mock_handler

    def test_multiple_operation_registration(
        self, registry: HyperliquidResponseHandlerRegistry
    ) -> None:
        """Test registering multiple operations across domains."""
        operations = [
            ("trading", "place_order"),
            ("trading", "cancel_order"),
            ("account", "get_balance"),
            ("market_data", "get_ticker"),
        ]

        handlers = {}
        for domain, operation in operations:
            mock_handler = MagicMock(spec=IResponseHandler)
            handlers[f"{domain}.{operation}"] = mock_handler
            getattr(registry, f"register_{domain}_handler")(operation, mock_handler)

        # Verify all operations are registered correctly
        for domain, operation in operations:
            handler_name = f"{domain}.{operation}"
            assert registry.is_registered(handler_name)
            assert registry.get(handler_name) is handlers[handler_name]

    def test_comprehensive_response_handling_workflow(
        self,
        registry: HyperliquidResponseHandlerRegistry,
        sample_response_data: tuple[ParsedJsonResponse, int, dict[str, str], str],
    ) -> None:
        """Test comprehensive workflow from registration to response handling."""
        # Setup
        domain = "trading"
        operation = "place_order"
        mock_handler = MagicMock(spec=IResponseHandler)
        mock_handler.handle_response.return_value = {"order_id": "12345"}

        # Register handler
        registry.register_trading_handler(operation, mock_handler)

        # Verify registration
        assert registry.is_registered(f"{domain}.{operation}")

        # Handle response through the registry
        response, status_code, headers, context = sample_response_data
        result = registry.handle_response_by_operation(
            domain, operation, response, status_code, headers, context
        )

        # Verify complete workflow
        assert result == {"order_id": "12345"}
        mock_handler.handle_response.assert_called_once_with(
            response, status_code, headers, context
        )

    def test_registry_inheritance_from_base(
        self, registry: HyperliquidResponseHandlerRegistry
    ) -> None:
        """Test that registry properly inherits from BaseComponentRegistry."""
        # Test inherited methods work
        mock_handler = MagicMock(spec=IResponseHandler)

        # Use inherited methods
        registry.register("custom.handler", mock_handler)
        assert registry.is_registered("custom.handler")
        assert registry.get("custom.handler") is mock_handler

        # Test len method
        assert len(registry) == 1
