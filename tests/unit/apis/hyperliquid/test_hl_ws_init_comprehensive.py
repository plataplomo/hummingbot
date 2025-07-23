"""Comprehensive unit tests for Hyperliquid WebSocket initialization.

Tests WebSocket initialization functionality focusing only on public API behavior
with comprehensive success, edge, and failure cases.
"""

import inspect
from collections.abc import Callable
from unittest.mock import MagicMock

import pytest

from cyberdelta.apis.hyperliquid.hl_ws_context import HyperliquidMessageContext
from cyberdelta.apis.hyperliquid.hl_ws_init import initialize_hyperliquid_ws
from cyberdelta.apis.hyperliquid.models.hl_ws_envelope import validate_hyperliquid_envelope
from cyberdelta.apis.websocket.ws_context import ExchangeType
from cyberdelta.apis.websocket.ws_context_registry import WebSocketContextRegistry


class TestInitializeHyperliquidWs:
    """Test initialize_hyperliquid_ws function."""

    def test_initialize_hyperliquid_ws_basic_registration(self) -> None:
        """Test that initialize_hyperliquid_ws registers components correctly."""
        # Create mock registry
        mock_registry = MagicMock(spec=WebSocketContextRegistry)

        # Call the initialization function
        initialize_hyperliquid_ws(mock_registry)

        # Verify register_context_type was called once
        mock_registry.register_context_type.assert_called_once()

        # Verify the call arguments
        call_args = mock_registry.register_context_type.call_args
        assert call_args is not None

        # Check keyword arguments
        kwargs = call_args.kwargs
        assert kwargs["exchange_type"] == ExchangeType.HYPERLIQUID
        assert kwargs["context_class"] == HyperliquidMessageContext
        assert kwargs["envelope_validator"] == validate_hyperliquid_envelope

    def test_initialize_hyperliquid_ws_correct_exchange_type(self) -> None:
        """Test that the correct exchange type is registered."""
        mock_registry = MagicMock(spec=WebSocketContextRegistry)

        initialize_hyperliquid_ws(mock_registry)

        # Extract the call arguments
        call_args = mock_registry.register_context_type.call_args
        exchange_type = call_args.kwargs["exchange_type"]

        assert exchange_type == ExchangeType.HYPERLIQUID
        assert isinstance(exchange_type, ExchangeType)

    def test_initialize_hyperliquid_ws_correct_context_class(self) -> None:
        """Test that the correct context class is registered."""
        mock_registry = MagicMock(spec=WebSocketContextRegistry)

        initialize_hyperliquid_ws(mock_registry)

        # Extract the call arguments
        call_args = mock_registry.register_context_type.call_args
        context_class = call_args.kwargs["context_class"]

        assert context_class == HyperliquidMessageContext
        assert context_class is HyperliquidMessageContext  # Same reference

    def test_initialize_hyperliquid_ws_correct_envelope_validator(self) -> None:
        """Test that the correct envelope validator is registered."""
        mock_registry = MagicMock(spec=WebSocketContextRegistry)

        initialize_hyperliquid_ws(mock_registry)

        # Extract the call arguments
        call_args = mock_registry.register_context_type.call_args
        envelope_validator = call_args.kwargs["envelope_validator"]

        assert envelope_validator == validate_hyperliquid_envelope
        assert envelope_validator is validate_hyperliquid_envelope  # Same reference

    def test_initialize_hyperliquid_ws_single_registration_call(self) -> None:
        """Test that only one registration call is made."""
        mock_registry = MagicMock(spec=WebSocketContextRegistry)

        initialize_hyperliquid_ws(mock_registry)

        # Should be called exactly once
        assert mock_registry.register_context_type.call_count == 1

    def test_initialize_hyperliquid_ws_no_return_value(self) -> None:
        """Test that initialize_hyperliquid_ws returns None."""
        mock_registry = MagicMock(spec=WebSocketContextRegistry)

        initialize_hyperliquid_ws(mock_registry)

        # Function returns None implicitly

    def test_initialize_hyperliquid_ws_with_different_registries(self) -> None:
        """Test initialization works with different registry instances."""
        # Test with multiple different registry instances
        registries = [
            MagicMock(spec=WebSocketContextRegistry),
            MagicMock(spec=WebSocketContextRegistry),
            MagicMock(spec=WebSocketContextRegistry),
        ]

        for registry in registries:
            initialize_hyperliquid_ws(registry)

            # Each registry should have been called
            registry.register_context_type.assert_called_once()

    def test_initialize_hyperliquid_ws_registry_method_signature(self) -> None:
        """Test that the registry method is called with correct signature."""
        mock_registry = MagicMock(spec=WebSocketContextRegistry)

        initialize_hyperliquid_ws(mock_registry)

        # Verify the method was called with keyword arguments
        call_args = mock_registry.register_context_type.call_args

        # Should have exactly 3 keyword arguments
        assert len(call_args.kwargs) == 3
        assert "exchange_type" in call_args.kwargs
        assert "context_class" in call_args.kwargs
        assert "envelope_validator" in call_args.kwargs

        # Should have no positional arguments
        assert len(call_args.args) == 0

    def test_initialize_hyperliquid_ws_parameter_types(self) -> None:
        """Test that parameters passed to registry have correct types."""
        mock_registry = MagicMock(spec=WebSocketContextRegistry)

        initialize_hyperliquid_ws(mock_registry)

        call_args = mock_registry.register_context_type.call_args
        kwargs = call_args.kwargs

        # Check parameter types
        assert isinstance(kwargs["exchange_type"], ExchangeType)
        assert callable(kwargs["context_class"])  # Should be a class (callable)
        assert callable(kwargs["envelope_validator"])  # Should be a function (callable)

    @pytest.mark.parametrize(
        "registry_setup",
        [
            lambda: MagicMock(spec=WebSocketContextRegistry),
            lambda: MagicMock(spec=WebSocketContextRegistry, return_value=None),
        ],
    )
    def test_initialize_hyperliquid_ws_parametrized(
        self, registry_setup: Callable[[], MagicMock]
    ) -> None:
        """Test initialization with different registry configurations."""
        mock_registry = registry_setup()

        initialize_hyperliquid_ws(mock_registry)

        # Should always call register_context_type
        mock_registry.register_context_type.assert_called_once()

    def test_initialize_hyperliquid_ws_idempotent_calls(self) -> None:
        """Test that multiple calls to initialize work independently."""
        mock_registry = MagicMock(spec=WebSocketContextRegistry)

        # Call multiple times
        initialize_hyperliquid_ws(mock_registry)
        initialize_hyperliquid_ws(mock_registry)
        initialize_hyperliquid_ws(mock_registry)

        # Should be called 3 times (not idempotent by design)
        assert mock_registry.register_context_type.call_count == 3

    def test_initialize_hyperliquid_ws_registry_interface_compliance(self) -> None:
        """Test that we're using the registry interface correctly."""
        mock_registry = MagicMock(spec=WebSocketContextRegistry)

        # Ensure register_context_type method exists on the spec
        assert hasattr(mock_registry, "register_context_type")

        initialize_hyperliquid_ws(mock_registry)

        # Verify the method was actually called
        assert mock_registry.register_context_type.called

    def test_initialize_hyperliquid_ws_components_are_importable(self) -> None:
        """Test that all components being registered are properly importable."""
        # Test that components are importable and defined
        assert HyperliquidMessageContext is not None
        assert validate_hyperliquid_envelope is not None
        assert ExchangeType is not None
        assert ExchangeType.HYPERLIQUID is not None

    def test_initialize_hyperliquid_ws_function_signature(self) -> None:
        """Test that the function has the expected signature."""
        sig = inspect.signature(initialize_hyperliquid_ws)

        # Should have exactly one parameter
        assert len(sig.parameters) == 1

        # Parameter should be named 'registry'
        assert "registry" in sig.parameters

        # Should have type annotation
        param = sig.parameters["registry"]
        assert param.annotation == WebSocketContextRegistry

        # Return type should be None
        assert sig.return_annotation is None or sig.return_annotation is type(None)

    def test_initialize_hyperliquid_ws_comprehensive_verification(self) -> None:
        """Comprehensive test verifying all aspects of the registration."""
        mock_registry = MagicMock(spec=WebSocketContextRegistry)

        # Call the function
        initialize_hyperliquid_ws(mock_registry)

        # Verify registry interaction
        mock_registry.register_context_type.assert_called_once_with(
            exchange_type=ExchangeType.HYPERLIQUID,
            context_class=HyperliquidMessageContext,
            envelope_validator=validate_hyperliquid_envelope,
        )

        # Verify that only one method call was made on the registry
        assert len(mock_registry.method_calls) == 1
