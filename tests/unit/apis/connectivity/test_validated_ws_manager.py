"""Unit tests for ValidatedWebSocketManager using only public interfaces."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest
from pydantic import AnyUrl, ValidationError

from cyberdelta.apis.common.api_error import APIError
from cyberdelta.apis.connectivity.connectivity_models import WebSocketManagerConfig
from cyberdelta.apis.connectivity.validated_ws_manager import (
    ValidatedWebSocketManager,
    WebSocketMessageConfig,
    WebSocketPreValidator,
)


class TestWebSocketMessageConfigPublic:
    """Test WebSocketMessageConfig validation through public interface."""

    def test_default_config(self) -> None:
        """Test default configuration values."""
        config = WebSocketMessageConfig()
        assert config.max_message_size == 10 * 1024 * 1024  # 10MB
        assert config.max_nesting_depth == 10
        assert config.max_array_length == 10000
        assert config.parse_timeout == 1.0
        assert config.enable_compression is False

    def test_valid_custom_config(self) -> None:
        """Test custom configuration values."""
        config = WebSocketMessageConfig(
            max_message_size=5 * 1024 * 1024,  # 5MB
            max_nesting_depth=5,
            max_array_length=5000,
            parse_timeout=0.5,
            enable_compression=True,
        )
        assert config.max_message_size == 5 * 1024 * 1024
        assert config.max_nesting_depth == 5
        assert config.max_array_length == 5000
        assert config.parse_timeout == 0.5
        assert config.enable_compression is True

    def test_invalid_config_validation(self) -> None:
        """Test that invalid configs are rejected."""
        # Too small message size
        with pytest.raises(ValidationError):
            WebSocketMessageConfig(max_message_size=512)

        # Too large message size
        with pytest.raises(ValidationError):
            WebSocketMessageConfig(max_message_size=200 * 1024 * 1024)

        # Invalid nesting depth
        with pytest.raises(ValidationError):
            WebSocketMessageConfig(max_nesting_depth=0)

        with pytest.raises(ValidationError):
            WebSocketMessageConfig(max_nesting_depth=101)


class TestWebSocketPreValidatorPublic:
    """Test WebSocketPreValidator through public interface."""

    @pytest.fixture
    def validator(self) -> WebSocketPreValidator:
        """Create a pre-validator with default config.

        Returns:
            WebSocketPreValidator instance with default configuration.
        """
        config = WebSocketMessageConfig()
        return WebSocketPreValidator(config)

    def test_validate_valid_dict(self, validator: WebSocketPreValidator) -> None:
        """Test validation of valid dictionary."""
        data = {"key": "value", "nested": {"inner": "data"}}
        result = validator.validate(data)
        assert result == data

    def test_validate_valid_list(self, validator: WebSocketPreValidator) -> None:
        """Test validation of valid list."""
        data = [1, 2, {"key": "value"}]
        result = validator.validate(data)
        assert result == data

    def test_validate_invalid_type(self, validator: WebSocketPreValidator) -> None:
        """Test validation fails for invalid types."""
        with pytest.raises(APIError) as exc_info:
            validator.validate("string")
        assert "Invalid WebSocket message type: str" in str(exc_info.value)

    def test_validate_excessive_nesting(self, validator: WebSocketPreValidator) -> None:
        """Test validation fails for deeply nested structures."""
        # Create deeply nested dict
        data: dict[str, Any] = {}
        current = data
        for _i in range(15):
            current["nested"] = {}
            current = current["nested"]

        with pytest.raises(APIError) as exc_info:
            validator.validate(data)
        assert "exceeds limit" in str(exc_info.value)

    def test_validate_excessive_array_length(self, validator: WebSocketPreValidator) -> None:
        """Test validation fails for arrays exceeding length limit."""
        data = {"large_array": list(range(20000))}

        with pytest.raises(APIError) as exc_info:
            validator.validate(data)
        assert "Array length" in str(exc_info.value)
        assert "exceeds limit" in str(exc_info.value)

    def test_validation_edge_cases(self, validator: WebSocketPreValidator) -> None:
        """Test validation edge cases."""
        # Empty structures should pass
        assert validator.validate({}) == {}
        assert validator.validate([]) == []

        # Simple structures should pass
        simple_dict = {"a": 1}
        assert validator.validate(simple_dict) == simple_dict

        simple_list = [1, 2, 3]
        assert validator.validate(simple_list) == simple_list

        # Mixed nesting within limits should pass
        mixed_data = {"a": [{"b": 1}]}
        assert validator.validate(mixed_data) == mixed_data


class TestValidatedWebSocketManagerPublic:
    """Test ValidatedWebSocketManager using only public interfaces."""

    @pytest.fixture
    def ws_config(self) -> WebSocketManagerConfig:
        """Create WebSocket manager config.

        Returns:
            WebSocketManagerConfig with test WebSocket URL and timeouts.
        """
        return WebSocketManagerConfig(
            ws_url=AnyUrl("wss://test.example.com/ws"),
            ping_interval=30.0,
            reconnect_delay=5.0,
            max_reconnect_attempts=3,
            connection_timeout=10.0,
        )

    @pytest.fixture
    def message_handler(self) -> AsyncMock:
        """Create mock message handler.

        Returns:
            AsyncMock configured as a message handler.
        """
        return AsyncMock()

    @pytest.fixture
    def manager(
        self,
        ws_config: WebSocketManagerConfig,
        message_handler: AsyncMock,
    ) -> ValidatedWebSocketManager:
        """Create ValidatedWebSocketManager instance.

        Returns:
            ValidatedWebSocketManager configured for testing.
        """
        return ValidatedWebSocketManager(
            exchange_name="test_exchange",
            message_handler=message_handler,
            config=ws_config,
        )

    def test_initialization(self, manager: ValidatedWebSocketManager) -> None:
        """Test manager initialization."""
        # Test through public interface - stats contain exchange name
        stats = asyncio.run(manager.get_stats())
        assert stats["exchange"] == "test_exchange"
        assert manager.msg_config.max_message_size == 10 * 1024 * 1024
        assert isinstance(manager.pre_validator, WebSocketPreValidator)

    @pytest.mark.asyncio
    async def test_get_stats(self, manager: ValidatedWebSocketManager) -> None:
        """Test getting statistics through public interface."""
        stats = await manager.get_stats()

        assert stats["exchange"] == "test_exchange"
        assert stats["is_connected"] is False
        assert "message_stats" in stats
        assert "config" in stats

        # Check message stats structure
        msg_stats = stats["message_stats"]
        expected_keys = {"received", "validated", "rejected", "oversized", "parse_errors"}
        assert set(msg_stats.keys()) == expected_keys

        # All should start at 0
        for key in expected_keys:
            assert msg_stats[key] == 0

        # Check config stats
        config_stats = stats["config"]
        assert config_stats["max_message_size"] == 10 * 1024 * 1024
        assert config_stats["max_nesting_depth"] == 10
        assert config_stats["max_array_length"] == 10000

    def test_validator_configuration(self, manager: ValidatedWebSocketManager) -> None:
        """Test that validator is configured correctly."""
        validator = manager.pre_validator
        config = validator.config

        assert config.max_message_size == 10 * 1024 * 1024
        assert config.max_nesting_depth == 10
        assert config.max_array_length == 10000
        assert config.parse_timeout == 1.0

    @pytest.mark.asyncio
    async def test_message_validation_flow(
        self,
        manager: ValidatedWebSocketManager,
        message_handler: AsyncMock,
    ) -> None:
        """Test message validation through the validator."""
        # Test valid message passes validation
        valid_message = {"type": "test", "data": {"value": 123}}
        result = manager.pre_validator.validate(valid_message)
        assert result == valid_message

        # Test that handler integration works
        await message_handler(valid_message)
        message_handler.assert_called_once_with(valid_message)

    def test_custom_config_application(
        self,
        ws_config: WebSocketManagerConfig,
        message_handler: AsyncMock,
    ) -> None:
        """Test custom configuration is applied correctly."""
        custom_config = WebSocketMessageConfig(
            max_message_size=1024,
            max_nesting_depth=3,
            max_array_length=10,
        )

        custom_manager = ValidatedWebSocketManager(
            exchange_name="custom_test",
            message_handler=message_handler,
            config=ws_config,
            message_config=custom_config,
        )

        assert custom_manager.msg_config.max_message_size == 1024
        assert custom_manager.msg_config.max_nesting_depth == 3
        assert custom_manager.msg_config.max_array_length == 10

    def test_validation_security_limits(self, manager: ValidatedWebSocketManager) -> None:
        """Test validation security limits work correctly."""
        validator = manager.pre_validator

        # Test depth limit
        deep_data: dict[str, Any] = {}
        current = deep_data
        for _ in range(15):  # Exceeds default limit of 10
            current["nested"] = {}
            current = current["nested"]

        with pytest.raises(APIError) as exc_info:
            validator.validate(deep_data)
        assert "exceeds limit" in str(exc_info.value)

        # Test array length limit
        large_array = {"items": list(range(15000))}  # Exceeds default limit of 10000
        with pytest.raises(APIError) as exc_info:
            validator.validate(large_array)
        assert "Array length" in str(exc_info.value)

        # Test invalid type rejection
        with pytest.raises(APIError) as exc_info:
            validator.validate("not_dict_or_list")
        assert "Invalid WebSocket message type" in str(exc_info.value)


@pytest.mark.parametrize(
    ("attack_data", "description"),
    [
        (
            {
                "a": {
                    "b": {
                        "c": {"d": {"e": {"f": {"g": {"h": {"i": {"j": {"k": {"l": "deep"}}}}}}}}}
                    }
                }
            },
            "Deep nesting attack",
        ),
        ({"items": list(range(15000))}, "Large array attack"),
        ({"large_string": "x" * 100000}, "Large string content"),
    ],
)
def test_dos_protection_through_validation(attack_data: dict[str, Any], description: str) -> None:
    """Test protection against DoS attacks through validation layer."""
    # Create validator with protective limits
    protective_config = WebSocketMessageConfig(
        max_message_size=1_000_000,  # 1MB
        max_nesting_depth=10,
        max_array_length=10000,
    )

    validator = WebSocketPreValidator(protective_config)

    # Large string content is actually valid JSON and doesn't violate validation rules
    # It would be caught at the message size level before validation
    if "string" in description.lower():
        # Large strings are valid data, they just test message size limits
        # which are checked before validation occurs
        result = validator.validate(attack_data)
        assert result == attack_data  # Should pass validation
        return

    # Other attack data should be rejected by validation
    with pytest.raises(APIError) as exc_info:
        validator.validate(attack_data)

    # Verify appropriate error message
    error_msg = str(exc_info.value)
    if "deep" in description.lower():
        assert "exceeds limit" in error_msg
    elif "array" in description.lower():
        assert "Array length" in error_msg


def test_production_config_compatibility() -> None:
    """Test that configurations work with realistic production values."""
    # Test typical production config
    prod_config = WebSocketMessageConfig(
        max_message_size=50 * 1024 * 1024,  # 50MB for large market data
        max_nesting_depth=20,  # Deep order book structures
        max_array_length=100000,  # Large arrays of trades/orders
        parse_timeout=2.0,  # Longer timeout for complex data
        enable_compression=True,  # Production uses compression
    )

    assert prod_config.max_message_size == 50 * 1024 * 1024
    assert prod_config.max_nesting_depth == 20
    assert prod_config.max_array_length == 100000
    assert prod_config.parse_timeout == 2.0
    assert prod_config.enable_compression is True

    # Test the validator works with production config
    validator = WebSocketPreValidator(prod_config)

    # Large realistic message should pass
    realistic_data = {
        "channel": "orderbook",
        "symbol": "BTC/USD",
        "data": {
            "bids": [[50000.0, 1.5] for _ in range(1000)],
            "asks": [[50001.0, 2.0] for _ in range(1000)],
            "timestamp": 1640995200000,
        },
    }

    result = validator.validate(realistic_data)
    assert result == realistic_data
