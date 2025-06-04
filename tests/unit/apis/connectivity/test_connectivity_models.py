"""Unit tests for connectivity models.
Tests the Pydantic models used for HTTP client and WebSocket manager configuration.
"""

from typing import Any

import pytest
from pydantic import AnyUrl, HttpUrl, ValidationError

from cyberdelta.apis.connectivity.connectivity_models import (
    MAX_CONTENT_TYPE_LENGTH,
    HttpClientConfig,
    ProcessedResponseHeaders,
    WebSocketManagerConfig,
)


class TestProcessedResponseHeaders:
    """Tests for the ProcessedResponseHeaders model."""

    def test_valid_instantiation_defaults(self) -> None:
        """Test successful instantiation with default values."""
        headers = ProcessedResponseHeaders()
        assert headers.content_type == ""
        assert headers.model_config.get("frozen") is True
        assert headers.model_config.get("extra") == "forbid"

    @pytest.mark.parametrize(
        "valid_content_type",
        [
            "application/json",
            "text/html; charset=utf-8",
            "application/vnd.api+json",
            "",
            "a" * MAX_CONTENT_TYPE_LENGTH,
        ],
    )
    def test_valid_content_type(self, valid_content_type: str) -> None:
        """Test with various valid content_type values."""
        headers = ProcessedResponseHeaders(content_type=valid_content_type)
        assert headers.content_type == valid_content_type

    @pytest.mark.parametrize(
        "invalid_content_type, expected_error_part",
        [
            ("a" * (MAX_CONTENT_TYPE_LENGTH + 1), "string should have at most 256 characters"),
            ("application/json\\n", "invalid characters"),
            ("\\t\\t", "content-type contains invalid character"),
            (" ", "content-type cannot be only whitespace"),
            ("你好世界", "invalid characters"),
        ],
    )
    def test_invalid_content_type(
        self, invalid_content_type: str, expected_error_part: str,
    ) -> None:
        """Test with invalid content_type values, expecting ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            ProcessedResponseHeaders(content_type=invalid_content_type)
        assert expected_error_part.lower() in str(exc_info.value).lower()

    def test_frozen_behavior(self) -> None:
        """Test that the model is frozen."""
        headers = ProcessedResponseHeaders(content_type="application/json")
        with pytest.raises(ValidationError) as exc_info:
            headers.content_type = "new/type"
        assert "frozen" in str(exc_info.value).lower()

    def test_extra_forbid_behavior(self) -> None:
        """Test that extra fields are forbidden."""
        # Test by creating a model with extra field using model_validate
        invalid_data = {"content_type": "app/json", "extra_field": "bad"}
        with pytest.raises(ValidationError) as exc_info:
            ProcessedResponseHeaders.model_validate(invalid_data)
        assert "extra inputs are not permitted" in str(exc_info.value).lower()


class TestHttpClientConfig:
    """Tests for the HttpClientConfig model."""

    def test_valid_instantiation_defaults(self) -> None:
        """Test successful instantiation with default values and a valid URL."""
        config = HttpClientConfig(rest_endpoint=HttpUrl("https://api.example.com"))
        assert (
            str(config.rest_endpoint) == "https://api.example.com/"
        )  # Pydantic adds trailing slash
        assert config.default_request_timeout == 30.0
        assert config.max_retries == 3
        assert config.retry_delay_seconds == 5.0
        assert config.model_config.get("frozen") is True
        assert config.model_config.get("extra") == "forbid"

    def test_valid_instantiation_custom_values(self) -> None:
        """Test successful instantiation with custom valid values."""
        config = HttpClientConfig(
            rest_endpoint=HttpUrl("http://localhost:8080/v1"),
            default_request_timeout=10.5,
            max_retries=5,
            retry_delay_seconds=2.0,
        )
        assert str(config.rest_endpoint) == "http://localhost:8080/v1"  # No slash if path present
        assert config.default_request_timeout == 10.5
        assert config.max_retries == 5
        assert config.retry_delay_seconds == 2.0

    def test_valid_optional_fields_none(self) -> None:
        """Test with optional fields explicitly set to None, using defaults."""
        config = HttpClientConfig(
            rest_endpoint=HttpUrl("https://api.example.com"),
            max_retries=None,
            retry_delay_seconds=None,
        )
        assert config.max_retries is None
        assert config.retry_delay_seconds is None

    @pytest.mark.parametrize(
        "field, invalid_value, error_part",
        [
            ("rest_endpoint", "not_a_url", "input should be a valid url"),
            ("default_request_timeout", 0.0, "Input should be greater than 0"),
            ("default_request_timeout", -1.0, "Input should be greater than 0"),
            ("default_request_timeout", 120.1, "Input should be less than or equal to 120"),
            ("max_retries", -1, "Input should be greater than or equal to 0"),
            ("max_retries", 11, "Input should be less than or equal to 10"),
            ("retry_delay_seconds", 0.0, "Input should be greater than 0"),
            ("retry_delay_seconds", -1.0, "Input should be greater than 0"),
            ("retry_delay_seconds", 300.1, "Input should be less than or equal to 300"),
        ],
    )
    def test_invalid_field_values(
        self, field: str, invalid_value: str | float, error_part: str,
    ) -> None:
        """Test invalid values for various fields, expecting ValidationError."""
        init_data_corrected: dict[str, Any] = {"rest_endpoint": HttpUrl("https://api.example.com")}
        init_data_corrected[field] = invalid_value

        with pytest.raises(ValidationError) as exc_info:
            HttpClientConfig(**init_data_corrected)
        assert error_part.lower() in str(exc_info.value).lower()

    def test_frozen_behavior(self) -> None:
        """Test that the model is frozen."""
        config = HttpClientConfig(rest_endpoint=HttpUrl("https://api.example.com"))
        with pytest.raises(ValidationError) as exc_info:
            config.default_request_timeout = 15.0
        assert "frozen" in str(exc_info.value).lower()

    def test_extra_forbid_behavior(self) -> None:
        """Test that extra fields are forbidden."""
        # Test by creating a model with extra field using model_validate
        invalid_data = {
            "rest_endpoint": "https://api.example.com",
            "unknown_field": "test",
        }
        with pytest.raises(ValidationError) as exc_info:
            HttpClientConfig.model_validate(invalid_data)
        assert "extra inputs are not permitted" in str(exc_info.value).lower()

    def test_max_retries_validation(self) -> None:
        """Test max_retries validation."""
        config_no_validation = HttpClientConfig(
            rest_endpoint=HttpUrl("http://example.com"),
            default_request_timeout=10.0,
            max_retries=1,
            retry_delay_seconds=1.0,
        )
        assert config_no_validation.max_retries == 1

        config_zero_retries = HttpClientConfig(
            rest_endpoint=HttpUrl("http://example.com"),
            default_request_timeout=10.0,
            max_retries=0,
        )
        assert config_zero_retries.max_retries == 0

        config_none_retries = HttpClientConfig(
            rest_endpoint=HttpUrl("http://example.com"),
            default_request_timeout=10.0,
            max_retries=None,
            retry_delay_seconds=1.0,
        )
        assert config_none_retries.max_retries is None


class TestWebSocketManagerConfig:
    """Tests for the WebSocketManagerConfig model."""

    def test_valid_instantiation_defaults(self) -> None:
        """Test successful instantiation with default values and a valid URL."""
        config = WebSocketManagerConfig(ws_url=AnyUrl("wss://ws.example.com/socket"))
        assert str(config.ws_url) == "wss://ws.example.com/socket"
        assert config.ping_interval == 30.0
        assert config.reconnect_delay == 5.0
        assert config.max_reconnect_attempts == 10
        assert config.connection_timeout == 30.0
        assert config.model_config.get("frozen") is True
        assert config.model_config.get("extra") == "forbid"

    def test_valid_instantiation_custom_values(self) -> None:
        """Test successful instantiation with custom valid values."""
        config = WebSocketManagerConfig(
            ws_url=AnyUrl("ws://localhost:9000"),
            ping_interval=15.0,
            reconnect_delay=2.5,
            max_reconnect_attempts=5,
            connection_timeout=10.0,
        )
        assert str(config.ws_url) == "ws://localhost:9000/"
        assert config.ping_interval == 15.0
        assert config.reconnect_delay == 2.5
        assert config.max_reconnect_attempts == 5
        assert config.connection_timeout == 10.0

    @pytest.mark.parametrize(
        "field, invalid_value, error_part",
        [
            ("ws_url", "not a websocket url", "input should be a valid url"),
            ("ping_interval", 0.0, "Input should be greater than 0"),
            ("ping_interval", -5.0, "Input should be greater than 0"),
            ("ping_interval", 60.1, "Input should be less than or equal to 60"),
            ("reconnect_delay", 0.0, "Input should be greater than 0"),
            ("reconnect_delay", -1.0, "Input should be greater than 0"),
            ("reconnect_delay", 300.1, "Input should be less than or equal to 300"),
            ("max_reconnect_attempts", -1, "Input should be greater than or equal to 0"),
            ("max_reconnect_attempts", 21, "Input should be less than or equal to 20"),
            ("connection_timeout", 0.0, "Input should be greater than 0"),
            ("connection_timeout", -10.0, "Input should be greater than 0"),
            ("connection_timeout", 120.1, "Input should be less than or equal to 120"),
        ],
    )
    def test_invalid_field_values(
        self, field: str, invalid_value: str | float, error_part: str,
    ) -> None:
        """Test invalid values for various fields, expecting ValidationError."""
        init_data_corrected: dict[str, Any] = {"ws_url": AnyUrl("wss://ws.example.com")}
        init_data_corrected[field] = invalid_value

        with pytest.raises(ValidationError) as exc_info:
            WebSocketManagerConfig(**init_data_corrected)
        assert error_part.lower() in str(exc_info.value).lower()

    def test_frozen_behavior(self) -> None:
        """Test that the model is frozen."""
        config = WebSocketManagerConfig(ws_url=AnyUrl("wss://ws.example.com"))
        with pytest.raises(ValidationError) as exc_info:
            config.ping_interval = 10.0
        assert "frozen" in str(exc_info.value).lower()

    def test_extra_forbid_behavior(self) -> None:
        """Test that extra fields are forbidden."""
        # Test by creating a model with extra field using model_validate
        invalid_data = {
            "ws_url": "wss://ws.example.com",
            "some_other_param": "value",
        }
        with pytest.raises(ValidationError) as exc_info:
            WebSocketManagerConfig.model_validate(invalid_data)
        assert "extra inputs are not permitted" in str(exc_info.value).lower()

    def test_max_retries_validation(self) -> None:
        """Test max_reconnect_attempts validation."""
        config_valid_retries = WebSocketManagerConfig(
            ws_url=AnyUrl("ws://example.com"),
            ping_interval=10.0,
            reconnect_delay=5.0,
            max_reconnect_attempts=1,
            connection_timeout=10.0,
        )
        assert config_valid_retries.max_reconnect_attempts == 1

        config_zero_retries = WebSocketManagerConfig(
            ws_url=AnyUrl("ws://example.com"), max_reconnect_attempts=0,
        )
        assert config_zero_retries.max_reconnect_attempts == 0

        config_default_retries = WebSocketManagerConfig(ws_url=AnyUrl("ws://example.com"))
        assert config_default_retries.max_reconnect_attempts == 10

    def test_ping_interval_validation(self) -> None:
        """Test ping_interval validation."""
        config_valid_ping = WebSocketManagerConfig(
            ws_url=AnyUrl("ws://example.com"),
            ping_interval=1.0,
        )
        assert config_valid_ping.ping_interval == 1.0

        config_default_ping = WebSocketManagerConfig(ws_url=AnyUrl("ws://example.com"))
        assert config_default_ping.ping_interval == 30.0
