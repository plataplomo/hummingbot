"""Simple unit tests for HyperliquidErrorMapper focusing on public API behavior."""

import pytest

from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper
from cyberdelta.apis.hyperliquid.models.hl_raw_api_error import HyperliquidRawApiError


class TestHyperliquidErrorMapperBasic:
    """Test basic error mapper functionality."""

    def test_init_creates_mapper(self) -> None:
        """Test that error mapper initializes correctly."""
        mapper = HyperliquidErrorMapper()

        assert mapper is not None
        assert hasattr(mapper, "map_string_error")
        assert hasattr(mapper, "map_exchange_error")

    def test_map_string_error_returns_api_error(self) -> None:
        """Test that map_string_error returns an APIError."""
        mapper = HyperliquidErrorMapper()

        api_error = mapper.map_string_error("test error message")

        assert isinstance(api_error, APIError)
        assert isinstance(api_error.code, int)
        assert isinstance(api_error.message, str)

    def test_map_exchange_error_with_status_code(self) -> None:
        """Test map_exchange_error with status code and error body."""
        mapper = HyperliquidErrorMapper()

        api_error = mapper.map_exchange_error(
            status_code=400, error_body="Bad Request", error_data=None
        )

        assert isinstance(api_error, APIError)
        assert isinstance(api_error.code, int)
        assert isinstance(api_error.message, str)

    def test_map_string_error_with_known_patterns(self) -> None:
        """Test that known error patterns are recognized."""
        mapper = HyperliquidErrorMapper()

        # Test with patterns that should be recognized
        test_cases = [
            "unauthorized",
            "insufficient margin",
            "order not found",
            "rate limit exceeded",
        ]

        for error_msg in test_cases:
            api_error = mapper.map_string_error(error_msg)
            assert isinstance(api_error, APIError)
            assert error_msg in api_error.message.lower()

    def test_map_string_error_with_unknown_pattern(self) -> None:
        """Test that unknown patterns return appropriate error code."""
        mapper = HyperliquidErrorMapper()

        api_error = mapper.map_string_error("completely unknown error message")

        assert isinstance(api_error, APIError)
        # Unknown patterns should map to EXCHANGE_SPECIFIC (201)
        assert api_error.code == APIErrorCode.EXCHANGE_SPECIFIC.value

    @pytest.mark.parametrize("status_code", [400, 401, 403, 404, 429, 500, 502, 503])
    def test_map_exchange_error_different_status_codes(self, status_code: int) -> None:
        """Test map_exchange_error with different HTTP status codes."""
        mapper = HyperliquidErrorMapper()

        api_error = mapper.map_exchange_error(
            status_code=status_code, error_body="Test error", error_data=None
        )

        assert isinstance(api_error, APIError)
        assert isinstance(api_error.code, int)
        assert "Test error" in api_error.message

    def test_hyperliquid_raw_api_error_model(self) -> None:
        """Test that HyperliquidRawApiError model works correctly."""
        raw_error = HyperliquidRawApiError(error="Test error message")

        assert raw_error.error == "Test error message"

        # Test that we can use it with the mapper
        mapper = HyperliquidErrorMapper()
        api_error = mapper.map_string_error(raw_error.error)

        assert isinstance(api_error, APIError)
        assert "Test error message" in api_error.message
