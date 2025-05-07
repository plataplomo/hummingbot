import pytest

from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode


@pytest.fixture
def hyperliquid_error_mapper() -> HyperliquidErrorMapper:
    """Provides a HyperliquidErrorMapper instance for tests."""
    return HyperliquidErrorMapper()


@pytest.mark.parametrize(
    "error_body_str, expected_code, expected_message_contains",
    [("Order not found", APIErrorCode.ORDER_NOT_FOUND, "Order not found")],
)
def test_map_hl_string_error_order_not_found(
    hyperliquid_error_mapper: HyperliquidErrorMapper,
) -> None:
    """Test mapping Hyperliquid's string error for 'Order not found'."""
    error_body_str = "Order not found"
    error = hyperliquid_error_mapper.map_exchange_error(
        status_code=200,  # HL often returns 200 with error string in body
        error_body=error_body_str,
        error_data=None,  # Usually no structured data for these string errors
        request_path="/info",
    )
    assert isinstance(error, APIError)
    assert error.code == APIErrorCode.ORDER_NOT_FOUND.value
    assert error.http_status == 200
    assert "Order not found" in error.message
    assert error.exchange_message == error_body_str


def test_map_hl_string_error_insufficient_margin(
    hyperliquid_error_mapper: HyperliquidErrorMapper,
) -> None:
    """Test mapping Hyperliquid's string error for insufficient margin."""
    error_body_str = "exchange: Insufficient margin"
    error = hyperliquid_error_mapper.map_exchange_error(
        status_code=200, error_body=error_body_str, error_data=None, request_path="/exchange"
    )
    assert isinstance(error, APIError)
    assert error.code == APIErrorCode.INSUFFICIENT_FUNDS.value
    assert error.http_status == 200
    assert "Insufficient margin" in error.message
    assert error.exchange_message == error_body_str


def test_map_hl_string_error_invalid_order_size(
    hyperliquid_error_mapper: HyperliquidErrorMapper,
) -> None:
    """Test mapping Hyperliquid's string error for invalid order size."""
    error_body_str = "Invalid order size"
    error = hyperliquid_error_mapper.map_exchange_error(
        status_code=200, error_body=error_body_str, error_data=None, request_path="/exchange"
    )
    assert isinstance(error, APIError)
    assert error.code == APIErrorCode.INVALID_ORDER_SIZE.value
    assert error.http_status == 200
    assert "Invalid order size" in error.message
    assert error.exchange_message == error_body_str


def test_map_hl_string_error_rate_limit(hyperliquid_error_mapper: HyperliquidErrorMapper) -> None:
    """Test mapping Hyperliquid's string error for rate limiting."""
    # Hyperliquid might also use 429, but sometimes string errors too
    error_body_str = "Ratelimit exceeded"
    error = hyperliquid_error_mapper.map_exchange_error(
        status_code=200,  # Or 429
        error_body=error_body_str,
        error_data=None,
        request_path="/info",
    )
    assert isinstance(error, APIError)
    assert error.code == APIErrorCode.RATE_LIMITED.value
    assert error.http_status == 200  # Or 429 if status code is used by mapper
    assert "Ratelimit exceeded" in error.message
    assert error.exchange_message == error_body_str


def test_map_hl_string_error_user_not_found(
    hyperliquid_error_mapper: HyperliquidErrorMapper,
) -> None:
    """Test mapping Hyperliquid's string error for user not found."""
    error_body_str = "User not found"
    error = hyperliquid_error_mapper.map_exchange_error(
        status_code=200, error_body=error_body_str, error_data=None, request_path="/info"
    )
    assert isinstance(error, APIError)
    # This could be AUTHENTICATION_FAILED or a more specific USER_NOT_FOUND if we add one
    assert error.code == APIErrorCode.AUTHENTICATION_FAILED.value
    assert error.http_status == 200
    assert "User not found" in error.message
    assert error.exchange_message == error_body_str


def test_map_hl_unknown_string_error(hyperliquid_error_mapper: HyperliquidErrorMapper) -> None:
    """Test mapping an unknown Hyperliquid string error."""
    error_body_str = "An unexpected problem occurred on Hyperliquid."
    error = hyperliquid_error_mapper.map_exchange_error(
        status_code=200, error_body=error_body_str, error_data=None, request_path="/exchange"
    )
    assert isinstance(error, APIError)
    assert error.code == APIErrorCode.EXCHANGE_SPECIFIC.value  # Fallback for unknown strings
    assert error.http_status == 200
    assert "An unexpected problem occurred" in error.message
    assert error.exchange_message == error_body_str


def test_map_hl_error_with_http_error_status(
    hyperliquid_error_mapper: HyperliquidErrorMapper,
) -> None:
    """Test mapping when Hyperliquid returns a non-200 status with an error string."""
    error_body_str = "Request failed due to reasons."
    error = hyperliquid_error_mapper.map_exchange_error(
        status_code=503, error_body=error_body_str, error_data=None, request_path="/exchange"
    )
    assert isinstance(error, APIError)
    assert (
        error.code == APIErrorCode.SERVICE_UNAVAILABLE.value
    )  # Mapper should use HTTP status if string is generic
    assert error.http_status == 503
    assert "Request failed due to reasons." in error.message  # Or more generic based on status
    assert error.exchange_message == error_body_str


def test_map_hl_empty_error_body(hyperliquid_error_mapper: HyperliquidErrorMapper) -> None:
    """Test mapping when error body is empty but status code indicates error."""
    error = hyperliquid_error_mapper.map_exchange_error(
        status_code=401, error_body="", error_data=None, request_path="/exchange"
    )
    assert isinstance(error, APIError)
    assert error.code == APIErrorCode.AUTHENTICATION_FAILED.value
    assert error.http_status == 401
    assert "Authentication failed" in error.message  # Default for 401
    assert error.exchange_message == ""
