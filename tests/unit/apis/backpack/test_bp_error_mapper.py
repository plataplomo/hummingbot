import pytest

from cyberdelta.apis.backpack.bp_error_mapper import BackpackErrorMapper
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode


@pytest.fixture
def backpack_error_mapper() -> BackpackErrorMapper:
    """Provides a BackpackErrorMapper instance for tests."""
    return BackpackErrorMapper()


def test_map_generic_400_error(backpack_error_mapper: BackpackErrorMapper):
    """Test mapping a generic 400 Bad Request error."""
    error = backpack_error_mapper.map_exchange_error(
        status_code=400,
        error_body='{"error": "Invalid request parameters", "code": 1001}',
        error_data={"error": "Invalid request parameters", "code": 1001},
        request_path="/api/v1/test",
    )
    assert isinstance(error, APIError)
    assert error.code == APIErrorCode.INVALID_REQUEST.value
    assert error.http_status == 400
    assert "Invalid request parameters" in error.message
    assert error.exchange_message == '{"error": "Invalid request parameters", "code": 1001}'


def test_map_authentication_failed_401_error(backpack_error_mapper: BackpackErrorMapper):
    """Test mapping a 401 Unauthorized error."""
    error_body_str = '{"error": "Authentication failed", "code": 2000}'
    error = backpack_error_mapper.map_exchange_error(
        status_code=401,
        error_body=error_body_str,
        error_data={"error": "Authentication failed", "code": 2000},
        request_path="/wapi/v1/capital",
    )
    assert isinstance(error, APIError)
    assert error.code == APIErrorCode.AUTHENTICATION_FAILED.value
    assert error.http_status == 401
    assert "Authentication failed" in error.message
    assert error.exchange_message == error_body_str


def test_map_insufficient_funds_error(backpack_error_mapper: BackpackErrorMapper):
    """Test mapping an insufficient funds error (often 400 or specific code)."""
    # Backpack might return a specific error message/code for this.
    # Example: {"error":"Account has insufficient balance for requested action.","code":10004}
    error_body_str = (
        '{"error":"Account has insufficient balance for requested action.","code":10004}'
    )
    error = backpack_error_mapper.map_exchange_error(
        status_code=400,  # Assuming 400 for this example
        error_body=error_body_str,
        error_data={
            "error": "Account has insufficient balance for requested action.",
            "code": 10004,
        },
        request_path="/api/v1/order",
    )
    assert isinstance(error, APIError)
    assert error.code == APIErrorCode.INSUFFICIENT_FUNDS.value
    assert error.http_status == 400
    assert "Account has insufficient balance" in error.message
    assert error.exchange_message == error_body_str


def test_map_rate_limited_429_error(backpack_error_mapper: BackpackErrorMapper):
    """Test mapping a 429 Too Many Requests error."""
    error_body_str = '{"error":"Too many requests", "code": 3000}'
    error = backpack_error_mapper.map_exchange_error(
        status_code=429,
        error_body=error_body_str,
        error_data={"error": "Too many requests", "code": 3000},
        request_path="/api/v1/ticker",
    )
    assert isinstance(error, APIError)
    assert error.code == APIErrorCode.RATE_LIMITED.value
    assert error.http_status == 429
    assert "Too many requests" in error.message
    assert error.exchange_message == error_body_str


def test_map_server_error_500(backpack_error_mapper: BackpackErrorMapper):
    """Test mapping a generic 500 Internal Server Error."""
    error_body_str = "Internal Server Error"
    error = backpack_error_mapper.map_exchange_error(
        status_code=500,
        error_body=error_body_str,
        error_data=None,  # No structured data for plain text
        request_path="/api/v1/system",
    )
    assert isinstance(error, APIError)
    assert error.code == APIErrorCode.SERVER_ERROR.value
    assert error.http_status == 500
    assert "Internal Server Error" in error.message
    assert error.exchange_message == error_body_str


def test_map_service_unavailable_503(backpack_error_mapper: BackpackErrorMapper):
    """Test mapping a 503 Service Unavailable error."""
    error_body_str = "Service temporarily unavailable"
    error = backpack_error_mapper.map_exchange_error(
        status_code=503, error_body=error_body_str, error_data=None, request_path="/api/v1/status"
    )
    assert isinstance(error, APIError)
    assert error.code == APIErrorCode.SERVICE_UNAVAILABLE.value
    assert error.http_status == 503
    assert "Service temporarily unavailable" in error.message
    assert error.exchange_message == error_body_str


def test_map_order_not_found_error(backpack_error_mapper: BackpackErrorMapper):
    """Test mapping an order not found error."""
    # Example: {"error":"Order not found","code":10007}
    error_body_str = '{"error":"Order not found","code":10007}'
    error = backpack_error_mapper.map_exchange_error(
        status_code=404,  # Often 404, but could be 400 with specific code
        error_body=error_body_str,
        error_data={"error": "Order not found", "code": 10007},
        request_path="/api/v1/order",
    )
    assert isinstance(error, APIError)
    assert error.code == APIErrorCode.ORDER_NOT_FOUND.value
    assert error.http_status == 404
    assert "Order not found" in error.message


def test_map_invalid_symbol_error(backpack_error_mapper: BackpackErrorMapper):
    """Test mapping an invalid symbol error."""
    # Example: {"error":"Invalid symbol: XYZ_USDC","code":10001}
    error_body_str = '{"error":"Invalid symbol: XYZ_USDC","code":10001}'
    error = backpack_error_mapper.map_exchange_error(
        status_code=400,
        error_body=error_body_str,
        error_data={"error": "Invalid symbol: XYZ_USDC", "code": 10001},
        request_path="/api/v1/ticker",
    )
    assert isinstance(error, APIError)
    assert error.code == APIErrorCode.INVALID_SYMBOL.value  # Or SYMBOL_NOT_FOUND
    assert error.http_status == 400
    assert "Invalid symbol" in error.message


def test_map_non_json_error_body(backpack_error_mapper: BackpackErrorMapper):
    """Test mapping when error_body is not valid JSON."""
    error_body_str = "<html><body><h1>Gateway Timeout</h1></body></html>"
    error = backpack_error_mapper.map_exchange_error(
        status_code=504,
        error_body=error_body_str,
        error_data=None,  # error_data would be None if body isn't JSON
        request_path="/api/v1/data",
    )
    assert isinstance(error, APIError)
    assert error.code == APIErrorCode.SERVICE_UNAVAILABLE.value  # Or a generic server error
    assert error.http_status == 504
    assert "Gateway Timeout" in error.message  # Mapper might extract from HTML or use generic
    assert error.exchange_message == error_body_str


def test_map_unknown_error_code_in_json(backpack_error_mapper: BackpackErrorMapper):
    """Test mapping when JSON error code is unknown but status is informative."""
    error_body_str = '{"error":"A new undefined error occurred","code":99999}'
    error = backpack_error_mapper.map_exchange_error(
        status_code=400,
        error_body=error_body_str,
        error_data={"error": "A new undefined error occurred", "code": 99999},
        request_path="/api/v1/action",
    )
    assert isinstance(error, APIError)
    assert error.code == APIErrorCode.INVALID_REQUEST.value  # Fallback based on 400
    assert error.http_status == 400
    assert "A new undefined error occurred" in error.message
    assert error.exchange_message == error_body_str


def test_map_empty_error_body_and_data(backpack_error_mapper: BackpackErrorMapper):
    """Test mapping when error_body and error_data are empty/None for a 403."""
    error = backpack_error_mapper.map_exchange_error(
        status_code=403, error_body="", error_data=None, request_path="/api/v1/forbidden_resource"
    )
    assert isinstance(error, APIError)
    assert error.code == APIErrorCode.AUTHENTICATION_FAILED.value  # Or a generic "Forbidden"
    assert error.http_status == 403
    assert "Forbidden" in error.message  # Default message for 403
    assert error.exchange_message == ""
