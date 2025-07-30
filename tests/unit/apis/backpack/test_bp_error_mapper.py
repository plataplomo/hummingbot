"""Unit tests for Backpack Error Mapping Implementation.

Comprehensive test suite for the BackpackErrorMapper class covering:
- HTTP status code to API error code mapping
- Backpack-specific error code translation
- Error message extraction and formatting
- Retry-after parsing for rate limiting scenarios
- JSON and non-JSON error body handling
- Edge cases and malformed error responses

These tests ensure the error mapper correctly translates Backpack exchange errors
into standardized API error codes used throughout the CyberDeltaEngine trading system.
The mapper handles various error formats, extracts relevant information, and provides
consistent error handling across all Backpack API interactions.

Key test categories:
- Standard HTTP error code mapping (400, 401, 403, 429, 500, 503)
- Backpack-specific error code translation
- Rate limiting and retry-after parsing
- Authentication and authorization errors
- Insufficient funds and order-related errors
- Server errors and maintenance scenarios
- Malformed error response handling
"""

import contextlib
import json

import pytest

from cyberdelta.apis.backpack.bp_error_mapper import BackpackErrorMapper
from cyberdelta.apis.common import APIErrorCode


@pytest.fixture
def backpack_error_mapper() -> BackpackErrorMapper:
    """Create a BackpackErrorMapper instance for testing.

    Provides a fresh BackpackErrorMapper instance for each test to ensure
    isolation and consistent behavior across all error mapping tests.

    Returns:
        BackpackErrorMapper: An error mapper instance for testing.
    """
    return BackpackErrorMapper()


class TestBackpackErrorMapper:
    """Test suite for BackpackErrorMapper error translation functionality.

    Comprehensive testing of the Backpack exchange error mapping implementation
    including HTTP status code translation, exchange-specific error handling,
    retry-after parsing, and edge case scenarios. These tests ensure consistent
    error handling across all Backpack API interactions in the CyberDeltaEngine.

    Test coverage includes:
    - Standard HTTP error code mapping
    - Backpack-specific error code translation
    - Rate limiting and retry-after extraction
    - Authentication and authorization errors
    - Financial operation errors (insufficient funds, order not found)
    - Server errors and maintenance scenarios
    - Malformed error response handling
    """

    @pytest.mark.parametrize(
        ("http_status", "error_body", "expected_code", "expected_message_contains"),
        [
            (
                400,
                '{"message":"Generic client error","code":"INVALID_CLIENT_REQUEST"}',
                APIErrorCode.INVALID_REQUEST,
                "Generic client error",
            ),
            (
                400,
                '{"message":"Invalid parameter: field=symbol","code":"INVALID_CLIENT_REQUEST"}',
                APIErrorCode.INVALID_REQUEST,
                "Invalid parameter: field=symbol",
            ),
        ],
    )
    def test_map_generic_400_error(
        self,
        http_status: int,
        error_body: str,
        expected_code: APIErrorCode,
        expected_message_contains: str,
    ) -> None:
        """Test mapping of generic 400 Bad Request errors to standardized API error codes.

        Validates that the error mapper correctly translates generic client errors
        from Backpack into the appropriate INVALID_REQUEST error code while preserving
        the original error message for debugging purposes.
        """
        mapper = BackpackErrorMapper()
        api_error = mapper.map_exchange_error(
            http_status,
            error_body,
            error_data=json.loads(error_body),
        )
        assert api_error.code == expected_code.value
        assert expected_message_contains in api_error.message
        assert api_error.http_status == http_status
        assert api_error.exchange_message == json.loads(error_body)["message"]

    @pytest.mark.parametrize(
        ("http_status", "error_body", "expected_code", "expected_message_contains"),
        [
            (
                401,
                '{"message":"Authentication failed","code":"UNAUTHORIZED"}',
                APIErrorCode.AUTHENTICATION_FAILED,
                "Authentication failed",
            ),
            (
                403,
                '{"message":"Forbidden access","code":"FORBIDDEN"}',
                APIErrorCode.AUTHENTICATION_FAILED,  # Often 403 is also auth related
                "Forbidden access",
            ),
        ],
    )
    def test_map_authentication_failed_401_error(
        self,
        http_status: int,
        error_body: str,
        expected_code: APIErrorCode,
        expected_message_contains: str,
    ) -> None:
        """Test mapping of authentication and authorization errors to AUTHENTICATION_FAILED.

        Validates that both 401 Unauthorized and 403 Forbidden errors are correctly
        mapped to AUTHENTICATION_FAILED, as both typically indicate credential or
        permission issues in trading contexts.
        """
        mapper = BackpackErrorMapper()
        api_error = mapper.map_exchange_error(
            http_status,
            error_body,
            error_data=json.loads(error_body),
        )
        assert api_error.code == expected_code.value
        assert expected_message_contains in api_error.message
        assert api_error.http_status == http_status

    @pytest.mark.parametrize(
        (
            "http_status",
            "error_body",
            "bp_error_code_str",
            "expected_api_code",
            "expected_message_contains",
        ),
        [
            (
                400,
                '{"message":"Account has insufficient balance for requested action.",'
                '"code":"INSUFFICIENT_FUNDS"}',
                "INSUFFICIENT_FUNDS",
                APIErrorCode.INSUFFICIENT_FUNDS,
                "Account has insufficient balance",
            ),
            (
                400,  # Example: Order not found might be 400 or 404 depending on API
                '{"message":"Order not found or has been filled","code":"RESOURCE_NOT_FOUND"}',
                "RESOURCE_NOT_FOUND",
                APIErrorCode.ORDER_NOT_FOUND,
                "Order not found or has been filled",
            ),
        ],
    )
    def test_map_insufficient_funds_error(
        self,
        http_status: int,
        error_body: str,
        bp_error_code_str: str,
        expected_api_code: APIErrorCode,
        expected_message_contains: str,
    ) -> None:
        """Test mapping of financial operation errors to specific API error codes.

        Validates that Backpack-specific error codes like INSUFFICIENT_FUNDS and
        RESOURCE_NOT_FOUND are correctly translated to their corresponding API
        error codes, enabling proper error handling in trading operations.
        """
        mapper = BackpackErrorMapper()
        api_error = mapper.map_exchange_error(
            http_status,
            error_body,
            error_data=json.loads(error_body),
        )
        assert api_error.code == expected_api_code.value
        assert expected_message_contains in api_error.message
        assert api_error.http_status == http_status

    @pytest.mark.parametrize(
        ("http_status", "error_body", "expected_code", "expected_message_contains"),
        [
            (
                429,
                '{"message":"Too Many Requests","code":"TOO_MANY_REQUESTS"}',
                APIErrorCode.RATE_LIMITED,
                "Too Many Requests",
            ),
        ],
    )
    def test_map_rate_limited_429_error(
        self,
        http_status: int,
        error_body: str,
        expected_code: APIErrorCode,
        expected_message_contains: str,
    ) -> None:
        """Test mapping of rate limiting errors to RATE_LIMITED API error code.

        Validates that 429 Too Many Requests errors are correctly mapped to
        RATE_LIMITED, which is essential for implementing proper backoff
        strategies in the trading system.
        """
        mapper = BackpackErrorMapper()
        api_error = mapper.map_exchange_error(
            http_status,
            error_body,
            error_data=json.loads(error_body),
        )
        assert api_error.code == expected_code.value
        assert expected_message_contains in api_error.message
        assert api_error.http_status == http_status

    @pytest.mark.parametrize(
        ("http_status", "error_body", "expected_code", "expected_message_contains"),
        [
            (
                500,
                '{"message":"Internal server error","code":"SERVER_ERROR"}',
                APIErrorCode.SERVER_ERROR,
                "Internal server error",
            ),
            (500, "Internal Server Error", APIErrorCode.SERVER_ERROR, "Internal Server Error"),
        ],
    )
    def test_map_server_error_500(
        self,
        http_status: int,
        error_body: str,
        expected_code: APIErrorCode,
        expected_message_contains: str,
    ) -> None:
        """Test mapping of server errors to appropriate API error codes.

        Validates that 500 Internal Server Error responses are correctly mapped,
        handling both JSON and plain text error formats. This ensures proper
        error handling when Backpack experiences server-side issues.
        """
        mapper = BackpackErrorMapper()
        # error_data might not be parsable if body is not JSON
        error_data = None
        with contextlib.suppress(json.JSONDecodeError):
            error_data = json.loads(error_body)
        api_error = mapper.map_exchange_error(http_status, error_body, error_data=error_data)
        assert api_error.code == expected_code.value
        assert expected_message_contains in api_error.message
        assert api_error.http_status == http_status

    @pytest.mark.parametrize(
        ("http_status", "error_body", "expected_code", "expected_message_contains"),
        [
            (
                503,
                '{"message":"Service temporarily unavailable","code":"MAINTENANCE"}',
                APIErrorCode.MAINTENANCE,
                "Service temporarily unavailable",
            ),
        ],
    )
    def test_map_service_unavailable_503(
        self,
        http_status: int,
        error_body: str,
        expected_code: APIErrorCode,
        expected_message_contains: str,
    ) -> None:
        """Test map service unavailable 503."""
        mapper = BackpackErrorMapper()
        api_error = mapper.map_exchange_error(
            http_status,
            error_body,
            error_data=json.loads(error_body),
        )
        assert api_error.code == expected_code.value
        assert expected_message_contains in api_error.message

    @pytest.mark.parametrize(
        ("status_code", "error_body", "error_code_enum", "expected_message_contains"),
        [
            (
                404,
                '{"message":"Order not found","code":"RESOURCE_NOT_FOUND"}',
                APIErrorCode.ORDER_NOT_FOUND,
                "Order not found",
            ),
        ],
    )
    def test_map_order_not_found_error(
        self,
        backpack_error_mapper: BackpackErrorMapper,
        status_code: int,
        error_body: str,
        error_code_enum: APIErrorCode,
        expected_message_contains: str,
    ) -> None:
        """Test map order not found error."""
        error_data = json.loads(error_body)
        api_error = backpack_error_mapper.map_exchange_error(status_code, error_body, error_data)
        assert api_error.code == error_code_enum.value
        assert expected_message_contains in api_error.message
        assert api_error.http_status == status_code
        assert api_error.exchange_message == json.loads(error_body)["message"]

    @pytest.mark.parametrize(
        ("status_code", "error_body", "error_code_enum", "expected_message_contains"),
        [
            (
                400,
                '{"message":"Invalid symbol","code":"INVALID_SYMBOL"}',
                APIErrorCode.INVALID_SYMBOL,
                "Invalid symbol",
            ),
        ],
    )
    def test_map_invalid_symbol_error(
        self,
        backpack_error_mapper: BackpackErrorMapper,
        status_code: int,
        error_body: str,
        error_code_enum: APIErrorCode,
        expected_message_contains: str,
    ) -> None:
        """Test map invalid symbol error."""
        error_data = json.loads(error_body)
        api_error = backpack_error_mapper.map_exchange_error(status_code, error_body, error_data)
        assert api_error.code == error_code_enum.value
        assert expected_message_contains in api_error.message
        assert api_error.http_status == status_code
        assert api_error.exchange_message == json.loads(error_body)["message"]

    @pytest.mark.parametrize(
        ("status_code", "error_body", "error_code_enum", "expected_message_contains"),
        [
            (
                400,
                '{"message":"A message stating the symbol is invalid.","code":"INVALID_SYMBOL"}',
                APIErrorCode.INVALID_SYMBOL,
                "A message stating the symbol is invalid.",
            ),
        ],
    )
    def test_map_specific_bp_error_to_api_error_code(
        self,
        backpack_error_mapper: BackpackErrorMapper,
        status_code: int,
        error_body: str,
        error_code_enum: APIErrorCode,
        expected_message_contains: str,
    ) -> None:
        """Test mapping of a specific known Backpack error code to internal APIErrorCode."""
        error_data = json.loads(error_body)
        api_error = backpack_error_mapper.map_exchange_error(status_code, error_body, error_data)
        assert api_error.code == error_code_enum.value
        assert expected_message_contains in api_error.message
        assert api_error.http_status == status_code
        assert api_error.exchange_message == json.loads(error_body)["message"]

    @pytest.mark.parametrize(
        ("status_code", "error_body", "expected_api_code", "expected_message_part"),
        [
            (400, "Invalid JSON input", APIErrorCode.INVALID_REQUEST, "Invalid JSON input"),
            (
                500,
                "<html><body>Server Error</body></html>",
                APIErrorCode.SERVER_ERROR,
                "Server Error",
            ),
        ],
    )
    def test_map_non_json_error_body(
        self,
        backpack_error_mapper: BackpackErrorMapper,
        status_code: int,
        error_body: str,
        expected_api_code: APIErrorCode,
        expected_message_part: str,
    ) -> None:
        """Test map non json error body."""
        api_error = backpack_error_mapper.map_exchange_error(
            status_code,
            error_body,
            error_data=None,
        )
        assert api_error.code == expected_api_code.value
        assert expected_message_part in api_error.message
        assert api_error.http_status == status_code
        assert api_error.exchange_message == error_body

    @pytest.mark.parametrize(
        ("status_code", "error_body", "expected_api_code", "expected_message_part"),
        [
            (
                400,
                '{"message":"Some custom exchange error","code":"UNKNOWN_CODE_99999"}',
                APIErrorCode.INVALID_REQUEST,
                "Some custom exchange error",
            ),
        ],
    )
    def test_map_unknown_error_code_in_json(
        self,
        backpack_error_mapper: BackpackErrorMapper,
        status_code: int,
        error_body: str,
        expected_api_code: APIErrorCode,
        expected_message_part: str,
    ) -> None:
        """Test map unknown error code in json."""
        error_data = json.loads(error_body)
        api_error = backpack_error_mapper.map_exchange_error(status_code, error_body, error_data)
        assert api_error.code == expected_api_code.value
        assert expected_message_part in api_error.message
        assert api_error.http_status == status_code
        assert api_error.exchange_message == error_data.get("message")
        assert api_error.metadata == error_data

    def test_map_empty_error_body_and_data(
        self,
        backpack_error_mapper: BackpackErrorMapper,
    ) -> None:
        """Test map empty error body and data."""
        api_error = backpack_error_mapper.map_exchange_error(500, "", error_data=None)
        assert api_error.code == APIErrorCode.EXCHANGE_SPECIFIC.value
        assert api_error.http_status == 500
        assert not api_error.exchange_message

    @pytest.mark.parametrize(
        ("status_code", "error_data", "error_body", "expected_retry_after"),
        [
            # Test Case: Primary Regex - Seconds
            (
                429,
                {"message": "Retry after 30 seconds", "code": "TOO_MANY_REQUESTS"},
                None,
                30.0,
            ),
            # Test Case: Milliseconds
            (
                429,
                {"message": "Try again in 1500 ms.", "code": "TOO_MANY_REQUESTS"},
                None,
                1.5,
            ),
            # Test Case: No Retry Info
            (
                429,
                {"message": "Rate limit exceeded.", "code": "TOO_MANY_REQUESTS"},
                None,
                None,
            ),
            # Test Case: Error Body String (no error_data)
            (
                429,
                None,
                "Wait for 10000 milliseconds then try again",
                10.0,
            ),
            # Test Case: Non-Rate Limit Error
            (
                400,
                {"message": "Bad request.", "code": "INVALID_REQUEST"},
                None,
                None,
            ),
            # Test Case: Malformed Retry - Text Number
            (
                429,
                {"message": "Retry after twenty seconds.", "code": "TOO_MANY_REQUESTS"},
                None,
                None,
            ),
            # Additional test cases for other patterns
            (
                429,
                {"message": "Please wait 15s", "code": "TOO_MANY_REQUESTS"},
                None,
                15.0,
            ),
            (
                429,
                {"message": "Wait 45 seconds before retrying", "code": "TOO_MANY_REQUESTS"},
                None,
                45.0,
            ),
            # Case insensitive test
            (
                429,
                {"message": "RETRY AFTER 60 SECONDS", "code": "TOO_MANY_REQUESTS"},
                None,
                60.0,
            ),
        ],
    )
    def test_retry_after_parsing(
        self,
        backpack_error_mapper: BackpackErrorMapper,
        status_code: int,
        error_data: dict[str, str] | None,
        error_body: str | None,
        expected_retry_after: float | None,
    ) -> None:
        """Test parsing of retry_after from rate limit error messages."""
        # Prepare error_body if not provided but error_data is
        if error_body is None and error_data:
            error_body = json.dumps(error_data)

        api_error = backpack_error_mapper.map_exchange_error(
            status_code,
            error_body,
            error_data=error_data,
        )

        # Check retry_after value
        assert api_error.retry_after == expected_retry_after

        # Verify rate limit errors have correct code
        if status_code == 429 or (
            error_data and error_data.get("code") in ["TOO_MANY_REQUESTS", "RATE_LIMIT_EXCEEDED"]
        ):
            assert api_error.code == APIErrorCode.RATE_LIMITED.value
