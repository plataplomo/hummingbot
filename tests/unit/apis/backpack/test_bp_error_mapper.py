import json

import pytest

from cyberdelta.apis.backpack.bp_error_mapper import BackpackErrorMapper
from cyberdelta.apis.models.api_error_codes import APIErrorCode


@pytest.fixture
def backpack_error_mapper() -> BackpackErrorMapper:
    """Provides a BackpackErrorMapper instance for tests."""
    return BackpackErrorMapper()


class TestBackpackErrorMapper:
    @pytest.mark.parametrize(
        "http_status, error_body, expected_code, expected_message_contains",
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
        mapper = BackpackErrorMapper()
        api_error = mapper.map_exchange_error(
            http_status, error_body, error_data=json.loads(error_body)
        )
        assert api_error.code == expected_code.value
        assert expected_message_contains in api_error.message
        assert api_error.http_status == http_status
        assert api_error.exchange_message == json.loads(error_body)["message"]

    @pytest.mark.parametrize(
        "http_status, error_body, expected_code, expected_message_contains",
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
        mapper = BackpackErrorMapper()
        api_error = mapper.map_exchange_error(
            http_status, error_body, error_data=json.loads(error_body)
        )
        assert api_error.code == expected_code.value
        assert expected_message_contains in api_error.message
        assert api_error.http_status == http_status

    @pytest.mark.parametrize(
        "http_status, error_body, bp_error_code_str, expected_api_code, expected_message_contains",
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
        mapper = BackpackErrorMapper()
        api_error = mapper.map_exchange_error(
            http_status, error_body, error_data=json.loads(error_body)
        )
        assert api_error.code == expected_api_code.value
        assert expected_message_contains in api_error.message
        assert api_error.http_status == http_status

    @pytest.mark.parametrize(
        "http_status, error_body, expected_code, expected_message_contains",
        [
            (
                429,
                '{"message":"Too Many Requests","code":"TOO_MANY_REQUESTS"}',
                APIErrorCode.RATE_LIMITED,
                "Too Many Requests",
            )
        ],
    )
    def test_map_rate_limited_429_error(
        self,
        http_status: int,
        error_body: str,
        expected_code: APIErrorCode,
        expected_message_contains: str,
    ) -> None:
        mapper = BackpackErrorMapper()
        api_error = mapper.map_exchange_error(
            http_status, error_body, error_data=json.loads(error_body)
        )
        assert api_error.code == expected_code.value
        assert expected_message_contains in api_error.message
        assert api_error.http_status == http_status

    @pytest.mark.parametrize(
        "http_status, error_body, expected_code, expected_message_contains",
        [
            (
                500,
                '{"message":"Internal server error","code":"SERVER_ERROR"}',
                APIErrorCode.SERVER_ERROR,
                "Internal server error",
            ),
            (500, "Internal Server Error", APIErrorCode.EXCHANGE_SPECIFIC, "Internal Server Error"),
        ],
    )
    def test_map_server_error_500(
        self,
        http_status: int,
        error_body: str,
        expected_code: APIErrorCode,
        expected_message_contains: str,
    ) -> None:
        mapper = BackpackErrorMapper()
        # error_data might not be parsable if body is not JSON
        error_data = None
        try:
            error_data = json.loads(error_body)
        except json.JSONDecodeError:
            pass
        api_error = mapper.map_exchange_error(http_status, error_body, error_data=error_data)
        assert api_error.code == expected_code.value
        assert expected_message_contains in api_error.message
        assert api_error.http_status == http_status

    @pytest.mark.parametrize(
        "http_status, error_body, expected_code, expected_message_contains",
        [
            (
                503,
                '{"message":"Service temporarily unavailable","code":"MAINTENANCE"}',
                APIErrorCode.MAINTENANCE,
                "Service temporarily unavailable",
            )
        ],
    )
    def test_map_service_unavailable_503(
        self,
        http_status: int,
        error_body: str,
        expected_code: APIErrorCode,
        expected_message_contains: str,
    ) -> None:
        mapper = BackpackErrorMapper()
        api_error = mapper.map_exchange_error(
            http_status, error_body, error_data=json.loads(error_body)
        )
        assert api_error.code == expected_code.value
        assert expected_message_contains in api_error.message

    @pytest.mark.parametrize(
        "status_code, error_body, error_code_enum, expected_message_contains",
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
        error_data = json.loads(error_body)
        api_error = backpack_error_mapper.map_exchange_error(status_code, error_body, error_data)
        assert api_error.code == error_code_enum.value
        assert expected_message_contains in api_error.message
        assert api_error.http_status == status_code
        assert api_error.exchange_message == json.loads(error_body)["message"]

    @pytest.mark.parametrize(
        "status_code, error_body, error_code_enum, expected_message_contains",
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
        error_data = json.loads(error_body)
        api_error = backpack_error_mapper.map_exchange_error(status_code, error_body, error_data)
        assert api_error.code == error_code_enum.value
        assert expected_message_contains in api_error.message
        assert api_error.http_status == status_code
        assert api_error.exchange_message == json.loads(error_body)["message"]

    @pytest.mark.parametrize(
        "status_code, error_body, error_code_enum, expected_message_contains",
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
        "status_code, error_body, expected_api_code, expected_message_part",
        [
            (400, "Invalid JSON input", APIErrorCode.EXCHANGE_SPECIFIC, "Invalid JSON input"),
            (
                500,
                "<html><body>Server Error</body></html>",
                APIErrorCode.EXCHANGE_SPECIFIC,
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
        api_error = backpack_error_mapper.map_exchange_error(
            status_code, error_body, error_data=None
        )
        assert api_error.code == expected_api_code.value
        assert expected_message_part in api_error.message
        assert api_error.http_status == status_code
        assert api_error.exchange_message == error_body

    @pytest.mark.parametrize(
        "status_code, error_body, expected_api_code, expected_message_part",
        [
            (
                400,
                '{"message":"Some custom exchange error","code":"UNKNOWN_CODE_99999"}',
                APIErrorCode.EXCHANGE_SPECIFIC,
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
        error_data = json.loads(error_body)
        api_error = backpack_error_mapper.map_exchange_error(status_code, error_body, error_data)
        assert api_error.code == expected_api_code.value
        assert expected_message_part in api_error.message
        assert api_error.http_status == status_code
        assert api_error.exchange_message == error_data.get("message")

    def test_map_empty_error_body_and_data(
        self, backpack_error_mapper: BackpackErrorMapper
    ) -> None:
        api_error = backpack_error_mapper.map_exchange_error(500, "", error_data=None)
        assert api_error.code == APIErrorCode.EXCHANGE_SPECIFIC.value
        assert api_error.http_status == 500
        assert api_error.exchange_message == ""
