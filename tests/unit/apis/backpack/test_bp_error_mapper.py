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
                '{"error":"Generic client error","code":10000}',
                APIErrorCode.EXCHANGE_SPECIFIC,
                "Generic client error",
            ),
            (
                400,
                '{"error":"Invalid parameter","params":{"field":"symbol"},"code":10007}',
                APIErrorCode.INVALID_PARAMS,
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
        assert api_error.exchange_message == error_body

    @pytest.mark.parametrize(
        "http_status, error_body, expected_code, expected_message_contains",
        [
            (
                401,
                '{"error":"Authentication failed","code":20000}',
                APIErrorCode.AUTHENTICATION_FAILED,
                "Authentication failed",
            ),
            (
                403,
                '{"error":"Forbidden access","code":20001}',
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
        "http_status, error_body, bp_error_code, expected_api_code, expected_message_contains",
        [
            (
                400,
                '{"error":"Account has insufficient balance for requested action.","code":10004}',
                10004,
                APIErrorCode.INSUFFICIENT_FUNDS,
                "Account has insufficient balance",
            ),
            (
                400,  # Example: Order not found might be 400 or 404 depending on API
                '{"error":"UNKNOWN_ORDER","code":30005}',  # Assuming 30005 is their order not found
                30005,
                APIErrorCode.ORDER_NOT_FOUND,
                "Order not found or has been filled",
            ),
        ],
    )
    def test_map_insufficient_funds_error(
        self,
        http_status: int,
        error_body: str,
        bp_error_code: int,
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
                '{"error":"Too Many Requests","code":90001}',
                APIErrorCode.RATE_LIMITED,
                "Rate limit exceeded",
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
                '{"error":"Internal server error","code":null}',  # code might be null
                APIErrorCode.SERVER_ERROR,
                "Server error",
            ),
            (500, "Internal Server Error", APIErrorCode.SERVER_ERROR, "Server error"),
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
                '{"error":"Service temporarily unavailable","code":null}',
                APIErrorCode.SERVICE_UNAVAILABLE,
                "Service unavailable",
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

    def test_map_order_not_found_error(
        self, backpack_error_mapper_instance: BackpackErrorMapper
    ) -> None:
        http_status = 404
        error_body = '{"error":"UNKNOWN_ORDER","code":30005}'
        expected_code = APIErrorCode.ORDER_NOT_FOUND
        expected_message_contains = "Order not found"

        api_error = backpack_error_mapper_instance.map_exchange_error(
            http_status, error_body, error_data=json.loads(error_body)
        )
        assert api_error.code == expected_code.value
        assert expected_message_contains in api_error.message

    def test_map_invalid_symbol_error(
        self, backpack_error_mapper_instance: BackpackErrorMapper
    ) -> None:
        http_status = 400
        error_body = '{"error":"INVALID_SYMBOL","code":10001}'
        expected_code = APIErrorCode.INVALID_SYMBOL
        expected_message_contains = "Invalid symbol"

        api_error = backpack_error_mapper_instance.map_exchange_error(
            http_status, error_body, error_data=json.loads(error_body)
        )
        assert api_error.code == expected_code.value
        assert expected_message_contains in api_error.message

    def test_map_non_json_error_body(
        self, backpack_error_mapper_instance: BackpackErrorMapper
    ) -> None:
        http_status = 500
        error_body = "Internal Server Error - Plain Text"
        expected_code = APIErrorCode.SERVER_ERROR
        expected_message_contains = "Server error"

        api_error = backpack_error_mapper_instance.map_exchange_error(
            http_status,
            error_body,
            error_data=None,  # error_data would be None if JSON parsing fails
        )
        assert api_error.code == expected_code.value
        assert expected_message_contains in api_error.message

    def test_map_unknown_error_code_in_json(
        self, backpack_error_mapper_instance: BackpackErrorMapper
    ) -> None:
        http_status = 400
        error_body = '{"error":"Some new unexpected error","code":99999}'
        expected_code = APIErrorCode.EXCHANGE_SPECIFIC  # Fallback for unknown codes
        expected_message_contains = "Some new unexpected error"

        api_error = backpack_error_mapper_instance.map_exchange_error(
            http_status, error_body, error_data=json.loads(error_body)
        )
        assert api_error.code == expected_code.value
        assert expected_message_contains in api_error.message

    def test_map_empty_error_body_and_data(
        self, backpack_error_mapper_instance: BackpackErrorMapper
    ) -> None:
        http_status = 500
        error_body = ""
        error_data = None
        expected_code = APIErrorCode.SERVER_ERROR  # Default for 5xx without specifics
        expected_message_contains = "Server error or unexpected response format"

        api_error = backpack_error_mapper_instance.map_exchange_error(
            http_status, error_body, error_data
        )
        assert api_error.code == expected_code.value
        assert expected_message_contains in api_error.message
