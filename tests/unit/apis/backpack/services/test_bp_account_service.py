"""
Unit tests for the BackpackAccountService.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

# from typing import Any # Removed as it's unused for now
import pytest

from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder
from cyberdelta.apis.backpack.bp_response_handler import BackpackResponseHandler, RawJsonResponse
from cyberdelta.apis.backpack.models.bp_raw_account import (
    BackpackRawBalance,  # Added for mock typing
)
from cyberdelta.apis.backpack.models.bp_raw_account_summary import BackpackRawAccountSummary
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPosition
from cyberdelta.apis.backpack.services.bp_account_service import BackpackAccountService
from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.connectivity.rate_limiter_service import RateLimiterService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode


@pytest.fixture
def mock_http_client_requester() -> AsyncMock:
    """Provides a mock callable similar to an API client's _request method."""
    mock = AsyncMock()
    mock.return_value = ({}, 200, MagicMock())  # (data, status_code, headers)
    return mock


@pytest.fixture
def mock_request_builder() -> MagicMock:
    """Provides a mock BackpackRequestBuilder."""
    return MagicMock(spec=BackpackRequestBuilder)


@pytest.fixture
def mock_response_handler() -> MagicMock:
    """Provides a mock BackpackResponseHandler."""
    return MagicMock(spec=BackpackResponseHandler)


@pytest.fixture
def mock_authenticator() -> AsyncMock:
    """Provides a mock IAuthenticator."""
    mock = AsyncMock(spec=IAuthenticator)
    mock.prepare_request.return_value = {
        "headers": {"Authorization": "Bearer testtoken"},
        "params": None,
        "data": {"signed": True},
    }
    return mock


@pytest.fixture
def mock_rate_limiter_service() -> AsyncMock:
    """Provides a mock RateLimiterService."""
    return AsyncMock(spec=RateLimiterService)


@pytest.fixture
def bp_account_service(
    mock_http_client_requester: AsyncMock,
    mock_request_builder: MagicMock,
    mock_response_handler: MagicMock,
    mock_authenticator: AsyncMock,
    mock_rate_limiter_service: AsyncMock,
) -> BackpackAccountService:
    """Provides an instance of BackpackAccountService with mocked dependencies."""
    return BackpackAccountService(
        http_client_requester=mock_http_client_requester,
        request_builder=mock_request_builder,
        response_handler=mock_response_handler,
        authenticator=mock_authenticator,
        rate_limiter_service=mock_rate_limiter_service,
        exchange_name="backpack_test_account_svc",
    )


class TestBackpackAccountService:
    """Tests for the BackpackAccountService class."""

    @pytest.mark.asyncio
    async def test_transfer_raw_success(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_authenticator: AsyncMock,
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test transfer_raw successfully initiates a transfer and returns raw response."""
        asset = "USDC"
        amount = Decimal("100.0")
        from_account = "SPOT"
        to_account = "FUTURES"
        client_transfer_id = "testTransfer123"

        mock_payload = {
            "symbol": asset,
            "quantity": str(amount),
            "fromAccount": from_account,
            "toAccount": to_account,
            "clientId": client_transfer_id,
        }
        mock_raw_response_content: RawJsonResponse = {"status": "success", "id": "transfer789"}

        mock_request_builder.build_internal_transfer_payload.return_value = mock_payload
        mock_http_client_requester.return_value = (mock_raw_response_content, 200, MagicMock())

        result = await bp_account_service.transfer_raw(
            asset=asset,
            amount=amount,
            from_account_type=from_account,
            to_account_type=to_account,
            client_transfer_id=client_transfer_id,
        )

        mock_request_builder.build_internal_transfer_payload.assert_called_once_with(
            asset_symbol=asset,
            amount_str=str(amount),
            from_account=from_account,
            to_account=to_account,
            client_transfer_id=client_transfer_id,
        )
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint="/wapi/v1/capital/transfer/internal",
            data=mock_payload,
            is_signed=True,
        )
        assert result == mock_raw_response_content

    @pytest.mark.asyncio
    async def test_transfer_raw_api_error_from_requester(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_authenticator: AsyncMock,
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test transfer_raw handles APIError raised by the http_client_requester."""
        asset = "USDC"
        amount = Decimal("50")
        from_account = "SPOT"
        to_account = "DERIVATIVES"
        api_error_instance = APIError("Requester failed", code=APIErrorCode.SERVER_ERROR.value)

        mock_payload = {"some": "payload"}
        mock_request_builder.build_internal_transfer_payload.return_value = mock_payload
        mock_http_client_requester.side_effect = api_error_instance

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.transfer_raw(
                asset=asset,
                amount=amount,
                from_account_type=from_account,
                to_account_type=to_account,
            )

        assert exc_info.value == api_error_instance
        mock_http_client_requester.assert_called_once()

    @pytest.mark.asyncio
    async def test_transfer_raw_response_none(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_authenticator: AsyncMock,
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test transfer_raw handles None response from requester by raising APIError."""
        asset = "BTC"
        amount = Decimal("0.1")
        from_account = "MAIN"
        to_account = "TRADING"

        mock_payload = {"another": "payload"}
        mock_request_builder.build_internal_transfer_payload.return_value = mock_payload
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.transfer_raw(
                asset=asset,
                amount=amount,
                from_account_type=from_account,
                to_account_type=to_account,
            )

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "No response data received for transfer request" in exc_info.value.message
        mock_http_client_requester.assert_called_once()

    @pytest.mark.asyncio
    async def test_transfer_raw_unexpected_exception(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_authenticator: AsyncMock,
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test transfer_raw handles unexpected exceptions by wrapping them in APIError."""
        asset = "ETH"
        amount = Decimal("1.0")
        from_account = "SUB_01"
        to_account = "SPOT"
        unexpected_error = ValueError("Something went very wrong")

        mock_payload = {"unexpected": "payload"}
        mock_request_builder.build_internal_transfer_payload.return_value = mock_payload
        mock_http_client_requester.side_effect = unexpected_error

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.transfer_raw(
                asset=asset,
                amount=amount,
                from_account_type=from_account,
                to_account_type=to_account,
            )

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected error processing transfer" in exc_info.value.message
        assert exc_info.value.original_exception == unexpected_error
        mock_http_client_requester.assert_called_once()

    # Test get_balances_raw
    @pytest.mark.asyncio
    async def test_get_balances_raw_success(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_authenticator: AsyncMock,
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test get_balances_raw successfully fetches and processes balance data."""
        mock_raw_response_data = {
            "USDC": {"available": "1000.0", "locked": "0", "debt": "0", "total": "1000.0"}
        }
        mock_validated_balances: dict[str, BackpackRawBalance] = {
            "USDC": BackpackRawBalance(asset="USDC", available="1000.0", total="1000.0")
        }

        mock_http_client_requester.return_value = (mock_raw_response_data, 200, MagicMock())
        mock_response_handler.handle_get_balances_response.return_value = mock_validated_balances

        # Configure the mock rate limiter
        mock_limiter_instance = AsyncMock()
        mock_limiter_instance.acquire = AsyncMock()
        mock_rate_limiter_service.get_limiter.return_value = mock_limiter_instance

        # Explicitly set the return value for the mocked builder method
        expected_balance_params = None  # Backpack GET /capital usually has no params
        mock_request_builder.build_get_balances_params.return_value = expected_balance_params

        result = await bp_account_service.get_balances_raw()

        mock_rate_limiter_service.get_limiter.assert_called_once_with("GET", "/api/v1/capital")
        mock_limiter_instance.acquire.assert_awaited_once()
        mock_request_builder.build_get_balances_params.assert_called_once_with()

        mock_http_client_requester.assert_called_once_with(
            method="GET",
            endpoint="/api/v1/capital",
            params=expected_balance_params,  # Use the explicitly set return value
            endpoint_group="PRIVATE",
            is_signed=True,
        )

        mock_response_handler.handle_get_balances_response.assert_called_once_with(
            mock_raw_response_data
        )
        assert result == mock_validated_balances

    @pytest.mark.asyncio
    async def test_get_positions_raw_success(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_authenticator: AsyncMock,
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test get_positions_raw successfully fetches and processes position data."""
        symbol_arg = "SOL-PERP"

        mock_raw_positions_data = [
            {
                "symbol": "SOL-PERP",
                "netQuantity": "10.0",
                "entryPrice": "100.0",
                "markPrice": "110.0",
                "imf": "0.1",
                "mmf": "0.05",
                "pnlUnrealized": "100.0",
                "pnlRealized": "0.0",
                "netCost": "1000.0",
                "netExposureNotional": "1100.0",
                "netExposureQuantity": "10.0",
                "positionId": "pos123",
                "userId": 1,
                "breakEvenPrice": "105.0",
                "estLiquidationPrice": "90.0",
                "imfFunction": {"base": "0.005", "factor": "0.000001"},
                "mmfFunction": {"base": "0.002", "factor": "0.0000005"},
                "cumulativeFundingPayment": "-5.0",
                "cumulativeInterest": "-0.1",
            }
        ]
        mock_validated_positions = [BackpackRawPosition.model_validate(mock_raw_positions_data[0])]

        # Configure the mock rate limiter for the non-symbol call
        mock_limiter_no_symbol = AsyncMock()
        mock_limiter_no_symbol.acquire = AsyncMock()
        mock_rate_limiter_service.get_limiter.return_value = mock_limiter_no_symbol

        # Test without symbol first
        expected_params_no_symbol = (
            None  # build_get_positions_params(symbol=None) should return None
        )
        mock_request_builder.build_get_positions_params.return_value = expected_params_no_symbol
        mock_http_client_requester.return_value = (mock_raw_positions_data, 200, MagicMock())
        mock_response_handler.handle_get_positions_response.return_value = mock_validated_positions

        result_no_symbol = await bp_account_service.get_positions_raw()

        mock_rate_limiter_service.get_limiter.assert_called_with("GET", "/api/v1/positions")
        mock_limiter_no_symbol.acquire.assert_awaited_once()
        mock_request_builder.build_get_positions_params.assert_called_with(symbol=None)
        mock_http_client_requester.assert_called_with(
            method="GET",
            endpoint="/api/v1/positions",
            params=expected_params_no_symbol,
            endpoint_group="PRIVATE",
            is_signed=True,
        )
        mock_response_handler.handle_get_positions_response.assert_called_with(
            mock_raw_positions_data, None
        )
        assert result_no_symbol == mock_validated_positions

        # Reset mocks for the next call if necessary, or use different mock instances
        # For simplicity here, we'll reconfigure the existing mock_request_builder for the symbol call.
        mock_http_client_requester.reset_mock()  # Reset call count and args for the next assertion
        mock_response_handler.reset_mock()
        mock_request_builder.build_get_positions_params.reset_mock()  # Reset this specific method mock
        mock_rate_limiter_service.get_limiter.reset_mock()
        mock_limiter_no_symbol.acquire.reset_mock()  # If using the same limiter mock instance

        # Test with symbol
        mock_limiter_with_symbol = AsyncMock()
        mock_limiter_with_symbol.acquire = AsyncMock()
        # Make get_limiter return a new mock for the second call if its behavior/identity matters
        mock_rate_limiter_service.get_limiter.return_value = mock_limiter_with_symbol

        expected_params_with_symbol = {"symbol": symbol_arg}  # Example, actual depends on builder
        mock_request_builder.build_get_positions_params.return_value = expected_params_with_symbol
        # Assuming the HTTP response for positions with symbol is the same for this test
        mock_http_client_requester.return_value = (mock_raw_positions_data, 200, MagicMock())
        mock_response_handler.handle_get_positions_response.return_value = mock_validated_positions

        result_with_symbol = await bp_account_service.get_positions_raw(symbol=symbol_arg)

        mock_rate_limiter_service.get_limiter.assert_called_with("GET", "/api/v1/positions")
        mock_limiter_with_symbol.acquire.assert_awaited_once()
        mock_request_builder.build_get_positions_params.assert_called_with(symbol=symbol_arg)

        mock_http_client_requester.assert_called_with(
            method="GET",
            endpoint="/api/v1/positions",
            params=expected_params_with_symbol,
            endpoint_group="PRIVATE",
            is_signed=True,
        )

        mock_response_handler.handle_get_positions_response.assert_called_with(
            mock_raw_positions_data, symbol_arg
        )
        assert result_with_symbol == mock_validated_positions

    @pytest.mark.asyncio
    async def test_get_account_info_raw_success(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_authenticator: AsyncMock,
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test get_account_info_raw successfully fetches and processes account info."""
        mock_raw_response_data = {
            "autoBorrowSettlements": True,
            "autoLend": False,
            "autoRealizePnl": True,
            "autoRepayBorrows": True,
            "borrowLimit": "10000.00000000",
            "futuresMakerFee": "0.00020000",
            "futuresTakerFee": "0.00050000",
            "leverageLimit": "20.00000000",
            "limitOrders": 50,
            "liquidating": False,
            "positionLimit": "1000000.00000000",
            "spotMakerFee": "0.00080000",
            "spotTakerFee": "0.00100000",
            "triggerOrders": 20,
        }
        mock_validated_account_info = BackpackRawAccountSummary.model_validate(
            mock_raw_response_data
        )

        mock_http_client_requester.return_value = (mock_raw_response_data, 200, MagicMock())
        mock_response_handler.handle_get_account_info_response.return_value = (
            mock_validated_account_info
        )

        # Configure the mock rate limiter
        mock_limiter_instance = AsyncMock()
        mock_limiter_instance.acquire = AsyncMock()
        mock_rate_limiter_service.get_limiter.return_value = mock_limiter_instance

        result = await bp_account_service.get_account_info_raw()

        mock_rate_limiter_service.get_limiter.assert_called_once_with("GET", "/api/v1/account")
        mock_limiter_instance.acquire.assert_awaited_once()
        mock_http_client_requester.assert_called_once_with(
            method="GET",
            endpoint_group="PRIVATE",
            endpoint="/api/v1/account",
            is_signed=True,
        )
        mock_response_handler.handle_get_account_info_response.assert_called_once_with(
            mock_raw_response_data
        )
        assert result == mock_validated_account_info


# Add more tests for other methods in BackpackAccountService following similar patterns.
