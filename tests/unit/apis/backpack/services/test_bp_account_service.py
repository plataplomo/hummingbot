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
            endpoint_path="/wapi/v1/capital/transfer/internal",
            data=mock_payload,
            authenticator=mock_authenticator,
            rate_limiter_service=mock_rate_limiter_service,
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
        mock_params = None
        mock_raw_response_data = {
            "USDC": {"available": "1000.0", "locked": "0", "debt": "0", "total": "1000.0"}
        }
        # Correctly mock the return type of handle_get_balances_response
        mock_validated_balances: dict[str, BackpackRawBalance] = {
            "USDC": BackpackRawBalance(asset="USDC", available="1000.0", total="1000.0")
        }

        mock_request_builder.build_get_balances_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response_data, 200, MagicMock())
        mock_response_handler.handle_get_balances_response.return_value = mock_validated_balances

        result = await bp_account_service.get_balances_raw()

        mock_request_builder.build_get_balances_params.assert_called_once_with()
        mock_http_client_requester.assert_called_once_with(
            method="GET",
            endpoint_path="/api/v1/capital",
            params=mock_params,
            authenticator=mock_authenticator,
            rate_limiter_service=mock_rate_limiter_service,
            is_signed=True,
        )
        mock_response_handler.handle_get_balances_response.assert_called_once_with(
            mock_raw_response_data
        )
        assert result == mock_validated_balances


# Add more tests for other methods in BackpackAccountService following similar patterns.
