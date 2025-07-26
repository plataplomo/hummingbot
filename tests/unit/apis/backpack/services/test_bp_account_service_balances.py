"""Unit tests for BackpackAccountService balance functionality."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalanceResponse
from cyberdelta.apis.backpack.models.bp_raw_query_params import BackpackRawGetBalancesParams
from cyberdelta.apis.backpack.services.bp_account_service import BackpackAccountService
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.utils.typing import ParsedJsonResponse


class TestBackpackAccountServiceBalances:
    """Tests for the BackpackAccountService balance functionality."""

    @pytest.mark.asyncio
    async def test_get_balances_raw_success(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_balances successfully retrieves and processes balance data."""
        mock_raw_response_data_dict: ParsedJsonResponse = {
            "USDC": {"available": "1000.5", "locked": "10.0"},
        }
        mock_validated_raw_balances_dict: dict[str, BackpackRawBalanceResponse] = {
            "USDC": BackpackRawBalanceResponse(available="1000.5", locked="10.0", staked="0"),
        }

        # Mock request builder to return a valid params object
        mock_request_builder.build_get_balances_params.return_value = BackpackRawGetBalancesParams()

        # Mock the HTTP client and response handler to return expected data
        mock_http_client_requester.return_value = (mock_raw_response_data_dict, 200, MagicMock())
        mock_response_handler.handle_get_balances_response.return_value = (
            mock_validated_raw_balances_dict
        )

        result = await bp_account_service.get_balances()

        # The mapper transforms to exchange="backpack" not "backpack_test_account"
        assert result["USDC"].exchange == "backpack"
        assert result["USDC"].asset == "USDC"
        assert isinstance(result["USDC"].timestamp, datetime)
        assert result["USDC"].timestamp.tzinfo == UTC
        assert result["USDC"].total_quantity == Decimal("1010.5")
        assert result["USDC"].available_quantity == Decimal("1000.5")

    @pytest.mark.asyncio
    async def test_get_balances_success(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_balances successfully retrieves and processes balance data."""
        mock_params_from_builder_for_get_balances = BackpackRawGetBalancesParams()

        mock_raw_response_dict: ParsedJsonResponse = {
            "USDC": {"available": "1000.5", "locked": "10.0"},
            "SOL": {"available": "50.2", "locked": "0.5"},
        }
        mock_status_code = 200
        mock_headers: dict[str, str] = {}

        mock_request_builder.build_get_balances_params.return_value = (
            mock_params_from_builder_for_get_balances
        )

        mock_http_client_requester.return_value = (
            mock_raw_response_dict,
            mock_status_code,
            mock_headers,
        )

        mock_raw_balances_payload: dict[str, BackpackRawBalanceResponse] = {
            "USDC": BackpackRawBalanceResponse(available="1000.5", locked="10.0", staked="0"),
            "SOL": BackpackRawBalanceResponse(available="50.2", locked="0.5", staked="0"),
        }
        mock_response_handler.handle_get_balances_response.return_value = mock_raw_balances_payload

        result_balances = await bp_account_service.get_balances()

        assert len(result_balances) == 2
        assert "USDC" in result_balances
        assert "SOL" in result_balances

        # Check USDC balance
        usdc_balance = result_balances["USDC"]
        assert usdc_balance.exchange == "backpack"  # Mapper returns "backpack"
        assert usdc_balance.asset == "USDC"
        assert isinstance(usdc_balance.timestamp, datetime)
        assert usdc_balance.timestamp.tzinfo == UTC
        assert usdc_balance.total_quantity == Decimal("1010.5")
        assert usdc_balance.available_quantity == Decimal("1000.5")

        # Check SOL balance
        sol_balance = result_balances["SOL"]
        assert sol_balance.exchange == "backpack"  # Mapper returns "backpack"
        assert sol_balance.asset == "SOL"
        assert isinstance(sol_balance.timestamp, datetime)
        assert sol_balance.timestamp.tzinfo == UTC
        assert sol_balance.total_quantity == Decimal("50.7")
        assert sol_balance.available_quantity == Decimal("50.2")

    @pytest.mark.asyncio
    async def test_get_balances_http_client_returns_none(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test get_balances when HTTP client returns None content."""
        # Mock the HTTP client to return None which triggers error
        mock_http_client_requester.return_value = (None, 200, {})

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_balances()

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "No data received for balances, status: 200" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_balances_validation_error_via_public_api(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_balances handles validation error from response handler."""
        # Mock the HTTP client to return invalid data that causes validation error
        mock_http_client_requester.return_value = ({"invalid": "balance"}, 200, {})

        # Mock the response handler to raise ValidationError
        mock_response_handler.handle_get_balances_response.side_effect = (
            ValidationError.from_exception_data(
                title="ValidationError",
                line_errors=[],
            )
        )

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_balances()

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Failed to retrieve balance data" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_balances_unexpected_exception_via_public_api(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_balances handles unexpected exception from response handler."""
        # Mock the HTTP client to return valid data but response handler raises unexpected error
        mock_http_client_requester.return_value = ({"USDC": {"available": "100.0"}}, 200, {})

        # Mock the response handler to raise an unexpected exception
        mock_response_handler.handle_get_balances_response.side_effect = Exception(
            "Unexpected error"
        )

        # Business logic doesn't wrap general exceptions, so expect raw Exception
        with pytest.raises(Exception) as exc_info:
            await bp_account_service.get_balances()

        assert str(exc_info.value) == "Unexpected error"

    @pytest.mark.asyncio
    async def test_get_balances_validation_error_coverage(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_balances validation error coverage."""
        # Mock the HTTP client to return invalid data that causes validation error
        mock_http_client_requester.return_value = ({"invalid": "balance"}, 200, {})

        # Mock the response handler to raise ValidationError
        mock_response_handler.handle_get_balances_response.side_effect = (
            ValidationError.from_exception_data(
                title="ValidationError",
                line_errors=[],
            )
        )

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_balances()

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value

    @pytest.mark.asyncio
    async def test_get_balances_unexpected_exception_coverage(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_balances unexpected exception coverage."""
        # Mock the HTTP client to return valid data but response handler raises unexpected error
        mock_http_client_requester.return_value = ({"USDC": {"available": "100.0"}}, 200, {})

        # Mock the response handler to raise an unexpected exception
        mock_response_handler.handle_get_balances_response.side_effect = Exception(
            "Unexpected error"
        )

        # Business logic doesn't wrap general exceptions, so expect raw Exception
        with pytest.raises(Exception) as exc_info:
            await bp_account_service.get_balances()

        assert str(exc_info.value) == "Unexpected error"
