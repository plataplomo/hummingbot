"""Unit tests for BackpackAccountService account info functionality."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.backpack.models.bp_raw_account_summary import BackpackRawAccountSummary
from cyberdelta.apis.backpack.models.bp_raw_collateral import BackpackRawCollateralResponse
from cyberdelta.apis.backpack.models.bp_raw_query_params import BackpackRawGetAccountInfoParams
from cyberdelta.apis.backpack.services.bp_account_service import BackpackAccountService
from cyberdelta.apis.common import APIError, APIErrorCode


class TestBackpackAccountServiceAccountInfo:
    """Tests for the BackpackAccountService account info functionality."""

    @pytest.mark.asyncio
    async def test_get_account_summary_raw_success(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_account_summary successfully fetches and processes account info."""
        # Mock raw account data that would come from the API
        mock_raw_account_data = {
            "autoBorrowSettlements": True,
            "autoLend": False,
            "autoRealizePnl": True,
            "autoRepayBorrows": False,
            "borrowLimit": "10000.00",
            "futuresMakerFee": "0.0002",
            "futuresTakerFee": "0.0005",
            "leverageLimit": "20.00000000",
            "limitOrders": 50,
            "liquidating": False,
            "positionLimit": "500000.00",
            "spotMakerFee": "0.001",
            "spotTakerFee": "0.001",
            "triggerOrders": 10,
        }

        # Mock the collateral response (required for enhanced account summary)
        mock_collateral_response: dict[str, str | list[Any] | None] = {
            "netEquity": "10000.00",
            "netEquityAvailable": "9000.00",
            "netEquityLocked": "1000.00",
            "assetsValue": "10000.00",
            "liabilitiesValue": "0.00",
            "imf": "0.10",
            "mmf": "0.05",
            "marginFraction": None,
            "borrowLiability": "0.00",
            "pnlUnrealized": "0.00",
            "unsettledEquity": "0.00",
            "netExposureFutures": "0.00",
            "collateral": [],
        }

        # Set up HTTP client to return account info and collateral responses
        mock_http_client_requester.side_effect = [
            # First call: account info
            (mock_raw_account_data, 200, {}),
            # Second call: positions (empty)
            ([], 200, {}),
            # Third call: balances (empty dict)
            ({}, 200, {}),
            # Fourth call: collateral
            (mock_collateral_response, 200, {}),
        ]

        # Set up response handler to validate and return the data
        mock_response_handler.handle_get_account_info_response.return_value = (
            BackpackRawAccountSummary.model_validate(mock_raw_account_data)
        )
        mock_response_handler.handle_get_positions_response.return_value = []
        mock_response_handler.handle_get_balances_response.return_value = {}
        mock_response_handler.handle_get_collateral_response.return_value = (
            BackpackRawCollateralResponse.model_validate(mock_collateral_response)
        )

        # Execute the test
        result = await bp_account_service.get_account_summary()

        # Verify the result
        assert result.exchange == "backpack"  # Mapper hardcodes this
        assert isinstance(result.timestamp, datetime)
        assert result.timestamp.tzinfo == UTC
        assert result.total_equity == Decimal("10000.00")  # From collateral netEquity
        assert result.available_equity == Decimal("9000.00")  # From collateral netEquityAvailable
        assert result.bp_details is not None
        assert result.bp_details.imf_raw == "0.10"  # From collateral imf

    @pytest.mark.asyncio
    async def test_get_account_summary_http_client_returns_none(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test get_account_summary when HTTP client returns None content."""
        # Mock request builder to return a valid params object
        mock_request_builder.build_get_account_info_params.return_value = (
            BackpackRawGetAccountInfoParams()
        )
        # Mock HTTP client to return None content
        mock_http_client_requester.return_value = (None, 200, {})

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_account_summary()

        # Business logic wraps error with status code as error code
        assert exc_info.value.code == 200
        assert "No data received for account summary, status: 200" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_account_summary_validation_error_via_public_api(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_account_summary handles validation error from response handler."""
        mock_request_builder.build_get_account_info_params.return_value = (
            BackpackRawGetAccountInfoParams()
        )
        mock_http_client_requester.return_value = ({"invalid": "summary"}, 200, {})
        mock_response_handler.handle_get_account_info_response.side_effect = Exception(
            "Validation failed",
        )

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_account_summary()

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Failed to fetch account summary" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_account_summary_unexpected_exception_via_public_api(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_account_summary handles unexpected exception via public API."""
        mock_request_builder.build_get_account_info_params.return_value = (
            BackpackRawGetAccountInfoParams()
        )
        mock_http_client_requester.return_value = ({"equity": "100"}, 200, {})
        mock_response_handler.handle_get_account_info_response.side_effect = Exception(
            "Unexpected error",
        )

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_account_summary()

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Failed to fetch account summary" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_account_summary_validation_error_coverage(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_account_summary handles validation error from response handler."""
        mock_request_builder.build_get_account_info_params.return_value = (
            BackpackRawGetAccountInfoParams()
        )
        mock_http_client_requester.return_value = ({"invalid": "summary"}, 200, {})
        mock_response_handler.handle_get_account_info_response.side_effect = Exception(
            "Validation failed",
        )

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_account_summary()

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Failed to fetch account summary" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_account_summary_unexpected_exception_coverage(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_account_summary handles unexpected exception."""
        mock_request_builder.build_get_account_info_params.return_value = (
            BackpackRawGetAccountInfoParams()
        )
        mock_http_client_requester.return_value = ({"equity": "100"}, 200, {})
        mock_response_handler.handle_get_account_info_response.side_effect = Exception(
            "Unexpected error",
        )

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_account_summary()

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Failed to fetch account summary" in exc_info.value.message
