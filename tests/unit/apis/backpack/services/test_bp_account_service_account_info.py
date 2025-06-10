"""Unit tests for BackpackAccountService account info functionality."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalance
from cyberdelta.apis.backpack.models.bp_raw_account_summary import BackpackRawAccountSummary
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPosition
from cyberdelta.apis.backpack.models.bp_raw_query_params import BackpackRawGetAccountInfoParams
from cyberdelta.apis.backpack.services.bp_account_service import BackpackAccountService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models.margin_account import BackpackMarginDetails, MarginAccountSummary


class TestBackpackAccountServiceAccountInfo:
    """Tests for the BackpackAccountService account info functionality."""

    @pytest.mark.asyncio
    async def test_get_account_info_raw_success(
        self,
        bp_account_service: BackpackAccountService,
        mock_mapper: MagicMock,
    ) -> None:
        """Test get_account_info successfully fetches and processes account info.

        Test by mocking its internal helper methods that perform raw data fetching.
        """
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
        mock_validated_raw_account_summary = BackpackRawAccountSummary.model_validate(
            mock_raw_account_data,
        )
        mock_empty_raw_balances: dict[str, BackpackRawBalance] = {}
        mock_empty_raw_positions: list[BackpackRawPosition] = []

        mock_bp_details = BackpackMarginDetails(
            imf_raw=str(mock_validated_raw_account_summary.leverage_limit),
        )
        mock_internal_margin_summary = MarginAccountSummary(
            exchange="backpack_test_account",
            timestamp=datetime.now(UTC),
            total_equity=Decimal("0"),
            available_equity=Decimal("0"),
            bp_details=mock_bp_details,
            total_initial_margin_required=Decimal("0"),
            total_maintenance_margin_required=Decimal("0"),
            total_position_notional=Decimal("0"),
        )

        # Patch the internal helper methods of the service instance
        with (
            patch.object(
                bp_account_service,
                "_get_raw_account_summary_obj",
                new_callable=AsyncMock,
                return_value=mock_validated_raw_account_summary,
            ) as mock_get_summary_obj,
            patch.object(
                bp_account_service,
                "_get_raw_balances_dict",
                new_callable=AsyncMock,
                return_value=mock_empty_raw_balances,
            ) as mock_get_balances_dict,
            patch.object(
                bp_account_service,
                "_get_raw_positions_list",
                new_callable=AsyncMock,
                return_value=mock_empty_raw_positions,
            ) as mock_get_positions_list,
            patch.object(bp_account_service, "_mapper", mock_mapper),
        ):  # Patch the mapper as before
            mock_mapper.transform_raw_account_summary_to_internal.return_value = (
                mock_internal_margin_summary
            )
            result = await bp_account_service.get_account_info()

        mock_get_summary_obj.assert_called_once_with()
        mock_get_balances_dict.assert_called_once_with()
        mock_get_positions_list.assert_called_once_with()

        mock_mapper.transform_raw_account_summary_to_internal.assert_called_once_with(
            raw_settings=mock_validated_raw_account_summary,
            spot_balances_raw=mock_empty_raw_balances,
            derivative_positions_raw=mock_empty_raw_positions,
        )
        assert result.exchange == mock_internal_margin_summary.exchange
        assert isinstance(result.timestamp, datetime)
        assert result.timestamp.tzinfo == UTC
        assert result.total_equity == mock_internal_margin_summary.total_equity
        assert result.available_equity == mock_internal_margin_summary.available_equity
        assert result.bp_details == mock_internal_margin_summary.bp_details

    @pytest.mark.asyncio
    async def test_get_account_info_http_client_returns_none(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test get_account_info when HTTP client returns None content."""
        # Mock request builder to return a valid params object
        mock_request_builder.build_get_account_info_params.return_value = (
            BackpackRawGetAccountInfoParams()
        )
        # Mock HTTP client to return None content
        mock_http_client_requester.return_value = (None, 200, {})

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_account_info()

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "No data received for raw account summary, status: 200" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_account_info_validation_error_via_public_api(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_account_info handles validation error from response handler."""
        mock_request_builder.build_get_account_info_params.return_value = None
        mock_http_client_requester.return_value = ({"invalid": "summary"}, 200, {})
        mock_response_handler.handle_get_account_info_response.side_effect = Exception(
            "Validation failed",
        )

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_account_info()

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected error for raw account summary" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_account_info_unexpected_exception_via_public_api(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_account_info handles unexpected exception via public API."""
        mock_request_builder.build_get_account_info_params.return_value = None
        mock_http_client_requester.return_value = ({"equity": "100"}, 200, {})
        mock_response_handler.handle_get_account_info_response.side_effect = Exception(
            "Unexpected error",
        )

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_account_info()

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected error for raw account summary" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_account_info_validation_error_coverage(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_account_info handles validation error from response handler."""
        mock_request_builder.build_get_account_info_params.return_value = None
        mock_http_client_requester.return_value = ({"invalid": "summary"}, 200, {})
        mock_response_handler.handle_get_account_info_response.side_effect = Exception(
            "Validation failed",
        )

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_account_info()

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected error for raw account summary" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_account_info_unexpected_exception_coverage(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_account_info handles unexpected exception."""
        mock_request_builder.build_get_account_info_params.return_value = None
        mock_http_client_requester.return_value = ({"equity": "100"}, 200, {})
        mock_response_handler.handle_get_account_info_response.side_effect = Exception(
            "Unexpected error",
        )

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_account_info()

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected error for raw account summary" in exc_info.value.message
