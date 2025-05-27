"""
Unit tests for BackpackAccountService balance functionality.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_response_handler import RawJsonResponse
from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalance
from cyberdelta.apis.backpack.services.bp_account_service import BackpackAccountService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models.spot_balance import SpotBalance


class TestBackpackAccountServiceBalances:
    """Tests for the BackpackAccountService balance functionality."""

    @pytest.mark.asyncio
    async def test_get_balances_raw_success(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test _get_raw_balances_dict successfully fetches and processes balance data,
        tested via public get_balances."""
        mock_raw_response_data_dict: RawJsonResponse = {
            "USDC": {"available": "1000.0", "locked": "0", "debt": "0", "total": "1000.0"}
        }
        mock_validated_raw_balances_dict: dict[str, BackpackRawBalance] = {
            "USDC": BackpackRawBalance(asset="USDC", available="1000.0", total="1000.0")
        }

        mock_internal_spot_balance = SpotBalance(
            exchange="backpack_test_account",
            asset="USDC",
            timestamp=datetime.now(UTC),
            total_quantity=Decimal("1000.0"),
            available_quantity=Decimal("1000.0"),
        )
        expected_final_balances_result: dict[str, SpotBalance] = {
            "USDC": mock_internal_spot_balance
        }

        mock_request_builder.build_get_balances_params.return_value = None
        mock_http_client_requester.return_value = (mock_raw_response_data_dict, 200, MagicMock())
        mock_response_handler.handle_get_balances_response.return_value = (
            mock_validated_raw_balances_dict
        )

        mock_mapper.transform_raw_balance_to_internal.return_value = mock_internal_spot_balance

        with patch.object(bp_account_service, "_mapper", mock_mapper):
            result = await bp_account_service.get_balances()

        mock_request_builder.build_get_balances_params.assert_called_once_with()
        mock_http_client_requester.assert_called_once_with(
            method="GET",
            endpoint="/api/v1/capital",
            params=None,
            is_signed=True,
            endpoint_group="private",
            request_weight=1,
        )
        mock_response_handler.handle_get_balances_response.assert_called_once_with(
            mock_raw_response_data_dict
        )
        mock_mapper.transform_raw_balance_to_internal.assert_called_once_with(
            "USDC", mock_validated_raw_balances_dict["USDC"]
        )

        assert result["USDC"].exchange == expected_final_balances_result["USDC"].exchange
        assert result["USDC"].asset == expected_final_balances_result["USDC"].asset
        assert isinstance(result["USDC"].timestamp, datetime)
        assert result["USDC"].timestamp.tzinfo == UTC
        assert (
            result["USDC"].total_quantity == expected_final_balances_result["USDC"].total_quantity
        )
        assert (
            result["USDC"].available_quantity
            == expected_final_balances_result["USDC"].available_quantity
        )

    @pytest.mark.asyncio
    async def test_get_balances_success(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test get_balances successfully retrieves and processes balance data."""
        mock_endpoint_path_for_get_balances = "/api/v1/capital"
        mock_params_from_builder_for_get_balances = None

        mock_raw_response_dict: RawJsonResponse = {
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

        mock_raw_balances_payload: dict[str, BackpackRawBalance] = {
            "USDC": BackpackRawBalance(asset="USDC", available="1000.5", total="1010.5"),
            "SOL": BackpackRawBalance(asset="SOL", available="50.2", total="50.7"),
        }
        mock_response_handler.handle_get_balances_response.return_value = mock_raw_balances_payload

        expected_internal_balances: dict[str, SpotBalance] = {
            "USDC": SpotBalance(
                exchange="backpack_test_account",
                asset="USDC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("1010.5"),
                available_quantity=Decimal("1000.5"),
            ),
            "SOL": SpotBalance(
                exchange="backpack_test_account",
                asset="SOL",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("50.7"),
                available_quantity=Decimal("50.2"),
            ),
        }

        usdc_spot_balance = SpotBalance(
            exchange="backpack_test_account",
            asset="USDC",
            timestamp=datetime.now(UTC),
            total_quantity=Decimal("1010.5"),
            available_quantity=Decimal("1000.5"),
        )
        sol_spot_balance = SpotBalance(
            exchange="backpack_test_account",
            asset="SOL",
            timestamp=datetime.now(UTC),
            total_quantity=Decimal("50.7"),
            available_quantity=Decimal("50.2"),
        )

        def mapper_side_effect(
            asset_symbol: str, raw_balance_model: BackpackRawBalance
        ) -> SpotBalance:
            if asset_symbol == "USDC" and raw_balance_model == mock_raw_balances_payload["USDC"]:
                return usdc_spot_balance
            if asset_symbol == "SOL" and raw_balance_model == mock_raw_balances_payload["SOL"]:
                return sol_spot_balance
            pytest.fail(
                f"mock_mapper.transform_raw_balance_to_internal called with unexpected args: "
                f"{asset_symbol}, {raw_balance_model}"
            )
            raise AssertionError(
                "Fell through mapper_side_effect logic, should be impossible due to pytest.fail"
            )

        mock_mapper.transform_raw_balance_to_internal.side_effect = mapper_side_effect

        with patch.object(bp_account_service, "_mapper", mock_mapper):
            result_balances = await bp_account_service.get_balances()

        mock_request_builder.build_get_balances_params.assert_called_once_with()
        mock_http_client_requester.assert_called_once_with(
            method="GET",
            endpoint=mock_endpoint_path_for_get_balances,
            params=mock_params_from_builder_for_get_balances,
            is_signed=True,
            endpoint_group="private",
            request_weight=1,
        )
        mock_response_handler.handle_get_balances_response.assert_called_once_with(
            mock_raw_response_dict
        )

        assert mock_mapper.transform_raw_balance_to_internal.call_count == 2
        mock_mapper.transform_raw_balance_to_internal.assert_any_call(
            "USDC", mock_raw_balances_payload["USDC"]
        )
        mock_mapper.transform_raw_balance_to_internal.assert_any_call(
            "SOL", mock_raw_balances_payload["SOL"]
        )

        assert len(result_balances) == len(expected_internal_balances)
        for asset_key in expected_internal_balances:
            assert asset_key in result_balances
            actual_bal = result_balances[asset_key]
            expected_bal = expected_internal_balances[asset_key]
            assert actual_bal.exchange == expected_bal.exchange
            assert actual_bal.asset == expected_bal.asset
            assert isinstance(actual_bal.timestamp, datetime)
            assert actual_bal.timestamp.tzinfo == UTC
            assert actual_bal.total_quantity == expected_bal.total_quantity
            assert actual_bal.available_quantity == expected_bal.available_quantity

    @pytest.mark.asyncio
    async def test_get_balances_http_client_returns_none(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test get_balances when HTTP client returns None content."""
        mock_request_builder.build_get_balances_params.return_value = None
        mock_http_client_requester.return_value = (None, 200, {})

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_balances()

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "No data received for raw balances dict, status: 200" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_balances_validation_error_via_public_api(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_balances handles validation error from response handler."""
        mock_request_builder.build_get_balances_params.return_value = None
        mock_http_client_requester.return_value = ({"invalid": "data"}, 200, {})
        mock_response_handler.handle_get_balances_response.side_effect = (
            ValidationError.from_exception_data(
                title="ValidationError",
                line_errors=[],
            )
        )

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_balances()

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Processing raw balances dict data failed" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_balances_unexpected_exception_via_public_api(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_balances handles unexpected exception from response handler."""
        mock_request_builder.build_get_balances_params.return_value = None
        mock_http_client_requester.return_value = ({"balance": "data"}, 200, {})
        mock_response_handler.handle_get_balances_response.side_effect = Exception(
            "Unexpected error"
        )

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_balances()

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected error for raw balances dict" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_balances_validation_error_coverage(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_balances handles validation error from response handler."""
        mock_request_builder.build_get_balances_params.return_value = None
        mock_http_client_requester.return_value = ({"invalid": "data"}, 200, {})
        mock_response_handler.handle_get_balances_response.side_effect = (
            ValidationError.from_exception_data(
                title="ValidationError",
                line_errors=[],
            )
        )

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_balances()

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Processing raw balances dict data failed" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_balances_unexpected_exception_coverage(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_balances handles unexpected exception from response handler."""
        mock_request_builder.build_get_balances_params.return_value = None
        mock_http_client_requester.return_value = ({"balance": "data"}, 200, {})
        mock_response_handler.handle_get_balances_response.side_effect = Exception(
            "Unexpected error"
        )

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_balances()

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected error for raw balances dict" in exc_info.value.message
