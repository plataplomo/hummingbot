"""Unit tests for BackpackAccountService position functionality."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_margin_functions import (
    BackpackRawImfFunction,
    BackpackRawMmfFunction,
)
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPosition
from cyberdelta.apis.backpack.models.bp_raw_query_params import BackpackRawGetPositionsParams
from cyberdelta.apis.backpack.services.bp_account_service import BackpackAccountService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models.derivative_position import DerivativePosition
from cyberdelta.core.models.enums import OrderSide


class TestBackpackAccountServicePositions:
    """Tests for the BackpackAccountService position functionality."""

    @pytest.mark.asyncio
    async def test_get_positions_raw_success(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test _get_raw_positions_list successfully fetches and processes position data.

        Tested via public get_positions.
        """
        symbol_arg = "SOL-PERP"
        mock_raw_positions_data_item_dict = {
            "symbol": "SOL-PERP",
            "subaccountId": 0,  # Add missing required field
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
        mock_validated_raw_positions = [
            BackpackRawPosition.model_validate(mock_raw_positions_data_item_dict),
        ]

        mock_internal_derivative_position = DerivativePosition(
            exchange="backpack_test_account",
            symbol=symbol_arg,
            timestamp=datetime.now(UTC),
            side=OrderSide.BUY,
            size=Decimal("10.0"),
            entry_price=Decimal("100.0"),
            mark_price=Decimal("110.0"),
            unrealized_pnl=Decimal("100.0"),
            realized_pnl=Decimal("0.0"),
            liquidation_price=Decimal("90.0"),
            bp_details=None,
            hl_details=None,
        )
        expected_positions_result: list[DerivativePosition] = [mock_internal_derivative_position]

        def build_get_positions_params_side_effect(
            symbol: str | None = None,
        ) -> BackpackRawGetPositionsParams:
            """Build request parameters for get positions API calls."""
            # Return the actual model that the real request builder returns
            return BackpackRawGetPositionsParams()

        mock_request_builder.build_get_positions_params.side_effect = (
            build_get_positions_params_side_effect
        )

        mock_http_client_requester.return_value = (
            mock_raw_positions_data_item_dict,
            200,
            MagicMock(),
        )
        mock_response_handler.handle_get_positions_response.return_value = (
            mock_validated_raw_positions
        )
        mock_mapper.transform_raw_position_to_internal.return_value = (
            mock_internal_derivative_position
        )

        with patch.object(bp_account_service, "_mapper", mock_mapper):
            result_no_symbol = await bp_account_service.get_positions(symbol=None)

        mock_request_builder.build_get_positions_params.assert_called_with(None)
        mock_http_client_requester.assert_called_with(
            method="GET",
            endpoint="/api/v1/position",  # Endpoint for no symbol (singular)
            params={},  # Empty dict from BackpackRawGetPositionsParams().model_dump()
            is_signed=True,
            endpoint_group="private",
            request_weight=1,
        )
        mock_response_handler.handle_get_positions_response.assert_called_with(
            mock_raw_positions_data_item_dict,
            None,
            200,
        )
        mock_mapper.transform_raw_position_to_internal.assert_called_with(
            mock_validated_raw_positions[0],
        )
        assert len(result_no_symbol) == len(expected_positions_result)
        for actual, expected in zip(result_no_symbol, expected_positions_result, strict=False):
            assert actual.exchange == expected.exchange
            assert actual.symbol == expected.symbol
            assert isinstance(actual.timestamp, datetime)
            assert actual.timestamp.tzinfo == UTC
            assert actual.side == expected.side
            assert actual.size == expected.size
            assert actual.entry_price == expected.entry_price
            assert actual.mark_price == expected.mark_price
            assert actual.unrealized_pnl == expected.unrealized_pnl

        # --- Test with symbol ---
        mock_http_client_requester.reset_mock()
        mock_response_handler.reset_mock()
        mock_request_builder.build_get_positions_params.reset_mock()
        mock_mapper.transform_raw_position_to_internal.reset_mock()

        # Reconfigure mocks for the call with symbol
        mock_response_handler.handle_get_positions_response.return_value = (
            mock_validated_raw_positions  # Still returns list
        )
        mock_mapper.transform_raw_position_to_internal.return_value = (
            mock_internal_derivative_position
        )

        with patch.object(bp_account_service, "_mapper", mock_mapper):
            result_with_symbol = await bp_account_service.get_positions(symbol=symbol_arg)

        mock_request_builder.build_get_positions_params.assert_called_with(symbol_arg)
        mock_http_client_requester.assert_called_with(
            method="GET",
            endpoint="/api/v1/position",  # Singular endpoint
            params={},  # Empty dict from BackpackRawGetPositionsParams().model_dump()
            is_signed=True,
            endpoint_group="private",
            request_weight=1,
        )
        mock_response_handler.handle_get_positions_response.assert_called_with(
            mock_raw_positions_data_item_dict,
            symbol_arg,
            200,
        )
        mock_mapper.transform_raw_position_to_internal.assert_called_with(
            mock_validated_raw_positions[0],
        )
        assert len(result_with_symbol) == len(expected_positions_result)
        for actual, expected in zip(result_with_symbol, expected_positions_result, strict=False):
            assert actual.exchange == expected.exchange
            assert actual.symbol == expected.symbol
            assert isinstance(actual.timestamp, datetime)
            assert actual.timestamp.tzinfo == UTC
            assert actual.side == expected.side
            assert actual.size == expected.size
            assert actual.entry_price == expected.entry_price
            assert actual.mark_price == expected.mark_price
            assert actual.unrealized_pnl == expected.unrealized_pnl

    @pytest.mark.asyncio
    async def test_get_positions_http_client_returns_none(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test get_positions when HTTP client returns None content."""
        symbol = "SOL_USDC"
        mock_request_builder.build_get_positions_params.return_value = (
            BackpackRawGetPositionsParams()
        )
        mock_http_client_requester.return_value = (None, 200, {})

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_positions(symbol=symbol)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Unexpected response type for positions: NoneType" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_positions_validation_error_via_public_api(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_positions handles validation error from response handler."""
        symbol = "SOL_USDC"
        mock_request_builder.build_get_positions_params.return_value = (
            BackpackRawGetPositionsParams()
        )
        mock_http_client_requester.return_value = ([{"invalid": "position"}], 200, {})
        mock_response_handler.handle_get_positions_response.side_effect = (
            ValidationError.from_exception_data(
                title="ValidationError",
                line_errors=[],
            )
        )

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_positions(symbol)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Processing raw positions data failed" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_positions_unexpected_exception_via_public_api(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_positions handles unexpected exception via public API."""
        mock_request_builder.build_get_positions_params.return_value = (
            BackpackRawGetPositionsParams()
        )
        mock_http_client_requester.return_value = ([{"symbol": "SOL_USDC"}], 200, {})
        mock_response_handler.handle_get_positions_response.side_effect = Exception(
            "Unexpected error",
        )

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_positions()

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected error for raw positions" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_positions_with_all_symbol_scenarios(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test get_positions with None symbol (all positions scenario)."""
        mock_raw_position_data = {
            "symbol": "SOL_USDC",
            "subaccountId": 0,  # Add missing required field
            "breakEvenPrice": "100.0",
            "entryPrice": "100.0",
            "estLiquidationPrice": "90.0",
            "imf": "0.1",
            "imfFunction": {"base": "0.05", "factor": "0.01"},
            "markPrice": "105.0",
            "mmf": "0.05",
            "mmfFunction": {"base": "0.03", "factor": "0.005"},
            "netCost": "1000.0",
            "netQuantity": "10.0",
            "netExposureQuantity": "10.0",
            "netExposureNotional": "1050.0",
            "pnlRealized": "0.0",
            "pnlUnrealized": "50.0",
            "cumulativeFundingPayment": "5.0",
            "userId": 123,
            "positionId": "pos123",
            "cumulativeInterest": "0.0",
        }
        mock_raw_response = [mock_raw_position_data]
        mock_validated_positions = [BackpackRawPosition.model_validate(mock_raw_position_data)]

        expected_position = DerivativePosition(
            exchange="backpack_test_account",
            symbol="SOL_USDC",
            side=OrderSide.BUY,
            size=Decimal("10.0"),
            entry_price=Decimal("100.0"),
            mark_price=Decimal("105.0"),
            unrealized_pnl=Decimal("50.0"),
            liquidation_price=None,
            timestamp=datetime.now(UTC),
            bp_details=None,
            hl_details=None,
        )

        mock_request_builder.build_get_positions_params.return_value = (
            BackpackRawGetPositionsParams()
        )
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_get_positions_response.return_value = mock_validated_positions
        mock_mapper.transform_raw_position_to_internal.return_value = expected_position

        with patch.object(bp_account_service, "_mapper", mock_mapper):
            result = await bp_account_service.get_positions(symbol=None)

        mock_request_builder.build_get_positions_params.assert_called_once_with(None)
        assert len(result) == 1
        assert result[0].symbol == expected_position.symbol

    @pytest.mark.asyncio
    async def test_get_positions_comprehensive_scenarios(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test get_positions with comprehensive scenarios including proper construction.

        Tests proper DerivativePosition construction.
        """
        mock_raw_position_data = {
            "symbol": "SOL_USDC",
            "subaccountId": 0,  # Add missing required field
            "breakEvenPrice": "100.0",
            "entryPrice": "100.0",
            "estLiquidationPrice": "90.0",
            "imf": "0.1",
            "imfFunction": {"base": "0.05", "factor": "0.01"},
            "markPrice": "105.0",
            "mmf": "0.05",
            "mmfFunction": {"base": "0.03", "factor": "0.005"},
            "netCost": "1000.0",
            "netQuantity": "10.0",
            "netExposureQuantity": "10.0",
            "netExposureNotional": "1050.0",
            "pnlRealized": "0.0",
            "pnlUnrealized": "50.0",
            "cumulativeFundingPayment": "5.0",
            "userId": 123,
            "positionId": "pos123",
            "cumulativeInterest": "0.0",
        }
        mock_raw_response = [mock_raw_position_data]
        mock_validated_positions = [BackpackRawPosition.model_validate(mock_raw_position_data)]

        expected_position = DerivativePosition(
            exchange="backpack_test_account",
            symbol="SOL_USDC",
            side=OrderSide.BUY,
            size=Decimal("10.0"),
            entry_price=Decimal("100.0"),
            mark_price=Decimal("105.0"),
            unrealized_pnl=Decimal("50.0"),
            liquidation_price=None,
            timestamp=datetime.now(UTC),
            bp_details=None,
            hl_details=None,
        )

        mock_request_builder.build_get_positions_params.return_value = (
            BackpackRawGetPositionsParams()
        )
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_get_positions_response.return_value = mock_validated_positions
        mock_mapper.transform_raw_position_to_internal.return_value = expected_position

        with patch.object(bp_account_service, "_mapper", mock_mapper):
            result = await bp_account_service.get_positions(symbol=None)

        mock_request_builder.build_get_positions_params.assert_called_once_with(None)
        assert len(result) == 1
        assert result[0].symbol == expected_position.symbol

    @pytest.mark.asyncio
    async def test_get_positions_validation_error_coverage(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_positions handles validation error from response handler."""
        symbol = "SOL_USDC"
        mock_request_builder.build_get_positions_params.return_value = (
            BackpackRawGetPositionsParams()
        )
        mock_http_client_requester.return_value = ([{"invalid": "position"}], 200, {})
        mock_response_handler.handle_get_positions_response.side_effect = (
            ValidationError.from_exception_data(
                title="ValidationError",
                line_errors=[],
            )
        )

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_positions(symbol)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Processing raw positions data failed" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_positions_unexpected_exception_coverage(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_positions handles unexpected exception."""
        mock_request_builder.build_get_positions_params.return_value = (
            BackpackRawGetPositionsParams()
        )
        mock_http_client_requester.return_value = ([{"symbol": "SOL_USDC"}], 200, {})
        mock_response_handler.handle_get_positions_response.side_effect = Exception(
            "Unexpected error",
        )

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_positions()

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected error for raw positions" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_positions_with_symbol_parameter(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test get_positions with specific symbol parameter."""
        symbol = "BTC_USDC"
        mock_request_builder.build_get_positions_params.return_value = (
            BackpackRawGetPositionsParams()
        )
        mock_http_client_requester.return_value = ([], 200, {})
        mock_response_handler.handle_get_positions_response.return_value = []

        with patch.object(bp_account_service, "_mapper", mock_mapper):
            result = await bp_account_service.get_positions(symbol=symbol)

        assert result == []
        mock_request_builder.build_get_positions_params.assert_called_once_with(symbol)

    @pytest.mark.asyncio
    async def test_get_positions_mapper_exception_handling(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test get_positions handles mapper exceptions gracefully."""
        symbol = "SOL_USDC"
        mock_raw_position = BackpackRawPosition(
            symbol=symbol,
            subaccountId=0,  # Add missing required field
            breakEvenPrice="100.0",
            entryPrice="100.0",
            estLiquidationPrice="90.0",
            imf="0.1",
            imfFunction=BackpackRawImfFunction(base="0.05", factor="0.01"),
            markPrice="105.0",
            mmf="0.05",
            mmfFunction=BackpackRawMmfFunction(base="0.03", factor="0.005"),
            netCost="1000.0",
            netQuantity="10.0",
            netExposureQuantity="10.0",
            netExposureNotional="1050.0",
            pnlRealized="0.0",
            pnlUnrealized="50.0",
            cumulativeFundingPayment="5.0",
            userId=123,
            positionId="pos123",
            cumulativeInterest="0.0",
        )

        mock_request_builder.build_get_positions_params.return_value = (
            BackpackRawGetPositionsParams()
        )
        mock_http_client_requester.return_value = ([mock_raw_position.model_dump()], 200, {})
        mock_response_handler.handle_get_positions_response.return_value = [mock_raw_position]
        mock_mapper.transform_raw_position_to_internal.side_effect = (
            ValidationError.from_exception_data(
                title="ValidationError",
                line_errors=[],
            )
        )

        with patch.object(bp_account_service, "_mapper", mock_mapper):
            result = await bp_account_service.get_positions(symbol=symbol)

        # Should return empty list when mapping fails
        assert result == []
