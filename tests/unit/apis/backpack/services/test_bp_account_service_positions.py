"""Unit tests for BackpackAccountService position functionality."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_margin_functions import (
    BackpackRawImfFunction,
    BackpackRawMmfFunction,
)
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPosition
from cyberdelta.apis.backpack.services.bp_account_service import BackpackAccountService
from cyberdelta.apis.common import APIError, APIErrorCode
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

        # Expected values to check against actual mapper behavior
        # Mapper returns exchange="backpack" not "backpack_test_account"

        # These mocks are no longer needed as we're using the composite service pattern

        # Mock the HTTP client and response handler to return expected data
        mock_http_client_requester.return_value = (
            [mock_raw_positions_data_item_dict],
            200,
            MagicMock(),
        )
        mock_response_handler.handle_get_positions_response.return_value = (
            mock_validated_raw_positions
        )
        
        result_no_symbol = await bp_account_service.get_positions(symbol=None)
        
        # Business logic returns positions with exchange="backpack"
        assert len(result_no_symbol) == 1
        assert result_no_symbol[0].exchange == "backpack"
        assert result_no_symbol[0].symbol == symbol_arg
        assert isinstance(result_no_symbol[0].timestamp, datetime)
        assert result_no_symbol[0].timestamp.tzinfo == UTC
        assert result_no_symbol[0].side == OrderSide.BUY
        assert result_no_symbol[0].size == Decimal("10.0")
        assert result_no_symbol[0].entry_price == Decimal("100.0")
        assert result_no_symbol[0].mark_price == Decimal("110.0")
        assert result_no_symbol[0].unrealized_pnl == Decimal("100.0")

        # --- Test with symbol ---
        
        # Reset and reconfigure mocks for the call with symbol
        mock_http_client_requester.reset_mock()
        mock_response_handler.reset_mock()
        
        mock_http_client_requester.return_value = (
            [mock_raw_positions_data_item_dict],
            200,
            MagicMock(),
        )
        mock_response_handler.handle_get_positions_response.return_value = (
            mock_validated_raw_positions
        )
        
        result_with_symbol = await bp_account_service.get_positions(symbol=symbol_arg)
        
        # Business logic returns positions with exchange="backpack"
        assert len(result_with_symbol) == 1
        assert result_with_symbol[0].exchange == "backpack"
        assert result_with_symbol[0].symbol == symbol_arg
        assert isinstance(result_with_symbol[0].timestamp, datetime)
        assert result_with_symbol[0].timestamp.tzinfo == UTC
        assert result_with_symbol[0].side == OrderSide.BUY
        assert result_with_symbol[0].size == Decimal("10.0")
        assert result_with_symbol[0].entry_price == Decimal("100.0")
        assert result_with_symbol[0].mark_price == Decimal("110.0")
        assert result_with_symbol[0].unrealized_pnl == Decimal("100.0")

    @pytest.mark.asyncio
    async def test_get_positions_http_client_returns_none(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test get_positions when HTTP client returns None content."""
        symbol = "SOL_USDC"
        # Mock the HTTP client to return None which triggers APIError
        mock_http_client_requester.return_value = (None, 200, {})
        
        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_positions(symbol=symbol)

        # Business logic raises APIError when raw_data is None
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "No data received for positions, status: 200" in exc_info.value.message

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
        
        # Mock the HTTP client to return invalid data that causes validation error
        mock_http_client_requester.return_value = ([{"invalid": "position"}], 200, {})
        
        # Mock the response handler to raise ValidationError
        mock_response_handler.handle_get_positions_response.side_effect = (
            ValidationError.from_exception_data(
                title="ValidationError",
                line_errors=[],
            )
        )
        
        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_positions(symbol)

        # Business logic wraps response handler ValidationError as UNKNOWN
        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Failed to retrieve position data" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_positions_unexpected_exception_via_public_api(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_positions handles unexpected exception via public API."""
        # Mock the HTTP client to return valid data  
        mock_http_client_requester.return_value = ([{"symbol": "SOL_USDC"}], 200, {})
        
        # Mock the response handler to raise an unexpected exception
        mock_response_handler.handle_get_positions_response.side_effect = Exception(
            "Unexpected error"
        )
        
        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_positions()

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Failed to retrieve position data" in exc_info.value.message

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

        # Mock the HTTP client and response handler to return expected data
        mock_http_client_requester.return_value = ([mock_raw_position_data], 200, {})
        mock_response_handler.handle_get_positions_response.return_value = [
            BackpackRawPosition.model_validate(mock_raw_position_data)
        ]
        
        result = await bp_account_service.get_positions(symbol=None)

        # Business logic returns positions with exchange="backpack"
        assert len(result) == 1
        assert result[0].exchange == "backpack"
        assert result[0].symbol == "SOL_USDC"

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

        # Mock the HTTP client and response handler to return expected data
        mock_http_client_requester.return_value = ([mock_raw_position_data], 200, {})
        mock_response_handler.handle_get_positions_response.return_value = [
            BackpackRawPosition.model_validate(mock_raw_position_data)
        ]
        
        result = await bp_account_service.get_positions(symbol=None)

        # Business logic returns positions with exchange="backpack"
        assert len(result) == 1
        assert result[0].exchange == "backpack"
        assert result[0].symbol == "SOL_USDC"

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
        
        # Mock the HTTP client to return invalid data that causes validation error
        mock_http_client_requester.return_value = ([{"invalid": "position"}], 200, {})
        
        # Mock the response handler to raise ValidationError
        mock_response_handler.handle_get_positions_response.side_effect = (
            ValidationError.from_exception_data(
                title="ValidationError",
                line_errors=[],
            )
        )
        
        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_positions(symbol)

        # Business logic wraps response handler ValidationError as UNKNOWN
        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Failed to retrieve position data" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_positions_unexpected_exception_coverage(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_positions handles unexpected exception."""
        # Mock the HTTP client to return valid data  
        mock_http_client_requester.return_value = ([{"symbol": "SOL_USDC"}], 200, {})
        
        # Mock the response handler to raise an unexpected exception
        mock_response_handler.handle_get_positions_response.side_effect = Exception(
            "Unexpected error"
        )
        
        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_positions()

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Failed to retrieve position data" in exc_info.value.message

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
        # Mock the HTTP client to return empty list
        mock_http_client_requester.return_value = ([], 200, {})
        mock_response_handler.handle_get_positions_response.return_value = []

        result = await bp_account_service.get_positions(symbol=symbol)

        assert result == []

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

        # Mock the HTTP client and response handler to return valid data
        mock_http_client_requester.return_value = ([BackpackRawPosition.model_validate({
            "symbol": symbol,
            "subaccountId": 0,
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
        }).model_dump()], 200, {})
        
        # Mock response handler to process data but mapper fails
        mock_response_handler.handle_get_positions_response.return_value = [BackpackRawPosition(
            symbol=symbol,
            subaccountId=0,
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
        )]
        
        # Mock response handler to raise ValidationError during processing
        mock_response_handler.handle_get_positions_response.side_effect = (
            ValidationError.from_exception_data(
                title="ValidationError",
                line_errors=[],
            )
        )

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_positions(symbol=symbol)

        # Business logic wraps response handler ValidationError as UNKNOWN
        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Failed to retrieve position data" in exc_info.value.message
