"""Unit tests for BackpackMarketDataService market metadata functionality."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_market import BackpackRawMarketResponse
from cyberdelta.apis.backpack.services.bp_market_data_service import BackpackMarketDataService
from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.models.service_args.market_data import GetMarketArgs, GetMarketsArgs
from cyberdelta.models.market import Market


# Import fixtures from the shared conftest
pytest_plugins = ["tests.unit.apis.backpack.services.conftest_market_data"]


class TestBackpackMarketDataServiceMarketMetadata:
    """Tests for the BackpackMarketDataService market metadata functionality."""

    @pytest.mark.asyncio
    async def test_get_market_success(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_market successfully retrieves and processes market metadata.

        Note: Current business logic delegates to market metadata service.
        """
        symbol = "BTC_USDC"

        # Create expected internal market
        expected_market = MagicMock(spec=Market)
        expected_market.symbol = symbol

        # Mock the market metadata service since business logic delegates to it
        with patch.object(
            backpack_market_data_service, "_market_metadata_service"
        ) as mock_metadata_service:
            mock_metadata_service.get_market = AsyncMock(return_value=expected_market)

            args = GetMarketArgs(symbol=symbol)
            result = await backpack_market_data_service.get_market(args)

            # Verify the business logic calls the market metadata service with correct arguments
            mock_metadata_service.get_market.assert_called_once_with(args)
            assert result == expected_market

    @pytest.mark.asyncio
    async def test_get_market_empty_symbol_validation_error(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_market validation error for empty symbol at args model level.

        Note: Current business logic delegates to market metadata service.
        """
        empty_symbol = ""

        # The validation error should occur when creating GetMarketArgs, not in the service
        with pytest.raises(ValidationError) as exc_info:
            GetMarketArgs(symbol=empty_symbol)

        assert "String cannot be empty" in str(exc_info.value)

        # Note: No service calls are made because the validation fails at args creation

    @pytest.mark.asyncio
    async def test_get_market_http_client_returns_none(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_market when market metadata service raises error.

        Note: Current business logic delegates to market metadata service.
        """
        symbol = "BTC_USDC"

        # Mock the market metadata service to raise an API error
        with patch.object(
            backpack_market_data_service, "_market_metadata_service"
        ) as mock_metadata_service:
            api_error = APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message=f"No data received for market ({symbol}), status: 200",
            )
            mock_metadata_service.get_market = AsyncMock(side_effect=api_error)

            args = GetMarketArgs(symbol=symbol)
            with pytest.raises(APIError) as exc_info:
                await backpack_market_data_service.get_market(args)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            expected_error_msg = f"No data received for market ({symbol}), status: 200"
            assert exc_info.value.message == expected_error_msg

            # Verify the business logic calls the market metadata service
            mock_metadata_service.get_market.assert_called_once_with(args)

    @pytest.mark.asyncio
    async def test_get_market_invalid_response_type(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_market when market metadata service raises response format error.

        Note: Current business logic delegates to market metadata service.
        """
        symbol = "BTC_USDC"

        # Mock the market metadata service to raise an API error for invalid response type
        with patch.object(
            backpack_market_data_service, "_market_metadata_service"
        ) as mock_metadata_service:
            api_error = APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message=f"Unexpected market ({symbol}) response format: expected dict, got list",
            )
            mock_metadata_service.get_market = AsyncMock(side_effect=api_error)

            args = GetMarketArgs(symbol=symbol)
            with pytest.raises(APIError) as exc_info:
                await backpack_market_data_service.get_market(args)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            expected_error_msg = (
                f"Unexpected market ({symbol}) response format: expected dict, got list"
            )
            assert exc_info.value.message == expected_error_msg

            # Verify the business logic calls the market metadata service
            mock_metadata_service.get_market.assert_called_once_with(args)

    @pytest.mark.asyncio
    async def test_get_market_response_handler_validation_error(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_market handles validation error from market metadata service.

        Note: Current business logic delegates to market metadata service.
        """
        symbol = "BTC_USDC"

        # Create a ValidationError
        try:
            BackpackRawMarketResponse.model_validate({"invalid": "data"})
        except ValidationError as validation_error:
            # Mock the market metadata service to raise the validation error
            with patch.object(
                backpack_market_data_service, "_market_metadata_service"
            ) as mock_metadata_service:
                mock_metadata_service.get_market = AsyncMock(side_effect=validation_error)

                args = GetMarketArgs(symbol=symbol)
                with pytest.raises(ValidationError):
                    await backpack_market_data_service.get_market(args)

                # Verify the business logic calls the market metadata service
                mock_metadata_service.get_market.assert_called_once()
                call_args = mock_metadata_service.get_market.call_args[0][0]
                assert call_args.symbol == symbol

    @pytest.mark.asyncio
    async def test_get_market_transformation_error(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_market handles transformation error from market metadata service.

        Note: Current business logic delegates to market metadata service.
        """
        symbol = "BTC_USDC"

        # Mock the market metadata service to raise a transformation error
        with patch.object(
            backpack_market_data_service, "_market_metadata_service"
        ) as mock_metadata_service:
            transformation_error = TransformationError("Failed to transform market data")
            mock_metadata_service.get_market = AsyncMock(side_effect=transformation_error)

            args = GetMarketArgs(symbol=symbol)
            with pytest.raises(TransformationError) as exc_info:
                await backpack_market_data_service.get_market(args)

            assert "Failed to transform market data" in str(exc_info.value)

            # Verify the business logic calls the market metadata service
            mock_metadata_service.get_market.assert_called_once()
            call_args = mock_metadata_service.get_market.call_args[0][0]
            assert call_args.symbol == symbol

    @pytest.mark.asyncio
    async def test_get_market_unexpected_exception(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_market handles unexpected exception from market metadata service.

        Note: Current business logic delegates to market metadata service.
        """
        symbol = "BTC_USDC"

        # Mock the market metadata service to raise an unexpected exception
        with patch.object(
            backpack_market_data_service, "_market_metadata_service"
        ) as mock_metadata_service:
            mock_metadata_service.get_market = AsyncMock(side_effect=Exception("Unexpected error"))

            args = GetMarketArgs(symbol=symbol)
            with pytest.raises(Exception) as exc_info:
                await backpack_market_data_service.get_market(args)

            assert "Unexpected error" in str(exc_info.value)

            # Verify the business logic calls the market metadata service
            mock_metadata_service.get_market.assert_called_once()
            call_args = mock_metadata_service.get_market.call_args[0][0]
            assert call_args.symbol == symbol

    @pytest.mark.asyncio
    async def test_get_markets_success(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_markets successfully retrieves and processes all market metadata.

        Note: Current business logic delegates to market metadata service.
        """
        # Create expected internal markets
        expected_markets = [MagicMock(spec=Market), MagicMock(spec=Market)]
        expected_markets[0].symbol = "BTC_USDC"
        expected_markets[1].symbol = "ETH_USDC"

        # Mock the market metadata service since business logic delegates to it
        with patch.object(
            backpack_market_data_service, "_market_metadata_service"
        ) as mock_metadata_service:
            mock_metadata_service.get_markets = AsyncMock(return_value=expected_markets)

            args = GetMarketsArgs()
            result = await backpack_market_data_service.get_markets(args)

            # Verify the business logic calls the market metadata service with correct arguments
            mock_metadata_service.get_markets.assert_called_once_with(args)
            assert result == expected_markets

    @pytest.mark.asyncio
    async def test_get_markets_empty_list(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_markets handles empty markets list.

        Note: Current business logic delegates to market metadata service.
        """
        # Mock the market metadata service to return empty list
        with patch.object(
            backpack_market_data_service, "_market_metadata_service"
        ) as mock_metadata_service:
            mock_metadata_service.get_markets = AsyncMock(return_value=[])

            args = GetMarketsArgs()
            result = await backpack_market_data_service.get_markets(args)

            assert result == []
            # Verify the business logic calls the market metadata service
            mock_metadata_service.get_markets.assert_called_once_with(args)

    @pytest.mark.asyncio
    async def test_get_markets_http_client_returns_none(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_markets when market metadata service raises error.

        Note: Current business logic delegates to market metadata service.
        """
        # Mock the market metadata service to raise an API error
        with patch.object(
            backpack_market_data_service, "_market_metadata_service"
        ) as mock_metadata_service:
            api_error = APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="No data received for markets data, status: 200",
            )
            mock_metadata_service.get_markets = AsyncMock(side_effect=api_error)

            args = GetMarketsArgs()
            with pytest.raises(APIError) as exc_info:
                await backpack_market_data_service.get_markets(args)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            expected_error_msg = "No data received for markets data, status: 200"
            assert exc_info.value.message == expected_error_msg

            # Verify the business logic calls the market metadata service
            mock_metadata_service.get_markets.assert_called_once_with(args)

    @pytest.mark.asyncio
    async def test_get_markets_invalid_response_type(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_markets when market metadata service raises response format error.

        Note: Current business logic delegates to market metadata service.
        """
        # Mock the market metadata service to raise an API error for invalid response type
        with patch.object(
            backpack_market_data_service, "_market_metadata_service"
        ) as mock_metadata_service:
            api_error = APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Unexpected markets data response format: expected list, got dict",
            )
            mock_metadata_service.get_markets = AsyncMock(side_effect=api_error)

            args = GetMarketsArgs()
            with pytest.raises(APIError) as exc_info:
                await backpack_market_data_service.get_markets(args)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            expected_error_msg = "Unexpected markets data response format: expected list, got dict"
            assert exc_info.value.message == expected_error_msg

            # Verify the business logic calls the market metadata service
            mock_metadata_service.get_markets.assert_called_once_with(args)

    @pytest.mark.asyncio
    async def test_get_markets_response_handler_validation_error(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_markets handles validation error from market metadata service.

        Note: Current business logic delegates to market metadata service.
        """
        # Create a ValidationError
        try:
            BackpackRawMarketResponse.model_validate({"invalid": "data"})
        except ValidationError as validation_error:
            # Mock the market metadata service to raise the validation error
            with patch.object(
                backpack_market_data_service, "_market_metadata_service"
            ) as mock_metadata_service:
                mock_metadata_service.get_markets = AsyncMock(side_effect=validation_error)

                args = GetMarketsArgs()
                with pytest.raises(ValidationError):
                    await backpack_market_data_service.get_markets(args)

                # Verify the business logic calls the market metadata service
                mock_metadata_service.get_markets.assert_called_once_with(args)

    @pytest.mark.asyncio
    async def test_get_markets_transformation_error(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_markets handles transformation error from market metadata service.

        Note: Current business logic delegates to market metadata service.
        """
        # Mock the market metadata service to raise a transformation error
        with patch.object(
            backpack_market_data_service, "_market_metadata_service"
        ) as mock_metadata_service:
            transformation_error = TransformationError("Failed to transform markets data")
            mock_metadata_service.get_markets = AsyncMock(side_effect=transformation_error)

            args = GetMarketsArgs()
            with pytest.raises(TransformationError) as exc_info:
                await backpack_market_data_service.get_markets(args)

            assert "Failed to transform markets data" in str(exc_info.value)

            # Verify the business logic calls the market metadata service
            mock_metadata_service.get_markets.assert_called_once_with(args)

    @pytest.mark.asyncio
    async def test_get_markets_unexpected_exception(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_markets handles unexpected exception from market metadata service.

        Note: Current business logic delegates to market metadata service.
        """
        # Mock the market metadata service to raise an unexpected exception
        with patch.object(
            backpack_market_data_service, "_market_metadata_service"
        ) as mock_metadata_service:
            mock_metadata_service.get_markets = AsyncMock(side_effect=Exception("Unexpected error"))

            args = GetMarketsArgs()
            with pytest.raises(Exception) as exc_info:
                await backpack_market_data_service.get_markets(args)

            assert "Unexpected error" in str(exc_info.value)

            # Verify the business logic calls the market metadata service
            mock_metadata_service.get_markets.assert_called_once_with(args)

    @pytest.mark.asyncio
    async def test_get_markets_api_error_propagation(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_markets properly propagates APIError from market metadata service.

        Note: Current business logic delegates to market metadata service.
        """
        # Create an APIError that would come from the market metadata service
        api_error = APIError("Service unavailable", APIErrorCode.SERVICE_UNAVAILABLE.value)

        # Mock the market metadata service to raise the API error
        with patch.object(
            backpack_market_data_service, "_market_metadata_service"
        ) as mock_metadata_service:
            mock_metadata_service.get_markets = AsyncMock(side_effect=api_error)

            args = GetMarketsArgs()
            with pytest.raises(APIError) as exc_info:
                await backpack_market_data_service.get_markets(args)

            # The original APIError should be re-raised
            assert exc_info.value == api_error
            assert exc_info.value.code == APIErrorCode.SERVICE_UNAVAILABLE.value

            # Verify the business logic calls the market metadata service
            mock_metadata_service.get_markets.assert_called_once_with(args)

    @pytest.mark.asyncio
    async def test_get_market_api_error_propagation(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_market properly propagates APIError from market metadata service.

        Note: Current business logic delegates to market metadata service.
        """
        symbol = "BTC_USDC"

        # Create an APIError that would come from the market metadata service
        api_error = APIError("Symbol not found", APIErrorCode.SYMBOL_NOT_FOUND.value)

        # Mock the market metadata service to raise the API error
        with patch.object(
            backpack_market_data_service, "_market_metadata_service"
        ) as mock_metadata_service:
            mock_metadata_service.get_market = AsyncMock(side_effect=api_error)

            args = GetMarketArgs(symbol=symbol)
            with pytest.raises(APIError) as exc_info:
                await backpack_market_data_service.get_market(args)

            # The original APIError should be re-raised
            assert exc_info.value == api_error
            assert exc_info.value.code == APIErrorCode.SYMBOL_NOT_FOUND.value

            # Verify the business logic calls the market metadata service
            mock_metadata_service.get_market.assert_called_once_with(args)
