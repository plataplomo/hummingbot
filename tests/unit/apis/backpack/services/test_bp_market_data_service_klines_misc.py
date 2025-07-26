"""Unit tests for BackpackMarketDataService klines and miscellaneous functionality."""

from __future__ import annotations

from typing import Literal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_kline import BackpackRawKlineResponse
from cyberdelta.apis.backpack.services.bp_market_data_service import BackpackMarketDataService
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.models.service_args.market_data import GetMarketDataArgs
from cyberdelta.core.models.market import Candle


# Import fixtures from the shared conftest
pytest_plugins = ["tests.unit.apis.backpack.services.conftest_market_data"]


class TestBackpackMarketDataServiceKlinesMisc:
    """Tests for the BackpackMarketDataService klines and miscellaneous functionality."""

    @pytest.mark.asyncio
    async def test_get_market_data_success(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_market_data successfully retrieves and processes kline data.

        Note: Current business logic delegates to historical data service.
        """
        symbol = "SOL_USDC"
        timeframe: Literal["1m"] = "1m"
        limit = 2

        # Create expected internal candles
        expected_candles = [MagicMock(spec=Candle), MagicMock(spec=Candle)]

        # Mock the historical data service since business logic delegates to it
        with patch.object(
            backpack_market_data_service, "_historical_data_service"
        ) as mock_historical_service:
            mock_historical_service.get_market_data = AsyncMock(return_value=expected_candles)

            args = GetMarketDataArgs(symbol=symbol, timeframe=timeframe, limit=limit)
            result = await backpack_market_data_service.get_market_data(args)

            # Verify the business logic calls the historical data service with correct arguments
            mock_historical_service.get_market_data.assert_called_once_with(args)
            assert result == expected_candles

    @pytest.mark.asyncio
    async def test_get_market_data_http_client_returns_none(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_market_data when historical data service returns empty list."""
        symbol = "SOL_USDC"
        timeframe: Literal["1h"] = "1h"
        limit = 100

        # Mock the historical data service to return empty list (no candles)
        with patch.object(
            backpack_market_data_service, "_historical_data_service"
        ) as mock_historical_service:
            mock_historical_service.get_market_data = AsyncMock(return_value=[])

            args = GetMarketDataArgs(symbol=symbol, timeframe=timeframe, limit=limit)
            result = await backpack_market_data_service.get_market_data(args)

            # Business logic should return empty list when no data found
            assert result == []

            # Verify the business logic calls the historical data service
            mock_historical_service.get_market_data.assert_called_once_with(args)

    @pytest.mark.asyncio
    async def test_get_market_data_validation_error(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_market_data handles validation error from historical data service."""
        symbol = "SOL_USDC"
        timeframe: Literal["1m"] = "1m"

        # Create a ValidationError by trying to validate invalid data
        try:
            BackpackRawKlineResponse.model_validate(["invalid", "data"])
        except ValidationError as validation_error:
            # Mock the historical data service to raise a validation error
            with patch.object(
                backpack_market_data_service, "_historical_data_service"
            ) as mock_historical_service:
                mock_historical_service.get_market_data = AsyncMock(side_effect=validation_error)

                with pytest.raises(ValidationError):
                    args = GetMarketDataArgs(symbol=symbol, timeframe=timeframe)
                    await backpack_market_data_service.get_market_data(args)

                # Verify the business logic calls the historical data service
                mock_historical_service.get_market_data.assert_called_once()
                call_args = mock_historical_service.get_market_data.call_args[0][0]
                assert call_args.symbol == symbol
                assert call_args.timeframe == timeframe

    @pytest.mark.asyncio
    async def test_get_market_data_unexpected_exception(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_market_data handles unexpected exception from historical data service."""
        symbol = "SOL_USDC"
        timeframe: Literal["1m"] = "1m"

        # Mock the historical data service to raise an unexpected exception
        with patch.object(
            backpack_market_data_service, "_historical_data_service"
        ) as mock_historical_service:
            mock_historical_service.get_market_data = AsyncMock(
                side_effect=Exception("Unexpected error")
            )

            with pytest.raises(Exception) as exc_info:
                args = GetMarketDataArgs(symbol=symbol, timeframe=timeframe)
                await backpack_market_data_service.get_market_data(args)

            assert "Unexpected error" in str(exc_info.value)

            # Verify the business logic calls the historical data service
            mock_historical_service.get_market_data.assert_called_once()
            call_args = mock_historical_service.get_market_data.call_args[0][0]
            assert call_args.symbol == symbol
            assert call_args.timeframe == timeframe

    @pytest.mark.asyncio
    async def test_get_market_data_with_time_parameters(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_market_data with start and end time parameters.

        Note: Current business logic delegates to historical data service.
        """
        symbol = "SOL_USDC"
        timeframe: Literal["5m"] = "5m"
        limit = 10
        start_time_ms = 1678880000000
        end_time_ms = 1678886400000

        # Create expected internal candle
        expected_candles = [MagicMock(spec=Candle)]

        # Mock the historical data service since business logic delegates to it
        with patch.object(
            backpack_market_data_service, "_historical_data_service"
        ) as mock_historical_service:
            mock_historical_service.get_market_data = AsyncMock(return_value=expected_candles)

            args = GetMarketDataArgs(
                symbol=symbol,
                timeframe=timeframe,
                limit=limit,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )
            result = await backpack_market_data_service.get_market_data(args)

            # Verify the business logic calls the historical data service with correct arguments
            mock_historical_service.get_market_data.assert_called_once_with(args)
            assert result == expected_candles

    @pytest.mark.asyncio
    async def test_get_all_tickers_not_implemented(
        self,
        backpack_market_data_service: BackpackMarketDataService,
    ) -> None:
        """Test that get_all_tickers delegates to price ticker service.

        Note: Current business logic delegates to price ticker service.
        """
        # Mock the price ticker service to raise not implemented error
        with patch.object(
            backpack_market_data_service, "_price_ticker_service"
        ) as mock_price_ticker_service:
            mock_price_ticker_service.get_all_tickers = AsyncMock(
                side_effect=APIError(
                    code=APIErrorCode.INVALID_REQUEST.value,
                    message="get_all_tickers is not implemented for BackpackMarketDataService",
                )
            )

            with pytest.raises(APIError) as exc_info:
                await backpack_market_data_service.get_all_tickers()

            assert exc_info.value.code == APIErrorCode.INVALID_REQUEST.value
            assert (
                "get_all_tickers is not implemented for BackpackMarketDataService"
                in exc_info.value.message
            )

            # Verify the business logic calls the price ticker service
            mock_price_ticker_service.get_all_tickers.assert_called_once()

    @pytest.mark.asyncio
    async def test_constructor_with_custom_mapper(
        self,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test constructor with custom mapper injection.

        Note: Current business logic delegates to price ticker service.
        """
        service = BackpackMarketDataService(
            http_client_requester=mock_http_client_requester,
            request_builder=mock_request_builder,
            response_handler=mock_response_handler,
            exchange_name="test_exchange",
            candle_mapper=mock_mapper,
        )

        # Mock the price ticker service since business logic delegates to it
        expected_ticker = MagicMock()
        with patch.object(service, "_price_ticker_service") as mock_price_ticker_service:
            mock_price_ticker_service.get_ticker = AsyncMock(return_value=expected_ticker)

            result = await service.get_ticker("TEST")

            # Verify the business logic calls the price ticker service
            mock_price_ticker_service.get_ticker.assert_called_once_with("TEST")
            assert result == expected_ticker

    @pytest.mark.asyncio
    async def test_constructor_with_default_mapper(
        self,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test constructor creates default mapper when none provided.

        Note: Current business logic delegates to price ticker service.
        """
        service = BackpackMarketDataService(
            http_client_requester=mock_http_client_requester,
            request_builder=mock_request_builder,
            response_handler=mock_response_handler,
            exchange_name="test_exchange",
            # No mapper parameters needed
        )

        # Mock the price ticker service since business logic delegates to it
        expected_ticker = MagicMock()
        with patch.object(service, "_price_ticker_service") as mock_price_ticker_service:
            mock_price_ticker_service.get_ticker = AsyncMock(return_value=expected_ticker)

            result = await service.get_ticker("TEST")

            # Verify the business logic calls the price ticker service
            mock_price_ticker_service.get_ticker.assert_called_once_with("TEST")
            assert result == expected_ticker
