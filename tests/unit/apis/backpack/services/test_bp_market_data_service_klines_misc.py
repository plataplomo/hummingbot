"""Unit tests for BackpackMarketDataService klines and miscellaneous functionality."""

from __future__ import annotations

from typing import Literal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.mappers.bp_market_data_mapper import BackpackMarketDataMapper
from cyberdelta.apis.backpack.models.bp_raw_kline import BackpackRawKline
from cyberdelta.apis.backpack.models.bp_raw_query_params import (
    BackpackRawGetMarketDataParams,
    BackpackRawGetTickerParams,
)
from cyberdelta.apis.backpack.services.bp_market_data_service import BackpackMarketDataService
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.models.service_args_models import GetMarketDataArgs
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
        """Test get_market_data successfully retrieves and processes kline data."""
        symbol = "SOL_USDC"
        timeframe: Literal["1m"] = "1m"
        limit = 2

        # Mock data
        mock_raw_kline_data = [
            [
                1678886400,  # int
                "100.0",
                "101.0",
                "99.0",
                "100.5",
                "1000.0",
                1678886400,  # int
                "100000.0",
                50,  # int
                "60.0",
                "180000.0",
                "0",
            ],
            [
                1678886460,  # int
                "100.5",
                "101.5",
                "99.5",
                "101.0",
                "1200.0",
                1678886460,  # int
                "120000.0",
                60,  # int
                "70.0",
                "210000.0",
                "0",
            ],
        ]
        mock_validated_klines_raw = [
            BackpackRawKline.model_validate(kline) for kline in mock_raw_kline_data
        ]
        mock_headers_from_client = MagicMock()

        mock_request_builder.build_get_market_data_params.return_value = (
            BackpackRawGetMarketDataParams(
                symbol=symbol,
                interval=timeframe,
                limit=limit,
                startTime=None,
                endTime=None,
            )
        )

        mock_http_client_requester.return_value = (
            mock_raw_kline_data,  # Raw list of lists
            200,
            mock_headers_from_client,
        )
        mock_response_handler.handle_get_market_data_response.return_value = (
            mock_validated_klines_raw
        )

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            mock_internal_candles = [MagicMock(spec=Candle), MagicMock(spec=Candle)]
            mock_mapper.transform_raw_kline_to_internal.side_effect = mock_internal_candles

            args = GetMarketDataArgs(symbol=symbol, timeframe=timeframe, limit=limit)
            result = await backpack_market_data_service.get_market_data(args)

            mock_request_builder.build_get_market_data_params.assert_called_once_with(
                symbol=symbol,
                timeframe_str=timeframe,
                limit=limit,
                start_time_ms=None,
                end_time_ms=None,
            )
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint="/api/v1/klines",
                params={"symbol": symbol, "interval": timeframe, "limit": limit},
                is_signed=False,
                endpoint_group="public",
                request_weight=1,
            )
            mock_response_handler.handle_get_market_data_response.assert_called_once_with(
                mock_raw_kline_data,
                symbol,
                timeframe,
                200,
                mock_headers_from_client,
            )
            assert mock_mapper.transform_raw_kline_to_internal.call_count == len(
                mock_validated_klines_raw,
            )
            assert result == mock_internal_candles

    @pytest.mark.asyncio
    async def test_get_market_data_http_client_returns_none(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_market_data when HTTP client returns None content."""
        symbol = "SOL_USDC"
        timeframe: Literal["1h"] = "1h"
        limit = 100

        # Mock data

        mock_request_builder.build_get_market_data_params.return_value = (
            BackpackRawGetMarketDataParams(
                symbol=symbol,
                interval=timeframe,
                limit=limit,
                startTime=None,
                endTime=None,
            )
        )
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            with pytest.raises(APIError) as exc_info:
                args = GetMarketDataArgs(symbol=symbol, timeframe=timeframe, limit=limit)
                await backpack_market_data_service.get_market_data(args)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            expected_error_msg = f"No data received for klines ({symbol}@{timeframe}), status: 200"
            assert exc_info.value.message == expected_error_msg

            mock_request_builder.build_get_market_data_params.assert_called_once_with(
                symbol=symbol,
                timeframe_str=timeframe,
                limit=limit,
                start_time_ms=None,
                end_time_ms=None,
            )
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint="/api/v1/klines",
                params={"symbol": symbol, "interval": timeframe, "limit": limit},
                is_signed=False,
                endpoint_group="public",
                request_weight=1,
            )
            mock_response_handler.handle_get_market_data_response.assert_not_called()
            mock_mapper.transform_raw_kline_to_internal.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_market_data_validation_error(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_market_data handles validation error from response handler."""
        symbol = "SOL_USDC"
        timeframe: Literal["1m"] = "1m"
        mock_raw_response = [["invalid", "kline_data"]]

        mock_request_builder.build_get_market_data_params.return_value = (
            BackpackRawGetMarketDataParams(
                symbol=symbol,
                interval=timeframe,
                limit=100,
                startTime=None,
                endTime=None,
            )
        )
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})

        # Create a ValidationError by trying to validate invalid data
        try:
            BackpackRawKline.model_validate(["invalid", "data"])
        except ValidationError as e:
            mock_response_handler.handle_get_market_data_response.side_effect = e

        with pytest.raises(APIError) as exc_info:
            args = GetMarketDataArgs(symbol=symbol, timeframe=timeframe)
            await backpack_market_data_service.get_market_data(args)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Internal data validation failed." in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_market_data_unexpected_exception(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_market_data handles unexpected exception."""
        symbol = "SOL_USDC"
        timeframe: Literal["1m"] = "1m"
        mock_raw_response = [[1678886400, "100.0"]]

        mock_request_builder.build_get_market_data_params.return_value = (
            BackpackRawGetMarketDataParams(
                symbol=symbol,
                interval=timeframe,
                limit=100,
                startTime=None,
                endTime=None,
            )
        )
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_get_market_data_response.side_effect = Exception(
            "Unexpected error",
        )

        with pytest.raises(APIError) as exc_info:
            args = GetMarketDataArgs(symbol=symbol, timeframe=timeframe)
            await backpack_market_data_service.get_market_data(args)

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected service failure." in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_market_data_with_time_parameters(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_market_data with start and end time parameters."""
        symbol = "SOL_USDC"
        timeframe: Literal["5m"] = "5m"
        limit = 10
        start_time_ms = 1678880000000
        end_time_ms = 1678886400000

        mock_params = {
            "symbol": symbol,
            "interval": timeframe,
            "limit": limit,
            "startTime": start_time_ms,
            "endTime": end_time_ms,
        }
        mock_raw_kline_data = [
            [
                1678886400,
                "100.0",
                "101.0",
                "99.0",
                "100.5",
                "1000.0",
                1678886400,
                "100000.0",
                50,
                "60.0",
                "180000.0",
                "0",
            ],
        ]
        mock_validated_klines_raw = [BackpackRawKline.model_validate(mock_raw_kline_data[0])]

        mock_request_builder.build_get_market_data_params.return_value = (
            BackpackRawGetMarketDataParams(
                symbol=symbol,
                interval=timeframe,
                limit=limit,
                startTime=start_time_ms,
                endTime=end_time_ms,
            )
        )
        mock_http_client_requester.return_value = (mock_raw_kline_data, 200, {})
        mock_response_handler.handle_get_market_data_response.return_value = (
            mock_validated_klines_raw
        )

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            mock_internal_candles = [MagicMock(spec=Candle)]
            mock_mapper.transform_raw_kline_to_internal.side_effect = mock_internal_candles

            args = GetMarketDataArgs(
                symbol=symbol,
                timeframe=timeframe,
                limit=limit,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )
            result = await backpack_market_data_service.get_market_data(args)

            mock_request_builder.build_get_market_data_params.assert_called_once_with(
                symbol=symbol,
                timeframe_str=timeframe,
                limit=limit,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint="/api/v1/klines",
                params=mock_params,
                is_signed=False,
                endpoint_group="public",
                request_weight=1,
            )
            mock_response_handler.handle_get_market_data_response.assert_called_once_with(
                mock_raw_kline_data,
                symbol,
                timeframe,
                200,
                {},
            )
            assert mock_mapper.transform_raw_kline_to_internal.call_count == len(
                mock_validated_klines_raw,
            )
            assert result == mock_internal_candles

    @pytest.mark.asyncio
    async def test_get_all_tickers_not_implemented(
        self,
        backpack_market_data_service: BackpackMarketDataService,
    ) -> None:
        """Test that get_all_tickers raises APIError for not implemented functionality."""
        with pytest.raises(APIError) as exc_info:
            await backpack_market_data_service.get_all_tickers()

        assert exc_info.value.code == APIErrorCode.INVALID_REQUEST.value
        assert (
            "get_all_tickers is not implemented for BackpackMarketDataService"
            in exc_info.value.message
        )

    @pytest.mark.asyncio
    async def test_constructor_with_custom_mapper(
        self,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test constructor with custom mapper injection."""
        service = BackpackMarketDataService(
            http_client_requester=mock_http_client_requester,
            request_builder=mock_request_builder,
            response_handler=mock_response_handler,
            exchange_name="test_exchange",
            mapper=mock_mapper,
        )

        # Test behavior that uses the mapper to verify it was set correctly
        mock_request_builder.build_get_ticker_params.return_value = BackpackRawGetTickerParams(
            symbol="TEST",
        )
        mock_http_client_requester.return_value = ({"symbol": "TEST", "price": "100.0"}, 200, {})
        mock_response_handler.handle_get_ticker_response.return_value = MagicMock()

        # Configure the mock to have the method and set its return value
        mock_mapper.transform_raw_ticker_to_internal = MagicMock(return_value=MagicMock())

        await service.get_ticker("TEST")

        # Verify the mapper method was called
        mock_mapper.transform_raw_ticker_to_internal.assert_called_once()

    @pytest.mark.asyncio
    async def test_constructor_with_default_mapper(
        self,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test constructor creates default mapper when none provided."""
        service = BackpackMarketDataService(
            http_client_requester=mock_http_client_requester,
            request_builder=mock_request_builder,
            response_handler=mock_response_handler,
            exchange_name="test_exchange",
            mapper=None,
        )

        # Test behavior that uses the mapper to verify it's working
        mock_request_builder.build_get_ticker_params.return_value = BackpackRawGetTickerParams(
            symbol="TEST",
        )
        mock_http_client_requester.return_value = ({"symbol": "TEST", "price": "100.0"}, 200, {})
        mock_response_handler.handle_get_ticker_response.return_value = MagicMock()

        # Mock the static method on the class
        with patch.object(
            BackpackMarketDataMapper,
            "transform_raw_ticker_to_internal",
        ) as mock_transform:
            mock_transform.return_value = MagicMock()
            await service.get_ticker("TEST")

        # Verify the static method was called
        mock_transform.assert_called_once()
