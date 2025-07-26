"""Unit tests for HyperliquidMarketDataService market data/candles functionality."""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import HyperliquidRawCandleSnapshot
from cyberdelta.apis.hyperliquid.services.hl_market_data_service import HyperliquidMarketDataService
from cyberdelta.apis.models.service_args.hyperliquid import HyperliquidGetCandleSnapshotArgs
from cyberdelta.apis.models.service_args.market_data import GetMarketDataArgs
from cyberdelta.core.models.market.candle import Candle


# Unit tests for HyperliquidMarketDataService (moved from mislabeled integration tests)
# These are unit tests because they mock all dependencies and test individual methods

# Import fixtures from the shared conftest
pytest_plugins = ["tests.unit.apis.hyperliquid.services.conftest_market_data"]


class TestHyperliquidMarketDataServiceCandles:
    """Tests for the HyperliquidMarketDataService market data/candles functionality."""

    @pytest.mark.asyncio
    async def test_get_market_data_success(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
        mock_hl_mapper: MagicMock,
    ) -> None:
        """Test get_market_data (candlesticks) successfully retrieves and processes data."""
        symbol = "ETH"
        interval = "1m"
        start_time_ms = 1672531200000  # Example: 2023-01-01 00:00:00 UTC
        end_time_ms = 1672534800000  # Example: 2023-01-01 01:00:00 UTC

        # Mock raw response content matching HyperliquidRawCandleSnapshot structure
        raw_times = [start_time_ms, start_time_ms + 60000]
        raw_opens = ["3000", "3002"]
        raw_highs = ["3005", "3010"]
        raw_lows = ["2995", "3000"]
        raw_closes = ["3002", "3008"]
        raw_volumes = ["100", "120"]
        raw_status = "ok"

        mock_raw_candle_data: dict[str, Any] = {
            "t": raw_times,
            "o": raw_opens,
            "h": raw_highs,
            "l": raw_lows,
            "c": raw_closes,
            "v": raw_volumes,
            "s": raw_status,
        }
        # Correct instantiation of HyperliquidRawCandleSnapshot
        # Note: These are not used in the test but represent expected structure

        # Create a mock headers object to use consistently
        mock_headers = MagicMock()

        # Configure HTTP mocks to return valid candle data
        mock_http_client_requester.return_value = (mock_raw_candle_data, 200, mock_headers)

        # Configure request builder mock
        mock_payload_model = MagicMock()
        mock_payload_dict = {
            "type": "candleSnapshot",
            "req": {
                "coin": symbol,
                "interval": interval,
                "startTime": start_time_ms,
                "endTime": end_time_ms,
            },
        }
        mock_payload_model.model_dump.return_value = mock_payload_dict
        mock_hl_request_builder.build_candle_snapshot_payload.return_value = mock_payload_model

        # Create mock raw candle snapshot that would be returned by response handler
        mock_raw_candle_snapshot = HyperliquidRawCandleSnapshot(
            t=raw_times,
            o=raw_opens,
            h=raw_highs,
            l=raw_lows,
            c=raw_closes,
            v=raw_volumes,
            s=raw_status,
        )

        # Configure response handler to return the raw candle snapshot
        mock_hl_response_handler.handle_info_candle_snapshot_response.return_value = (
            mock_raw_candle_snapshot
        )

        # Create expected candle objects
        expected_candles = [
            Candle(
                symbol=symbol,
                interval=interval,
                open_time=datetime.fromtimestamp(start_time_ms / 1000, tz=UTC),
                open=Decimal(3000),
                high=Decimal(3005),
                low=Decimal(2995),
                close=Decimal(3002),
                volume=Decimal(100),
            ),
            Candle(
                symbol=symbol,
                interval=interval,
                open_time=datetime.fromtimestamp((start_time_ms + 60000) / 1000, tz=UTC),
                open=Decimal(3002),
                high=Decimal(3010),
                low=Decimal(3000),
                close=Decimal(3008),
                volume=Decimal(120),
            ),
        ]

        # Configure the historical data mapper (via the service fixture) to return expected candles
        # The service uses the mock_hl_mapper for historical_data_mapper
        # Access the mapper through the fixture which is the same instance used by the service
        mock_hl_mapper.transform_raw_candle_snapshot_to_candles.return_value = expected_candles

        # Use GetMarketDataArgs instead of individual parameters
        args = GetMarketDataArgs(
            symbol=symbol,
            timeframe=interval,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )
        result_candles = await hyperliquid_market_data_service.get_market_data(args)

        # Verify the HTTP request was made
        mock_http_client_requester.assert_called_once()

        # Verify result structure and content
        assert isinstance(result_candles, list)
        assert len(result_candles) == 2

        # Verify candles are properly returned
        assert all(isinstance(candle, Candle) for candle in result_candles)
        assert result_candles[0].symbol == symbol
        assert result_candles[0].interval == interval
        assert result_candles[0].open == Decimal(3000)
        assert result_candles[1].open == Decimal(3002)

        # Verify response handler was called with correct parameters
        mock_hl_response_handler.handle_info_candle_snapshot_response.assert_called_once_with(
            mock_raw_candle_data,
            symbol,
            interval,
            200,
            mock_headers,
        )

        # Verify mapper was called with the raw candle snapshot
        mock_hl_mapper.transform_raw_candle_snapshot_to_candles.assert_called_once_with(
            mock_raw_candle_snapshot,
            symbol,
            interval,
        )

    @pytest.mark.asyncio
    async def test_get_market_data_http_client_returns_none(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
    ) -> None:
        """Test get_market_data when HTTP client returns None content."""
        symbol = "ETH"
        interval = "1h"
        start_time_ms = 1678886400000
        end_time_ms = 1678890000000

        # Configure HTTP mocks to return None content
        mock_http_client_requester.return_value = (None, 200, {})

        # Configure request builder mock
        mock_payload_model = MagicMock()
        mock_payload_dict = {
            "type": "candleSnapshot",
            "req": {
                "coin": symbol,
                "interval": interval,
                "startTime": start_time_ms,
                "endTime": end_time_ms,
            },
        }
        mock_payload_model.model_dump.return_value = mock_payload_dict
        mock_hl_request_builder.build_candle_snapshot_payload.return_value = mock_payload_model

        with pytest.raises(APIError) as exc_info:
            # Use GetMarketDataArgs instead of individual parameters
            args = GetMarketDataArgs(
                symbol=symbol,
                timeframe=interval,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )
            await hyperliquid_market_data_service.get_market_data(args)

        # Verify the HTTP request was made
        mock_http_client_requester.assert_called_once()
        assert exc_info.value.message is not None

    @pytest.mark.asyncio
    async def test_get_market_data_timeout_error_propagation(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
    ) -> None:
        """Test get_market_data propagates TIMEOUT error correctly."""
        symbol = "BTC"
        interval = "5m"
        start_time_ms = 1672531200000
        end_time_ms = 1672534800000

        # Setup basic mocks
        mock_payload_model = MagicMock()
        mock_payload_dict = {
            "type": "candleSnapshot",
            "req": {
                "coin": symbol,
                "interval": interval,
                "startTime": start_time_ms,
                "endTime": end_time_ms,
            },
        }
        mock_payload_model.model_dump.return_value = mock_payload_dict
        mock_hl_request_builder.build_candle_snapshot_payload.return_value = mock_payload_model

        # Mock HTTP client to raise TIMEOUT APIError
        mock_http_client_requester.side_effect = APIError(
            message="Request timeout while fetching market data",
            code=APIErrorCode.TIMEOUT.value,
            http_status=408,
        )

        with pytest.raises(APIError) as exc_info:
            # Use GetMarketDataArgs instead of individual parameters
            args = GetMarketDataArgs(
                symbol=symbol,
                timeframe=interval,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )
            await hyperliquid_market_data_service.get_market_data(args)

        assert exc_info.value.code == APIErrorCode.TIMEOUT.value
        assert exc_info.value.http_status == 408
        assert "Request timeout while fetching market data" in exc_info.value.message

        mock_hl_request_builder.build_candle_snapshot_payload.assert_called_once_with(
            HyperliquidGetCandleSnapshotArgs(
                symbol=symbol,
                timeframe=interval,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            ),
        )
        mock_hl_response_handler.handle_info_candle_snapshot_response.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_market_data_request_builder_key_error(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
    ) -> None:
        """Test get_market_data handles KeyError from request builder."""
        symbol = "ETH"
        interval = "1d"
        start_time_ms = 1672531200000
        end_time_ms = 1672617600000

        # Mock request builder to raise KeyError
        mock_hl_request_builder.build_candle_snapshot_payload.side_effect = KeyError(
            "Invalid interval or missing required field",
        )

        with pytest.raises(APIError) as exc_info:
            # Use GetMarketDataArgs instead of individual parameters
            args = GetMarketDataArgs(
                symbol=symbol,
                timeframe=interval,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )
            await hyperliquid_market_data_service.get_market_data(args)

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert (
            f"Failed to build candle snapshot request for symbol {symbol}" in exc_info.value.message
        )
        assert isinstance(exc_info.value.__cause__, KeyError)

        mock_hl_request_builder.build_candle_snapshot_payload.assert_called_once_with(
            HyperliquidGetCandleSnapshotArgs(
                symbol=symbol,
                timeframe=interval,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            ),
        )
        mock_http_client_requester.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_market_data_with_invalid_time_range(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_hl_request_builder: MagicMock,
    ) -> None:
        """Test get_market_data with invalid time range (end < start)."""
        symbol = "BTC"
        interval = "1m"
        start_time_ms = 1672534800000  # Later time
        end_time_ms = 1672531200000  # Earlier time

        # Service should validate time range and raise ValueError directly
        with pytest.raises(ValueError) as exc_info:
            # Use GetMarketDataArgs instead of individual parameters
            args = GetMarketDataArgs(
                symbol=symbol,
                timeframe=interval,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )
            await hyperliquid_market_data_service.get_market_data(args)

        assert "start_time_ms must be before end_time_ms" in str(exc_info.value)
        # Request builder should not be called due to early validation
        mock_hl_request_builder.build_candle_snapshot_payload.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_market_data_response_handler_validation_error(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
    ) -> None:
        """Test get_market_data handles response handler validation errors."""
        symbol = "ETH"
        interval = "4h"
        start_time_ms = 1672531200000
        end_time_ms = 1672534800000

        # Setup mocks
        mock_payload_model = MagicMock()
        mock_payload_dict = {
            "type": "candleSnapshot",
            "req": {
                "coin": symbol,
                "interval": interval,
                "startTime": start_time_ms,
                "endTime": end_time_ms,
            },
        }
        mock_payload_model.model_dump.return_value = mock_payload_dict
        mock_hl_request_builder.build_candle_snapshot_payload.return_value = mock_payload_model

        # Mock malformed candle response
        mock_malformed_response = {
            "t": "invalid_time_format",  # Should be list of integers
            "o": ["3000"],
            "h": ["3010"],
            "l": ["2990"],
            "c": ["3005"],
            "v": ["100"],
            "s": "ok",
        }
        mock_http_client_requester.return_value = (mock_malformed_response, 200, {})

        # Mock response handler to raise APIError
        mock_hl_response_handler.handle_info_candle_snapshot_response.side_effect = APIError(
            message="Invalid candle snapshot response structure",
            code=APIErrorCode.INVALID_RESPONSE.value,
            http_status=200,
        )

        with pytest.raises(APIError) as exc_info:
            # Use GetMarketDataArgs instead of individual parameters
            args = GetMarketDataArgs(
                symbol=symbol,
                timeframe=interval,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )
            await hyperliquid_market_data_service.get_market_data(args)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Invalid candle snapshot response structure" in exc_info.value.message
        mock_hl_response_handler.handle_info_candle_snapshot_response.assert_called_once_with(
            mock_malformed_response,
            symbol,
            interval,
            200,
            {},
        )

    @pytest.mark.asyncio
    async def test_get_market_data_mapper_error(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
        mock_hl_mapper: MagicMock,
    ) -> None:
        """Test get_market_data handles mapper errors gracefully."""
        symbol = "BTC"
        interval = "15m"
        start_time_ms = 1672531200000
        end_time_ms = 1672534800000

        # Configure HTTP mocks to return valid response data
        mock_valid_response = {
            "t": [start_time_ms],
            "o": ["3000"],
            "h": ["3010"],
            "l": ["2990"],
            "c": ["3005"],
            "v": ["100"],
            "s": "ok",
        }
        mock_http_client_requester.return_value = (mock_valid_response, 200, {})

        # Configure request builder mock
        mock_payload_model = MagicMock()
        mock_payload_dict = {
            "type": "candleSnapshot",
            "req": {
                "coin": symbol,
                "interval": interval,
                "startTime": start_time_ms,
                "endTime": end_time_ms,
            },
        }
        mock_payload_model.model_dump.return_value = mock_payload_dict
        mock_hl_request_builder.build_candle_snapshot_payload.return_value = mock_payload_model

        # Create valid raw candle snapshot that would be returned by response handler
        mock_raw_candle_snapshot = HyperliquidRawCandleSnapshot(
            t=[start_time_ms],
            o=["3000"],
            h=["3010"],
            l=["2990"],
            c=["3005"],
            v=["100"],
            s="ok",
        )

        # Configure response handler to return valid raw data
        mock_hl_response_handler.handle_info_candle_snapshot_response.return_value = (
            mock_raw_candle_snapshot
        )

        # Configure mapper to raise a TransformationError (simulating mapping failure)
        mock_hl_mapper.transform_raw_candle_snapshot_to_candles.side_effect = TransformationError(
            message="Failed to transform candle data: invalid decimal value",
            original_exception=ValueError("Invalid decimal format"),
        )

        with pytest.raises(APIError) as exc_info:
            # Use GetMarketDataArgs instead of individual parameters
            args = GetMarketDataArgs(
                symbol=symbol,
                timeframe=interval,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )
            await hyperliquid_market_data_service.get_market_data(args)

        # Verify the error propagation
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Failed to process/transform exchange data" in exc_info.value.message

        # Verify the HTTP request was made
        mock_http_client_requester.assert_called_once()

        # Verify response handler was called with correct parameters
        mock_hl_response_handler.handle_info_candle_snapshot_response.assert_called_once_with(
            mock_valid_response,
            symbol,
            interval,
            200,
            {},
        )

        # Verify mapper was called with the raw candle snapshot
        mock_hl_mapper.transform_raw_candle_snapshot_to_candles.assert_called_once_with(
            mock_raw_candle_snapshot,
            symbol,
            interval,
        )

    @pytest.mark.asyncio
    async def test_get_market_data_empty_successful_response(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
        mock_hl_mapper: MagicMock,
    ) -> None:
        """Test get_market_data handles empty but successful response correctly."""
        symbol = "ETH"
        interval = "1w"
        start_time_ms = 1672531200000
        end_time_ms = 1672534800000

        # Configure HTTP mocks to return empty but valid response
        mock_empty_response: dict[str, Any] = {
            "t": [],
            "o": [],
            "h": [],
            "l": [],
            "c": [],
            "v": [],
            "s": "ok",
        }
        mock_http_client_requester.return_value = (mock_empty_response, 200, {})

        # Configure request builder mock
        mock_payload_model = MagicMock()
        mock_payload_dict = {
            "type": "candleSnapshot",
            "req": {
                "coin": symbol,
                "interval": interval,
                "startTime": start_time_ms,
                "endTime": end_time_ms,
            },
        }
        mock_payload_model.model_dump.return_value = mock_payload_dict
        mock_hl_request_builder.build_candle_snapshot_payload.return_value = mock_payload_model

        # Create empty raw candle snapshot that would be returned by response handler
        mock_empty_raw_candle_snapshot = HyperliquidRawCandleSnapshot(
            t=[],
            o=[],
            h=[],
            l=[],
            c=[],
            v=[],
            s="ok",
        )

        # Configure response handler to return the empty raw candle snapshot
        mock_hl_response_handler.handle_info_candle_snapshot_response.return_value = (
            mock_empty_raw_candle_snapshot
        )

        # Configure mapper to return empty list for empty snapshot (business logic)
        mock_hl_mapper.transform_raw_candle_snapshot_to_candles.return_value = []

        # Use GetMarketDataArgs instead of individual parameters
        args = GetMarketDataArgs(
            symbol=symbol,
            timeframe=interval,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )
        result = await hyperliquid_market_data_service.get_market_data(args)

        # Verify the HTTP request was made
        mock_http_client_requester.assert_called_once()

        # Verify result structure
        assert isinstance(result, list)
        assert len(result) == 0  # Should be exactly 0 for empty response

        # Verify response handler was called with correct parameters
        mock_hl_response_handler.handle_info_candle_snapshot_response.assert_called_once_with(
            mock_empty_response,
            symbol,
            interval,
            200,
            {},
        )

        # Verify mapper was called with the empty raw candle snapshot
        mock_hl_mapper.transform_raw_candle_snapshot_to_candles.assert_called_once_with(
            mock_empty_raw_candle_snapshot,
            symbol,
            interval,
        )

    @pytest.mark.asyncio
    async def test_get_market_data_server_error_propagation(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
    ) -> None:
        """Test get_market_data propagates SERVER_ERROR correctly."""
        symbol = "BTC"
        interval = "1h"
        start_time_ms = 1672531200000
        end_time_ms = 1672534800000

        # Setup mocks
        mock_payload_model = MagicMock()
        mock_payload_dict = {"type": "candleSnapshot", "req": {"coin": symbol}}
        mock_payload_model.model_dump.return_value = mock_payload_dict
        mock_hl_request_builder.build_candle_snapshot_payload.return_value = mock_payload_model

        # Mock HTTP response with server error status
        mock_error_response = {"error": "Internal server error"}
        mock_http_client_requester.return_value = (mock_error_response, 500, {})

        # Mock response handler to raise SERVER_ERROR APIError
        mock_hl_response_handler.handle_info_candle_snapshot_response.side_effect = APIError(
            message="Server error while fetching candle data",
            code=APIErrorCode.SERVER_ERROR.value,
            http_status=500,
            exchange_message="Internal server error",
        )

        with pytest.raises(APIError) as exc_info:
            # Use GetMarketDataArgs instead of individual parameters
            args = GetMarketDataArgs(
                symbol=symbol,
                timeframe=interval,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )
            await hyperliquid_market_data_service.get_market_data(args)

        assert exc_info.value.code == APIErrorCode.SERVER_ERROR.value
        assert exc_info.value.http_status == 500
        assert "Server error while fetching candle data" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_market_data_with_various_intervals(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
        mock_hl_mapper: MagicMock,
    ) -> None:
        """Test get_market_data works with various interval types."""
        symbol = "ETH"
        intervals = ["1m", "5m", "15m", "1h", "4h", "1d", "1w"]
        start_time_ms = 1672531200000
        end_time_ms = 1672534800000

        for interval in intervals:
            # Reset mocks for each iteration
            mock_http_client_requester.reset_mock()
            mock_hl_request_builder.reset_mock()
            mock_hl_response_handler.reset_mock()
            mock_hl_mapper.reset_mock()

            # Configure HTTP mocks to return valid candle data for this interval
            mock_interval_response = {
                "t": [start_time_ms],
                "o": ["3000"],
                "h": ["3010"],
                "l": ["2990"],
                "c": ["3005"],
                "v": ["100"],
                "s": "ok",
            }
            mock_http_client_requester.return_value = (mock_interval_response, 200, {})

            # Configure request builder mock
            mock_payload_model = MagicMock()
            mock_payload_dict = {
                "type": "candleSnapshot",
                "req": {
                    "coin": symbol,
                    "interval": interval,
                    "startTime": start_time_ms,
                    "endTime": end_time_ms,
                },
            }
            mock_payload_model.model_dump.return_value = mock_payload_dict
            mock_hl_request_builder.build_candle_snapshot_payload.return_value = mock_payload_model

            # Create mock raw candle snapshot that would be returned by response handler
            mock_raw_candle_snapshot = HyperliquidRawCandleSnapshot(
                t=[start_time_ms],
                o=["3000"],
                h=["3010"],
                l=["2990"],
                c=["3005"],
                v=["100"],
                s="ok",
            )

            # Configure response handler to return the raw candle snapshot
            mock_hl_response_handler.handle_info_candle_snapshot_response.return_value = (
                mock_raw_candle_snapshot
            )

            # Create expected candle for this interval
            expected_candle = Candle(
                symbol=symbol,
                interval=interval,
                open_time=datetime.fromtimestamp(start_time_ms / 1000, tz=UTC),
                open=Decimal(3000),
                high=Decimal(3010),
                low=Decimal(2990),
                close=Decimal(3005),
                volume=Decimal(100),
            )

            # Configure mapper to return expected candle
            mock_hl_mapper.transform_raw_candle_snapshot_to_candles.return_value = [expected_candle]

            # Use GetMarketDataArgs instead of individual parameters
            args = GetMarketDataArgs(
                symbol=symbol,
                timeframe=interval,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )
            result = await hyperliquid_market_data_service.get_market_data(args)

            # Verify the HTTP request was made
            mock_http_client_requester.assert_called_once()

            # Verify result structure
            assert isinstance(result, list)
            assert len(result) == 1
            assert isinstance(result[0], Candle)
            assert result[0].symbol == symbol
            assert result[0].interval == interval
            assert result[0].open == Decimal(3000)

            # Verify request builder was called with correct parameters
            mock_hl_request_builder.build_candle_snapshot_payload.assert_called_once_with(
                HyperliquidGetCandleSnapshotArgs(
                    symbol=symbol,
                    timeframe=interval,
                    start_time_ms=start_time_ms,
                    end_time_ms=end_time_ms,
                ),
            )

            # Verify response handler was called with correct parameters
            mock_hl_response_handler.handle_info_candle_snapshot_response.assert_called_once_with(
                mock_interval_response,
                symbol,
                interval,
                200,
                {},
            )

            # Verify mapper was called with the raw candle snapshot
            mock_hl_mapper.transform_raw_candle_snapshot_to_candles.assert_called_once_with(
                mock_raw_candle_snapshot,
                symbol,
                interval,
            )
