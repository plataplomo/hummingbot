"""Unit tests for HyperliquidMarketDataService market data/candles functionality."""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cyberdelta.apis.hyperliquid.models.hl_raw_candles import (
    HyperliquidRawCandleSnapshot,
    HyperliquidRawCandleSnapshotRequestPayload,
)
from cyberdelta.apis.hyperliquid.services.hl_market_data_service import HyperliquidMarketDataService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.service_args_models import GetMarketDataArgs
from cyberdelta.core.models.market import Candle

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
        mock_validated_response = HyperliquidRawCandleSnapshot(
            t=raw_times,
            o=raw_opens,
            h=raw_highs,
            l=raw_lows,
            c=raw_closes,
            v=raw_volumes,
            s=raw_status,
        )

        expected_candles = [
            Candle(  # Use open_time for internal Candle model
                open_time=datetime.fromtimestamp(start_time_ms / 1000, tz=UTC),
                open=Decimal("3000"),
                high=Decimal("3005"),
                low=Decimal("2995"),
                close=Decimal("3002"),
                volume=Decimal("100"),
                symbol=symbol,
                interval=interval,
            ),
            Candle(
                open_time=datetime.fromtimestamp((start_time_ms + 60000) / 1000, tz=UTC),
                open=Decimal("3002"),
                high=Decimal("3010"),
                low=Decimal("3000"),
                close=Decimal("3008"),
                volume=Decimal("120"),
                symbol=symbol,
                interval=interval,
            ),
        ]

        # Create a mock headers object to use consistently
        mock_headers = MagicMock()

        # Patch the _mapper attribute on the service instance
        with patch.object(hyperliquid_market_data_service, "_mapper") as mock_mapper_instance:
            mock_hl_request_builder.build_candle_snapshot_payload.return_value = MagicMock(
                spec=HyperliquidRawCandleSnapshotRequestPayload,
                model_dump=MagicMock(
                    return_value={
                        "type": "candleSnapshot",
                        "req": {
                            "coin": symbol,
                            "interval": interval,
                            "startTime": start_time_ms,
                            "endTime": end_time_ms,
                        },
                    },
                ),
            )
            mock_http_client_requester.return_value = (
                mock_raw_candle_data,
                200,
                mock_headers,
            )
            mock_hl_response_handler.handle_info_candle_snapshot_response.return_value = (
                mock_validated_response
            )
            mock_mapper_instance.transform_raw_candle_snapshot_to_candles.return_value = (
                expected_candles
            )

            # Use GetMarketDataArgs instead of individual parameters
            args = GetMarketDataArgs(
                symbol=symbol,
                timeframe=interval,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )
            result_candles = await hyperliquid_market_data_service.get_market_data(args)

            mock_hl_request_builder.build_candle_snapshot_payload.assert_called_once_with(
                symbol=symbol,
                timeframe=interval,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )
            mock_hl_response_handler.handle_info_candle_snapshot_response.assert_called_once_with(
                mock_raw_candle_data,
                symbol,
                interval,
                200,
                mock_headers,  # Use the same mock headers object
            )
            mock_mapper_instance.transform_raw_candle_snapshot_to_candles.assert_called_once_with(
                mock_validated_response,
                symbol,
                interval,  # Positional arguments
            )
            assert result_candles == expected_candles

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

        # Use a proper mock for the request payload model
        mock_payload_model = MagicMock()
        mock_request_payload_dict = {
            "type": "candleSnapshot",
            "req": {
                "coin": symbol,
                "interval": interval,
                "startTime": start_time_ms,
                "endTime": end_time_ms,
            },
        }
        mock_payload_model.model_dump.return_value = mock_request_payload_dict
        mock_hl_request_builder.build_candle_snapshot_payload.return_value = mock_payload_model

        mock_http_client_requester.return_value = (None, 200, MagicMock())

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
        assert (
            f"No data received for market data (candles) for {symbol}, status: 200"
            in exc_info.value.message
        )
        mock_hl_request_builder.build_candle_snapshot_payload.assert_called_once_with(
            symbol=symbol,
            timeframe=interval,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )
        mock_hl_response_handler.handle_info_candle_snapshot_response.assert_not_called()
        # Mapper should not be called since HTTP client returned None

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
            symbol=symbol,
            timeframe=interval,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
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
            symbol=symbol,
            timeframe=interval,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
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
            mock_malformed_response, symbol, interval, 200, {},
        )

    @pytest.mark.asyncio
    async def test_get_market_data_mapper_error(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
    ) -> None:
        """Test get_market_data handles mapper errors gracefully."""
        symbol = "BTC"
        interval = "15m"
        start_time_ms = 1672531200000
        end_time_ms = 1672534800000

        # Setup successful HTTP and response handler mocks
        mock_payload_model = MagicMock()
        mock_payload_dict = {
            "type": "candleSnapshot",
            "req": {"coin": symbol, "interval": interval},
        }
        mock_payload_model.model_dump.return_value = mock_payload_dict
        mock_hl_request_builder.build_candle_snapshot_payload.return_value = mock_payload_model

        mock_raw_candle_data = {
            "t": [1672531200000],
            "o": ["3000"],
            "h": ["3010"],
            "l": ["2990"],
            "c": ["3005"],
            "v": ["100"],
            "s": "ok",
        }
        mock_http_client_requester.return_value = (mock_raw_candle_data, 200, {})

        mock_validated_candle_snapshot = HyperliquidRawCandleSnapshot(
            t=[1672531200000],
            o=["3000"],
            h=["3010"],
            l=["2990"],
            c=["3005"],
            v=["100"],
            s="ok",
        )
        mock_hl_response_handler.handle_info_candle_snapshot_response.return_value = (
            mock_validated_candle_snapshot
        )

        # Configure mapper to raise an error
        with patch.object(hyperliquid_market_data_service, "_mapper") as mock_mapper_instance:
            mock_mapper_instance.transform_raw_candle_snapshot_to_candles.side_effect = ValueError(
                "Mapper processing failed for candle data",
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
            assert "Service internal logic error." in exc_info.value.message
            assert isinstance(exc_info.value.__cause__, ValueError)
            assert "Mapper processing failed for candle data" in str(exc_info.value.__cause__)
            mock_mapper_instance.transform_raw_candle_snapshot_to_candles.assert_called_once_with(
                mock_validated_candle_snapshot, symbol, interval,
            )

    @pytest.mark.asyncio
    async def test_get_market_data_empty_successful_response(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
    ) -> None:
        """Test get_market_data handles empty but successful response correctly."""
        symbol = "ETH"
        interval = "1w"
        start_time_ms = 1672531200000
        end_time_ms = 1672534800000

        # Setup mocks for request building
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

        # Mock HTTP response with empty candle data (but successful)
        mock_empty_candle_response: dict[str, Any] = {
            "t": [],
            "o": [],
            "h": [],
            "l": [],
            "c": [],
            "v": [],
            "s": "no_data",
        }
        mock_http_client_requester.return_value = (mock_empty_candle_response, 200, {})

        # Mock response handler to return empty validated candle snapshot
        mock_empty_candle_snapshot = HyperliquidRawCandleSnapshot(
            t=[], o=[], h=[], l=[], c=[], v=[], s="no_data",
        )
        mock_hl_response_handler.handle_info_candle_snapshot_response.return_value = (
            mock_empty_candle_snapshot
        )

        # Mock mapper to return empty list
        with patch.object(hyperliquid_market_data_service, "_mapper") as mock_mapper_instance:
            mock_mapper_instance.transform_raw_candle_snapshot_to_candles.return_value = []

            # Use GetMarketDataArgs instead of individual parameters
            args = GetMarketDataArgs(
                symbol=symbol,
                timeframe=interval,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )
            result = await hyperliquid_market_data_service.get_market_data(args)

            assert result == []
            mock_hl_response_handler.handle_info_candle_snapshot_response.assert_called_once_with(
                mock_empty_candle_response, symbol, interval, 200, {},
            )
            mock_mapper_instance.transform_raw_candle_snapshot_to_candles.assert_called_once_with(
                mock_empty_candle_snapshot, symbol, interval,
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
    ) -> None:
        """Test get_market_data works with various interval types."""
        symbol = "ETH"
        intervals = ["1m", "5m", "15m", "1h", "4h", "1d", "1w"]
        start_time_ms = 1672531200000
        end_time_ms = 1672534800000

        for interval in intervals:
            # Reset mocks for each iteration
            mock_hl_request_builder.reset_mock()
            mock_http_client_requester.reset_mock()
            mock_hl_response_handler.reset_mock()

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

            # Mock successful response
            mock_candle_data = {
                "t": [start_time_ms],
                "o": ["3000"],
                "h": ["3010"],
                "l": ["2990"],
                "c": ["3005"],
                "v": ["100"],
                "s": "ok",
            }
            mock_http_client_requester.return_value = (mock_candle_data, 200, {})

            mock_candle_snapshot = HyperliquidRawCandleSnapshot(
                t=[start_time_ms],
                o=["3000"],
                h=["3010"],
                l=["2990"],
                c=["3005"],
                v=["100"],
                s="ok",
            )
            mock_hl_response_handler.handle_info_candle_snapshot_response.return_value = (
                mock_candle_snapshot
            )

            # Mock mapper
            expected_candle = Candle(
                open_time=datetime.fromtimestamp(start_time_ms / 1000, tz=UTC),
                open=Decimal("3000"),
                high=Decimal("3010"),
                low=Decimal("2990"),
                close=Decimal("3005"),
                volume=Decimal("100"),
                symbol=symbol,
                interval=interval,
            )

            with patch.object(hyperliquid_market_data_service, "_mapper") as mock_mapper_instance:
                mock_mapper_instance.transform_raw_candle_snapshot_to_candles.return_value = [
                    expected_candle,
                ]

                # Use GetMarketDataArgs instead of individual parameters
                args = GetMarketDataArgs(
                    symbol=symbol,
                    timeframe=interval,
                    start_time_ms=start_time_ms,
                    end_time_ms=end_time_ms,
                )
                result = await hyperliquid_market_data_service.get_market_data(args)

                assert len(result) == 1
                assert result[0].interval == interval
                mock_hl_request_builder.build_candle_snapshot_payload.assert_called_once_with(
                    symbol=symbol,
                    timeframe=interval,
                    start_time_ms=start_time_ms,
                    end_time_ms=end_time_ms,
                )
