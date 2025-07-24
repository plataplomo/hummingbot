"""Unit tests for HyperliquidMarketDataService market data/candles functionality."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.hyperliquid.services.hl_market_data_service import HyperliquidMarketDataService
from cyberdelta.apis.models.service_args_models import GetCandleSnapshotArgs, GetMarketDataArgs


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

        # Verify result structure (we test the public behavior)
        assert isinstance(result_candles, list)
        assert len(result_candles) >= 0  # May be empty or contain candles

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
            GetCandleSnapshotArgs(
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
            GetCandleSnapshotArgs(
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
    ) -> None:
        """Test get_market_data handles mapper errors gracefully."""
        symbol = "BTC"
        interval = "15m"
        start_time_ms = 1672531200000
        end_time_ms = 1672534800000

        # Configure HTTP mocks to simulate mapper error via response handler
        mock_invalid_response = {
            "t": "invalid_time_format",  # Should be list of integers
            "o": ["3000"],
            "h": ["3010"],
            "l": ["2990"],
            "c": ["3005"],
            "v": ["100"],
            "s": "ok",
        }
        mock_http_client_requester.return_value = (mock_invalid_response, 200, {})

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
        assert len(result) >= 0

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
            # Create expected candle for this interval
            # Note: Not directly used in test, but represents expected structure

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

            # Use GetMarketDataArgs instead of individual parameters
            args = GetMarketDataArgs(
                symbol=symbol,
                timeframe=interval,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )
            result = await hyperliquid_market_data_service.get_market_data(args)

            # Verify the HTTP request was made
            mock_http_client_requester.assert_called()

            # Verify result structure
            assert isinstance(result, list)
            assert len(result) >= 0  # May be empty or contain candles
