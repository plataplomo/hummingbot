"""Unit tests for Hyperliquid Historical Data Service.

Tests cover all methods of the HyperliquidHistoricalDataService including:
- Current funding rates retrieval
- Historical funding rates with time filtering
- Historical candlestick/kline data
- Time validation and parameter processing
- Error handling and edge cases
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError

# TODO: Fix import - old module was refactored
from cyberdelta.apis.hyperliquid.hl_response_handler import HyperliquidResponseHandler
from cyberdelta.apis.hyperliquid.mappers.market_data.hl_historical_data_mapper import (
    HyperliquidHistoricalDataMapper,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import HyperliquidRawCandleSnapshot
from cyberdelta.apis.hyperliquid.models.hl_raw_funding_history_info import (
    HyperliquidRawFundingHistoryItem,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetCtx,
    HyperliquidRawAssetDefinition,
    HyperliquidRawMetaAndAssetCtxsResponse,
    HyperliquidRawMetaResponse,
)
from cyberdelta.apis.hyperliquid.request_builders.hl_market_data_request_builder import (
    HyperliquidMarketDataRequestBuilder,
)
from cyberdelta.apis.hyperliquid.services.market_data.hl_historical_data_service import (
    HyperliquidHistoricalDataService,
)
from cyberdelta.apis.models.service_args_models import (
    GetFundingRatesArgs,
    GetHistoricalFundingRatesArgs,
    GetMarketDataArgs,
)
from cyberdelta.core.models import FundingRate
from cyberdelta.core.models.market.candle import Candle


@pytest.fixture
def mock_http_requester() -> AsyncMock:
    """Create a mock HTTP requester."""
    return AsyncMock()


@pytest.fixture
def mock_request_builder() -> Mock:
    """Create a mock request builder."""
    return MagicMock(spec=HyperliquidMarketDataRequestBuilder)


@pytest.fixture
def mock_response_handler() -> Mock:
    """Create a mock response handler."""
    return MagicMock(spec=HyperliquidResponseHandler)


@pytest.fixture
def mock_mapper() -> Mock:
    """Create a mock data mapper."""
    return MagicMock(spec=HyperliquidHistoricalDataMapper)


@pytest.fixture
def historical_data_service(
    mock_http_requester: AsyncMock,
    mock_request_builder: Mock,
    mock_response_handler: Mock,
    mock_mapper: Mock,
) -> HyperliquidHistoricalDataService:
    """Create a historical data service instance with mocks."""
    return HyperliquidHistoricalDataService(
        http_client_requester=mock_http_requester,
        request_builder=mock_request_builder,
        response_handler=mock_response_handler,
        mapper=mock_mapper,
        exchange_name="hyperliquid",
    )


@pytest.fixture
def mock_asset_ctx() -> HyperliquidRawAssetCtx:
    """Create a mock asset context with funding rate."""
    return HyperliquidRawAssetCtx(
        funding="0.0001",
        markPx="3500.00",
        openInterest="1000000.00",
        oraclePx="3499.50",
        prevDayPx="3400.00",
        dayNtlVlm="50000000.00",
        dayBaseVlm="1000.00",
        premium="0.0005",
        name="BTC",
        midPx="3499.75",
        impactPx="3500.10",
        impactPxs=["3500.05", "3500.15"],
    )


@pytest.fixture
def mock_meta_and_asset_ctxs_response(
    mock_asset_ctx: HyperliquidRawAssetCtx,
) -> HyperliquidRawMetaAndAssetCtxsResponse:
    """Create a mock meta and asset contexts response."""
    return HyperliquidRawMetaAndAssetCtxsResponse(
        meta=HyperliquidRawMetaResponse(
            universe=[
                HyperliquidRawAssetDefinition(
                    name="ETH",
                    szDecimals=4,
                    maxLeverage=50,
                    marginTableId=None,
                    isDelisted=None,
                    onlyIsolated=False,
                ),
                HyperliquidRawAssetDefinition(
                    name="BTC",
                    szDecimals=5,
                    maxLeverage=50,
                    marginTableId=None,
                    isDelisted=None,
                    onlyIsolated=False,
                ),
            ],
            marginTables=None,
        ),
        asset_ctxs=[
            mock_asset_ctx,
            HyperliquidRawAssetCtx(
                funding="0.0002",
                markPx="65000.00",
                openInterest="5000000.00",
                oraclePx="64999.00",
                prevDayPx="64000.00",
                dayNtlVlm="100000000.00",
                dayBaseVlm="2000.00",
                premium="0.0010",
                name="ETH",
                midPx="64999.50",
                impactPx="65000.20",
                impactPxs=["65000.10", "65000.30"],
            ),
        ],
    )


@pytest.fixture
def mock_funding_rate() -> FundingRate:
    """Create a mock funding rate."""
    return FundingRate(
        symbol="ETH",
        funding_rate=Decimal("0.0001"),
        mark_price=Decimal("3500.00"),
        index_price=Decimal("3499.50"),
        timestamp=datetime.now(UTC),
    )


@pytest.fixture
def mock_historical_funding_item() -> HyperliquidRawFundingHistoryItem:
    """Create a mock historical funding rate item."""
    return HyperliquidRawFundingHistoryItem(
        coin="ETH",
        fundingRate="0.0001",
        premium="0.0001",
        time=1704067200000,  # 2024-01-01 00:00:00
    )


@pytest.fixture
def mock_candle_snapshot() -> HyperliquidRawCandleSnapshot:
    """Create a mock candle snapshot."""
    return HyperliquidRawCandleSnapshot(
        t=[1704067200000, 1704070800000],  # Timestamps
        o=["64000.00", "64500.00"],  # Open prices
        h=["64600.00", "65100.00"],  # High prices
        l=["63900.00", "64400.00"],  # Low prices
        c=["64500.00", "65000.00"],  # Close prices
        v=["100.50", "120.75"],  # Volumes
        s="ok",  # Status
    )


@pytest.fixture
def mock_candle() -> Candle:
    """Create a mock candle."""
    return Candle(
        symbol="BTC",
        interval="1h",
        open_time=datetime(2024, 1, 1, tzinfo=UTC),
        open=Decimal("64000.00"),
        high=Decimal("64600.00"),
        low=Decimal("63900.00"),
        close=Decimal("64500.00"),
        volume=Decimal("100.50"),
    )


class TestHyperliquidHistoricalDataService:
    """Test suite for HyperliquidHistoricalDataService."""

    @pytest.mark.asyncio
    async def test_get_funding_rates_all_symbols(
        self,
        historical_data_service: HyperliquidHistoricalDataService,
        mock_meta_and_asset_ctxs_response: HyperliquidRawMetaAndAssetCtxsResponse,
        mock_mapper: Mock,
        mock_funding_rate: FundingRate,
    ) -> None:
        """Test retrieval of funding rates for all symbols."""
        # Arrange
        args = GetFundingRatesArgs(symbols=None)  # None means all symbols

        with patch.object(
            historical_data_service,
            "_get_all_asset_contexts_raw",
            return_value=mock_meta_and_asset_ctxs_response,
        ):
            # Create two funding rates for ETH and BTC
            funding_rate_btc = FundingRate(
                symbol="BTC",
                funding_rate=Decimal("0.0002"),
                mark_price=Decimal("65000.00"),
                index_price=Decimal("64999.00"),
                timestamp=datetime.now(UTC),
            )

            mock_mapper.transform_raw_asset_ctx_to_funding_rate.side_effect = [
                mock_funding_rate,
                funding_rate_btc,
            ]

            # Act
            result = await historical_data_service.get_funding_rates(args)

            # Assert
            assert len(result) == 2
            assert result[0].symbol == "ETH"
            assert result[1].symbol == "BTC"
            assert mock_mapper.transform_raw_asset_ctx_to_funding_rate.call_count == 2

    @pytest.mark.asyncio
    async def test_get_funding_rates_specific_symbols(
        self,
        historical_data_service: HyperliquidHistoricalDataService,
        mock_meta_and_asset_ctxs_response: HyperliquidRawMetaAndAssetCtxsResponse,
        mock_mapper: Mock,
        mock_funding_rate: FundingRate,
    ) -> None:
        """Test retrieval of funding rates for specific symbols."""
        # Arrange
        args = GetFundingRatesArgs(symbols=["ETH"])

        with patch.object(
            historical_data_service,
            "_get_all_asset_contexts_raw",
            return_value=mock_meta_and_asset_ctxs_response,
        ):
            mock_mapper.transform_raw_asset_ctx_to_funding_rate.return_value = mock_funding_rate

            # Act
            result = await historical_data_service.get_funding_rates(args)

            # Assert
            assert len(result) == 1
            assert result[0].symbol == "ETH"
            mock_mapper.transform_raw_asset_ctx_to_funding_rate.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_funding_rates_invalid_symbols(
        self,
        historical_data_service: HyperliquidHistoricalDataService,
    ) -> None:
        """Test funding rates retrieval with invalid symbols."""
        # Arrange
        args = GetFundingRatesArgs(symbols=["", "ETH"])

        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            await historical_data_service.get_funding_rates(args)

        assert "non-empty string" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_funding_rates_empty_asset_contexts(
        self,
        historical_data_service: HyperliquidHistoricalDataService,
    ) -> None:
        """Test funding rates retrieval with empty asset contexts."""
        # Arrange
        args = GetFundingRatesArgs(symbols=None)
        empty_response = HyperliquidRawMetaAndAssetCtxsResponse(
            meta=HyperliquidRawMetaResponse(universe=[], marginTables=None),
            asset_ctxs=[],
        )

        with patch.object(
            historical_data_service, "_get_all_asset_contexts_raw", return_value=empty_response
        ):
            # Act
            result = await historical_data_service.get_funding_rates(args)

            # Assert
            assert result == []

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_success(
        self,
        historical_data_service: HyperliquidHistoricalDataService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_mapper: Mock,
        mock_historical_funding_item: HyperliquidRawFundingHistoryItem,
        mock_funding_rate: FundingRate,
    ) -> None:
        """Test successful historical funding rates retrieval."""
        # Arrange
        args = GetHistoricalFundingRatesArgs(
            symbol="ETH",
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 2, tzinfo=UTC),
        )

        mock_request_payload = MagicMock()
        mock_request_builder.build_funding_history_request_payload.return_value = (
            mock_request_payload
        )

        raw_response = [{"coin": "ETH", "fundingRate": "0.0001", "time": 1704067200000}]
        mock_http_requester.return_value = (raw_response, 200, {})

        mock_response_handler.handle_funding_history_response.return_value = [
            mock_historical_funding_item
        ]
        mock_mapper.transform_raw_funding_history_item_to_funding_rate.return_value = (
            mock_funding_rate
        )

        # Act
        result = await historical_data_service.get_historical_funding_rates(args)

        # Assert
        assert len(result) == 1
        assert result[0] == mock_funding_rate
        mock_request_builder.build_funding_history_request_payload.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_invalid_time_range(
        self,
        historical_data_service: HyperliquidHistoricalDataService,
    ) -> None:
        """Test historical funding rates with invalid time range."""
        # Arrange
        args = GetHistoricalFundingRatesArgs(
            symbol="ETH",
            start_time=datetime(2024, 1, 2, tzinfo=UTC),
            end_time=datetime(2024, 1, 1, tzinfo=UTC),  # End before start
        )

        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            await historical_data_service.get_historical_funding_rates(args)

        assert "start_time must be before end_time" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_future_time(
        self,
        historical_data_service: HyperliquidHistoricalDataService,
    ) -> None:
        """Test historical funding rates with future time."""
        # Arrange
        future_time = datetime.now(UTC) + timedelta(days=1)
        args = GetHistoricalFundingRatesArgs(
            symbol="ETH",
            start_time=datetime.now(UTC),
            end_time=future_time,
        )

        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            await historical_data_service.get_historical_funding_rates(args)

        assert "cannot be in the future" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_market_data_success(
        self,
        historical_data_service: HyperliquidHistoricalDataService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_mapper: Mock,
        mock_candle_snapshot: HyperliquidRawCandleSnapshot,
        mock_candle: Candle,
    ) -> None:
        """Test successful market data (candles) retrieval."""
        # Arrange
        args = GetMarketDataArgs(
            symbol="BTC",
            timeframe="1h",
            limit=100,
            start_time_ms=1704067200000,
            end_time_ms=1704153600000,
        )

        mock_request_payload = MagicMock()
        mock_request_builder.build_candle_snapshot_request_payload.return_value = (
            mock_request_payload
        )

        raw_response = {
            "s": "ok",
            "t": [1704067200000],
            "o": ["64000.00"],
            "c": ["64500.00"],
            "h": ["64600.00"],
            "l": ["63900.00"],
            "v": ["100.50"],
            "n": [1500],
        }
        mock_http_requester.return_value = (raw_response, 200, {})

        mock_response_handler.handle_candle_snapshot_response.return_value = mock_candle_snapshot
        mock_mapper.transform_raw_candle_snapshot_to_candles.return_value = [mock_candle]

        # Act
        result = await historical_data_service.get_market_data(args)

        # Assert
        assert len(result) == 1
        assert result[0] == mock_candle
        mock_mapper.transform_raw_candle_snapshot_to_candles.assert_called_once_with(
            mock_candle_snapshot,
            "BTC",
            "1h",
        )

    @pytest.mark.asyncio
    async def test_get_market_data_invalid_timeframe(
        self,
        historical_data_service: HyperliquidHistoricalDataService,
    ) -> None:
        """Test market data retrieval with invalid timeframe."""
        # Arrange
        args = GetMarketDataArgs(
            symbol="BTC",
            timeframe="invalid",
            limit=100,
        )

        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            await historical_data_service.get_market_data(args)

        assert "invalid timeframe" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_get_market_data_invalid_limit(
        self,
        historical_data_service: HyperliquidHistoricalDataService,
    ) -> None:
        """Test market data retrieval with invalid limit."""
        # Arrange
        args = GetMarketDataArgs(
            symbol="BTC",
            timeframe="1h",
            limit=0,  # Invalid limit
        )

        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            await historical_data_service.get_market_data(args)

        assert "limit must be between 1 and" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_funding_rates_transformation_error(
        self,
        historical_data_service: HyperliquidHistoricalDataService,
        mock_meta_and_asset_ctxs_response: HyperliquidRawMetaAndAssetCtxsResponse,
        mock_mapper: Mock,
    ) -> None:
        """Test funding rates with transformation error."""
        # Arrange
        args = GetFundingRatesArgs(symbols=["ETH"])

        with patch.object(
            historical_data_service,
            "_get_all_asset_contexts_raw",
            return_value=mock_meta_and_asset_ctxs_response,
        ):
            mock_mapper.transform_raw_asset_ctx_to_funding_rate.side_effect = TransformationError(
                "Invalid funding data"
            )

            # Act & Assert
            with pytest.raises(APIError) as exc_info:
                await historical_data_service.get_funding_rates(args)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value

    @pytest.mark.asyncio
    async def test_get_market_data_empty_candles(
        self,
        historical_data_service: HyperliquidHistoricalDataService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_mapper: Mock,
    ) -> None:
        """Test market data retrieval with empty candles response."""
        # Arrange
        args = GetMarketDataArgs(
            symbol="BTC",
            timeframe="1h",
            limit=100,
        )

        mock_request_builder.build_candle_snapshot_request_payload.return_value = MagicMock()
        mock_http_requester.return_value = ({"s": "ok", "t": []}, 200, {})

        empty_snapshot = HyperliquidRawCandleSnapshot(t=[], o=[], h=[], l=[], c=[], v=[], s="ok")
        mock_response_handler.handle_candle_snapshot_response.return_value = empty_snapshot
        mock_mapper.transform_raw_candle_snapshot_to_candles.return_value = []

        # Act
        result = await historical_data_service.get_market_data(args)

        # Assert
        assert result == []

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_http_error(
        self,
        historical_data_service: HyperliquidHistoricalDataService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
    ) -> None:
        """Test historical funding rates with HTTP error."""
        # Arrange
        args = GetHistoricalFundingRatesArgs(
            symbol="ETH",
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 2, tzinfo=UTC),
        )

        mock_request_builder.build_funding_history_request_payload.return_value = MagicMock()
        mock_http_requester.side_effect = Exception("Network timeout")

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await historical_data_service.get_historical_funding_rates(args)

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Network timeout" in str(exc_info.value)
