"""
Unit tests for the HyperliquidMarketDataService.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.connectivity.http_client import HttpClient
from cyberdelta.apis.connectivity.rate_limiter_service import RateLimiterService
from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.hl_response_handler import (
    HyperliquidResponseHandler,
    RawJsonResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import (
    HyperliquidRawCandleRequestDetails,
    HyperliquidRawCandleSnapshot,
    HyperliquidRawCandleSnapshotRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetCtx,  # For get_ticker, get_funding_rate
    HyperliquidRawAssetDefinition,
    HyperliquidRawMetaAndAssetCtxsRequestPayload,
    HyperliquidRawMetaAndAssetCtxsResponse,  # For get_all_asset_contexts
    HyperliquidRawMetaResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import (
    HyperliquidRawL2Book,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import (
    HyperliquidRawPublicTrade,
)
from cyberdelta.apis.hyperliquid.services.hl_market_data_service import HyperliquidMarketDataService


@pytest.fixture
def mock_http_client() -> AsyncMock:
    """Provides a mock HttpClient."""
    client = AsyncMock(spec=HttpClient)
    # Explicitly set .request to be an AsyncMock. This new mock won't use
    # HttpClient.request spec for its own call validation during assertions.
    # It will accept any kwargs. The spec on 'client' handles attribute errors.
    client.request = AsyncMock()
    return client


@pytest.fixture
def mock_request_builder() -> MagicMock:
    """Provides a mock HyperliquidRequestBuilder."""
    return MagicMock(spec=HyperliquidRequestBuilder)


@pytest.fixture
def mock_response_handler() -> MagicMock:
    """Provides a mock HyperliquidResponseHandler."""
    return MagicMock(spec=HyperliquidResponseHandler)


@pytest.fixture
def mock_rate_limiter_service() -> AsyncMock:
    """Provides a mock RateLimiterService."""
    # Configure wait_for_permission to be an async method
    mock_service = AsyncMock(spec=RateLimiterService)
    mock_service.wait_for_permission = AsyncMock()
    return mock_service


@pytest.fixture
def hl_market_data_service(
    mock_http_client: AsyncMock,
    mock_request_builder: MagicMock,
    mock_response_handler: MagicMock,
    mock_rate_limiter_service: AsyncMock,
) -> HyperliquidMarketDataService:
    """Provides an instance of HyperliquidMarketDataService with mocked dependencies."""
    return HyperliquidMarketDataService(
        http_client=mock_http_client,
        request_builder=mock_request_builder,
        response_handler=mock_response_handler,
        rate_limiter_service=mock_rate_limiter_service,
    )


class TestHyperliquidMarketDataService:
    """Tests for the HyperliquidMarketDataService class."""

    @pytest.mark.asyncio
    async def test_get_all_asset_contexts_success(
        self,
        hl_market_data_service: HyperliquidMarketDataService,
        mock_http_client: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test get_all_asset_contexts successfully retrieves and processes data."""
        # build_info_request_payload now returns a Pydantic model
        mock_payload_from_builder = HyperliquidRawMetaAndAssetCtxsRequestPayload(
            type="metaAndAssetCtxs"
        )
        expected_data_dict = mock_payload_from_builder.model_dump(by_alias=True, exclude_none=True)

        # Construct a realistic raw response content based on
        # HyperliquidRawMetaAndAssetCtxsResponse structure
        mock_raw_response_content: list[RawJsonResponse] = [
            {"universe": []},  # Meta part
            [],  # AssetContexts part (empty for simplicity)
        ]
        mock_validated_response = HyperliquidRawMetaAndAssetCtxsResponse(
            meta=HyperliquidRawMetaResponse(universe=[]),
            asset_ctxs=[],
        )

        mock_request_builder.build_info_request_payload.return_value = mock_payload_from_builder
        mock_http_client.request.return_value = (mock_raw_response_content, 200, MagicMock())
        # The response handler in the SUT is called with the raw_response_content
        mock_response_handler.handle_info_meta_and_asset_ctxs_response.return_value = (
            mock_validated_response
        )

        result = await hl_market_data_service.get_all_asset_contexts()

        # Check that the correct request builder method was called
        mock_request_builder.build_info_request_payload.assert_called_once_with()
        # Check that HttpClient was called with data from the builder
        mock_http_client.request.assert_called_once_with(
            method="POST",
            endpoint_path="/info",
            data=expected_data_dict,  # Assert with the dumped model dict
            rate_limiter_service=mock_rate_limiter_service,
        )
        # Check that the response handler was called correctly
        mock_response_handler.handle_info_meta_and_asset_ctxs_response.assert_called_once_with(
            mock_raw_response_content
        )
        assert result == mock_validated_response

    @pytest.mark.asyncio
    async def test_get_ticker_success(
        self,
        hl_market_data_service: HyperliquidMarketDataService,
        mock_response_handler: MagicMock,  # Only need this for direct mocking
    ) -> None:
        """Test get_ticker successfully retrieves and processes ticker data."""
        symbol = "ETH"
        mock_asset_ctx = HyperliquidRawAssetCtx(
            name="ETH",
            markPx="2000.0",
            funding="0.0001",
            prevDayPx="1990.0",
            dayNtlVlm="1000000",
            impactPx="2001.0",
        )

        hl_market_data_service.get_all_asset_contexts = AsyncMock(  # type: ignore[method-assign]
            return_value=HyperliquidRawMetaAndAssetCtxsResponse(
                meta=HyperliquidRawMetaResponse(
                    universe=[
                        HyperliquidRawAssetDefinition(
                            name="BTC", szDecimals=5, maxLeverage=100, onlyIsolated=False
                        ),
                        HyperliquidRawAssetDefinition(
                            name="ETH", szDecimals=5, maxLeverage=100, onlyIsolated=False
                        ),
                        HyperliquidRawAssetDefinition(
                            name="SOL", szDecimals=6, maxLeverage=50, onlyIsolated=False
                        ),
                    ]
                ),
                asset_ctxs=[
                    HyperliquidRawAssetCtx(
                        name="BTC",
                        markPx="30000.0",
                        funding="0.0002",
                        prevDayPx="29900.0",
                        dayNtlVlm="2000000",
                        impactPx="30010.0",
                    ),
                    mock_asset_ctx,
                    HyperliquidRawAssetCtx(
                        name="SOL",
                        markPx="100.0",
                        funding="0.0003",
                        prevDayPx="99.0",
                        dayNtlVlm="500000",
                        impactPx="101.0",
                    ),
                ],
            )
        )

        result = await hl_market_data_service.get_ticker(symbol)

        hl_market_data_service.get_all_asset_contexts.assert_called_once()
        assert result is not None
        assert result.name == symbol
        assert result == mock_asset_ctx

    @pytest.mark.asyncio
    async def test_get_ticker_not_found(
        self,
        hl_market_data_service: HyperliquidMarketDataService,
    ) -> None:
        """Test get_ticker returns None when symbol is not found."""
        symbol = "UNKNOWN"
        hl_market_data_service.get_all_asset_contexts = AsyncMock(  # type: ignore[method-assign]
            return_value=HyperliquidRawMetaAndAssetCtxsResponse(
                meta=HyperliquidRawMetaResponse(
                    universe=[
                        HyperliquidRawAssetDefinition(
                            name="BTC", szDecimals=5, maxLeverage=100, onlyIsolated=False
                        )
                    ]
                ),
                asset_ctxs=[
                    HyperliquidRawAssetCtx(
                        name="BTC",
                        markPx="30000.0",
                        funding="0.0002",
                        prevDayPx="29900.0",
                        dayNtlVlm="2000000",
                        impactPx="30010.0",
                    ),
                ],
            )
        )

        result = await hl_market_data_service.get_ticker(symbol)
        assert result is None

    @pytest.mark.asyncio
    async def test_get_funding_rate_success(
        self,
        hl_market_data_service: HyperliquidMarketDataService,
    ) -> None:
        """Test get_funding_rate successfully retrieves funding rate (via asset context)."""
        symbol = "ETH"
        mock_asset_ctx = HyperliquidRawAssetCtx(
            name="ETH",
            markPx="2000.0",
            funding="0.0001",  # Target data
            prevDayPx="1990.0",
            dayNtlVlm="1000000",
            impactPx="2001.0",
        )

        hl_market_data_service.get_all_asset_contexts = AsyncMock(  # type: ignore[method-assign]
            return_value=HyperliquidRawMetaAndAssetCtxsResponse(
                meta=HyperliquidRawMetaResponse(
                    universe=[
                        HyperliquidRawAssetDefinition(
                            name="ETH", szDecimals=5, maxLeverage=100, onlyIsolated=False
                        )
                    ]
                ),
                asset_ctxs=[mock_asset_ctx],
            )
        )

        result = await hl_market_data_service.get_funding_rate(symbol)
        hl_market_data_service.get_all_asset_contexts.assert_called_once()
        assert result is not None
        assert result.funding == "0.0001"
        assert result == mock_asset_ctx

    @pytest.mark.asyncio
    async def test_get_funding_rate_not_found(
        self,
        hl_market_data_service: HyperliquidMarketDataService,
    ) -> None:
        """Test get_funding_rate returns None when symbol is not found."""
        symbol = "UNKNOWN"
        hl_market_data_service.get_all_asset_contexts = AsyncMock(  # type: ignore[method-assign]
            return_value=HyperliquidRawMetaAndAssetCtxsResponse(
                meta=HyperliquidRawMetaResponse(universe=[]), asset_ctxs=[]
            )
        )
        result = await hl_market_data_service.get_funding_rate(symbol)
        assert result is None

    @pytest.mark.asyncio
    async def test_get_order_book_success(
        self,
        hl_market_data_service: HyperliquidMarketDataService,
        mock_http_client: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test get_order_book successfully retrieves and processes order book data."""
        symbol = "ETH"
        mock_request_payload_model = (
            MagicMock()  # Represents HyperliquidApiL2BookRequestPayload
        )
        mock_request_payload_dict = {"type": "l2Book", "coin": symbol}
        mock_raw_book_data: RawJsonResponse = {  # Type hint for clarity
            "coin": symbol,
            "levels": [[], []],
            "time": 1234567890000,
        }
        mock_validated_book = HyperliquidRawL2Book(coin=symbol, levels=[[], []], time=1234567890000)

        mock_request_builder.build_l2_book_request_payload.return_value = mock_request_payload_model
        mock_request_payload_model.model_dump.return_value = mock_request_payload_dict
        mock_http_client.request.return_value = (mock_raw_book_data, 200, MagicMock())
        mock_response_handler.handle_info_l2_book_response.return_value = mock_validated_book

        result = await hl_market_data_service.get_order_book(symbol)

        mock_request_builder.build_l2_book_request_payload.assert_called_once_with(symbol=symbol)
        mock_request_payload_model.model_dump.assert_called_once_with(
            by_alias=True, exclude_none=True
        )
        mock_http_client.request.assert_called_once_with(
            method="POST",
            endpoint_path="/info",
            data=mock_request_payload_dict,
            rate_limiter_service=mock_rate_limiter_service,
        )
        mock_response_handler.handle_info_l2_book_response.assert_called_once_with(
            mock_raw_book_data, symbol=symbol
        )
        assert result == mock_validated_book

    @pytest.mark.asyncio
    async def test_get_recent_trades_success(
        self,
        hl_market_data_service: HyperliquidMarketDataService,
        mock_http_client: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test get_recent_trades successfully retrieves and processes trade data."""
        symbol = "ETH"
        trade_time = 1234567890
        trade_hash = "0x123abc"
        mock_request_payload_model = (
            MagicMock()
        )  # Represents HyperliquidApiRecentTradesRequestPayload
        mock_request_payload_dict = {"type": "recentTrades", "coin": symbol}
        mock_raw_trades_data = [
            {
                "coin": symbol,
                "side": "B",
                "px": "2000",
                "sz": "1",
                "time": trade_time,
                "hash": trade_hash,
            }
        ]
        mock_validated_trades = [
            HyperliquidRawPublicTrade(
                coin=symbol, side="B", px="2000", sz="1", time=trade_time, hash=trade_hash
            )
        ]

        mock_request_builder.build_recent_trades_request_payload.return_value = (
            mock_request_payload_model
        )
        mock_request_payload_model.model_dump.return_value = mock_request_payload_dict
        mock_http_client.request.return_value = (mock_raw_trades_data, 200, MagicMock())
        mock_response_handler.handle_info_recent_trades_response.return_value = (
            mock_validated_trades
        )

        result = await hl_market_data_service.get_recent_trades(symbol)

        mock_request_builder.build_recent_trades_request_payload.assert_called_once_with(
            symbol=symbol
        )
        mock_request_payload_model.model_dump.assert_called_once_with(
            by_alias=True, exclude_none=True
        )
        mock_http_client.request.assert_called_once_with(
            method="POST",
            endpoint_path="/info",
            data=mock_request_payload_dict,
            rate_limiter_service=mock_rate_limiter_service,
        )
        mock_response_handler.handle_info_recent_trades_response.assert_called_once_with(
            mock_raw_trades_data, symbol=symbol
        )
        assert result == mock_validated_trades

    @pytest.mark.asyncio
    async def test_get_market_data_success(
        self,
        hl_market_data_service: HyperliquidMarketDataService,
        mock_http_client: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test get_market_data successfully fetches and processes candle snapshot data."""
        symbol = "ETH"
        interval = "1h"  # Test variable for interval
        start_time_ms = 1672531200000  # Example start time
        end_time_ms = 1672617600000  # Example end time

        mock_payload_model = HyperliquidRawCandleSnapshotRequestPayload(
            type="candleSnapshot",
            req=HyperliquidRawCandleRequestDetails(
                coin=symbol, interval=interval, startTime=start_time_ms, endTime=end_time_ms
            ),
        )
        mock_request_payload_data = mock_payload_model.model_dump(by_alias=True, exclude_none=True)

        mock_raw_response_content: RawJsonResponse = {  # Type hint for clarity
            "t": [start_time_ms],
            "o": ["1200.0"],
            "h": ["1250.0"],
            "l": ["1190.0"],
            "c": ["1240.0"],
            "v": ["1000.0"],
            "s": "ok",
        }

        mock_validated_snapshot = HyperliquidRawCandleSnapshot(
            t=[start_time_ms],
            o=["1200.0"],
            h=["1250.0"],
            l=["1190.0"],
            c=["1240.0"],
            v=["1000.0"],
            s="ok",
        )

        mock_request_builder.build_candle_snapshot_payload.return_value = mock_payload_model
        mock_http_client.request.return_value = (mock_raw_response_content, 200, MagicMock())
        mock_response_handler.handle_info_candle_snapshot_response.return_value = (
            mock_validated_snapshot
        )

        result = await hl_market_data_service.get_market_data(
            symbol, interval, start_time_ms, end_time_ms
        )

        mock_request_builder.build_candle_snapshot_payload.assert_called_once_with(
            symbol=symbol, timeframe=interval, start_time_ms=start_time_ms, end_time_ms=end_time_ms
        )
        mock_http_client.request.assert_called_once_with(
            method="POST",
            endpoint_path="/info",
            data=mock_request_payload_data,
            rate_limiter_service=mock_rate_limiter_service,
        )
        mock_response_handler.handle_info_candle_snapshot_response.assert_called_once_with(
            raw_response_content=mock_raw_response_content,
            symbol=symbol,
            interval=interval,
        )
        assert result == mock_validated_snapshot

    # Add more tests for other methods: get_funding_rate, get_order_book, etc.
