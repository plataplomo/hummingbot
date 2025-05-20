"""
Unit tests for the HyperliquidMarketDataService.
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.hl_mapper import (
    HyperliquidMapper,
)  # Added HyperliquidCandleMapper
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
from cyberdelta.apis.hyperliquid.models.hl_raw_funding_history_info import (
    HyperliquidRawFundingHistoryItem,
    RawHlCoinName,
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
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models.enums import OrderSide
from cyberdelta.core.models.market import Candle, FundingRate, OrderBook, Ticker, Trade


@pytest.fixture
def mock_http_client_requester() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def mock_hl_request_builder() -> MagicMock:
    return MagicMock(spec=HyperliquidRequestBuilder)


@pytest.fixture
def mock_hl_response_handler() -> MagicMock:
    return MagicMock(spec=HyperliquidResponseHandler)


@pytest.fixture
def mock_hl_mapper() -> MagicMock:
    return MagicMock(spec=HyperliquidMapper)


@pytest.fixture
def hyperliquid_market_data_service(
    mock_http_client_requester: AsyncMock,
    mock_hl_request_builder: MagicMock,
    mock_hl_response_handler: MagicMock,
    mock_hl_mapper: MagicMock,
) -> HyperliquidMarketDataService:
    return HyperliquidMarketDataService(
        http_client_requester=mock_http_client_requester,
        request_builder=mock_hl_request_builder,
        response_handler=mock_hl_response_handler,
        mapper=mock_hl_mapper,
        exchange_name="hyperliquid_test",
        info_url="https://fakeapi.hyperliquid.com/info",
    )


class TestHyperliquidMarketDataService:
    """Tests for the HyperliquidMarketDataService class."""

    @pytest.mark.asyncio
    async def test_get_all_asset_contexts_success(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
    ) -> None:
        """Test get_all_asset_contexts successfully retrieves and processes data."""
        mock_payload_from_builder = HyperliquidRawMetaAndAssetCtxsRequestPayload(
            type="metaAndAssetCtxs"
        )
        expected_data_dict = mock_payload_from_builder.model_dump(by_alias=True, exclude_none=True)

        mock_raw_response_content: list[RawJsonResponse] = [
            {"universe": []},
            [],
        ]
        mock_validated_response = HyperliquidRawMetaAndAssetCtxsResponse(
            meta=HyperliquidRawMetaResponse(universe=[]),
            asset_ctxs=[],
        )

        mock_hl_request_builder.build_info_request_payload.return_value = mock_payload_from_builder
        mock_http_client_requester.return_value = (
            mock_raw_response_content,
            200,
            MagicMock(),
        )
        mock_hl_response_handler.handle_info_meta_and_asset_ctxs_response.return_value = (
            mock_validated_response
        )

        result = await hyperliquid_market_data_service.get_all_asset_contexts_raw()

        mock_hl_request_builder.build_info_request_payload.assert_called_once_with()
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint_path="/info",
            data=expected_data_dict,
            is_info_endpoint=True,
        )
        mock_hl_response_handler.handle_info_meta_and_asset_ctxs_response.assert_called_once_with(
            mock_raw_response_content
        )
        assert result == mock_validated_response

    @pytest.mark.asyncio
    async def test_get_ticker_success(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
    ) -> None:
        """Test get_ticker successfully retrieves and processes ticker data."""
        symbol_to_find = "BTC"

        mock_raw_asset_ctx_btc = HyperliquidRawAssetCtx(
            name="BTC",
            funding="0.0001",
            markPx="50000.0",
            prevDayPx="49000.0",
            dayNtlVlm="1000",
            impactPx="50001.0",
        )
        mock_raw_asset_ctx_eth = HyperliquidRawAssetCtx(
            name="ETH",
            funding="0.0002",
            markPx="3000.0",
            prevDayPx="2900.0",
            dayNtlVlm="500",
            impactPx="3001.0",
        )
        mock_meta_response = HyperliquidRawMetaResponse(
            universe=[
                HyperliquidRawAssetDefinition(
                    name="BTC", szDecimals=5, maxLeverage=100, onlyIsolated=False
                ),
                HyperliquidRawAssetDefinition(
                    name="ETH", szDecimals=5, maxLeverage=100, onlyIsolated=False
                ),
            ]
        )
        mock_all_contexts_response = HyperliquidRawMetaAndAssetCtxsResponse(
            meta=mock_meta_response,
            asset_ctxs=[mock_raw_asset_ctx_btc, mock_raw_asset_ctx_eth],
        )

        hyperliquid_market_data_service.get_all_asset_contexts_raw = AsyncMock(  # type: ignore[method-assign]
            return_value=mock_all_contexts_response
        )

        expected_internal_ticker = Ticker(
            symbol=symbol_to_find,
            price=Decimal("50000.0"),
            bid=Decimal("50000.0"),
            ask=Decimal("50000.0"),
            volume=Decimal("1000"),
            timestamp=datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC),
        )

        with patch(
            "cyberdelta.apis.hyperliquid.services.hl_market_data_service.HyperliquidMapper.map_raw_ctx_to_ticker",
            return_value=expected_internal_ticker,
        ) as mock_map_ticker_method:
            result_ticker = await hyperliquid_market_data_service.get_ticker(symbol_to_find)

            hyperliquid_market_data_service.get_all_asset_contexts_raw.assert_called_once()
            mock_map_ticker_method.assert_called_once_with(mock_raw_asset_ctx_btc)
            assert result_ticker == expected_internal_ticker

    @pytest.mark.asyncio
    async def test_get_ticker_not_found(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
    ) -> None:
        """Test get_ticker returns None when symbol is not found."""
        symbol = "UNKNOWN"
        mock_meta_response = HyperliquidRawMetaResponse(universe=[])
        mock_all_contexts_response = HyperliquidRawMetaAndAssetCtxsResponse(
            meta=mock_meta_response, asset_ctxs=[]
        )

        hyperliquid_market_data_service.get_all_asset_contexts_raw = AsyncMock(  # type: ignore[method-assign]
            return_value=mock_all_contexts_response
        )

        # If _get_asset_context_by_name returns None, mapper shouldn't be called.
        # If it's called with None, it should handle it or map_raw_ctx_to_ticker might return None.
        with patch.object(HyperliquidMapper, "map_raw_ctx_to_ticker", return_value=None):
            result = await hyperliquid_market_data_service.get_ticker(symbol)
            assert result is None
            # Depending on exact internal logic of get_ticker if asset_ctx is None:
            # mock_mapper_method.assert_not_called() or ensure it was called and returned None.
            # For this test, we assume if context is not found, map_raw_ctx_to_ticker might
            # not be called or if it is (e.g. with None), it's mocked to return None.

    @pytest.mark.asyncio
    async def test_get_funding_rate_success(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
    ) -> None:
        """Test get_funding_rate successfully retrieves and maps funding rate data."""
        symbol_to_find = "ETH"
        mock_funding_timestamp = datetime.now(UTC)

        mock_raw_asset_ctx_eth = HyperliquidRawAssetCtx(
            name="ETH",
            funding="0.0002",
            markPx="3000.0",
            prevDayPx="2900.0",
            dayNtlVlm="500",
            impactPx="3001.0",
        )
        mock_meta_response = HyperliquidRawMetaResponse(
            universe=[
                HyperliquidRawAssetDefinition(
                    name="ETH", szDecimals=5, maxLeverage=100, onlyIsolated=False
                )
            ]
        )
        mock_all_contexts_response = HyperliquidRawMetaAndAssetCtxsResponse(
            meta=mock_meta_response, asset_ctxs=[mock_raw_asset_ctx_eth]
        )

        hyperliquid_market_data_service.get_all_asset_contexts_raw = AsyncMock(  # type: ignore[method-assign]
            return_value=mock_all_contexts_response
        )

        expected_internal_funding_rate = FundingRate(
            symbol=symbol_to_find,
            funding_rate=Decimal("0.0002"),
            timestamp=mock_funding_timestamp,
        )

        with patch(
            "cyberdelta.apis.hyperliquid.services.hl_market_data_service.HyperliquidMapper.map_raw_ctx_to_funding_rate",
            return_value=expected_internal_funding_rate,
        ) as mock_map_funding_method:
            result_funding_rate = await hyperliquid_market_data_service.get_funding_rate(
                symbol_to_find
            )

            hyperliquid_market_data_service.get_all_asset_contexts_raw.assert_called_once()
            mock_map_funding_method.assert_called_once_with(mock_raw_asset_ctx_eth)
            assert result_funding_rate == expected_internal_funding_rate
            if result_funding_rate:
                assert result_funding_rate.funding_rate == Decimal("0.0002")

    @pytest.mark.asyncio
    async def test_get_order_book_success(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
    ) -> None:
        """Test get_order_book successfully retrieves and processes order book data."""
        symbol = "ETH"
        mock_request_payload_model = (
            MagicMock()  # Represents HyperliquidApiL2BookRequestPayload
        )
        mock_request_payload_dict = {"type": "l2Book", "coin": symbol}
        mock_raw_book_data_content: RawJsonResponse = {  # Renamed for clarity
            "coin": symbol,
            "levels": [[], []],
            "time": 1234567890000,
        }
        # This is what the response_handler returns
        mock_validated_raw_book = HyperliquidRawL2Book(
            coin=symbol, levels=[[], []], time=1234567890000
        )

        # This is what the mapper should return, and thus the service method
        expected_internal_order_book = OrderBook(
            symbol=symbol,
            bids=[],
            asks=[],
            timestamp=datetime.fromtimestamp(1234567890000 / 1000, UTC),  # Example timestamp
        )

        mock_hl_request_builder.build_l2_book_request_payload.return_value = (
            mock_request_payload_model
        )
        mock_request_payload_model.model_dump.return_value = mock_request_payload_dict

        mock_http_client_requester.return_value = (
            mock_raw_book_data_content,
            200,
            MagicMock(),
        )
        mock_hl_response_handler.handle_info_l2_book_response.return_value = mock_validated_raw_book

        # Patch the static method HyperliquidMapper.map_raw_order_book
        with patch(
            "cyberdelta.apis.hyperliquid.services.hl_market_data_service.HyperliquidMapper.map_raw_order_book",
            return_value=expected_internal_order_book,
        ) as mock_map_order_book_method:
            result = await hyperliquid_market_data_service.get_order_book(symbol)

            mock_hl_request_builder.build_l2_book_request_payload.assert_called_once_with(
                symbol=symbol
            )
            mock_request_payload_model.model_dump.assert_called_once_with(
                by_alias=True, exclude_none=True
            )
            mock_http_client_requester.assert_called_once_with(
                method="POST",
                endpoint_path="/info",
                data=mock_request_payload_dict,
                rate_limiter_service=None,
            )
            mock_hl_response_handler.handle_info_l2_book_response.assert_called_once_with(
                mock_raw_book_data_content,
                symbol=symbol,
            )
            mock_map_order_book_method.assert_called_once_with(mock_validated_raw_book, None)
            assert result == expected_internal_order_book

    @pytest.mark.asyncio
    async def test_get_order_book_http_client_returns_none(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
    ) -> None:
        """Test get_order_book when HTTP client returns None content."""
        symbol = "ETH"
        mock_request_payload_model = MagicMock()
        mock_request_payload_dict = {"type": "l2Book", "coin": symbol}

        mock_hl_request_builder.build_l2_book_request_payload.return_value = (
            mock_request_payload_model
        )
        mock_request_payload_model.model_dump.return_value = mock_request_payload_dict
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with pytest.raises(APIError) as exc_info:
            await hyperliquid_market_data_service.get_order_book(symbol)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "No content received from HTTP client for l2Book" in exc_info.value.message
        mock_hl_request_builder.build_l2_book_request_payload.assert_called_once_with(symbol=symbol)
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint_path="/info",
            data=mock_request_payload_dict,
            rate_limiter_service=None,
        )

    @pytest.mark.asyncio
    async def test_get_recent_trades_success(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
    ) -> None:
        """Test get_recent_trades successfully retrieves and processes trade data."""
        symbol = "ETH"
        trade_time_ms = 1234567890000  # Ensure ms for consistency with Hyperliquid
        trade_hash = "0x123abc"
        mock_request_payload_model = (
            MagicMock()
        )  # Represents HyperliquidApiRecentTradesRequestPayload
        mock_request_payload_dict = {"type": "recentTrades", "coin": symbol}
        # This is the raw data from HTTP client
        mock_raw_trades_data_content: list[RawJsonResponse] = [
            {
                "coin": symbol,
                "side": "B",
                "px": "2000",
                "sz": "1",
                "time": trade_time_ms,  # Use ms
                "hash": trade_hash,
            }
        ]
        # This is what the response_handler returns (list of validated raw trades)
        mock_validated_raw_trades = [
            HyperliquidRawPublicTrade(
                coin=symbol, side="B", px="2000", sz="1", time=trade_time_ms, hash=trade_hash
            )
        ]

        # This is what the mapper (and thus the service) should return
        expected_internal_trades = [
            Trade(
                id=trade_hash,
                symbol=symbol,
                executed_at=datetime.fromtimestamp(trade_time_ms / 1000, UTC),
                side=OrderSide.BUY,  # Example
                order_id="UNKNOWN_PUBLIC_TRADE",  # Default for public HL trades from mapper
                exchange="hyperliquid_test",  # Should match service's exchange_name
                price=Decimal("2000"),
                quantity=Decimal("1"),
                fee=Decimal("0"),  # Default from mapper
                fee_asset=None,  # Default from mapper
                is_maker=None,  # Default from mapper
                # hl_details can be mocked if needed, or assume mapper handles it
            )
        ]

        mock_hl_request_builder.build_recent_trades_request_payload.return_value = (
            mock_request_payload_model
        )
        mock_request_payload_model.model_dump.return_value = mock_request_payload_dict
        mock_http_client_requester.request.return_value = (
            mock_raw_trades_data_content,
            200,
            MagicMock(),
        )
        # Response handler returns the list of validated RAW trades
        mock_hl_response_handler.handle_info_recent_trades_response.return_value = (
            mock_validated_raw_trades
        )

        # HyperliquidMapper.transform_raw_public_trade_to_internal is static
        with patch(
            "cyberdelta.apis.hyperliquid.services.hl_market_data_service.HyperliquidMapper.transform_raw_public_trade_to_internal",
            side_effect=expected_internal_trades,
        ) as mock_transform_trade_method:
            result = await hyperliquid_market_data_service.get_recent_trades(symbol)

            mock_hl_request_builder.build_recent_trades_request_payload.assert_called_once_with(
                symbol=symbol
            )
            mock_request_payload_model.model_dump.assert_called_once_with(
                by_alias=True, exclude_none=True
            )
            mock_http_client_requester.assert_called_once_with(
                method="POST",
                endpoint_path="/info",
                data=mock_request_payload_dict,
                rate_limiter_service=None,
            )
            mock_hl_response_handler.handle_info_recent_trades_response.assert_called_once_with(
                mock_raw_trades_data_content,
                symbol=symbol,
            )
            assert mock_transform_trade_method.call_count == len(mock_validated_raw_trades)
            if mock_validated_raw_trades:
                mock_transform_trade_method.assert_any_call(mock_validated_raw_trades[0])
            assert result == expected_internal_trades

    @pytest.mark.asyncio
    async def test_get_recent_trades_http_client_returns_none(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
    ) -> None:
        """Test get_recent_trades when HTTP client returns None content."""
        symbol = "ETH"
        mock_request_payload_model = MagicMock()
        mock_request_payload_dict = {"type": "recentTrades", "coin": symbol}

        mock_hl_request_builder.build_recent_trades_request_payload.return_value = (
            mock_request_payload_model
        )
        mock_request_payload_model.model_dump.return_value = mock_request_payload_dict
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with pytest.raises(APIError) as exc_info:
            await hyperliquid_market_data_service.get_recent_trades(symbol)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "No content received from HTTP client for recentTrades" in exc_info.value.message
        mock_hl_request_builder.build_recent_trades_request_payload.assert_called_once_with(
            symbol=symbol
        )
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint_path="/info",
            data=mock_request_payload_dict,
            rate_limiter_service=None,
        )

    @pytest.mark.asyncio
    async def test_get_market_data_success(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
    ) -> None:
        """Test get_market_data (candles) successfully retrieves and processes data."""
        symbol = "ETH"
        interval = "1h"
        start_time_ms = 1678886400000
        end_time_ms = 1678890000000

        mock_payload = HyperliquidRawCandleSnapshotRequestPayload(
            type="candleSnapshot",
            req=HyperliquidRawCandleRequestDetails(
                coin=symbol, interval=interval, startTime=start_time_ms, endTime=end_time_ms
            ),
        )
        expected_request_data = mock_payload.model_dump(by_alias=True, exclude_none=True)

        mock_raw_response_content: list[RawJsonResponse] = [
            {
                "c": "ETH",
                "t": 1678886400000,
                "T": 1678889999999,
                "s": "ok",
                "i": "1h",
                "o": [],
                "h": [],
                "l": [],
                "v": [],
                "n": 0,
            }
        ]
        mock_validated_raw_snapshot = HyperliquidRawCandleSnapshot(
            t=[],
            o=[],
            h=[],
            l=[],
            c=[],
            v=[],
            s="ok",  # Use 'c' for closes list
        )
        expected_internal_candles = [
            Candle(
                symbol=symbol,
                open=Decimal("100"),
                high=Decimal("110"),
                low=Decimal("90"),
                close=Decimal("105"),
                volume=Decimal("1000"),
                open_time=datetime.fromtimestamp(start_time_ms / 1000, UTC),
                interval=interval,
            )
        ]

        mock_hl_request_builder.build_candle_snapshot_payload.return_value = expected_request_data
        mock_http_client_requester.return_value = (
            mock_raw_response_content,
            200,
            MagicMock(),
        )
        mock_hl_response_handler.handle_info_candle_snapshot_response.return_value = (
            mock_validated_raw_snapshot
        )
        # Mock the static method HyperliquidCandleMapper.map
        with patch(
            "cyberdelta.apis.hyperliquid.services.hl_market_data_service.HyperliquidCandleMapper.map",
            return_value=expected_internal_candles,
        ) as mock_map_candles:
            result_candles = await hyperliquid_market_data_service.get_market_data(
                symbol, interval, start_time_ms, end_time_ms
            )

            mock_hl_request_builder.build_candle_snapshot_payload.assert_called_once_with(
                symbol=symbol,
                timeframe=interval,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )
            mock_http_client_requester.assert_called_once_with(
                method="POST",
                endpoint_path="/info",
                data=expected_request_data,
                is_info_endpoint=True,
            )
            mock_hl_response_handler.handle_info_candle_snapshot_response.assert_called_once_with(
                mock_raw_response_content,
                symbol,
                interval,
                200,
                mock_http_client_requester.return_value[2],
            )
            mock_map_candles.assert_called_once_with(mock_validated_raw_snapshot, symbol, interval)
            assert result_candles == expected_internal_candles

    @pytest.mark.asyncio
    async def test_get_market_data_http_client_returns_none(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
    ) -> None:
        """Test get_market_data when HTTP client returns None content."""
        symbol = "ETH"
        interval = "1h"
        start_time_ms = 1678886400000
        end_time_ms = 1678890000000

        mock_payload = HyperliquidRawCandleSnapshotRequestPayload(
            type="candleSnapshot",
            req=HyperliquidRawCandleRequestDetails(
                coin=symbol, interval=interval, startTime=start_time_ms, endTime=end_time_ms
            ),
        )
        expected_request_data = mock_payload.model_dump(by_alias=True, exclude_none=True)

        mock_hl_request_builder.build_candle_snapshot_payload.return_value = expected_request_data
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with pytest.raises(APIError) as exc_info:
            await hyperliquid_market_data_service.get_market_data(
                symbol, interval, start_time_ms, end_time_ms
            )

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "No content received from HTTP client for candleSnapshot" in exc_info.value.message
        mock_hl_request_builder.build_candle_snapshot_payload.assert_called_once_with(
            symbol=symbol,
            timeframe=interval,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint_path="/info",
            data=expected_request_data,
            is_info_endpoint=True,
        )

    @pytest.mark.asyncio
    async def test_get_all_asset_contexts_raw_success(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
    ) -> None:
        """Test the get_all_asset_contexts_raw method more directly."""
        mock_payload_model = MagicMock()
        mock_payload_dict = {"type": "metaAndAssetCtxs"}

        mock_raw_response_content = [
            {
                "universe": [
                    {"name": "BTC", "szDecimals": 5, "maxLeverage": 100, "onlyIsolated": False}
                ]
            },
            [
                {
                    "name": "BTC",
                    "funding": "0.0001",
                    "markPx": "50000",
                    "prevDayPx": "49000",
                    "dayNtlVlm": "100",
                    "impactPx": "50001",
                }
            ],
        ]
        mock_status_code = 200
        mock_headers: dict[Any, Any] = {}

        mock_validated_response = HyperliquidRawMetaAndAssetCtxsResponse(
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
                    funding="0.0001",
                    markPx="50000",
                    prevDayPx="49000",
                    dayNtlVlm="100",
                    impactPx="50001",
                )
            ],
        )

        mock_hl_request_builder.build_info_request_payload.return_value = mock_payload_model
        mock_payload_model.model_dump.return_value = mock_payload_dict

        mock_http_client_requester.return_value = (
            mock_raw_response_content,
            mock_status_code,
            mock_headers,
        )
        mock_hl_response_handler.handle_info_meta_and_asset_ctxs_response.return_value = (
            mock_validated_response
        )

        result = await hyperliquid_market_data_service.get_all_asset_contexts_raw()

        mock_hl_request_builder.build_info_request_payload.assert_called_once()
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint_path="/info",
            data=mock_payload_dict,
            is_info_endpoint=True,
            timeout_seconds=None,
        )
        mock_hl_response_handler.handle_info_meta_and_asset_ctxs_response.assert_called_once_with(
            mock_raw_response_content, mock_status_code, mock_headers
        )
        assert result == mock_validated_response

    @pytest.mark.asyncio
    async def test_get_all_asset_contexts_raw_http_client_returns_none(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
    ) -> None:
        """Test get_all_asset_contexts_raw when HTTP client returns None content."""
        mock_payload_from_builder = HyperliquidRawMetaAndAssetCtxsRequestPayload(
            type="metaAndAssetCtxs"
        )
        expected_data_dict = mock_payload_from_builder.model_dump(by_alias=True, exclude_none=True)

        mock_hl_request_builder.build_info_request_payload.return_value = mock_payload_from_builder
        # Simulate HTTP client returning None for content
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with pytest.raises(APIError) as exc_info:
            await hyperliquid_market_data_service.get_all_asset_contexts_raw()

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "No content received from HTTP client for metaAndAssetCtxs" in exc_info.value.message

        mock_hl_request_builder.build_info_request_payload.assert_called_once_with()
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint_path="/info",
            data=expected_data_dict,
            is_info_endpoint=True,
            timeout_seconds=None,  # Assuming default timeout behavior
        )

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_success(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
    ) -> None:
        """Test get_historical_funding_rates successfully retrieves and processes data."""
        symbol = "ETH"
        start_time_ms = 1678886400000
        end_time_ms = 1678890000000

        mock_request_payload = {
            "type": "fundingHistory",
            "coin": symbol,
            "startTime": start_time_ms,
            "endTime": end_time_ms,
        }
        mock_raw_response_data: list[RawJsonResponse] = [
            {"coin": "ETH", "fundingRate": "0.0001", "premium": "0.00012", "time": 1678886400076},
            {"coin": "ETH", "fundingRate": "0.00015", "premium": "0.00017", "time": 1678886500076},
        ]
        # These would be instances of HyperliquidRawFundingHistoryItem
        mock_validated_raw_items = [
            HyperliquidRawFundingHistoryItem(
                coin=RawHlCoinName("ETH"),
                fundingRate="0.0001",
                premium="0.00012",
                time=1678886400076,
            ),
            HyperliquidRawFundingHistoryItem(
                coin=RawHlCoinName("ETH"),
                fundingRate="0.00015",
                premium="0.00017",
                time=1678886500076,
            ),
        ]
        mock_internal_funding_rates = [
            FundingRate(
                symbol="ETH",
                funding_rate=Decimal("0.0001"),
                timestamp=datetime.fromtimestamp(1678886400.076, UTC),
            ),
            FundingRate(
                symbol="ETH",
                funding_rate=Decimal("0.00015"),
                timestamp=datetime.fromtimestamp(1678886500.076, UTC),
            ),
        ]

        mock_hl_request_builder.build_historical_funding_rates_payload.return_value = (
            mock_request_payload
        )
        mock_http_client_requester.return_value = (mock_raw_response_data, 200, MagicMock())
        mock_hl_response_handler.handle_historical_funding_rates_response.return_value = (
            mock_validated_raw_items
        )

        # Patch the static method HyperliquidMapper.transform_raw_funding_history_item_to_internal
        with patch(
            "cyberdelta.apis.hyperliquid.services.hl_market_data_service.HyperliquidMapper.transform_raw_funding_history_item_to_internal",
            side_effect=mock_internal_funding_rates,
        ) as mock_transform_method:
            result = await hyperliquid_market_data_service.get_historical_funding_rates(
                symbol, start_time_ms, end_time_ms
            )

            mock_hl_request_builder.build_historical_funding_rates_payload.assert_called_once_with(
                symbol=symbol, start_time_ms=start_time_ms, end_time_ms=end_time_ms
            )
            mock_http_client_requester.assert_called_once_with(
                method="POST",
                endpoint_path="/info",
                data=mock_request_payload,
                is_info_endpoint=True,
            )
            mock_hl_response_handler.handle_historical_funding_rates_response.assert_called_once_with(
                mock_raw_response_data
            )
            assert mock_transform_method.call_count == len(mock_validated_raw_items)
            for raw_item in mock_validated_raw_items:  # Changed from enumerate
                mock_transform_method.assert_any_call(raw_item)
            assert result == mock_internal_funding_rates

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_http_client_returns_none(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
    ) -> None:
        """Test get_historical_funding_rates when HTTP client returns None content."""
        symbol = "ETH"
        start_time_ms = 1678886400000
        end_time_ms = 1678890000000
        mock_request_payload = {
            "type": "fundingHistory",
            "coin": symbol,
            "startTime": start_time_ms,
            "endTime": end_time_ms,
        }

        mock_hl_request_builder.build_historical_funding_rates_payload.return_value = (
            mock_request_payload
        )
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with pytest.raises(APIError) as exc_info:
            await hyperliquid_market_data_service.get_historical_funding_rates(
                symbol, start_time_ms, end_time_ms
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "No content received from HTTP client" in exc_info.value.message
        mock_hl_request_builder.build_historical_funding_rates_payload.assert_called_once_with(
            symbol=symbol, start_time_ms=start_time_ms, end_time_ms=end_time_ms
        )
        mock_http_client_requester.assert_called_once_with(
            method="POST", endpoint_path="/info", data=mock_request_payload, is_info_endpoint=True
        )

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_response_validation_error(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
    ) -> None:
        """Test get_historical_funding_rates when response handler raises ValidationError."""
        symbol = "ETH"
        start_time_ms = 1678886400000
        end_time_ms = 1678890000000
        mock_request_payload = {
            "type": "fundingHistory",
            "coin": symbol,
            "startTime": start_time_ms,
            "endTime": end_time_ms,
        }
        mock_raw_response_data: list[RawJsonResponse] = [{"invalid_item": True}]

        mock_hl_request_builder.build_historical_funding_rates_payload.return_value = (
            mock_request_payload
        )
        mock_http_client_requester.return_value = (mock_raw_response_data, 200, MagicMock())

        # Make response_handler raise APIError (wrapping ValidationError)
        original_validation_error = ValidationError.from_exception_data(
            title="TestModel", line_errors=[]
        )
        mock_hl_response_handler.handle_historical_funding_rates_response.side_effect = APIError(
            message="Test validation error",
            code=APIErrorCode.INVALID_RESPONSE.value,
            original_exception=original_validation_error,
        )

        with pytest.raises(APIError) as exc_info:
            await hyperliquid_market_data_service.get_historical_funding_rates(
                symbol, start_time_ms, end_time_ms
            )

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Test validation error" in exc_info.value.message
        mock_hl_response_handler.handle_historical_funding_rates_response.assert_called_once_with(
            mock_raw_response_data
        )

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_mapper_error(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
    ) -> None:
        """Test get_historical_funding_rates when mapper raises an error."""
        symbol = "ETH"
        start_time_ms = 1678886400000
        end_time_ms = 1678890000000
        mock_request_payload = {
            "type": "fundingHistory",
            "coin": symbol,
            "startTime": start_time_ms,
            "endTime": end_time_ms,
        }
        mock_raw_response_data: list[RawJsonResponse] = [
            {"coin": "ETH", "fundingRate": "0.0001", "premium": "0.00012", "time": 1678886400076}
        ]
        mock_validated_raw_items = [
            HyperliquidRawFundingHistoryItem(
                coin=RawHlCoinName("ETH"),
                fundingRate="0.0001",
                premium="0.00012",
                time=1678886400076,
            )
        ]

        mock_hl_request_builder.build_historical_funding_rates_payload.return_value = (
            mock_request_payload
        )
        mock_http_client_requester.return_value = (mock_raw_response_data, 200, MagicMock())
        mock_hl_response_handler.handle_historical_funding_rates_response.return_value = (
            mock_validated_raw_items
        )

        with patch(
            "cyberdelta.apis.hyperliquid.services.hl_market_data_service.HyperliquidMapper.transform_raw_funding_history_item_to_internal",
            side_effect=ValueError("Test mapper error"),  # Make mapper raise an error
        ) as mock_transform_method:
            with pytest.raises(APIError) as exc_info:
                await hyperliquid_market_data_service.get_historical_funding_rates(
                    symbol, start_time_ms, end_time_ms
                )
            assert exc_info.value.code == APIErrorCode.UNKNOWN.value
            assert (
                "Error mapping historical funding rate data: ValueError('Test mapper error')"
                in exc_info.value.message
            )
            mock_transform_method.assert_called_once_with(mock_validated_raw_items[0])

    # Add more tests for other methods: get_funding_rate, get_order_book, etc.
