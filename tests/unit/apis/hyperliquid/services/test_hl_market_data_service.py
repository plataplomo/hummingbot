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
from cyberdelta.apis.hyperliquid.models.common_raw_types import RawHlCoinName
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import (
    HyperliquidRawCandleRequestDetails,
    HyperliquidRawCandleSnapshot,
    HyperliquidRawCandleSnapshotRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_funding_history_info import (
    HyperliquidRawFundingHistoryItem,
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
    HyperliquidRawL2BookRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import (
    HyperliquidRawPublicTrade,
)
from cyberdelta.apis.hyperliquid.services.hl_market_data_service import HyperliquidMarketDataService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models.enums import OrderSide
from cyberdelta.core.models.market import FundingRate, OrderBook, Ticker, Trade
from cyberdelta.core.models.market.candle import Candle


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
        mock_headers: dict[str, str] = {}
        mock_http_client_requester.return_value = (
            mock_raw_response_content,
            200,
            mock_headers,
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
            mock_raw_response_content, status_code=200, headers=mock_headers
        )
        assert result == mock_validated_response

    @pytest.mark.asyncio
    async def test_get_ticker_success(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_hl_mapper: MagicMock,
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

        mock_hl_mapper.map_raw_ctx_to_ticker.return_value = expected_internal_ticker

        result_ticker = await hyperliquid_market_data_service.get_ticker(symbol_to_find)

        hyperliquid_market_data_service.get_all_asset_contexts_raw.assert_called_once_with()
        mock_hl_mapper.map_raw_ctx_to_ticker.assert_called_once_with(mock_raw_asset_ctx_btc)
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
        mock_hl_mapper: MagicMock,
    ) -> None:
        """Test get_funding_rate successfully retrieves and processes funding rate data."""
        symbol_to_find = "ETH"
        current_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)

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
            funding="0.0002",  # Target funding rate
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

        expected_internal_funding_rate = FundingRate(
            symbol=symbol_to_find,
            funding_rate=Decimal("0.0002"),
            timestamp=current_time,
            mark_price=Decimal("3000.0"),
        )

        # Use the injected mock_hl_mapper
        mock_hl_mapper.map_raw_ctx_to_funding_rate.return_value = expected_internal_funding_rate

        # Patch datetime.now to control the timestamp
        with patch(
            "cyberdelta.apis.hyperliquid.hl_mapper.datetime",
            new=MagicMock(datetime=MagicMock(now=MagicMock(side_effect=lambda: current_time))),
        ):
            result_funding_rate = await hyperliquid_market_data_service.get_funding_rate(
                symbol_to_find
            )

        hyperliquid_market_data_service.get_all_asset_contexts_raw.assert_called_once()
        # Assert call on the injected mock_hl_mapper
        mock_hl_mapper.map_raw_ctx_to_funding_rate.assert_called_once_with(mock_raw_asset_ctx_eth)
        assert result_funding_rate == expected_internal_funding_rate

    @pytest.mark.asyncio
    async def test_get_order_book_success(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
        mock_hl_mapper: MagicMock,
    ) -> None:
        """Test get_order_book successfully retrieves and processes order book data."""
        symbol_to_find = "BTC"

        # Setup for the request payload that build_l2_book_payload would return
        # This should be a HyperliquidRawL2BookRequestPayload instance or its mock dump
        mock_l2_book_request_payload_model = MagicMock(spec=HyperliquidRawL2BookRequestPayload)
        expected_data_dict = {"type": "l2Book", "coin": symbol_to_find}
        mock_l2_book_request_payload_model.model_dump.return_value = expected_data_dict
        mock_hl_request_builder.build_l2_book_request_payload.return_value = (
            mock_l2_book_request_payload_model
        )

        mock_raw_response_content: RawJsonResponse = {
            "levels": [[], []],
            "time": 1234567890,
        }  # Raw L2Book structure
        mock_validated_response = HyperliquidRawL2Book(
            coin=symbol_to_find, time=1234567890, levels=[[], []]
        )
        expected_internal_order_book = OrderBook(
            symbol=symbol_to_find,
            bids=[],
            asks=[],
            timestamp=datetime.fromtimestamp(1234567890 / 1000, tz=UTC),
        )

        mock_headers: dict[str, str] = {}
        mock_http_client_requester.return_value = (
            mock_raw_response_content,
            200,
            mock_headers,
        )
        mock_hl_response_handler.handle_info_l2_book_response.return_value = mock_validated_response
        mock_hl_mapper.map_raw_order_book.return_value = expected_internal_order_book

        result_order_book = await hyperliquid_market_data_service.get_order_book(symbol_to_find)

        mock_hl_request_builder.build_l2_book_request_payload.assert_called_once_with(
            symbol=symbol_to_find
        )
        # Ensure the mocked model's dump was called
        mock_l2_book_request_payload_model.model_dump.assert_called_once_with(
            by_alias=True, exclude_none=True
        )
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint_path="/info",
            data=expected_data_dict,  # This comes from the model_dump of the L2BookRequestPayload
            is_info_endpoint=True,
        )
        mock_hl_response_handler.handle_info_l2_book_response.assert_called_once_with(
            mock_raw_response_content, symbol=symbol_to_find, status_code=200, headers=mock_headers
        )
        mock_hl_mapper.map_raw_order_book.assert_called_once_with(mock_validated_response)
        assert result_order_book == expected_internal_order_book

    @pytest.mark.asyncio
    async def test_get_order_book_http_client_returns_none(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
        mock_hl_mapper: MagicMock,
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
            is_info_endpoint=True,
        )
        mock_hl_response_handler.handle_info_l2_book_response.assert_not_called()
        mock_hl_mapper.map_raw_order_book.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_recent_trades_success(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
        mock_hl_mapper: MagicMock,
    ) -> None:
        """Test get_recent_trades successfully retrieves and processes recent trades data."""
        symbol_to_find = "ETH"

        # Request payload building
        mock_request_payload_dict = {"type": "recentTrades", "coin": symbol_to_find}
        mock_payload_model = MagicMock()
        mock_payload_model.model_dump.return_value = mock_request_payload_dict
        mock_hl_request_builder.build_recent_trades_request_payload.return_value = (
            mock_payload_model
        )

        # Raw trades as received from API
        mock_raw_trade_1 = HyperliquidRawPublicTrade(
            coin=symbol_to_find,
            side="B",
            px="3000.1",
            sz="0.5",
            time=1672531201000,  # ms
            hash="0xhash1",
        )
        mock_raw_trade_2 = HyperliquidRawPublicTrade(
            coin=symbol_to_find,
            side="A",  # Corrected from "S" to "A" for sell
            px="3000.0",
            sz="0.2",
            time=1672531202000,  # ms
            hash="0xhash2",
        )
        mock_raw_response_content: list[RawJsonResponse] = [
            mock_raw_trade_1.model_dump(),
            mock_raw_trade_2.model_dump(),
        ]

        # Validated raw trades (output of response_handler)
        mock_validated_response_from_handler: list[HyperliquidRawPublicTrade] = [
            mock_raw_trade_1,
            mock_raw_trade_2,
        ]

        # Expected internal Trade objects (output of mapper)
        expected_internal_trades = [
            Trade(
                id="0xhash1",
                symbol=symbol_to_find,
                executed_at=datetime.fromtimestamp(1672531201000 / 1000, tz=UTC),
                side=OrderSide.BUY,
                order_id="UNKNOWN_PUBLIC_TRADE",
                exchange="hyperliquid_test",  # Use the known exchange name from fixture
                price=Decimal("3000.1"),
                quantity=Decimal("0.5"),
                fee=Decimal("0"),
                fee_asset=None,
                is_maker=None,
            ),
            Trade(
                id="0xhash2",
                symbol=symbol_to_find,
                executed_at=datetime.fromtimestamp(1672531202000 / 1000, tz=UTC),
                side=OrderSide.SELL,
                order_id="UNKNOWN_PUBLIC_TRADE",
                exchange="hyperliquid_test",  # Use the known exchange name from fixture
                price=Decimal("3000.0"),
                quantity=Decimal("0.2"),
                fee=Decimal("0"),
                fee_asset=None,
                is_maker=None,
            ),
        ]

        # Mock HTTP client response
        mock_headers: dict[str, str] = {}
        mock_http_client_requester.return_value = (
            mock_raw_response_content,
            200,
            mock_headers,
        )
        # Mock response handler output
        mock_hl_response_handler.handle_info_recent_trades_response.return_value = (
            mock_validated_response_from_handler
        )
        # Mock mapper output
        mock_hl_mapper.transform_raw_public_trade_to_internal.side_effect = expected_internal_trades

        # Call the service method (no limit argument)
        result_trades = await hyperliquid_market_data_service.get_recent_trades(symbol_to_find)

        # Assertions
        mock_hl_request_builder.build_recent_trades_request_payload.assert_called_once_with(
            symbol=symbol_to_find
        )
        mock_payload_model.model_dump.assert_called_once_with(by_alias=True, exclude_none=True)
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint_path="/info",
            data=mock_request_payload_dict,
            is_info_endpoint=True,
        )
        mock_hl_response_handler.handle_info_recent_trades_response.assert_called_once_with(
            mock_raw_response_content, symbol=symbol_to_find, status_code=200, headers=mock_headers
        )
        # Mapper is called with validated raw trades, and no limit as service doesn't pass it.
        for raw_trade in mock_validated_response_from_handler:
            mock_hl_mapper.transform_raw_public_trade_to_internal.assert_any_call(raw_trade)
        assert result_trades == expected_internal_trades

    @pytest.mark.asyncio
    async def test_get_recent_trades_http_client_returns_none(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
        mock_hl_mapper: MagicMock,
    ) -> None:
        """Test get_recent_trades when HTTP client returns None content."""
        symbol = "ETH"
        mock_request_payload_model = MagicMock()
        mock_request_payload_dict = {"type": "recentTrades", "coin": symbol}

        mock_hl_request_builder.build_recent_trades_request_payload.return_value = (
            mock_request_payload_model
        )
        mock_request_payload_model.model_dump.return_value = mock_request_payload_dict
        mock_headers: dict[str, str] = {}
        mock_http_client_requester.return_value = (None, 200, mock_headers)

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
            is_info_endpoint=True,
        )
        mock_hl_response_handler.handle_info_recent_trades_response.assert_not_called()
        mock_hl_mapper.map_raw_trades.assert_not_called()

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.hyperliquid.services.hl_market_data_service.HyperliquidCandleMapper")
    async def test_get_market_data_success(
        self,
        mock_candle_mapper_class: MagicMock,
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

        mock_candle_mapper_instance = mock_candle_mapper_class.return_value

        mock_request_details = HyperliquidRawCandleRequestDetails(
            coin=symbol, interval=interval, startTime=start_time_ms, endTime=end_time_ms
        )
        # Explicitly provide type for HyperliquidRawCandleSnapshotRequestPayload
        mock_payload_from_builder = HyperliquidRawCandleSnapshotRequestPayload(
            type="candleSnapshot", req=mock_request_details
        )
        # Mock model_dump to return dict expected by http_client_requester

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

        mock_hl_request_builder.build_candle_snapshot_payload.return_value = (
            mock_payload_from_builder
        )
        mock_http_client_requester.return_value = (
            mock_raw_candle_data,
            200,
            MagicMock(),
        )
        mock_hl_response_handler.handle_info_candle_snapshot_response.return_value = (
            mock_validated_response
        )
        mock_candle_mapper_instance.map.return_value = expected_candles

        result_candles = await hyperliquid_market_data_service.get_market_data(
            symbol, interval, start_time_ms, end_time_ms
        )

        mock_hl_request_builder.build_candle_snapshot_payload.assert_called_once_with(
            symbol=symbol, timeframe=interval, start_time_ms=start_time_ms, end_time_ms=end_time_ms
        )
        mock_hl_response_handler.handle_info_candle_snapshot_response.assert_called_once_with(
            mock_raw_candle_data,
            symbol,
            interval,
            200,
            ANY,  # Use ANY for headers, consistent with service call.
            # If service passes specific mock, use that instead.
            # Assumes MagicMock() from mock_http_client_requester return value.
        )
        mock_candle_mapper_instance.map.assert_called_once_with(
            raw_snapshot=mock_validated_response, symbol=symbol, interval=interval
        )
        assert result_candles == expected_candles

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.hyperliquid.services.hl_market_data_service.HyperliquidCandleMapper")
    async def test_get_market_data_http_client_returns_none(
        self,
        mock_candle_mapper_class: MagicMock,
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

        mock_candle_mapper_instance = mock_candle_mapper_class.return_value

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
            await hyperliquid_market_data_service.get_market_data(
                symbol, interval, start_time_ms, end_time_ms
            )

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
        mock_candle_mapper_instance.map.assert_not_called()

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
        )
        mock_hl_response_handler.handle_info_meta_and_asset_ctxs_response.assert_called_once_with(
            mock_raw_response_content, status_code=mock_status_code, headers=mock_headers
        )
        assert result == mock_validated_response

    @pytest.mark.asyncio
    async def test_get_all_asset_contexts_raw_http_client_returns_none(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
    ) -> None:
        """Test get_all_asset_contexts_raw when HTTP client returns None content."""
        mock_payload_from_builder = HyperliquidRawMetaAndAssetCtxsRequestPayload(
            type="metaAndAssetCtxs"
        )
        expected_data_dict = mock_payload_from_builder.model_dump(by_alias=True, exclude_none=True)

        mock_hl_request_builder.build_info_request_payload.return_value = mock_payload_from_builder
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with pytest.raises(APIError) as exc_info:
            await hyperliquid_market_data_service.get_all_asset_contexts_raw()

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            "No content received from HTTP client for metaAndAssetCtxs." in exc_info.value.message
        )

        mock_hl_request_builder.build_info_request_payload.assert_called_once_with()
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint_path="/info",
            data=expected_data_dict,
            is_info_endpoint=True,
        )
        mock_hl_response_handler.handle_info_meta_and_asset_ctxs_response.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_success(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
        mocker: MagicMock,  # Added type hint for mocker
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
        mock_headers: dict[str, str] = {}
        mock_http_client_requester.return_value = (mock_raw_response_data, 200, mock_headers)
        mock_hl_response_handler.handle_historical_funding_rates_response.return_value = (
            mock_validated_raw_items
        )

        mock_transform_method = mocker.patch.object(
            hyperliquid_market_data_service._mapper,
            "transform_raw_funding_history_item_to_internal",
            side_effect=mock_internal_funding_rates,
        )

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
            raw_response_content=mock_raw_response_data, status_code=200, headers=mock_headers
        )
        assert mock_transform_method.call_count == len(mock_validated_raw_items)
        for raw_item in mock_validated_raw_items:
            mock_transform_method.assert_any_call(raw_item)
        assert result == mock_internal_funding_rates

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_http_client_returns_none(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
        mock_hl_mapper: MagicMock,
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
        assert (
            "No data received for historical funding rates for ETH, status: 200"
            in exc_info.value.message
        )
        mock_hl_request_builder.build_historical_funding_rates_payload.assert_called_once_with(
            symbol=symbol, start_time_ms=start_time_ms, end_time_ms=end_time_ms
        )
        mock_http_client_requester.assert_called_once_with(
            method="POST", endpoint_path="/info", data=mock_request_payload, is_info_endpoint=True
        )
        mock_hl_response_handler.handle_historical_funding_rates_response.assert_not_called()
        # Since the mapper method is static, direct check on mock_hl_mapper instance method
        # won't work
        # To check if HyperliquidMapper.transform_raw_funding_history_item_to_internal was called,
        # we would need to patch it directly if this test was for a success case involving mapping.
        # For a None response, the handler isn't called, so mapping isn't reached.
        # So, no specific assert_not_called for the static mapper method here is needed beyond
        # handler check.

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
        mock_headers: dict[str, str] = {}
        mock_http_client_requester.return_value = (mock_raw_response_data, 200, mock_headers)

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
            raw_response_content=mock_raw_response_data, status_code=200, headers=mock_headers
        )

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Test validation error" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_mapper_error(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
        mocker: MagicMock,
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

        mock_transform_method = mocker.patch.object(
            hyperliquid_market_data_service._mapper,
            "transform_raw_funding_history_item_to_internal",
            side_effect=ValueError("Test mapper error"),
        )
        with pytest.raises(APIError) as exc_info:
            await hyperliquid_market_data_service.get_historical_funding_rates(
                symbol, start_time_ms, end_time_ms
            )
            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            assert (
                "Processing historical funding rate data failed: ValueError('Test mapper error')"
                in exc_info.value.message
            )
            mock_transform_method.assert_called_once_with(mock_validated_raw_items[0])

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_api_error_from_handler(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
    ) -> None:
        """Test get_historical_funding_rates when handler raises APIError for non-200 status."""
        symbol = "ETH"
        start_time_ms = 1678886400000
        end_time_ms = 1678890000000
        mock_request_payload = {
            "type": "fundingHistory",
            "coin": symbol,
            "startTime": start_time_ms,
            "endTime": end_time_ms,
        }
        # Simulate some raw response content, though it might not be fully
        # processed by handler in error case
        mock_raw_response_data: list[RawJsonResponse] = [{"error": "true"}]
        mock_status_code = 400  # Example error status
        mock_headers = MagicMock()

        mock_hl_request_builder.build_historical_funding_rates_payload.return_value = (
            mock_request_payload
        )
        mock_http_client_requester.return_value = (
            mock_raw_response_data,
            mock_status_code,
            mock_headers,
        )

        # Configure response_handler to raise APIError
        expected_api_error = APIError(
            message="Simulated API error from handler",
            code=APIErrorCode.EXCHANGE_SPECIFIC.value,
            http_status=mock_status_code,
            exchange_message=str(mock_raw_response_data),
        )
        mock_hl_response_handler.handle_historical_funding_rates_response.side_effect = (
            expected_api_error
        )

        with pytest.raises(APIError) as exc_info:
            await hyperliquid_market_data_service.get_historical_funding_rates(
                symbol, start_time_ms, end_time_ms
            )

        assert exc_info.value.code == expected_api_error.code
        assert exc_info.value.message == expected_api_error.message
        assert exc_info.value.http_status == mock_status_code

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
            raw_response_content=mock_raw_response_data,
            status_code=mock_status_code,
            headers=mock_headers,
        )

    # Add more tests for other methods: get_funding_rate, get_order_book, etc.
