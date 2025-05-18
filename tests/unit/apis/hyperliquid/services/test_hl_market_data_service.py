"""
Unit tests for the HyperliquidMarketDataService.
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cyberdelta.apis.hyperliquid.hl_mapper import (
    HyperliquidCandleMapper,
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
from cyberdelta.core.models.enums import OrderSide  # Added OrderSide import
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
def hyperliquid_market_data_service(
    mock_http_client_requester: AsyncMock,
    mock_hl_request_builder: MagicMock,
    mock_hl_response_handler: MagicMock,
) -> HyperliquidMarketDataService:
    return HyperliquidMarketDataService(
        http_client_requester=mock_http_client_requester,
        request_builder=mock_hl_request_builder,
        response_handler=mock_hl_response_handler,
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

        mock_hl_request_builder.build_info_request_payload.return_value = mock_payload_from_builder
        mock_http_client_requester.request.return_value = (
            mock_raw_response_content,
            200,
            MagicMock(),
        )
        # The response handler in the SUT is called with the raw_response_content
        mock_hl_response_handler.handle_info_meta_and_asset_ctxs_response.return_value = (
            mock_validated_response
        )

        result = await hyperliquid_market_data_service.get_all_asset_contexts_raw()

        # Check that the correct request builder method was called
        mock_hl_request_builder.build_info_request_payload.assert_called_once_with()
        # Check that HttpClient was called with data from the builder
        mock_http_client_requester.request.assert_called_once_with(
            method="POST",
            endpoint_path="/info",
            data=expected_data_dict,  # Assert with the dumped model dict
            rate_limiter_service=None,
        )
        # Check that the response handler was called correctly
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
        mock_ticker_timestamp = datetime.now(UTC)

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
            timestamp=mock_ticker_timestamp,
        )

        with patch.object(
            HyperliquidMapper, "map_raw_ctx_to_ticker", return_value=expected_internal_ticker
        ) as mock_mapper_method:
            result_ticker = await hyperliquid_market_data_service.get_ticker(symbol_to_find)

            hyperliquid_market_data_service.get_all_asset_contexts_raw.assert_called_once()
            mock_mapper_method.assert_called_once_with(mock_raw_asset_ctx_btc)
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
            funding_rate=Decimal("0.0002"),  # This field exists on FundingRate
            timestamp=mock_funding_timestamp,
        )

        with patch.object(
            HyperliquidMapper,
            "map_raw_ctx_to_funding_rate",
            return_value=expected_internal_funding_rate,
        ) as mock_mapper_method:
            result_funding_rate = await hyperliquid_market_data_service.get_funding_rate(
                symbol_to_find
            )

            hyperliquid_market_data_service.get_all_asset_contexts_raw.assert_called_once()
            mock_mapper_method.assert_called_once_with(mock_raw_asset_ctx_eth)
            # Ensure we are comparing the correct attribute and type
            assert result_funding_rate == expected_internal_funding_rate
            if result_funding_rate:  # mypy check for Optional
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
        mock_http_client_requester.request.return_value = (
            mock_raw_book_data_content,
            200,
            MagicMock(),
        )
        # Response handler returns the validated RAW model
        mock_hl_response_handler.handle_info_l2_book_response.return_value = mock_validated_raw_book

        # Patch the mapper method used by the service
        with patch.object(
            HyperliquidMapper,
            "map_raw_l2_book_to_order_book",
            return_value=expected_internal_order_book,
        ) as mock_mapper_method:
            result = await hyperliquid_market_data_service.get_order_book(symbol)

            mock_hl_request_builder.build_l2_book_request_payload.assert_called_once_with(
                symbol=symbol
            )
            mock_request_payload_model.model_dump.assert_called_once_with(
                by_alias=True, exclude_none=True
            )
            mock_http_client_requester.request.assert_called_once_with(
                method="POST",
                endpoint_path="/info",
                data=mock_request_payload_dict,
                rate_limiter_service=None,
            )
            mock_hl_response_handler.handle_info_l2_book_response.assert_called_once_with(
                mock_raw_book_data_content,
                symbol=symbol,  # Response handler gets raw content
            )
            # Service method should call the mapper with the validated raw book
            mock_mapper_method.assert_called_once_with(mock_validated_raw_book)
            assert result == expected_internal_order_book

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

        # Patch the mapper method used by the service in a loop
        # The service calls transform_raw_public_trade_to_internal for each raw trade
        with patch.object(
            HyperliquidMapper,
            "transform_raw_public_trade_to_internal",
            side_effect=expected_internal_trades,
        ) as mock_mapper_method:  # side_effect will return items from list one by one
            result = await hyperliquid_market_data_service.get_recent_trades(symbol)

            mock_hl_request_builder.build_recent_trades_request_payload.assert_called_once_with(
                symbol=symbol
            )
            mock_request_payload_model.model_dump.assert_called_once_with(
                by_alias=True, exclude_none=True
            )
            mock_http_client_requester.request.assert_called_once_with(
                method="POST",
                endpoint_path="/info",
                data=mock_request_payload_dict,
                rate_limiter_service=None,
            )
            mock_hl_response_handler.handle_info_recent_trades_response.assert_called_once_with(
                mock_raw_trades_data_content,
                symbol=symbol,  # Response handler gets raw list
            )
            # Service method should have called the mapper with each validated raw trade
            assert mock_mapper_method.call_count == len(mock_validated_raw_trades)
            if mock_validated_raw_trades:  # Ensure it was called if list not empty
                mock_mapper_method.assert_any_call(mock_validated_raw_trades[0])
            assert result == expected_internal_trades

    @pytest.mark.asyncio
    async def test_get_market_data_success(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
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

        # This is the raw data from HTTP client
        mock_raw_response_content_candles: RawJsonResponse = {  # Renamed for clarity
            "t": [start_time_ms],
            "o": ["1200.0"],
            "h": ["1250.0"],
            "l": ["1190.0"],
            "c": ["1240.0"],
            "v": ["1000.0"],
            "s": "ok",
        }

        # This is what the response_handler returns
        mock_validated_raw_snapshot = HyperliquidRawCandleSnapshot(
            t=[start_time_ms],
            o=["1200.0"],
            h=["1250.0"],
            l=["1190.0"],
            c=["1240.0"],
            v=["1000.0"],
            s="ok",
        )

        # This is what the mapper (and thus the service) should return
        expected_internal_candles = [
            Candle(
                open_time=datetime.fromtimestamp(start_time_ms / 1000, UTC),
                open=Decimal("1200.0"),
                high=Decimal("1250.0"),
                low=Decimal("1190.0"),
                close=Decimal("1240.0"),
                volume=Decimal("1000.0"),
                symbol=symbol,
                interval=interval,
            )
        ]

        mock_hl_request_builder.build_candle_snapshot_payload.return_value = mock_payload_model
        mock_http_client_requester.request.return_value = (
            mock_raw_response_content_candles,
            200,
            MagicMock(),
        )
        # Response handler returns the validated RAW snapshot
        mock_hl_response_handler.handle_info_candle_snapshot_response.return_value = (
            mock_validated_raw_snapshot
        )

        # Patch the mapper method used by the service
        with patch.object(
            HyperliquidCandleMapper,
            "map_raw_candle_snapshot_to_candles",
            return_value=expected_internal_candles,
        ) as mock_mapper_method:
            result = await hyperliquid_market_data_service.get_market_data(
                symbol, interval, start_time_ms, end_time_ms
            )

            mock_hl_request_builder.build_candle_snapshot_payload.assert_called_once_with(
                symbol=symbol,
                timeframe=interval,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )
            mock_http_client_requester.request.assert_called_once_with(
                method="POST",
                endpoint_path="/info",
                data=mock_request_payload_data,
                rate_limiter_service=None,
            )
            mock_hl_response_handler.handle_info_candle_snapshot_response.assert_called_once_with(
                raw_response_content=mock_raw_response_content_candles,  # Corrected arg name
                symbol=symbol,
                interval=interval,
            )
            # Service method should call the mapper with the validated raw snapshot
            mock_mapper_method.assert_called_once_with(
                mock_validated_raw_snapshot, symbol, interval
            )
            assert result == expected_internal_candles

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

    # Add more tests for other methods: get_funding_rate, get_order_book, etc.
