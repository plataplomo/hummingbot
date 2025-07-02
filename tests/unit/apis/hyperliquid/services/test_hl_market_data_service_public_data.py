"""Unit tests for HyperliquidMarketDataService public data operations.

Tests the public market data methods including get_ticker, get_order_book, and get_recent_trades.
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.hyperliquid.hl_response_handler import RawJsonResponse
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetCtx,
    HyperliquidRawAssetDefinition,
    HyperliquidRawMetaAndAssetCtxsRequestPayload,
    HyperliquidRawMetaAndAssetCtxsResponse,
    HyperliquidRawMetaResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import (
    HyperliquidRawL2Book,
    HyperliquidRawL2BookRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import HyperliquidRawPublicTrade
from cyberdelta.apis.hyperliquid.services.hl_market_data_service import HyperliquidMarketDataService
from cyberdelta.apis.models.service_args_models import GetL2BookArgs, GetRecentTradesArgs
from cyberdelta.core.models.enums import OrderSide
from cyberdelta.core.models.market import OrderBook, Ticker, Trade


# Unit tests for HyperliquidMarketDataService (moved from mislabeled integration tests)
# These are unit tests because they mock all dependencies and test individual methods

# Import fixtures from the shared conftest
pytest_plugins = ["tests.unit.apis.hyperliquid.services.conftest_market_data"]


def create_asset_ctx(
    name: str,
    funding: str,
    mark_px: str,
    prev_day_px: str,
    day_ntl_vlm: str,
    impact_px: str | None = None,
    open_interest: str | None = None,
    premium: str | None = None,
    oracle_px: str | None = None,
    mid_px: str | None = None,
    impact_pxs: list[str] | None = None,
    day_base_vlm: str | None = None,
) -> HyperliquidRawAssetCtx:
    """Helper to create HyperliquidRawAssetCtx with defaults for required fields."""
    return HyperliquidRawAssetCtx(
        name=name,
        funding=funding,
        markPx=mark_px,
        prevDayPx=prev_day_px,
        dayNtlVlm=day_ntl_vlm,
        impactPx=impact_px,
        openInterest=open_interest or "1000000.00",
        premium=premium or "0.0001",
        oraclePx=oracle_px or mark_px,  # Default to mark price
        midPx=mid_px or mark_px,  # Default to mark price
        impactPxs=impact_pxs or [str(float(mark_px) - 5), str(float(mark_px) + 5)],
        dayBaseVlm=day_base_vlm or str(float(day_ntl_vlm) / float(mark_px)),
    )


class TestHyperliquidMarketDataServicePublicData:
    """Tests for the HyperliquidMarketDataService public market data functionality."""

    # =============================================================================
    # INPUT VALIDATION TESTS (NEW - ITERATION 2)
    # =============================================================================

    @pytest.mark.asyncio
    async def test_get_ticker_empty_symbol_validation(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
    ) -> None:
        """Test get_ticker raises ValueError for empty symbol (direct validation error)."""
        with pytest.raises(ValueError) as exc_info:
            await hyperliquid_market_data_service.get_ticker("")

        # The service raises ValueError directly for input validation
        assert "'symbol' must be a non-empty string" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_ticker_none_symbol_validation(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
    ) -> None:
        """Test get_ticker raises ValueError for None symbol (direct validation error)."""
        # JUSTIFICATION FOR CAST:
        # This test intentionally passes None to the get_ticker method to verify that the method
        # properly validates input types and raises ValueError. The type checker correctly
        # identifies this as a type error, but we need to test the runtime behavior when
        # invalid types are passed. Alternative typing solutions like Union types would not
        # work here as we specifically want to test the error case.
        # The developer is certain this cast is safe because the test expects a ValueError.
        none_symbol = cast("str", None)
        # Runtime verification: none_symbol is None at this point

        with pytest.raises(ValueError) as exc_info:
            await hyperliquid_market_data_service.get_ticker(none_symbol)

        # The service raises ValueError directly for input validation
        assert "'symbol' must be a non-empty string" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_ticker_whitespace_symbol_validation(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
    ) -> None:
        """Test get_ticker raises ValueError for whitespace-only symbol after strip()."""
        # The service now properly validates whitespace-only symbols and raises ValueError
        with pytest.raises(ValueError) as exc_info:
            await hyperliquid_market_data_service.get_ticker("   ")

        assert "[get_ticker] 'symbol' cannot be empty or whitespace only." in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_order_book_empty_symbol_validation(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
    ) -> None:
        """Test get_order_book raises ValueError for empty symbol."""
        with pytest.raises(ValueError) as exc_info:
            await hyperliquid_market_data_service.get_order_book("")

        # The service raises ValueError directly for input validation
        assert "'symbol' must be a non-empty string" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_order_book_none_symbol_validation(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
    ) -> None:
        """Test get_order_book raises ValueError for None symbol."""
        # JUSTIFICATION FOR CAST:
        # This test intentionally passes None to the get_order_book method to verify that the method
        # properly validates input types and raises ValueError. The type checker correctly
        # identifies this as a type error, but we need to test the runtime behavior when
        # invalid types are passed. Alternative typing solutions like Union types would not
        # work here as we specifically want to test the error case.
        # The developer is certain this cast is safe because the test expects a ValueError.
        none_symbol = cast("str", None)
        # Runtime verification: none_symbol is None at this point

        with pytest.raises(ValueError) as exc_info:
            await hyperliquid_market_data_service.get_order_book(none_symbol)

        # The service raises ValueError directly for input validation
        assert "'symbol' must be a non-empty string" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_recent_trades_empty_symbol_validation(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
    ) -> None:
        """Test get_recent_trades raises ValueError for empty symbol."""
        with pytest.raises(ValueError) as exc_info:
            await hyperliquid_market_data_service.get_recent_trades("")

        # The service raises ValueError directly for input validation
        assert "'symbol' must be a non-empty string" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_recent_trades_none_symbol_validation(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
    ) -> None:
        """Test get_recent_trades raises ValueError for None symbol."""
        # JUSTIFICATION FOR CAST:
        # This test intentionally passes None to the get_recent_trades method to verify that
        # the method
        # properly validates input types and raises ValueError. The type checker correctly
        # identifies this as a type error, but we need to test the runtime behavior when
        # invalid types are passed. Alternative typing solutions like Union types would not
        # work here as we specifically want to test the error case.
        # The developer is certain this cast is safe because the test expects a ValueError.
        none_symbol = cast("str", None)
        # Runtime verification: none_symbol is None at this point

        with pytest.raises(ValueError) as exc_info:
            await hyperliquid_market_data_service.get_recent_trades(none_symbol)

        # The service raises ValueError directly for input validation
        assert "'symbol' must be a non-empty string" in str(exc_info.value)

    # =============================================================================
    # EXISTING TESTS (Updated tests below)
    # =============================================================================

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
            type="metaAndAssetCtxs",
        )
        expected_data_dict = mock_payload_from_builder.model_dump(by_alias=True, exclude_none=True)

        mock_raw_response_content: list[RawJsonResponse] = [
            {"universe": []},
            [],
        ]
        mock_validated_response = HyperliquidRawMetaAndAssetCtxsResponse.model_validate([
            {"universe": [], "marginTables": None},
            [],
        ])

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
            endpoint="/info",
            data=expected_data_dict,
            is_signed=False,
            endpoint_group="public",
            request_weight=1,
        )
        mock_hl_response_handler.handle_info_meta_and_asset_ctxs_response.assert_called_once_with(
            mock_raw_response_content,
            status_code=200,
            headers=mock_headers,
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
            type="metaAndAssetCtxs",
        )
        expected_data_dict = mock_payload_from_builder.model_dump(by_alias=True, exclude_none=True)

        mock_hl_request_builder.build_info_request_payload.return_value = mock_payload_from_builder
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with pytest.raises(APIError) as exc_info:
            await hyperliquid_market_data_service.get_all_asset_contexts_raw()

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "No data received for metaAndAssetCtxs, status: 200" in exc_info.value.message

        mock_hl_request_builder.build_info_request_payload.assert_called_once_with()
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint="/info",
            data=expected_data_dict,
            is_signed=False,
            endpoint_group="public",
            request_weight=1,
        )
        mock_hl_response_handler.handle_info_meta_and_asset_ctxs_response.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_ticker_success(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_hl_mapper: MagicMock,
    ) -> None:
        """Test get_ticker successfully retrieves and processes ticker data."""
        symbol_to_find = "BTC"

        mock_raw_asset_ctx_btc = create_asset_ctx(
            name="BTC",
            funding="0.0001",
            mark_px="50000.0",
            prev_day_px="49000.0",
            day_ntl_vlm="1000",
            impact_px="50001.0",
        )
        mock_raw_asset_ctx_eth = create_asset_ctx(
            name="ETH",
            funding="0.0002",
            mark_px="3000.0",
            prev_day_px="2900.0",
            day_ntl_vlm="500",
            impact_px="3001.0",
        )
        mock_meta_response = HyperliquidRawMetaResponse(
            universe=[
                HyperliquidRawAssetDefinition(
                    name="BTC",
                    szDecimals=5,
                    maxLeverage=100,
                    onlyIsolated=False,
                    marginTableId=None,
                    isDelisted=None,
                ),
                HyperliquidRawAssetDefinition(
                    name="ETH",
                    szDecimals=5,
                    maxLeverage=100,
                    onlyIsolated=False,
                    marginTableId=None,
                    isDelisted=None,
                ),
            ],
            marginTables=None,
        )
        mock_all_contexts_response = HyperliquidRawMetaAndAssetCtxsResponse.model_validate([
            mock_meta_response.model_dump(by_alias=True),
            [
                mock_raw_asset_ctx_btc.model_dump(by_alias=True),
                mock_raw_asset_ctx_eth.model_dump(by_alias=True),
            ],
        ])

        expected_internal_ticker = Ticker(
            symbol=symbol_to_find,
            price=Decimal("50000.0"),
            bid=Decimal("50000.0"),
            ask=Decimal("50000.0"),
            volume=Decimal(1000),
            timestamp=datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC),
        )

        mock_hl_mapper.transform_raw_asset_ctx_to_ticker.return_value = expected_internal_ticker

        # Mock the get_all_asset_contexts_raw method using patch
        with patch.object(
            hyperliquid_market_data_service,
            "get_all_asset_contexts_raw",
            new=AsyncMock(return_value=mock_all_contexts_response),
        ) as mock_get_contexts:
            result_ticker = await hyperliquid_market_data_service.get_ticker(symbol_to_find)

            mock_get_contexts.assert_called_once_with()
            mock_hl_mapper.transform_raw_asset_ctx_to_ticker.assert_called_once_with(
                mock_raw_asset_ctx_btc,
            )
            assert result_ticker == expected_internal_ticker

    @pytest.mark.asyncio
    async def test_get_ticker_not_found(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
    ) -> None:
        """Test get_ticker returns None when symbol is not found."""
        symbol = "UNKNOWN"
        mock_meta_response = HyperliquidRawMetaResponse(universe=[], marginTables=None)
        mock_all_contexts_response = HyperliquidRawMetaAndAssetCtxsResponse.model_validate([
            mock_meta_response.model_dump(by_alias=True),
            [],
        ])

        with patch.object(
            hyperliquid_market_data_service,
            "get_all_asset_contexts_raw",
            new=AsyncMock(return_value=mock_all_contexts_response),
        ):
            result = await hyperliquid_market_data_service.get_ticker(symbol)
            assert result is None

    @pytest.mark.asyncio
    async def test_get_ticker_response_handler_validation_error(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
    ) -> None:
        """Test get_ticker handles ResponseHandler ValidationError gracefully."""
        symbol = "BTC"

        # Mock request building
        mock_payload_model = MagicMock()
        mock_payload_dict = {"type": "metaAndAssetCtxs"}
        mock_payload_model.model_dump.return_value = mock_payload_dict
        mock_hl_request_builder.build_info_request_payload.return_value = mock_payload_model

        # Mock HTTP response with malformed data that would cause Pydantic ValidationError
        mock_malformed_response = [
            {"universe": "invalid_type_should_be_list"},  # Wrong type
            {"invalid_structure": True},  # Missing expected fields
        ]
        mock_http_client_requester.return_value = (mock_malformed_response, 200, {})

        # Mock response handler to raise APIError wrapping ValidationError
        validation_error = ValidationError.from_exception_data(
            title="HyperliquidRawMetaAndAssetCtxsResponse",
            line_errors=[],
        )
        mock_hl_response_handler.handle_info_meta_and_asset_ctxs_response.side_effect = APIError(
            message="Invalid response structure for metaAndAssetCtxs",
            code=APIErrorCode.INVALID_RESPONSE.value,
            original_exception=validation_error,
        )

        # The service should propagate the APIError from response handler
        with pytest.raises(APIError) as exc_info:
            await hyperliquid_market_data_service.get_ticker(symbol)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Invalid response structure" in exc_info.value.message
        mock_hl_response_handler.handle_info_meta_and_asset_ctxs_response.assert_called_once()

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
            coin=symbol_to_find,
            time=1234567890,
            levels=[[], []],
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
        mock_hl_mapper.transform_raw_order_book_to_internal.return_value = (
            expected_internal_order_book
        )

        result_order_book = await hyperliquid_market_data_service.get_order_book(symbol_to_find)

        mock_hl_request_builder.build_l2_book_request_payload.assert_called_once_with(
            GetL2BookArgs(symbol=symbol_to_find),
        )
        # Ensure the mocked model's dump was called
        mock_l2_book_request_payload_model.model_dump.assert_called_once_with(
            by_alias=True,
            exclude_none=True,
        )
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint="/info",
            data=expected_data_dict,  # This comes from the model_dump of the L2BookRequestPayload
            is_signed=False,
            endpoint_group="public",
            request_weight=1,
        )
        mock_hl_response_handler.handle_info_l2_book_response.assert_called_once_with(
            mock_raw_response_content,
            symbol=symbol_to_find,
            status_code=200,
            headers=mock_headers,
        )
        mock_hl_mapper.transform_raw_order_book_to_internal.assert_called_once_with(
            mock_validated_response,
        )
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
        assert "Empty l2Book data received" in exc_info.value.message
        mock_hl_request_builder.build_l2_book_request_payload.assert_called_once_with(
            GetL2BookArgs(symbol=symbol),
        )
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint="/info",
            data=mock_request_payload_dict,
            is_signed=False,
            endpoint_group="public",
            request_weight=1,
        )
        mock_hl_response_handler.handle_info_l2_book_response.assert_not_called()
        mock_hl_mapper.transform_raw_order_book_to_internal.assert_not_called()

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
            tid=1,
            users=["0xuser1", "0xuser2"],
        )
        mock_raw_trade_2 = HyperliquidRawPublicTrade(
            coin=symbol_to_find,
            side="A",  # Corrected from "S" to "A" for sell
            px="3000.0",
            sz="0.2",
            time=1672531202000,  # ms
            hash="0xhash2",
            tid=2,
            users=["0xuser3", "0xuser4"],
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
                fee=Decimal(0),
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
                fee=Decimal(0),
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
            GetRecentTradesArgs(symbol=symbol_to_find),
        )
        mock_payload_model.model_dump.assert_called_once_with(by_alias=True, exclude_none=True)
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint="/info",
            data=mock_request_payload_dict,
            is_signed=False,
            endpoint_group="public",
            request_weight=1,
        )
        mock_hl_response_handler.handle_info_recent_trades_response.assert_called_once_with(
            mock_raw_response_content,
            symbol=symbol_to_find,
            status_code=200,
            headers=mock_headers,
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
            GetRecentTradesArgs(symbol=symbol),
        )
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint="/info",
            data=mock_request_payload_dict,
            is_signed=False,
            endpoint_group="public",
            request_weight=1,
        )
        mock_hl_response_handler.handle_info_recent_trades_response.assert_not_called()
        mock_hl_mapper.transform_raw_public_trade_to_internal.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_recent_trades_empty_successful_response(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
        mock_hl_mapper: MagicMock,
    ) -> None:
        """Test get_recent_trades handles empty but successful response correctly."""
        symbol = "BTC"

        # Setup mocks for request building
        mock_payload_model = MagicMock()
        mock_payload_dict = {"type": "recentTrades", "coin": symbol}
        mock_payload_model.model_dump.return_value = mock_payload_dict
        mock_hl_request_builder.build_recent_trades_request_payload.return_value = (
            mock_payload_model
        )

        # Mock HTTP response with empty list (but successful)
        mock_empty_response: list[RawJsonResponse] = []
        mock_http_client_requester.return_value = (mock_empty_response, 200, {})

        # Mock response handler to return empty validated list
        mock_hl_response_handler.handle_info_recent_trades_response.return_value = []

        result = await hyperliquid_market_data_service.get_recent_trades(symbol)

        assert result == []
        mock_hl_response_handler.handle_info_recent_trades_response.assert_called_once_with(
            mock_empty_response,
            symbol=symbol,
            status_code=200,
            headers={},
        )
        # Mapper should not be called with empty list
        mock_hl_mapper.transform_raw_public_trade_to_internal.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_ticker_with_none_symbol_input(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
    ) -> None:
        """Test get_ticker with None symbol input raises ValueError."""
        # JUSTIFICATION FOR CAST:
        # This test intentionally passes None to the get_ticker method to verify that the method
        # properly validates input types and raises ValueError. The type checker correctly
        # identifies this as a type error, but we need to test the runtime behavior when
        # invalid types are passed. Alternative typing solutions like Union types would not
        # work here as we specifically want to test the error case.
        # The developer is certain this cast is safe because the test expects a ValueError.
        none_symbol = cast("str", None)
        # Runtime verification: none_symbol is None at this point

        with pytest.raises(ValueError) as exc_info:
            await hyperliquid_market_data_service.get_ticker(none_symbol)

        assert "'symbol' must be a non-empty string" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_ticker_with_empty_string_symbol(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
    ) -> None:
        """Test get_ticker with empty string symbol raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            await hyperliquid_market_data_service.get_ticker("")

        assert "'symbol' must be a non-empty string" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_order_book_response_handler_raises_api_error(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
    ) -> None:
        """Test get_order_book propagates APIError from response handler correctly."""
        symbol = "ETH"

        # Setup request building mocks
        mock_payload_model = MagicMock()
        mock_payload_dict = {"type": "l2Book", "coin": symbol}
        mock_payload_model.model_dump.return_value = mock_payload_dict
        mock_hl_request_builder.build_l2_book_request_payload.return_value = mock_payload_model

        # Mock HTTP response
        mock_raw_response = {"malformed": "data"}
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})

        # Mock response handler to raise APIError
        mock_hl_response_handler.handle_info_l2_book_response.side_effect = APIError(
            message="L2Book response validation failed",
            code=APIErrorCode.INVALID_RESPONSE.value,
            http_status=200,
        )

        with pytest.raises(APIError) as exc_info:
            await hyperliquid_market_data_service.get_order_book(symbol)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "L2Book response validation failed" in exc_info.value.message
        mock_hl_response_handler.handle_info_l2_book_response.assert_called_once_with(
            mock_raw_response,
            symbol=symbol,
            status_code=200,
            headers={},
        )

    @pytest.mark.asyncio
    async def test_get_ticker_rate_limited_error_propagation(
        self,
        hyperliquid_market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
    ) -> None:
        """Test get_ticker propagates RATE_LIMITED error correctly."""
        symbol = "BTC"

        # Setup basic mocks
        mock_payload_model = MagicMock()
        mock_hl_request_builder.build_info_request_payload.return_value = mock_payload_model
        mock_payload_model.model_dump.return_value = {"type": "metaAndAssetCtxs"}

        # Mock HTTP client to raise RATE_LIMITED APIError directly
        mock_http_client_requester.side_effect = APIError(
            message="Rate limit exceeded",
            code=APIErrorCode.RATE_LIMITED.value,
            http_status=429,
            exchange_message="rate limited",
        )

        # Response handler won't be called since HTTP client raises error

        # The service should propagate the APIError from response handler
        with pytest.raises(APIError) as exc_info:
            await hyperliquid_market_data_service.get_ticker(symbol)

        assert exc_info.value.code == APIErrorCode.RATE_LIMITED.value
        assert exc_info.value.http_status == 429
        assert "Rate limit exceeded" in exc_info.value.message
