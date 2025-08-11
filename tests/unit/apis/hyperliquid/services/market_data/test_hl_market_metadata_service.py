"""Unit tests for Hyperliquid Market Metadata Service.

Tests cover all methods of the HyperliquidMarketMetadataService including:
- Market listings retrieval for all assets
- Individual market metadata lookup
- Symbol validation and error handling
- Asset context transformation to market models
"""

from __future__ import annotations

from decimal import Decimal
from typing import cast
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.exceptions.market_data_service import SymbolNotFoundError
from cyberdelta.apis.hyperliquid.hl_response_handler import HyperliquidResponseHandler
from cyberdelta.apis.hyperliquid.mappers.market_data.hl_market_metadata_mapper import (
    HyperliquidMarketMetadataMapper,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetCtx,
    HyperliquidRawAssetDefinition,
    HyperliquidRawMetaAndAssetCtxsResponse,
)
from cyberdelta.apis.hyperliquid.request_builders.hl_market_data_request_builder import (
    HyperliquidMarketDataRequestBuilder,
)
from cyberdelta.apis.hyperliquid.services.market_data.hl_market_metadata_service import (
    HyperliquidMarketMetadataService,
)
from cyberdelta.apis.models.service_args.market_data import GetMarketArgs, GetMarketsArgs
from cyberdelta.enums import ExchangeName
from cyberdelta.models.market import Market
from cyberdelta.symbols import exchanges
from tests.common_symbols import BTC_HL, ETH_HL, SOL_HL


@pytest.fixture
def mock_http_requester() -> AsyncMock:
    """Create a mock HTTP requester.

    Returns:
        AsyncMock: A mock instance of the HTTP requester.
    """
    return AsyncMock()


@pytest.fixture
def mock_request_builder() -> Mock:
    """Create a mock request builder.

    Returns:
        Mock: A mock instance of HyperliquidMarketDataRequestBuilder.
    """
    return MagicMock(spec=HyperliquidMarketDataRequestBuilder)


@pytest.fixture
def mock_response_handler() -> Mock:
    """Create a mock response handler.

    Returns:
        Mock: A mock instance of HyperliquidResponseHandler.
    """
    return MagicMock(spec=HyperliquidResponseHandler)


@pytest.fixture
def mock_mapper() -> Mock:
    """Create a mock data mapper.

    Returns:
        Mock: A mock instance of HyperliquidMarketMetadataMapper.
    """
    return MagicMock(spec=HyperliquidMarketMetadataMapper)


@pytest.fixture
def market_metadata_service(
    mock_http_requester: AsyncMock,
    mock_request_builder: Mock,
    mock_response_handler: Mock,
    mock_mapper: Mock,
) -> HyperliquidMarketMetadataService:
    """Create a market metadata service instance with mocks.

    Returns:
        HyperliquidMarketMetadataService: Service instance configured with mock dependencies.
    """
    return HyperliquidMarketMetadataService(
        http_client_requester=mock_http_requester,
        request_builder=mock_request_builder,
        response_handler=mock_response_handler,
        mapper=mock_mapper,
        exchange_name=ExchangeName.HYPERLIQUID,
    )


@pytest.fixture
def mock_asset_definitions() -> list[HyperliquidRawAssetDefinition]:
    """Create mock asset definitions.

    Returns:
        list[HyperliquidRawAssetDefinition]: A list of mock asset definitions for ETH, BTC, SOL.
    """
    return [
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
        HyperliquidRawAssetDefinition(
            name="SOL",
            szDecimals=3,
            maxLeverage=20,
            marginTableId=None,
            isDelisted=None,
            onlyIsolated=True,
        ),
    ]


@pytest.fixture
def mock_asset_ctxs() -> list[HyperliquidRawAssetCtx]:
    """Create mock asset contexts.

    Returns:
        list[HyperliquidRawAssetCtx]: A list of mock asset contexts with market data.
    """
    return [
        HyperliquidRawAssetCtx(
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
        ),
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
        HyperliquidRawAssetCtx(
            funding="0.0003",
            markPx="150.00",
            openInterest="500000.00",
            oraclePx="149.50",
            prevDayPx="145.00",
            dayNtlVlm="10000000.00",
            dayBaseVlm="500.00",
            premium="0.0002",
            name="SOL",
            midPx="149.75",
            impactPx="150.05",
            impactPxs=["150.02", "150.08"],
        ),
    ]


@pytest.fixture
def mock_meta_and_asset_ctxs_response(
    mock_asset_definitions: list[HyperliquidRawAssetDefinition],
    mock_asset_ctxs: list[HyperliquidRawAssetCtx],
) -> HyperliquidRawMetaAndAssetCtxsResponse:
    """Create a mock meta and asset contexts response.

    Returns:
        HyperliquidRawMetaAndAssetCtxsResponse: Mock response containing metadata and
            asset contexts.
    """
    # HyperliquidRawMetaAndAssetCtxsResponse expects a list format [meta, assetCtxs]
    # as that's what the API actually returns
    meta_dict = {
        "universe": [asset_def.model_dump(by_alias=True) for asset_def in mock_asset_definitions],
        "marginTables": None,
    }
    asset_ctxs_list = [asset_ctx.model_dump(by_alias=True) for asset_ctx in mock_asset_ctxs]

    return HyperliquidRawMetaAndAssetCtxsResponse.model_validate([meta_dict, asset_ctxs_list])


@pytest.fixture
def mock_markets() -> list[Market]:
    """Create mock market objects.

    Returns:
        list[Market]: A list of mock market objects for ETH, BTC, SOL.
    """
    return [
        Market(
            symbol=ETH_HL,
            market_type="perpetual",
            tick_size=Decimal("0.01"),
            step_size=Decimal("0.0001"),
            min_quantity=Decimal("0.0001"),
            max_quantity=Decimal(1000),
            status="Trading",
        ),
        Market(
            symbol=BTC_HL,
            market_type="perpetual",
            tick_size=Decimal("0.01"),
            step_size=Decimal("0.00001"),
            min_quantity=Decimal("0.00001"),
            max_quantity=Decimal(100),
            status="Trading",
        ),
        Market(
            symbol=SOL_HL,
            market_type="perpetual",
            tick_size=Decimal("0.01"),
            step_size=Decimal("0.001"),
            min_quantity=Decimal("0.001"),
            max_quantity=Decimal(10000),
            status="Trading",
        ),
    ]


class TestHyperliquidMarketMetadataService:
    """Test suite for HyperliquidMarketMetadataService."""

    @pytest.mark.asyncio
    async def test_get_markets_success(
        self,
        market_metadata_service: HyperliquidMarketMetadataService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_mapper: Mock,
        mock_meta_and_asset_ctxs_response: HyperliquidRawMetaAndAssetCtxsResponse,
        mock_markets: list[Market],
    ) -> None:
        """Test successful retrieval of all markets."""
        # Arrange
        args = GetMarketsArgs()

        mock_request_payload = MagicMock()
        mock_request_builder.build_info_request_payload.return_value = mock_request_payload

        # HTTP response should be a list format [meta, assetCtxs] as per API documentation
        raw_response: list[dict[str, object] | list[object]] = [
            {"universe": [], "marginTables": None},  # meta
            cast(list[object], []),  # assetCtxs as empty list
        ]
        mock_http_requester.return_value = (raw_response, 200, {})

        mock_response_handler.handle_info_meta_and_asset_ctxs_response.return_value = (
            mock_meta_and_asset_ctxs_response
        )
        mock_mapper.transform_raw_meta_and_asset_ctxs_to_markets.return_value = mock_markets

        # Act
        result = await market_metadata_service.get_markets(args)

        # Assert
        assert len(result) == 3
        assert result[0].symbol == ETH_HL
        assert result[1].symbol == BTC_HL
        assert result[2].symbol == SOL_HL
        mock_mapper.transform_raw_meta_and_asset_ctxs_to_markets.assert_called_once_with(
            mock_meta_and_asset_ctxs_response
        )

    @pytest.mark.asyncio
    async def test_get_markets_empty_response(
        self,
        market_metadata_service: HyperliquidMarketMetadataService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_mapper: Mock,
    ) -> None:
        """Test markets retrieval with empty response."""
        # Arrange
        args = GetMarketsArgs()

        mock_request_builder.build_info_request_payload.return_value = MagicMock()
        # HTTP response should be a list format [meta, assetCtxs] as per API documentation
        mock_http_requester.return_value = ([{"universe": [], "marginTables": None}, []], 200, {})

        # Use list format for empty response too
        empty_meta_dict: dict[str, list[object] | None] = {"universe": [], "marginTables": None}
        empty_response = HyperliquidRawMetaAndAssetCtxsResponse.model_validate([
            empty_meta_dict,
            [],
        ])
        mock_response_handler.handle_info_meta_and_asset_ctxs_response.return_value = empty_response
        mock_mapper.transform_raw_meta_and_asset_ctxs_to_markets.return_value = []

        # Act
        result = await market_metadata_service.get_markets(args)

        # Assert
        assert result == []

    @pytest.mark.asyncio
    async def test_get_markets_http_error(
        self,
        market_metadata_service: HyperliquidMarketMetadataService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
    ) -> None:
        """Test markets retrieval with HTTP error."""
        # Arrange
        args = GetMarketsArgs()

        mock_request_builder.build_info_request_payload.return_value = MagicMock()
        mock_http_requester.side_effect = Exception("Network error")

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await market_metadata_service.get_markets(args)

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected error occurred" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_markets_transformation_error(
        self,
        market_metadata_service: HyperliquidMarketMetadataService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_mapper: Mock,
        mock_meta_and_asset_ctxs_response: HyperliquidRawMetaAndAssetCtxsResponse,
    ) -> None:
        """Test markets retrieval with transformation error."""
        # Arrange
        args = GetMarketsArgs()

        mock_request_builder.build_info_request_payload.return_value = MagicMock()
        # HTTP response should be a list format [meta, assetCtxs] as per API documentation
        mock_http_requester.return_value = ([{"universe": [], "marginTables": None}, []], 200, {})
        mock_response_handler.handle_info_meta_and_asset_ctxs_response.return_value = (
            mock_meta_and_asset_ctxs_response
        )

        mock_mapper.transform_raw_meta_and_asset_ctxs_to_markets.side_effect = TransformationError(
            "Invalid market data"
        )

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await market_metadata_service.get_markets(args)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Failed to process/transform" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_market_success(
        self,
        market_metadata_service: HyperliquidMarketMetadataService,
        mock_markets: list[Market],
    ) -> None:
        """Test successful retrieval of individual market."""
        # Arrange
        args = GetMarketArgs(symbol=BTC_HL)

        # Mock get_markets to return our test markets
        with patch.object(market_metadata_service, "get_markets", return_value=mock_markets):
            # Act
            result = await market_metadata_service.get_market(args)

            # Assert
            assert result.symbol == BTC_HL
            assert result.market_type == "perpetual"
            assert result.status == "Trading"

    @pytest.mark.asyncio
    async def test_get_market_symbol_not_found(
        self,
        market_metadata_service: HyperliquidMarketMetadataService,
        mock_markets: list[Market],
    ) -> None:
        """Test market retrieval when symbol is not found."""
        # Arrange

        args = GetMarketArgs(symbol=exchanges.hyperliquid("NONEXISTENT"))

        with patch.object(market_metadata_service, "get_markets", return_value=mock_markets):
            # Act & Assert
            with pytest.raises(SymbolNotFoundError) as exc_info:
                await market_metadata_service.get_market(args)

            assert exc_info.value.symbol == "NONEXISTENT"
            assert exc_info.value.available_symbols is not None
            assert "ETH" in exc_info.value.available_symbols
            assert "BTC" in exc_info.value.available_symbols
            assert "SOL" in exc_info.value.available_symbols

    @pytest.mark.asyncio
    async def test_get_market_api_error_propagation(
        self,
        market_metadata_service: HyperliquidMarketMetadataService,
    ) -> None:
        """Test that API errors from get_markets are propagated."""
        # Arrange
        args = GetMarketArgs(symbol=BTC_HL)

        with patch.object(
            market_metadata_service,
            "get_markets",
            side_effect=APIError(
                message="API failure",
                code="NETWORK_ERROR",
            ),
        ):
            # Act & Assert
            with pytest.raises(APIError) as exc_info:
                await market_metadata_service.get_market(args)

            assert exc_info.value.code == "NETWORK_ERROR"
            assert "API failure" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_markets_none_response_error(
        self,
        market_metadata_service: HyperliquidMarketMetadataService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
    ) -> None:
        """Test markets retrieval with None response from HTTP client."""
        # Arrange
        args = GetMarketsArgs()
        mock_request_builder.build_info_request_payload.return_value = MagicMock()
        mock_http_requester.return_value = (None, 200, {})

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await market_metadata_service.get_markets(args)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "No content received" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_markets_invalid_response_type_error(
        self,
        market_metadata_service: HyperliquidMarketMetadataService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
    ) -> None:
        """Test markets retrieval with invalid response type (not a list)."""
        # Arrange
        args = GetMarketsArgs()
        mock_request_builder.build_info_request_payload.return_value = MagicMock()
        # Return a dict instead of list to trigger the type validation error
        mock_http_requester.return_value = ({"error": "invalid"}, 200, {})

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await market_metadata_service.get_markets(args)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Expected list response" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_markets_with_isolated_margin_markets(
        self,
        market_metadata_service: HyperliquidMarketMetadataService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_mapper: Mock,
        mock_meta_and_asset_ctxs_response: HyperliquidRawMetaAndAssetCtxsResponse,
        mock_markets: list[Market],
    ) -> None:
        """Test markets retrieval includes isolated margin markets."""
        # Arrange
        args = GetMarketsArgs()

        mock_request_builder.build_info_request_payload.return_value = MagicMock()
        # HTTP response should be a list format [meta, assetCtxs] as per API documentation
        mock_http_requester.return_value = ([{"universe": [], "marginTables": None}, []], 200, {})
        mock_response_handler.handle_info_meta_and_asset_ctxs_response.return_value = (
            mock_meta_and_asset_ctxs_response
        )
        mock_mapper.transform_raw_meta_and_asset_ctxs_to_markets.return_value = mock_markets

        # Act
        result = await market_metadata_service.get_markets(args)

        # Assert
        # Check that SOL (isolated margin) is included
        sol_market = next((m for m in result if m.symbol == SOL_HL), None)
        assert sol_market is not None
        assert sol_market.status == "Trading"  # Market is trading

    @pytest.mark.asyncio
    async def test_get_market_with_empty_markets_list(
        self,
        market_metadata_service: HyperliquidMarketMetadataService,
    ) -> None:
        """Test market retrieval when no markets are available."""
        # Arrange
        args = GetMarketArgs(symbol=BTC_HL)
        empty_markets: list[Market] = []

        # Mock get_markets to return empty list
        with patch.object(market_metadata_service, "get_markets", return_value=empty_markets):
            # Act & Assert
            with pytest.raises(SymbolNotFoundError) as exc_info:
                await market_metadata_service.get_market(args)

            assert exc_info.value.symbol == "BTC"
            assert exc_info.value.available_symbols is not None
            assert len(exc_info.value.available_symbols) == 0
