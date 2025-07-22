"""Unit tests for Hyperliquid Price Ticker Service.

Tests cover all methods of the HyperliquidPriceTickerService including:
- Asset contexts retrieval
- Individual ticker data
- All mid prices fetching
- Funding rate retrieval
- Error handling and validation
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from pydantic import ValidationError

from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.exceptions.response_validation import EmptyResponseError
from cyberdelta.apis.hyperliquid.hl_response_handler import HyperliquidResponseHandler
from cyberdelta.apis.hyperliquid.mappers.market_data.hl_price_ticker_mapper import (
    HyperliquidPriceTickerMapper,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_all_mids import HyperliquidRawAllMids
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetCtx,
    HyperliquidRawAssetDefinition,
    HyperliquidRawMetaAndAssetCtxsResponse,
    HyperliquidRawMetaResponse,
)
from cyberdelta.apis.hyperliquid.request_builders.hl_market_data_request_builder import (
    HyperliquidMarketDataRequestBuilder,
)
from cyberdelta.apis.hyperliquid.services.market_data.hl_price_ticker_service import (
    HyperliquidPriceTickerService,
)
from cyberdelta.core.models import FundingRate, Ticker
from cyberdelta.core.models.market.mid_prices import MidPrices


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
    return MagicMock(spec=HyperliquidPriceTickerMapper)


@pytest.fixture
def price_ticker_service(
    mock_http_requester: AsyncMock,
    mock_request_builder: Mock,
    mock_response_handler: Mock,
    mock_mapper: Mock,
) -> HyperliquidPriceTickerService:
    """Create a price ticker service instance with mocks."""
    mock_historical_data_mapper = MagicMock()
    return HyperliquidPriceTickerService(
        http_client_requester=mock_http_requester,
        request_builder=mock_request_builder,
        response_handler=mock_response_handler,
        mapper=mock_mapper,
        historical_data_mapper=mock_historical_data_mapper,
        exchange_name="hyperliquid",
    )


@pytest.fixture
def mock_asset_ctx() -> HyperliquidRawAssetCtx:
    """Create a mock asset context."""
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
def mock_asset_definition() -> HyperliquidRawAssetDefinition:
    """Create a mock asset definition."""
    return HyperliquidRawAssetDefinition(
        name="ETH",
        szDecimals=4,
        maxLeverage=50,
        marginTableId=None,
        isDelisted=None,
        onlyIsolated=False,
    )


@pytest.fixture
def mock_meta_and_asset_ctxs_response(
    mock_asset_ctx: HyperliquidRawAssetCtx,
    mock_asset_definition: HyperliquidRawAssetDefinition,
) -> HyperliquidRawMetaAndAssetCtxsResponse:
    """Create a mock meta and asset contexts response."""
    return HyperliquidRawMetaAndAssetCtxsResponse(
        meta=HyperliquidRawMetaResponse(
            universe=[mock_asset_definition],
            marginTables=None,
        ),
        asset_ctxs=[mock_asset_ctx],
    )


@pytest.fixture
def mock_ticker() -> Ticker:
    """Create a mock ticker."""
    return Ticker(
        symbol="ETH",
        exchange="hyperliquid",
        timestamp=datetime.now(UTC),
        price=Decimal("3500.00"),
        bid=Decimal("3499.50"),
        ask=Decimal("3500.50"),
        volume=Decimal(50000),
    )


@pytest.fixture
def mock_funding_rate() -> FundingRate:
    """Create a mock funding rate."""
    return FundingRate(
        symbol="ETH",
        timestamp=datetime.now(UTC),
        funding_rate=Decimal("0.0001"),
        mark_price=Decimal("3500.00"),
        index_price=Decimal("3499.50"),
    )


@pytest.fixture
def mock_all_mids() -> HyperliquidRawAllMids:
    """Create a mock all mids response."""
    return HyperliquidRawAllMids({
        "ETH": "3500.00",
        "BTC": "65000.00",
        "SOL": "150.00",
    })


@pytest.fixture
def mock_mid_prices() -> MidPrices:
    """Create a mock mid prices object."""
    return MidPrices(
        prices={
            "ETH": Decimal("3500.00"),
            "BTC": Decimal("65000.00"),
            "SOL": Decimal("150.00"),
        },
        exchange="hyperliquid",
        timestamp=datetime.now(UTC),
    )


class TestHyperliquidPriceTickerService:
    """Test suite for HyperliquidPriceTickerService."""

    @pytest.mark.asyncio
    async def test_get_all_asset_contexts_raw_success(
        self,
        price_ticker_service: HyperliquidPriceTickerService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_meta_and_asset_ctxs_response: HyperliquidRawMetaAndAssetCtxsResponse,
    ) -> None:
        """Test successful retrieval of all asset contexts."""
        # Arrange
        mock_request_payload = MagicMock()
        mock_request_builder.build_meta_and_asset_ctxs_request_payload.return_value = (
            mock_request_payload
        )

        raw_response: dict[str, object] = {"meta": {}, "assetCtxs": []}
        mock_http_requester.return_value = (raw_response, 200, {})

        mock_response_handler.handle_meta_and_asset_ctxs_response.return_value = (
            mock_meta_and_asset_ctxs_response
        )

        # Act
        result = await price_ticker_service.get_all_asset_contexts_raw()

        # Assert
        assert result == mock_meta_and_asset_ctxs_response
        mock_request_builder.build_meta_and_asset_ctxs_request_payload.assert_called_once()
        mock_http_requester.assert_called_once()
        assert mock_http_requester.call_args.kwargs["method"] == "POST"
        assert mock_http_requester.call_args.kwargs["endpoint"] == "/info"
        assert mock_http_requester.call_args.kwargs["is_signed"] is False

    @pytest.mark.asyncio
    async def test_get_all_asset_contexts_raw_empty_response(
        self,
        price_ticker_service: HyperliquidPriceTickerService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
    ) -> None:
        """Test asset contexts retrieval with None response."""
        # Arrange
        mock_request_builder.build_meta_and_asset_ctxs_request_payload.return_value = MagicMock()
        mock_http_requester.return_value = (None, 200, {})

        # Act & Assert
        with pytest.raises(EmptyResponseError) as exc_info:
            await price_ticker_service.get_all_asset_contexts_raw()

        assert "metaAndAssetCtxs data" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_all_asset_contexts_raw_http_error(
        self,
        price_ticker_service: HyperliquidPriceTickerService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
    ) -> None:
        """Test asset contexts retrieval with HTTP error."""
        # Arrange
        mock_request_builder.build_meta_and_asset_ctxs_request_payload.return_value = MagicMock()
        mock_http_requester.side_effect = Exception("Network error")

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await price_ticker_service.get_all_asset_contexts_raw()

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Network error" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_ticker_success(
        self,
        price_ticker_service: HyperliquidPriceTickerService,
        mock_meta_and_asset_ctxs_response: HyperliquidRawMetaAndAssetCtxsResponse,
        mock_mapper: Mock,
        mock_ticker: Ticker,
    ) -> None:
        """Test successful ticker retrieval."""
        # Arrange
        symbol = "ETH"

        # Mock get_all_asset_contexts_raw to return our mock response
        with patch.object(
            price_ticker_service,
            "get_all_asset_contexts_raw",
            return_value=mock_meta_and_asset_ctxs_response,
        ):
            mock_mapper.transform_raw_asset_ctx_to_ticker.return_value = mock_ticker

            # Act
            result = await price_ticker_service.get_ticker(symbol)

            # Assert
            assert result == mock_ticker
            mock_mapper.transform_raw_asset_ctx_to_ticker.assert_called_once()
            # Verify the asset context was enriched with the symbol name
            call_args = mock_mapper.transform_raw_asset_ctx_to_ticker.call_args
            assert hasattr(call_args[0][0], "name")

    @pytest.mark.asyncio
    async def test_get_ticker_symbol_not_found(
        self,
        price_ticker_service: HyperliquidPriceTickerService,
        mock_meta_and_asset_ctxs_response: HyperliquidRawMetaAndAssetCtxsResponse,
    ) -> None:
        """Test ticker retrieval when symbol is not found."""
        # Arrange
        symbol = "NONEXISTENT"

        with patch.object(
            price_ticker_service,
            "get_all_asset_contexts_raw",
            return_value=mock_meta_and_asset_ctxs_response,
        ):
            # Act
            result = await price_ticker_service.get_ticker(symbol)

            # Assert
            assert result is None

    @pytest.mark.asyncio
    async def test_get_ticker_invalid_symbol(
        self,
        price_ticker_service: HyperliquidPriceTickerService,
    ) -> None:
        """Test ticker retrieval with invalid symbol."""
        # Arrange & Act & Assert
        with pytest.raises(ValueError) as exc_info:
            await price_ticker_service.get_ticker("")

        assert "'symbol' must be a non-empty string" in str(exc_info.value)

        with pytest.raises(ValueError) as exc_info:
            await price_ticker_service.get_ticker("   ")

        assert "'symbol' cannot be empty or whitespace only" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_ticker_transformation_error(
        self,
        price_ticker_service: HyperliquidPriceTickerService,
        mock_meta_and_asset_ctxs_response: HyperliquidRawMetaAndAssetCtxsResponse,
        mock_mapper: Mock,
    ) -> None:
        """Test ticker retrieval with transformation error."""
        # Arrange
        symbol = "ETH"

        with patch.object(
            price_ticker_service,
            "get_all_asset_contexts_raw",
            return_value=mock_meta_and_asset_ctxs_response,
        ):
            mock_mapper.transform_raw_asset_ctx_to_ticker.side_effect = TransformationError(
                "Invalid ticker data"
            )

            # Act & Assert
            with pytest.raises(APIError) as exc_info:
                await price_ticker_service.get_ticker(symbol)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value

    @pytest.mark.asyncio
    async def test_get_all_mids_success(
        self,
        price_ticker_service: HyperliquidPriceTickerService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_mapper: Mock,
        mock_all_mids: HyperliquidRawAllMids,
        mock_mid_prices: MidPrices,
    ) -> None:
        """Test successful retrieval of all mid prices."""
        # Arrange
        mock_request_payload = MagicMock()
        mock_request_builder.build_all_mids_request_payload.return_value = mock_request_payload

        raw_response: dict[str, str] = {"ETH": "3500.00", "BTC": "65000.00"}
        mock_http_requester.return_value = (raw_response, 200, {})

        mock_response_handler.handle_all_mids_response.return_value = mock_all_mids
        mock_mapper.transform_raw_all_mids_to_internal.return_value = mock_mid_prices

        # Act
        result = await price_ticker_service.get_all_mids()

        # Assert
        assert result == mock_mid_prices
        mock_request_builder.build_all_mids_request_payload.assert_called_once()
        mock_http_requester.assert_called_once()
        assert mock_http_requester.call_args.kwargs["request_weight"] == 2

    @pytest.mark.asyncio
    async def test_get_all_mids_validation_error(
        self,
        price_ticker_service: HyperliquidPriceTickerService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
    ) -> None:
        """Test all mids retrieval with validation error."""
        # Arrange
        mock_request_builder.build_all_mids_request_payload.return_value = MagicMock()
        mock_http_requester.return_value = ({"invalid": "data"}, 200, {})

        mock_response_handler.handle_all_mids_response.side_effect = (
            ValidationError.from_exception_data(
                "validation_error",
                [{"type": "missing", "loc": ("mids",), "input": {"invalid": "data"}}],
            )
        )

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await price_ticker_service.get_all_mids()

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Failed to validate AllMids response" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_funding_rate_success(
        self,
        price_ticker_service: HyperliquidPriceTickerService,
        mock_meta_and_asset_ctxs_response: HyperliquidRawMetaAndAssetCtxsResponse,
        mock_mapper: Mock,
        mock_funding_rate: FundingRate,
    ) -> None:
        """Test successful funding rate retrieval."""
        # Arrange
        symbol = "ETH"

        with patch.object(
            price_ticker_service,
            "get_all_asset_contexts_raw",
            return_value=mock_meta_and_asset_ctxs_response,
        ):
            mock_mapper.transform_raw_asset_ctx_to_funding_rate.return_value = mock_funding_rate

            # Act
            result = await price_ticker_service.get_funding_rate(symbol)

            # Assert
            assert result == mock_funding_rate
            mock_mapper.transform_raw_asset_ctx_to_funding_rate.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_funding_rate_symbol_not_found(
        self,
        price_ticker_service: HyperliquidPriceTickerService,
        mock_meta_and_asset_ctxs_response: HyperliquidRawMetaAndAssetCtxsResponse,
    ) -> None:
        """Test funding rate retrieval when symbol is not found."""
        # Arrange
        symbol = "NONEXISTENT"

        with patch.object(
            price_ticker_service,
            "get_all_asset_contexts_raw",
            return_value=mock_meta_and_asset_ctxs_response,
        ):
            # Act
            result = await price_ticker_service.get_funding_rate(symbol)

            # Assert
            assert result is None

    @pytest.mark.asyncio
    async def test_get_funding_rate_invalid_symbol(
        self,
        price_ticker_service: HyperliquidPriceTickerService,
    ) -> None:
        """Test funding rate retrieval with invalid symbol."""
        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            await price_ticker_service.get_funding_rate("")

        assert "'symbol' must be a non-empty string" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_ticker_empty_contexts(
        self,
        price_ticker_service: HyperliquidPriceTickerService,
    ) -> None:
        """Test ticker retrieval with empty contexts response."""
        # Arrange
        symbol = "ETH"
        empty_response = HyperliquidRawMetaAndAssetCtxsResponse(
            meta=HyperliquidRawMetaResponse(universe=[], marginTables=None),
            asset_ctxs=[],
        )

        with patch.object(
            price_ticker_service, "get_all_asset_contexts_raw", return_value=empty_response
        ):
            # Act
            result = await price_ticker_service.get_ticker(symbol)

            # Assert
            assert result is None

    @pytest.mark.asyncio
    async def test_get_all_mids_http_error(
        self,
        price_ticker_service: HyperliquidPriceTickerService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
    ) -> None:
        """Test all mids retrieval with HTTP error."""
        # Arrange
        mock_request_builder.build_all_mids_request_payload.return_value = MagicMock()
        mock_http_requester.side_effect = Exception("Connection timeout")

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await price_ticker_service.get_all_mids()

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Connection timeout" in str(exc_info.value)
