"""
Unit tests for the HyperliquidMarketDataService.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.connectivity.http_client import HttpClient
from cyberdelta.apis.connectivity.rate_limiter_service import RateLimiterService
from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.hl_response_handler import HyperliquidResponseHandler
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetCtx,  # For get_ticker, get_funding_rate
    HyperliquidRawAssetDefinition,
    HyperliquidRawMetaAndAssetCtxsResponse,  # For get_all_asset_contexts
    HyperliquidRawMetaResponse,
)
from cyberdelta.apis.hyperliquid.services.hl_market_data_service import HyperliquidMarketDataService


@pytest.fixture
def mock_http_client() -> AsyncMock:
    """Provides a mock HttpClient."""
    return AsyncMock(spec=HttpClient)


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
        # build_info_request_payload returns None
        mock_payload_from_builder = None
        # Construct a realistic raw response content based on HyperliquidRawMetaAndAssetCtxsResponse structure
        mock_raw_response_content = [
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

        # Check that rate limiter was called
        mock_rate_limiter_service.wait_for_permission.assert_called_once_with("/info")
        # Check that the correct request builder method was called
        mock_request_builder.build_info_request_payload.assert_called_once_with()
        # Check that HttpClient was called with data=None (since builder returns None)
        mock_http_client.request.assert_called_once_with(
            method="POST",
            endpoint_path="/info",
            data=mock_payload_from_builder,  # This is None
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

        hl_market_data_service.get_all_asset_contexts = AsyncMock(
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
        hl_market_data_service.get_all_asset_contexts = AsyncMock(
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

    # Add more tests for other methods: get_funding_rate, get_order_book, etc.
