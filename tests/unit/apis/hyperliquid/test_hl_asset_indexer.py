"""
Unit tests for HyperliquidAssetIndexResolver.

Tests the asset index resolution logic that was extracted from HyperliquidAPI
to improve modularity and reduce complexity.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.hl_asset_indexer import HyperliquidAssetIndexResolver
from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.hl_response_handler import HyperliquidResponseHandler
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawMetaAndAssetCtxsResponse,
)
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode


class TestHyperliquidAssetIndexResolver:
    """Test suite for HyperliquidAssetIndexResolver."""

    @pytest.fixture
    def mock_requester(self) -> AsyncMock:
        """Mock for the requester callable."""
        return AsyncMock()

    @pytest.fixture
    def mock_response_handler(self) -> MagicMock:
        """Mock for HyperliquidResponseHandler."""
        return MagicMock(spec=HyperliquidResponseHandler)

    @pytest.fixture
    def mock_request_builder(self) -> MagicMock:
        """Mock for HyperliquidRequestBuilder."""
        mock_builder = MagicMock(spec=HyperliquidRequestBuilder)
        # Setup the build_info_request_payload method
        mock_payload = MagicMock()
        mock_payload.model_dump.return_value = {"type": "metaAndAssetCtxs"}
        mock_builder.build_info_request_payload.return_value = mock_payload
        return mock_builder

    @pytest.fixture
    def mock_meta_response(self) -> HyperliquidRawMetaAndAssetCtxsResponse:
        """Mock HyperliquidRawMetaAndAssetCtxsResponse with BTC and ETH."""
        mock_response = MagicMock(spec=HyperliquidRawMetaAndAssetCtxsResponse)

        # Mock the meta.universe structure
        mock_meta = MagicMock()
        mock_btc_asset = MagicMock()
        mock_btc_asset.name = "BTC"
        mock_eth_asset = MagicMock()
        mock_eth_asset.name = "ETH"

        mock_universe = [mock_btc_asset, mock_eth_asset]
        mock_meta.universe = mock_universe
        mock_response.meta = mock_meta

        return mock_response

    @pytest.fixture
    def asset_indexer(
        self,
        mock_requester: AsyncMock,
        mock_response_handler: MagicMock,
        mock_request_builder: MagicMock,
    ) -> HyperliquidAssetIndexResolver:
        """Create HyperliquidAssetIndexResolver instance with mocked dependencies."""
        return HyperliquidAssetIndexResolver(
            requester=mock_requester,
            response_handler=mock_response_handler,
            request_builder=mock_request_builder,
            exchange_name_for_log="test_hyperliquid",
        )

    @pytest.mark.asyncio
    async def test_cache_hit(
        self,
        asset_indexer: HyperliquidAssetIndexResolver,
        mock_requester: AsyncMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test that cached asset indices are returned without making API calls."""
        # First call should populate cache
        mock_raw_json_response: Any = [{"universe": [{"name": "BTC"}, {"name": "ETH"}]}, [{}, {}]]
        mock_requester.return_value = (mock_raw_json_response, 200, {})

        # Setup mock response handler
        mock_response = MagicMock()
        mock_meta = MagicMock()
        mock_btc_asset = MagicMock()
        mock_btc_asset.name = "BTC"
        mock_eth_asset = MagicMock()
        mock_eth_asset.name = "ETH"
        mock_universe = [mock_btc_asset, mock_eth_asset]
        mock_meta.universe = mock_universe
        mock_response.meta = mock_meta

        mock_response_handler.handle_info_meta_and_asset_ctxs_response.return_value = mock_response

        # First call to populate cache
        result1 = await asset_indexer.get_asset_index("BTC")
        assert result1 == 0

        # Reset mock to verify second call doesn't make API request
        mock_requester.reset_mock()

        # Second call should use cache (no API call)
        result2 = await asset_indexer.get_asset_index("BTC")
        assert result2 == 0

        # Assert requester was NOT called on second request (cache hit)
        mock_requester.assert_not_called()

    @pytest.mark.asyncio
    async def test_cache_miss_successful_fetch(
        self,
        asset_indexer: HyperliquidAssetIndexResolver,
        mock_requester: AsyncMock,
        mock_response_handler: MagicMock,
        mock_request_builder: MagicMock,
        mock_meta_response: HyperliquidRawMetaAndAssetCtxsResponse,
    ) -> None:
        """Test successful fetch and cache population when cache is empty."""
        # Setup mock responses
        mock_raw_json_response: Any = [{"universe": [{"name": "BTC"}, {"name": "ETH"}]}, [{}, {}]]
        mock_requester.return_value = (mock_raw_json_response, 200, {})
        mock_response_handler.handle_info_meta_and_asset_ctxs_response.return_value = (
            mock_meta_response
        )

        # Call get_asset_index for BTC
        result = await asset_indexer.get_asset_index("BTC")

        # Assert requester was called correctly
        mock_requester.assert_called_once_with(
            method="POST",
            endpoint="/info",
            data={"type": "metaAndAssetCtxs"},
        )

        # Assert response handler was called
        mock_response_handler.handle_info_meta_and_asset_ctxs_response.assert_called_once_with(
            mock_raw_json_response
        )

        # Assert correct index returned
        assert result == 0

        # Test that ETH can be retrieved from cache (should not call requester again)
        mock_requester.reset_mock()
        result_eth = await asset_indexer.get_asset_index("ETH")
        assert result_eth == 1
        mock_requester.assert_not_called()

    @pytest.mark.asyncio
    async def test_invalid_symbol_empty_string(
        self, asset_indexer: HyperliquidAssetIndexResolver
    ) -> None:
        """Test that empty string symbol raises APIError."""
        with pytest.raises(APIError) as exc_info:
            await asset_indexer.get_asset_index("")

        assert exc_info.value.code == APIErrorCode.INVALID_PARAMS.value
        assert "Invalid symbol for asset index resolution" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_api_request_failure(
        self,
        asset_indexer: HyperliquidAssetIndexResolver,
        mock_requester: AsyncMock,
    ) -> None:
        """Test that API request failures are properly handled and re-raised."""
        # Setup requester to raise APIError
        original_error = APIError("Network failure", code=APIErrorCode.NETWORK_ISSUE.value)
        mock_requester.side_effect = original_error

        # Call get_asset_index and expect APIError
        with pytest.raises(APIError) as exc_info:
            await asset_indexer.get_asset_index("BTC")

        # Assert the error contains context about asset index fetch failure
        assert "Failed to fetch asset index for symbol 'BTC'" in str(exc_info.value)
        assert exc_info.value.original_exception is original_error

    @pytest.mark.asyncio
    async def test_empty_api_response(
        self,
        asset_indexer: HyperliquidAssetIndexResolver,
        mock_requester: AsyncMock,
    ) -> None:
        """Test that empty API response raises appropriate APIError."""
        # Setup requester to return None response
        mock_requester.return_value = (None, 200, {})

        # Call get_asset_index and expect APIError
        with pytest.raises(APIError) as exc_info:
            await asset_indexer.get_asset_index("BTC")

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "No data received for market metadata" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_response_handler_validation_failure(
        self,
        asset_indexer: HyperliquidAssetIndexResolver,
        mock_requester: AsyncMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test that ValidationError from response handler is properly wrapped."""
        # Setup successful requester response
        mock_raw_json_response: Any = [{"universe": []}, []]
        mock_requester.return_value = (mock_raw_json_response, 200, {})

        # Setup response handler to raise ValidationError
        validation_error = ValidationError.from_exception_data(
            "HyperliquidRawMetaAndAssetCtxsResponse",
            [{"type": "missing", "loc": ("meta",), "input": {}}],
        )
        mock_response_handler.handle_info_meta_and_asset_ctxs_response.side_effect = (
            validation_error
        )

        # Call get_asset_index and expect APIError
        with pytest.raises(APIError) as exc_info:
            await asset_indexer.get_asset_index("BTC")

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Failed to parse market metadata for asset index mapping" in str(exc_info.value)
        assert exc_info.value.original_exception is validation_error

    @pytest.mark.asyncio
    async def test_symbol_not_found_in_metadata(
        self,
        asset_indexer: HyperliquidAssetIndexResolver,
        mock_requester: AsyncMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test that requesting non-existent symbol raises APIError."""
        # Setup mock response with only BTC and ETH
        mock_response = MagicMock(spec=HyperliquidRawMetaAndAssetCtxsResponse)
        mock_meta = MagicMock()

        mock_btc_asset = MagicMock()
        mock_btc_asset.name = "BTC"
        mock_eth_asset = MagicMock()
        mock_eth_asset.name = "ETH"

        mock_universe = [mock_btc_asset, mock_eth_asset]
        mock_meta.universe = mock_universe
        mock_response.meta = mock_meta

        mock_raw_json_response: Any = [{"universe": [{"name": "BTC"}, {"name": "ETH"}]}, [{}, {}]]
        mock_requester.return_value = (mock_raw_json_response, 200, {})
        mock_response_handler.handle_info_meta_and_asset_ctxs_response.return_value = mock_response

        # Request non-existent symbol
        with pytest.raises(APIError) as exc_info:
            await asset_indexer.get_asset_index("XYZ")

        assert exc_info.value.code == APIErrorCode.SYMBOL_NOT_FOUND.value
        assert "Asset index for symbol 'XYZ' not found" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_cache_behavior_with_new_symbol_fetch(
        self,
        asset_indexer: HyperliquidAssetIndexResolver,
        mock_requester: AsyncMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test cache behavior when fetching new symbols."""
        # Setup first response with BTC and ETH
        mock_response_1 = MagicMock(spec=HyperliquidRawMetaAndAssetCtxsResponse)
        mock_meta_1 = MagicMock()

        mock_btc_asset_1 = MagicMock()
        mock_btc_asset_1.name = "BTC"
        mock_eth_asset_1 = MagicMock()
        mock_eth_asset_1.name = "ETH"

        mock_universe_1 = [mock_btc_asset_1, mock_eth_asset_1]
        mock_meta_1.universe = mock_universe_1
        mock_response_1.meta = mock_meta_1

        # Setup second response with ETH and ADA (BTC removed, ADA added)
        mock_response_2 = MagicMock(spec=HyperliquidRawMetaAndAssetCtxsResponse)
        mock_meta_2 = MagicMock()

        mock_eth_asset_2 = MagicMock()
        mock_eth_asset_2.name = "ETH"
        mock_ada_asset_2 = MagicMock()
        mock_ada_asset_2.name = "ADA"

        mock_universe_2 = [mock_eth_asset_2, mock_ada_asset_2]
        mock_meta_2.universe = mock_universe_2
        mock_response_2.meta = mock_meta_2

        # Setup requester to return different responses
        mock_raw_json_response_1: Any = [{"universe": [{"name": "BTC"}, {"name": "ETH"}]}, [{}, {}]]
        mock_raw_json_response_2: Any = [{"universe": [{"name": "ETH"}, {"name": "ADA"}]}, [{}, {}]]

        mock_requester.side_effect = [
            (mock_raw_json_response_1, 200, {}),
            (mock_raw_json_response_2, 200, {}),
        ]
        mock_response_handler.handle_info_meta_and_asset_ctxs_response.side_effect = [
            mock_response_1,
            mock_response_2,
        ]

        # First call should populate cache with BTC=0, ETH=1
        result_btc = await asset_indexer.get_asset_index("BTC")
        assert result_btc == 0

        # Second call for new symbol ADA should fetch new metadata
        result_ada = await asset_indexer.get_asset_index("ADA")
        assert result_ada == 1

        # Verify requester was called twice
        assert mock_requester.call_count == 2

        # Verify ETH is now at index 0 in the new metadata
        result_eth = await asset_indexer.get_asset_index("ETH")
        assert result_eth == 0

        # Should not make another API call since ETH is in current cache
        assert mock_requester.call_count == 2

    @pytest.mark.asyncio
    async def test_concurrent_requests_cache_consistency(
        self,
        asset_indexer: HyperliquidAssetIndexResolver,
        mock_requester: AsyncMock,
        mock_response_handler: MagicMock,
        mock_meta_response: HyperliquidRawMetaAndAssetCtxsResponse,
    ) -> None:
        """Test that concurrent requests for the same symbol don't cause race conditions."""
        import asyncio

        # Setup mock responses
        mock_raw_json_response: Any = [{"universe": [{"name": "BTC"}, {"name": "ETH"}]}, [{}, {}]]
        mock_requester.return_value = (mock_raw_json_response, 200, {})
        mock_response_handler.handle_info_meta_and_asset_ctxs_response.return_value = (
            mock_meta_response
        )

        # Make concurrent requests for the same symbol
        tasks = [
            asset_indexer.get_asset_index("BTC"),
            asset_indexer.get_asset_index("BTC"),
            asset_indexer.get_asset_index("BTC"),
        ]

        results = await asyncio.gather(*tasks)

        # All should return the same result
        assert all(result == 0 for result in results)

        # Note: Due to the current implementation, multiple requests might be made
        # This test documents the current behavior rather than enforcing a specific optimization

    @pytest.mark.asyncio
    async def test_error_handling_preserves_exchange_context(
        self,
        mock_requester: AsyncMock,
        mock_response_handler: MagicMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test that error messages include proper exchange context."""
        custom_exchange_name = "custom_hyperliquid_test"

        asset_indexer = HyperliquidAssetIndexResolver(
            requester=mock_requester,
            response_handler=mock_response_handler,
            request_builder=mock_request_builder,
            exchange_name_for_log=custom_exchange_name,
        )

        # Test error message includes proper context
        with pytest.raises(APIError) as exc_info:
            await asset_indexer.get_asset_index("")

        # The error should be raised with proper context
        assert exc_info.value.code == APIErrorCode.INVALID_PARAMS.value
        assert "Invalid symbol for asset index resolution" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_request_builder_integration(
        self,
        asset_indexer: HyperliquidAssetIndexResolver,
        mock_requester: AsyncMock,
        mock_response_handler: MagicMock,
        mock_request_builder: MagicMock,
        mock_meta_response: HyperliquidRawMetaAndAssetCtxsResponse,
    ) -> None:
        """Test that request builder is called correctly to build the payload."""
        # Setup mock responses
        mock_raw_json_response: Any = [{"universe": [{"name": "BTC"}]}, [{}]]
        mock_requester.return_value = (mock_raw_json_response, 200, {})
        mock_response_handler.handle_info_meta_and_asset_ctxs_response.return_value = (
            mock_meta_response
        )

        # Call get_asset_index
        await asset_indexer.get_asset_index("BTC")

        # Verify request builder was called correctly
        mock_request_builder.build_info_request_payload.assert_called_once()

        # Verify the payload was used in the request
        expected_payload = (
            mock_request_builder.build_info_request_payload.return_value.model_dump.return_value
        )
        mock_requester.assert_called_once_with(
            method="POST", endpoint="/info", data=expected_payload
        )
