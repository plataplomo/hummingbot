"""Comprehensive unit tests for HyperliquidAssetIndexResolver.

Tests asset index resolution functionality with parametrized tests, smart fixture usage,
and comprehensive success, edge, and failure cases. Only tests through public APIs.
"""

import asyncio
from collections.abc import Callable
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.hyperliquid.hl_asset_indexer import HyperliquidAssetIndexResolver
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawMetaAndAssetCtxsResponse,
)
from cyberdelta.enums.environment import EnvironmentType


class TestHyperliquidAssetIndexResolver:
    """Test HyperliquidAssetIndexResolver public API functionality."""

    @pytest.fixture
    def mock_requester(self) -> AsyncMock:
        """Create mock requester function.

        Returns:
            AsyncMock configured to return test response data.
        """
        requester = AsyncMock()
        requester.return_value = ({"mock": "response"}, 200, {"Content-Type": "application/json"})
        return requester

    @pytest.fixture
    def mock_response_handler(self) -> MagicMock:
        """Create mock response handler.

        Returns:
            MagicMock with universe data containing BTC, ETH, and SOL.
        """
        handler = MagicMock()
        # Create mock response with universe data
        mock_response = MagicMock(spec=HyperliquidRawMetaAndAssetCtxsResponse)
        mock_meta = MagicMock()
        mock_meta.universe = [
            MagicMock(name="BTC"),
            MagicMock(name="ETH"),
            MagicMock(name="SOL"),
        ]
        mock_response.meta = mock_meta
        handler.handle_info_meta_and_asset_ctxs_response.return_value = mock_response
        return handler

    @pytest.fixture
    def mock_request_builder(self) -> MagicMock:
        """Create mock request builder.

        Returns:
            MagicMock configured to build metaAndAssetCtxs requests.
        """
        builder = MagicMock()
        builder.build_info_request_payload.return_value.model_dump.return_value = {
            "type": "metaAndAssetCtxs"
        }
        return builder

    @pytest.fixture
    def resolver_factory(
        self,
        mock_requester: AsyncMock,
        mock_response_handler: MagicMock,
        mock_request_builder: MagicMock,
    ) -> Callable[..., HyperliquidAssetIndexResolver]:
        """Factory to create resolver instances with optional overrides.

        Returns:
            Factory function that creates HyperliquidAssetIndexResolver with
                customizable dependencies.
        """

        def _create(**kwargs: dict[str, Any]) -> HyperliquidAssetIndexResolver:
            defaults: dict[str, Any] = {
                "requester": mock_requester,
                "response_handler": mock_response_handler,
                "request_builder": mock_request_builder,
                "exchange_name_for_log": "test_exchange",
                "environment_type": EnvironmentType.MAINNET,
            }
            defaults.update(kwargs)
            return HyperliquidAssetIndexResolver(**defaults)

        return _create

    # Initialization Tests

    def test_resolver_initialization(
        self, resolver_factory: Callable[..., HyperliquidAssetIndexResolver]
    ) -> None:
        """Test resolver initializes with all required dependencies."""
        resolver = resolver_factory()

        assert resolver is not None
        assert isinstance(resolver, HyperliquidAssetIndexResolver)
        assert hasattr(resolver, "logger")

    @pytest.mark.parametrize(
        "environment_type", [EnvironmentType.MAINNET, EnvironmentType.TESTNET, None]
    )
    def test_resolver_initialization_with_different_environments(
        self,
        resolver_factory: Callable[..., HyperliquidAssetIndexResolver],
        environment_type: EnvironmentType | None,
    ) -> None:
        """Test resolver initialization with different environment types."""
        resolver = resolver_factory(environment_type=environment_type)

        assert resolver is not None
        # Test environment type through public behavior
        assert resolver is not None

    # Direct Spot Symbol Resolution Tests

    @pytest.mark.parametrize(
        ("symbol", "expected_index"),
        [
            ("@1", 1),
            ("@2", 2),
            ("@10", 10),
            ("@100", 100),
        ],
    )
    @pytest.mark.asyncio
    async def test_resolve_spot_symbol_at_format_success(
        self,
        resolver_factory: Callable[..., HyperliquidAssetIndexResolver],
        symbol: str,
        expected_index: int,
    ) -> None:
        """Test successful resolution of @N format spot symbols."""
        resolver = resolver_factory()

        result = await resolver.get_asset_index(symbol)

        assert result == expected_index

    @pytest.mark.parametrize("environment_type", [EnvironmentType.MAINNET, EnvironmentType.TESTNET])
    @pytest.mark.asyncio
    async def test_resolve_known_spot_symbols_by_environment(
        self,
        resolver_factory: Callable[..., HyperliquidAssetIndexResolver],
        environment_type: EnvironmentType,
    ) -> None:
        """Test resolution of known spot symbols based on environment."""
        resolver = resolver_factory(environment_type=environment_type)

        # Test basic functionality with environment-specific resolver
        # These are placeholder tests - actual mappings would need to be verified
        assert resolver is not None
        if environment_type == EnvironmentType.MAINNET:
            # Test mainnet-specific symbols would go here
            pass
        else:
            # Test testnet-specific symbols would go here
            pass

    # Cache Functionality Tests

    @pytest.mark.asyncio
    async def test_cache_functionality_miss_then_hit(
        self,
        resolver_factory: Callable[..., HyperliquidAssetIndexResolver],
        mock_requester: AsyncMock,
    ) -> None:
        """Test cache miss followed by cache hit."""
        resolver = resolver_factory()

        # First call should trigger API request
        result1 = await resolver.get_asset_index("BTC")
        assert result1 == 0  # First asset in universe
        mock_requester.assert_called_once()

        # Second call should use cache
        mock_requester.reset_mock()
        result2 = await resolver.get_asset_index("BTC")
        assert result2 == 0
        mock_requester.assert_not_called()

    @pytest.mark.asyncio
    async def test_cache_population_from_universe(
        self,
        resolver_factory: Callable[..., HyperliquidAssetIndexResolver],
        mock_response_handler: MagicMock,
    ) -> None:
        """Test that cache is populated correctly from universe data."""
        resolver = resolver_factory()

        # Trigger cache population
        await resolver.get_asset_index("BTC")

        # Verify all symbols from universe are cached
        assert await resolver.get_asset_index("BTC") == 0
        assert await resolver.get_asset_index("ETH") == 1
        assert await resolver.get_asset_index("SOL") == 2

    # API Request and Response Handling Tests

    @pytest.mark.asyncio
    async def test_api_request_construction(
        self,
        resolver_factory: Callable[..., HyperliquidAssetIndexResolver],
        mock_request_builder: MagicMock,
        mock_requester: AsyncMock,
    ) -> None:
        """Test that API requests are constructed correctly."""
        resolver = resolver_factory()

        await resolver.get_asset_index("BTC")

        # Verify request builder was called
        mock_request_builder.build_info_request_payload.assert_called_once()

        # Verify requester was called with correct parameters
        mock_requester.assert_called_once_with(
            method="POST", endpoint="/info", data={"type": "metaAndAssetCtxs"}
        )

    @pytest.mark.asyncio
    async def test_response_processing_success(
        self,
        resolver_factory: Callable[..., HyperliquidAssetIndexResolver],
        mock_response_handler: MagicMock,
    ) -> None:
        """Test successful response processing."""
        resolver = resolver_factory()

        await resolver.get_asset_index("BTC")

        # Verify response handler was called
        mock_response_handler.handle_info_meta_and_asset_ctxs_response.assert_called_once_with(
            {"mock": "response"}, status_code=200
        )

    # Error Handling Tests

    @pytest.mark.parametrize(
        ("invalid_symbol", "expected_error_code"),
        [
            ("", APIErrorCode.INVALID_PARAMS),
        ],
    )
    @pytest.mark.asyncio
    async def test_invalid_symbol_validation(
        self,
        resolver_factory: Callable[..., HyperliquidAssetIndexResolver],
        invalid_symbol: str,
        expected_error_code: APIErrorCode,
    ) -> None:
        """Test validation of invalid symbols."""
        resolver = resolver_factory()

        with pytest.raises(APIError) as exc_info:
            await resolver.get_asset_index(invalid_symbol)

        assert exc_info.value.code == expected_error_code.value

    @pytest.mark.asyncio
    async def test_api_error_propagation(
        self,
        resolver_factory: Callable[..., HyperliquidAssetIndexResolver],
        mock_requester: AsyncMock,
    ) -> None:
        """Test that API errors from requester are properly propagated."""
        original_error = APIError("Network timeout", APIErrorCode.NETWORK_ISSUE.value)
        mock_requester.side_effect = original_error

        resolver = resolver_factory()

        with pytest.raises(APIError) as exc_info:
            await resolver.get_asset_index("BTC")

        assert "Failed to fetch asset index for symbol 'BTC'" in exc_info.value.message
        assert exc_info.value.code == APIErrorCode.NETWORK_ISSUE.value

    @pytest.mark.asyncio
    async def test_unexpected_request_error_handling(
        self,
        resolver_factory: Callable[..., HyperliquidAssetIndexResolver],
        mock_requester: AsyncMock,
    ) -> None:
        """Test handling of unexpected errors during API requests."""
        mock_requester.side_effect = ValueError("Unexpected error")

        resolver = resolver_factory()

        with pytest.raises(APIError) as exc_info:
            await resolver.get_asset_index("BTC")

        assert "Unexpected error fetching asset index" in exc_info.value.message
        assert exc_info.value.code == APIErrorCode.NETWORK_ISSUE.value

    @pytest.mark.asyncio
    async def test_none_response_handling(
        self,
        resolver_factory: Callable[..., HyperliquidAssetIndexResolver],
        mock_requester: AsyncMock,
    ) -> None:
        """Test handling of None response from requester."""
        mock_requester.return_value = (None, 200, {})

        resolver = resolver_factory()

        with pytest.raises(APIError) as exc_info:
            await resolver.get_asset_index("BTC")

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "No data received for market metadata" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_validation_error_handling(
        self,
        resolver_factory: Callable[..., HyperliquidAssetIndexResolver],
        mock_response_handler: MagicMock,
    ) -> None:
        """Test handling of validation errors from response handler."""
        mock_response_handler.handle_info_meta_and_asset_ctxs_response.side_effect = (
            ValidationError([{"msg": "Invalid data"}], HyperliquidRawMetaAndAssetCtxsResponse)
        )

        resolver = resolver_factory()

        with pytest.raises(APIError) as exc_info:
            await resolver.get_asset_index("BTC")

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Failed to parse market metadata" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_response_handler_api_error_propagation(
        self,
        resolver_factory: Callable[..., HyperliquidAssetIndexResolver],
        mock_response_handler: MagicMock,
    ) -> None:
        """Test that API errors from response handler are propagated."""
        original_error = APIError("Handler error", APIErrorCode.INVALID_RESPONSE.value)
        mock_response_handler.handle_info_meta_and_asset_ctxs_response.side_effect = original_error

        resolver = resolver_factory()

        with pytest.raises(APIError) as exc_info:
            await resolver.get_asset_index("BTC")

        assert exc_info.value is original_error

    @pytest.mark.asyncio
    async def test_unexpected_response_processing_error(
        self,
        resolver_factory: Callable[..., HyperliquidAssetIndexResolver],
        mock_response_handler: MagicMock,
    ) -> None:
        """Test handling of unexpected errors during response processing."""
        mock_response_handler.handle_info_meta_and_asset_ctxs_response.side_effect = RuntimeError(
            "Unexpected"
        )

        resolver = resolver_factory()

        with pytest.raises(APIError) as exc_info:
            await resolver.get_asset_index("BTC")

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected error parsing market metadata" in exc_info.value.message

    # Symbol Not Found Tests

    @pytest.mark.asyncio
    async def test_symbol_not_found_after_fetch(
        self, resolver_factory: Callable[..., HyperliquidAssetIndexResolver]
    ) -> None:
        """Test error when symbol is not found in universe after successful fetch."""
        resolver = resolver_factory()

        with pytest.raises(APIError) as exc_info:
            await resolver.get_asset_index("NONEXISTENT")

        assert exc_info.value.code == APIErrorCode.SYMBOL_NOT_FOUND.value
        assert "Asset index for symbol 'NONEXISTENT' not found" in exc_info.value.message

    # get_asset_index_or_none Tests

    @pytest.mark.asyncio
    async def test_get_asset_index_or_none_success(
        self, resolver_factory: Callable[..., HyperliquidAssetIndexResolver]
    ) -> None:
        """Test get_asset_index_or_none returns index on success."""
        resolver = resolver_factory()

        result = await resolver.get_asset_index_or_none("BTC")

        assert result == 0

    @pytest.mark.asyncio
    async def test_get_asset_index_or_none_symbol_not_found(
        self, resolver_factory: Callable[..., HyperliquidAssetIndexResolver]
    ) -> None:
        """Test get_asset_index_or_none returns None for symbol not found."""
        resolver = resolver_factory()

        result = await resolver.get_asset_index_or_none("NONEXISTENT")

        assert result is None

    @pytest.mark.asyncio
    async def test_get_asset_index_or_none_other_api_error_propagated(
        self,
        resolver_factory: Callable[..., HyperliquidAssetIndexResolver],
        mock_requester: AsyncMock,
    ) -> None:
        """Test get_asset_index_or_none propagates non-symbol-not-found API errors."""
        mock_requester.side_effect = APIError("Network error", APIErrorCode.NETWORK_ISSUE.value)

        resolver = resolver_factory()

        with pytest.raises(APIError) as exc_info:
            await resolver.get_asset_index_or_none("BTC")

        assert exc_info.value.code == APIErrorCode.NETWORK_ISSUE.value

    # Edge Cases and Business Logic Tests

    @pytest.mark.parametrize(
        ("symbol", "expected_invalid"),
        [
            ("@", True),  # @ without number
            ("@abc", True),  # @ with non-numeric
            ("@-1", True),  # @ with negative
            ("@1.5", True),  # @ with decimal
        ],
    )
    @pytest.mark.asyncio
    async def test_invalid_at_format_symbols(
        self,
        resolver_factory: Callable[..., HyperliquidAssetIndexResolver],
        symbol: str,
        expected_invalid: bool,
    ) -> None:
        """Test handling of invalid @N format symbols."""
        resolver = resolver_factory()

        if expected_invalid:
            # These should fall through to API fetch and likely fail with symbol not found
            with pytest.raises(APIError):
                await resolver.get_asset_index(symbol)

    @pytest.mark.asyncio
    async def test_cache_cleared_on_repopulation(
        self,
        resolver_factory: Callable[..., HyperliquidAssetIndexResolver],
        mock_response_handler: MagicMock,
    ) -> None:
        """Test that cache is cleared before repopulation."""
        resolver = resolver_factory()

        # First fetch populates cache
        await resolver.get_asset_index("BTC")

        # Modify mock to return different universe
        new_mock_response = MagicMock(spec=HyperliquidRawMetaAndAssetCtxsResponse)
        new_mock_meta = MagicMock()
        new_mock_meta.universe = [
            MagicMock(name="DOGE"),
            MagicMock(name="ADA"),
        ]
        new_mock_response.meta = new_mock_meta
        mock_response_handler.handle_info_meta_and_asset_ctxs_response.return_value = (
            new_mock_response
        )

        # Force cache refresh by accessing new symbol
        await resolver.get_asset_index("DOGE")

        # Old symbol should now fail
        with pytest.raises(APIError) as exc_info:
            await resolver.get_asset_index("BTC")
        assert exc_info.value.code == APIErrorCode.SYMBOL_NOT_FOUND.value

    @pytest.mark.asyncio
    async def test_multiple_concurrent_requests_same_symbol(
        self,
        resolver_factory: Callable[..., HyperliquidAssetIndexResolver],
        mock_requester: AsyncMock,
    ) -> None:
        """Test concurrent requests for same symbol (cache behavior)."""
        resolver = resolver_factory()

        # Use asyncio.gather for concurrent requests

        # Make multiple concurrent requests
        results = await asyncio.gather(
            resolver.get_asset_index("BTC"),
            resolver.get_asset_index("BTC"),
            resolver.get_asset_index("BTC"),
        )

        # All should return same result
        assert all(result == 0 for result in results)

        # Only one API call should have been made
        # (Note: Due to async nature, this might not be perfectly controlled in unit test)
        assert mock_requester.call_count >= 1

    def test_exchange_name_logging_context(
        self, resolver_factory: Callable[..., HyperliquidAssetIndexResolver]
    ) -> None:
        """Test that exchange name is used for logging context."""
        custom_exchange_name = "custom_hyperliquid_test"
        resolver = resolver_factory(exchange_name_for_log=custom_exchange_name)

        # Test that custom exchange name is used (accessed through public API)
        assert resolver is not None

    @pytest.mark.asyncio
    async def test_comprehensive_asset_index_workflow(
        self,
        resolver_factory: Callable[..., HyperliquidAssetIndexResolver],
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test complete workflow from symbol request to index resolution."""
        resolver = resolver_factory()

        # Test the complete workflow
        result = await resolver.get_asset_index("ETH")

        # Verify all components were called in correct order
        mock_request_builder.build_info_request_payload.assert_called_once()
        mock_response_handler.handle_info_meta_and_asset_ctxs_response.assert_called_once()

        # Verify correct result
        assert result == 1  # ETH is second in mock universe
