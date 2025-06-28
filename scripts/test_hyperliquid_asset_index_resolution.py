#!/usr/bin/env python3
"""Test script to verify Hyperliquid asset index resolution is working properly."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

from cyberdelta.apis.common import APIError
from cyberdelta.apis.hyperliquid.hl_asset_indexer import HyperliquidAssetIndexResolver
from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.hl_response_handler import HyperliquidResponseHandler
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetDefinition,
    HyperliquidRawMetaAndAssetCtxsResponse,
    HyperliquidRawMetaResponse,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.utils.typing import ParsedJsonResponse


# Test constants for expected asset indices in mock data
EXPECTED_SOL_INDEX = 5  # SOL is at index 5 in the mock universe array

logger = get_logger(__name__)


async def test_asset_index_resolution() -> None:
    """Test the asset index resolution functionality."""
    logger.info("=== Testing Hyperliquid Asset Index Resolution ===\n")

    # Create mock dependencies
    mock_requester = AsyncMock()
    mock_response_handler = MagicMock(spec=HyperliquidResponseHandler)
    mock_request_builder = MagicMock(spec=HyperliquidRequestBuilder)

    # Create the asset indexer
    indexer = HyperliquidAssetIndexResolver(
        requester=mock_requester,
        response_handler=mock_response_handler,
        request_builder=mock_request_builder,
        exchange_name_for_log="test_hyperliquid",
    )

    # Mock the request builder to return a proper payload
    mock_request_builder.build_info_request_payload.return_value = MagicMock(
        model_dump=lambda by_alias=True, exclude_none=True: {"type": "metaAndAssetCtxs"},
    )

    # Mock the API response with sample asset metadata
    mock_api_response: ParsedJsonResponse = [
        {
            "universe": [
                {"name": "BTC", "szDecimals": 8},
                {"name": "ETH", "szDecimals": 18},
                {"name": "ARB", "szDecimals": 18},
                {"name": "OP", "szDecimals": 18},
                {"name": "MATIC", "szDecimals": 18},
                {"name": "SOL", "szDecimals": 9},
            ],
        },
        [],  # Empty asset contexts for this test
    ]

    mock_requester.return_value = (mock_api_response, 200, {})

    # Mock the response handler to parse and return validated response
    mock_validated_response = HyperliquidRawMetaAndAssetCtxsResponse(
        meta=HyperliquidRawMetaResponse(
            universe=[
                HyperliquidRawAssetDefinition(
                    name="BTC",
                    szDecimals=8,
                    maxLeverage=50,
                    onlyIsolated=False,
                    marginTableId=0,
                    isDelisted=False,
                ),
                HyperliquidRawAssetDefinition(
                    name="ETH",
                    szDecimals=18,
                    maxLeverage=50,
                    onlyIsolated=False,
                    marginTableId=0,
                    isDelisted=False,
                ),
                HyperliquidRawAssetDefinition(
                    name="ARB",
                    szDecimals=18,
                    maxLeverage=20,
                    onlyIsolated=False,
                    marginTableId=1,
                    isDelisted=False,
                ),
                HyperliquidRawAssetDefinition(
                    name="OP",
                    szDecimals=18,
                    maxLeverage=20,
                    onlyIsolated=False,
                    marginTableId=1,
                    isDelisted=False,
                ),
                HyperliquidRawAssetDefinition(
                    name="MATIC",
                    szDecimals=18,
                    maxLeverage=20,
                    onlyIsolated=False,
                    marginTableId=1,
                    isDelisted=False,
                ),
                HyperliquidRawAssetDefinition(
                    name="SOL",
                    szDecimals=9,
                    maxLeverage=50,
                    onlyIsolated=False,
                    marginTableId=0,
                    isDelisted=False,
                ),
            ],
            marginTables=None,  # Optional field - use alias
        ),
        asset_ctxs=[],
    )

    mock_response_handler.handle_info_meta_and_asset_ctxs_response.return_value = (
        mock_validated_response
    )

    # Test 1: Fetch asset index for BTC (should be 0)
    logger.info("Test 1: Fetching asset index for BTC")
    btc_index = await indexer.get_asset_index("BTC")
    logger.info("btc_index: BTC asset index", btc_index=btc_index)
    if btc_index != 0:
        raise AssertionError(f"Expected BTC index to be 0, got {btc_index}")
    logger.info("  ✓ BTC index is correct\n")

    # Test 2: Fetch asset index for ETH (should be 1)
    logger.info("Test 2: Fetching asset index for ETH (from cache)")
    eth_index = await indexer.get_asset_index("ETH")
    logger.info("eth_index: ETH asset index", eth_index=eth_index)
    if eth_index != 1:
        raise AssertionError(f"Expected ETH index to be 1, got {eth_index}")
    logger.info("  ✓ ETH index is correct\n")

    # Test 3: Fetch asset index for SOL (should be 5)
    logger.info("Test 3: Fetching asset index for SOL (from cache)")
    sol_index = await indexer.get_asset_index("SOL")
    logger.info("sol_index: SOL asset index", sol_index=sol_index)
    if sol_index != EXPECTED_SOL_INDEX:
        raise AssertionError(f"Expected SOL index to be {EXPECTED_SOL_INDEX}, got {sol_index}")
    logger.info("  ✓ SOL index is correct\n")

    # Test 4: Verify caching works (API should only be called once)
    logger.info("Test 4: Verifying caching behavior")
    logger.info("api_call_count: API call count verification", call_count=mock_requester.call_count)
    if mock_requester.call_count != 1:
        raise AssertionError("API should only be called once due to caching")
    logger.info("  ✓ Caching is working correctly\n")

    # Test 5: Test invalid symbol
    logger.info("Test 5: Testing invalid symbol handling")
    try:
        await indexer.get_asset_index("INVALID_SYMBOL")
        logger.error("  ✗ Should have raised an error for invalid symbol")
        raise AssertionError("Should have raised APIError for invalid symbol")
    except APIError as e:
        logger.info("api_error_raised: Correctly raised APIError", error_message=e.message)

    # Test 6: Verify known assets are in cache
    logger.info("Test 6: Verifying known assets are cached:")
    # We know these were fetched, so they should resolve from cache
    known_assets = [("BTC", 0), ("ETH", 1), ("SOL", EXPECTED_SOL_INDEX)]
    for symbol, expected_index in known_assets:
        index = await indexer.get_asset_index(symbol)
        logger.info("asset_index_cached: Cached asset index", symbol=symbol, index=index)
        if index != expected_index:
            raise AssertionError(f"Expected {symbol} to have index {expected_index}, got {index}")
    logger.info("  ✓ All known assets verified")

    logger.info("\n✅ All asset index resolution tests passed!")
    logger.info("\nKey takeaways:")
    logger.info("- Asset indices are fetched dynamically from Hyperliquid's /info endpoint")
    logger.info("- Results are cached for efficiency")
    logger.info("- No hardcoded mappings are used")
    logger.info("- The resolver handles errors gracefully")


if __name__ == "__main__":
    asyncio.run(test_asset_index_resolution())
