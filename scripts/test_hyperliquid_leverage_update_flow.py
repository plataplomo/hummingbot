#!/usr/bin/env python3
"""Integration test showing the complete Hyperliquid leverage update flow."""

import asyncio
import logging
from datetime import UTC, datetime
from decimal import Decimal

from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.service_args_models import UpdateAccountSettingsArgs


# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


async def simulate_hyperliquid_leverage_update() -> None:
    """Simulate the complete flow of updating leverage in Hyperliquid."""
    logger.info("=== Hyperliquid Leverage Update Flow ===\n")

    # Step 1: Create update args
    args = UpdateAccountSettingsArgs(
        leverage_limit=Decimal("20"),
        auto_lend=True,  # Will be ignored
        auto_borrow_settlements=False,  # Will be ignored
    )

    logger.info("1. UpdateAccountSettingsArgs created:")
    logger.info(f"   - leverage_limit: {args.leverage_limit}")
    logger.info(f"   - auto_lend: {args.auto_lend} (will be ignored)")
    logger.info("")

    # Step 2: Validate leverage_limit is provided
    if args.leverage_limit is None:
        logger.error("   ERROR: leverage_limit is required for Hyperliquid!")
        raise APIError(
            message="leverage_limit is required for Hyperliquid account settings update.",
            code=APIErrorCode.INVALID_REQUEST.value,
        )

    # Step 3: Convert to integer and validate range
    leverage_int = int(args.leverage_limit)
    logger.info(f"2. Convert leverage to int: {args.leverage_limit} -> {leverage_int}")

    if leverage_int < 1 or leverage_int > 100:
        logger.error(f"   ERROR: Invalid leverage {leverage_int}")
        raise APIError(
            message=f"Invalid leverage value: {leverage_int}. Must be between 1 and 100.",
            code=APIErrorCode.INVALID_REQUEST.value,
        )
    logger.info("   ✓ Leverage is valid")
    logger.info("")

    # Step 4: Simulate getting current positions
    logger.info("3. Get current positions to update leverage for each:")
    mock_positions = [
        {"symbol": "BTC", "asset_index": 0, "size": "0.5"},
        {"symbol": "ETH", "asset_index": 1, "size": "10.0"},
        {"symbol": "SOL", "asset_index": 5, "size": "100.0"},
    ]

    for pos in mock_positions:
        logger.info(f"   - {pos['symbol']}: {pos['size']} contracts")
    logger.info("")

    # Step 5: Update leverage for each position
    logger.info("4. Update leverage for each position:")
    asset_leverage_settings = {}

    for pos in mock_positions:
        # Simulate building request
        logger.info(f"   Updating {pos['symbol']} (asset index {pos['asset_index']}):")
        logger.info("     - Request: POST /exchange")
        logger.info("     - Payload: {")
        logger.info("         'type': 'updateLeverage',")
        logger.info("         'action': {")
        logger.info(f"           'asset': {pos['asset_index']},")
        logger.info("           'isCross': true,")
        logger.info(f"           'leverage': {leverage_int}")
        logger.info("         }")
        logger.info("       }")
        logger.info("     - Requires EIP-712 signature")
        logger.info(f"     ✓ Leverage updated to {leverage_int}x")

        asset_leverage_settings[pos["asset_index"]] = leverage_int
    logger.info("")

    # Step 6: Create AccountSettings response
    logger.info("5. Create AccountSettings response:")
    logger.info("   AccountSettings {")
    logger.info("     exchange: 'hyperliquid',")
    logger.info(f"     timestamp: {datetime.now(UTC).isoformat()},")
    logger.info(f"     leverage_limit: {args.leverage_limit},  // Default for new positions")
    logger.info("     auto_lend: null,  // Not supported")
    logger.info("     auto_borrow_settlements: null,  // Not supported")
    logger.info("     auto_realize_pnl: null,  // Not supported")
    logger.info("     auto_repay_borrows: null,  // Not supported")
    logger.info("     hl_details: {")
    logger.info(f"       asset_leverage_settings: {asset_leverage_settings},")
    logger.info("       cross_margin_enabled: true")
    logger.info("     }")
    logger.info("   }")
    logger.info("")

    logger.info("✅ Hyperliquid leverage update completed successfully!")
    logger.info("")
    logger.info("Key differences from Backpack:")
    logger.info("- Leverage is per-asset, not global")
    logger.info("- Only leverage_limit is supported (no auto-trading settings)")
    logger.info("- Uses EIP-712 signed actions via /exchange endpoint")
    logger.info("- Returns asset_leverage_settings dict in hl_details")


if __name__ == "__main__":
    asyncio.run(simulate_hyperliquid_leverage_update())
