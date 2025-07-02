#!/usr/bin/env python3
"""Integration test showing the complete Hyperliquid leverage update flow."""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.hyperliquid.services.hl_account_service import MAX_LEVERAGE_VALUE
from cyberdelta.apis.models.service_args_models import UpdateAccountSettingsArgs
from cyberdelta.config.structlog_config import get_logger


logger = get_logger(__name__)


def simulate_hyperliquid_leverage_update() -> None:
    """Simulate the complete flow of updating leverage in Hyperliquid."""
    logger.info("=== Hyperliquid Leverage Update Flow ===\n")

    # Step 1: Create update args
    args = UpdateAccountSettingsArgs(
        leverage_limit=Decimal(20),
        auto_lend=True,  # Will be ignored
        auto_borrow_settlements=False,  # Will be ignored
    )

    logger.info("1. UpdateAccountSettingsArgs created:")
    logger.info("leverage_limit: Setting leverage limit", leverage_limit=args.leverage_limit)
    logger.info("auto_lend: Setting auto-lend (will be ignored)", auto_lend=args.auto_lend)
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
    logger.info(
        "leverage_conversion: Converting leverage to int",
        from_value=args.leverage_limit,
        to_value=leverage_int,
    )

    if leverage_int < 1 or leverage_int > MAX_LEVERAGE_VALUE:
        logger.error("invalid_leverage: Invalid leverage value", leverage=leverage_int)
        raise APIError(
            message=(
                f"Invalid leverage value: {leverage_int}. "
                f"Must be between 1 and {MAX_LEVERAGE_VALUE}."
            ),
            code=APIErrorCode.INVALID_REQUEST.value,
        )
    logger.info("   ✓ Leverage is valid")
    logger.info("")

    # Step 4: Simulate getting current positions
    logger.info("3. Get current positions to update leverage for each:")
    mock_positions: list[dict[str, Any]] = [
        {"symbol": "BTC", "asset_index": 0, "size": "0.5"},
        {"symbol": "ETH", "asset_index": 1, "size": "10.0"},
        {"symbol": "SOL", "asset_index": 5, "size": "100.0"},
    ]

    for pos in mock_positions:
        logger.info(
            "position_info: Current position",
            symbol=pos["symbol"],
            size=pos["size"],
            contracts="contracts",
        )
    logger.info("")

    # Step 5: Update leverage for each position
    logger.info("4. Update leverage for each position:")
    asset_leverage_settings: dict[int, int] = {}

    for pos in mock_positions:
        # Simulate building request
        logger.info(
            "updating_leverage: Updating leverage for position",
            symbol=pos["symbol"],
            asset_index=pos["asset_index"],
        )
        logger.info("     - Request: POST /exchange")
        logger.info("     - Payload: {")
        logger.info("         'type': 'updateLeverage',")
        logger.info("         'action': {")
        logger.info("request_asset: Asset index in request", asset=pos["asset_index"])
        logger.info("           'isCross': true,")
        logger.info("request_leverage: Leverage value in request", leverage=leverage_int)
        logger.info("         }")
        logger.info("       }")
        logger.info("     - Requires EIP-712 signature")
        logger.info("leverage_updated: Leverage update complete", leverage=leverage_int, unit="x")

        asset_leverage_settings[int(pos["asset_index"])] = leverage_int
    logger.info("")

    # Step 6: Create AccountSettings response
    logger.info("5. Create AccountSettings response:")
    logger.info("   AccountSettings {")
    logger.info("     exchange: 'hyperliquid',")
    logger.info("response_timestamp: Response timestamp", timestamp=datetime.now(UTC).isoformat())
    logger.info(
        "response_leverage_limit: Default leverage for new positions",
        leverage_limit=args.leverage_limit,
    )
    logger.info("     auto_lend: null,  // Not supported")
    logger.info("     auto_borrow_settlements: null,  // Not supported")
    logger.info("     auto_realize_pnl: null,  // Not supported")
    logger.info("     auto_repay_borrows: null,  // Not supported")
    logger.info("     hl_details: {")
    logger.info(
        "asset_leverage_settings: Asset-specific leverage settings",
        settings=asset_leverage_settings,
    )
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
    simulate_hyperliquid_leverage_update()
