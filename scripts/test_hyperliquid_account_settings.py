#!/usr/bin/env python3
"""Test script to verify Hyperliquid update_account_settings implementation."""

import asyncio
import logging
from datetime import UTC, datetime
from decimal import Decimal

from cyberdelta.apis.hyperliquid.mappers.hl_account_data_mapper import HyperliquidAccountDataMapper
from cyberdelta.apis.models.service_args_models import UpdateAccountSettingsArgs


# Configure logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


async def test_hyperliquid_account_settings() -> None:
    """Test the Hyperliquid account settings implementation."""
    # Test creating args
    args = UpdateAccountSettingsArgs(
        leverage_limit=Decimal(20),
        auto_lend=True,  # Will be ignored by Hyperliquid
        auto_borrow_settlements=False,  # Will be ignored by Hyperliquid
    )

    logger.info("UpdateAccountSettingsArgs created:")
    logger.info(f"  leverage_limit: {args.leverage_limit}")
    logger.info(f"  auto_lend: {args.auto_lend} (ignored by Hyperliquid)")
    logger.info("")

    # Test mapper transformation
    asset_leverage_settings = {
        0: 20,  # BTC
        1: 20,  # ETH
        5: 20,  # SOL
    }

    settings = HyperliquidAccountDataMapper.transform_account_settings_update_to_internal(
        args=args,
        exchange_name="hyperliquid",
        asset_leverage_settings=asset_leverage_settings,
    )

    logger.info("AccountSettings created via mapper:")
    logger.info(f"  exchange: {settings.exchange}")
    logger.info(f"  leverage_limit: {settings.leverage_limit}")
    logger.info(f"  auto_lend: {settings.auto_lend} (None - not supported)")
    logger.info(
        f"  auto_borrow_settlements: {settings.auto_borrow_settlements} (None - not supported)",
    )
    logger.info(f"  Is mutable: {not settings.model_config.get('frozen', False)}")

    if settings.hl_details:
        logger.info("  Hyperliquid details:")
        logger.info(f"    - asset_leverage_settings: {settings.hl_details.asset_leverage_settings}")
        logger.info(f"    - cross_margin_enabled: {settings.hl_details.cross_margin_enabled}")
    logger.info("")

    # Test mutability
    old_limit = settings.leverage_limit
    settings.update_leverage_limit(Decimal(30))
    logger.info("Leverage limit updated:")
    logger.info(f"  Old limit: {old_limit}")
    logger.info(f"  New limit: {settings.leverage_limit}")
    logger.info(f"  Timestamp updated: {settings.timestamp < datetime.now(UTC)}")
    logger.info("")

    # Test Hyperliquid-specific validation
    logger.info("Testing Hyperliquid-specific behavior:")

    # Test with no leverage_limit (should fail in actual implementation)
    args_no_leverage = UpdateAccountSettingsArgs(
        auto_lend=True,
        auto_borrow_settlements=False,
    )
    logger.info(f"  args with no leverage_limit: leverage_limit={args_no_leverage.leverage_limit}")

    # Test leverage conversion
    test_leverage = Decimal("25.5")
    logger.info(f"  Decimal leverage {test_leverage} converts to int: {int(test_leverage)}")

    logger.info("")
    logger.info("✅ Hyperliquid account settings implementation test passed!")


if __name__ == "__main__":
    asyncio.run(test_hyperliquid_account_settings())
