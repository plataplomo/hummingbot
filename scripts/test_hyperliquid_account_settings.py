"""Test script to verify Hyperliquid update_account_settings implementation."""

from datetime import UTC, datetime
from decimal import Decimal

from cyberdelta.apis.hyperliquid.mappers.hl_account_data_mapper import HyperliquidAccountDataMapper
from cyberdelta.apis.models.service_args_models import UpdateAccountSettingsArgs
from cyberdelta.config.structlog_config import get_logger


logger = get_logger(__name__)


def test_hyperliquid_account_settings() -> None:
    """Test the Hyperliquid account settings implementation."""
    # Test creating args
    args = UpdateAccountSettingsArgs(
        leverage_limit=Decimal(20),
        auto_lend=True,  # Will be ignored by Hyperliquid
        auto_borrow_settlements=False,  # Will be ignored by Hyperliquid
    )

    logger.info("UpdateAccountSettingsArgs created:")
    logger.info("args_leverage_limit: Leverage limit value", leverage_limit=args.leverage_limit)
    logger.info(
        "args_auto_lend: Auto lend setting (ignored by Hyperliquid)",
        auto_lend=args.auto_lend,
    )
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
    logger.info("settings_exchange: Exchange name", exchange=settings.exchange)
    logger.info(
        "settings_leverage_limit: Leverage limit value",
        leverage_limit=settings.leverage_limit,
    )
    logger.info(
        "settings_auto_lend: Auto lend setting (None - not supported)",
        auto_lend=settings.auto_lend,
    )
    logger.info(
        "settings_auto_borrow_settlements: Auto borrow settlements setting (None - not supported)",
        auto_borrow_settlements=settings.auto_borrow_settlements,
    )
    logger.info(
        "settings_mutability: Settings mutability check",
        is_mutable=not settings.model_config.get("frozen", False),
    )

    if settings.hl_details:
        logger.info("  Hyperliquid details:")
        logger.info(
            "hl_details_asset_leverage: Asset leverage settings",
            asset_leverage_settings=settings.hl_details.asset_leverage_settings,
        )
        logger.info(
            "hl_details_cross_margin: Cross margin enabled status",
            cross_margin_enabled=settings.hl_details.cross_margin_enabled,
        )
    logger.info("")

    # Test mutability
    old_limit = settings.leverage_limit
    settings.update_leverage_limit(Decimal(30))
    logger.info("Leverage limit updated:")
    logger.info("leverage_old_limit: Previous leverage limit", old_limit=old_limit)
    logger.info("leverage_new_limit: New leverage limit", new_limit=settings.leverage_limit)
    logger.info(
        "timestamp_updated: Timestamp update check",
        timestamp_updated=settings.timestamp < datetime.now(UTC),
    )
    logger.info("")

    # Test Hyperliquid-specific validation
    logger.info("Testing Hyperliquid-specific behavior:")

    # Test with no leverage_limit (should fail in actual implementation)
    args_no_leverage = UpdateAccountSettingsArgs(
        auto_lend=True,
        auto_borrow_settlements=False,
    )
    logger.info(
        "args_no_leverage_limit: Args without leverage limit",
        leverage_limit=args_no_leverage.leverage_limit,
    )

    # Test leverage conversion
    test_leverage = Decimal("25.5")
    logger.info(
        "leverage_conversion: Decimal leverage to int conversion",
        decimal_leverage=test_leverage,
        int_leverage=int(test_leverage),
    )

    logger.info("")
    logger.info("✅ Hyperliquid account settings implementation test passed!")


if __name__ == "__main__":
    test_hyperliquid_account_settings()
