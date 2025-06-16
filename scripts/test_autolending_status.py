#!/usr/bin/env python3
"""Test script to check Backpack autolending status and its effect on balances."""

import asyncio
import logging
from pathlib import Path

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.config.config_manager import ConfigManager
from cyberdelta.config.secrets_manager import SecretsManager

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


async def get_autolend_status(api: BackpackAPI) -> bool | None:
    """Get the autoLend status from account settings."""
    logger.info("1. ACCOUNT SETTINGS (/api/v1/account):")
    logger.info("   NOTE: The Backpack API client does not currently expose a public method")
    logger.info("   to retrieve account settings (autoLend, autoBorrowSettlements, etc.)")
    logger.info("   This functionality would require accessing internal/protected methods.")
    logger.info("")
    logger.info("   To properly implement this, the BackpackAPI would need a new method like:")
    logger.info("   async def get_account_settings() -> AccountSettings")
    logger.info("")
    return None


async def display_spot_balances(api: BackpackAPI) -> None:
    """Display spot balances."""
    spot_balances = await api.get_balances()
    logger.info("   Spot Balances (/api/v1/capital):")
    for symbol, balance in spot_balances.items():
        if balance.total_quantity > 0 or symbol in ["USDC", "SOL"]:
            logger.info(
                f"   {symbol}: total={balance.total_quantity}, "
                f"available={balance.available_quantity}"
            )


async def display_collateral_data(api: BackpackAPI) -> None:
    """Display collateral data."""
    logger.info("\n   Collateral Data (/api/v1/capital/collateral):")
    logger.info("   NOTE: The Backpack API client does not currently expose a public method")
    logger.info("   to retrieve collateral data directly. This would require accessing")
    logger.info("   internal/protected methods or implementing a new service method like:")
    logger.info("   async def get_collateral() -> CollateralData")
    logger.info("")
    logger.info("   For production use, consider implementing this method in the BackpackAPI")
    logger.info("   or BackpackAccountService to provide proper access to collateral data.")


def analyze_autolend_status(auto_lend: bool | None) -> None:
    """Analyze and display the meaning of autoLend status."""
    logger.info("")
    logger.info("3. ANALYSIS:")

    # Analyze the relationship
    if auto_lend is True:
        logger.info("   ✅ autoLend is ENABLED")
        logger.info("   - Spot balance endpoint shows zeros because funds are auto-staked")
        logger.info("   - Use collateral endpoint to get true balances")
        logger.info("   - lendQuantity shows the amount that's earning yield")
    else:
        logger.info("   ❌ autoLend is DISABLED")
        logger.info("   - Spot balance endpoint should show actual balances")
        logger.info("   - Collateral endpoint also shows balances but with more detail")


async def test() -> None:
    """Test autolending status and its effect on balance endpoints."""
    # Load configuration
    config_path = Path("tests/config/test_config.yaml")
    secrets_path = Path("tests/config/test_secrets.yaml")

    config_manager = ConfigManager(str(config_path))
    secrets_manager = SecretsManager(str(secrets_path))

    app_settings = config_manager.settings
    secrets_config = secrets_manager.secrets_data

    if app_settings is None:
        raise RuntimeError("Failed to load application settings")
    if secrets_config is None:
        raise RuntimeError("Failed to load secrets configuration")

    bp_config = app_settings.exchanges["backpack"]
    bp_secrets = secrets_config.exchanges["backpack"]

    api = BackpackAPI(exchange_config=bp_config, exchange_secrets=bp_secrets)

    try:
        logger.info("🔍 TESTING BACKPACK AUTOLENDING STATUS")
        logger.info("=" * 60)

        # 1. Get account summary to check autoLend setting
        auto_lend = await get_autolend_status(api)

        logger.info("")
        logger.info("2. BALANCE COMPARISON:")

        # 2. Get spot balances
        await display_spot_balances(api)

        # 3. Get collateral data
        await display_collateral_data(api)

        # 4. Analyze the relationship
        analyze_autolend_status(auto_lend)

    finally:
        await api.close()


if __name__ == "__main__":
    asyncio.run(test())
