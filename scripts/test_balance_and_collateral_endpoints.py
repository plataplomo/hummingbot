"""Final test script for Backpack API integration."""

import asyncio
from pathlib import Path

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.config.config_manager import ConfigManager
from cyberdelta.config.secrets_manager import SecretsManager
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import SpotBalance


logger = get_logger(__name__)


async def test_spot_balances(api: BackpackAPI) -> dict[str, SpotBalance]:
    """Test spot balances endpoint."""
    logger.info("1. SPOT BALANCES (/api/v1/capital):")

    # Initialize spot_balances
    spot_balances: dict[str, SpotBalance] = {}
    try:
        spot_balances = await api.get_balances()
        logger.info("spot_balance_keys: Available balance symbols", keys=list(spot_balances.keys()))
        logger.info("spot_balance_count: Number of balances", count=len(spot_balances))
    except (OSError, ConnectionError, TimeoutError, ValueError, AttributeError, KeyError) as e:
        logger.exception("balance_fetch_error: Error getting balances", error=str(e))

    if "USDC" in spot_balances:
        usdc = spot_balances["USDC"]
        logger.info(
            "usdc_spot_balance: USDC spot balance details",
            available=usdc.available_quantity,
            total=usdc.total_quantity,
        )
    else:
        logger.info("   USDC: NOT FOUND in spot")

    return spot_balances


def test_collateral_endpoint(api: BackpackAPI) -> None:
    """Test collateral endpoint."""
    logger.info("")
    logger.info("2. COLLATERAL ENDPOINT (/api/v1/capital/collateral):")
    logger.info("   NOTE: The Backpack API client does not currently expose a public method")
    logger.info("   to retrieve collateral data directly. This would require:")
    logger.info("   1. Adding a new method to BackpackAccountService like:")
    logger.info("      async def get_collateral() -> CollateralData")
    logger.info("   2. Or accessing protected internal methods (not recommended)")
    logger.info("")
    logger.info("   For production use, the proper approach would be to implement")
    logger.info("   a public method in BackpackAPI or BackpackAccountService.")


def display_summary(spot_balances: dict[str, SpotBalance]) -> None:
    """Display summary of findings."""
    logger.info("")
    logger.info("3. SUMMARY:")
    spot_usdc = spot_balances.get("USDC")
    if spot_usdc and spot_usdc.available_quantity > 0:
        logger.info("usdc_found_spot: USDC found in SPOT", amount=spot_usdc.available_quantity)
    else:
        spot_amt = spot_usdc.available_quantity if spot_usdc else "None"
        logger.info("usdc_not_found_spot: USDC not found in SPOT", amount=spot_amt)
        logger.info("   🔍 Check collateral endpoint results above for your $1 USDC")


async def test() -> None:
    """Test Backpack API balance and collateral endpoints."""
    # Load configuration using the same approach as test fixtures
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
        logger.info("🔍 TESTING BACKPACK BALANCE ENDPOINTS")
        logger.info("=" * 60)

        # Test spot balances
        spot_balances = await test_spot_balances(api)

        # Test collateral endpoint
        test_collateral_endpoint(api)

        # Display summary
        display_summary(spot_balances)

    finally:
        await api.close()


if __name__ == "__main__":
    asyncio.run(test())
