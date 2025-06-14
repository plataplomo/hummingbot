#!/usr/bin/env python3
"""Final test script for Backpack API integration."""

import asyncio
import logging
from pathlib import Path

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.config.config_manager import ConfigManager
from cyberdelta.config.secrets_manager import SecretsManager

# Set up logging instead of using print
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


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

        logger.info("1. SPOT BALANCES (/api/v1/capital):")

        # Get raw response directly to debug format
        raw_data, status_code, _ = await api.account_service._http_client_requester(
            method="GET",
            endpoint="/api/v1/capital",
            params={},
            is_signed=True,
            endpoint_group="private",
            request_weight=1,
        )
        logger.info(f"   Raw API response: {raw_data}")
        logger.info(f"   Raw API response type: {type(raw_data)}")

        try:
            spot_balances = await api.get_balances()
            logger.info(f"   Keys: {list(spot_balances.keys())}")
        except Exception as e:
            logger.error(f"   Error getting balances: {e}")
        if "USDC" in spot_balances:
            usdc = spot_balances["USDC"]
            logger.info(
                f"   USDC Spot: available=${usdc.available_quantity}, total=${usdc.total_quantity}"
            )
        else:
            logger.info("   USDC: NOT FOUND in spot")

        logger.info("")
        logger.info("2. COLLATERAL ENDPOINT (/api/v1/capital/collateral):")
        try:
            # Use the HTTP client requester directly
            http_client_requester = api.account_service._http_client_requester

            # Make request to collateral endpoint
            raw_data, status_code, headers = await http_client_requester(
                method="GET",
                endpoint="/api/v1/capital/collateral",
                params={},
                is_signed=True,
                endpoint_group="private",
                request_weight=1,
            )

            logger.info(f"   Status: {status_code}")
            if status_code == 200 and raw_data is not None:
                logger.info(f"   Collateral response: {raw_data}")
                if isinstance(raw_data, dict):
                    # Check for USDC in collateral array
                    if "collateral" in raw_data and isinstance(raw_data["collateral"], list):
                        for asset in raw_data["collateral"]:
                            if isinstance(asset, dict) and asset.get("symbol") == "USDC":
                                logger.info("   🎯 FOUND USDC IN COLLATERAL:")
                                logger.info(
                                    f"      - Total Quantity: {asset.get('totalQuantity', 'N/A')}"
                                )
                                logger.info(
                                    f"      - Lend Quantity: {asset.get('lendQuantity', 'N/A')}"
                                )
                                logger.info(
                                    f"      - Available Quantity: {asset.get('availableQuantity', 'N/A')}"
                                )
                                logger.info(
                                    f"      - Collateral Value: ${asset.get('collateralValue', 'N/A')}"
                                )

                    # Also show summary values
                    logger.info("   Summary:")
                    logger.info(f"      - Net Equity: ${raw_data.get('netEquity', 'N/A')}")
                    logger.info(f"      - Assets Value: ${raw_data.get('assetsValue', 'N/A')}")
            else:
                logger.info(f"   Error: Status {status_code}, Data: {raw_data}")

        except Exception as e:
            logger.exception(f"   Exception: {e}")

        logger.info("")
        logger.info("3. SUMMARY:")
        spot_usdc = spot_balances.get("USDC")
        if spot_usdc and spot_usdc.available_quantity > 0:
            logger.info(f"   ✅ USDC found in SPOT: ${spot_usdc.available_quantity}")
        else:
            spot_amt = spot_usdc.available_quantity if spot_usdc else "None"
            logger.info(f"   ❌ USDC in SPOT: ${spot_amt}")
            logger.info("   🔍 Check collateral endpoint results above for your $1 USDC")

    finally:
        await api.close()


if __name__ == "__main__":
    asyncio.run(test())
