#!/usr/bin/env python3
"""Test minimal order placement to isolate the HTTP 422 issue."""

import asyncio
import logging
from decimal import Decimal

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.service_args_models import PlaceOrderArgs
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_minimal_order():
    """Test the simplest possible order to isolate the issue."""
    try:
        # Create API instance using existing test config
        from cyberdelta.config.config_loader import load_config_with_secrets

        config_data = load_config_with_secrets()

        api = HyperliquidAPI(
            exchange_config=config_data.exchange_configs.hyperliquid,
            exchange_secrets=config_data.secrets.hyperliquid,
        )

        logger.info("API created successfully")

        # Test with smallest possible order
        # Use values that should definitely pass validation
        minimal_order = PlaceOrderArgs(
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.001"),  # Very small quantity
            price=Decimal("100000"),  # High price to avoid fills
            time_in_force=TimeInForce.GTC,
        )

        logger.info(f"Placing minimal order: {minimal_order}")

        # This should either work or give us the exact error
        result = await api.place_order(minimal_order)
        logger.info(f"Success! Order result: {result}")

        # Clean up - cancel the order
        try:
            from cyberdelta.apis.models.service_args_models import CancelOrderArgs

            if result.exchange_order_id:
                cancel_args = CancelOrderArgs(order_id=result.exchange_order_id, symbol="BTC")
                await api.cancel_order(cancel_args)
                logger.info("Order cancelled successfully")
        except Exception as e:
            logger.warning(f"Failed to cancel order: {e}")

    except Exception as e:
        logger.error(f"Order placement failed: {e}")
        # Print more details
        if hasattr(e, "http_status"):
            logger.error(f"HTTP Status: {e.http_status}")
        if hasattr(e, "exchange_message"):
            logger.error(f"Exchange message: {e.exchange_message}")
        if hasattr(e, "code"):
            logger.error(f"Error code: {e.code}")
        raise


if __name__ == "__main__":
    asyncio.run(test_minimal_order())
