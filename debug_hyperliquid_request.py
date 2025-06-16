#!/usr/bin/env python3
"""Debug script to test Hyperliquid request formation."""

import asyncio
import logging
from decimal import Decimal

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.service_args_models import PlaceOrderArgs
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce

# Setup debug logging
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logging.getLogger("cyberdelta").setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)


async def debug_request():
    """Debug the place order request."""
    try:
        # Create API instance (this will use testnet config)
        api = HyperliquidAPI()

        # Define minimal order args
        order_args = PlaceOrderArgs(
            symbol="PURR",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            price=Decimal("0.01"),
            time_in_force=TimeInForce.GTC,
        )

        logger.info("About to place order...")
        logger.info(f"Order args: {order_args}")

        # This should trigger the authentication flow
        placed_order = await api.place_order(order_args)
        logger.info(f"Order placed: {placed_order}")

    except Exception as e:
        logger.error(f"Failed to place order: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    asyncio.run(debug_request())
