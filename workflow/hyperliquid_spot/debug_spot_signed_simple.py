#!/usr/bin/env python3
"""Debug script to test Hyperliquid spot trading with real signed requests.

Uses the existing test configuration and authentication from the codebase.
"""

import asyncio
import json
from datetime import datetime
from pathlib import Path

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.config.app_settings import AppSettings
from cyberdelta.config.secrets_config import SecretsConfig
from cyberdelta.config.structlog_config import get_logger

logger = get_logger(__name__)

# Load configurations
app_config = AppSettings.from_yaml("tests/config/test_config.yaml")
secrets = SecretsConfig.from_yaml("tests/config/test_secrets.yaml")


class HyperliquidSpotSignedTester:
    """Test Hyperliquid spot operations with real signed requests."""

    def __init__(self):
        self.output_dir = Path("workflow/hyperliquid_spot/debug_output_signed")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.api = None

    async def initialize_api(self):
        """Initialize the Hyperliquid API with test credentials."""
        # Get Hyperliquid config
        hl_config = next(
            (ex for ex in app_config.exchanges if ex.exchange_name == "hyperliquid"),
            None
        )
        if not hl_config:
            raise ValueError("Hyperliquid configuration not found")

        # Get secrets
        hl_secrets = secrets.exchanges.get("hyperliquid")
        if not hl_secrets:
            raise ValueError("Hyperliquid secrets not found")

        # Initialize API
        self.api = HyperliquidAPI(
            network_environment=hl_config.environment,
            private_key=hl_secrets.private_key,
            log_requests=True
        )

        await self.api.initialize()
        logger.info(f"Initialized API with wallet: {self.api.wallet_address}")

    async def save_result(self, filename: str, data: dict):
        """Save test result to file."""
        filepath = self.output_dir / f"{filename}.json"
        with open(filepath, 'w') as f:
            json.dump({
                "timestamp": datetime.now().isoformat(),
                "wallet_address": self.api.wallet_address if self.api else "unknown",
                "data": data
            }, f, indent=2)
        logger.info(f"Saved result to {filepath}")

    async def test_spot_order_placement(self):
        """Test placing a spot order."""
        logger.info("Testing spot order placement...")

        from cyberdelta.apis.models.service_args import PlaceOrderArgs
        from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce
        from decimal import Decimal

        # Try to place an order for @1 token
        order_args = PlaceOrderArgs(
            symbol="@1",  # Spot token
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1"),
            price=Decimal("0.0001"),  # Very low price
            time_in_force=TimeInForce.GTC
        )

        try:
            result = await self.api.place_order(order_args)
            await self.save_result("01_spot_order_success", {
                "request": order_args.model_dump(mode="json"),
                "response": {
                    "order_id": result.exchange_order_id,
                    "status": result.status.value,
                    "symbol": result.symbol,
                    "error": None
                }
            })
        except Exception as e:
            await self.save_result("01_spot_order_error", {
                "request": order_args.model_dump(mode="json"),
                "error": {
                    "type": type(e).__name__,
                    "message": str(e),
                    "details": getattr(e, "__dict__", {})
                }
            })
            logger.error(f"Spot order placement failed: {e}")

    async def test_spot_order_cancellation(self):
        """Test cancelling a spot order."""
        logger.info("Testing spot order cancellation...")

        from cyberdelta.apis.models.service_args import CancelOrderArgs

        # Try to cancel a non-existent spot order
        cancel_args = CancelOrderArgs(
            order_id="999999",
            symbol="@1"
        )

        try:
            result = await self.api.cancel_order(cancel_args)
            await self.save_result("02_spot_cancel_success", {
                "request": cancel_args.model_dump(mode="json"),
                "response": {
                    "success": result.success,
                    "message": result.message,
                    "status": result.status.value
                }
            })
        except Exception as e:
            await self.save_result("02_spot_cancel_error", {
                "request": cancel_args.model_dump(mode="json"),
                "error": {
                    "type": type(e).__name__,
                    "message": str(e),
                    "details": getattr(e, "__dict__", {})
                }
            })
            logger.error(f"Spot order cancellation failed: {e}")

    async def test_spot_symbol_with_slash(self):
        """Test operations with NAME/USDC format symbols."""
        logger.info("Testing NAME/USDC format spot symbol...")

        from cyberdelta.apis.models.service_args import PlaceOrderArgs
        from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce
        from decimal import Decimal

        # Try a hypothetical spot pair
        order_args = PlaceOrderArgs(
            symbol="PURR/USDC",  # Canonical spot format
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("10"),
            price=Decimal("0.001"),
            time_in_force=TimeInForce.GTC
        )

        try:
            result = await self.api.place_order(order_args)
            await self.save_result("03_spot_slash_symbol_success", {
                "request": order_args.model_dump(mode="json"),
                "response": {
                    "order_id": result.exchange_order_id,
                    "status": result.status.value,
                    "symbol": result.symbol
                }
            })
        except Exception as e:
            await self.save_result("03_spot_slash_symbol_error", {
                "request": order_args.model_dump(mode="json"),
                "error": {
                    "type": type(e).__name__,
                    "message": str(e),
                    "details": getattr(e, "__dict__", {})
                }
            })
            logger.error(f"Spot slash symbol order failed: {e}")

    async def test_get_spot_balances(self):
        """Test getting spot balances."""
        logger.info("Testing get spot balances...")

        try:
            # Get all balances
            balances = await self.api.get_balances()

            # Filter for spot balances
            spot_balances = [
                {
                    "asset": b.asset,
                    "available": str(b.available),
                    "total": str(b.total),
                    "held": str(b.held)
                }
                for b in balances
                if b.asset != "USDC"  # Non-USDC assets are spot tokens
            ]

            await self.save_result("04_spot_balances", {
                "total_assets": len(balances),
                "spot_assets": len(spot_balances),
                "balances": spot_balances
            })

        except Exception as e:
            await self.save_result("04_spot_balances_error", {
                "error": {
                    "type": type(e).__name__,
                    "message": str(e)
                }
            })
            logger.error(f"Get spot balances failed: {e}")

    async def test_get_open_orders(self):
        """Test getting open orders to see if spot orders appear."""
        logger.info("Testing get open orders...")

        try:
            orders = await self.api.get_open_orders()

            # Check for any spot orders
            spot_orders = [
                {
                    "order_id": o.exchange_order_id,
                    "symbol": o.symbol,
                    "side": o.side.value,
                    "price": str(o.price),
                    "quantity": str(o.quantity),
                    "status": o.status.value
                }
                for o in orders
                if "@" in o.symbol or "/" in o.symbol
            ]

            await self.save_result("05_open_orders", {
                "total_orders": len(orders),
                "spot_orders": len(spot_orders),
                "orders": spot_orders
            })

        except Exception as e:
            await self.save_result("05_open_orders_error", {
                "error": {
                    "type": type(e).__name__,
                    "message": str(e)
                }
            })
            logger.error(f"Get open orders failed: {e}")

    async def run_all_tests(self):
        """Run all spot trading tests."""
        logger.info("Starting Hyperliquid spot signed tests...")

        try:
            await self.initialize_api()

            # Run tests
            await self.test_spot_order_placement()
            await asyncio.sleep(1)  # Avoid rate limiting

            await self.test_spot_order_cancellation()
            await asyncio.sleep(1)

            await self.test_spot_symbol_with_slash()
            await asyncio.sleep(1)

            await self.test_get_spot_balances()
            await asyncio.sleep(1)

            await self.test_get_open_orders()

            # Create summary
            summary = {
                "wallet_address": self.api.wallet_address,
                "tests_performed": [
                    "spot_order_placement (@1)",
                    "spot_order_cancellation (@1)",
                    "spot_order_slash_symbol (PURR/USDC)",
                    "get_spot_balances",
                    "get_open_orders"
                ],
                "note": "Check individual result files for details"
            }

            await self.save_result("00_test_summary", summary)

        finally:
            if self.api:
                await self.api.close()

        logger.info("All tests completed! Check workflow/hyperliquid_spot/debug_output_signed/")


async def main():
    """Run the test suite."""
    tester = HyperliquidSpotSignedTester()
    await tester.run_all_tests()


if __name__ == "__main__":
    asyncio.run(main())
