#!/usr/bin/env python3
"""Download real signed endpoint responses using existing codebase."""

import asyncio
import json
from pathlib import Path
from datetime import datetime

# Use existing codebase
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.config.app_settings import AppSettings
from cyberdelta.config.secrets_config import SecretsConfig
from decimal import Decimal

async def main():
    """Download signed endpoint responses."""
    # Load configs
    app_config = AppSettings.from_yaml("tests/config/test_config.yaml")
    secrets = SecretsConfig.from_yaml("tests/config/test_secrets.yaml")

    # Get Hyperliquid config
    hl_config = next((ex for ex in app_config.exchanges if ex.exchange_name == "hyperliquid"), None)
    hl_secrets = secrets.exchanges.get("hyperliquid")

    # Initialize API
    api = HyperliquidAPI(
        network_environment=hl_config.environment,
        private_key=hl_secrets.private_key,
        log_requests=True
    )

    await api.initialize()
    print(f"Using wallet: {api.wallet_address}")

    output_dir = Path("workflow/hyperliquid_spot/private_endpoints")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Test different spot operations and save raw responses
    tests = [
        ("spot_order_@1", {
            "symbol": "@1", "side": "BUY", "type": "LIMIT",
            "quantity": Decimal("1"), "price": Decimal("0.0001")
        }),
        ("spot_order_@10", {
            "symbol": "@10", "side": "BUY", "type": "LIMIT",
            "quantity": Decimal("1"), "price": Decimal("0.001")
        }),
        ("spot_order_purr_usdc", {
            "symbol": "PURR/USDC", "side": "BUY", "type": "LIMIT",
            "quantity": Decimal("10"), "price": Decimal("0.001")
        }),
        ("spot_cancel_@1", {
            "order_id": "999999", "symbol": "@1"
        })
    ]

    for test_name, params in tests:
        try:
            print(f"\n=== {test_name} ===")

            if "order_id" in params:
                # Cancel operation
                from cyberdelta.apis.models.service_args import CancelOrderArgs
                result = await api.cancel_order(CancelOrderArgs(**params))
                response_data = {
                    "timestamp": datetime.now().isoformat(),
                    "test_name": test_name,
                    "wallet": api.wallet_address,
                    "operation": "cancel_order",
                    "request": params,
                    "success": True,
                    "result": {
                        "success": result.success,
                        "message": result.message,
                        "status": result.status.value,
                        "symbol": result.symbol,
                        "order_id": result.order_id
                    }
                }
            else:
                # Place order operation
                from cyberdelta.apis.models.service_args import PlaceOrderArgs
                from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce

                order_args = PlaceOrderArgs(
                    symbol=params["symbol"],
                    side=OrderSide(params["side"]),
                    order_type=OrderType(params["type"]),
                    quantity=params["quantity"],
                    price=params["price"],
                    time_in_force=TimeInForce.GTC
                )

                result = await api.place_order(order_args)
                response_data = {
                    "timestamp": datetime.now().isoformat(),
                    "test_name": test_name,
                    "wallet": api.wallet_address,
                    "operation": "place_order",
                    "request": {
                        "symbol": params["symbol"],
                        "side": params["side"],
                        "type": params["type"],
                        "quantity": str(params["quantity"]),
                        "price": str(params["price"])
                    },
                    "success": True,
                    "result": {
                        "order_id": result.exchange_order_id,
                        "status": result.status.value,
                        "symbol": result.symbol,
                        "side": result.side.value,
                        "price": str(result.price),
                        "quantity": str(result.quantity)
                    }
                }

        except Exception as e:
            response_data = {
                "timestamp": datetime.now().isoformat(),
                "test_name": test_name,
                "wallet": api.wallet_address,
                "operation": "place_order" if "order_id" not in params else "cancel_order",
                "request": {k: str(v) for k, v in params.items()},
                "success": False,
                "error": {
                    "type": type(e).__name__,
                    "message": str(e),
                    "details": getattr(e, "__dict__", {})
                }
            }

        # Save response
        with open(output_dir / f"{test_name}.json", 'w') as f:
            json.dump(response_data, f, indent=2)

        print(f"✓ Saved {test_name}.json")
        print(f"  Success: {response_data['success']}")
        if not response_data['success']:
            print(f"  Error: {response_data['error']['message']}")

        await asyncio.sleep(1)  # Rate limiting

    await api.close()
    print(f"\n✅ All endpoint responses saved to {output_dir}/")

if __name__ == "__main__":
    asyncio.run(main())
