#!/usr/bin/env python3
"""Simple debug script to test Hyperliquid spot private endpoints using existing codebase."""

import asyncio
import json
from pathlib import Path
from datetime import datetime

# Use the existing codebase
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.service_args.trading import PlaceOrderArgs, CancelOrderArgs
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.config.app_settings import AppSettings
from cyberdelta.config.secrets_config import SecretsConfig
from decimal import Decimal


async def main():
    # Load test configs
    app_config = AppSettings.from_yaml("tests/config/test_config.yaml")
    secrets = SecretsConfig.from_yaml("tests/config/test_secrets.yaml")

    # Get Hyperliquid config
    hl_config = next(
        (ex for ex in app_config.exchanges if ex.exchange_name == "hyperliquid"),
        None
    )
    hl_secrets = secrets.exchanges.get("hyperliquid")

    # Initialize API
    api = HyperliquidAPI(
        network_environment=hl_config.environment,
        private_key=hl_secrets.private_key,
        log_requests=True
    )

    await api.initialize()
    print(f"Initialized with wallet: {api.wallet_address}")

    output_dir = Path("workflow/hyperliquid_spot/private_endpoints")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Test 1: Place spot order
    print("\n1. Testing spot order placement (@1)...")
    try:
        result = await api.place_order(PlaceOrderArgs(
            symbol="@1",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1"),
            price=Decimal("0.0001"),
            time_in_force=TimeInForce.GTC
        ))

        with open(output_dir / "01_spot_order_success.json", 'w') as f:
            json.dump({
                "timestamp": datetime.now().isoformat(),
                "wallet": api.wallet_address,
                "request": {
                    "symbol": "@1",
                    "side": "BUY",
                    "type": "LIMIT",
                    "quantity": "1",
                    "price": "0.0001"
                },
                "response": {
                    "order_id": result.exchange_order_id,
                    "status": result.status.value,
                    "symbol": result.symbol
                }
            }, f, indent=2)

    except Exception as e:
        with open(output_dir / "01_spot_order_error.json", 'w') as f:
            json.dump({
                "timestamp": datetime.now().isoformat(),
                "wallet": api.wallet_address,
                "error": {
                    "type": type(e).__name__,
                    "message": str(e),
                    "details": getattr(e, "__dict__", {})
                }
            }, f, indent=2)
        print(f"Error: {e}")

    await asyncio.sleep(1)

    # Test 2: Cancel spot order
    print("\n2. Testing spot order cancellation...")
    try:
        result = await api.cancel_order(CancelOrderArgs(
            order_id="999999",
            symbol="@1"
        ))

        with open(output_dir / "02_spot_cancel_success.json", 'w') as f:
            json.dump({
                "timestamp": datetime.now().isoformat(),
                "wallet": api.wallet_address,
                "response": {
                    "success": result.success,
                    "message": result.message,
                    "status": result.status.value
                }
            }, f, indent=2)

    except Exception as e:
        with open(output_dir / "02_spot_cancel_error.json", 'w') as f:
            json.dump({
                "timestamp": datetime.now().isoformat(),
                "wallet": api.wallet_address,
                "error": {
                    "type": type(e).__name__,
                    "message": str(e),
                    "details": getattr(e, "__dict__", {})
                }
            }, f, indent=2)
        print(f"Error: {e}")

    await asyncio.sleep(1)

    # Test 3: Try NAME/USDC format
    print("\n3. Testing NAME/USDC format spot order...")
    try:
        result = await api.place_order(PlaceOrderArgs(
            symbol="PURR/USDC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("10"),
            price=Decimal("0.001"),
            time_in_force=TimeInForce.GTC
        ))

        with open(output_dir / "03_slash_format_success.json", 'w') as f:
            json.dump({
                "timestamp": datetime.now().isoformat(),
                "wallet": api.wallet_address,
                "response": {
                    "order_id": result.exchange_order_id,
                    "status": result.status.value
                }
            }, f, indent=2)

    except Exception as e:
        with open(output_dir / "03_slash_format_error.json", 'w') as f:
            json.dump({
                "timestamp": datetime.now().isoformat(),
                "wallet": api.wallet_address,
                "error": {
                    "type": type(e).__name__,
                    "message": str(e),
                    "details": getattr(e, "__dict__", {})
                }
            }, f, indent=2)
        print(f"Error: {e}")

    await api.close()

    print(f"\nDone! Check {output_dir} for results")


if __name__ == "__main__":
    asyncio.run(main())
