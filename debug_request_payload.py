#!/usr/bin/env python3
"""Debug script to examine the exact request payload being sent to Hyperliquid."""

import asyncio
import json
from decimal import Decimal

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.service_args_models import PlaceOrderArgs
from cyberdelta.config.config_manager import ConfigManager
from cyberdelta.config.secrets_manager import SecretsManager
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce


async def debug_request_payload():
    """Debug the exact request payload being sent."""
    # Load actual test configuration
    config_manager = ConfigManager("tests/config/test_config.yaml")
    app_settings = config_manager.get_app_settings()
    hl_config = app_settings.exchanges["hyperliquid"]

    # Load test secrets
    secrets_manager = SecretsManager("tests/config/test_secrets.yaml")
    if secrets_manager.secrets_data is None:
        print("Error: Could not load test secrets")
        return
    hl_secrets = secrets_manager.secrets_data.exchanges["hyperliquid"]

    # Create API instance
    api = HyperliquidAPI(
        exchange_config=hl_config,
        exchange_secrets=hl_secrets,
    )

    # Create test order
    order_args = PlaceOrderArgs(
        symbol="BTC",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=Decimal("0.001"),
        price=Decimal("30000.0"),
        time_in_force=TimeInForce.GTC,
    )

    print("=== Order Args ===")
    print(f"Symbol: {order_args.symbol}")
    print(f"Side: {order_args.side}")
    print(f"Order Type: {order_args.order_type}")
    print(f"Quantity: {order_args.quantity}")
    print(f"Price: {order_args.price}")
    print(f"Time in Force: {order_args.time_in_force}")

    # Get the asset index
    try:
        asset_index = await api.get_asset_index("BTC")
        print(f"\nAsset index for BTC: {asset_index}")
    except Exception as e:
        print(f"Error getting asset index: {e}")
        return

    # Build the request payload manually to see what it looks like
    try:
        # Access the private method to build the payload
        request_builder = api._request_builder

        place_order_request = request_builder.build_place_order_payload(
            asset_index=asset_index,
            side=order_args.side,
            order_type=order_args.order_type,
            quantity=order_args.quantity,
            time_in_force=order_args.time_in_force,
            price=order_args.price,
            reduce_only=False,
        )

        print("\n=== Raw Request Object ===")
        print(place_order_request)

        # Convert to dict and examine the JSON
        request_dict = place_order_request.model_dump(by_alias=True, exclude_none=True)
        print("\n=== Request Dict (exclude_none=True) ===")
        print(json.dumps(request_dict, indent=2))

        # Try with exclude_none=False to see all fields
        request_dict_with_none = place_order_request.model_dump(by_alias=True, exclude_none=False)
        print("\n=== Request Dict (exclude_none=False) ===")
        print(json.dumps(request_dict_with_none, indent=2, default=str))

        # Apply the cleaning logic and see what changes
        import copy

        cleaned_dict = copy.deepcopy(request_dict)
        authenticator = api._authenticator
        authenticator._clean_order_type_fields(cleaned_dict)
        authenticator._lowercase_addresses_in_payload(cleaned_dict)

        print("\n=== Cleaned Request Dict ===")
        print(json.dumps(cleaned_dict, indent=2))

    except Exception as e:
        print(f"Error building request payload: {e}")
        import traceback

        traceback.print_exc()

    await api.close()


if __name__ == "__main__":
    asyncio.run(debug_request_payload())
