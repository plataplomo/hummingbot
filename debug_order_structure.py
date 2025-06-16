#!/usr/bin/env python3
"""Debug the exact order structure being sent."""

import json
from decimal import Decimal

from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce


def debug_order_structure():
    """Debug the order structure."""
    # Test the request builder directly
    builder = HyperliquidRequestBuilder()

    # Create a test order request
    place_order_request = builder.build_place_order_payload(
        asset_index=3,  # BTC asset index
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=Decimal("0.001"),
        time_in_force=TimeInForce.GTC,
        price=Decimal("30000.0"),
        reduce_only=False,
    )

    print("=== Raw Request Object ===")
    print(f"Type: {type(place_order_request)}")
    print(f"Object: {place_order_request}")

    # Convert to dict with by_alias=True (what should be sent)
    request_dict = place_order_request.model_dump(by_alias=True, exclude_none=False)
    print("\n=== Request Dict (by_alias=True, exclude_none=False) ===")
    print(json.dumps(request_dict, indent=2))

    # Convert to dict with exclude_none=True (what might be sent)
    request_dict_exclude_none = place_order_request.model_dump(by_alias=True, exclude_none=True)
    print("\n=== Request Dict (by_alias=True, exclude_none=True) ===")
    print(json.dumps(request_dict_exclude_none, indent=2))

    # Let's also check individual components
    actions = request_dict["actions"]
    if actions:
        action = actions[0]
        print("\n=== First Action ===")
        print(json.dumps(action, indent=2))

        print("\n=== Field Types ===")
        for key, value in action.items():
            print(f"{key}: {type(value)} = {value}")


if __name__ == "__main__":
    debug_order_structure()
