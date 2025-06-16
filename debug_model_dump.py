#!/usr/bin/env python3
"""Debug script to test model serialization."""

from decimal import Decimal

from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce


def test_model_dump():
    """Test the model dump output."""
    # Build a sample request like the trading service does - match the failing test
    asset_index = 3  # BTC asset index
    side = OrderSide.BUY
    order_type = OrderType.LIMIT
    quantity = Decimal("0.001")
    price = Decimal("50000")  # No decimal point
    time_in_force = TimeInForce.GTC

    # Build the payload model
    request_model = HyperliquidRequestBuilder.build_place_order_payload(
        asset_index=asset_index,
        side=side,
        order_type=order_type,
        quantity=quantity,
        time_in_force=time_in_force,
        price=price,
    )

    print("Request model type:", type(request_model))
    print("Request model:", request_model)

    # Serialize it the same way the trading service does
    serialized = request_model.model_dump(by_alias=True, exclude_none=True)

    print("\nSerialized payload:")
    import json

    print(json.dumps(serialized, indent=2))

    print(f"\nPayload keys: {list(serialized.keys())}")
    if "actions" in serialized:
        print(f"Actions type: {type(serialized['actions'])}")
        print(f"Actions length: {len(serialized['actions'])}")
        if serialized["actions"]:
            print(f"First action keys: {list(serialized['actions'][0].keys())}")


if __name__ == "__main__":
    test_model_dump()
