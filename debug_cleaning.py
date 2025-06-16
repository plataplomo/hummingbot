#!/usr/bin/env python3
"""Debug the order cleaning process."""

import json
from typing import Any, TypeGuard


def _is_dict_str_any(value: object) -> TypeGuard[dict[str, Any]]:
    """Type guard to check if value is a dict[str, Any]."""
    return isinstance(value, dict)


def _is_list_any(value: object) -> TypeGuard[list[Any]]:
    """Type guard to check if value is a list."""
    return isinstance(value, list)


def _clean_order_type_fields(data: dict[str, Any]) -> None:
    """Recursively clean None values from order type structures in JSON payload.

    This is specifically for Hyperliquid API which expects order types to have
    only the active field (limit OR market), not both with one as null.
    Also removes other null fields that should be omitted.
    """
    # Handle order type structures: {"limit": {...}, "market": null} -> {"limit": {...}}
    if "limit" in data and "market" in data:
        # This looks like an order type structure
        if data["limit"] is None and data["market"] is not None:
            del data["limit"]
        elif data["market"] is None and data["limit"] is not None:
            del data["market"]

    # Remove any null fields (except for specific cases where null is meaningful)
    keys_to_remove = []
    for key, value in data.items():
        if value is None:
            keys_to_remove.append(key)

    for key in keys_to_remove:
        del data[key]

    # Recursively clean nested structures
    for value in data.values():
        if _is_dict_str_any(value):
            # TypeGuard confirms it's a dict[str, Any]
            _clean_order_type_fields(value)
        elif _is_list_any(value):
            # Use TypeGuard to properly type the list
            for item in value:
                if _is_dict_str_any(item):
                    # TypeGuard confirms it's a dict[str, Any]
                    _clean_order_type_fields(item)


def test_cleaning():
    """Test the cleaning function."""
    # Test data with null values
    test_data = {
        "type": "order",
        "actions": [
            {
                "asset": 3,
                "isBuy": True,
                "limitPx": "30000.0",
                "sz": "0.001",
                "reduceOnly": False,
                "orderType": {
                    "limit": {"tif": "Gtc"},
                    "market": None,  # This should be removed
                },
                "trigger": None,  # This should be removed
                "cloid": None,  # This should be removed
            }
        ],
    }

    print("=== Before Cleaning ===")
    print(json.dumps(test_data, indent=2))

    # Apply cleaning
    _clean_order_type_fields(test_data)

    print("\n=== After Cleaning ===")
    print(json.dumps(test_data, indent=2))


if __name__ == "__main__":
    test_cleaning()
