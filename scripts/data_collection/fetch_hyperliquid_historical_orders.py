#!/usr/bin/env python3
"""Fetch historical orders from Hyperliquid API and save as raw JSON.

This script downloads all historical orders for a specified wallet address
and saves them to the test fixtures directory for use in tests.
"""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import requests

# Configuration
WALLET_ADDRESS = "0x02Cd79f858bEF99588Cc2E650f2A4Fbf2baE8CB5"
API_URL = "https://api.hyperliquid-testnet.xyz/info"
OUTPUT_DIR = Path("/workspaces/CyberDeltaEngine/tests/fixtures/raw_api_data/hl/historicalOrders")


def fetch_historical_orders(wallet_address: str) -> dict[str, Any]:
    """Fetch historical orders for a wallet address."""
    payload = {"type": "historicalOrders", "user": wallet_address}

    # Fetching historical orders for wallet

    try:
        response = requests.post(API_URL, json=payload, timeout=30)
        response.raise_for_status()

        data: list[dict[str, Any]] = response.json()
        # Successfully fetched orders

        return {
            "wallet": wallet_address,
            "fetched_at": datetime.now(UTC).isoformat(),
            "endpoint": "historicalOrders",
            "api_url": API_URL,
            "orders": data,
        }

    except requests.exceptions.RequestException:
        # Error fetching data
        raise


def save_json(data: dict[str, Any], output_dir: Path) -> None:
    """Save data as JSON file."""
    # Create directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate filename with timestamp
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    filename = f"historicalOrders_{data['wallet'][:8]}_{timestamp}.json"
    filepath = output_dir / filename

    # Save JSON with pretty formatting
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2, sort_keys=True)

    # Saved data to file

    # Also save a "latest" version for easy access
    latest_filepath = output_dir / f"historicalOrders_{data['wallet'][:8]}_latest.json"
    with open(latest_filepath, "w") as f:
        json.dump(data, f, indent=2, sort_keys=True)

    # Also saved as latest file


def main() -> None:
    """Main function."""
    # Hyperliquid Historical Orders Fetcher

    # Fetch data
    data = fetch_historical_orders(WALLET_ADDRESS)

    # Analyze the data
    orders_raw = data.get("orders", [])
    if isinstance(orders_raw, list) and orders_raw:
        # Type assertion: we know orders_raw is a list at this point
        orders = cast(list[dict[str, Any]], orders_raw)
        # Order Analysis
        _ = len(orders)  # Analysis completed

        # Check fields in first order
        first_order: dict[str, Any] = orders[0]
        # Fields in orders

        # Check for different order types/statuses
        order_types: set[str] = set()
        sides: set[str] = set()
        coins: set[str] = set()

        order: dict[str, Any]
        for order in orders:
            order_type = order.get("orderType")
            if isinstance(order_type, str):
                order_types.add(order_type)
            side = order.get("side")
            if isinstance(side, str):
                sides.add(side)
            coin = order.get("coin")
            if isinstance(coin, str):
                coins.add(coin)

        # Analysis completed

        # Check if these are orders or fills
        if "dir" in first_order and "closedPnl" in first_order:
            # NOTE: These appear to be fills/trades, not orders!
            pass
    else:
        # No orders found for this wallet
        pass

    # Save data
    save_json(data, OUTPUT_DIR)

    # Done!


if __name__ == "__main__":
    main()
