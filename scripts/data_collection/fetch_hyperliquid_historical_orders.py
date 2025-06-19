#!/usr/bin/env python3
"""Fetch historical orders from Hyperliquid API and save as raw JSON.

This script downloads all historical orders for a specified wallet address
and saves them to the test fixtures directory for use in tests.
"""

import json
from datetime import datetime
from pathlib import Path

import requests

# Configuration
WALLET_ADDRESS = "0x02Cd79f858bEF99588Cc2E650f2A4Fbf2baE8CB5"
API_URL = "https://api.hyperliquid-testnet.xyz/info"
OUTPUT_DIR = Path("/workspaces/CyberDeltaEngine/tests/fixtures/raw_api_data/hl/historicalOrders")


def fetch_historical_orders(wallet_address: str) -> dict:
    """Fetch historical orders for a wallet address."""
    payload = {"type": "historicalOrders", "user": wallet_address}

    print(f"Fetching historical orders for wallet: {wallet_address}")

    try:
        response = requests.post(API_URL, json=payload, timeout=30)
        response.raise_for_status()

        data = response.json()
        print(f"Successfully fetched {len(data) if isinstance(data, list) else 0} orders")

        return {
            "wallet": wallet_address,
            "fetched_at": datetime.utcnow().isoformat() + "Z",
            "endpoint": "historicalOrders",
            "api_url": API_URL,
            "orders": data,
        }

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        raise


def save_json(data: dict, output_dir: Path) -> None:
    """Save data as JSON file."""
    # Create directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate filename with timestamp
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"historicalOrders_{data['wallet'][:8]}_{timestamp}.json"
    filepath = output_dir / filename

    # Save JSON with pretty formatting
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2, sort_keys=True)

    print(f"Saved data to: {filepath}")

    # Also save a "latest" version for easy access
    latest_filepath = output_dir / f"historicalOrders_{data['wallet'][:8]}_latest.json"
    with open(latest_filepath, "w") as f:
        json.dump(data, f, indent=2, sort_keys=True)

    print(f"Also saved as: {latest_filepath}")


def main():
    """Main function."""
    print("=" * 60)
    print("Hyperliquid Historical Orders Fetcher")
    print("=" * 60)

    # Fetch data
    data = fetch_historical_orders(WALLET_ADDRESS)

    # Analyze the data
    if isinstance(data["orders"], list) and len(data["orders"]) > 0:
        print("\nOrder Analysis:")
        print(f"- Total orders: {len(data['orders'])}")

        # Check fields in first order
        first_order = data["orders"][0]
        print(f"- Fields in orders: {sorted(first_order.keys())}")

        # Check for different order types/statuses
        order_types = set()
        sides = set()
        coins = set()

        for order in data["orders"]:
            if "orderType" in order:
                order_types.add(order["orderType"])
            if "side" in order:
                sides.add(order["side"])
            if "coin" in order:
                coins.add(order["coin"])

        print(f"- Order types found: {sorted(order_types) if order_types else 'N/A'}")
        print(f"- Sides found: {sorted(sides) if sides else 'N/A'}")
        print(f"- Coins traded: {sorted(coins) if coins else 'N/A'}")

        # Check if these are orders or fills
        if "dir" in first_order and "closedPnl" in first_order:
            print("\nNOTE: These appear to be fills/trades, not orders!")
            print("Fields suggest these are execution records, not order records.")
    else:
        print("\nNo orders found for this wallet.")

    # Save data
    save_json(data, OUTPUT_DIR)

    print("\nDone!")


if __name__ == "__main__":
    main()
