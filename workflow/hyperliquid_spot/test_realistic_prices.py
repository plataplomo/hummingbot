#!/usr/bin/env python3
"""Test spot orders with realistic prices to get successful placements."""

import asyncio
import json
import time
from pathlib import Path
from datetime import datetime
import aiohttp

# Import the corrected signer
import sys
sys.path.append('workflow/hyperliquid_spot')
from correct_signing_implementation import HyperliquidSigner, load_test_credentials, TESTNET_EXCHANGE_URL

async def get_current_prices():
    """Get current market prices for reference."""
    info_url = "https://api.hyperliquid-testnet.xyz/info"

    # Get spot mid prices
    async with aiohttp.ClientSession() as session:
        # Get all spot mid prices
        async with session.post(info_url, json={"type": "spotMidPrices"}) as response:
            if response.status == 200:
                spot_prices = await response.json()
                print("Current spot mid prices:")
                for symbol, price in list(spot_prices.items())[:10]:  # Show first 10
                    print(f"  {symbol}: {price}")
                return spot_prices
            else:
                print(f"Failed to get spot prices: {response.status}")
                return {}

async def test_realistic_order(action, filename, private_key):
    """Test an order with realistic pricing."""
    signer = HyperliquidSigner(private_key, is_mainnet=False)
    nonce = int(time.time() * 1000)

    signed_payload = signer.sign_action(action, nonce)

    output_dir = Path("workflow/hyperliquid_spot/private_endpoints")
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n=== Testing {filename} ===")
    print(f"Action: {json.dumps(action, separators=(',', ':'))}")

    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(TESTNET_EXCHANGE_URL, json=signed_payload, timeout=10) as response:
                raw_body = await response.text()

                result = {
                    "timestamp": datetime.now().isoformat(),
                    "test_name": filename,
                    "wallet": signer.wallet_address,
                    "action_structure": action,
                    "request_payload": signed_payload,
                    "response": {
                        "status": response.status,
                        "raw_body": raw_body,
                        "body_length": len(raw_body)
                    }
                }

                try:
                    parsed = json.loads(raw_body)
                    result["response"]["parsed_json"] = parsed
                except json.JSONDecodeError:
                    result["response"]["parse_error"] = "Not valid JSON"

                # Save to file
                output_file = output_dir / f"{filename}_realistic.json"
                with open(output_file, 'w') as f:
                    json.dump(result, f, indent=2)

                success = response.status == 200
                print(f"{'✓' if success else '✗'} HTTP {response.status}")
                print(f"  Response: {raw_body[:200]}...")

                # Check for successful order placement
                if success and "parsed_json" in result["response"]:
                    resp_data = result["response"]["parsed_json"]
                    if resp_data.get("status") == "ok":
                        statuses = resp_data.get("response", {}).get("data", {}).get("statuses", [])
                        for status in statuses:
                            if "resting" in status:
                                print(f"  🎉 ORDER PLACED! OID: {status['resting']['oid']}")
                            elif "error" in status:
                                print(f"  ❌ Order error: {status['error']}")

                return success, result

        except Exception as e:
            print(f"✗ Error: {e}")
            return False, None

async def main():
    """Test spot orders with realistic prices."""
    private_key = load_test_credentials()

    print("Testing spot orders with realistic pricing...")

    # Get current market prices
    spot_prices = await get_current_prices()

    # Test 1: PURR/USDC order with realistic price
    purr_price = float(spot_prices.get("PURR/USDC", "1.0"))
    # Set buy price 50% below market (should be well within 80% range)
    buy_price = purr_price * 0.5

    purr_order = {
        "type": "order",
        "orders": [{
            "a": 0,                                      # asset_index for PURR/USDC
            "b": True,                                   # is_buy (buy order)
            "p": f"{buy_price:.6f}",                    # limit_px (50% below market)
            "s": "10",                                  # size (10 PURR)
            "r": False,                                 # reduce_only
            "t": {"limit": {"tif": "Gtc"}},            # order_type
        }],
        "grouping": "na"
    }
    success, _ = await test_realistic_order(purr_order, "purr_realistic_buy", private_key)
    await asyncio.sleep(2)

    # Test 2: @1 token order (if we can find its price)
    token1_price = 0.001  # Default conservative price
    if "@1" in spot_prices:
        token1_price = float(spot_prices["@1"])

    buy_price_1 = token1_price * 0.7  # 30% below market

    token1_order = {
        "type": "order",
        "orders": [{
            "a": 1,                                     # asset_index for @1
            "b": True,                                  # is_buy
            "p": f"{buy_price_1:.8f}",                 # limit_px (30% below market)
            "s": "100",                                # size (100 tokens)
            "r": False,                                # reduce_only
            "t": {"limit": {"tif": "Gtc"}},           # order_type
        }],
        "grouping": "na"
    }
    success, _ = await test_realistic_order(token1_order, "token1_realistic_buy", private_key)
    await asyncio.sleep(2)

    # Test 3: Small conservative order
    small_order = {
        "type": "order",
        "orders": [{
            "a": 0,                                     # PURR/USDC
            "b": True,                                  # is_buy
            "p": "0.1",                                # Very low price (likely well below market)
            "s": "1",                                  # Small size
            "r": False,                                # reduce_only
            "t": {"limit": {"tif": "Gtc"}},           # order_type
        }],
        "grouping": "na"
    }
    success, _ = await test_realistic_order(small_order, "small_conservative_order", private_key)
    await asyncio.sleep(2)

    # Test 4: Different time in force
    ioc_order = {
        "type": "order",
        "orders": [{
            "a": 0,                                     # PURR/USDC
            "b": True,                                  # is_buy
            "p": f"{purr_price * 0.6:.6f}",           # 40% below market
            "s": "5",                                  # size
            "r": False,                                # reduce_only
            "t": {"limit": {"tif": "Ioc"}},           # Immediate or Cancel
        }],
        "grouping": "na"
    }
    success, _ = await test_realistic_order(ioc_order, "ioc_order", private_key)

    print(f"\n✅ Realistic pricing tests completed!")

if __name__ == "__main__":
    asyncio.run(main())
