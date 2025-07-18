#!/usr/bin/env python3
"""Test Hyperliquid payloads with correct short field names."""

import asyncio
import json
import time
from pathlib import Path
from datetime import datetime
import aiohttp
import yaml

# Import the corrected signer
import sys
sys.path.append('workflow/hyperliquid_spot')
from correct_signing_implementation import HyperliquidSigner, load_test_credentials, TESTNET_EXCHANGE_URL

async def test_payload(action, filename, private_key):
    """Test a specific payload structure."""
    signer = HyperliquidSigner(private_key, is_mainnet=False)
    nonce = int(time.time() * 1000)

    # Sign the action correctly
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

                # Try to parse JSON
                try:
                    parsed = json.loads(raw_body)
                    result["response"]["parsed_json"] = parsed
                except json.JSONDecodeError:
                    result["response"]["parse_error"] = "Not valid JSON"

                # Save to file
                output_file = output_dir / f"{filename}_short_fields.json"
                with open(output_file, 'w') as f:
                    json.dump(result, f, indent=2)

                success = response.status == 200
                print(f"{'✓' if success else '✗'} HTTP {response.status}")
                if success:
                    print(f"  SUCCESS: {raw_body[:150]}...")
                else:
                    print(f"  ERROR: {raw_body[:150]}...")

                return success, result

        except Exception as e:
            print(f"✗ Error: {e}")
            return False, None

async def main():
    """Test correct payload structures using short field names."""
    private_key = load_test_credentials()

    print("Testing Hyperliquid payloads with correct short field names...")

    # Test 1: Spot order with @1 token using short field names
    spot_order_at1 = {
        "type": "order",
        "orders": [{
            "a": 1,                                    # asset_index for @1
            "b": True,                                 # is_buy
            "p": "0.0001",                            # limit_px (string)
            "s": "1",                                 # size (string)
            "r": False,                               # reduce_only
            "t": {"limit": {"tif": "Gtc"}},          # order_type
        }],
        "grouping": "na"
    }
    success, _ = await test_payload(spot_order_at1, "spot_order_at1_short", private_key)
    await asyncio.sleep(1)

    # Test 2: Spot order with @10 token
    spot_order_at10 = {
        "type": "order",
        "orders": [{
            "a": 10,                                  # asset_index for @10
            "b": True,                                # is_buy
            "p": "0.001",                            # limit_px (string)
            "s": "1",                                # size (string)
            "r": False,                              # reduce_only
            "t": {"limit": {"tif": "Gtc"}},         # order_type
        }],
        "grouping": "na"
    }
    success, _ = await test_payload(spot_order_at10, "spot_order_at10_short", private_key)
    await asyncio.sleep(1)

    # Test 3: Try to find PURR asset index (common spot token on testnet)
    # First try different asset indices for PURR/USDC
    for asset_idx in [0, 2, 3, 4, 5]:
        spot_order_purr = {
            "type": "order",
            "orders": [{
                "a": asset_idx,                          # asset_index for PURR/USDC (unknown)
                "b": True,                               # is_buy
                "p": "0.5",                             # limit_px (string)
                "s": "1",                               # size (string)
                "r": False,                             # reduce_only
                "t": {"limit": {"tif": "Gtc"}},        # order_type
            }],
            "grouping": "na"
        }
        success, _ = await test_payload(spot_order_purr, f"spot_order_purr_a{asset_idx}_short", private_key)
        if success:
            print(f"🎉 Found PURR asset index: {asset_idx}")
            break
        await asyncio.sleep(1)

    # Test 4: USD transfer with correct structure
    usd_transfer = {
        "type": "usdTransfer",
        "amount": "1000000",  # Try amount in wei (1 USDC = 1,000,000 wei)
        "toPerp": True
    }
    success, _ = await test_payload(usd_transfer, "usd_transfer_wei", private_key)
    await asyncio.sleep(1)

    # Test 5: USD transfer with string decimal
    usd_transfer_decimal = {
        "type": "usdTransfer",
        "amount": "1.0",      # String decimal format
        "toPerp": True
    }
    success, _ = await test_payload(usd_transfer_decimal, "usd_transfer_decimal", private_key)
    await asyncio.sleep(1)

    # Test 6: Try usdClassTransfer (from SDK examples)
    usd_class_transfer = {
        "type": "usdClassTransfer",
        "amount": "1000000",  # Wei format
        "toPerp": True
    }
    success, _ = await test_payload(usd_class_transfer, "usd_class_transfer", private_key)
    await asyncio.sleep(1)

    # Test 7: Minimal order structure (remove optional fields)
    minimal_order = {
        "type": "order",
        "orders": [{
            "a": 1,                                   # asset_index for @1
            "b": True,                                # is_buy
            "p": "0.0001",                           # limit_px
            "s": "1",                                # size
            "t": {"limit": {"tif": "Gtc"}},         # order_type
        }]
    }
    success, _ = await test_payload(minimal_order, "minimal_order_short", private_key)
    await asyncio.sleep(1)

    print(f"\n✅ All short field name tests completed!")

if __name__ == "__main__":
    asyncio.run(main())
