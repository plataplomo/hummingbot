#!/usr/bin/env python3
"""Test correct payload structures for Hyperliquid spot trading."""

import asyncio
import json
import time
from pathlib import Path
from datetime import datetime
import aiohttp
import yaml
import msgpack
from eth_account import Account
from eth_account.messages import encode_typed_data
from eth_utils import keccak, to_bytes

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
                output_file = output_dir / f"{filename}_payload_test.json"
                with open(output_file, 'w') as f:
                    json.dump(result, f, indent=2)

                success = response.status == 200
                print(f"{'✓' if success else '✗'} HTTP {response.status}")
                print(f"  Response: {raw_body[:100]}...")

                return success, result

        except Exception as e:
            print(f"✗ Error: {e}")
            return False, None

async def main():
    """Test various payload structures to find the correct format."""
    private_key = load_test_credentials()

    print("Testing payload structures based on Hyperliquid SDK examples...")

    # Test 1: Basic spot order (from SDK basic_spot_order.py)
    basic_spot_order = {
        "type": "order",
        "orders": [{
            "coin": "PURR/USDC",
            "is_buy": True,
            "sz": "24",
            "limit_px": "0.5",
            "order_type": {"limit": {"tif": "Gtc"}},
            "reduce_only": False
        }],
        "grouping": "na"
    }
    await test_payload(basic_spot_order, "basic_spot_order", private_key)
    await asyncio.sleep(1)

    # Test 2: Spot order with @N format
    spot_at_format = {
        "type": "order",
        "orders": [{
            "coin": "@1",
            "is_buy": True,
            "sz": "1",
            "limit_px": "0.0001",
            "order_type": {"limit": {"tif": "Gtc"}},
            "reduce_only": False
        }],
        "grouping": "na"
    }
    await test_payload(spot_at_format, "spot_at_format", private_key)
    await asyncio.sleep(1)

    # Test 3: Minimal spot order (remove optional fields)
    minimal_spot = {
        "type": "order",
        "orders": [{
            "coin": "PURR/USDC",
            "is_buy": True,
            "sz": "1",
            "limit_px": "0.5",
            "order_type": {"limit": {"tif": "Gtc"}}
        }],
        "grouping": "na"
    }
    await test_payload(minimal_spot, "minimal_spot", private_key)
    await asyncio.sleep(1)

    # Test 4: USD transfer (from SDK basic_spot_to_perp.py)
    usd_transfer = {
        "type": "usdTransfer",
        "amount": "1.0",
        "toPerp": True
    }
    await test_payload(usd_transfer, "usd_transfer", private_key)
    await asyncio.sleep(1)

    # Test 5: Different USD transfer format
    usd_transfer_alt = {
        "type": "usdClassTransfer",
        "amount": "1.0",
        "toPerp": True
    }
    await test_payload(usd_transfer_alt, "usd_transfer_alt", private_key)
    await asyncio.sleep(1)

    # Test 6: Spot order with different field names (msgpack style)
    msgpack_style = {
        "type": "order",
        "orders": [{
            "coin": "PURR/USDC",
            "is_buy": True,
            "sz": "1",
            "limit_px": "0.5",
            "order_type": {"limit": {"tif": "Gtc"}},
            "reduce_only": False
        }],
        "grouping": "na"
    }
    await test_payload(msgpack_style, "msgpack_style", private_key)
    await asyncio.sleep(1)

    # Test 7: Order without grouping
    no_grouping = {
        "type": "order",
        "orders": [{
            "coin": "PURR/USDC",
            "is_buy": True,
            "sz": "1",
            "limit_px": "0.5",
            "order_type": {"limit": {"tif": "Gtc"}},
            "reduce_only": False
        }]
    }
    await test_payload(no_grouping, "no_grouping", private_key)
    await asyncio.sleep(1)

    # Test 8: Try different order type structure
    different_order_type = {
        "type": "order",
        "orders": [{
            "coin": "PURR/USDC",
            "is_buy": True,
            "sz": "1",
            "limit_px": "0.5",
            "order_type": {"limit": {"tif": "Gtc"}},
            "reduce_only": False
        }],
        "grouping": "na"
    }
    await test_payload(different_order_type, "different_order_type", private_key)

    print(f"\n✅ All payload structure tests completed!")

if __name__ == "__main__":
    asyncio.run(main())
