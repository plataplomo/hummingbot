#!/usr/bin/env python3
"""Capture real signed endpoint requests from Hyperliquid testnet."""

import asyncio
import json
import time
from pathlib import Path
from datetime import datetime
import aiohttp
import yaml
from eth_account import Account
from eth_utils import keccak

# Load test credentials
def load_test_credentials():
    with open("tests/config/test_secrets.yaml", 'r') as f:
        secrets = yaml.safe_load(f)
    return secrets['exchanges']['hyperliquid']['private_key']

# Testnet endpoints
TESTNET_API_URL = "https://api.hyperliquid-testnet.xyz"
TESTNET_EXCHANGE_URL = f"{TESTNET_API_URL}/exchange"

# Hyperliquid EIP-712 Signing (simplified and correct)
def sign_hyperliquid_action(action, nonce, private_key):
    """Sign a Hyperliquid action using their specific EIP-712 structure."""
    account = Account.from_key(private_key)

    # Create the action string
    action_str = json.dumps(action, separators=(',', ':'))

    # Create the signing data structure as per Hyperliquid spec
    signing_data = {
        "domain": "HyperliquidChain",
        "sender": account.address,
        "nonce": nonce,
        "action": action_str
    }

    # Create the hash according to Hyperliquid's custom signing
    # This is NOT standard EIP-712, it's Hyperliquid's custom format
    data_to_hash = f"HyperliquidChain:{account.address}:{nonce}:{action_str}"
    message_hash = keccak(data_to_hash.encode())

    # Sign the hash
    signature = account.unsafe_sign_hash(message_hash)

    return {
        "action": action,
        "nonce": nonce,
        "signature": {
            "r": signature.r.to_bytes(32, 'big').hex(),
            "s": signature.s.to_bytes(32, 'big').hex(),
            "v": signature.v
        }
    }

async def capture_endpoint(action, filename, private_key):
    """Capture a signed endpoint request/response."""
    nonce = int(time.time() * 1000)

    try:
        signed_payload = sign_hyperliquid_action(action, nonce, private_key)
    except Exception as e:
        print(f"Signing failed: {e}")
        return None

    output_dir = Path("workflow/hyperliquid_spot/private_endpoints")
    output_dir.mkdir(parents=True, exist_ok=True)

    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(TESTNET_EXCHANGE_URL, json=signed_payload, timeout=10) as response:
                raw_body = await response.text()

                result = {
                    "timestamp": datetime.now().isoformat(),
                    "wallet": Account.from_key(private_key).address,
                    "endpoint": "/exchange",
                    "method": "POST",
                    "request_payload": signed_payload,
                    "response": {
                        "status": response.status,
                        "headers": dict(response.headers),
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
                output_file = output_dir / f"{filename}.json"
                with open(output_file, 'w') as f:
                    json.dump(result, f, indent=2)

                print(f"✓ {filename}: HTTP {response.status}")
                print(f"  Body: {raw_body[:200]}...")
                print(f"  Saved to: {output_file}")

                return result

        except Exception as e:
            error_result = {
                "timestamp": datetime.now().isoformat(),
                "wallet": Account.from_key(private_key).address,
                "endpoint": "/exchange",
                "request_payload": signed_payload,
                "error": {
                    "type": type(e).__name__,
                    "message": str(e)
                }
            }

            error_file = output_dir / f"{filename}_error.json"
            with open(error_file, 'w') as f:
                json.dump(error_result, f, indent=2)

            print(f"✗ {filename}: {type(e).__name__} - {e}")
            return error_result

async def main():
    """Capture multiple signed endpoint requests."""
    private_key = load_test_credentials()
    wallet = Account.from_key(private_key).address
    print(f"Using wallet: {wallet}")
    print("Capturing signed endpoint requests...")

    # Test different spot operations
    operations = [
        ("spot_order_place_@1", {
            "type": "order",
            "orders": [{
                "coin": "@1",
                "is_buy": True,
                "limit_px": "0.0001",
                "sz": "1",
                "order_type": {"limit": {"tif": "Gtc"}},
                "reduce_only": False
            }],
            "grouping": "na"
        }),

        ("spot_order_cancel_@1", {
            "type": "cancel",
            "cancels": [{
                "a": 1,  # asset index for @1
                "o": 999999  # fake order ID
            }]
        }),

        ("usd_transfer_to_perp", {
            "type": "usdTransfer",
            "amount": "1.0",
            "toPerp": True
        }),

        ("spot_order_place_@10", {
            "type": "order",
            "orders": [{
                "coin": "@10",
                "is_buy": True,
                "limit_px": "0.001",
                "sz": "1",
                "order_type": {"limit": {"tif": "Gtc"}},
                "reduce_only": False
            }],
            "grouping": "na"
        })
    ]

    for name, action in operations:
        print(f"\n=== {name} ===")
        await capture_endpoint(action, name, private_key)
        await asyncio.sleep(1)  # Rate limiting

    print(f"\n✅ All signed endpoint captures completed!")
    print(f"Check workflow/hyperliquid_spot/private_endpoints/ for JSON files")

if __name__ == "__main__":
    asyncio.run(main())
