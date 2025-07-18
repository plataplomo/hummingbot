#!/usr/bin/env python3
"""Download real signed endpoint JSON responses from Hyperliquid testnet."""

import asyncio
import json
import time
from pathlib import Path
from datetime import datetime
import aiohttp
import yaml
from eth_account import Account
from eth_account.messages import encode_typed_data

# Load actual test credentials
def load_test_credentials():
    with open("tests/config/test_secrets.yaml", 'r') as f:
        secrets = yaml.safe_load(f)
    return secrets['exchanges']['hyperliquid']['private_key']

# Testnet endpoints
TESTNET_API_URL = "https://api.hyperliquid-testnet.xyz"
TESTNET_EXCHANGE_URL = f"{TESTNET_API_URL}/exchange"

# EIP-712 Domain
HYPERLIQUID_DOMAIN = {
    "name": "Exchange",
    "version": "1",
    "chainId": 421614,
    "verifyingContract": "0x0000000000000000000000000000000000000000",
}

output_dir = Path("workflow/hyperliquid_spot/private_endpoints")
output_dir.mkdir(parents=True, exist_ok=True)

def sign_action(action, nonce, private_key):
    """Sign action with EIP-712."""
    account = Account.from_key(private_key)

    typed_data = {
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
            "HyperliquidTransaction:HyperliquidChain": [
                {"name": "domain", "type": "string"},
                {"name": "sender", "type": "address"},
                {"name": "nonce", "type": "uint64"},
                {"name": "action", "type": "string"},
            ],
        },
        "primaryType": "HyperliquidTransaction:HyperliquidChain",
        "domain": HYPERLIQUID_DOMAIN,
        "message": {
            "domain": "HyperliquidChain",
            "sender": account.address,
            "nonce": nonce,
            "action": json.dumps(action, separators=(',', ':')),
        },
    }

    encoded = encode_typed_data(typed_data)
    signature = account.sign_message(encoded)

    return {
        "action": action,
        "nonce": nonce,
        "signature": {
            "r": hex(signature.r),
            "s": hex(signature.s),
            "v": signature.v,
        },
    }

async def download_endpoint(action, filename, private_key):
    """Download a signed endpoint response."""
    nonce = int(time.time() * 1000)
    signed_payload = sign_action(action, nonce, private_key)

    async with aiohttp.ClientSession() as session:
        async with session.post(TESTNET_EXCHANGE_URL, json=signed_payload) as response:
            raw_body = await response.text()

            result = {
                "timestamp": datetime.now().isoformat(),
                "wallet": Account.from_key(private_key).address,
                "endpoint": "/exchange",
                "request": signed_payload,
                "response": {
                    "status": response.status,
                    "headers": dict(response.headers),
                    "raw_body": raw_body
                }
            }

            # Try to parse JSON
            try:
                result["response"]["parsed_json"] = json.loads(raw_body)
            except:
                result["response"]["parse_error"] = "Not valid JSON"

            # Save to file
            with open(output_dir / f"{filename}.json", 'w') as f:
                json.dump(result, f, indent=2)

            print(f"✓ {filename}: {response.status} - {raw_body[:100]}...")
            return result

async def main():
    """Download all signed endpoint responses."""
    private_key = load_test_credentials()
    wallet = Account.from_key(private_key).address
    print(f"Using wallet: {wallet}")

    # 1. Spot order placement
    await download_endpoint({
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
    }, "spot_order_place", private_key)

    await asyncio.sleep(1)

    # 2. Spot order cancel
    await download_endpoint({
        "type": "cancel",
        "cancels": [{
            "a": 1,  # asset index for @1
            "o": 999999
        }]
    }, "spot_order_cancel", private_key)

    await asyncio.sleep(1)

    # 3. USD transfer spot to perp
    await download_endpoint({
        "type": "usdTransfer",
        "amount": "1.0",
        "toPerp": True
    }, "usd_transfer_to_perp", private_key)

    await asyncio.sleep(1)

    # 4. USD transfer perp to spot
    await download_endpoint({
        "type": "usdTransfer",
        "amount": "1.0",
        "toPerp": False
    }, "usd_transfer_to_spot", private_key)

    await asyncio.sleep(1)

    # 5. Different spot token
    await download_endpoint({
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
    }, "spot_order_token_10", private_key)

    await asyncio.sleep(1)

    # 6. Try NAME/USDC format
    await download_endpoint({
        "type": "order",
        "orders": [{
            "coin": "PURR/USDC",
            "is_buy": True,
            "limit_px": "0.001",
            "sz": "10",
            "order_type": {"limit": {"tif": "Gtc"}},
            "reduce_only": False
        }],
        "grouping": "na"
    }, "spot_order_name_usdc", private_key)

    print(f"\n✅ All signed endpoint JSONs downloaded to {output_dir}/")

if __name__ == "__main__":
    asyncio.run(main())
