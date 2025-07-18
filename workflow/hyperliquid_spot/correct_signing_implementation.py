#!/usr/bin/env python3
"""Correct Hyperliquid signing implementation based on CyberDeltaEngine codebase."""

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

# Load test credentials
def load_test_credentials():
    with open("tests/config/test_secrets.yaml", 'r') as f:
        secrets = yaml.safe_load(f)
    return secrets['exchanges']['hyperliquid']['private_key']

# Testnet endpoints
TESTNET_API_URL = "https://api.hyperliquid-testnet.xyz"
TESTNET_EXCHANGE_URL = f"{TESTNET_API_URL}/exchange"

def address_to_bytes(address: str) -> bytes:
    """Convert hex address string to bytes."""
    return to_bytes(hexstr=address)

class HyperliquidSigner:
    """Correct Hyperliquid signing implementation based on CyberDeltaEngine code."""

    def __init__(self, private_key: str, is_mainnet: bool = False):
        self.account = Account.from_key(private_key)
        self.wallet_address = self.account.address.lower()
        self.is_mainnet = is_mainnet

        # EIP-712 Domain - always these values for Exchange
        self.exchange_domain = {
            "name": "Exchange",
            "version": "1",
            "chainId": 1337,  # Always 1337 for Exchange domain
            "verifyingContract": "0x0000000000000000000000000000000000000000"
        }

        # EIP-712 Types for Agent signing
        self.agent_types = {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"}
            ],
            "Agent": [
                {"name": "source", "type": "string"},
                {"name": "connectionId", "type": "bytes32"}
            ]
        }

    def _compute_action_hash(self, action: dict, nonce: int, vault_address: str = None, expires_after: int = None) -> bytes:
        """Compute action hash exactly as per CyberDeltaEngine implementation."""
        # 1. Serialize action with msgpack
        msgpacked_action = msgpack.packb(action)

        # 2. Build hash components in exact order
        hash_parts = [msgpacked_action]
        hash_parts.append(nonce.to_bytes(8, "big"))

        # 3. Add vault address flag and address (if present)
        if vault_address is not None:
            hash_parts.append(b"\x01")
            hash_parts.append(address_to_bytes(vault_address))
        else:
            hash_parts.append(b"\x00")

        # 4. Add expires_after if present
        if expires_after is not None:
            hash_parts.append(b"\x00")
            hash_parts.append(expires_after.to_bytes(8, "big"))

        # 5. Compute keccak hash
        action_hash_input = b"".join(hash_parts)
        return keccak(action_hash_input)

    def _create_phantom_agent_message(self, action_hash: bytes) -> dict:
        """Create phantom agent message for EIP-712 signing."""
        source_char = "a" if self.is_mainnet else "b"
        return {
            "source": source_char,
            "connectionId": action_hash
        }

    def _sign_eip712_message(self, phantom_agent_message: dict) -> dict:
        """Sign EIP-712 structured data."""
        structured_data = {
            "types": self.agent_types,
            "primaryType": "Agent",
            "domain": self.exchange_domain,
            "message": phantom_agent_message
        }

        signable_message = encode_typed_data(full_message=structured_data)
        signed_msg = self.account.sign_message(signable_message)

        return {
            "r": hex(signed_msg.r),
            "s": hex(signed_msg.s),
            "v": signed_msg.v
        }

    def sign_action(self, action: dict, nonce: int, vault_address: str = None, expires_after: int = None) -> dict:
        """Complete signing process for Hyperliquid action."""
        # Step 1: Compute action hash
        action_hash = self._compute_action_hash(action, nonce, vault_address, expires_after)

        # Step 2: Create phantom agent message
        phantom_agent = self._create_phantom_agent_message(action_hash)

        # Step 3: Sign with EIP-712
        signature = self._sign_eip712_message(phantom_agent)

        # Step 4: Build final payload
        payload = {
            "action": action,
            "nonce": nonce,
            "signature": signature,
            "vaultAddress": vault_address  # Include even if None
        }

        # Add optional fields if present
        if expires_after is not None:
            payload["expiresAfter"] = expires_after

        return payload

async def test_correct_signing(action, filename, private_key):
    """Test the corrected signing implementation."""
    signer = HyperliquidSigner(private_key, is_mainnet=False)
    nonce = int(time.time() * 1000)

    # Sign the action correctly
    signed_payload = signer.sign_action(action, nonce)

    output_dir = Path("workflow/hyperliquid_spot/private_endpoints")
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Signing with wallet: {signer.wallet_address}")
    print(f"Action: {json.dumps(action, separators=(',', ':'))}")
    print(f"Nonce: {nonce}")

    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(TESTNET_EXCHANGE_URL, json=signed_payload, timeout=10) as response:
                raw_body = await response.text()

                result = {
                    "timestamp": datetime.now().isoformat(),
                    "wallet": signer.wallet_address,
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
                output_file = output_dir / f"{filename}_corrected.json"
                with open(output_file, 'w') as f:
                    json.dump(result, f, indent=2)

                print(f"✓ {filename}: HTTP {response.status}")
                print(f"  Body: {raw_body[:200]}...")
                print(f"  Saved to: {output_file}")

                return result

        except Exception as e:
            print(f"✗ {filename}: {type(e).__name__} - {e}")
            return None

async def main():
    """Test corrected signing with multiple operations."""
    private_key = load_test_credentials()
    print("Testing corrected Hyperliquid signing implementation...")

    # Test different operations from the examples
    operations = [
        ("spot_order_place_purr", {
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
        }),

        ("spot_order_place_@1", {
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
        }),

        ("spot_order_cancel_@1", {
            "type": "cancel",
            "cancels": [{"a": 1, "o": 999999}]
        }),

        ("usd_transfer_to_perp", {
            "type": "usdTransfer",
            "amount": "1.0",
            "toPerp": True
        })
    ]

    for name, action in operations:
        print(f"\n=== Testing {name} ===")
        await test_correct_signing(action, name, private_key)
        await asyncio.sleep(1)  # Rate limiting

    print(f"\n✅ All corrected signing tests completed!")

if __name__ == "__main__":
    asyncio.run(main())
