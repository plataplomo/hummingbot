#!/usr/bin/env python3
"""Debug script to test Hyperliquid EIP-712 signature generation."""

import asyncio
import json
import os

import msgpack
from eth_account import Account
from eth_account.messages import encode_typed_data
from eth_utils.conversions import to_hex
from eth_utils.crypto import keccak
from pydantic import SecretStr

from cyberdelta.apis.hyperliquid.hl_auth import HyperliquidEip712Authenticator


def address_to_bytes(address: str) -> bytes:
    """Convert an Ethereum address string to bytes."""
    return bytes.fromhex(address[2:] if address.startswith("0x") else address)


async def test_signature_generation():
    """Test and debug EIP-712 signature generation."""
    # Get test private key from environment
    test_private_key = os.getenv("TEST_PRIVATE_KEY")
    if not test_private_key:
        print("Error: TEST_PRIVATE_KEY environment variable not set")
        return

    # Test configuration
    chain_id = 421614  # Hyperliquid testnet chain ID
    is_mainnet = False

    # Create authenticator
    authenticator = HyperliquidEip712Authenticator(
        wallet_private_key_secret=SecretStr(test_private_key),
        chain_id=chain_id,
        is_mainnet_environment=is_mainnet,
    )

    print(f"Wallet address: {authenticator.wallet_address}")
    print(f"Chain ID: {authenticator.chain_id}")
    print(f"Is mainnet: {is_mainnet}")

    # Test action data (simple order)
    action_data = {
        "type": "order",
        "actions": [
            {
                "asset": 125,  # PURR asset ID
                "isBuy": True,
                "limitPx": "0.01",
                "orderType": {"limit": {"tif": "Gtc"}},
                "reduceOnly": False,
                "sz": "1.0",
            }
        ],
    }

    print(f"\nAction data: {json.dumps(action_data, indent=2)}")

    # Manual signature generation for debugging
    print("\n=== Manual signature generation ===")

    # Step 1: Clean the action data
    action_payload_dict = dict(action_data)
    authenticator._clean_order_type_fields(action_payload_dict)
    authenticator._lowercase_addresses_in_payload(action_payload_dict)

    print(f"Cleaned action data: {json.dumps(action_payload_dict, indent=2)}")

    # Step 2: Generate nonce
    nonce = await authenticator._get_next_nonce_ms()
    print(f"Nonce: {nonce}")

    # Step 3: Calculate action hash
    msgpacked_action = msgpack.packb(action_payload_dict)
    print(f"Msgpack bytes length: {len(msgpacked_action)}")
    print(f"Msgpack hex: {msgpacked_action.hex()}")

    action_hash_data_parts = [msgpacked_action]
    action_hash_data_parts.append(nonce.to_bytes(8, "big"))
    action_hash_data_parts.append(b"\x00")  # No vault address

    action_hash_input_bytes = b"".join(action_hash_data_parts)
    action_hash_bytes = keccak(action_hash_input_bytes)

    print(f"Action hash input bytes length: {len(action_hash_input_bytes)}")
    print(f"Action hash: {action_hash_bytes.hex()}")

    # Step 4: Create EIP-712 message
    source_char = "b"  # testnet
    phantom_agent_message = {
        "source": source_char,
        "connectionId": action_hash_bytes,
    }

    print(f"Agent message: {phantom_agent_message}")

    # Step 5: EIP-712 domain and types
    domain = {
        "name": "Exchange",
        "version": "1",
        "chainId": chain_id,
        "verifyingContract": "0x0000000000000000000000000000000000000000",
    }

    types = {
        "EIP712Domain": [
            {"name": "name", "type": "string"},
            {"name": "version", "type": "string"},
            {"name": "chainId", "type": "uint256"},
            {"name": "verifyingContract", "type": "address"},
        ],
        "Agent": [
            {"name": "source", "type": "string"},
            {"name": "connectionId", "type": "bytes32"},
        ],
    }

    structured_data = {
        "domain": domain,
        "message": phantom_agent_message,
        "primaryType": "Agent",
        "types": types,
    }

    print(f"\nEIP-712 structured data: {json.dumps(structured_data, indent=2, default=str)}")

    # Step 6: Sign
    account = Account.from_key(test_private_key)
    signable_message = encode_typed_data(full_message=structured_data)
    signed_message = account.sign_message(signable_message)

    signature_dict = {
        "r": to_hex(signed_message.r),
        "s": to_hex(signed_message.s),
        "v": signed_message.v,
    }

    print(f"\nSignature: {signature_dict}")

    # Final request body
    final_body = {
        "action": action_payload_dict,
        "nonce": nonce,
        "signature": signature_dict,
    }

    print(f"\nFinal request body: {json.dumps(final_body, indent=2)}")

    # Test with authenticator's prepare_request
    print("\n=== Testing authenticator prepare_request ===")
    try:
        auth_result = await authenticator.prepare_request(
            method="POST",
            path="/exchange",
            params=None,
            data=action_data,
            headers=None,
        )
        print(f"Authenticator result: {json.dumps(auth_result['data'], indent=2)}")
    except Exception as e:
        print(f"Authenticator error: {e}")


if __name__ == "__main__":
    asyncio.run(test_signature_generation())
