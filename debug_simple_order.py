#!/usr/bin/env python3
"""Simple script to test order placement with minimal dependencies."""

import asyncio
import json
import logging

import aiohttp
from pydantic import SecretStr

from cyberdelta.apis.hyperliquid.hl_auth import HyperliquidEip712Authenticator
from cyberdelta.config.secrets_models import PrivateKeyAuthSecrets

# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


async def test_simple_order():
    """Test a simple order placement with minimal payload."""
    # Use test config private key
    private_key = "0x9bca46ad726b8e6c04d5f4fb6e8ad5b0d8a76b8fbdcc8cdfb60095f8b31b1234"
    wallet_address = "0xYourWalletAddress"  # You'll need to update this

    # Create authenticator
    secrets = PrivateKeyAuthSecrets(private_key=SecretStr(private_key))
    auth = HyperliquidEip712Authenticator(
        user_wallet_address=wallet_address,
        secrets=secrets,
        chain_id=421614,  # Testnet chain ID
    )

    # Create minimal order payload
    payload = {
        "type": "order",
        "actions": [
            {
                "asset": 3,
                "isBuy": True,
                "limitPx": "50000",
                "sz": "0.001",
                "reduceOnly": False,
                "orderType": {"limit": {"tif": "Gtc"}},
            }
        ],
    }

    logger.info(f"Order payload: {json.dumps(payload, indent=2)}")

    # Prepare authenticated request
    auth_components = await auth.prepare_request(
        method="POST", path="/exchange", params=None, data=payload, headers={}
    )

    # Make request
    url = "https://api.hyperliquid-testnet.xyz/exchange"

    async with aiohttp.ClientSession() as session:
        async with session.post(
            url, json=auth_components.data, headers=auth_components.headers
        ) as response:
            response_text = await response.text()
            logger.info(f"Response status: {response.status}")
            logger.info(f"Response body: {response_text}")

            if response.status != 200:
                logger.error(f"Request failed with status {response.status}")
                logger.error(f"Response: {response_text}")


if __name__ == "__main__":
    asyncio.run(test_simple_order())
