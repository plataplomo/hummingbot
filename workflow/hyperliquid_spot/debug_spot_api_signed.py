#!/usr/bin/env python3
"""Debug script to explore Hyperliquid spot trading API with signed requests.

This script connects to Hyperliquid testnet and executes real signed requests
to test spot trading functionality.
"""

import asyncio
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import aiohttp
from eth_account import Account
from eth_account.messages import encode_structured_data
from eth_account.signers.local import LocalAccount
import structlog

# Configure logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.dev.ConsoleRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

# Testnet configuration
TESTNET_API_URL = "https://api.hyperliquid-testnet.xyz"
TESTNET_INFO_URL = f"{TESTNET_API_URL}/info"
TESTNET_EXCHANGE_URL = f"{TESTNET_API_URL}/exchange"

# EIP-712 Domain for Hyperliquid
HYPERLIQUID_DOMAIN = {
    "name": "Exchange",
    "version": "1",
    "chainId": 421614,  # Arbitrum Sepolia testnet
    "verifyingContract": "0x0000000000000000000000000000000000000000",
}


class HyperliquidSpotSignedDebugger:
    """Debug tool for exploring Hyperliquid spot trading API with signed requests."""

    def __init__(self, private_key: str = None):
        self.session = None
        # Use provided key or generate a test one
        if private_key:
            self.account: LocalAccount = Account.from_key(private_key)
        else:
            # Generate a test account
            self.account = Account.create()
            logger.warning(f"Using generated test account: {self.account.address}")

        self.wallet_address = self.account.address
        self.output_dir = Path("workflow/hyperliquid_spot/debug_output_signed")
        self.output_dir.mkdir(parents=True, exist_ok=True)

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    def sign_l1_action(self, action: Dict[str, Any], nonce: int) -> Dict[str, Any]:
        """Sign an L1 action using EIP-712."""
        # Build the typed data structure
        typed_data = {
            "types": {
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
                "sender": self.wallet_address,
                "nonce": nonce,
                "action": json.dumps(action, separators=(',', ':')),
            },
        }

        # Sign the typed data
        encoded = encode_structured_data(typed_data)
        signature = self.account.sign_message(encoded)

        return {
            "action": action,
            "nonce": nonce,
            "signature": {
                "r": hex(signature.r),
                "s": hex(signature.s),
                "v": signature.v,
            },
        }

    async def make_signed_request(self, action: Dict[str, Any], nonce: int = None) -> Dict[str, Any]:
        """Make a signed request to the /exchange endpoint."""
        if nonce is None:
            nonce = int(time.time() * 1000)  # Use current timestamp as nonce

        signed_payload = self.sign_l1_action(action, nonce)

        logger.info(f"Making signed request with action type: {action.get('type')}")

        async with self.session.post(TESTNET_EXCHANGE_URL, json=signed_payload) as response:
            response_data = {
                "status": response.status,
                "headers": dict(response.headers),
                "body": await response.text(),
            }

            # Try to parse JSON response
            try:
                response_data["parsed_body"] = json.loads(response_data["body"])
            except:
                pass

            return response_data

    async def save_debug_output(self, filename: str, data: dict):
        """Save debug output to a JSON file."""
        filepath = self.output_dir / f"{filename}.json"
        with open(filepath, 'w') as f:
            json.dump({
                "timestamp": datetime.now().isoformat(),
                "wallet_address": self.wallet_address,
                "data": data
            }, f, indent=2)
        logger.info(f"Saved debug output to {filepath}")

    async def test_spot_order_placement(self, spot_symbol: str = "@1"):
        """Test placing a spot order with proper signing."""
        logger.info(f"Testing signed spot order placement for: {spot_symbol}")

        # Build spot order action
        action = {
            "type": "order",
            "orders": [{
                "coin": spot_symbol,
                "is_buy": True,
                "limit_px": "0.0001",  # Very low price to avoid execution
                "sz": "1",  # Minimal size
                "order_type": {"limit": {"tif": "Gtc"}},
                "reduce_only": False
            }],
            "grouping": "na"
        }

        response = await self.make_signed_request(action)
        await self.save_debug_output(f"01_spot_order_placement_{spot_symbol.replace('@', 'at')}", {
            "request": {
                "action": action,
                "endpoint": "/exchange",
                "method": "POST"
            },
            "response": response
        })

        return response

    async def test_spot_order_cancellation(self, spot_symbol: str = "@1"):
        """Test cancelling a spot order."""
        logger.info(f"Testing spot order cancellation for: {spot_symbol}")

        # First, we need to get the asset index for the symbol
        # For testing, we'll assume @1 = index 1 (this might not be correct)
        action = {
            "type": "cancel",
            "cancels": [{
                "a": 1,  # asset index (guessing)
                "o": 999999  # non-existent order ID
            }]
        }

        response = await self.make_signed_request(action)
        await self.save_debug_output(f"02_spot_order_cancel_{spot_symbol.replace('@', 'at')}", {
            "request": {
                "action": action,
                "endpoint": "/exchange",
                "method": "POST"
            },
            "response": response
        })

        return response

    async def test_usd_transfer(self):
        """Test USD transfer between spot and perp."""
        logger.info("Testing USD transfer (spot to perp)")

        action = {
            "type": "usdTransfer",
            "amount": "1.0",  # Transfer 1 USDC
            "toPerp": True    # From spot to perp
        }

        response = await self.make_signed_request(action)
        await self.save_debug_output("03_usd_transfer_spot_to_perp", {
            "request": {
                "action": action,
                "endpoint": "/exchange",
                "method": "POST"
            },
            "response": response
        })

        return response

    async def test_spot_withdrawal(self):
        """Test spot token withdrawal."""
        logger.info("Testing spot token withdrawal")

        # This is hypothetical - need to confirm actual format
        action = {
            "type": "withdraw",
            "asset": "@1",
            "amount": "1.0",
            "destination": self.wallet_address  # Withdraw to self for testing
        }

        response = await self.make_signed_request(action)
        await self.save_debug_output("04_spot_withdrawal", {
            "request": {
                "action": action,
                "endpoint": "/exchange",
                "method": "POST"
            },
            "response": response
        })

        return response

    async def test_invalid_spot_symbol(self):
        """Test with an invalid spot symbol to see error response."""
        logger.info("Testing with invalid spot symbol")

        action = {
            "type": "order",
            "orders": [{
                "coin": "INVALID_SPOT",
                "is_buy": True,
                "limit_px": "0.0001",
                "sz": "1",
                "order_type": {"limit": {"tif": "Gtc"}},
                "reduce_only": False
            }],
            "grouping": "na"
        }

        response = await self.make_signed_request(action)
        await self.save_debug_output("05_invalid_spot_symbol", {
            "request": {
                "action": action,
                "endpoint": "/exchange",
                "method": "POST"
            },
            "response": response
        })

        return response

    async def get_open_orders(self):
        """Get open orders to see if spot orders are returned."""
        logger.info("Fetching open orders via /info endpoint")

        # This uses the info endpoint, not exchange
        payload = {
            "type": "openOrders",
            "user": self.wallet_address
        }

        async with self.session.post(TESTNET_INFO_URL, json=payload) as response:
            response_data = {
                "status": response.status,
                "body": await response.json()
            }

        await self.save_debug_output("06_open_orders", {
            "request": {
                "payload": payload,
                "endpoint": "/info",
                "method": "POST"
            },
            "response": response_data
        })

        return response_data

    async def run_signed_debug_sequence(self):
        """Run the complete signed debug sequence."""
        logger.info("Starting Hyperliquid spot API signed debug sequence...")
        logger.info(f"Using wallet address: {self.wallet_address}")

        results = {}

        # 1. Test spot order placement
        try:
            results["spot_order"] = await self.test_spot_order_placement("@1")
        except Exception as e:
            logger.error(f"Spot order placement failed: {e}")
            results["spot_order"] = {"error": str(e)}

        # 2. Test spot order cancellation
        try:
            results["spot_cancel"] = await self.test_spot_order_cancellation("@1")
        except Exception as e:
            logger.error(f"Spot order cancellation failed: {e}")
            results["spot_cancel"] = {"error": str(e)}

        # 3. Test USD transfer
        try:
            results["usd_transfer"] = await self.test_usd_transfer()
        except Exception as e:
            logger.error(f"USD transfer failed: {e}")
            results["usd_transfer"] = {"error": str(e)}

        # 4. Test spot withdrawal
        try:
            results["spot_withdrawal"] = await self.test_spot_withdrawal()
        except Exception as e:
            logger.error(f"Spot withdrawal failed: {e}")
            results["spot_withdrawal"] = {"error": str(e)}

        # 5. Test invalid symbol
        try:
            results["invalid_symbol"] = await self.test_invalid_spot_symbol()
        except Exception as e:
            logger.error(f"Invalid symbol test failed: {e}")
            results["invalid_symbol"] = {"error": str(e)}

        # 6. Get open orders
        try:
            results["open_orders"] = await self.get_open_orders()
        except Exception as e:
            logger.error(f"Get open orders failed: {e}")
            results["open_orders"] = {"error": str(e)}

        # Create summary
        summary = {
            "testnet_url": TESTNET_API_URL,
            "wallet_address": self.wallet_address,
            "tests_performed": list(results.keys()),
            "endpoints_tested": [
                "/exchange - order (spot)",
                "/exchange - cancel (spot)",
                "/exchange - usdTransfer",
                "/exchange - withdraw",
                "/info - openOrders"
            ],
            "results_summary": {
                test: "success" if "error" not in result else f"failed: {result['error']}"
                for test, result in results.items()
            }
        }

        await self.save_debug_output("00_signed_debug_summary", summary)

        logger.info("Signed debug sequence completed! Check workflow/hyperliquid_spot/debug_output_signed/ for results")


async def main():
    """Run the signed debugger."""
    # You can provide a testnet private key here or let it generate one
    # private_key = "0x..."  # Your testnet private key
    private_key = None  # Will generate a test account

    async with HyperliquidSpotSignedDebugger(private_key) as debugger:
        await debugger.run_signed_debug_sequence()


if __name__ == "__main__":
    asyncio.run(main())
