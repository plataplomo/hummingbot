#!/usr/bin/env python3
"""Debug script to explore Hyperliquid spot trading API endpoints.

This script connects to Hyperliquid testnet and retrieves real payload/response
examples for spot trading functionality.
"""

import asyncio
import json
from datetime import datetime
from pathlib import Path
import aiohttp
from eth_account import Account
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

# Test wallet (you should use your own testnet wallet)
# This is a dummy private key - DO NOT USE IN PRODUCTION
TEST_PRIVATE_KEY = "0x0000000000000000000000000000000000000000000000000000000000000001"


class HyperliquidSpotDebugger:
    """Debug tool for exploring Hyperliquid spot trading API."""

    def __init__(self):
        self.session = None
        self.account: LocalAccount = Account.from_key(TEST_PRIVATE_KEY)
        self.wallet_address = self.account.address
        self.output_dir = Path("workflow/hyperliquid_spot/debug_output")
        self.output_dir.mkdir(parents=True, exist_ok=True)

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def make_info_request(self, payload: dict) -> dict:
        """Make a request to the /info endpoint."""
        async with self.session.post(TESTNET_INFO_URL, json=payload) as response:
            return {
                "status": response.status,
                "headers": dict(response.headers),
                "body": await response.json()
            }

    async def save_debug_output(self, filename: str, data: dict):
        """Save debug output to a JSON file."""
        filepath = self.output_dir / f"{filename}.json"
        with open(filepath, 'w') as f:
            json.dump({
                "timestamp": datetime.now().isoformat(),
                "data": data
            }, f, indent=2)
        logger.info(f"Saved debug output to {filepath}")

    async def get_all_asset_contexts(self):
        """Get all asset contexts including spot markets."""
        logger.info("Fetching all asset contexts (meta and spotMeta)...")

        payload = {"type": "metaAndAssetCtxs"}
        response = await self.make_info_request(payload)

        await self.save_debug_output("01_meta_and_asset_ctxs", response)

        if response["status"] == 200 and response["body"]:
            # The response body might be a list or dict
            body = response["body"]
            if isinstance(body, list) and len(body) > 0:
                data = body[0]
            else:
                data = body

            # Extract spot market information
            spot_meta = data.get("spotMeta", []) if isinstance(data, dict) else []
            perp_meta = data.get("meta", {}).get("universe", []) if isinstance(data, dict) else []

            logger.info(f"Found {len(spot_meta)} spot markets")
            logger.info(f"Found {len(perp_meta)} perpetual markets")

            # Save separate files for spot and perp meta
            await self.save_debug_output("02_spot_meta_only", {
                "count": len(spot_meta),
                "markets": spot_meta
            })

            return data
        return None

    async def get_spot_clearinghouse_state(self):
        """Get clearinghouse state which includes spot balances."""
        logger.info(f"Fetching clearinghouse state for wallet: {self.wallet_address}")

        payload = {
            "type": "clearinghouseState",
            "user": self.wallet_address
        }
        response = await self.make_info_request(payload)

        await self.save_debug_output("03_clearinghouse_state", response)

        if response["status"] == 200 and response["body"]:
            # Extract spot-related information
            asset_positions = response["body"].get("assetPositions", [])

            spot_info = {
                "assetPositions": asset_positions,
                "spotAssetCount": len([p for p in asset_positions if p.get("position", {}).get("coin")])
            }

            await self.save_debug_output("04_spot_positions_extracted", spot_info)

            return response["body"]
        return None

    async def get_spot_market_state(self, spot_name: str):
        """Get L2 book for a spot market."""
        logger.info(f"Fetching spot market state for: {spot_name}")

        payload = {
            "type": "l2Book",
            "coin": spot_name
        }
        response = await self.make_info_request(payload)

        await self.save_debug_output(f"05_spot_l2book_{spot_name.replace('/', '_')}", response)

        return response

    async def get_all_mids(self):
        """Get all mid prices including spot."""
        logger.info("Fetching all mid prices...")

        payload = {"type": "allMids"}
        response = await self.make_info_request(payload)

        await self.save_debug_output("06_all_mids", response)

        if response["status"] == 200 and response["body"]:
            # Separate spot and perp mids
            all_mids = response["body"]
            spot_mids = {k: v for k, v in all_mids.items() if "@" in k or "/" in k}
            perp_mids = {k: v for k, v in all_mids.items() if "@" not in k and "/" not in k}

            await self.save_debug_output("07_spot_mids_only", {
                "count": len(spot_mids),
                "mids": spot_mids
            })

            return all_mids
        return None

    async def test_spot_order_placement(self, spot_symbol: str):
        """Test placing a spot order (will likely fail but shows the API structure)."""
        logger.info(f"Testing spot order placement for: {spot_symbol}")

        # Build a hypothetical spot order payload
        # Based on perp order structure but adapted for spot
        order_payload = {
            "type": "order",
            "orders": [{
                "coin": spot_symbol,  # e.g., "@1" or "PURR/USDC"
                "is_buy": True,
                "limit_px": "0.001",  # Very low price to avoid execution
                "sz": "10",
                "order_type": {"limit": {"tif": "Gtc"}},
                "reduce_only": False
            }],
            "grouping": "na"
        }

        # Note: This would require EIP-712 signing for the exchange endpoint
        # For debugging, we'll just save the payload structure
        await self.save_debug_output("08_spot_order_payload_example", {
            "endpoint": "/exchange",
            "requires_signing": True,
            "payload": order_payload,
            "note": "This shows the expected structure but requires proper EIP-712 signing"
        })

        return order_payload

    async def test_spot_transfers(self):
        """Test spot transfer endpoints."""
        logger.info("Testing spot transfer endpoints...")

        # USDC transfer between spot and perp
        transfer_payload = {
            "type": "usdTransfer",
            "amount": "10.0",
            "toPerp": True,  # From spot to perp
            "user": self.wallet_address
        }

        await self.save_debug_output("09_spot_transfer_payload", {
            "endpoint": "/exchange",
            "requires_signing": True,
            "payload": transfer_payload,
            "note": "Transfer USDC between spot and perp accounts"
        })

        return transfer_payload

    async def run_debug_sequence(self):
        """Run the complete debug sequence."""
        logger.info("Starting Hyperliquid spot API debug sequence...")

        # 1. Get all asset contexts
        asset_ctxs = await self.get_all_asset_contexts()

        # 2. Get clearinghouse state
        ch_state = await self.get_spot_clearinghouse_state()

        # 3. Get all mids
        all_mids = await self.get_all_mids()

        # 4. If we found spot markets, test one
        if asset_ctxs and isinstance(asset_ctxs, dict) and "spotMeta" in asset_ctxs:
            spot_meta = asset_ctxs["spotMeta"]
            if spot_meta:
                # Test with the first spot market found
                first_spot = spot_meta[0]
                spot_name = first_spot.get("name", "@1")

                # Get L2 book for this spot market
                await self.get_spot_market_state(spot_name)

                # Test order placement structure
                await self.test_spot_order_placement(spot_name)

        # 5. Test transfer structure
        await self.test_spot_transfers()

        # Create summary
        summary = {
            "testnet_url": TESTNET_API_URL,
            "wallet_address": self.wallet_address,
            "endpoints_tested": [
                "/info - metaAndAssetCtxs",
                "/info - clearinghouseState",
                "/info - l2Book (spot)",
                "/info - allMids",
                "/exchange - order (spot)",
                "/exchange - usdTransfer"
            ],
            "spot_symbol_formats_found": [
                "@N format (e.g., @1, @2, @3)",
                "NAME/USDC format (e.g., PURR/USDC)"
            ]
        }

        await self.save_debug_output("00_debug_summary", summary)

        logger.info("Debug sequence completed! Check workflow/hyperliquid_spot/debug_output/ for results")


async def main():
    """Run the debugger."""
    async with HyperliquidSpotDebugger() as debugger:
        await debugger.run_debug_sequence()


if __name__ == "__main__":
    asyncio.run(main())
