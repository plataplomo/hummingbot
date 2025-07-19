#!/usr/bin/env python3
"""Script to discover and download ALL mainnet spot asset mappings for Hyperliquid.

This script:
1. Connects to Hyperliquid mainnet API
2. Fetches comprehensive spot metadata
3. Extracts ALL spot symbol to asset index mappings
4. Generates production-ready Python code
"""

import asyncio
import json
from datetime import datetime
from pathlib import Path
import aiohttp
from typing import Dict, Any, Set
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

# Mainnet API URL
MAINNET_API_URL = "https://api.hyperliquid.xyz"


class MainnetSpotMappingDiscoverer:
    """Discovers ALL spot asset mappings from Hyperliquid mainnet API."""

    def __init__(self):
        self.base_url = MAINNET_API_URL
        self.info_url = f"{self.base_url}/info"
        self.session = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def fetch_spot_metadata(self) -> Dict[str, Any]:
        """Fetch comprehensive spot metadata from mainnet."""
        payload = {"type": "spotMeta"}

        try:
            async with self.session.post(self.info_url, json=payload) as response:
                data = await response.json()
                logger.info(f"Fetched spotMeta from mainnet, status: {response.status}")
                return data
        except Exception as e:
            logger.error(f"Error fetching spot metadata: {e}")
            return {}

    async def fetch_meta_and_asset_ctxs(self) -> Dict[str, Any]:
        """Fetch full metadata including spot context."""
        payload = {"type": "metaAndAssetCtxs"}

        try:
            async with self.session.post(self.info_url, json=payload) as response:
                data = await response.json()
                logger.info(f"Fetched metaAndAssetCtxs from mainnet, status: {response.status}")
                return data
        except Exception as e:
            logger.error(f"Error fetching meta and asset ctxs: {e}")
            return {}

    async def fetch_all_mids(self) -> Dict[str, Any]:
        """Fetch all mid prices to discover active spot pairs."""
        payload = {"type": "allMids"}

        try:
            async with self.session.post(self.info_url, json=payload) as response:
                data = await response.json()
                logger.info(f"Fetched allMids from mainnet, status: {response.status}")
                return data
        except Exception as e:
            logger.error(f"Error fetching all mids: {e}")
            return {}

    async def fetch_spot_clearinghouse_state(self) -> Dict[str, Any]:
        """Fetch spot clearinghouse state for additional metadata."""
        payload = {"type": "spotClearinghouseState", "user": "0x0000000000000000000000000000000000000000"}

        try:
            async with self.session.post(self.info_url, json=payload) as response:
                data = await response.json()
                logger.info(f"Fetched spotClearinghouseState from mainnet, status: {response.status}")
                return data
        except Exception as e:
            logger.error(f"Error fetching spot clearinghouse state: {e}")
            return {}

    async def discover_all_mappings(self) -> Dict[str, int]:
        """Discover ALL spot symbol to asset index mappings."""
        mappings = {}
        discovered_indices = set()

        # Fetch all available data
        spot_meta = await self.fetch_spot_metadata()
        meta_and_ctxs = await self.fetch_meta_and_asset_ctxs()
        all_mids = await self.fetch_all_mids()
        spot_ch_state = await self.fetch_spot_clearinghouse_state()

        # Save raw responses for analysis
        output_dir = Path("mainnet_spot_data")
        output_dir.mkdir(exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        with open(output_dir / f"mainnet_spot_meta_{timestamp}.json", "w") as f:
            json.dump(spot_meta, f, indent=2)

        with open(output_dir / f"mainnet_meta_and_asset_ctxs_{timestamp}.json", "w") as f:
            json.dump(meta_and_ctxs, f, indent=2)

        with open(output_dir / f"mainnet_all_mids_{timestamp}.json", "w") as f:
            json.dump(all_mids, f, indent=2)

        with open(output_dir / f"mainnet_spot_ch_state_{timestamp}.json", "w") as f:
            json.dump(spot_ch_state, f, indent=2)

        # Extract from spotMeta universe
        if isinstance(spot_meta, dict) and "universe" in spot_meta:
            universe = spot_meta["universe"]
            logger.info(f"Found {len(universe)} pairs in spotMeta universe")

            for pair_info in universe:
                if isinstance(pair_info, dict):
                    name = pair_info.get("name", "")
                    index = pair_info.get("index")

                    if index is not None:
                        mappings[name] = index
                        mappings[f"@{index}"] = index
                        discovered_indices.add(index)

        # Extract token information
        if isinstance(spot_meta, dict) and "tokens" in spot_meta:
            tokens = spot_meta["tokens"]
            logger.info(f"Found {len(tokens)} tokens in spotMeta")

            # Create a token index to name mapping
            token_names = {}
            for token_info in tokens:
                if isinstance(token_info, dict):
                    name = token_info.get("name", "")
                    token_id = token_info.get("tokenId", "")
                    # Token index appears to be the position in the array
                    if name:
                        token_names[len(token_names)] = name

            # Now update universe pairs with proper names
            if "universe" in spot_meta:
                for pair_info in spot_meta["universe"]:
                    if isinstance(pair_info, dict):
                        tokens_list = pair_info.get("tokens", [])
                        index = pair_info.get("index")
                        current_name = pair_info.get("name", "")

                        # If name is @N format and we have token names, create proper name
                        if current_name.startswith("@") and len(tokens_list) >= 2:
                            token1_idx = tokens_list[0]
                            token2_idx = tokens_list[1]

                            if token1_idx in token_names and token2_idx in token_names:
                                proper_name = f"{token_names[token1_idx]}/{token_names[token2_idx]}"
                                mappings[proper_name] = index
                                logger.info(f"Mapped {current_name} -> {proper_name} (index {index})")

        # Extract from spot universe in metaAndAssetCtxs
        if isinstance(meta_and_ctxs, dict):
            # Check for spot-specific data
            if "spot" in meta_and_ctxs:
                spot_data = meta_and_ctxs["spot"]
                if isinstance(spot_data, list):
                    for i, asset in enumerate(spot_data):
                        if isinstance(asset, dict):
                            name = asset.get("name", asset.get("ticker", ""))
                            if name:
                                mappings[f"{name}/USDC"] = i
                            mappings[f"@{i}"] = i
                            discovered_indices.add(i)

        # Extract spot pairs from allMids
        if isinstance(all_mids, dict):
            for symbol, price in all_mids.items():
                # Look for spot-specific patterns
                if symbol.startswith("@") and symbol[1:].isdigit():
                    index = int(symbol[1:])
                    mappings[symbol] = index
                    discovered_indices.add(index)
                elif "/" in symbol and not any(perp_suffix in symbol for perp_suffix in ["-PERP", "PERP", "_PERP"]):
                    # This might be a spot pair
                    logger.info(f"Found potential spot pair: {symbol} (price: {price})")

        # Ensure we have @N mappings for all discovered indices
        max_index = max(discovered_indices) if discovered_indices else 0
        logger.info(f"Max discovered index: {max_index}")

        # Add @N format for all indices up to max discovered
        for i in range(max_index + 1):
            mappings[f"@{i}"] = i

        logger.info(f"Total mappings discovered: {len(mappings)}")
        logger.info(f"Unique indices: {len(discovered_indices)}")

        return mappings

    def generate_production_code(self, mappings: Dict[str, int]) -> str:
        """Generate production-ready Python code for mainnet mappings."""
        code = f'''"""Hyperliquid Mainnet Spot Asset Mappings.

Auto-generated from mainnet API discovery.
Last updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

This module contains the COMPLETE mainnet spot symbol to asset index mappings.
"""

from enum import IntEnum


class HyperliquidMainnetSpotAssets(IntEnum):
    """Common mainnet spot assets for type-safe access."""

'''

        # Extract named tokens (not @N format) and sort by index
        named_tokens = [(name.replace("/USDC", ""), idx) for name, idx in mappings.items()
                       if "/" in name and idx < 100]  # First 100 for enum
        named_tokens.sort(key=lambda x: x[1])

        # Add to enum (clean names, no special chars)
        added_to_enum = set()
        for name, idx in named_tokens:
            clean_name = name.replace("-", "_").replace(" ", "_").replace(".", "_")
            clean_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in clean_name)
            if clean_name and clean_name[0].isalpha() and clean_name not in added_to_enum:
                code += f"    {clean_name} = {idx}\n"
                added_to_enum.add(clean_name)

        code += '''

# Complete mainnet mappings dictionary
MAINNET_SPOT_SYMBOL_MAPPINGS = {
'''

        # Sort mappings by index then by symbol
        sorted_mappings = sorted(mappings.items(), key=lambda x: (x[1], x[0]))

        # Group by asset index
        current_index = None
        for symbol, index in sorted_mappings:
            if current_index != index:
                if current_index is not None:
                    code += "\n"
                code += f"    # Asset index {index}\n"
                current_index = index
            code += f'    "{symbol}": {index},\n'

        code += "}\n"
        return code


async def main():
    """Download and process ALL mainnet spot mappings."""

    logger.info("Starting mainnet spot mappings discovery...")

    async with MainnetSpotMappingDiscoverer() as discoverer:
        mappings = await discoverer.discover_all_mappings()

        if not mappings:
            logger.error("No mappings discovered! Check API connectivity.")
            return

        # Generate production code
        production_code = discoverer.generate_production_code(mappings)

        # Display summary
        print("\n" + "="*80)
        print(f"MAINNET SPOT MAPPINGS DISCOVERED: {len(mappings)} total")
        print("="*80)
        print(f"Sample mappings:")
        for i, (symbol, index) in enumerate(sorted(mappings.items())[:20]):
            print(f"  {symbol}: {index}")
        print("  ...")
        print(f"\nTotal unique indices: {len(set(mappings.values()))}")

        # Save to file
        output_file = Path("mainnet_spot_mappings_complete.py")
        with open(output_file, "w") as f:
            f.write(production_code)

        logger.info(f"Mainnet mappings saved to: {output_file}")

        # Also save raw mappings as JSON for reference
        json_file = Path("mainnet_spot_mappings.json")
        with open(json_file, "w") as f:
            json.dump(mappings, f, indent=2, sort_keys=True)

        logger.info(f"Raw mappings saved to: {json_file}")


if __name__ == "__main__":
    asyncio.run(main())
