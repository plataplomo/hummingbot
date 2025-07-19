#!/usr/bin/env python3
"""Script to discover and populate spot asset mappings for Hyperliquid.

This script:
1. Fetches spot metadata from Hyperliquid API
2. Maps spot symbols to their asset indices
3. Generates code for both testnet and mainnet mappings
"""

import asyncio
import json
from datetime import datetime
from pathlib import Path
import aiohttp
from typing import Dict, Optional, Any
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

# API URLs
TESTNET_API_URL = "https://api.hyperliquid-testnet.xyz"
MAINNET_API_URL = "https://api.hyperliquid.xyz"


class SpotMappingDiscoverer:
    """Discovers spot asset mappings from Hyperliquid API."""

    def __init__(self, is_testnet: bool = True):
        self.is_testnet = is_testnet
        self.base_url = TESTNET_API_URL if is_testnet else MAINNET_API_URL
        self.info_url = f"{self.base_url}/info"
        self.session = None
        self.network_name = "testnet" if is_testnet else "mainnet"

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def fetch_spot_metadata(self) -> Dict[str, Any]:
        """Fetch spot market metadata from the API."""
        # First, let's try the standard metaAndAssetCtxs endpoint
        payload = {"type": "metaAndAssetCtxs"}

        try:
            async with self.session.post(self.info_url, json=payload) as response:
                data = await response.json()
                logger.info(f"Fetched metaAndAssetCtxs from {self.network_name}")
                return data
        except Exception as e:
            logger.error(f"Error fetching metadata: {e}")
            return {}

    async def fetch_spot_meta(self) -> Dict[str, Any]:
        """Fetch spot-specific metadata."""
        # Try spot-specific endpoint
        payload = {"type": "spotMeta"}

        try:
            async with self.session.post(self.info_url, json=payload) as response:
                data = await response.json()
                logger.info(f"Fetched spotMeta from {self.network_name}")
                return data
        except Exception as e:
            logger.error(f"Error fetching spot meta: {e}")
            return {}

    async def fetch_all_mids(self) -> Dict[str, Any]:
        """Fetch all mid prices including spot."""
        payload = {"type": "allMids"}

        try:
            async with self.session.post(self.info_url, json=payload) as response:
                data = await response.json()
                logger.info(f"Fetched allMids from {self.network_name}")
                return data
        except Exception as e:
            logger.error(f"Error fetching all mids: {e}")
            return {}

    async def discover_mappings(self) -> Dict[str, int]:
        """Discover spot symbol to asset index mappings."""
        mappings = {}

        # Fetch metadata
        meta_data = await self.fetch_spot_metadata()
        spot_meta = await self.fetch_spot_meta()
        all_mids = await self.fetch_all_mids()

        # Save raw responses for analysis
        output_dir = Path("debug_output")
        output_dir.mkdir(exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        with open(output_dir / f"{self.network_name}_meta_and_asset_ctxs_{timestamp}.json", "w") as f:
            json.dump(meta_data, f, indent=2)

        with open(output_dir / f"{self.network_name}_spot_meta_{timestamp}.json", "w") as f:
            json.dump(spot_meta, f, indent=2)

        with open(output_dir / f"{self.network_name}_all_mids_{timestamp}.json", "w") as f:
            json.dump(all_mids, f, indent=2)

        # Extract spot tokens from metadata
        if isinstance(spot_meta, dict):
            # Look for spot tokens in the response
            spot_tokens = spot_meta.get("tokens", [])
            if spot_tokens:
                logger.info(f"Found {len(spot_tokens)} spot tokens in spotMeta")
                for i, token in enumerate(spot_tokens):
                    if isinstance(token, dict):
                        name = token.get("name", token.get("ticker", f"TOKEN{i}"))
                        index = token.get("index", token.get("tokenId", i))
                        mappings[f"{name}/USDC"] = index
                        mappings[f"@{index}"] = index
                    elif isinstance(token, str):
                        # Token might just be a name
                        mappings[f"{token}/USDC"] = i
                        mappings[f"@{i}"] = i

        # Extract from allMids - spot pairs usually have specific format
        if isinstance(all_mids, dict):
            for symbol, price in all_mids.items():
                # Spot symbols might include @N format or NAME/USDC format
                if symbol.startswith("@") and symbol[1:].isdigit():
                    index = int(symbol[1:])
                    mappings[symbol] = index
                elif "/" in symbol and symbol.endswith("/USDC"):
                    # We need to find the index for this symbol
                    # For now, we'll log it for manual inspection
                    logger.info(f"Found spot pair {symbol} with mid price {price}")

        # Look for spot universe in meta data
        if isinstance(meta_data, dict) and "spotMetaAndAssetCtxs" in meta_data:
            spot_data = meta_data["spotMetaAndAssetCtxs"]
            if "meta" in spot_data and "universe" in spot_data["meta"]:
                universe = spot_data["meta"]["universe"]
                for i, asset in enumerate(universe):
                    if isinstance(asset, dict):
                        name = asset.get("name", f"ASSET{i}")
                        mappings[f"{name}/USDC"] = i
                        mappings[f"@{i}"] = i

        # Special known mappings from our captured data
        if self.is_testnet:
            # Add known testnet mappings from captured data
            known_testnet_mappings = {
                "PURR/USDC": 0,
                "@0": 0,
                "@1": 1,
                "@2": 2,
                "@10": 10,
                # Add more as discovered
            }
            mappings.update(known_testnet_mappings)

        return mappings

    def generate_mapping_code(self, mappings: Dict[str, int]) -> str:
        """Generate Python code for the mappings."""
        code = f"""# Spot asset mappings for Hyperliquid {self.network_name}
# Generated on {datetime.now().isoformat()}

{"TESTNET_" if self.is_testnet else ""}SPOT_MAPPINGS = {{
"""

        # Sort mappings by value (asset index) then by key (symbol)
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
    """Discover spot mappings for both testnet and mainnet."""

    # Discover testnet mappings
    logger.info("Discovering TESTNET spot mappings...")
    async with SpotMappingDiscoverer(is_testnet=True) as discoverer:
        testnet_mappings = await discoverer.discover_mappings()
        testnet_code = discoverer.generate_mapping_code(testnet_mappings)

        logger.info(f"Found {len(testnet_mappings)} testnet mappings")
        print("\n" + "="*80)
        print("TESTNET MAPPINGS:")
        print("="*80)
        print(testnet_code)

        # Save to file
        with open("spot_mappings_testnet.py", "w") as f:
            f.write(testnet_code)

    # Placeholder for mainnet (as requested)
    logger.info("Creating MAINNET placeholder mappings...")
    mainnet_code = """# Spot asset mappings for Hyperliquid mainnet
# Generated on """ + datetime.now().isoformat() + """
# PLACEHOLDER - To be populated when mainnet access is available

MAINNET_SPOT_MAPPINGS = {
    # Asset index 0
    # "SYMBOL/USDC": 0,
    # "@0": 0,

    # TODO: Populate with actual mainnet mappings
}
"""

    print("\n" + "="*80)
    print("MAINNET MAPPINGS (PLACEHOLDER):")
    print("="*80)
    print(mainnet_code)

    # Save to file
    with open("spot_mappings_mainnet.py", "w") as f:
        f.write(mainnet_code)

    # Generate combined mapping function
    combined_code = '''"""Spot asset mappings for Hyperliquid."""

from typing import Optional

''' + testnet_code + '\n' + mainnet_code + '''

def get_spot_mappings(is_testnet: bool = True) -> dict[str, int]:
    """Get spot mappings for the specified network.

    Args:
        is_testnet: True for testnet, False for mainnet

    Returns:
        Dictionary mapping spot symbols to asset indices
    """
    return TESTNET_SPOT_MAPPINGS if is_testnet else MAINNET_SPOT_MAPPINGS


def resolve_spot_symbol(symbol: str, is_testnet: bool = True) -> Optional[int]:
    """Resolve a spot symbol to its asset index.

    Args:
        symbol: The spot symbol (e.g., "@1", "PURR/USDC")
        is_testnet: True for testnet, False for mainnet

    Returns:
        Asset index if found, None otherwise
    """
    mappings = get_spot_mappings(is_testnet)
    return mappings.get(symbol)
'''

    # Save combined file
    with open("hyperliquid_spot_mappings.py", "w") as f:
        f.write(combined_code)

    logger.info("Mapping files generated successfully!")
    logger.info("- spot_mappings_testnet.py")
    logger.info("- spot_mappings_mainnet.py")
    logger.info("- hyperliquid_spot_mappings.py (combined)")


if __name__ == "__main__":
    asyncio.run(main())
