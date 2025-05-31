#!/usr/bin/env python3
"""
Fetch raw JSON data from Hyperliquid public REST API endpoints.

This script collects fresh JSON responses from Hyperliquid's public API endpoints
and saves them as fixture files for testing purposes. It uses the CyberDeltaEngine
configuration system to get exchange settings.

Usage:
    python fetch_hyperliquid_public_data.py --output-dir tests/fixtures/raw_api_data/hyperliquid
    python fetch_hyperliquid_public_data.py --coins ETH,BTC --output-dir fixtures/
"""

import argparse
import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any

import aiohttp

from cyberdelta.config import get_app_settings
from cyberdelta.config.logging_config import setup_logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class HyperliquidDataCollector:
    """Collects raw JSON data from Hyperliquid public API endpoints."""

    def __init__(self, output_dir: Path, session: aiohttp.ClientSession) -> None:
        self.output_dir = output_dir
        self.session = session
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Get configuration
        try:
            app_settings = get_app_settings()
            hyperliquid_config = app_settings.exchanges.get("hyperliquid")
            if not hyperliquid_config:
                raise ValueError("Hyperliquid exchange configuration not found")
            if not hyperliquid_config.enabled:
                raise ValueError("Hyperliquid exchange is disabled in configuration")

            self.api_base_url = str(hyperliquid_config.api_base_url).rstrip("/")
            self.configured_symbols = hyperliquid_config.symbols
            logger.info(f"Using Hyperliquid API base URL: {self.api_base_url}")
            logger.info(f"Configured symbols: {self.configured_symbols}")

        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            # Fallback to hardcoded values for data collection
            self.api_base_url = "https://api.hyperliquid.xyz"
            self.configured_symbols = {"BTC": "BTC", "ETH": "ETH"}
            logger.warning(f"Using fallback configuration: {self.api_base_url}")

    async def _fetch_json(self, url: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        """Fetch JSON data from a URL with POST payload and error handling."""
        try:
            logger.info(f"Fetching: {url} with payload: {payload}")
            async with self.session.post(url, json=payload) as response:
                if response.status == 200:
                    data: dict[str, Any] = await response.json()
                    logger.info(f"Successfully fetched data from {url}")
                    return data
                else:
                    logger.error(f"HTTP {response.status} error for {url}: {await response.text()}")
                    return None
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None

    def _save_json(self, data: dict[str, Any], filename: str) -> None:
        """Save JSON data to a file."""
        filepath = self.output_dir / filename
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved fixture: {filepath}")
        except Exception as e:
            logger.error(f"Error saving {filepath}: {e}")

    async def fetch_meta_and_asset_ctxs(self) -> None:
        """Fetch meta and asset contexts."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "metaAndAssetCtxs"}
        data = await self._fetch_json(url, payload)
        if data:
            self._save_json(data, "hl_info_meta_asset_ctxs.json")

    async def fetch_l2_book(self, coin: str) -> None:
        """Fetch L2 order book for a coin."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "l2Book", "coin": coin}
        data = await self._fetch_json(url, payload)
        if data:
            filename = f"hl_info_l2book_{coin.lower()}.json"
            self._save_json(data, filename)

    async def fetch_recent_trades(self, coin: str) -> None:
        """Fetch recent public trades for a coin."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "recentTrades", "coin": coin}
        data = await self._fetch_json(url, payload)
        if data:
            filename = f"hl_info_recenttrades_{coin.lower()}.json"
            self._save_json(data, filename)

    async def fetch_candle_snapshot(
        self, coin: str, interval: str = "1m", hours_back: int = 1
    ) -> None:
        """Fetch candle snapshot for a coin."""
        url = f"{self.api_base_url}/info"

        # Calculate start and end times in milliseconds
        end_time_ms = int(time.time() * 1000)
        start_time_ms = end_time_ms - (hours_back * 3600 * 1000)

        payload = {
            "type": "candleSnapshot",
            "req": {
                "coin": coin,
                "interval": interval,
                "startTime": start_time_ms,
                "endTime": end_time_ms,
            },
        }

        data = await self._fetch_json(url, payload)
        if data:
            filename = f"hl_info_candlesnapshot_{coin.lower()}_{interval}.json"
            self._save_json(data, filename)

    async def fetch_all_mids(self) -> None:
        """Fetch all mid prices."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "allMids"}
        data = await self._fetch_json(url, payload)
        if data:
            self._save_json(data, "hl_info_allmids.json")

    async def fetch_user_open_orders(self, user: str) -> None:
        """Fetch open orders for a user (public endpoint)."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "openOrders", "user": user}
        data = await self._fetch_json(url, payload)
        if data:
            filename = f"hl_info_openorders_{user[:8]}.json"  # Use first 8 chars of address
            self._save_json(data, filename)

    async def fetch_funding_history(
        self, coin: str, start_time: int | None = None, end_time: int | None = None
    ) -> None:
        """Fetch funding history for a coin."""
        url = f"{self.api_base_url}/info"

        # Default to last 24 hours if no times provided
        if end_time is None:
            end_time = int(time.time() * 1000)
        if start_time is None:
            start_time = end_time - (24 * 3600 * 1000)  # 24 hours ago

        payload = {
            "type": "fundingHistory",
            "coin": coin,
            "startTime": start_time,
            "endTime": end_time,
        }

        data = await self._fetch_json(url, payload)
        if data:
            filename = f"hl_info_fundinghistory_{coin.lower()}.json"
            self._save_json(data, filename)

    def get_default_coins(self) -> list[str]:
        """Get default coins from configuration or fallback."""
        # Use configured symbols, fallback to common ones
        return list(self.configured_symbols.values()) if self.configured_symbols else ["ETH", "BTC"]

    async def collect_all_data(self, coins: list[str]) -> None:
        """Collect data from all endpoints for the given coins."""
        logger.info("Starting Hyperliquid data collection...")

        # System-wide endpoints (no coin required)
        await self.fetch_meta_and_asset_ctxs()
        await self.fetch_all_mids()

        # Coin-specific endpoints
        for coin in coins:
            logger.info(f"Collecting data for coin: {coin}")

            # Basic market data
            await self.fetch_l2_book(coin)
            await self.fetch_recent_trades(coin)
            await self.fetch_candle_snapshot(coin, interval="1m")
            await self.fetch_candle_snapshot(coin, interval="5m")
            await self.fetch_funding_history(coin)

            # Small delay between coins to be respectful to the API
            await asyncio.sleep(0.5)

        # Example user open orders (using a known public address if available)
        # Note: This might not return data for random addresses
        example_user = "0x0000000000000000000000000000000000000000"  # Placeholder
        await self.fetch_user_open_orders(example_user)

        logger.info("Hyperliquid data collection completed!")


async def main() -> None:
    """Main function to run the data collection."""
    parser = argparse.ArgumentParser(description="Fetch Hyperliquid public API data")
    parser.add_argument(
        "--coins",
        type=str,
        default=None,
        help="Comma-separated list of coins to fetch (default: use configured symbols)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="tests/fixtures/raw_api_data/hyperliquid",
        help="Output directory for fixture files",
    )
    parser.add_argument(
        "--config-path",
        type=str,
        default=None,
        help="Path to configuration file (optional, uses default config discovery)",
    )

    args = parser.parse_args()
    output_dir = Path(args.output_dir)

    # Initialize configuration and logging
    try:
        app_settings = get_app_settings()
        setup_logging(app_settings)
        logger.info("Configuration and logging initialized successfully")
    except Exception as e:
        logger.warning(f"Failed to initialize configuration: {e}. Using basic logging.")

    async with aiohttp.ClientSession() as session:
        collector = HyperliquidDataCollector(output_dir, session)

        # Determine coins to collect
        if args.coins:
            coins = [s.strip() for s in args.coins.split(",")]
        else:
            coins = collector.get_default_coins()

        logger.info(f"Collecting data for coins: {coins}")
        logger.info(f"Output directory: {output_dir}")

        await collector.collect_all_data(coins)


if __name__ == "__main__":
    asyncio.run(main())
