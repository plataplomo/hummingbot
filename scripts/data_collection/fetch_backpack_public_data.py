#!/usr/bin/env python3
"""
Fetch raw JSON data from Backpack public REST API endpoints.

This script collects fresh JSON responses from Backpack's public API endpoints
and saves them as fixture files for testing purposes. It uses the CyberDeltaEngine
configuration system to get exchange settings.

Usage:
    python fetch_backpack_public_data.py --output-dir tests/fixtures/raw_api_data/backpack
    python fetch_backpack_public_data.py --symbols SOL_USDC,BTC_USDC --output-dir fixtures/
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


class BackpackDataCollector:
    """Collects raw JSON data from Backpack public API endpoints."""

    def __init__(self, output_dir: Path, session: aiohttp.ClientSession) -> None:
        self.output_dir = output_dir
        self.session = session
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Get configuration
        try:
            app_settings = get_app_settings()
            backpack_config = app_settings.exchanges.get("backpack")
            if not backpack_config:
                raise ValueError("Backpack exchange configuration not found")
            if not backpack_config.enabled:
                raise ValueError("Backpack exchange is disabled in configuration")

            self.api_base_url = str(backpack_config.api_base_url).rstrip("/")
            self.configured_symbols = backpack_config.symbols
            logger.info(f"Using Backpack API base URL: {self.api_base_url}")
            logger.info(f"Configured symbols: {self.configured_symbols}")

        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            # Fallback to hardcoded values for data collection
            self.api_base_url = "https://api.backpack.exchange"
            self.configured_symbols = {"BTC": "BTC_USDC", "SOL": "SOL_USDC"}
            logger.warning(f"Using fallback configuration: {self.api_base_url}")

    async def _fetch_json(
        self, url: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
        """Fetch JSON data from a URL with error handling."""
        try:
            logger.info(f"Fetching: {url} with params: {params}")
            async with self.session.get(url, params=params) as response:
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

    async def fetch_ticker(self, symbol: str) -> None:
        """Fetch ticker data for a symbol."""
        url = f"{self.api_base_url}/api/v1/ticker"
        params = {"symbol": symbol}
        data = await self._fetch_json(url, params)
        if data:
            filename = f"bp_ticker_{symbol.lower()}.json"
            self._save_json(data, filename)

    async def fetch_depth(self, symbol: str, limit: int | None = None) -> None:
        """Fetch order book depth for a symbol."""
        url = f"{self.api_base_url}/api/v1/depth"
        params = {"symbol": symbol}
        if limit:
            params["limit"] = str(limit)

        data = await self._fetch_json(url, params)
        if data:
            if limit:
                filename = f"bp_depth_{symbol.lower()}_limit{limit}.json"
            else:
                filename = f"bp_depth_{symbol.lower()}.json"
            self._save_json(data, filename)

    async def fetch_trades(self, symbol: str, limit: int = 100) -> None:
        """Fetch recent public trades for a symbol."""
        url = f"{self.api_base_url}/api/v1/trades"
        params = {"symbol": symbol, "limit": str(limit)}
        data = await self._fetch_json(url, params)
        if data:
            filename = f"bp_trades_{symbol.lower()}_limit{limit}.json"
            self._save_json(data, filename)

    async def fetch_klines(self, symbol: str, interval: str = "1m", hours_back: int = 1) -> None:
        """Fetch klines (candlestick) data for a symbol."""
        url = f"{self.api_base_url}/api/v1/klines"

        # Calculate start time (hours_back hours ago)
        end_time = int(time.time())
        start_time = end_time - (hours_back * 3600)

        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_time,
            "endTime": end_time,
        }

        data = await self._fetch_json(url, params)
        if data:
            filename = f"bp_klines_{symbol.lower()}_{interval}.json"
            self._save_json(data, filename)

    async def fetch_status(self) -> None:
        """Fetch system status."""
        url = f"{self.api_base_url}/api/v1/status"
        data = await self._fetch_json(url)
        if data:
            self._save_json(data, "bp_status.json")

    async def fetch_time(self) -> None:
        """Fetch system time."""
        url = f"{self.api_base_url}/api/v1/time"
        data = await self._fetch_json(url)
        if data:
            self._save_json(data, "bp_time.json")

    async def fetch_markets(self) -> None:
        """Fetch markets information."""
        url = f"{self.api_base_url}/api/v1/markets"
        data = await self._fetch_json(url)
        if data:
            self._save_json(data, "bp_markets.json")

    async def fetch_assets(self) -> None:
        """Fetch assets information."""
        url = f"{self.api_base_url}/api/v1/assets"
        data = await self._fetch_json(url)
        if data:
            self._save_json(data, "bp_assets.json")

    async def fetch_collateral(self) -> None:
        """Fetch collateral information."""
        url = f"{self.api_base_url}/api/v1/collateral"
        data = await self._fetch_json(url)
        if data:
            self._save_json(data, "bp_collateral.json")

    async def fetch_mark_prices(self, symbol: str | None = None) -> None:
        """Fetch mark prices."""
        url = f"{self.api_base_url}/api/v1/markPrices"
        params = {"symbol": symbol} if symbol else None
        data = await self._fetch_json(url, params)
        if data:
            if symbol:
                filename = f"bp_markprices_{symbol.lower()}.json"
            else:
                filename = "bp_markprices_all.json"
            self._save_json(data, filename)

    async def fetch_open_interest(self, symbol: str | None = None) -> None:
        """Fetch open interest."""
        url = f"{self.api_base_url}/api/v1/openInterest"
        params = {"symbol": symbol} if symbol else None
        data = await self._fetch_json(url, params)
        if data:
            if symbol:
                filename = f"bp_openinterest_{symbol.lower()}.json"
            else:
                filename = "bp_openinterest_all.json"
            self._save_json(data, filename)

    def get_default_symbols(self) -> list[str]:
        """Get default symbols from configuration or fallback."""
        # Use configured symbols, fallback to common ones
        return (
            list(self.configured_symbols.values())
            if self.configured_symbols
            else ["SOL_USDC", "BTC_USDC"]
        )

    async def collect_all_data(self, symbols: list[str]) -> None:
        """Collect data from all endpoints for the given symbols."""
        logger.info("Starting Backpack data collection...")

        # System-wide endpoints (no symbol required)
        await self.fetch_status()
        await self.fetch_time()
        await self.fetch_markets()
        await self.fetch_assets()
        await self.fetch_collateral()
        await self.fetch_mark_prices()  # All mark prices
        await self.fetch_open_interest()  # All open interest

        # Symbol-specific endpoints
        for symbol in symbols:
            logger.info(f"Collecting data for symbol: {symbol}")

            # Basic market data
            await self.fetch_ticker(symbol)
            await self.fetch_depth(symbol)
            await self.fetch_depth(symbol, limit=20)
            await self.fetch_trades(symbol, limit=100)
            await self.fetch_klines(symbol, interval="1m")
            await self.fetch_klines(symbol, interval="5m")

            # Symbol-specific mark prices and open interest
            await self.fetch_mark_prices(symbol)
            await self.fetch_open_interest(symbol)

            # Small delay between symbols to be respectful to the API
            await asyncio.sleep(0.5)

        logger.info("Backpack data collection completed!")


async def main() -> None:
    """Main function to run the data collection."""
    parser = argparse.ArgumentParser(description="Fetch Backpack public API data")
    parser.add_argument(
        "--symbols",
        type=str,
        default=None,
        help="Comma-separated list of symbols to fetch (default: use configured symbols)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="tests/fixtures/raw_api_data/backpack",
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
        collector = BackpackDataCollector(output_dir, session)

        # Determine symbols to collect
        if args.symbols:
            symbols = [s.strip() for s in args.symbols.split(",")]
        else:
            symbols = collector.get_default_symbols()

        logger.info(f"Collecting data for symbols: {symbols}")
        logger.info(f"Output directory: {output_dir}")

        await collector.collect_all_data(symbols)


if __name__ == "__main__":
    asyncio.run(main())
