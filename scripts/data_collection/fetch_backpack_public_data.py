"""Fetch raw JSON data from Backpack public REST API endpoints.

This script collects fresh JSON responses from Backpack's public API endpoints
and saves them as fixture files for testing purposes. It uses the CyberDeltaEngine
configuration system to get exchange settings.

Usage:
    python fetch_backpack_public_data.py --output-dir tests/fixtures/raw_api_data/backpack/public
    python fetch_backpack_public_data.py --symbols SOL_USDC,BTC_USDC --output-dir fixtures/
"""

import argparse
import asyncio
import json
import time
from http import HTTPStatus
from pathlib import Path
from typing import Any

import aiohttp

from cyberdelta.config import get_app_settings
from cyberdelta.config.logging_config import setup_logging
from cyberdelta.config.structlog_config import get_logger


logger = get_logger(__name__)


class BackpackDataCollector:
    """Collects raw JSON data from Backpack public API endpoints."""

    def __init__(self, output_dir: Path, session: aiohttp.ClientSession) -> None:
        """Initialize the Backpack public data collector with configuration.

        Args:
            output_dir: Directory where collected JSON data files will be saved
            session: aiohttp session for making public API requests
        """
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

            self.api_base_url = str(backpack_config.api_base_url_mainnet).rstrip("/")
            self.configured_symbols = backpack_config.symbols
            logger.info(
                "backpack_api_configured: Using Backpack API base URL",
                api_base_url=self.api_base_url,
            )
            logger.info(
                "backpack_symbols_configured: Configured symbols",
                symbols=self.configured_symbols,
            )

        except Exception as e:
            logger.error("configuration_load_failed: Failed to load configuration", error=str(e))
            # Fallback to hardcoded values for data collection
            self.api_base_url = "https://api.backpack.exchange"
            self.configured_symbols = {"BTC": "BTC_USDC", "SOL": "SOL_USDC"}
            logger.warning(
                "fallback_configuration: Using fallback configuration",
                api_base_url=self.api_base_url,
            )

    async def _fetch_json(
        self,
        url: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Fetch JSON data from a URL with error handling."""
        try:
            logger.info("fetching_json: Fetching JSON data", url=url, params=params)
            async with self.session.get(url, params=params) as response:
                if response.status == HTTPStatus.OK.value:
                    data: dict[str, Any] = await response.json()
                    logger.info("fetch_json_success: Successfully fetched data", url=url)
                    return data
                logger.error(
                    "fetch_json_http_error: HTTP error",
                    status=response.status,
                    url=url,
                    response_text=await response.text(),
                )
                return None
        except Exception as e:
            logger.error("fetch_json_error: Error fetching data", url=url, error=str(e))
            return None

    async def _fetch_text(self, url: str, params: dict[str, Any] | None = None) -> str | None:
        """Fetch text data from a URL with error handling."""
        try:
            logger.info("fetching_text: Fetching text data", url=url, params=params)
            async with self.session.get(url, params=params) as response:
                if response.status == HTTPStatus.OK.value:
                    data: str = await response.text()
                    logger.info("fetch_text_success: Successfully fetched text", url=url)
                    return data
                logger.error(
                    "fetch_text_http_error: HTTP error",
                    status=response.status,
                    url=url,
                    response_text=await response.text(),
                )
                return None
        except Exception as e:
            logger.error("fetch_text_error: Error fetching text", url=url, error=str(e))
            return None

    def _save_json(self, data: dict[str, Any], filename: str) -> None:
        """Save JSON data to a file."""
        filepath = self.output_dir / filename
        try:
            json_content = json.dumps(data, indent=2, ensure_ascii=False)
            filepath.write_text(json_content, encoding="utf-8")
            logger.info("fixture_saved: Saved JSON fixture", filepath=str(filepath))
        except Exception as e:
            logger.error("save_json_error: Error saving JSON", filepath=str(filepath), error=str(e))

    def _save_text(self, data: str, filename: str) -> None:
        """Save text data to a file."""
        filepath = self.output_dir / filename
        try:
            filepath.write_text(data, encoding="utf-8")
            logger.info("fixture_saved: Saved text fixture", filepath=str(filepath))
        except Exception as e:
            logger.error("save_text_error: Error saving text", filepath=str(filepath), error=str(e))

    # System endpoints
    async def fetch_ping(self) -> None:
        """Fetch ping response."""
        url = f"{self.api_base_url}/api/v1/ping"
        data = await self._fetch_text(url)
        if data:
            self._save_text(data, "bp_ping.txt")

    async def fetch_status(self) -> None:
        """Fetch system status."""
        url = f"{self.api_base_url}/api/v1/status"
        data = await self._fetch_json(url)
        if data:
            self._save_json(data, "bp_status.json")

    async def fetch_time(self) -> None:
        """Fetch system time."""
        url = f"{self.api_base_url}/api/v1/time"
        # /api/v1/time returns text/plain, not JSON
        data = await self._fetch_text(url)
        if data:
            self._save_text(data, "bp_time.txt")

    # Market information endpoints
    async def fetch_markets(self) -> None:
        """Fetch markets information."""
        url = f"{self.api_base_url}/api/v1/markets"
        data = await self._fetch_json(url)
        if data:
            self._save_json(data, "bp_markets.json")

    async def fetch_market(self, symbol: str) -> None:
        """Fetch single market information."""
        url = f"{self.api_base_url}/api/v1/market"
        params = {"symbol": symbol}
        data = await self._fetch_json(url, params)
        if data:
            filename = f"bp_market_{symbol.lower()}.json"
            self._save_json(data, filename)

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

    # Market data endpoints
    async def fetch_ticker(self, symbol: str) -> None:
        """Fetch ticker data for a symbol."""
        url = f"{self.api_base_url}/api/v1/ticker"
        params = {"symbol": symbol}
        data = await self._fetch_json(url, params)
        if data:
            filename = f"bp_ticker_{symbol.lower()}.json"
            self._save_json(data, filename)

    async def fetch_tickers(self, interval: str | None = None) -> None:
        """Fetch all tickers."""
        url = f"{self.api_base_url}/api/v1/tickers"
        params = {"interval": interval} if interval else None
        data = await self._fetch_json(url, params)
        if data:
            filename = f"bp_tickers_{interval}.json" if interval else "bp_tickers_all.json"
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

    async def fetch_mark_prices(self, symbol: str | None = None) -> None:
        """Fetch mark prices (only works for futures/perp symbols)."""
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
        """Fetch open interest (only works for futures/perp symbols)."""
        url = f"{self.api_base_url}/api/v1/openInterest"
        params = {"symbol": symbol} if symbol else None
        data = await self._fetch_json(url, params)
        if data:
            if symbol:
                filename = f"bp_openinterest_{symbol.lower()}.json"
            else:
                filename = "bp_openinterest_all.json"
            self._save_json(data, filename)

    async def fetch_funding_rates(self, symbol: str, limit: int = 100, offset: int = 0) -> None:
        """Fetch funding interval rates for futures (only works for perp symbols)."""
        url = f"{self.api_base_url}/api/v1/fundingRates"
        params = {
            "symbol": symbol,
            "limit": str(limit),
            "offset": str(offset),
        }
        data = await self._fetch_json(url, params)
        if data:
            filename = f"bp_funding_rates_{symbol.lower()}_limit{limit}_offset{offset}.json"
            self._save_json(data, filename)

    # Borrow Lend endpoints
    async def fetch_borrow_lend_markets(self) -> None:
        """Fetch borrow lend markets."""
        url = f"{self.api_base_url}/api/v1/borrowLend/markets"
        data = await self._fetch_json(url)
        if data:
            self._save_json(data, "bp_borrow_lend_markets.json")

    async def fetch_borrow_lend_markets_history(
        self,
        interval: str,
        symbol: str | None = None,
    ) -> None:
        """Fetch borrow lend markets history (requires interval parameter)."""
        url = f"{self.api_base_url}/api/v1/borrowLend/markets/history"
        params = {"interval": interval}
        if symbol:
            params["symbol"] = symbol

        data = await self._fetch_json(url, params)
        if data:
            if symbol:
                filename = f"bp_borrow_lend_markets_history_{symbol.lower()}_{interval}.json"
            else:
                filename = f"bp_borrow_lend_markets_history_all_{interval}.json"
            self._save_json(data, filename)

    def get_default_symbols(self) -> list[str]:
        """Get default symbols from configuration or fallback."""
        # Use configured symbols, fallback to common ones
        return (
            list(self.configured_symbols.values())
            if self.configured_symbols
            else ["SOL_USDC", "BTC_USDC"]
        )

    def get_perp_symbols(self) -> list[str]:
        """Get perpetual futures symbols for funding rate data."""
        # Correct perpetual futures symbols on Backpack (with _PERP suffix)
        return [
            "SOL_USDC_PERP",
            "BTC_USDC_PERP",
            "ETH_USDC_PERP",
            "WIF_USDC_PERP",
            "DOGE_USDC_PERP",
        ]

    def get_borrow_lend_symbols(self) -> list[str]:
        """Get common borrow/lend market symbols."""
        # These should be actual market symbols, not just assets
        return ["SOL_USDC", "BTC_USDC", "ETH_USDC", "USDC_USD"]

    async def collect_all_data(self, symbols: list[str]) -> None:
        """Collect data from all public endpoints for the given symbols."""
        logger.info("Starting comprehensive Backpack public data collection...")

        # System endpoints
        logger.info("Fetching system endpoints...")
        await self.fetch_ping()
        await self.fetch_status()
        await self.fetch_time()

        # Market information endpoints
        logger.info("Fetching market information endpoints...")
        await self.fetch_markets()
        await self.fetch_assets()
        await self.fetch_collateral()

        # Global market data endpoints
        logger.info("Fetching global market data...")
        await self.fetch_tickers()  # All tickers
        await self.fetch_tickers("1d")  # 1d interval tickers (valid)
        await self.fetch_tickers("1w")  # 1w interval tickers (valid)
        await self.fetch_mark_prices()  # All mark prices
        await self.fetch_open_interest()  # All open interest

        # Borrow Lend market endpoints
        logger.info("Fetching borrow/lend market data...")
        await self.fetch_borrow_lend_markets()

        # Fetch borrow/lend history with valid intervals
        borrow_lend_intervals = ["1d", "1w", "1month", "1year"]
        for interval in borrow_lend_intervals:
            await self.fetch_borrow_lend_markets_history(interval)  # All markets
            await asyncio.sleep(0.2)  # Small delay between requests

        # Fetch borrow/lend history for specific market symbols
        for symbol in self.get_borrow_lend_symbols():
            for interval in ["1d", "1w"]:  # Just use shorter intervals for specific symbols
                await self.fetch_borrow_lend_markets_history(interval, symbol)
                await asyncio.sleep(0.2)

        # Symbol-specific endpoints (Spot markets)
        logger.info("Fetching symbol-specific data...")
        for symbol in symbols:
            logger.info("collecting_symbol_data: Collecting data for symbol", symbol=symbol)

            # Basic market data
            await self.fetch_market(symbol)
            await self.fetch_ticker(symbol)
            await self.fetch_depth(symbol)
            await self.fetch_depth(symbol, limit=20)
            await self.fetch_depth(symbol, limit=100)
            await self.fetch_trades(symbol, limit=50)
            await self.fetch_trades(symbol, limit=100)

            # Klines with different intervals
            await self.fetch_klines(symbol, interval="1m", hours_back=1)
            await self.fetch_klines(symbol, interval="5m", hours_back=2)
            await self.fetch_klines(symbol, interval="1h", hours_back=24)
            await self.fetch_klines(symbol, interval="1d", hours_back=168)  # 7 days

            # Note: Mark prices and open interest don't work for spot symbols
            # They only work for perpetual futures symbols

            # Small delay between symbols to be respectful to the API
            await asyncio.sleep(0.5)

        # Perpetual futures-specific data
        logger.info("Fetching perpetual futures data...")
        perp_symbols = self.get_perp_symbols()
        for symbol in perp_symbols:
            logger.info("collecting_perp_data: Collecting perp data", symbol=symbol)

            # Basic market data for perp symbols
            await self.fetch_market(symbol)
            await self.fetch_ticker(symbol)
            await self.fetch_depth(symbol, limit=20)
            await self.fetch_trades(symbol, limit=50)
            await self.fetch_klines(symbol, interval="1h", hours_back=6)

            # Perp-specific data
            await self.fetch_mark_prices(symbol)
            await self.fetch_open_interest(symbol)
            await self.fetch_funding_rates(symbol, limit=50)
            await self.fetch_funding_rates(symbol, limit=100, offset=50)
            await asyncio.sleep(0.3)

        logger.info("Comprehensive Backpack public data collection completed!")


async def main() -> None:
    """Execute comprehensive Backpack public API data collection process.

    Parses command-line arguments, sets up configuration and HTTP session,
    and runs the complete public data collection across all Backpack public
    REST API endpoints. Saves collected market data as JSON fixtures for testing.
    """
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
        default="tests/fixtures/raw_api_data/backpack/public",
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
        logger.warning(
            "configuration_init_failed: Failed to initialize configuration. Using basic logging.",
            error=str(e),
        )

    async with aiohttp.ClientSession() as session:
        collector = BackpackDataCollector(output_dir, session)

        # Determine symbols to collect
        if args.symbols:
            symbols = [s.strip() for s in args.symbols.split(",")]
        else:
            symbols = collector.get_default_symbols()

        logger.info("data_collection_starting: Collecting data for symbols", symbols=symbols)
        logger.info("output_directory: Output directory configured", output_dir=str(output_dir))

        await collector.collect_all_data(symbols)


if __name__ == "__main__":
    asyncio.run(main())
