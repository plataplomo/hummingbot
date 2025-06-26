#!/usr/bin/env python3
"""Fetch raw JSON data from Backpack private API endpoints.

This script collects fresh JSON responses from Backpack's private API endpoints
that require authentication and saves them as fixture files for testing purposes.
It uses the CyberDeltaEngine configuration system and Backpack ED25519 authenticator.

Based on the OpenAPI specification (openapi_backpack.json), this script comprehensively
covers ALL private endpoints that require authentication.

Usage:
    python fetch_backpack_private_data.py --output-dir tests/fixtures/raw_api_data/backpack/private
    python fetch_backpack_private_data.py --symbols SOL_USDC,BTC_USDC --output-dir fixtures/
"""

import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

import aiohttp


# Add the project root to the Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
os.chdir(project_root)  # Change to project root to ensure imports work

# Configure logging before other imports
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def lazy_import_authenticator() -> type:
    """Lazily import the authenticator to avoid circular imports."""
    from cyberdelta.apis.backpack.bp_auth import BackpackEd25519Authenticator

    return BackpackEd25519Authenticator


def lazy_import_config() -> tuple[Any, Any, Any]:
    """Lazily import config functions to avoid circular imports."""
    from cyberdelta.config import get_app_settings, get_secrets_config
    from cyberdelta.config.logging_config import setup_logging

    return get_app_settings, get_secrets_config, setup_logging


class BackpackPrivateDataCollector:
    """Collects raw JSON data from Backpack private API endpoints."""

    def __init__(self, output_dir: Path, session: aiohttp.ClientSession) -> None:
        """Initialize the Backpack private data collector with authentication setup.

        Args:
            output_dir: Directory where collected JSON data files will be saved
            session: Authenticated aiohttp session for making API requests
        """
        self.output_dir = output_dir
        self.session = session
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Lazy import to avoid circular dependencies
        get_app_settings, get_secrets_config, _ = lazy_import_config()
        authenticator_class = lazy_import_authenticator()

        # Initialize configuration and authenticator
        try:
            app_settings = get_app_settings()
            backpack_config = app_settings.exchanges.get("backpack")
            if not backpack_config:
                raise ValueError("Backpack exchange configuration not found")
            if not backpack_config.enabled:
                raise ValueError("Backpack exchange is disabled in configuration")

            self.api_base_url = str(backpack_config.api_base_url).rstrip("/")
            self.configured_symbols = backpack_config.symbols

            # Get authentication secrets
            secrets = get_secrets_config()
            if not secrets or not secrets.exchanges:
                raise ValueError("Exchange secrets configuration not found")

            backpack_secrets = secrets.exchanges.get("backpack")
            if not backpack_secrets:
                raise ValueError("Backpack exchange secrets not found")

            if backpack_secrets.auth_type != "api_key":
                raise ValueError("Backpack authentication must be 'api_key' type")

            # Initialize authenticator
            self.authenticator = authenticator_class(
                api_key_b64_secret=backpack_secrets.api_key,
                private_key_b64_secret=backpack_secrets.api_secret,
            )

            logger.info(f"Using Backpack API base URL: {self.api_base_url}")
            logger.info(f"Configured symbols: {self.configured_symbols}")
            logger.info("Backpack authenticator initialized successfully")

        except Exception as e:
            logger.error(f"Failed to load configuration or initialize authenticator: {e}")
            raise

    async def _fetch_authenticated_json(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Fetch JSON data from an authenticated endpoint with error handling."""
        try:
            # Prepare authenticated request
            auth_components = await self.authenticator.prepare_request(
                method=method,
                path=path,
                params=params,
                data=data,
                headers=None,
            )

            url = f"{self.api_base_url}{path}"

            logger.info(f"Fetching: {method} {url} with params: {params}")

            # Make the request with authentication headers
            # For POST/PUT methods, always send JSON data (even if empty) for proper content-type
            json_data = (
                auth_components.data
                if method.upper() in ["POST", "PUT"]
                else (auth_components.data or None)
            )

            # Add timeout to prevent signature expiration (Backpack has 5 second window)
            timeout = aiohttp.ClientTimeout(total=30)  # 30 second total timeout
            async with self.session.request(
                method,
                url,
                params=auth_components.params,
                json=json_data,
                headers=auth_components.headers,
                timeout=timeout,
            ) as response:
                if response.status == 200:
                    response_data: dict[str, Any] = await response.json()
                    logger.info(f"Successfully fetched data from {url}")
                    return response_data
                error_text = await response.text()
                logger.error(f"HTTP {response.status} error for {url}: {error_text}")
                return None
        except Exception as e:
            logger.error(f"Error fetching {method} {path}: {e}")
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

    # Account Management Endpoints
    async def fetch_account_info(self) -> None:
        """Fetch account information."""
        data = await self._fetch_authenticated_json("GET", "/api/v1/account")
        if data:
            self._save_json(data, "bp_private_account.json")

    async def fetch_convert_dust(self, symbol: str = "SOL") -> None:
        """Convert dust balances to USDC for a specific asset."""
        # This endpoint requires a symbol parameter specifying which asset's dust to convert
        # Using SOL as default as it's commonly held and likely to have dust
        payload = {"symbol": symbol}
        data = await self._fetch_authenticated_json(
            "POST",
            "/api/v1/account/convertDust",
            data=payload,
        )
        if data:
            filename = f"bp_private_convert_dust_{symbol.lower()}.json"
            self._save_json(data, filename)

    # Account Limits Endpoints
    async def fetch_max_borrow_quantity(self, symbol: str) -> None:
        """Get max borrow quantity for an asset."""
        # Extract asset symbol from trading pair (e.g., BTC from BTC_USDC)
        asset = symbol.split("_")[0] if "_" in symbol else symbol
        data = await self._fetch_authenticated_json(
            "GET",
            "/api/v1/account/limits/borrow",
            params={"symbol": asset},
        )
        if data:
            filename = f"bp_private_max_borrow_{asset.lower()}.json"
            self._save_json(data, filename)

    async def fetch_max_order_quantity(
        self,
        symbol: str,
        side: str,
        price: str | None = None,
    ) -> None:
        """Get max order quantity based on balances."""
        # Convert to proper Backpack enum values
        side_enum = "Bid" if side.lower() == "buy" else "Ask"
        params = {"symbol": symbol, "side": side_enum}
        if price:
            params["price"] = price

        data = await self._fetch_authenticated_json(
            "GET",
            "/api/v1/account/limits/order",
            params=params,
        )
        if data:
            filename = f"bp_private_max_order_{symbol.lower()}_{side.lower()}.json"
            self._save_json(data, filename)

    async def fetch_max_withdrawal_quantity(self, symbol: str) -> None:
        """Get max withdrawal quantity."""
        # Extract asset symbol from trading pair
        asset = symbol.split("_")[0] if "_" in symbol else symbol
        data = await self._fetch_authenticated_json(
            "GET",
            "/api/v1/account/limits/withdrawal",
            params={"symbol": asset},
        )
        if data:
            filename = f"bp_private_max_withdrawal_{asset.lower()}.json"
            self._save_json(data, filename)

    # Capital & Balance Endpoints
    async def fetch_capital_balances(self) -> None:
        """Get account balances."""
        data = await self._fetch_authenticated_json("GET", "/api/v1/capital")
        if data:
            self._save_json(data, "bp_private_capital_balances.json")

    async def fetch_capital_collateral(self) -> None:
        """Get collateral information."""
        data = await self._fetch_authenticated_json("GET", "/api/v1/capital/collateral")
        if data:
            self._save_json(data, "bp_private_capital_collateral.json")

    async def fetch_collateral_info(self) -> None:
        """Get collateral parameters."""
        data = await self._fetch_authenticated_json("GET", "/api/v1/collateral")
        if data:
            self._save_json(data, "bp_private_collateral.json")

    # Deposit Endpoints
    async def fetch_deposit_history(
        self,
        limit: int = 100,
        offset: int = 0,
        from_ts: int | None = None,
        to_ts: int | None = None,
    ) -> None:
        """Get deposit history."""
        params = {"limit": str(limit), "offset": str(offset)}
        if from_ts:
            params["from"] = str(from_ts)
        if to_ts:
            params["to"] = str(to_ts)

        data = await self._fetch_authenticated_json(
            "GET",
            "/wapi/v1/capital/deposits",
            params=params,
        )
        if data:
            filename = f"bp_private_deposits_limit{limit}_offset{offset}.json"
            self._save_json(data, filename)

    async def fetch_deposit_address(self, blockchain: str) -> None:
        """Get deposit address for blockchain."""
        data = await self._fetch_authenticated_json(
            "GET",
            "/wapi/v1/capital/deposit/address",
            params={"blockchain": blockchain},
        )
        if data:
            filename = f"bp_private_deposit_address_{blockchain.lower()}.json"
            self._save_json(data, filename)

    # Withdrawal Endpoints
    async def fetch_withdrawal_history(
        self,
        limit: int = 100,
        offset: int = 0,
        from_ts: int | None = None,
        to_ts: int | None = None,
    ) -> None:
        """Get withdrawal history."""
        params = {"limit": str(limit), "offset": str(offset)}
        if from_ts:
            params["from"] = str(from_ts)
        if to_ts:
            params["to"] = str(to_ts)

        data = await self._fetch_authenticated_json(
            "GET",
            "/wapi/v1/capital/withdrawals",
            params=params,
        )
        if data:
            filename = f"bp_private_withdrawals_limit{limit}_offset{offset}.json"
            self._save_json(data, filename)

    # Trading - Orders Endpoints
    async def fetch_open_orders(self, symbol: str | None = None) -> None:
        """Get all open orders."""
        params = {"symbol": symbol} if symbol else None
        data = await self._fetch_authenticated_json("GET", "/api/v1/orders", params=params)
        if data:
            if symbol:
                filename = f"bp_private_open_orders_{symbol.lower()}.json"
            else:
                filename = "bp_private_open_orders_all.json"
            self._save_json(data, filename)

    async def fetch_specific_order(self, symbol: str, order_id: str) -> None:
        """Get specific open order."""
        data = await self._fetch_authenticated_json(
            "GET",
            "/api/v1/order",
            params={"symbol": symbol, "orderId": order_id},
        )
        if data:
            filename = f"bp_private_order_{symbol.lower()}_{order_id}.json"
            self._save_json(data, filename)

    # Trading - Positions Endpoints
    async def fetch_position_summary(self, symbol: str | None = None) -> None:
        """Get account position summary."""
        # Note: Backpack /api/v1/position endpoint does not support symbol filtering
        # It returns all positions regardless of symbol parameter
        if symbol:
            logger.warning(
                f"Position endpoint doesn't support symbol filtering. "
                f"Fetching all positions instead of {symbol}",
            )

        data = await self._fetch_authenticated_json("GET", "/api/v1/position", params=None)
        if data:
            if symbol:
                filename = f"bp_private_positions_all_requested_{symbol.lower()}.json"
            else:
                filename = "bp_private_positions_all.json"
            self._save_json(data, filename)

    async def fetch_borrow_lend_positions(self) -> None:
        """Get borrow/lend positions."""
        data = await self._fetch_authenticated_json("GET", "/api/v1/borrowLend/positions")
        if data:
            self._save_json(data, "bp_private_borrow_lend_positions.json")

    # History Endpoints
    async def fetch_order_history(
        self,
        symbol: str | None = None,
        limit: int = 100,
        offset: int = 0,
        order_id: str | None = None,
        from_ts: int | None = None,
        to_ts: int | None = None,
    ) -> None:
        """Get historical orders."""
        params = {"limit": str(limit), "offset": str(offset)}
        if symbol:
            params["symbol"] = symbol
        if order_id:
            params["orderId"] = order_id
        if from_ts:
            params["from"] = str(from_ts)
        if to_ts:
            params["to"] = str(to_ts)

        data = await self._fetch_authenticated_json("GET", "/wapi/v1/history/orders", params=params)
        if data:
            symbol_part = symbol.lower() if symbol else "all"
            filename = f"bp_private_order_history_{symbol_part}_limit{limit}_offset{offset}.json"
            self._save_json(data, filename)

    async def fetch_fill_history(
        self,
        symbol: str | None = None,
        limit: int = 100,
        offset: int = 0,
        from_ts: int | None = None,
        to_ts: int | None = None,
        fill_type: str | None = None,
    ) -> None:
        """Get fill history."""
        params = {"limit": str(limit), "offset": str(offset)}
        if symbol:
            params["symbol"] = symbol
        if from_ts:
            params["from"] = str(from_ts)
        if to_ts:
            params["to"] = str(to_ts)
        if fill_type:
            params["fillType"] = fill_type

        data = await self._fetch_authenticated_json("GET", "/wapi/v1/history/fills", params=params)
        if data:
            symbol_part = symbol.lower() if symbol else "all"
            filename = f"bp_private_fill_history_{symbol_part}_limit{limit}_offset{offset}.json"
            self._save_json(data, filename)

    async def fetch_trade_history(self, symbol: str, limit: int = 100, offset: int = 0) -> None:
        """Get historical trades for symbol."""
        params = {"symbol": symbol, "limit": str(limit), "offset": str(offset)}
        data = await self._fetch_authenticated_json("GET", "/api/v1/trades/history", params=params)
        if data:
            filename = f"bp_private_trade_history_{symbol.lower()}_limit{limit}_offset{offset}.json"
            self._save_json(data, filename)

    async def fetch_funding_history(
        self,
        symbol: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> None:
        """Get funding payment history."""
        params = {"limit": str(limit), "offset": str(offset)}
        if symbol:
            params["symbol"] = symbol

        data = await self._fetch_authenticated_json(
            "GET",
            "/wapi/v1/history/funding",
            params=params,
        )
        if data:
            symbol_part = symbol.lower() if symbol else "all"
            filename = f"bp_private_funding_history_{symbol_part}_limit{limit}_offset{offset}.json"
            self._save_json(data, filename)

    async def fetch_pnl_history(
        self,
        symbol: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> None:
        """Get profit/loss history."""
        params = {"limit": str(limit), "offset": str(offset)}
        if symbol:
            params["symbol"] = symbol

        data = await self._fetch_authenticated_json("GET", "/wapi/v1/history/pnl", params=params)
        if data:
            symbol_part = symbol.lower() if symbol else "all"
            filename = f"bp_private_pnl_history_{symbol_part}_limit{limit}_offset{offset}.json"
            self._save_json(data, filename)

    async def fetch_settlement_history(
        self,
        symbol: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> None:
        """Get settlement history."""
        params = {"limit": str(limit), "offset": str(offset)}
        if symbol:
            params["symbol"] = symbol

        data = await self._fetch_authenticated_json(
            "GET",
            "/wapi/v1/history/settlement",
            params=params,
        )
        if data:
            symbol_part = symbol.lower() if symbol else "all"
            filename = (
                f"bp_private_settlement_history_{symbol_part}_limit{limit}_offset{offset}.json"
            )
            self._save_json(data, filename)

    # Borrow/Lend History Endpoints
    async def fetch_borrow_lend_history(
        self,
        limit: int = 100,
        offset: int = 0,
    ) -> None:
        """Get borrow/lend operation history."""
        params = {"limit": str(limit), "offset": str(offset)}
        data = await self._fetch_authenticated_json(
            "GET",
            "/wapi/v1/history/borrowLend",
            params=params,
        )
        if data:
            filename = f"bp_private_borrow_lend_history_limit{limit}_offset{offset}.json"
            self._save_json(data, filename)

    async def fetch_borrow_lend_position_history(
        self,
        limit: int = 100,
        offset: int = 0,
    ) -> None:
        """Get borrow/lend position history."""
        params = {"limit": str(limit), "offset": str(offset)}
        data = await self._fetch_authenticated_json(
            "GET",
            "/wapi/v1/history/borrowLend/positions",
            params=params,
        )
        if data:
            filename = f"bp_private_borrow_lend_position_history_limit{limit}_offset{offset}.json"
            self._save_json(data, filename)

    async def fetch_interest_history(
        self,
        limit: int = 100,
        offset: int = 0,
    ) -> None:
        """Get interest payment history."""
        params = {"limit": str(limit), "offset": str(offset)}
        data = await self._fetch_authenticated_json(
            "GET",
            "/wapi/v1/history/interest",
            params=params,
        )
        if data:
            filename = f"bp_private_interest_history_limit{limit}_offset{offset}.json"
            self._save_json(data, filename)

    async def fetch_borrow_lend_market_history(self, symbol: str, interval: str) -> None:
        """Get borrow/lend market history."""
        params = {"symbol": symbol, "interval": interval}
        data = await self._fetch_authenticated_json(
            "GET",
            "/api/v1/borrowLend/markets/history",
            params=params,
        )
        if data:
            filename = f"bp_private_borrow_lend_market_history_{symbol.lower()}_{interval}.json"
            self._save_json(data, filename)

    # RFQ (Request for Quote) Endpoints
    async def fetch_account_rfqs(self) -> None:
        """Get account's RFQs and quotes."""
        data = await self._fetch_authenticated_json("GET", "/api/v1/rfq")
        if data:
            self._save_json(data, "bp_private_account_rfqs.json")

    async def fetch_all_open_rfqs(self) -> None:
        """Get all open RFQs."""
        data = await self._fetch_authenticated_json("GET", "/api/v1/rfq/all")
        if data:
            self._save_json(data, "bp_private_all_open_rfqs.json")

    async def fetch_account_quotes(self) -> None:
        """Get account's submitted quotes."""
        data = await self._fetch_authenticated_json("GET", "/api/v1/rfq/quote")
        if data:
            self._save_json(data, "bp_private_account_quotes.json")

    def get_default_symbols(self) -> list[str]:
        """Get default symbols from configuration or fallback."""
        return (
            list(self.configured_symbols.values())
            if self.configured_symbols
            else ["SOL_USDC", "BTC_USDC", "ETH_USDC"]
        )

    def get_sample_order_ids(self) -> list[str]:
        """Get sample order IDs for testing (these would be real order IDs in practice)."""
        return ["123456789", "987654321", "555666777"]

    def get_blockchains(self) -> list[str]:
        """Get supported blockchains for deposit addresses."""
        # Only include blockchains that are known to work with Backpack
        # Removed: "Polygon" (not supported), "Bitcoin" (treasury service issues)
        return ["Solana", "Ethereum"]

    def get_borrow_lend_intervals(self) -> list[str]:
        """Get valid intervals for borrow/lend history."""
        return ["1d", "1w", "1month", "1year"]

    def get_dust_conversion_assets(self) -> list[str]:
        """Get common assets that might have dust to convert."""
        # Test dust conversion for common assets that users might hold
        return ["SOL", "BTC", "ETH"]

    async def _collect_account_management_data(self) -> None:
        """Collect account management data."""
        logger.info("Fetching account management data...")
        await self.fetch_account_info()

        # Test dust conversion for multiple assets
        dust_assets = self.get_dust_conversion_assets()
        for asset in dust_assets:
            await self.fetch_convert_dust(asset)
            await asyncio.sleep(0.1)  # Small delay between requests

    async def _collect_capital_and_balance_data(self) -> None:
        """Collect capital and balance data."""
        logger.info("Fetching capital and balance data...")
        await self.fetch_capital_balances()
        await self.fetch_capital_collateral()
        await self.fetch_collateral_info()

    async def _collect_account_limits_data(self, symbols: list[str]) -> None:
        """Collect account limits data for each symbol."""
        logger.info("Fetching account limits data...")
        for symbol in symbols:
            await self.fetch_max_borrow_quantity(symbol)
            await self.fetch_max_order_quantity(symbol, "buy", "100.0")  # lowercase
            await self.fetch_max_order_quantity(symbol, "sell", "100.0")  # lowercase
            await self.fetch_max_withdrawal_quantity(symbol)
            await asyncio.sleep(0.2)

    async def _collect_current_positions_data(self, symbols: list[str]) -> None:
        """Collect current trading positions data."""
        logger.info("Fetching current trading positions...")
        await self.fetch_position_summary()
        for symbol in symbols:
            await self.fetch_position_summary(symbol)
            await asyncio.sleep(0.1)
        await self.fetch_borrow_lend_positions()

    async def _collect_current_orders_data(self, symbols: list[str]) -> None:
        """Collect current open orders data."""
        logger.info("Fetching current open orders...")
        await self.fetch_open_orders()
        for symbol in symbols:
            await self.fetch_open_orders(symbol)
            await asyncio.sleep(0.1)

        # Sample specific order queries (would use real order IDs in practice)
        sample_order_ids = self.get_sample_order_ids()
        for symbol in symbols[:2]:  # Just test a couple symbols
            for order_id in sample_order_ids[:2]:  # Just test a couple order IDs
                await self.fetch_specific_order(symbol, order_id)
                await asyncio.sleep(0.1)

    async def _collect_deposits_withdrawals_data(self) -> None:
        """Collect deposits and withdrawals data."""
        logger.info("Fetching deposit and withdrawal data...")
        await self.fetch_deposit_history(limit=50)
        await self.fetch_deposit_history(limit=100, offset=50)
        await self.fetch_withdrawal_history(limit=50)
        await self.fetch_withdrawal_history(limit=100, offset=50)

        # Deposit addresses for different blockchains
        blockchains = self.get_blockchains()
        for blockchain in blockchains:
            await self.fetch_deposit_address(blockchain)
            await asyncio.sleep(0.1)

    async def _collect_historical_trading_data(self, symbols: list[str]) -> None:
        """Collect historical trading data."""
        logger.info("Fetching historical trading data...")

        # Order history
        await self.fetch_order_history(limit=100)
        for symbol in symbols:
            await self.fetch_order_history(symbol=symbol, limit=50)
            await asyncio.sleep(0.2)

        # Fill history
        await self.fetch_fill_history(limit=100)
        for symbol in symbols:
            await self.fetch_fill_history(symbol=symbol, limit=50)
            # Test different fill types (only User is valid, System was removed)
            await self.fetch_fill_history(symbol=symbol, limit=25, fill_type="User")
            await asyncio.sleep(0.2)

        # Trade history
        for symbol in symbols:
            await self.fetch_trade_history(symbol, limit=50)
            await asyncio.sleep(0.2)

        # Funding history
        await self.fetch_funding_history(limit=100)
        for symbol in symbols:
            await self.fetch_funding_history(symbol=symbol, limit=50)
            await asyncio.sleep(0.1)

        # P&L history
        await self.fetch_pnl_history(limit=100)
        for symbol in symbols:
            await self.fetch_pnl_history(symbol=symbol, limit=50)
            await asyncio.sleep(0.1)

        # Settlement history
        await self.fetch_settlement_history(limit=100)
        for symbol in symbols:
            await self.fetch_settlement_history(symbol=symbol, limit=50)
            await asyncio.sleep(0.1)

    async def _collect_borrow_lend_data(self, symbols: list[str]) -> None:
        """Collect borrow/lend data."""
        logger.info("Fetching borrow/lend data...")
        await self.fetch_borrow_lend_history(limit=100)
        await self.fetch_borrow_lend_position_history(limit=100)
        await self.fetch_interest_history(limit=100)

        # Borrow/lend market history
        intervals = self.get_borrow_lend_intervals()
        for symbol in symbols[:2]:  # Just test a couple symbols
            for interval in intervals[:2]:  # Just test a couple intervals
                await self.fetch_borrow_lend_market_history(symbol, interval)
                await asyncio.sleep(0.1)

    async def _collect_rfq_data(self) -> None:
        """Collect RFQ data."""
        logger.info("Fetching RFQ data...")
        await self.fetch_account_rfqs()
        await self.fetch_all_open_rfqs()
        await self.fetch_account_quotes()

    async def collect_all_private_data(self, symbols: list[str]) -> None:
        """Collect data from all private endpoints."""
        logger.info("Starting comprehensive Backpack private data collection...")

        await self._collect_account_management_data()
        await self._collect_capital_and_balance_data()
        await self._collect_account_limits_data(symbols)
        await self._collect_current_positions_data(symbols)
        await self._collect_current_orders_data(symbols)
        await self._collect_deposits_withdrawals_data()
        await self._collect_historical_trading_data(symbols)
        await self._collect_borrow_lend_data(symbols)
        await self._collect_rfq_data()

        logger.info("Comprehensive Backpack private data collection completed!")


async def main() -> None:
    """Execute comprehensive Backpack private API data collection with authentication.

    Parses command-line arguments, sets up configuration and authentication,
    and runs the complete private data collection process across all authenticated
    Backpack API endpoints. Saves collected data as JSON fixtures for testing.
    """
    parser = argparse.ArgumentParser(description="Fetch Backpack private API data")
    parser.add_argument(
        "--symbols",
        type=str,
        default=None,
        help="Comma-separated list of symbols to fetch (default: use configured symbols)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="tests/fixtures/raw_api_data/backpack/private",
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
        get_app_settings, _, setup_logging = lazy_import_config()
        app_settings = get_app_settings()
        setup_logging(app_settings)
        logger.info("Configuration and logging initialized successfully")
    except Exception as e:
        logger.warning(f"Failed to initialize configuration: {e}. Using basic logging.")

    async with aiohttp.ClientSession() as session:
        try:
            collector = BackpackPrivateDataCollector(output_dir, session)

            # Determine symbols to collect
            if args.symbols:
                symbols = [s.strip() for s in args.symbols.split(",")]
            else:
                symbols = collector.get_default_symbols()

            logger.info(f"Collecting private data for symbols: {symbols}")
            logger.info(f"Output directory: {output_dir}")

            await collector.collect_all_private_data(symbols)

        except Exception as e:
            logger.error(f"Failed to initialize collector or collect data: {e}")
            raise


if __name__ == "__main__":
    asyncio.run(main())
