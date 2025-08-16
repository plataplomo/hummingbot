"""Fetch raw JSON data from Backpack private API endpoints.

This script collects fresh JSON responses from Backpack's private API endpoints
that require authentication and saves them as fixture files for testing purposes.
It uses the CyberDeltaEngine configuration system and Backpack ED25519 authenticator.

Based on the OpenAPI specification (openapi_backpack.json), this script comprehensively
covers ALL private endpoints that require authentication.

Usage:
    From project root directory:
    python scripts/data_collection/fetch_backpack_private_data.py \
        --output-dir tests/fixtures/raw_api_data/backpack/private
    python scripts/data_collection/fetch_backpack_private_data.py \
        --symbols SOL_USDC,BTC_USDC --output-dir fixtures/

    Note: This script must be run from the project root directory to ensure proper imports.
"""

import argparse
import asyncio
import json
import sys
from http import HTTPStatus
from pathlib import Path
from typing import Any

import aiohttp

from cyberdelta.apis.backpack.bp_auth import BackpackEd25519Authenticator
from cyberdelta.config import get_app_settings, get_secrets_config
from cyberdelta.config.logging_config import setup_logging
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import ExchangeName
from cyberdelta.exceptions.base import (
    InvalidAuthTypeError,
    RequiredParameterError,
    SecretsNotLoadedError,
)
from cyberdelta.exceptions.configuration import ConfigurationError


def _ensure_project_root() -> None:
    """Ensure script is run from project root directory."""
    current_dir = Path.cwd()
    expected_files = ["pyproject.toml", "cyberdelta", "scripts"]

    missing_files = [f for f in expected_files if not (current_dir / f).exists()]
    if missing_files:
        msg = (
            f"This script must be run from the project root directory. "
            f"Missing: {missing_files}. Current directory: {current_dir}"
        )
        sys.stderr.write(f"{msg}\n")
        sys.exit(1)


class BackpackPrivateDataCollector:
    """Collects raw JSON data from Backpack private API endpoints."""

    def __init__(self, output_dir: Path, session: aiohttp.ClientSession) -> None:
        """Initialize the Backpack private data collector with authentication setup.

        Args:
            output_dir: Directory where collected JSON data files will be saved
            session: Authenticated aiohttp session for making API requests

        Raises:
            ConfigurationError: If Backpack configuration is missing or disabled.
            SecretsNotLoadedError: If secrets configuration cannot be loaded.
            RequiredParameterError: If required Backpack secrets are missing.
            InvalidAuthTypeError: If authentication type is not 'api_key'.
            AttributeError: If required configuration attributes are missing.
            ImportError: If required modules cannot be imported.
            KeyError: If required configuration keys are missing.
            ValueError: If configuration values are invalid.
        """
        self.logger = get_logger(__name__)
        self.output_dir = output_dir
        self.session = session
        self.output_dir.mkdir(parents=True, exist_ok=True)

        authenticator_class = BackpackEd25519Authenticator

        # Initialize configuration and authenticator
        try:
            app_settings = get_app_settings()
            backpack_config = app_settings.exchanges.get("backpack")
            if not backpack_config:
                raise ConfigurationError("Backpack exchange configuration not found")
            if not backpack_config.enabled:
                raise ConfigurationError("Backpack exchange is disabled in configuration")

            self.api_base_url = str(backpack_config.active_api_base_url).rstrip("/")
            self.configured_symbols = backpack_config.symbols

            # Get authentication secrets
            secrets = get_secrets_config()
            if not secrets or not secrets.exchanges:
                raise SecretsNotLoadedError("secrets.json", False, True)

            backpack_secrets = secrets.exchanges.get("backpack")
            if not backpack_secrets:
                raise RequiredParameterError("backpack", "exchange secrets", None)

            if backpack_secrets.auth_type != "api_key":
                raise InvalidAuthTypeError(
                    ExchangeName.BACKPACK, "api_key", backpack_secrets.auth_type
                )

            # Initialize authenticator
            self.authenticator = authenticator_class(
                api_key_b64_secret=backpack_secrets.api_key,
                private_key_b64_secret=backpack_secrets.api_secret,
            )

            self.logger.info(
                "api_base_url_configured: Using Backpack API base URL",
                api_base_url=self.api_base_url,
            )
            self.logger.info(
                "symbols_configured: Configured symbols", symbols=self.configured_symbols
            )
            self.logger.info("authenticator_initialized: Backpack authenticator initialized")

        except (ValueError, ImportError, AttributeError, KeyError) as e:
            self.logger.exception(
                "config_init_failed: Failed to load config or init authenticator", error=str(e)
            )
            raise

    async def _fetch_authenticated_json(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Fetch JSON data from an authenticated endpoint with error handling.

        Returns:
            dict[str, Any] | None: JSON response data or None if error occurs.
        """
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

            self.logger.info(
                "fetching_endpoint: Fetching endpoint", method=method, url=url, params=params
            )

            # Make the request with authentication headers
            # For POST/PUT methods, always send JSON data (even if empty) for proper content-type
            json_data = (
                auth_components.data
                if method.upper() in {"POST", "PUT"}
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
                if response.status == HTTPStatus.OK.value:
                    response_data: dict[str, Any] = await response.json()
                    self.logger.info("fetch_successful: Successfully fetched data", url=url)
                    return response_data
                error_text = await response.text()
                self.logger.error(
                    "http_error: HTTP error occurred",
                    status=response.status,
                    url=url,
                    error=error_text,
                )
                return None
        except (OSError, ConnectionError, TimeoutError, ValueError) as e:
            self.logger.exception(
                "fetch_error: Error fetching endpoint", method=method, path=path, error=str(e)
            )
            return None

    def _save_json(self, data: dict[str, Any], filename: str) -> None:
        """Save JSON data to a file."""
        filepath = self.output_dir / filename
        try:
            with filepath.open("w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            self.logger.info("fixture_saved: Saved fixture", filepath=str(filepath))
        except (OSError, UnicodeEncodeError, PermissionError) as e:
            self.logger.exception(
                "save_error: Error saving file", filepath=str(filepath), error=str(e)
            )

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
        asset = symbol.split("_", maxsplit=1)[0] if "_" in symbol else symbol
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
        asset = symbol.split("_", maxsplit=1)[0] if "_" in symbol else symbol
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
            self.logger.warning(
                "position_filter_unsupported: Position endpoint doesn't support symbol filtering",
                requested_symbol=symbol,
                action="fetching_all_positions",
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
        """Get default symbols from configuration or fallback.

        Returns:
            list[str]: List of default trading symbols.
        """
        return (
            list(self.configured_symbols.values())
            if self.configured_symbols
            else ["SOL_USDC", "BTC_USDC", "ETH_USDC"]
        )

    def get_sample_order_ids(self) -> list[str]:
        """Get sample order IDs for testing (these would be real order IDs in practice).

        Returns:
            list[str]: List of sample order IDs.
        """
        return ["123456789", "987654321", "555666777"]

    def get_blockchains(self) -> list[str]:
        """Get supported blockchains for deposit addresses.

        Returns:
            list[str]: List of supported blockchain names.
        """
        # Only include blockchains that are known to work with Backpack
        # Removed: "Polygon" (not supported), "Bitcoin" (treasury service issues)
        return ["Solana", "Ethereum"]

    def get_borrow_lend_intervals(self) -> list[str]:
        """Get valid intervals for borrow/lend history.

        Returns:
            list[str]: List of valid interval strings.
        """
        return ["1d", "1w", "1month", "1year"]

    def get_dust_conversion_assets(self) -> list[str]:
        """Get common assets that might have dust to convert.

        Returns:
            list[str]: List of asset symbols that might have dust.
        """
        # Test dust conversion for common assets that users might hold
        return ["SOL", "BTC", "ETH"]

    async def _collect_account_management_data(self) -> None:
        """Collect account management data."""
        self.logger.info("Fetching account management data...")
        await self.fetch_account_info()

        # Test dust conversion for multiple assets
        dust_assets = self.get_dust_conversion_assets()
        for asset in dust_assets:
            await self.fetch_convert_dust(asset)
            await asyncio.sleep(0.1)  # Small delay between requests

    async def _collect_capital_and_balance_data(self) -> None:
        """Collect capital and balance data."""
        self.logger.info("Fetching capital and balance data...")
        await self.fetch_capital_balances()
        await self.fetch_capital_collateral()
        await self.fetch_collateral_info()

    async def _collect_account_limits_data(self, symbols: list[str]) -> None:
        """Collect account limits data for each symbol."""
        self.logger.info("Fetching account limits data...")
        for symbol in symbols:
            await self.fetch_max_borrow_quantity(symbol)
            await self.fetch_max_order_quantity(symbol, "buy", "100.0")  # lowercase
            await self.fetch_max_order_quantity(symbol, "sell", "100.0")  # lowercase
            await self.fetch_max_withdrawal_quantity(symbol)
            await asyncio.sleep(0.2)

    async def _collect_current_positions_data(self, symbols: list[str]) -> None:
        """Collect current trading positions data."""
        self.logger.info("Fetching current trading positions...")
        await self.fetch_position_summary()
        for symbol in symbols:
            await self.fetch_position_summary(symbol)
            await asyncio.sleep(0.1)
        await self.fetch_borrow_lend_positions()

    async def _collect_current_orders_data(self, symbols: list[str]) -> None:
        """Collect current open orders data."""
        self.logger.info("Fetching current open orders...")
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
        self.logger.info("Fetching deposit and withdrawal data...")
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
        self.logger.info("Fetching historical trading data...")

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
        self.logger.info("Fetching borrow/lend data...")
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
        self.logger.info("Fetching RFQ data...")
        await self.fetch_account_rfqs()
        await self.fetch_all_open_rfqs()
        await self.fetch_account_quotes()

    async def collect_all_private_data(self, symbols: list[str]) -> None:
        """Collect data from all private endpoints."""
        self.logger.info("Starting comprehensive Backpack private data collection...")

        await self._collect_account_management_data()
        await self._collect_capital_and_balance_data()
        await self._collect_account_limits_data(symbols)
        await self._collect_current_positions_data(symbols)
        await self._collect_current_orders_data(symbols)
        await self._collect_deposits_withdrawals_data()
        await self._collect_historical_trading_data(symbols)
        await self._collect_borrow_lend_data(symbols)
        await self._collect_rfq_data()

        self.logger.info("Comprehensive Backpack private data collection completed!")


async def main() -> None:
    """Execute comprehensive Backpack private API data collection with authentication.

    Parses command-line arguments, sets up configuration and authentication,
    and runs the complete private data collection process across all authenticated
    Backpack API endpoints. Saves collected data as JSON fixtures for testing.

    Raises:
        AttributeError: If required attributes are missing.
        ConnectionError: If connection to API fails.
        ImportError: If required modules cannot be imported.
        KeyError: If required configuration keys are missing.
        OSError: If file operations fail.
        RuntimeError: If runtime errors occur.
        ValueError: If invalid values are encountered.
    """
    # Ensure we're running from project root
    _ensure_project_root()

    logger = get_logger(__name__)
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
        # Direct access to imports now available at top level
        app_settings = get_app_settings()
        setup_logging(app_settings)
        logger.info("Configuration and logging initialized successfully")
    except (ImportError, AttributeError, ValueError, KeyError, OSError) as e:
        logger.warning(
            "config_init_warning: Failed to init config. Using basic logging.", error=str(e)
        )

    async with aiohttp.ClientSession() as session:
        try:
            collector = BackpackPrivateDataCollector(output_dir, session)

            # Determine symbols to collect
            if args.symbols:
                symbols = [s.strip() for s in args.symbols.split(",")]
            else:
                symbols = collector.get_default_symbols()

            logger.info(
                "data_collection_started: Collecting private data for symbols", symbols=symbols
            )
            logger.info(
                "output_directory_set: Output directory configured", output_dir=str(output_dir)
            )

            await collector.collect_all_private_data(symbols)

        except (
            ValueError,
            ImportError,
            AttributeError,
            KeyError,
            OSError,
            ConnectionError,
            RuntimeError,
        ) as e:
            logger.exception("Failed to initialize collector or collect data", error=str(e))
            raise


if __name__ == "__main__":
    asyncio.run(main())
