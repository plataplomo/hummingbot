"""Fetch raw JSON data from Hyperliquid public REST API endpoints.

This script collects fresh JSON responses from Hyperliquid's public API endpoints
and saves them as fixture files for testing purposes. It uses the CyberDeltaEngine
configuration system to get exchange settings.

Based on the OpenAPI specification, Hyperliquid SDK, official documentation, and web research,
this script comprehensively covers ALL public info endpoints that don't require authentication.

Data Sources:
1. Reverse-engineered OpenAPI spec (openapi_hl.json)
2. Official Hyperliquid Python SDK (hyperliquid-python-sdk)
3. Official documentation (hyperliquid.gitbook.io)
4. CCXT library documentation

Usage:
    python fetch_hyperliquid_public_data.py --output-dir \
        tests/fixtures/raw_api_data/hyperliquid/public
    python fetch_hyperliquid_public_data.py --coins ETH,BTC --output-dir fixtures/
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


class HyperliquidRateLimiter:
    """Rate limiter for Hyperliquid API based on official documentation.

    Rate limits per IP address:
    - REST requests share an aggregated weight limit of 1200 per minute
    - Info requests have different weights:
      - Weight 2: l2Book, allMids, clearinghouseState, orderStatus,
        spotClearinghouseState, exchangeStatus
      - Weight 60: userRole
      - Weight 20: All other info requests
      - Weight 40: Explorer API requests
    """

    def __init__(self) -> None:
        """Initialize the rate limiter with Hyperliquid's IP weight limits.

        Sets up tracking for a 1200 weight per minute limit with cleanup of old requests.
        """
        self.total_weight_limit = 1200  # per minute
        self.window_seconds = 60
        self.requests: list[tuple[float, int]] = []  # (timestamp, weight) pairs

    def _cleanup_old_requests(self) -> None:
        """Remove requests older than the time window."""
        current_time = time.time()
        cutoff_time = current_time - self.window_seconds
        self.requests = [(ts, weight) for ts, weight in self.requests if ts > cutoff_time]

    def _get_current_weight(self) -> int:
        """Get current total weight in the time window."""
        self._cleanup_old_requests()
        return sum(weight for _, weight in self.requests)

    def _get_request_weight(self, request_type: str) -> int:
        """Get the weight for a specific request type."""
        # Weight 2 requests
        weight_2_requests = {
            "l2Book",
            "allMids",
            "clearinghouseState",
            "orderStatus",
            "spotClearinghouseState",
            "exchangeStatus",
        }

        # Weight 60 requests
        weight_60_requests = {"userRole"}

        if request_type in weight_2_requests:
            return 2
        if request_type in weight_60_requests:
            return 60
        # All other info requests have weight 20
        return 20

    async def acquire(self, request_type: str) -> None:
        """Acquire permission to make a request, waiting if necessary.

        Args:
            request_type: The type of request (e.g., 'meta', 'l2Book', etc.)

        """
        weight = self._get_request_weight(request_type)

        while True:
            current_weight = self._get_current_weight()
            if current_weight + weight <= self.total_weight_limit:
                # Record the request
                self.requests.append((time.time(), weight))
                logger.debug(
                    "rate_limiter_acquired: Acquired weight for request",
                    weight=weight,
                    request_type=request_type,
                    current_total=current_weight + weight,
                    total_limit=self.total_weight_limit,
                )
                break
            # Calculate wait time based on oldest request that will expire
            if self.requests:
                oldest_time = min(ts for ts, _ in self.requests)
                wait_time = max(0.1, oldest_time + self.window_seconds - time.time())
            else:
                wait_time = 1.0

            logger.info(
                "rate_limit_reached: Waiting for rate limit window",
                wait_time=round(wait_time, 1),
                request_type=request_type,
            )
            await asyncio.sleep(wait_time)


class HyperliquidDataCollector:
    """Collects raw JSON data from Hyperliquid public API endpoints."""

    def __init__(self, output_dir: Path, session: aiohttp.ClientSession) -> None:
        """Initialize the Hyperliquid public data collector with rate limiting.

        Args:
            output_dir: Directory where collected JSON data files will be saved
            session: aiohttp session for making public API requests
        """
        self.output_dir = output_dir
        self.session = session
        self.rate_limiter = HyperliquidRateLimiter()
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Get configuration
        try:
            app_settings = get_app_settings()
            hyperliquid_config = app_settings.exchanges.get("hyperliquid")
            if not hyperliquid_config:
                raise ValueError("Hyperliquid exchange configuration not found")
            if not hyperliquid_config.enabled:
                raise ValueError("Hyperliquid exchange is disabled in configuration")

            self.api_base_url = str(hyperliquid_config.api_base_url_mainnet).rstrip("/")
            self.configured_symbols = hyperliquid_config.symbols
            logger.info(
                "hyperliquid_api_configured: Using API configuration",
                api_base_url=self.api_base_url,
            )
            logger.info(
                "configured_symbols: Loaded symbols from configuration",
                symbols=self.configured_symbols,
            )

        except Exception as e:
            logger.error(
                "configuration_load_failed: Failed to load configuration",
                error=str(e),
            )
            # Fallback to hardcoded values for data collection
            self.api_base_url = "https://api.hyperliquid.xyz"
            self.configured_symbols = {"BTC": "BTC", "ETH": "ETH"}
            logger.warning(
                "using_fallback_config: Using fallback configuration",
                api_base_url=self.api_base_url,
            )

    async def _fetch_json(
        self,
        url: str,
        payload: dict[str, Any],
        request_type: str,
    ) -> dict[str, Any] | None:
        """Fetch JSON data from a URL with POST payload, rate limiting, and error handling."""
        # Apply rate limiting
        await self.rate_limiter.acquire(request_type)

        try:
            logger.info(
                "fetching_data: Making API request",
                url=url,
                payload=payload,
            )
            async with self.session.post(url, json=payload) as response:
                if response.status == HTTPStatus.OK.value:
                    data: dict[str, Any] = await response.json()
                    logger.info(
                        "fetch_success: Successfully fetched data",
                        url=url,
                    )
                    return data
                logger.error(
                    "http_error: HTTP error response",
                    status=response.status,
                    url=url,
                    response_text=await response.text(),
                )
                return None
        except Exception as e:
            logger.error(
                "fetch_error: Error fetching data",
                url=url,
                error=str(e),
            )
            return None

    def _save_json(self, data: dict[str, Any], filename: str) -> None:
        """Save JSON data to a file."""
        filepath = self.output_dir / filename
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info(
                "fixture_saved: Saved JSON fixture",
                filepath=str(filepath),
            )
        except Exception as e:
            logger.error(
                "save_error: Error saving file",
                filepath=str(filepath),
                error=str(e),
            )

    # Basic info endpoints (no authentication required)
    async def fetch_meta(self) -> None:
        """Fetch asset metadata (universe)."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "meta"}
        data = await self._fetch_json(url, payload, "meta")
        if data:
            self._save_json(data, "hl_info_meta.json")

    async def fetch_meta_and_asset_ctxs(self) -> None:
        """Fetch meta and asset contexts."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "metaAndAssetCtxs"}
        data = await self._fetch_json(url, payload, "metaAndAssetCtxs")
        if data:
            self._save_json(data, "hl_info_meta_asset_ctxs.json")

    async def fetch_all_mids(self) -> None:
        """Fetch all mid prices."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "allMids"}
        data = await self._fetch_json(url, payload, "allMids")
        if data:
            self._save_json(data, "hl_info_allmids.json")

    # Market data endpoints (coin-specific)
    async def fetch_l2_book(self, coin: str) -> None:
        """Fetch L2 order book for a coin."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "l2Book", "coin": coin}
        data = await self._fetch_json(url, payload, "l2Book")
        if data:
            filename = f"hl_info_l2book_{coin.lower()}.json"
            self._save_json(data, filename)

    async def fetch_recent_trades(self, coin: str) -> None:
        """Fetch recent public trades for a coin."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "recentTrades", "coin": coin}
        data = await self._fetch_json(url, payload, "recentTrades")
        if data:
            filename = f"hl_info_recenttrades_{coin.lower()}.json"
            self._save_json(data, filename)

    async def fetch_candle_snapshot(
        self,
        coin: str,
        interval: str = "1m",
        hours_back: int = 1,
    ) -> None:
        """Fetch candle snapshot for a coin."""
        url = f"{self.api_base_url}/info"

        # Calculate start and end times in milliseconds
        end_time_ms = int(time.time() * 1000)
        start_time_ms = end_time_ms - (hours_back * 3600 * 1000)

        payload = {
            "type": "candleSnapshot",
            "coin": coin,
            "interval": interval,
            "startTime": start_time_ms,
            "endTime": end_time_ms,
        }

        data = await self._fetch_json(url, payload, "candleSnapshot")
        if data:
            filename = f"hl_info_candlesnapshot_{coin.lower()}_{interval}.json"
            self._save_json(data, filename)

    # User state endpoints (require user address but are publicly accessible)
    async def fetch_user_open_orders(self, user: str) -> None:
        """Fetch open orders for a user (public endpoint)."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "openOrders", "user": user}
        data = await self._fetch_json(url, payload, "openOrders")
        if data:
            filename = f"hl_info_openorders_{user[:8]}.json"  # Use first 8 chars of address
            self._save_json(data, filename)

    async def fetch_user_state(self, user: str) -> None:
        """Fetch user state (clearinghouse state) for a user."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "clearinghouseState", "user": user}
        data = await self._fetch_json(url, payload, "clearinghouseState")
        if data:
            filename = f"hl_info_clearinghousestate_{user[:8]}.json"
            self._save_json(data, filename)

    async def fetch_user_fills(self, user: str) -> None:
        """Fetch user fills/trades for a user."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "userFills", "user": user}
        data = await self._fetch_json(url, payload, "userFills")
        if data:
            filename = f"hl_info_userfills_{user[:8]}.json"
            self._save_json(data, filename)

    # Additional info types based on SDK and documentation
    async def fetch_funding_history(
        self,
        coin: str,
        start_time: int | None = None,
        end_time: int | None = None,
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

        data = await self._fetch_json(url, payload, "fundingHistory")
        if data:
            filename = f"hl_info_fundinghistory_{coin.lower()}.json"
            self._save_json(data, filename)

    async def fetch_historical_orders(self, user: str) -> None:
        """Fetch historical orders for a user."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "historicalOrders", "user": user}
        data = await self._fetch_json(url, payload, "historicalOrders")
        if data:
            filename = f"hl_info_historicalorders_{user[:8]}.json"
            self._save_json(data, filename)

    async def fetch_token_details(self) -> None:
        """Fetch token details."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "tokenDetails"}
        data = await self._fetch_json(url, payload, "tokenDetails")
        if data:
            self._save_json(data, "hl_info_tokendetails.json")

    async def fetch_spot_meta(self) -> None:
        """Fetch spot market metadata."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "spotMeta"}
        data = await self._fetch_json(url, payload, "spotMeta")
        if data:
            self._save_json(data, "hl_info_spotmeta.json")

    async def fetch_spot_clearinghouse_state(self, user: str) -> None:
        """Fetch spot clearinghouse state for a user."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "spotClearinghouseState", "user": user}
        data = await self._fetch_json(url, payload, "spotClearinghouseState")
        if data:
            filename = f"hl_info_spotclearinghousestate_{user[:8]}.json"
            self._save_json(data, filename)

    async def fetch_vault_details(self, vault_address: str) -> None:
        """Fetch vault details for a vault address."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "vaultDetails", "vaultAddress": vault_address}
        data = await self._fetch_json(url, payload, "vaultDetails")
        if data:
            filename = f"hl_info_vaultdetails_{vault_address[:8]}.json"
            self._save_json(data, filename)

    async def fetch_user_vault_equity(self, user: str) -> None:
        """Fetch user vault equity."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "userVaultEquity", "user": user}
        data = await self._fetch_json(url, payload, "userVaultEquity")
        if data:
            filename = f"hl_info_uservaultequity_{user[:8]}.json"
            self._save_json(data, filename)

    async def fetch_referral_state(self, user: str) -> None:
        """Fetch referral state for a user."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "referralState", "user": user}
        data = await self._fetch_json(url, payload, "referralState")
        if data:
            filename = f"hl_info_referralstate_{user[:8]}.json"
            self._save_json(data, filename)

    async def fetch_staking_info(self) -> None:
        """Fetch staking information."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "stakingInfo"}
        data = await self._fetch_json(url, payload, "stakingInfo")
        if data:
            self._save_json(data, "hl_info_stakinginfo.json")

    async def fetch_sub_accounts(self, user: str) -> None:
        """Fetch sub accounts for a user."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "subAccounts", "user": user}
        data = await self._fetch_json(url, payload, "subAccounts")
        if data:
            filename = f"hl_info_subaccounts_{user[:8]}.json"
            self._save_json(data, filename)

    async def fetch_frontend_orders(self, user: str) -> None:
        """Fetch frontend orders for a user."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "frontendOrders", "user": user}
        data = await self._fetch_json(url, payload, "frontendOrders")
        if data:
            filename = f"hl_info_frontendorders_{user[:8]}.json"
            self._save_json(data, filename)

    async def fetch_builder_fee(self, user: str) -> None:
        """Fetch builder fee for a user."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "builderFee", "user": user}
        data = await self._fetch_json(url, payload, "builderFee")
        if data:
            filename = f"hl_info_builderfee_{user[:8]}.json"
            self._save_json(data, filename)

    # Additional endpoints discovered from Hyperliquid SDK and documentation
    async def fetch_user_fills_by_time(
        self,
        user: str,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> None:
        """Fetch user fills/trades for a user within a specific time range."""
        url = f"{self.api_base_url}/info"

        # Default to last 24 hours if no times provided
        if end_time is None:
            end_time = int(time.time() * 1000)
        if start_time is None:
            start_time = end_time - (24 * 3600 * 1000)  # 24 hours ago

        payload = {
            "type": "userFillsByTime",
            "user": user,
            "startTime": start_time,
            "endTime": end_time,
        }
        data = await self._fetch_json(url, payload, "userFillsByTime")
        if data:
            filename = f"hl_info_userfillsbytime_{user[:8]}.json"
            self._save_json(data, filename)

    async def fetch_user_funding_history(
        self,
        user: str,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> None:
        """Fetch user funding history."""
        url = f"{self.api_base_url}/info"

        # Default to last 24 hours if no times provided
        if end_time is None:
            end_time = int(time.time() * 1000)
        if start_time is None:
            start_time = end_time - (24 * 3600 * 1000)  # 24 hours ago

        payload = {
            "type": "userFunding",
            "user": user,
            "startTime": start_time,
            "endTime": end_time,
        }
        data = await self._fetch_json(url, payload, "userFunding")
        if data:
            filename = f"hl_info_userfunding_{user[:8]}.json"
            self._save_json(data, filename)

    async def fetch_user_fees(self, user: str) -> None:
        """Fetch user fees information."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "userFees", "user": user}
        data = await self._fetch_json(url, payload, "userFees")
        if data:
            filename = f"hl_info_userfees_{user[:8]}.json"
            self._save_json(data, filename)

    async def fetch_user_staking_summary(self, user: str) -> None:
        """Fetch user staking summary."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "delegatorSummary", "user": user}
        data = await self._fetch_json(url, payload, "delegatorSummary")
        if data:
            filename = f"hl_info_delegatorsummary_{user[:8]}.json"
            self._save_json(data, filename)

    async def fetch_user_staking_delegations(self, user: str) -> None:
        """Fetch user staking delegations."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "delegations", "user": user}
        data = await self._fetch_json(url, payload, "delegations")
        if data:
            filename = f"hl_info_delegations_{user[:8]}.json"
            self._save_json(data, filename)

    async def fetch_user_staking_rewards(self, user: str) -> None:
        """Fetch user staking rewards."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "delegatorRewards", "user": user}
        data = await self._fetch_json(url, payload, "delegatorRewards")
        if data:
            filename = f"hl_info_delegatorrewards_{user[:8]}.json"
            self._save_json(data, filename)

    async def fetch_order_status_by_oid(self, user: str, oid: int) -> None:
        """Fetch order status by order ID."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "orderStatus", "user": user, "oid": oid}
        data = await self._fetch_json(url, payload, "orderStatus")
        if data:
            filename = f"hl_info_orderstatus_{user[:8]}_oid{oid}.json"
            self._save_json(data, filename)

    async def fetch_order_status_by_cloid(self, user: str, cloid: str) -> None:
        """Fetch order status by client order ID."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "orderStatus", "user": user, "cloid": cloid}
        data = await self._fetch_json(url, payload, "orderStatus")
        if data:
            filename = f"hl_info_orderstatus_{user[:8]}_cloid{cloid[:8]}.json"
            self._save_json(data, filename)

    async def fetch_perp_dexs(self) -> None:
        """Fetch perpetual DEX information."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "perpDexs"}
        data = await self._fetch_json(url, payload, "perpDexs")
        if data:
            self._save_json(data, "hl_info_perpdexs.json")

    async def fetch_spot_meta_and_asset_ctxs(self) -> None:
        """Fetch spot metadata and asset contexts."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "spotMetaAndAssetCtxs"}
        data = await self._fetch_json(url, payload, "spotMetaAndAssetCtxs")
        if data:
            self._save_json(data, "hl_info_spotmetaandassetctxs.json")

    async def fetch_user_to_multi_sig_signers(self, multi_sig_user: str) -> None:
        """Fetch multi-sig signers for a user."""
        url = f"{self.api_base_url}/info"
        payload = {"type": "userToMultiSigSigners", "multiSigUser": multi_sig_user}
        data = await self._fetch_json(url, payload, "userToMultiSigSigners")
        if data:
            filename = f"hl_info_multisigsigners_{multi_sig_user[:8]}.json"
            self._save_json(data, filename)

    def get_default_coins(self) -> list[str]:
        """Get default coins from configuration or fallback."""
        # Use configured symbols, fallback to common ones
        if self.configured_symbols:
            return list(self.configured_symbols.values())
        return ["ETH", "BTC", "SOL", "DOGE", "MATIC"]

    def get_sample_users(self) -> list[str]:
        """Get sample user addresses for testing user-specific endpoints."""
        # These are example addresses - in practice, you'd want real addresses with activity
        return [
            "0xAe24B4BDAD4633f0961dc66A491473d6BC8E5BA0",  # Zero address
        ]

    def get_sample_vault_addresses(self) -> list[str]:
        """Get sample vault addresses for testing vault endpoints."""
        # These are example vault addresses
        return [
            "0x1234567890123456789012345678901234567890",
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
        ]

    def get_sample_order_ids(self) -> list[int]:
        """Get sample order IDs for testing order status endpoints."""
        return [123456789, 987654321, 555666777]

    def get_sample_client_order_ids(self) -> list[str]:
        """Get sample client order IDs for testing order status endpoints."""
        return ["0x123456789abcdef", "0xfedcba987654321", "0xabc123def456789"]

    async def collect_all_data(self, coins: list[str]) -> None:
        """Collect data from all public endpoints for the given coins."""
        logger.info("Starting comprehensive Hyperliquid public data collection...")

        # System-wide endpoints (no coin or user required)
        logger.info("Fetching system-wide endpoints...")
        await self.fetch_meta()
        await self.fetch_meta_and_asset_ctxs()
        await self.fetch_all_mids()
        await self.fetch_token_details()
        await self.fetch_spot_meta()
        await self.fetch_spot_meta_and_asset_ctxs()
        await self.fetch_staking_info()
        await self.fetch_perp_dexs()

        # Coin-specific endpoints
        logger.info("Fetching coin-specific market data...")
        for coin in coins:
            logger.info(
                "collecting_coin_data: Collecting market data for coin",
                coin=coin,
            )

            # Basic market data
            await self.fetch_l2_book(coin)
            await self.fetch_recent_trades(coin)
            await self.fetch_funding_history(coin)

            # Candle data with different intervals
            intervals = ["1m", "5m", "15m", "1h", "4h", "1d"]
            hours_back_options = [1, 6]  # 1h and 6h for variety

            for interval in intervals[:3]:  # Limit to avoid too many requests
                for hours_back in hours_back_options:
                    await self.fetch_candle_snapshot(coin, interval, hours_back)

        # User-specific endpoints (using sample addresses)
        logger.info("Fetching user-specific data with sample addresses...")
        sample_users = self.get_sample_users()
        for user in sample_users:
            logger.info(
                "collecting_user_data: Collecting user data",
                user_prefix=user[:10],
            )

            # User state and trading data
            await self.fetch_user_open_orders(user)
            await self.fetch_user_state(user)
            await self.fetch_user_fills(user)
            await self.fetch_user_fills_by_time(user)  # Last 24h
            await self.fetch_user_funding_history(user)
            await self.fetch_historical_orders(user)
            await self.fetch_spot_clearinghouse_state(user)
            await self.fetch_user_vault_equity(user)
            await self.fetch_referral_state(user)
            await self.fetch_sub_accounts(user)
            await self.fetch_frontend_orders(user)
            await self.fetch_builder_fee(user)
            await self.fetch_user_fees(user)
            await self.fetch_user_staking_summary(user)
            await self.fetch_user_staking_delegations(user)
            await self.fetch_user_staking_rewards(user)
            await self.fetch_user_to_multi_sig_signers(user)

            # Order status endpoints with sample IDs
            sample_order_ids = self.get_sample_order_ids()
            for oid in sample_order_ids[:2]:  # Just test a couple
                await self.fetch_order_status_by_oid(user, oid)

            sample_client_order_ids = self.get_sample_client_order_ids()
            for cloid in sample_client_order_ids[:2]:  # Just test a couple
                await self.fetch_order_status_by_cloid(user, cloid)

        # Vault-specific endpoints (using sample vault addresses)
        logger.info("Fetching vault data with sample addresses...")
        sample_vaults = self.get_sample_vault_addresses()
        for vault in sample_vaults:
            logger.info(
                "collecting_vault_data: Collecting vault data",
                vault_prefix=vault[:10],
            )
            await self.fetch_vault_details(vault)

        logger.info("Comprehensive Hyperliquid public data collection completed!")


async def main() -> None:
    """Execute comprehensive Hyperliquid public API data collection process.

    Parses command-line arguments, sets up configuration and rate-limited HTTP session,
    and runs the complete public data collection across all Hyperliquid info endpoints.
    Respects API rate limits and saves collected market data as JSON fixtures for testing.
    """
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
        default="tests/fixtures/raw_api_data/hyperliquid/public",
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
            "config_init_failed: Failed to initialize configuration, using basic logging",
            error=str(e),
        )

    async with aiohttp.ClientSession() as session:
        collector = HyperliquidDataCollector(output_dir, session)

        # Determine coins to collect
        if args.coins:
            coins = [s.strip() for s in args.coins.split(",")]
        else:
            coins = collector.get_default_coins()

        logger.info(
            "data_collection_starting: Starting data collection",
            coins=coins,
            output_dir=str(output_dir),
        )

        await collector.collect_all_data(coins)


if __name__ == "__main__":
    asyncio.run(main())
