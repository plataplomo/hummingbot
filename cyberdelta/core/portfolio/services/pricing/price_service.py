"""Price data service with direct AppSettings access following risk module patterns."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any

from cyberdelta.config import AppSettings
from cyberdelta.config.structlog_config import get_logger


if TYPE_CHECKING:
    from cyberdelta.apis.base.exchange_api import ExchangeAPI
    from cyberdelta.core.portfolio.portfolio_types.service_protocols import CacheServiceProtocol


class PriceDataService:
    """Manages price data access and conversions with direct AppSettings access.

    Follows risk module patterns:
    - Direct AppSettings access
    - Protocol-based dependencies
    - No inheritance from base services
    - Configuration from AppSettings

    Handles caching and price validation to improve performance
    and address the individual price lookup bottlenecks identified
    in the original PortfolioTracker.
    """

    def __init__(
        self,
        app_settings: AppSettings,
        cache_service: CacheServiceProtocol[str, Decimal] | None = None,
        api_clients: dict[str, ExchangeAPI] | None = None,
    ) -> None:
        """Initialize the price data service.

        Args:
            app_settings: Application settings with portfolio configuration
            cache_service: Service for caching price data
            api_clients: Dictionary of exchange API clients
        """
        self.app_settings = app_settings
        self.portfolio_config = app_settings.portfolio_tracker
        self.cache_service = cache_service
        self.api_clients = api_clients or {}
        self.logger = get_logger(self.__class__.__name__)

        # Configuration from AppSettings
        self.default_cache_ttl = float(self.portfolio_config.calculation.price_cache_ttl)
        self.batch_size_limit = int(self.portfolio_config.calculation.batch_size_limit)
        self.price_staleness_threshold = float(
            self.portfolio_config.calculation.price_staleness_threshold
        )

        self.logger.info(
            "price_data_service_created",
            cache_ttl=self.default_cache_ttl,
            batch_size_limit=self.batch_size_limit,
            price_staleness_threshold=self.price_staleness_threshold,
        )

    async def get_current_price(self, symbol: str, exchange_id: str | None = None) -> Decimal:
        """Get current price for a symbol.

        Args:
            symbol: Trading symbol
            exchange_id: Optional specific exchange ID

        Returns:
            Current price as Decimal

        Raises:
            ValueError: If price cannot be retrieved
        """
        # Check cache first
        cache_key = self._get_price_cache_key(symbol, exchange_id)

        if self.cache_service:
            cached_price = await self.cache_service.get(cache_key)
            if cached_price is not None:
                self.logger.debug(
                    "price_cache_hit", symbol=symbol, exchange_id=exchange_id, price=cached_price
                )
                return Decimal(str(cached_price))

        # Fetch from exchange API
        try:
            price = await self._fetch_price_from_api(symbol, exchange_id)

            # Cache the result
            if self.cache_service:
                await self.cache_service.set(cache_key, price, ttl=self.default_cache_ttl)

            self.logger.debug("price_fetched", symbol=symbol, exchange_id=exchange_id, price=price)

        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
            self.logger.exception("price_fetch_failed", symbol=symbol, exchange_id=exchange_id)
            raise ValueError from e
        else:
            return price

    async def get_price_in_currency(
        self, symbol: str, target_currency: str, exchange_id: str | None = None
    ) -> Decimal:
        """Get price converted to target currency.

        Args:
            symbol: Trading symbol
            target_currency: Currency to convert to
            exchange_id: Optional specific exchange ID

        Returns:
            Price in target currency
        """
        # For now, implement basic conversion logic
        # In production, this would use real exchange rates
        base_price = await self.get_current_price(symbol, exchange_id)

        # Extract base currency from symbol (simplified logic)
        if "/" in symbol:
            quote_currency = symbol.split("/")[1]
        elif "-" in symbol:
            quote_currency = symbol.split("-")[1]
        else:
            quote_currency = "USD"  # Default assumption

        if quote_currency == target_currency:
            return base_price

        # Simplified currency conversion - in production this would use real exchange rates
        conversion_rate = self._get_conversion_rate(quote_currency, target_currency)

        self.logger.debug(
            "currency_conversion_applied",
            symbol=symbol,
            quote_currency=quote_currency,
            target_currency=target_currency,
            conversion_rate=conversion_rate,
        )

        return base_price * conversion_rate

    async def batch_get_prices(
        self, symbols: list[str], target_currency: str, exchange_id: str | None = None
    ) -> dict[str, Decimal]:
        """Get prices for multiple symbols in batch.

        This addresses the performance bottleneck of individual price lookups
        identified in the original PortfolioTracker.

        Args:
            symbols: List of trading symbols
            target_currency: Currency for all prices
            exchange_id: Optional specific exchange ID

        Returns:
            Dictionary mapping symbol to price
        """
        # Handle large batches
        if len(symbols) > self.batch_size_limit:
            self.logger.warning(
                "batch_size_limit_exceeded",
                requested_size=len(symbols),
                limit=self.batch_size_limit,
            )
            return await self._process_large_batch(symbols, target_currency, exchange_id)

        # Get prices from cache and API
        raw_prices = await self._get_batch_prices_raw(symbols, exchange_id)

        # Convert currencies
        converted_prices = await self._convert_batch_currencies(
            raw_prices, target_currency, exchange_id
        )

        self.logger.info(
            "batch_prices_retrieved",
            symbol_count=len(converted_prices),
            target_currency=target_currency,
        )

        return converted_prices

    async def _get_batch_prices_raw(
        self, symbols: list[str], exchange_id: str | None
    ) -> dict[str, Decimal]:
        """Get raw prices from cache and API.

        Args:
            symbols: List of trading symbols to fetch prices for
            exchange_id: Optional specific exchange ID

        Returns:
            dict[str, Decimal]: Dictionary mapping symbols to their raw prices
        """
        result: dict[str, Decimal] = {}
        cache_hits, cache_misses = await self._check_price_cache(symbols, exchange_id)

        # Add cache hits to result
        result.update(cache_hits)

        # Fetch missing prices from API
        if cache_misses:
            fetched_prices = await self._fetch_and_cache_prices(cache_misses, exchange_id)
            result.update(fetched_prices)

        return result

    async def _check_price_cache(
        self, symbols: list[str], exchange_id: str | None
    ) -> tuple[dict[str, Decimal], list[str]]:
        """Check cache for symbol prices.

        Args:
            symbols: List of trading symbols to check in cache
            exchange_id: Optional specific exchange ID

        Returns:
            tuple[dict[str, Decimal], list[str]]: Tuple of (cache_hits, cache_misses)
                where cache_hits maps symbols to cached prices and cache_misses
                contains symbols not found in cache
        """
        cache_hits: dict[str, Decimal] = {}
        cache_misses: list[str] = []

        if self.cache_service:
            for symbol in symbols:
                cache_key = self._get_price_cache_key(symbol, exchange_id)
                cached_price = await self.cache_service.get(cache_key)
                if cached_price is not None:
                    # Cast from object to Decimal (cache stores Decimal values)
                    cache_hits[symbol] = Decimal(str(cached_price))
                else:
                    cache_misses.append(symbol)
        else:
            cache_misses = symbols

        self.logger.debug(
            "batch_price_lookup",
            total_symbols=len(symbols),
            cache_hits=len(cache_hits),
            cache_misses=len(cache_misses),
        )

        return cache_hits, cache_misses

    async def _fetch_and_cache_prices(
        self, symbols: list[str], exchange_id: str | None
    ) -> dict[str, Decimal]:
        """Fetch prices from API and cache them.

        Args:
            symbols: List of trading symbols to fetch from API
            exchange_id: Optional specific exchange ID

        Returns:
            dict[str, Decimal]: Dictionary mapping symbols to fetched prices
        """
        result: dict[str, Decimal] = {}

        try:
            fetched_prices = await self._batch_fetch_from_api(symbols, exchange_id)

            # Cache the fetched prices
            if self.cache_service:
                for symbol, price in fetched_prices.items():
                    cache_key = self._get_price_cache_key(symbol, exchange_id)
                    await self.cache_service.set(cache_key, price, ttl=self.default_cache_ttl)

            result.update(fetched_prices)

        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
            self.logger.exception("batch_price_fetch_failed", symbols=symbols)
            # Fill missing symbols with fallback values
            for symbol in symbols:
                if symbol not in result:
                    self.logger.warning("price_unavailable", symbol=symbol)
                    result[symbol] = Decimal(0)

        return result

    async def _convert_batch_currencies(
        self, prices: dict[str, Decimal], target_currency: str, exchange_id: str | None
    ) -> dict[str, Decimal]:
        """Convert batch prices to target currency.

        Args:
            prices: Dictionary of symbols to prices to convert
            target_currency: Currency to convert all prices to
            exchange_id: Optional specific exchange ID

        Returns:
            dict[str, Decimal]: Dictionary mapping symbols to converted prices
        """
        converted_result: dict[str, Decimal] = {}

        for symbol, price in prices.items():
            try:
                converted_price = await self.get_price_in_currency(
                    symbol, target_currency, exchange_id
                )
                converted_result[symbol] = converted_price
            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
                self.logger.warning(
                    "price_conversion_failed", symbol=symbol, target_currency=target_currency
                )
                converted_result[symbol] = price  # Use unconverted price

        return converted_result

    async def _fetch_price_from_api(self, symbol: str, exchange_id: str | None = None) -> Decimal:
        """Fetch price from exchange API.

        Args:
            symbol: Trading symbol
            exchange_id: Optional specific exchange ID

        Returns:
            Price from API

        Raises:
            ValueError: If price cannot be fetched
        """
        if not self.api_clients:
            raise ValueError

        # If exchange_id specified, use that client
        if exchange_id:
            if exchange_id not in self.api_clients:
                raise ValueError

            # In a real implementation, this would call the exchange API
            # For now, return a price based on symbol to make it more realistic
            return self._get_mock_price_for_symbol(symbol)

        # Otherwise, try first available exchange
        next(iter(self.api_clients.keys()))
        # In a real implementation, this would call the exchange API
        # For now, return a price based on symbol to make it more realistic
        return self._get_mock_price_for_symbol(symbol)

    def _get_mock_price_for_symbol(self, symbol: str) -> Decimal:
        """Get a mock price for a symbol to make testing more realistic.

        Args:
            symbol: Trading symbol to get mock price for

        Returns:
            Decimal: Mock price for the symbol based on known currency mappings
        """
        symbol_upper = symbol.upper()

        # Mock prices for common symbols
        mock_prices = {
            "BTC": Decimal("50000.00"),
            "ETH": Decimal("3000.00"),
            "SOL": Decimal("100.00"),
            "USDC": Decimal("1.00"),
            "USDT": Decimal("1.00"),
            "USD": Decimal("1.00"),
        }

        # Check if symbol contains any of the known currencies
        for currency, price in mock_prices.items():
            if currency in symbol_upper:
                return price

        # Default fallback price
        return Decimal("1.00")

    def _get_conversion_rate(self, from_currency: str, to_currency: str) -> Decimal:
        """Get simplified conversion rate between currencies.

        Args:
            from_currency: Source currency code
            to_currency: Target currency code

        Returns:
            Decimal: Conversion rate to multiply source amount by
        """
        if from_currency == to_currency:
            return Decimal("1.00")

        # Simplified conversion rates (in production, use real exchange rates)
        usd_rates = {
            "EUR": Decimal("0.85"),
            "GBP": Decimal("0.75"),
            "JPY": Decimal("110.00"),
            "USDT": Decimal("1.00"),
            "USDC": Decimal("1.00"),
        }

        # Convert through USD as base
        from_rate = usd_rates.get(from_currency, Decimal("1.00"))
        to_rate = usd_rates.get(to_currency, Decimal("1.00"))

        if from_currency == "USD":
            return to_rate
        if to_currency == "USD":
            return Decimal("1.00") / from_rate
        # Convert via USD
        return to_rate / from_rate

    async def _batch_fetch_from_api(
        self, symbols: list[str], exchange_id: str | None = None
    ) -> dict[str, Decimal]:
        """Fetch multiple prices from exchange API in batch.

        Args:
            symbols: List of symbols to fetch
            exchange_id: Optional specific exchange ID

        Returns:
            Dictionary mapping symbol to price
        """
        # Placeholder implementation - in production this would
        # use the exchange's batch price API if available
        result: dict[str, Decimal] = {}
        for symbol in symbols:
            try:
                price = await self._fetch_price_from_api(symbol, exchange_id)
                result[symbol] = price
            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
                self.logger.warning("individual_price_fetch_failed", symbol=symbol)

        return result

    async def _process_large_batch(
        self, symbols: list[str], target_currency: str, exchange_id: str | None = None
    ) -> dict[str, Decimal]:
        """Process large batch by splitting into smaller chunks.

        Args:
            symbols: List of symbols
            target_currency: Target currency
            exchange_id: Optional exchange ID

        Returns:
            Dictionary mapping symbol to price
        """
        result: dict[str, Decimal] = {}

        # Split into chunks
        for i in range(0, len(symbols), self.batch_size_limit):
            chunk = symbols[i : i + self.batch_size_limit]
            chunk_result = await self.batch_get_prices(chunk, target_currency, exchange_id)
            result.update(chunk_result)

        return result

    def _get_price_cache_key(self, symbol: str, exchange_id: str | None = None) -> str:
        """Generate cache key for price data.

        Args:
            symbol: Trading symbol
            exchange_id: Optional exchange ID

        Returns:
            Cache key string
        """
        if exchange_id:
            return f"price:{exchange_id}:{symbol}"
        return f"price:any:{symbol}"

    async def invalidate_price_cache(self, symbol: str | None = None) -> None:
        """Invalidate price cache for symbol or all symbols.

        Args:
            symbol: Optional specific symbol to invalidate
        """
        if not self.cache_service:
            return

        if symbol:
            # Invalidate specific symbol across all exchanges
            for exchange_id in self.api_clients:
                cache_key = self._get_price_cache_key(symbol, exchange_id)
                await self.cache_service.delete(cache_key)

            # Also invalidate the 'any' exchange version
            cache_key = self._get_price_cache_key(symbol, None)
            await self.cache_service.delete(cache_key)
        else:
            # Clear entire cache
            await self.cache_service.clear()

        self.logger.info("price_cache_invalidated", symbol=symbol or "all")

    def get_service_stats(self) -> dict[str, Any]:
        """Get service statistics.

        Returns:
            Dictionary with service statistics
        """
        return {
            "service_name": self.__class__.__name__,
            "api_clients_count": len(self.api_clients),
            "cache_enabled": self.cache_service is not None,
            "default_cache_ttl": self.default_cache_ttl,
            "batch_size_limit": self.batch_size_limit,
            "price_staleness_threshold": self.price_staleness_threshold,
        }
