"""Market Order Service.

This service calculates aggressive prices for market order execution and
manages the business logic for converting market orders into IoC limit orders.
"""

from collections.abc import Awaitable, Callable
from decimal import ROUND_DOWN, Decimal
from typing import cast

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.apis.models.service_args_models import GetMarketArgs
from cyberdelta.config.logging_config import get_logger
from cyberdelta.core.execution.orders.market_order_config import MarketOrderConfig
from cyberdelta.core.execution.orders.market_order_errors import (
    InsufficientLiquidityError,
    MarketOrderError,
    PriceDeviationError,
)
from cyberdelta.core.models import OrderBook, OrderSide
from cyberdelta.core.models.market.mid_prices import MidPrices
from cyberdelta.core.signal_generator import SignalGenerator

logger = get_logger(__name__)


class MarketOrderService:
    """Service for calculating aggressive prices and managing market order execution logic."""

    def __init__(
        self,
        exchange_api: ExchangeAPI,
        signal_generator: SignalGenerator | None = None,
        config: MarketOrderConfig | None = None,
    ) -> None:
        """Initialize MarketOrderService.

        Args:
            exchange_api: Exchange API instance for market data access
            signal_generator: Optional signal generator for slippage estimation
            config: Market order configuration
        """
        self._exchange = exchange_api
        self._signal_generator = signal_generator
        self._config = config or MarketOrderConfig()

    async def calculate_aggressive_price(
        self,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        max_slippage: Decimal | None = None,
    ) -> Decimal:
        """Calculate aggressive price with comprehensive safety checks.

        Args:
            symbol: Trading symbol
            side: Order side (BUY/SELL)
            quantity: Order quantity
            max_slippage: Optional maximum slippage override

        Returns:
            Decimal: Calculated aggressive price for IoC limit order

        Raises:
            MarketOrderError: If market data is unavailable
            InsufficientLiquidityError: If insufficient liquidity
            PriceDeviationError: If price deviation exceeds limits
        """
        # 1. Get order book for accurate pricing
        order_book = await self._exchange.get_order_book(symbol)
        if not order_book or not order_book.bids or not order_book.asks:
            raise MarketOrderError(
                f"Cannot calculate market order price: no order book for {symbol}"
            )

        # 2. Check liquidity sufficiency
        available_liquidity = self._calculate_available_liquidity(order_book, side, quantity)
        required_liquidity = quantity * self._config.min_liquidity_ratio
        if available_liquidity < required_liquidity:
            raise InsufficientLiquidityError(
                symbol=symbol,
                requested_quantity=quantity,
                available_quantity=available_liquidity,
            )

        # 3. Get reference price (best bid/ask)
        if side == OrderSide.BUY:
            reference_price = order_book.asks[0][0]  # Best ask
        else:
            reference_price = order_book.bids[0][0]  # Best bid

        # 4. Calculate slippage
        estimated_slippage = self._estimate_slippage(symbol, quantity)

        # 5. Apply configured slippage limits
        max_allowed_slippage = self._config.get_slippage_for_symbol(symbol)
        if max_slippage:
            max_allowed_slippage = min(max_allowed_slippage, max_slippage)

        final_slippage = min(estimated_slippage, max_allowed_slippage)

        # 6. Calculate aggressive price
        if side == OrderSide.BUY:
            aggressive_price = reference_price * (Decimal("1") + final_slippage)
        else:
            aggressive_price = reference_price * (Decimal("1") - final_slippage)

        # 7. Apply safety validation
        self._validate_price_bounds(aggressive_price, symbol, reference_price)

        # 8. Round to tick size if available
        return await self.round_to_tick_size(aggressive_price, symbol)

    def _calculate_available_liquidity(
        self, order_book: OrderBook, side: OrderSide, quantity: Decimal
    ) -> Decimal:
        """Calculate available liquidity for the order.

        Args:
            order_book: Current order book
            side: Order side (BUY/SELL)
            quantity: Requested quantity

        Returns:
            Decimal: Total available liquidity across all price levels
        """
        available = Decimal("0")
        levels = order_book.asks if side == OrderSide.BUY else order_book.bids

        for _price, size in levels:
            available += size
            if available >= quantity:
                return available

        return available

    def _estimate_slippage(self, symbol: str, quantity: Decimal) -> Decimal:
        """Estimate slippage for the given symbol and quantity.

        Args:
            symbol: Trading symbol
            quantity: Order quantity

        Returns:
            Decimal: Estimated slippage percentage
        """
        # Use signal generator if available
        if self._signal_generator:
            try:
                return self._signal_generator.estimate_slippage(
                    exchange=self._exchange.exchange_name,
                    symbol=symbol,
                    size=quantity,
                )
            except Exception as e:
                logger.warning(
                    f"Failed to estimate slippage using SignalGenerator: {e}, "
                    f"falling back to config default"
                )

        # Fallback to configured default
        return self._config.get_slippage_for_symbol(symbol)

    def _validate_price_bounds(
        self, aggressive_price: Decimal, symbol: str, reference_price: Decimal
    ) -> None:
        """Validate price is within acceptable bounds.

        Args:
            aggressive_price: Calculated aggressive price
            symbol: Trading symbol
            reference_price: Reference price (best bid/ask)

        Raises:
            PriceDeviationError: If price deviation exceeds limits
            MarketOrderError: If price is invalid
        """
        # Maximum deviation from reference price
        max_deviation = self._config.max_price_deviation_pct

        deviation = abs(aggressive_price - reference_price) / reference_price
        if deviation > max_deviation:
            raise PriceDeviationError(
                symbol=symbol,
                aggressive_price=aggressive_price,
                reference_price=reference_price,
                deviation_pct=deviation,
                max_deviation_pct=max_deviation,
            )

        # Ensure price is positive and finite
        if not aggressive_price.is_finite() or aggressive_price <= Decimal("0"):
            raise MarketOrderError(f"Invalid aggressive price: {aggressive_price}")

    async def round_to_tick_size(self, price: Decimal, symbol: str) -> Decimal:
        """Round price to exchange tick size.

        Args:
            price: Price to round
            symbol: Trading symbol

        Returns:
            Decimal: Rounded price
        """
        try:
            # Get market metadata from exchange
            market = await self._exchange.get_market(GetMarketArgs(symbol=symbol))

            if market and market.tick_size:
                # Round to nearest tick_size multiple
                tick_size = market.tick_size
                rounded = (price / tick_size).quantize(
                    Decimal("1"), rounding=ROUND_DOWN
                ) * tick_size
                logger.debug(
                    f"Rounded price for {symbol} using tick_size {tick_size}: {price} -> {rounded}"
                )
                return rounded
            else:
                logger.warning(f"No tick size found for {symbol}, returning original price")
                return price

        except MarketOrderError:
            raise
        except Exception as e:
            logger.error(
                f"Failed to get market metadata for {symbol}: {e}, returning original price"
            )
            return price

    async def round_to_step_size(self, quantity: Decimal, symbol: str) -> Decimal:
        """Round quantity to exchange step size.

        Args:
            quantity: Quantity to round
            symbol: Trading symbol

        Returns:
            Decimal: Rounded quantity
        """
        try:
            # Get market metadata from exchange
            market = await self._exchange.get_market(GetMarketArgs(symbol=symbol))

            if market and market.step_size:
                # Round to nearest step_size multiple
                step_size = market.step_size
                rounded = (quantity / step_size).quantize(
                    Decimal("1"), rounding=ROUND_DOWN
                ) * step_size
                logger.debug(
                    f"Rounded quantity for {symbol} using step_size {step_size}: "
                    f"{quantity} -> {rounded}"
                )
                return rounded
            else:
                logger.warning(f"No step size found for {symbol}, returning original quantity")
                return quantity

        except MarketOrderError:
            raise
        except Exception as e:
            logger.error(
                f"Failed to get market metadata for {symbol}: {e}, returning original quantity"
            )
            return quantity

    async def get_reference_price_all_mids(self, symbol: str) -> Decimal | None:
        """Get reference price from AllMids endpoint if available.

        Args:
            symbol: Trading symbol

        Returns:
            Decimal | None: Mid price if available, None otherwise
        """
        if not self._config.use_all_mids_for_reference:
            return None

        try:
            # Check if exchange supports get_all_mids
            if hasattr(self._exchange, "get_all_mids"):
                get_all_mids_method = getattr(self._exchange, "get_all_mids", None)
                if get_all_mids_method and callable(get_all_mids_method):
                    # Type assertion for dynamic method
                    typed_method = cast(Callable[[], Awaitable[MidPrices]], get_all_mids_method)
                    all_mids = await typed_method()
                    return all_mids.get(symbol)
        except MarketOrderError:
            raise
        except Exception as e:
            logger.debug(f"Failed to fetch AllMids: {e}")

        return None

    def calculate_liquidity_ratio(
        self, order_book: OrderBook, side: OrderSide, quantity: Decimal
    ) -> Decimal:
        """Calculate the liquidity ratio for the order.

        Args:
            order_book: Current order book
            side: Order side
            quantity: Order quantity

        Returns:
            Decimal: Ratio of available liquidity to requested quantity
        """
        available = self._calculate_available_liquidity(order_book, side, quantity)
        return available / quantity if quantity > Decimal("0") else Decimal("0")

    def validate_config(self) -> None:
        """Validate market order configuration.

        Raises:
            ValueError: If configuration is invalid
        """
        if not self._config.enabled:
            raise ValueError("Market orders are disabled in configuration")

        if self._config.max_slippage_pct <= Decimal("0"):
            raise ValueError("Maximum slippage must be positive")

        if self._config.max_price_deviation_pct <= Decimal("0"):
            raise ValueError("Maximum price deviation must be positive")
