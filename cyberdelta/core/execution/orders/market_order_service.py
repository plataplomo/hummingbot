"""Market Order Service.

This service calculates aggressive prices for market order execution and
manages the business logic for converting market orders into IoC limit orders.
"""

from decimal import ROUND_DOWN, Decimal

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.apis.models.service_args.market_data import GetMarketArgs
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.execution.orders.market_order_config import MarketOrderConfig
from cyberdelta.core.execution.orders.market_order_errors import (
    InsufficientLiquidityError,
    MarketOrderError,
    MarketOrderParameterError,
    PriceDeviationError,
)
from cyberdelta.core.symbols import Symbol, exchanges
from cyberdelta.enums import ExchangeName, OrderSide
from cyberdelta.models import OrderBook


logger = get_logger(__name__)


class MarketOrderService:
    """Service for calculating aggressive prices and managing market order execution logic."""

    def __init__(
        self,
        exchange_api: ExchangeAPI,
        # No signal generator - slippage handled by config
        config: MarketOrderConfig | None = None,
    ) -> None:
        """Initialize MarketOrderService.

        Args:
            exchange_api: Exchange API instance for market data access
            # Slippage estimation handled by configuration
            config: Market order configuration
        """
        self._exchange = exchange_api
        # Slippage estimation handled by configuration
        self._config = config or MarketOrderConfig()

    async def calculate_aggressive_price(
        self,
        symbol: Symbol,
        side: OrderSide,
        quantity: Decimal,
        max_slippage: Decimal | None = None,
    ) -> Decimal:
        """Calculate aggressive price with comprehensive safety checks.

        Args:
            symbol: Trading Symbol object
            side: Order side (BUY/SELL)
            quantity: Order quantity
            max_slippage: Optional maximum slippage override

        Returns:
            Decimal: Calculated aggressive price for IoC limit order

        Raises:
            InsufficientLiquidityError: If insufficient liquidity
            MarketOrderError.no_order_book_error: If order book is unavailable
        """
        # 1. Get order book for accurate pricing
        order_book = await self._exchange.get_order_book(symbol)
        if not order_book or not order_book.bids or not order_book.asks:
            raise MarketOrderError.no_order_book_error(symbol)

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
        reference_price = order_book.asks[0][0] if side == OrderSide.BUY else order_book.bids[0][0]

        # 4. Calculate slippage
        estimated_slippage = self._estimate_slippage(symbol, quantity)

        # 5. Apply configured slippage limits
        max_allowed_slippage = self._config.get_slippage_for_symbol(symbol)
        if max_slippage:
            max_allowed_slippage = min(max_allowed_slippage, max_slippage)

        final_slippage = min(estimated_slippage, max_allowed_slippage)

        # 6. Calculate aggressive price
        if side == OrderSide.BUY:
            aggressive_price = reference_price * (Decimal(1) + final_slippage)
        else:
            aggressive_price = reference_price * (Decimal(1) - final_slippage)

        # 7. Apply safety validation
        self._validate_price_bounds(aggressive_price, symbol, reference_price)

        # 8. Round to tick size if available
        return await self.round_to_tick_size(aggressive_price, symbol)

    def _calculate_available_liquidity(
        self,
        order_book: OrderBook,
        side: OrderSide,
        quantity: Decimal,
    ) -> Decimal:
        """Calculate available liquidity for the order.

        Args:
            order_book: Current order book
            side: Order side (BUY/SELL)
            quantity: Requested quantity

        Returns:
            Decimal: Total available liquidity across all price levels
        """
        available = Decimal(0)
        levels = order_book.asks if side == OrderSide.BUY else order_book.bids

        for _price, size in levels:
            available += size
            if available >= quantity:
                return available

        return available

    def _estimate_slippage(self, symbol: Symbol, quantity: Decimal) -> Decimal:
        """Estimate slippage for the given symbol and quantity.

        Args:
            symbol: Trading Symbol object
            quantity: Order quantity

        Returns:
            Decimal: Estimated slippage percentage
        """
        # Use configured slippage - no external signal generator
        return self._config.get_slippage_for_symbol(symbol)

    def _validate_price_bounds(
        self,
        aggressive_price: Decimal,
        symbol: Symbol,
        reference_price: Decimal,
    ) -> None:
        """Validate price is within acceptable bounds.

        Args:
            aggressive_price: Calculated aggressive price
            symbol: Trading symbol
            reference_price: Reference price (best bid/ask)

        Raises:
            PriceDeviationError: If price deviation exceeds limits
            MarketOrderError.invalid_price_error: If price is invalid
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
        if not aggressive_price.is_finite() or aggressive_price <= Decimal(0):
            raise MarketOrderError.invalid_price_error(aggressive_price)

    async def round_to_tick_size(self, price: Decimal, symbol: Symbol) -> Decimal:
        """Round price to exchange tick size.

        Args:
            price: Price to round
            symbol: Trading symbol

        Returns:
            Decimal: Rounded price

        Raises:
            MarketOrderError: If market metadata retrieval fails with specific errors.
        """
        try:
            # Get market metadata from exchange

            exchange_name = (
                ExchangeName.HYPERLIQUID
                if self._exchange.exchange_name == "hyperliquid"
                else ExchangeName.BACKPACK
            )
            # Get the appropriate exchange symbol using type-safe method
            if exchange_name == ExchangeName.HYPERLIQUID:
                exchange_symbol = exchanges.hyperliquid(value=symbol.value)
            else:  # ExchangeName.BACKPACK
                exchange_symbol = exchanges.backpack(value=symbol.value)
            market = await self._exchange.get_market(GetMarketArgs(symbol=exchange_symbol))
        except MarketOrderError:
            raise
        except (ValueError, TypeError, AttributeError, OSError) as e:
            logger.exception(
                "market_metadata_fetch_failed_price",
                symbol=symbol,
                error=str(e),
                message="Failed to get market metadata, returning original price",
            )
            return price
        else:
            if market and market.tick_size:
                # Round to nearest tick_size multiple
                tick_size = market.tick_size
                rounded = (price / tick_size).quantize(
                    Decimal(1),
                    rounding=ROUND_DOWN,
                ) * tick_size
                logger.debug(
                    "price_rounded_tick_size",
                    symbol=symbol,
                    tick_size=float(tick_size),
                    original_price=float(price),
                    rounded_price=float(rounded),
                    message="Rounded price using tick_size",
                )
                return rounded
            logger.warning(
                "no_tick_size_found",
                action="round_price",
                symbol=symbol,
                price=float(price),
                message=f"No tick size found for {symbol}, returning original price",
            )
            return price

    async def round_to_step_size(self, quantity: Decimal, symbol: Symbol) -> Decimal:
        """Round quantity to exchange step size.

        Args:
            quantity: Quantity to round
            symbol: Trading symbol

        Returns:
            Decimal: Rounded quantity

        Raises:
            MarketOrderError: If market metadata retrieval fails with specific errors.
        """
        try:
            # Get market metadata from exchange

            exchange_name = (
                ExchangeName.HYPERLIQUID
                if self._exchange.exchange_name == "hyperliquid"
                else ExchangeName.BACKPACK
            )
            # Get the appropriate exchange symbol using type-safe method
            if exchange_name == ExchangeName.HYPERLIQUID:
                exchange_symbol = exchanges.hyperliquid(value=symbol.value)
            else:  # ExchangeName.BACKPACK
                exchange_symbol = exchanges.backpack(value=symbol.value)
            market = await self._exchange.get_market(GetMarketArgs(symbol=exchange_symbol))
        except MarketOrderError:
            raise
        except (ValueError, TypeError, AttributeError, OSError) as e:
            logger.exception(
                "market_metadata_fetch_failed_quantity",
                symbol=symbol,
                error=str(e),
                message="Failed to get market metadata, returning original quantity",
            )
            return quantity
        else:
            if market and market.step_size:
                # Round to nearest step_size multiple
                step_size = market.step_size
                rounded = (quantity / step_size).quantize(
                    Decimal(1),
                    rounding=ROUND_DOWN,
                ) * step_size
                logger.debug(
                    "quantity_rounded_step_size",
                    symbol=symbol,
                    step_size=float(step_size),
                    original_quantity=float(quantity),
                    rounded_quantity=float(rounded),
                    message="Rounded quantity using step_size",
                )
                return rounded
            logger.warning(
                "no_step_size_found",
                action="round_quantity",
                symbol=symbol,
                quantity=float(quantity),
                message="No step size found, returning original quantity",
            )
            return quantity

    async def get_reference_price_all_mids(self, symbol: Symbol) -> Decimal | None:
        """Get reference price from AllMids endpoint if available.

        Args:
            symbol: Trading Symbol object

        Returns:
            Decimal | None: Mid price if available, None otherwise

        Raises:
            MarketOrderError: If AllMids fetch fails with specific errors.
        """
        if not self._config.use_all_mids_for_reference:
            return None

        try:
            # Use the standard ExchangeAPI get_all_mids method
            all_mids = await self._exchange.get_all_mids()
            return all_mids.get(symbol)
        except NotImplementedError:
            # Exchange doesn't support get_all_mids (e.g., Backpack)
            logger.debug(
                "allmids_not_supported",
                exchange=self._exchange.exchange_name,
                message=f"Exchange {self._exchange.exchange_name} does not support get_all_mids",
            )
            return None
        except MarketOrderError:
            raise
        except (ValueError, TypeError, AttributeError, OSError) as e:
            logger.debug(
                "allmids_fetch_failed",
                action="fetch_mids",
                error=str(e),
                message=f"Failed to fetch AllMids: {e}",
            )

        return None

    def calculate_liquidity_ratio(
        self,
        order_book: OrderBook,
        side: OrderSide,
        quantity: Decimal,
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
        return available / quantity if quantity > Decimal(0) else Decimal(0)

    def validate_config(self) -> None:
        """Validate market order configuration.

        Raises:
            MarketOrderParameterError.config_disabled_error: If market orders are disabled
            MarketOrderParameterError.config_slippage_error: If slippage config is invalid
            MarketOrderParameterError.config_deviation_error: If price deviation config is invalid
        """
        if not self._config.enabled:
            raise MarketOrderParameterError.config_disabled_error()

        if self._config.max_slippage_pct <= Decimal(0):
            raise MarketOrderParameterError.config_slippage_error()

        if self._config.max_price_deviation_pct <= Decimal(0):
            raise MarketOrderParameterError.config_deviation_error()
