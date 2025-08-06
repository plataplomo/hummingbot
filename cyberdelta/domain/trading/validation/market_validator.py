"""Market condition validation against current market data.

This module validates orders against current market conditions like price deviation
and order book depth using validated AppSettings configuration.
"""

from __future__ import annotations

from cyberdelta.config.models.exchange_config import ExchangeSpecificConfig
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import OrderSide
from cyberdelta.models.market.market_snapshot import MarketSnapshot
from cyberdelta.models.market.order import Order
from cyberdelta.models.market.order_book import OrderBook
from cyberdelta.models.market.ticker import Ticker


logger = get_logger(__name__)


class MarketValidator:
    """Validates orders against current market conditions from configuration.

    IMPORTANT: Following CODING_STANDARDS.md:
    - Check price deviation from config.exchanges[exchange].max_price_deviation_pct
    - NO assumptions about market behavior
    - Uses current market data for validation
    - All monetary values as Decimal, NOT float
    """

    @staticmethod
    async def validate(
        order: Order,
        market_snapshot: MarketSnapshot,
        exchange_config: ExchangeSpecificConfig,
    ) -> list[str]:
        """Validate order against current market conditions.

        Args:
            order: Order to validate
            market_snapshot: Current market data snapshot
            exchange_config: Exchange configuration from AppSettings

        Returns:
            List of market condition violations

        Note:
        - Check price deviation from config.exchanges[exchange].max_price_deviation_pct
        - NO assumptions about market behavior
        - Uses current market data for validation
        """
        violations: list[str] = []

        try:
            # Get current ticker for symbol/exchange
            ticker = market_snapshot.get_ticker(order.exchange, order.symbol)
            if not ticker:
                violations.append(
                    f"No market data available for {order.symbol.value} on {order.exchange}"
                )
                return violations

            # Check price deviation
            violations.extend(
                MarketValidator._check_price_deviation(order, ticker, exchange_config)
            )

            # Check order book depth
            violations.extend(
                MarketValidator._check_order_book_depth(order, market_snapshot, exchange_config)
            )

        except Exception as e:
            logger.exception(
                "market_condition_validation_error",
                order_id=order.exchange_order_id,
                error=str(e),
            )
            violations.append(f"Market condition validation failed: {e!s}")

        return violations

    @staticmethod
    def _check_price_deviation(
        order: Order, ticker: Ticker, exchange_config: ExchangeSpecificConfig
    ) -> list[str]:
        """Check order price deviation from market.

        Returns:
            List of violations if any.
        """
        violations: list[str] = []

        if exchange_config.max_price_deviation_pct is not None and order.price and ticker.price:
            max_deviation = exchange_config.max_price_deviation_pct
            current_price = ticker.price
            price_deviation = abs(order.price - current_price) / current_price * 100

            if price_deviation > max_deviation:
                violations.append(
                    f"Order price {order.price} deviates {price_deviation:.2f}% from market "
                    f"price {current_price}, exceeds max {max_deviation}%"
                )

        return violations

    @staticmethod
    def _check_order_book_depth(
        order: Order,
        market_snapshot: MarketSnapshot,
        exchange_config: ExchangeSpecificConfig,
    ) -> list[str]:
        """Check order book depth for sufficient liquidity.

        Returns:
            List of violations if any.
        """
        violations: list[str] = []

        if exchange_config.min_order_book_depth is not None:
            order_book = market_snapshot.get_order_book(order.exchange, order.symbol)
            if order_book:
                violations.extend(
                    MarketValidator._validate_book_depth(
                        order, order_book, exchange_config.min_order_book_depth
                    )
                )

        return violations

    @staticmethod
    def _validate_book_depth(order: Order, order_book: OrderBook, min_depth: float) -> list[str]:
        """Validate order book has sufficient depth.

        Returns:
            List of violations if any.
        """
        violations: list[str] = []

        if order.side == OrderSide.BUY and order_book.asks:
            depth = sum(ask[1] for ask in order_book.asks[:5])  # Top 5 levels
            if depth < min_depth:
                violations.append(
                    f"Insufficient ask depth {depth} for buy order, minimum {min_depth}"
                )
        elif order.side == OrderSide.SELL and order_book.bids:
            depth = sum(bid[1] for bid in order_book.bids[:5])  # Top 5 levels
            if depth < min_depth:
                violations.append(
                    f"Insufficient bid depth {depth} for sell order, minimum {min_depth}"
                )

        return violations
