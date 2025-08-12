"""Market validation rules for crypto trading.

This module implements validation rules that check market conditions
for 24/7 crypto markets before allowing orders.

These rules consolidate market validation that was previously scattered
across MarketValidator and other validators.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import TradingState, ValidationCategory
from cyberdelta.models.validation import ValidationResult


if TYPE_CHECKING:
    from cyberdelta.infrastructure.validation.validation_context import ValidationContext
    from cyberdelta.models.market.order import Order

logger = get_logger(__name__)


class MarketStatusRule:
    """Validates that the market is open and trading is allowed.

    This rule ensures that orders are only placed when the market
    is in a tradeable state.

    Consolidates market status checks from MarketValidator.

    IMPORTANT: Following CODING_STANDARDS.md:
    - Market status from market snapshot or exchange API
    - NO assumptions about market hours or status
    - Uses TradingState from context for system-level halts
    """

    def __init__(self, enabled: bool = True) -> None:
        """Initialize market status rule.

        Args:
            enabled: Whether this rule is enabled
        """
        self._enabled = enabled

    @property
    def name(self) -> str:
        """Rule name for identification."""
        return "market_status"

    @property
    def category(self) -> ValidationCategory:
        """Rule category - MARKET checks for trading status."""
        return ValidationCategory.MARKET

    @property
    def enabled(self) -> bool:
        """Whether rule is enabled."""
        return self._enabled

    @property
    def bypass_on_reduce_only(self) -> bool:
        """Market status checks may be relaxed for reduce-only orders."""
        return True  # Allow position closing even when market is transitioning

    async def validate(self, order: Order, context: ValidationContext) -> ValidationResult:
        """Validate that market is open for trading.

        Args:
            order: Order to validate
            context: Validation context with market snapshot and trading state

        Returns:
            ValidationResult with any violations

        Note:
            - Checks both system-level trading state and market-specific status
            - May allow reduce-only orders during market transitions
        """
        violations: list[str] = []

        # Check system-level trading state first
        if context.trading_state == TradingState.HALTED:
            # System is halted - no trading allowed unless reduce-only
            if not context.is_reduce_only:
                violations.append(
                    f"Trading system is HALTED - new positions not allowed for {order.symbol.value}"
                )
            else:
                logger.info(
                    "reduce_only_order_allowed_during_halt",
                    order_id=order.exchange_order_id,
                    symbol=order.symbol.value,
                    trading_state=context.trading_state.value,
                )

        # Check market-specific status if available
        if context.has_market_data() and context.market_snapshot is not None:
            market_snapshot = context.market_snapshot

            # Validate ticker data exists - if no ticker, market is not available
            ticker = market_snapshot.get_ticker(order.exchange, order.symbol)
            if ticker is None:
                violations.append(
                    f"Market not available for {order.symbol.value} on "
                    f"{order.exchange.value} - no ticker data"
                )
                logger.warning(
                    "market_not_available_no_ticker",
                    order_id=order.exchange_order_id,
                    symbol=order.symbol.value,
                    exchange=order.exchange.value,
                )

        # If no market data available, we can't validate market status
        # This is not necessarily a violation - log and proceed
        elif not context.has_market_data():
            logger.warning(
                "market_status_validation_no_data",
                order_id=order.exchange_order_id,
                symbol=order.symbol.value,
                msg="No market data available for market status validation",
            )

        return ValidationResult(
            violations=violations,
            category=self.category,
            rule_name=self.name,
        )


# TradingHoursRule removed - crypto markets are 24/7, no trading hours validation needed


class LiquidityRule:
    """Validates that sufficient market liquidity exists for the order.

    This rule ensures that orders are not placed when market liquidity
    is insufficient to execute them reasonably.

    IMPORTANT: Following CODING_STANDARDS.md:
    - Liquidity thresholds from configuration
    - Uses market snapshot for bid/ask and volume data
    - NO hardcoded liquidity assumptions
    """

    def __init__(self, enabled: bool = True) -> None:
        """Initialize liquidity rule.

        Args:
            enabled: Whether this rule is enabled
        """
        self._enabled = enabled

    @property
    def name(self) -> str:
        """Rule name for identification."""
        return "market_liquidity"

    @property
    def category(self) -> ValidationCategory:
        """Rule category - MARKET checks for liquidity."""
        return ValidationCategory.MARKET

    @property
    def enabled(self) -> bool:
        """Whether rule is enabled."""
        return self._enabled

    @property
    def bypass_on_reduce_only(self) -> bool:
        """Liquidity checks always apply."""
        return False  # Even reduce-only orders need liquidity

    async def validate(self, order: Order, context: ValidationContext) -> ValidationResult:
        """Validate that sufficient liquidity exists for the order.

        Args:
            order: Order to validate
            context: Validation context with market snapshot

        Returns:
            ValidationResult with any violations

        Note:
            - Requires market snapshot with bid/ask data
            - Checks both price impact and volume constraints
        """
        violations: list[str] = []

        # Skip if no market data available
        if not context.has_market_data() or context.market_snapshot is None:
            logger.debug(
                "liquidity_validation_no_data",
                order_id=order.exchange_order_id,
                symbol=order.symbol.value,
                msg="No market data available for liquidity validation",
            )
            return ValidationResult(violations=[])

        market_snapshot = context.market_snapshot

        # Get ticker data for the order's symbol/exchange
        ticker = market_snapshot.get_ticker(order.exchange, order.symbol)
        if ticker is not None:
            # Check bid/ask spread if available
            if ticker.bid and ticker.ask and ticker.bid > 0 and ticker.ask > 0:
                spread_pct = ((ticker.ask - ticker.bid) / ((ticker.bid + ticker.ask) / 2)) * 100

                # Check against configured maximum spread
                max_spread = self._get_max_spread_threshold(context)
                if max_spread and spread_pct > max_spread:
                    violations.append(
                        f"Bid-ask spread {spread_pct:.2f}% exceeds maximum {max_spread}% "
                        f"for {order.symbol.value}"
                    )

            # Check volume requirements if available
            if ticker.volume:
                min_volume = self._get_min_volume_threshold(context)
                if min_volume and ticker.volume < min_volume:
                    violations.append(
                        f"24h volume ${ticker.volume:,.0f} below minimum ${min_volume:,.0f} "
                        f"for {order.symbol.value}"
                    )

        return ValidationResult(
            violations=violations,
            category=self.category,
            rule_name=self.name,
        )

    def _get_max_spread_threshold(self, context: ValidationContext) -> float | None:
        """Get maximum allowed bid-ask spread percentage.

        Args:
            context: Validation context with configuration

        Returns:
            Maximum spread percentage or None if not configured
        """
        # Check exchange-specific configuration first
        if (
            context.has_exchange_config()
            and context.exchange_config is not None
            and context.exchange_config.max_spread_pct is not None
        ):
            return context.exchange_config.max_spread_pct

        # Fallback to global validation configuration
        return context.config.validation.max_spread_pct

    def _get_min_volume_threshold(self, context: ValidationContext) -> float | None:
        """Get minimum required 24h volume.

        Args:
            context: Validation context with configuration

        Returns:
            Minimum volume or None if not configured
        """
        # Check exchange-specific configuration first
        if (
            context.has_exchange_config()
            and context.exchange_config is not None
            and context.exchange_config.min_volume_24h is not None
        ):
            return context.exchange_config.min_volume_24h

        # Fallback to global validation configuration
        return context.config.validation.min_volume_24h
