"""Mark-to-market PnL calculator - standard unrealized/realized PnL calculations.

Single source of truth for mark-to-market PnL calculations in the system.
PURE MATHEMATICAL CALCULATIONS - NO STATE DEPENDENCIES
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import OrderSide
from cyberdelta.models import DerivativePosition, Fill
from cyberdelta.models.financial import PnLResult
from cyberdelta.protocols.financial import FeeCalculatorProtocol


logger = get_logger(__name__)


class MarkToMarketCalculator:
    """Mark-to-market PnL calculator - standard approach.

    Replaces:
    - DerivativePosition.calculate_unrealized_pnl()
    - PortfolioStateManager._calculate_realized_pnl()
    - PerformanceTracker._calculate_pnl methods
    - Current PnLCalculator in portfolio domain

    PURE MATHEMATICAL CALCULATIONS - NO STATE DEPENDENCIES
    """

    def __init__(
        self,
        config: AppSettings,
        fee_calculator: FeeCalculatorProtocol | None = None,
    ) -> None:
        """Initialize mark-to-market calculator with configuration.

        Args:
            config: Application settings containing financial calculation configuration
            fee_calculator: Optional fee calculator for fee-inclusive PnL calculations
        """
        self.config = config
        self._fee_calc = fee_calculator

        # Cache financial calculation settings from config
        self._financial_config = config.financial
        self._include_fees = self._financial_config.pnl.include_fees_in_pnl
        self._base_currency = self._financial_config.currency.base_currency
        self._calculation_precision = self._financial_config.precision.calculation_precision
        self._pnl_method = self._financial_config.pnl.calculation_method

        logger.info(
            "mark_to_market_calculator_initialized",
            base_currency=self._base_currency,
            include_fees=self._include_fees,
            calculation_method=self._pnl_method,
            precision=self._calculation_precision,
        )

    def calculate_unrealized_pnl(
        self,
        position: DerivativePosition,
        mark_price: Decimal,
        include_fees: bool | None = None,
    ) -> PnLResult:
        """Calculate unrealized PnL with configuration support.

        Args:
            position: Position to calculate PnL for
            mark_price: Current market price
            include_fees: Override config default for fee inclusion

        Returns:
            PnLResult with amount, currency, and calculation metadata

        Raises:
            ValueError: If fee inclusion requested but no FeeCalculator provided
        """
        # Use config defaults if not specified
        include_fees = include_fees if include_fees is not None else self._include_fees

        logger.debug(
            "calculating_unrealized_pnl",
            position_symbol=position.symbol.value if hasattr(position, "symbol") else "unknown",
            position_size=position.size,
            mark_price=mark_price,
            include_fees=include_fees,
        )

        # Validate inputs
        if position.size == Decimal(0):
            return self._create_zero_pnl_result(include_fees, "zero_position", mark_price)

        if not position.entry_price or position.entry_price == Decimal(0):
            logger.warning(
                "no_entry_price_for_position",
                position_size=position.size,
                symbol=getattr(position, "symbol", "unknown"),
            )
            return self._create_zero_pnl_result(include_fees, "no_entry_price", mark_price)

        # Calculate base PnL based on position direction
        size_abs = abs(position.size)

        if position.side == OrderSide.BUY:
            # Long position: profit when mark > entry
            gross_pnl = size_abs * (mark_price - position.entry_price)
        else:
            # Short position: profit when entry > mark
            gross_pnl = size_abs * (position.entry_price - mark_price)

        # Apply fees if configured
        net_pnl = gross_pnl
        fees_amount = Decimal(0)

        if include_fees:
            # Following CODING_STANDARDS.md: Explicit fee handling, no assumptions
            if self._fee_calc is None:
                msg = (
                    "Fee inclusion requested but no FeeCalculator provided. "
                    "Inject FeeCalculator in constructor or set include_fees=False."
                )
                raise ValueError(msg)

            logger.debug(
                "fee_calculation_not_available_for_position_pnl",
                position_size=position.size,
                symbol=getattr(position, "symbol", "unknown"),
                reason="Position PnL uses mark-to-market, fees calculated on fills",
            )

        # Apply precision from config
        precision_quantizer = Decimal(10) ** -self._calculation_precision
        net_pnl = net_pnl.quantize(precision_quantizer)
        gross_pnl = gross_pnl.quantize(precision_quantizer)
        if fees_amount > 0:
            fees_amount = fees_amount.quantize(precision_quantizer)

        result = PnLResult(
            amount=net_pnl,
            currency=self._base_currency,
            gross_amount=gross_pnl,
            fees_amount=fees_amount if fees_amount > 0 else None,
            includes_fees=include_fees,
            calculation_method=f"mark_to_market_{self._pnl_method}",
            calculation_timestamp=datetime.now(UTC),
            position_size=position.size,
            entry_price=position.entry_price,
            mark_price=mark_price,
            precision=self._calculation_precision,
        )

        logger.debug(
            "unrealized_pnl_calculated",
            pnl_amount=result.amount,
            currency=result.currency,
            is_profitable=result.is_profitable,
            calculation_method=result.calculation_method,
        )

        return result

    def calculate_realized_pnl(
        self,
        position: DerivativePosition,
        fill: Fill,
        include_fees: bool | None = None,
    ) -> PnLResult:
        """Calculate realized PnL for position closing fill.

        Args:
            position: Position being closed/reduced
            fill: Fill that closes/reduces the position
            include_fees: Override config default for fee inclusion

        Returns:
            PnLResult with realized amount, currency, and calculation metadata
        """
        # Use config defaults if not specified
        include_fees = include_fees if include_fees is not None else self._include_fees

        logger.debug(
            "calculating_realized_pnl",
            position_symbol=position.symbol.value if hasattr(position, "symbol") else "unknown",
            position_size=position.size,
            fill_price=fill.price,
            fill_quantity=fill.quantity,
            include_fees=include_fees,
        )

        # Validate that fill actually closes/reduces position
        if position.size == Decimal(0):
            return self._create_zero_pnl_result(include_fees, "zero_position_close")

        if not position.entry_price:
            return self._create_zero_pnl_result(include_fees, "no_entry_price_close")

        # Calculate realized PnL based on fill quantity and price difference
        fill_quantity_abs = abs(fill.quantity)

        if position.side == OrderSide.BUY:
            # Closing long position: realized PnL = (exit_price - entry_price) * quantity
            gross_pnl = fill_quantity_abs * (fill.price - position.entry_price)
        else:
            # Closing short position: realized PnL = (entry_price - exit_price) * quantity
            gross_pnl = fill_quantity_abs * (position.entry_price - fill.price)

        # Apply fees if configured and available
        net_pnl = gross_pnl
        fees_amount = Decimal(0)

        if include_fees and self._fee_calc:
            try:
                # Calculate fees for this specific fill
                exchange_config = getattr(self.config.exchanges, fill.exchange.value, {})
                fee_result = self._fee_calc.calculate_fee(
                    fill=fill, exchange_config=exchange_config
                )
                fees_amount = fee_result.amount
                net_pnl = gross_pnl - fees_amount

                logger.debug(
                    "fees_applied_to_realized_pnl",
                    gross_pnl=gross_pnl,
                    fees_amount=fees_amount,
                    net_pnl=net_pnl,
                )
            except ValueError as e:
                logger.warning(
                    "failed_to_calculate_fees_for_realized_pnl",
                    error=str(e),
                    fill_id=getattr(fill, "id", "unknown"),
                )

        # Apply precision
        precision_quantizer = Decimal(10) ** -self._calculation_precision
        net_pnl = net_pnl.quantize(precision_quantizer)
        gross_pnl = gross_pnl.quantize(precision_quantizer)
        if fees_amount > 0:
            fees_amount = fees_amount.quantize(precision_quantizer)

        result = PnLResult(
            amount=net_pnl,
            currency=self._base_currency,
            gross_amount=gross_pnl,
            fees_amount=fees_amount if fees_amount > 0 else None,
            includes_fees=include_fees,
            calculation_method=f"realized_{self._pnl_method}",
            calculation_timestamp=datetime.now(UTC),
            position_size=fill.quantity,
            entry_price=position.entry_price,
            mark_price=fill.price,
            precision=self._calculation_precision,
        )

        logger.debug(
            "realized_pnl_calculated",
            pnl_amount=result.amount,
            currency=result.currency,
            is_profitable=result.is_profitable,
            calculation_method=result.calculation_method,
        )

        return result

    def calculate_portfolio_pnl(
        self,
        positions: list[DerivativePosition],
        mark_prices: dict[str, Decimal],
        include_fees: bool | None = None,
    ) -> PnLResult:
        """Calculate total portfolio PnL across all positions.

        Args:
            positions: List of all portfolio positions
            mark_prices: Current market prices by symbol
            include_fees: Override config default for fee inclusion

        Returns:
            PnLResult with total portfolio PnL and calculation metadata
        """
        # Use config defaults if not specified
        include_fees = include_fees if include_fees is not None else self._include_fees

        logger.debug(
            "calculating_portfolio_pnl",
            position_count=len(positions),
            include_fees=include_fees,
        )

        if not positions:
            return self._create_zero_pnl_result(include_fees, "empty_portfolio")

        total_pnl = Decimal(0)
        total_gross_pnl = Decimal(0)
        total_fees = Decimal(0)
        calculated_positions = 0

        for position in positions:
            # Skip zero positions
            if position.size == Decimal(0):
                continue

            # Get symbol for mark price lookup
            symbol_key = (
                position.symbol.value if hasattr(position, "symbol") else str(position.symbol)
            )

            if symbol_key not in mark_prices:
                logger.warning(
                    "no_mark_price_for_position", symbol=symbol_key, position_size=position.size
                )
                continue

            mark_price = mark_prices[symbol_key]

            # Calculate unrealized PnL for this position
            position_pnl = self.calculate_unrealized_pnl(
                position=position,
                mark_price=mark_price,
                include_fees=include_fees,
            )

            # Accumulate totals
            total_pnl += position_pnl.amount
            if position_pnl.gross_amount:
                total_gross_pnl += position_pnl.gross_amount
            if position_pnl.fees_amount:
                total_fees += position_pnl.fees_amount

            calculated_positions += 1

        logger.debug(
            "portfolio_pnl_calculated",
            total_positions=len(positions),
            calculated_positions=calculated_positions,
            total_pnl=total_pnl,
        )

        # Apply precision
        precision_quantizer = Decimal(10) ** -self._calculation_precision
        total_pnl = total_pnl.quantize(precision_quantizer)
        total_gross_pnl = total_gross_pnl.quantize(precision_quantizer)
        if total_fees > 0:
            total_fees = total_fees.quantize(precision_quantizer)

        return PnLResult(
            amount=total_pnl,
            currency=self._base_currency,
            gross_amount=total_gross_pnl if total_gross_pnl != total_pnl else None,
            fees_amount=total_fees if total_fees > 0 else None,
            includes_fees=include_fees,
            calculation_method=f"portfolio_{self._pnl_method}",
            calculation_timestamp=datetime.now(UTC),
            position_size=None,
            entry_price=None,
            mark_price=None,
            precision=self._calculation_precision,
        )

    def _create_zero_pnl_result(
        self, includes_fees: bool, method: str, mark_price: Decimal | None = None
    ) -> PnLResult:
        """Create a zero PnL result with consistent structure.

        Args:
            includes_fees: Whether fees are included in the calculation
            method: Calculation method identifier
            mark_price: Optional mark price for the result

        Returns:
            PnLResult with zero amounts and provided metadata
        """
        return PnLResult(
            amount=Decimal(0),
            currency=self._base_currency,
            gross_amount=Decimal(0),
            fees_amount=None,
            includes_fees=includes_fees,
            calculation_method=method,
            calculation_timestamp=datetime.now(UTC),
            position_size=None,
            entry_price=None,
            mark_price=mark_price,
            precision=self._calculation_precision,
        )
