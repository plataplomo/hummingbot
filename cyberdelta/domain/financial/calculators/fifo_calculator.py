"""FIFO (First-In-First-Out) PnL calculator - regulatory accounting method.

PURE MATHEMATICAL CALCULATIONS - NO STATE DEPENDENCIES
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import OrderSide
from cyberdelta.models import Fill
from cyberdelta.models.financial import PnLResult
from cyberdelta.protocols.financial import FeeCalculatorProtocol


logger = get_logger(__name__)


class FIFOCalculator:
    """First-in-first-out PnL calculation - regulatory accounting method.

    Used for tax reporting and regulatory compliance where trades must be
    matched using FIFO accounting principles.
    """

    def __init__(
        self,
        config: AppSettings,
        fee_calculator: FeeCalculatorProtocol | None = None,
    ) -> None:
        """Initialize FIFO calculator with configuration.

        Args:
            config: Application settings containing financial calculation configuration
            fee_calculator: Optional fee calculator for fee-inclusive PnL calculations
        """
        self.config = config
        self._fee_calc = fee_calculator
        self._financial_config = config.financial
        self._base_currency = self._financial_config.currency.base_currency
        self._calculation_precision = self._financial_config.precision.calculation_precision

        logger.info(
            "fifo_calculator_initialized",
            base_currency=self._base_currency,
            precision=self._calculation_precision,
        )

    def calculate_realized_pnl_fifo(
        self,
        entry_fills: list[Fill],
        exit_fill: Fill,
        include_fees: bool = False,
    ) -> PnLResult:
        """Calculate FIFO realized PnL - matches exit with oldest entries first.

        Args:
            entry_fills: List of entry fills to match against
            exit_fill: Exit fill to calculate PnL for
            include_fees: Whether to include fees in calculation

        Returns:
            PnLResult with FIFO-calculated PnL
        """
        if not entry_fills:
            return self._create_zero_result(include_fees, "fifo")

        logger.debug(
            "calculating_fifo_pnl",
            entry_fills_count=len(entry_fills),
            exit_fill_quantity=exit_fill.quantity,
            exit_fill_price=exit_fill.price,
            include_fees=include_fees,
        )

        # Sort entry fills by timestamp (oldest first)
        sorted_entries = sorted(entry_fills, key=lambda f: f.executed_at)

        remaining_exit_qty = abs(exit_fill.quantity)
        total_pnl = Decimal(0)
        matched_entries: list[dict[str, Decimal]] = []

        for entry_fill in sorted_entries:
            if remaining_exit_qty <= 0:
                break

            # Calculate PnL for this entry-exit pair
            matched_qty = min(abs(entry_fill.quantity), remaining_exit_qty)

            if exit_fill.side == OrderSide.SELL:
                # Closing long: (exit_price - entry_price) * quantity
                pair_pnl = matched_qty * (exit_fill.price - entry_fill.price)
            else:
                # Closing short: (entry_price - exit_price) * quantity
                pair_pnl = matched_qty * (entry_fill.price - exit_fill.price)

            total_pnl += pair_pnl
            remaining_exit_qty -= matched_qty

            matched_entries.append({
                "entry_price": entry_fill.price,
                "matched_quantity": matched_qty,
                "pair_pnl": pair_pnl,
            })

        # Apply fees if requested
        fees_amount = Decimal(0)
        if include_fees and self._fee_calc:
            try:
                exchange_config = getattr(self.config.exchanges, exit_fill.exchange.value, {})
                fee_result = self._fee_calc.calculate_fee(
                    fill=exit_fill, exchange_config=exchange_config
                )
                fees_amount = fee_result.amount
                total_pnl -= fees_amount

                logger.debug(
                    "fifo_fees_applied",
                    fees_amount=fees_amount,
                    net_pnl=total_pnl,
                )
            except ValueError as e:
                logger.warning(
                    "failed_to_calculate_fees_for_fifo",
                    error=str(e),
                    fill_id=getattr(exit_fill, "id", "unknown"),
                )

        # Apply precision
        precision_quantizer = Decimal(10) ** -self._calculation_precision
        total_pnl = total_pnl.quantize(precision_quantizer)
        if fees_amount > 0:
            fees_amount = fees_amount.quantize(precision_quantizer)

        logger.debug(
            "fifo_pnl_calculated",
            matched_entries_count=len(matched_entries),
            total_pnl=total_pnl,
            fees_amount=fees_amount,
            unmatched_quantity=remaining_exit_qty,
        )

        return PnLResult(
            amount=total_pnl,
            currency=self._base_currency,
            gross_amount=total_pnl + fees_amount if fees_amount > 0 else total_pnl,
            fees_amount=fees_amount if fees_amount > 0 else None,
            includes_fees=include_fees,
            calculation_method="fifo",
            calculation_timestamp=datetime.now(UTC),
            position_size=exit_fill.quantity,
            entry_price=None,  # Multiple entry prices in FIFO
            mark_price=exit_fill.price,
            precision=self._calculation_precision,
        )

    def _create_zero_result(self, includes_fees: bool, method: str) -> PnLResult:
        """Create zero PnL result for FIFO calculator.

        Args:
            includes_fees: Whether fees are included in the calculation
            method: Calculation method identifier

        Returns:
            Zero PnLResult with consistent structure
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
            mark_price=None,
            precision=self._calculation_precision,
        )
