"""Realized P&L calculator with direct AppSettings access following risk module patterns."""

from __future__ import annotations

from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING

from pydantic import Field, ValidationInfo, field_validator
from pydantic.dataclasses import dataclass

from cyberdelta.config import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import DerivativePosition, Trade
from cyberdelta.core.portfolio.base import CalculationResult, TypedCalculator
from cyberdelta.core.portfolio.base.typed_calculator import (
    CalculationMetadata as TypedCalculatorMetadata,
)
from cyberdelta.core.portfolio.exceptions import InvalidCalculationInputError
from cyberdelta.core.portfolio.portfolio_types.calculations import (
    CalculationMetadata,
    RealizedPnLResult,
)
from cyberdelta.enums import OrderSide


logger = get_logger(__name__)


if TYPE_CHECKING:
    from cyberdelta.core.portfolio.models.base import BaseStateModel
    from cyberdelta.core.portfolio.portfolio_types.protocols import StateContainerProtocol


class PnLCalculationMethod(Enum):
    """Methods for calculating realized P&L."""

    FIFO = "fifo"  # First In, First Out
    LIFO = "lifo"  # Last In, First Out
    WEIGHTED_AVERAGE = "weighted_average"


@dataclass
class RealizedPnLInput:
    """Input for realized P&L calculation with validation."""

    position: DerivativePosition = Field(description="Current derivative position")
    trade: Trade = Field(description="Trade to calculate P&L for")
    calculation_method: PnLCalculationMethod | None = Field(
        default=None, description="P&L calculation method"
    )

    @field_validator("trade", mode="after")
    @classmethod
    def validate_trade(cls, v: Trade) -> Trade:
        """Validate trade has required fields.

        Returns:
            The validated trade.

        Raises:
            InvalidCalculationInputError: If trade validation fails.
        """
        if v.quantity <= 0:
            raise InvalidCalculationInputError(
                parameter="quantity", value=v.quantity, expected="positive value"
            )
        if v.price <= 0:
            raise InvalidCalculationInputError(
                parameter="price", value=v.price, expected="positive value"
            )
        return v

    @field_validator("position", "trade", mode="after")
    @classmethod
    def validate_symbol_match(
        cls, v: DerivativePosition | Trade, info: ValidationInfo
    ) -> DerivativePosition | Trade:
        """Validate position and trade symbols match.

        Returns:
            The validated position or trade.

        Raises:
            InvalidCalculationInputError: If symbols do not match.
        """
        if info.field_name == "trade" and "position" in info.data:
            position = info.data["position"]
            if position.symbol != v.symbol:
                raise InvalidCalculationInputError(
                    parameter="trade",
                    value=f"{position.symbol} != {v.symbol}",
                    expected="matching symbols",
                )
        return v


class RealizedPnLCalculator(TypedCalculator[RealizedPnLInput, RealizedPnLResult]):
    """Calculates realized P&L from trade executions.

    Follows risk module patterns:
    - Direct AppSettings access
    - Inherits from TypedCalculator
    - Protocol-based dependencies
    - Strong typing with result types

    Fixes the critical short position calculation errors identified in the
    original PortfolioTracker implementation.
    """

    def __init__(
        self,
        app_settings: AppSettings,
        state_container: StateContainerProtocol[BaseStateModel],
        default_calculation_method: PnLCalculationMethod = PnLCalculationMethod.FIFO,
    ) -> None:
        """Initialize the realized P&L calculator.

        Args:
            app_settings: Application settings with portfolio configuration
            state_container: State container for data access
            default_calculation_method: Default method for P&L calculations
        """
        super().__init__(app_settings, state_container, "RealizedPnLCalculator")

        self.default_calculation_method = default_calculation_method

        self.logger.info(
            "realized_pnl_calculator_created",
            calculator_name=self.calculator_name,
            default_calculation_method=default_calculation_method.value,
        )

    async def calculate(self, input_data: RealizedPnLInput) -> CalculationResult[RealizedPnLResult]:
        """Calculate realized P&L from a trade execution.

        Args:
            input_data: Input containing position and trade data

        Returns:
            Calculation result with realized P&L details
        """
        try:
            position = input_data.position
            trade = input_data.trade
            method = input_data.calculation_method or self.default_calculation_method

            result = await self.calculate_from_trade(position, trade, method)

            return CalculationResult[RealizedPnLResult].success_result(
                result=result,
                metadata=TypedCalculatorMetadata(calculator=self.calculator_name),
            )

        except (ValueError, TypeError, ArithmeticError) as e:
            return CalculationResult[RealizedPnLResult].failure_result(
                errors=[f"Realized P&L calculation failed: {e}"],
                metadata=TypedCalculatorMetadata(
                    calculator=self.calculator_name, error_type=type(e).__name__
                ),
            )

    async def calculate_from_trade(
        self,
        position: DerivativePosition,
        trade: Trade,
        method: PnLCalculationMethod,
    ) -> RealizedPnLResult:
        """Calculate realized P&L when a trade affects an existing position.

        This implements the corrected logic that fixes the short position
        calculation errors from the original PortfolioTracker.

        Args:
            position: Current position before trade
            trade: Trade that was executed
            method: P&L calculation method to use

        Returns:
            RealizedPnLResult with calculation details
        """
        # If no existing position, no realized P&L
        if position.size == Decimal(0):
            return RealizedPnLResult(
                pnl=Decimal(0),
                position_size_change=self._get_position_size_change(trade),
                average_entry_price=trade.price,
                calculation_method=method.value,
                metadata=CalculationMetadata(
                    calculation_method=method.value, notes=f"new_position trade_id={trade.id}"
                ),
            )

        # Determine if trade is closing/reducing position
        is_closing_trade = self._is_closing_trade(position, trade)

        if not is_closing_trade:
            # Trade is increasing position size - no realized P&L
            new_avg_price = self._calculate_new_average_price(
                position.size, position.entry_price or Decimal(0), trade.quantity, trade.price
            )
            return RealizedPnLResult(
                pnl=Decimal(0),
                position_size_change=self._get_position_size_change(trade),
                average_entry_price=new_avg_price,
                calculation_method=method.value,
                metadata=CalculationMetadata(
                    calculation_method=method.value,
                    notes=(
                        f"position_increase trade_id={trade.id} "
                        f"old_entry_price={position.entry_price}"
                    ),
                ),
            )

        # Calculate realized P&L from closing/reducing position
        closing_quantity = min(trade.quantity, abs(position.size))
        realized_pnl = self._calculate_closing_pnl(position, trade, closing_quantity)

        # Calculate new average price (remains same for partial closes)
        new_avg_price = position.entry_price or Decimal(0)

        # If trade size is larger than position, calculate new entry price
        # for the remaining quantity that opens a new position
        if trade.quantity > abs(position.size):
            new_avg_price = trade.price

        logger.info(
            "realized_pnl_calculated",
            symbol=trade.symbol,
            trade_id=trade.id,
            realized_pnl=realized_pnl,
            closing_quantity=closing_quantity,
            old_position_size=position.size,
            trade_quantity=trade.quantity,
        )

        return RealizedPnLResult(
            pnl=realized_pnl,
            position_size_change=self._get_position_size_change(trade),
            average_entry_price=new_avg_price,
            calculation_method=method.value,
            metadata=CalculationMetadata(
                calculation_method=method.value,
                notes=(
                    f"position_close trade_id={trade.id} "
                    f"closing_quantity={closing_quantity} "
                    f"old_position_size={position.size}"
                ),
            ),
        )

    async def calculate_for_position_close(
        self,
        position: DerivativePosition,
        close_price: Decimal,
        calculation_method: PnLCalculationMethod | None = None,
    ) -> RealizedPnLResult:
        """Calculate realized P&L when closing an entire position.

        Args:
            position: Position being closed
            close_price: Price at which position is closed
            calculation_method: Method to use (defaults to configured method)

        Returns:
            RealizedPnLResult with calculation details
        """
        method = calculation_method or self.default_calculation_method

        if position.size == Decimal(0):
            return RealizedPnLResult(
                pnl=Decimal(0),
                position_size_change=Decimal(0),
                average_entry_price=close_price,
                calculation_method=method.value,
                metadata=CalculationMetadata(
                    calculation_method=method.value, notes="no_position_to_close"
                ),
            )

        # Calculate P&L based on position direction
        entry_price = position.entry_price or Decimal(0)
        if position.size > Decimal(0):
            # Long position: P&L = (close_price - entry_price) * size
            pnl = (close_price - entry_price) * position.size
        else:
            # Short position: P&L = (entry_price - close_price) * abs(size)
            pnl = (entry_price - close_price) * abs(position.size)

        return RealizedPnLResult(
            pnl=pnl,
            position_size_change=-position.size,  # Closing entire position
            average_entry_price=None,  # No position remaining
            calculation_method=method.value,
            metadata=CalculationMetadata(
                calculation_method=method.value,
                notes=(
                    f"full_position_close closed_size={position.size} "
                    f"entry_price={position.entry_price} "
                    f"close_price={close_price}"
                ),
            ),
        )

    def _is_closing_trade(self, position: DerivativePosition, trade: Trade) -> bool:
        """Check if trade is closing/reducing the position.

        Args:
            position: Current position
            trade: Trade being executed

        Returns:
            True if trade reduces position size
        """
        if position.size > Decimal(0):
            # Long position - sell orders close/reduce
            return trade.side == OrderSide.SELL
        # Short position - buy orders close/reduce
        return trade.side == OrderSide.BUY

    def _get_position_size_change(self, trade: Trade) -> Decimal:
        """Get the position size change from a trade.

        Args:
            trade: Trade being executed

        Returns:
            Position size change (positive for long, negative for short)
        """
        if trade.side == OrderSide.BUY:
            return trade.quantity  # Positive for long positions
        return -trade.quantity  # Negative for short positions

    def _calculate_new_average_price(
        self,
        current_size: Decimal,
        current_price: Decimal,
        trade_quantity: Decimal,
        trade_price: Decimal,
    ) -> Decimal:
        """Calculate new weighted average entry price when position increases.

        Args:
            current_size: Current position size (can be negative for short)
            current_price: Current average entry price
            trade_quantity: Quantity of the new trade
            trade_price: Price of the new trade

        Returns:
            New weighted average entry price
        """
        # Use absolute values for calculation to handle both long and short positions
        abs_current_size = abs(current_size)
        total_value = (abs_current_size * current_price) + (trade_quantity * trade_price)
        total_size = abs_current_size + trade_quantity

        return total_value / total_size

    def _calculate_closing_pnl(
        self,
        position: DerivativePosition,
        trade: Trade,
        closing_quantity: Decimal,
    ) -> Decimal:
        """Calculate realized P&L when closing/reducing a position.

        This implements the corrected logic that fixes the short position
        calculation errors from the original implementation.

        Args:
            position: Current position
            trade: Closing trade
            closing_quantity: Quantity being closed

        Returns:
            Realized P&L from the closing trade
        """
        entry_price = position.entry_price or Decimal(0)
        exit_price = trade.price

        if position.size > Decimal(0):
            # Long position: P&L = (exit_price - entry_price) * quantity
            pnl = (exit_price - entry_price) * closing_quantity
        else:
            # Short position: P&L = (entry_price - exit_price) * quantity
            # This is the CORRECTED calculation that was wrong in the original
            pnl = (entry_price - exit_price) * closing_quantity

        return pnl

    async def validate_input(self, input_data: RealizedPnLInput) -> tuple[bool, list[str]]:
        """Validate input data for realized P&L calculation.

        Args:
            input_data: Input to validate

        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors: list[str] = []

        if input_data.trade.quantity <= Decimal(0):
            errors.append(f"Invalid trade quantity: {input_data.trade.quantity}")

        if input_data.trade.price <= Decimal(0):
            errors.append(f"Invalid trade price: {input_data.trade.price}")

        if input_data.position.symbol != input_data.trade.symbol:
            errors.append("Position and trade symbols must match")

        return len(errors) == 0, errors

    def validate_inputs(self, position: DerivativePosition, trade: Trade, **kwargs: object) -> None:
        """Validate inputs for P&L calculation.

        Args:
            position: Position to validate
            trade: Trade to validate
            **kwargs: Additional parameters

        Raises:
            ValueError: If inputs are invalid
        """
        if trade.quantity <= Decimal(0):
            raise ValueError

        if trade.price <= Decimal(0):
            raise ValueError

        if position.entry_price is not None and position.entry_price <= Decimal(0):
            raise ValueError

        # Validate that trade symbol matches position symbol
        # Both DerivativePosition and Trade are guaranteed to have symbol by type contract
        if position.symbol != trade.symbol:
            raise ValueError
