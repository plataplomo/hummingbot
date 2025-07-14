"""Sizing-specific exceptions for the risk management module.

These exceptions provide specialized error handling for different types of
position sizing operations, following the pattern established in core exceptions.
"""

from typing import Any

from cyberdelta.core.risk.exceptions.base_exceptions import RiskSizingError


class SizingError(RiskSizingError):
    """Exception raised when position sizing fails."""

    # Predefined error messages
    UNKNOWN_SIZING_METHOD = "Unknown sizing method"
    FIXED_USD_AMOUNT_MUST_BE_POSITIVE = "Fixed USD amount must be positive"
    FIXED_FRACTION_OUT_OF_RANGE = "Fixed fraction must be between 0 and 1"
    METHOD_MUST_BE_VALID = "Method must be 'fixed_usd' or 'fixed_fraction'"
    CANNOT_CREATE_SIZED_OPPORTUNITY_FROM_FAILED_RESULT = (
        "Cannot create SizedOpportunity from failed sizing result"
    )
    MIN_SIZE_MUST_BE_LESS_THAN_MAX_SIZE = "min_size must be less than max_size"
    MIN_ALLOCATION_MUST_BE_LESS_THAN_MAX_ALLOCATION = (
        "min_allocation must be less than max_allocation"
    )
    ALLOCATION_PERCENTAGES_MUST_BE_BETWEEN_0_AND_1 = (
        "Allocation percentages must be between 0 and 1"
    )
    INVALID_TYPE_CONVERSION = "Cannot convert value to Decimal"


class KellyCalculationError(SizingError):
    """Exception raised when Kelly criterion calculation fails."""

    # Predefined error messages
    INVALID_VOLATILITY = "Invalid volatility for Kelly calculation"
    INVALID_WIN_PROBABILITY = "Invalid win probability"
    INVALID_WIN_LOSS_AMOUNTS = "Invalid win/loss amounts"
    KELLY_MULTIPLIER_OUT_OF_RANGE = "Kelly multiplier must be between 0 and 1"
    KELLY_ALLOCATION_BOUNDS_INVALID = "min_allocation must be less than max_allocation"
    NEGATIVE_RISK_FREE_RATE = "Risk-free rate must be non-negative"
    INVALID_RISK_FREE_RATE = "Invalid risk-free rate"
    INVALID_OUTCOME_PROBABILITIES = "Invalid outcome probabilities"
    BINARY_KELLY_CALCULATION_REQUIREMENTS = (
        "Binary Kelly calculation requires win_probability, win_amount, and loss_amount"
    )
    MULTI_OUTCOME_KELLY_CALCULATION_REQUIREMENTS = (
        "Multi-outcome Kelly calculation requires outcomes"
    )
    MIN_FRACTION_MUST_BE_LESS_THAN_MAX_FRACTION = "min_fraction must be less than max_fraction"
    MULTIPLIER_MUST_BE_BETWEEN_0_AND_1 = "Multiplier must be between 0 and 1"
    INVALID_EXPECTED_RETURN = "Invalid expected return"
    UNKNOWN_ADJUSTMENT_TYPE = "Unknown adjustment type"

    def __init__(
        self,
        message: str,
        *,
        win_rate: float | None = None,
        win_loss_ratio: float | None = None,
        kelly_fraction: float | None = None,
        sizing_method: str | None = None,
        position_size: float | None = None,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
        symbol: str | None = None,
        exchange: str | None = None,
    ) -> None:
        """Initialize Kelly calculation error.

        Args:
            message: Human-readable error description
            win_rate: Win rate used in calculation
            win_loss_ratio: Win/loss ratio used in calculation
            kelly_fraction: Calculated Kelly fraction
            sizing_method: Sizing method used
            position_size: Position size that caused the error
            metadata: Additional error context
            original_exception: The underlying exception
            symbol: Trading symbol associated with the error
            exchange: Exchange name associated with the error
        """
        self.win_rate = win_rate
        self.win_loss_ratio = win_loss_ratio
        self.kelly_fraction = kelly_fraction

        # Add Kelly calculation context to metadata
        if metadata is None:
            metadata = {}
        if win_rate is not None:
            metadata["win_rate"] = win_rate
        if win_loss_ratio is not None:
            metadata["win_loss_ratio"] = win_loss_ratio
        if kelly_fraction is not None:
            metadata["kelly_fraction"] = kelly_fraction

        super().__init__(
            message,
            sizing_method=sizing_method,
            position_size=position_size,
            metadata=metadata,
            original_exception=original_exception,
            symbol=symbol,
            exchange=exchange,
        )


class VolatilityCalculationError(SizingError):
    """Exception raised when volatility calculation fails."""

    # Predefined error messages
    NO_PRICE_DATA = "No price data provided"
    INSUFFICIENT_DATA_POINTS = "Insufficient data points"
    LAMBDA_OUT_OF_RANGE = "Lambda must be between 0 and 1"
    GARCH_PARAMS_NEGATIVE = "GARCH parameters must be non-negative"
    GARCH_PARAMS_UNSTABLE = "Alpha + Beta must be less than 1 for stability"
    UNSUPPORTED_METHOD = "Unsupported method"
    MIN_VOLATILITY_MUST_BE_LESS_THAN_MAX = "min_volatility must be less than max_volatility"

    def __init__(
        self,
        message: str,
        *,
        data_points: int | None = None,
        calculation_method: str | None = None,
        time_period: str | None = None,
        sizing_method: str | None = None,
        position_size: float | None = None,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
        symbol: str | None = None,
        exchange: str | None = None,
    ) -> None:
        """Initialize volatility calculation error.

        Args:
            message: Human-readable error description
            data_points: Number of data points used in calculation
            calculation_method: Method used for calculation (e.g., "historical", "ewma")
            time_period: Time period for calculation (e.g., "1d", "7d")
            sizing_method: Sizing method used
            position_size: Position size that caused the error
            metadata: Additional error context
            original_exception: The underlying exception
            symbol: Trading symbol associated with the error
            exchange: Exchange name associated with the error
        """
        self.data_points = data_points
        self.calculation_method = calculation_method
        self.time_period = time_period

        # Add volatility calculation context to metadata
        if metadata is None:
            metadata = {}
        if data_points is not None:
            metadata["data_points"] = data_points
        if calculation_method:
            metadata["calculation_method"] = calculation_method
        if time_period:
            metadata["time_period"] = time_period

        super().__init__(
            message,
            sizing_method=sizing_method,
            position_size=position_size,
            metadata=metadata,
            original_exception=original_exception,
            symbol=symbol,
            exchange=exchange,
        )


class ValidationFactorError(SizingError):
    """Exception raised when validation factor application fails."""

    # Predefined error messages
    WEIGHT_OUT_OF_RANGE = "Weight must be between 0 and 1"
    SCORE_OUT_OF_RANGE = "Score must be between 0 and 1"

    def __init__(
        self,
        message: str,
        *,
        factor_value: float | None = None,
        factor_type: str | None = None,
        original_size: float | None = None,
        adjusted_size: float | None = None,
        sizing_method: str | None = None,
        position_size: float | None = None,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
        symbol: str | None = None,
        exchange: str | None = None,
    ) -> None:
        """Initialize validation factor error.

        Args:
            message: Human-readable error description
            factor_value: Factor value that caused the error
            factor_type: Type of factor (e.g., "validation", "safety")
            original_size: Original position size before factor application
            adjusted_size: Adjusted position size after factor application
            sizing_method: Sizing method used
            position_size: Position size that caused the error
            metadata: Additional error context
            original_exception: The underlying exception
            symbol: Trading symbol associated with the error
            exchange: Exchange name associated with the error
        """
        self.factor_value = factor_value
        self.factor_type = factor_type
        self.original_size = original_size
        self.adjusted_size = adjusted_size

        # Add validation factor context to metadata
        if metadata is None:
            metadata = {}
        if factor_value is not None:
            metadata["factor_value"] = factor_value
        if factor_type:
            metadata["factor_type"] = factor_type
        if original_size is not None:
            metadata["original_size"] = original_size
        if adjusted_size is not None:
            metadata["adjusted_size"] = adjusted_size

        super().__init__(
            message,
            sizing_method=sizing_method,
            position_size=position_size,
            metadata=metadata,
            original_exception=original_exception,
            symbol=symbol,
            exchange=exchange,
        )


class InsufficientCapitalError(SizingError):
    """Exception raised when insufficient capital is available."""

    def __init__(
        self,
        message: str,
        *,
        available_capital: float | None = None,
        required_capital: float | None = None,
        shortfall: float | None = None,
        sizing_method: str | None = None,
        position_size: float | None = None,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
        symbol: str | None = None,
        exchange: str | None = None,
    ) -> None:
        """Initialize insufficient capital error.

        Args:
            message: Human-readable error description
            available_capital: Available capital for trading
            required_capital: Required capital for the operation
            shortfall: Amount of capital shortage
            sizing_method: Sizing method used
            position_size: Position size that caused the error
            metadata: Additional error context
            original_exception: The underlying exception
            symbol: Trading symbol associated with the error
            exchange: Exchange name associated with the error
        """
        self.available_capital = available_capital
        self.required_capital = required_capital
        self.shortfall = shortfall

        # Add capital context to metadata
        if metadata is None:
            metadata = {}
        if available_capital is not None:
            metadata["available_capital"] = available_capital
        if required_capital is not None:
            metadata["required_capital"] = required_capital
        if shortfall is not None:
            metadata["shortfall"] = shortfall

        super().__init__(
            message,
            sizing_method=sizing_method,
            position_size=position_size,
            metadata=metadata,
            original_exception=original_exception,
            symbol=symbol,
            exchange=exchange,
        )
