"""Check-specific exceptions for the risk management module.

These exceptions provide specialized error handling for different types of
opportunity checks, following the pattern established in core exceptions.
"""

from typing import Any

from cyberdelta.core.risk.exceptions.base_exceptions import RiskCheckError


class OpportunityCheckError(RiskCheckError):
    """Exception raised when opportunity checking fails."""


class RequiredFieldsError(OpportunityCheckError):
    """Exception raised when required fields are missing or invalid."""

    def __init__(
        self,
        message: str,
        *,
        missing_fields: list[str] | None = None,
        invalid_fields: dict[str, str] | None = None,
        checker_name: str | None = None,
        check_type: str | None = None,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
        symbol: str | None = None,
        exchange: str | None = None,
    ) -> None:
        """Initialize required fields error.

        Args:
            message: Human-readable error description
            missing_fields: List of fields that are missing
            invalid_fields: Dictionary of field names to error descriptions
            checker_name: Name of the checker that failed
            check_type: Type of check that failed
            metadata: Additional error context
            original_exception: The underlying exception
            symbol: Trading symbol associated with the error
            exchange: Exchange name associated with the error
        """
        self.missing_fields = missing_fields or []
        self.invalid_fields = invalid_fields or {}

        # Add field validation context to metadata
        if metadata is None:
            metadata = {}
        if missing_fields:
            metadata["missing_fields"] = missing_fields
        if invalid_fields:
            metadata["invalid_fields"] = invalid_fields

        super().__init__(
            message,
            checker_name=checker_name,
            check_type=check_type,
            metadata=metadata,
            original_exception=original_exception,
            symbol=symbol,
            exchange=exchange,
        )


class ProfitabilityError(OpportunityCheckError):
    """Exception raised when profitability check fails."""

    def __init__(
        self,
        message: str,
        *,
        profit_usd: float | None = None,
        min_profit_usd: float | None = None,
        profit_percentage: float | None = None,
        checker_name: str | None = None,
        check_type: str | None = None,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
        symbol: str | None = None,
        exchange: str | None = None,
    ) -> None:
        """Initialize profitability error.

        Args:
            message: Human-readable error description
            profit_usd: Calculated profit in USD
            min_profit_usd: Minimum required profit in USD
            profit_percentage: Calculated profit percentage
            checker_name: Name of the checker that failed
            check_type: Type of check that failed
            metadata: Additional error context
            original_exception: The underlying exception
            symbol: Trading symbol associated with the error
            exchange: Exchange name associated with the error
        """
        self.profit_usd = profit_usd
        self.min_profit_usd = min_profit_usd
        self.profit_percentage = profit_percentage

        # Add profitability context to metadata
        if metadata is None:
            metadata = {}
        if profit_usd is not None:
            metadata["profit_usd"] = profit_usd
        if min_profit_usd is not None:
            metadata["min_profit_usd"] = min_profit_usd
        if profit_percentage is not None:
            metadata["profit_percentage"] = profit_percentage

        super().__init__(
            message,
            checker_name=checker_name,
            check_type=check_type,
            metadata=metadata,
            original_exception=original_exception,
            symbol=symbol,
            exchange=exchange,
        )


class CircuitBreakerError(OpportunityCheckError):
    """Exception raised when circuit breaker check fails."""

    def __init__(
        self,
        message: str,
        *,
        breaker_state: str | None = None,
        trigger_count: int | None = None,
        time_remaining: float | None = None,
        checker_name: str | None = None,
        check_type: str | None = None,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
        symbol: str | None = None,
        exchange: str | None = None,
    ) -> None:
        """Initialize circuit breaker error.

        Args:
            message: Human-readable error description
            breaker_state: Current state of the circuit breaker
            trigger_count: Number of consecutive failures
            time_remaining: Time remaining until reset (in seconds)
            checker_name: Name of the checker that failed
            check_type: Type of check that failed
            metadata: Additional error context
            original_exception: The underlying exception
            symbol: Trading symbol associated with the error
            exchange: Exchange name associated with the error
        """
        self.breaker_state = breaker_state
        self.trigger_count = trigger_count
        self.time_remaining = time_remaining

        # Add circuit breaker context to metadata
        if metadata is None:
            metadata = {}
        if breaker_state:
            metadata["breaker_state"] = breaker_state
        if trigger_count is not None:
            metadata["trigger_count"] = trigger_count
        if time_remaining is not None:
            metadata["time_remaining"] = time_remaining

        super().__init__(
            message,
            checker_name=checker_name,
            check_type=check_type,
            metadata=metadata,
            original_exception=original_exception,
            symbol=symbol,
            exchange=exchange,
        )


class PriceSanityError(OpportunityCheckError):
    """Exception raised when price sanity check fails."""

    # Predefined error messages
    INVALID_TYPE_CONVERSION = "Cannot convert value to Decimal"

    def __init__(
        self,
        message: str,
        *,
        price_value: float | None = None,
        price_bounds: tuple[float, float] | None = None,
        price_deviation: float | None = None,
        checker_name: str | None = None,
        check_type: str | None = None,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
        symbol: str | None = None,
        exchange: str | None = None,
    ) -> None:
        """Initialize price sanity error.

        Args:
            message: Human-readable error description
            price_value: Price value that failed validation
            price_bounds: Valid price bounds (min, max)
            price_deviation: Price deviation from expected range
            checker_name: Name of the checker that failed
            check_type: Type of check that failed
            metadata: Additional error context
            original_exception: The underlying exception
            symbol: Trading symbol associated with the error
            exchange: Exchange name associated with the error
        """
        self.price_value = price_value
        self.price_bounds = price_bounds
        self.price_deviation = price_deviation

        # Add price validation context to metadata
        if metadata is None:
            metadata = {}
        if price_value is not None:
            metadata["price_value"] = price_value
        if price_bounds is not None:
            metadata["price_bounds"] = {
                "min": price_bounds[0],
                "max": price_bounds[1],
            }
        if price_deviation is not None:
            metadata["price_deviation"] = price_deviation

        super().__init__(
            message,
            checker_name=checker_name,
            check_type=check_type,
            metadata=metadata,
            original_exception=original_exception,
            symbol=symbol,
            exchange=exchange,
        )


class FundingRateError(OpportunityCheckError):
    """Exception raised when funding rate check fails."""

    def __init__(
        self,
        message: str,
        *,
        funding_rate: float | None = None,
        funding_rate_bounds: tuple[float, float] | None = None,
        spread: float | None = None,
        checker_name: str | None = None,
        check_type: str | None = None,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
        symbol: str | None = None,
        exchange: str | None = None,
    ) -> None:
        """Initialize funding rate error.

        Args:
            message: Human-readable error description
            funding_rate: Funding rate that failed validation
            funding_rate_bounds: Valid funding rate bounds (min, max)
            spread: Funding rate spread between exchanges
            checker_name: Name of the checker that failed
            check_type: Type of check that failed
            metadata: Additional error context
            original_exception: The underlying exception
            symbol: Trading symbol associated with the error
            exchange: Exchange name associated with the error
        """
        self.funding_rate = funding_rate
        self.funding_rate_bounds = funding_rate_bounds
        self.spread = spread

        # Add funding rate context to metadata
        if metadata is None:
            metadata = {}
        if funding_rate is not None:
            metadata["funding_rate"] = funding_rate
        if funding_rate_bounds is not None:
            metadata["funding_rate_bounds"] = {
                "min": funding_rate_bounds[0],
                "max": funding_rate_bounds[1],
            }
        if spread is not None:
            metadata["spread"] = spread

        super().__init__(
            message,
            checker_name=checker_name,
            check_type=check_type,
            metadata=metadata,
            original_exception=original_exception,
            symbol=symbol,
            exchange=exchange,
        )


class VolatilityError(OpportunityCheckError):
    """Exception raised when volatility check fails."""

    # Predefined error messages
    INVALID_TYPE_CONVERSION = "Cannot convert value to Decimal"

    def __init__(
        self,
        message: str,
        *,
        volatility_value: float | None = None,
        volatility_bounds: tuple[float, float] | None = None,
        market_regime: str | None = None,
        checker_name: str | None = None,
        check_type: str | None = None,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
        symbol: str | None = None,
        exchange: str | None = None,
    ) -> None:
        """Initialize volatility error.

        Args:
            message: Human-readable error description
            volatility_value: Volatility value that failed validation
            volatility_bounds: Valid volatility bounds (min, max)
            market_regime: Current market regime (normal, high_volatility)
            checker_name: Name of the checker that failed
            check_type: Type of check that failed
            metadata: Additional error context
            original_exception: The underlying exception
            symbol: Trading symbol associated with the error
            exchange: Exchange name associated with the error
        """
        self.volatility_value = volatility_value
        self.volatility_bounds = volatility_bounds
        self.market_regime = market_regime

        # Add volatility context to metadata
        if metadata is None:
            metadata = {}
        if volatility_value is not None:
            metadata["volatility_value"] = volatility_value
        if volatility_bounds is not None:
            metadata["volatility_bounds"] = {
                "min": volatility_bounds[0],
                "max": volatility_bounds[1],
            }
        if market_regime:
            metadata["market_regime"] = market_regime

        super().__init__(
            message,
            checker_name=checker_name,
            check_type=check_type,
            metadata=metadata,
            original_exception=original_exception,
            symbol=symbol,
            exchange=exchange,
        )


class ExchangeBalanceError(OpportunityCheckError):
    """Exception raised when exchange balance check fails."""

    def __init__(
        self,
        message: str,
        *,
        current_balance: float | None = None,
        required_balance: float | None = None,
        balance_ratio: float | None = None,
        checker_name: str | None = None,
        check_type: str | None = None,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
        symbol: str | None = None,
        exchange: str | None = None,
    ) -> None:
        """Initialize exchange balance error.

        Args:
            message: Human-readable error description
            current_balance: Current account balance
            required_balance: Required minimum balance
            balance_ratio: Current balance ratio
            checker_name: Name of the checker that failed
            check_type: Type of check that failed
            metadata: Additional error context
            original_exception: The underlying exception
            symbol: Trading symbol associated with the error
            exchange: Exchange name associated with the error
        """
        self.current_balance = current_balance
        self.required_balance = required_balance
        self.balance_ratio = balance_ratio

        # Add balance context to metadata
        if metadata is None:
            metadata = {}
        if current_balance is not None:
            metadata["current_balance"] = current_balance
        if required_balance is not None:
            metadata["required_balance"] = required_balance
        if balance_ratio is not None:
            metadata["balance_ratio"] = balance_ratio

        super().__init__(
            message,
            checker_name=checker_name,
            check_type=check_type,
            metadata=metadata,
            original_exception=original_exception,
            symbol=symbol,
            exchange=exchange,
        )
