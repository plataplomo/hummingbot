"""Data integrity and consistency exceptions for portfolio management."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any, Unpack

from cyberdelta.core.infrastructure.exceptions.base import CoreError
from cyberdelta.core.portfolio.exceptions.service import ServiceError


if TYPE_CHECKING:
    from typing_extensions import TypedDict

    class ExceptionKwargs(TypedDict, total=False):
        """Typed dictionary for exception kwargs."""

        error_code: str | None
        context: dict[str, Any] | None
        recoverable: bool


class PortfolioIntegrityError(CoreError):
    """Base exception for portfolio data integrity and consistency errors."""

    def _get_default_error_code(self) -> str:
        """Get default error code for validation exceptions.

        Returns:
            str: Default error code in format 'VALID_<CLASSNAME>'
        """
        return f"VALID_{self.__class__.__name__.upper()}"


class MalformedTradeError(PortfolioIntegrityError):
    """Raised when trade data is malformed or inconsistent."""

    def __init__(
        self,
        message: str,
        trade_id: str | None = None,
        field: str | None = None,
        value: Decimal | float | str | None = None,
        integrity_errors: list[dict[str, Any]] | None = None,
        field_name: str | None = None,
        field_value: str | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize trade validation exception.

        Args:
            message: Error message
            trade_id: Trade ID
            field: Field that failed validation
            value: Invalid value
            integrity_errors: List of integrity validation errors
            field_name: Alternative field name (for validators)
            field_value: Alternative field value (for validators)
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "trade_id": trade_id,
            "field": field or field_name,
            "value": str(value) if value is not None else field_value,
            "integrity_errors": integrity_errors or [],
        })
        kwargs["context"] = context
        kwargs["error_code"] = "VALID_TRADE_FAILED"
        super().__init__(message, **kwargs)


class InvalidPositionError(PortfolioIntegrityError):
    """Raised when position data is invalid or inconsistent."""

    def __init__(
        self,
        message: str,
        position_id: str | None = None,
        exchange_id: str | None = None,
        symbol: str | None = None,
        reason: str | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize position validation exception.

        Args:
            message: Error message
            position_id: Position ID
            exchange_id: Exchange ID
            symbol: Trading symbol
            reason: Validation failure reason
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "position_id": position_id,
            "exchange_id": exchange_id,
            "symbol": symbol,
            "reason": reason,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "VALID_POSITION_FAILED"
        super().__init__(message, **kwargs)


class BalanceDiscrepancyError(PortfolioIntegrityError):
    """Raised when balance data shows discrepancies or inconsistencies."""

    def __init__(
        self,
        message: str,
        exchange_id: str | None = None,
        asset: str | None = None,
        expected_balance: str | None = None,
        actual_balance: str | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize balance validation exception.

        Args:
            message: Error message
            exchange_id: Exchange ID
            asset: Asset/currency
            expected_balance: Expected balance
            actual_balance: Actual balance
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "exchange_id": exchange_id,
            "asset": asset,
            "expected_balance": expected_balance,
            "actual_balance": actual_balance,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "VALID_BALANCE_FAILED"
        super().__init__(message, **kwargs)


class OrderConstraintError(PortfolioIntegrityError):
    """Raised when order violates constraints or business rules."""

    def __init__(
        self,
        message: str,
        order_id: str | None = None,
        order_type: str | None = None,
        constraint_violations: list[str] | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize order validation exception.

        Args:
            message: Error message
            order_id: Order ID
            order_type: Type of order
            constraint_violations: List of constraint violations
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "order_id": order_id,
            "order_type": order_type,
            "constraint_violations": constraint_violations or [],
        })
        kwargs["context"] = context
        kwargs["error_code"] = "VALID_ORDER_FAILED"
        super().__init__(message, **kwargs)


class DataCorruptionError(PortfolioIntegrityError):
    """Raised when data corruption or integrity violations are detected."""

    def __init__(
        self,
        message: str,
        data_type: str | None = None,
        integrity_check: str | None = None,
        details: dict[str, Any] | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize data integrity exception.

        Args:
            message: Error message
            data_type: Type of data
            integrity_check: Type of integrity check
            details: Additional details
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "data_type": data_type,
            "integrity_check": integrity_check,
            "details": details or {},
        })
        kwargs["context"] = context
        kwargs["recoverable"] = False  # Data integrity issues are critical
        kwargs["error_code"] = "VALID_DATA_INTEGRITY"
        super().__init__(message, **kwargs)


class ConfigurationError(PortfolioIntegrityError):
    """Raised when configuration is invalid or incompatible."""

    def __init__(
        self,
        message: str,
        config_section: str | None = None,
        invalid_fields: list[str] | None = None,
        key: str | None = None,
        value: str | None = None,
        valid_values: list[str] | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize configuration validation exception.

        Args:
            message: Error message
            config_section: Configuration section
            invalid_fields: List of invalid fields
            key: Configuration key
            value: Invalid value
            valid_values: List of valid values
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "config_section": config_section,
            "invalid_fields": invalid_fields or [],
            "key": key,
            "value": value,
            "valid_values": valid_values,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "VALID_CONFIG_FAILED"
        super().__init__(message, **kwargs)


# Specific validation exceptions without message parameters to comply with TRY003
class EmptySymbolError(MalformedTradeError):
    """Raised when symbol is empty."""

    def __init__(self, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize empty symbol exception."""
        super().__init__("Symbol cannot be empty", field_name="symbol", field_value="", **kwargs)


class InvalidSymbolLengthError(MalformedTradeError):
    """Raised when symbol length is invalid."""

    def __init__(self, symbol: str, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize invalid symbol length exception."""
        super().__init__(
            "Symbol must be 2-20 characters", field_name="symbol", field_value=symbol, **kwargs
        )


class InvalidSymbolCharactersError(MalformedTradeError):
    """Raised when symbol contains invalid characters."""

    def __init__(self, symbol: str, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize invalid symbol characters exception."""
        super().__init__(
            "Symbol contains invalid characters", field_name="symbol", field_value=symbol, **kwargs
        )


class EmptyAssetError(MalformedTradeError):
    """Raised when asset is empty."""

    def __init__(self, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize empty asset exception."""
        super().__init__("Asset cannot be empty", field_name="asset", field_value="", **kwargs)


class InvalidAssetLengthError(MalformedTradeError):
    """Raised when asset length is invalid."""

    def __init__(self, asset: str, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize invalid asset length exception."""
        super().__init__(
            "Asset must be 1-10 characters", field_name="asset", field_value=asset, **kwargs
        )


class InvalidAssetCharactersError(MalformedTradeError):
    """Raised when asset is not alphanumeric."""

    def __init__(self, asset: str, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize invalid asset characters exception."""
        super().__init__(
            "Asset must be alphanumeric", field_name="asset", field_value=asset, **kwargs
        )


class InvalidExchangeError(ConfigurationError):
    """Raised when exchange name is invalid."""

    def __init__(
        self, exchange: str, valid_exchanges: set[str], **kwargs: Unpack[ExceptionKwargs]
    ) -> None:
        """Initialize invalid exchange exception."""
        super().__init__(
            f"Invalid exchange: {exchange}. Must be one of {valid_exchanges}",
            key="exchange",
            value=exchange,
            valid_values=list(valid_exchanges),
            **kwargs,
        )


class InvalidTimestampRangeError(MalformedTradeError):
    """Raised when timestamp is outside valid range."""

    def __init__(self, timestamp: float, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize invalid timestamp exception."""
        super().__init__(
            f"Timestamp {timestamp} is outside valid range",
            field_name="timestamp",
            field_value=str(timestamp),
            **kwargs,
        )


class EmptyListError(ConfigurationError):
    """Raised when a list that should not be empty is empty."""

    def __init__(self, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize empty list exception."""
        super().__init__("List cannot be empty", key="list", value="[]", **kwargs)


class NonUniqueListError(ConfigurationError):
    """Raised when a list contains non-unique elements."""

    def __init__(self, items: list[Any], **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize non-unique list exception."""
        super().__init__(
            "List must contain unique elements", key="list", value=str(items), **kwargs
        )


class PositionSizeTooLargeError(MalformedTradeError):
    """Raised when position size exceeds maximum."""

    def __init__(self, size: float, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize position size too large exception."""
        super().__init__(
            "Position size too large", field_name="position_size", field_value=str(size), **kwargs
        )


class InvalidCacheSizeError(ConfigurationError):
    """Raised when cache size is out of bounds."""

    def __init__(self, size: int, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize invalid cache size exception."""
        super().__init__(
            "Cache size must be between 10 and 1,000,000",
            key="cache_size",
            value=str(size),
            **kwargs,
        )


class InvalidTTLError(ConfigurationError):
    """Raised when TTL is out of bounds."""

    def __init__(self, ttl: float, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize invalid TTL exception."""
        super().__init__(
            "TTL must be between 1 second and 30 days", key="ttl_seconds", value=str(ttl), **kwargs
        )


class InvalidURLFormatError(ConfigurationError):
    """Raised when URL format is invalid."""

    def __init__(self, url: str, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize invalid URL format exception."""
        super().__init__(
            "URL must start with http://, https://, ws://, or wss://",
            key="url",
            value=url,
            **kwargs,
        )


class InvalidMarketHoursFormatError(ConfigurationError):
    """Raised when market hours format is invalid."""

    def __init__(self, hours: str, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize invalid market hours format exception."""
        super().__init__(
            "Market hours must be in format HH:MM-HH:MM", key="market_hours", value=hours, **kwargs
        )


class CacheSizeMustBePositiveError(ConfigurationError):
    """Raised when cache size is not positive."""

    def __init__(self, size: int, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize cache size must be positive exception."""
        super().__init__("Cache size must be positive", key="max_size", value=str(size), **kwargs)


class CalculatorCreationError(ConfigurationError):
    """Raised when calculator creation fails."""

    def __init__(self, error_details: str, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize calculator creation exception."""
        super().__init__(
            "Calculator creation failed",
            config_section="calculator",
            invalid_fields=[error_details],
            **kwargs,
        )


class ConfigurationValidationError(ConfigurationError):
    """Raised when configuration validation fails."""

    def __init__(self, critical_errors: list[str], **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize configuration validation exception."""
        super().__init__(
            f"Configuration validation failed: {'; '.join(critical_errors)}",
            config_section="startup",
            invalid_fields=critical_errors,
            **kwargs,
        )


# Balance validation exceptions
class EmptyBalanceFieldError(PortfolioIntegrityError):
    """Raised when a required balance field is empty."""

    def __init__(self, field_name: str, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize empty balance field exception."""
        super().__init__(f"Balance field '{field_name}' cannot be empty", **kwargs)


class NonFiniteBalanceError(PortfolioIntegrityError):
    """Raised when balance amount is not finite."""

    def __init__(self, field_name: str = "balance", **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize non-finite balance exception."""
        super().__init__(f"{field_name} amounts must be finite", **kwargs)


class NegativeBalanceError(PortfolioIntegrityError):
    """Raised when balance amount is negative."""

    def __init__(self, field_name: str = "balance", **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize negative balance exception."""
        super().__init__(f"{field_name} amounts cannot be negative", **kwargs)


# Position validation exceptions
class EmptyPositionFieldError(InvalidPositionError):
    """Raised when a required position field is empty."""

    def __init__(self, field_name: str, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize empty position field exception."""
        super().__init__(
            f"Position field '{field_name}' cannot be empty", reason="empty_field", **kwargs
        )


class InvalidPositionSideError(InvalidPositionError):
    """Raised when position side is invalid."""

    def __init__(self, side: str, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize invalid position side exception."""
        super().__init__("Side must be LONG or SHORT", reason=f"invalid_side: {side}", **kwargs)


class NonFinitePriceError(PortfolioIntegrityError):
    """Raised when price is not finite."""

    def __init__(self, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize non-finite price exception."""
        super().__init__("Prices must be finite", **kwargs)


class NonPositivePriceError(PortfolioIntegrityError):
    """Raised when price is not positive."""

    def __init__(self, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize non-positive price exception."""
        super().__init__("Prices must be positive", **kwargs)


class NonFiniteFinancialValueError(PortfolioIntegrityError):
    """Raised when financial value is not finite."""

    def __init__(self, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize non-finite financial value exception."""
        super().__init__("Financial values must be finite", **kwargs)


class NonFiniteMarginError(PortfolioIntegrityError):
    """Raised when margin value is not finite."""

    def __init__(self, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize non-finite margin exception."""
        super().__init__("Margin used must be finite", **kwargs)


class NegativeMarginError(PortfolioIntegrityError):
    """Raised when margin value is negative."""

    def __init__(self, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize negative margin exception."""
        super().__init__("Margin used cannot be negative", **kwargs)


class NonFiniteLeverageError(PortfolioIntegrityError):
    """Raised when leverage is not finite."""

    def __init__(self, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize non-finite leverage exception."""
        super().__init__("Leverage must be finite", **kwargs)


class NonPositiveLeverageError(PortfolioIntegrityError):
    """Raised when leverage is not positive."""

    def __init__(self, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize non-positive leverage exception."""
        super().__init__("Leverage must be positive", **kwargs)


# Timestamp validation exceptions
class NonPositiveTimestampError(PortfolioIntegrityError):
    """Raised when timestamp is not positive."""

    def __init__(self, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize non-positive timestamp exception."""
        super().__init__("Timestamp must be positive", **kwargs)


# Configuration value validation exceptions
class InvalidNumericStringError(ConfigurationError):
    """Raised when numeric string cannot be parsed."""

    def __init__(
        self, value: str, field_name: str, section: str, **kwargs: Unpack[ExceptionKwargs]
    ) -> None:
        """Initialize invalid numeric string exception."""
        super().__init__(
            "Invalid numeric string", config_section=section, key=field_name, value=value, **kwargs
        )


class NonFiniteConfigValueError(ConfigurationError):
    """Raised when configuration value is not finite."""

    def __init__(
        self, value: str, field_name: str, section: str, **kwargs: Unpack[ExceptionKwargs]
    ) -> None:
        """Initialize non-finite config value exception."""
        super().__init__(
            "Value must be finite", config_section=section, key=field_name, value=value, **kwargs
        )


class NonPositiveConfigValueError(ConfigurationError):
    """Raised when configuration value must be positive but isn't."""

    def __init__(
        self, value: str, field_name: str, section: str, **kwargs: Unpack[ExceptionKwargs]
    ) -> None:
        """Initialize non-positive config value exception."""
        super().__init__(
            "Value must be positive", config_section=section, key=field_name, value=value, **kwargs
        )


class ConfigValueTooLargeError(ConfigurationError):
    """Raised when configuration value exceeds maximum."""

    def __init__(
        self,
        value: str,
        field_name: str,
        section: str,
        max_value: str,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize config value too large exception."""
        super().__init__(
            f"Value too large (max {max_value})",
            config_section=section,
            key=field_name,
            value=value,
            **kwargs,
        )


class ConfigValueTooSmallError(ConfigurationError):
    """Raised when configuration value is below minimum."""

    def __init__(
        self,
        value: str,
        field_name: str,
        section: str,
        min_value: str,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize config value too small exception."""
        super().__init__(
            f"Value too small (min {min_value})",
            config_section=section,
            key=field_name,
            value=value,
            **kwargs,
        )


class InvalidConfigChoiceError(ConfigurationError):
    """Raised when configuration value is not one of valid choices."""

    def __init__(
        self,
        value: str,
        field_name: str,
        section: str,
        valid_choices: list[str],
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize invalid config choice exception."""
        super().__init__(
            f"Must be one of: {', '.join(valid_choices)}",
            config_section=section,
            key=field_name,
            value=value,
            valid_values=valid_choices,
            **kwargs,
        )


class NegativeConfigValueError(ConfigurationError):
    """Raised when configuration value cannot be negative."""

    def __init__(
        self, value: str, field_name: str, section: str, **kwargs: Unpack[ExceptionKwargs]
    ) -> None:
        """Initialize negative config value exception."""
        super().__init__(
            "Value cannot be negative",
            config_section=section,
            key=field_name,
            value=value,
            **kwargs,
        )


class ConfigPrecisionTooHighError(ConfigurationError):
    """Raised when precision configuration is too high."""

    def __init__(
        self,
        value: str,
        field_name: str,
        section: str,
        max_precision: int = 18,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize precision too high exception."""
        super().__init__(
            f"Precision too high (max {max_precision})",
            config_section=section,
            key=field_name,
            value=value,
            **kwargs,
        )


# Service-specific exceptions
class EmptyServiceNameError(ServiceError):
    """Raised when service name is empty."""

    def __init__(self, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize empty service name exception."""
        super().__init__("Service name cannot be empty", **kwargs)


class InvalidServiceTimeoutError(ServiceError):
    """Raised when service timeout is invalid."""

    def __init__(
        self,
        timeout: float,
        min_timeout: float = 0,
        max_timeout: float = 3600,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize invalid service timeout exception."""
        super().__init__(
            f"Timeout must be between {min_timeout} and {max_timeout} seconds", **kwargs
        )


# NOTE: ContainerSizeLimitExceededError moved to state.py to avoid circular imports
