"""Position data screening and validation components."""

from __future__ import annotations

import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, NamedTuple

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.screening.base.base_screener import BaseScreener


if TYPE_CHECKING:
    from cyberdelta.core.models import DerivativePosition

logger = get_logger(__name__)

# Constants
MIN_POSITION_ID_LENGTH = 1
MAX_POSITION_ID_LENGTH = 100
MIN_SYMBOL_LENGTH = 2
MAX_SYMBOL_LENGTH = 20
MIN_EXCHANGE_ID_LENGTH = 2
MAX_EXCHANGE_ID_LENGTH = 50
DECIMAL_PRECISION_OFFSET = -8


class PositionValidationResult(NamedTuple):
    """Result of position validation."""

    is_valid: bool
    errors: list[str]
    warnings: list[str]
    cleaned_position: DerivativePosition | None = None


class PositionDataScreener(BaseScreener):
    """Validates and screens position data before processing.

    Provides comprehensive validation of derivative position data
    including size, entry price, and position metadata validation.
    """

    def __init__(
        self,
        name: str = "PositionDataScreener",
        config: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the position data screener.

        Args:
            name: Screener name
            config: Configuration dictionary
        """
        cfg = config or {}
        super().__init__(name, config)

        # Position validation configuration
        self.allow_zero_positions = cfg.get("allow_zero_positions", True)
        self.allow_negative_entry_prices = cfg.get("allow_negative_entry_prices", False)
        self.max_position_size = Decimal(cfg.get("max_position_size", "1000000"))
        self.max_entry_price = Decimal(cfg.get("max_entry_price", "1000000"))
        self.min_entry_price = Decimal(cfg.get("min_entry_price", "0.00001"))
        self.max_position_value = Decimal(cfg.get("max_position_value", "100000000"))

        # Symbol validation
        self.symbol_validation_enabled = cfg.get("symbol_validation_enabled", True)
        self.allowed_symbols = cfg.get("allowed_symbols", [])

        # Exchange validation
        self.require_exchange_id = cfg.get("require_exchange_id", True)
        self.allowed_exchanges = cfg.get("allowed_exchanges", [])

        # Position metadata validation
        self.require_position_id = cfg.get("require_position_id", True)
        self.validate_timestamps = cfg.get("validate_timestamps", True)

        logger.info(
            "position_data_screener_created",
            screener_name=name,
            allow_zero_positions=self.allow_zero_positions,
            allow_negative_entry_prices=self.allow_negative_entry_prices,
            max_position_size=self.max_position_size,
        )

    async def _initialize_internal(self) -> None:
        """Initialize internal state."""
        logger.info("position_data_screener_initializing")

    async def _shutdown_internal(self) -> None:
        """Shutdown internal state."""
        logger.info("position_data_screener_shutting_down")

    async def validate_position(
        self, position: DerivativePosition | None
    ) -> PositionValidationResult:
        """Validate position data comprehensively.

        Args:
            position: Position object to validate

        Returns:
            PositionValidationResult with validation details
        """
        self._ensure_initialized()

        errors: list[str] = []
        warnings: list[str] = []

        # Basic structure validation
        if position is None:
            errors.append("Position object is None")
            return PositionValidationResult(
                is_valid=False,
                errors=errors,
                warnings=warnings,
            )

        # Validate the position (already checked for None above)
        # Validate required fields
        field_errors = await self._validate_required_fields(position)
        errors.extend(field_errors)

        # Validate position ID
        if self.require_position_id:
            id_errors = await self._validate_position_id(position)
            errors.extend(id_errors)

        # Validate symbol
        if self.symbol_validation_enabled:
            symbol_errors, symbol_warnings = await self._validate_symbol(position)
            errors.extend(symbol_errors)
            warnings.extend(symbol_warnings)

        # Validate exchange
        if self.require_exchange_id:
            exchange_errors = await self._validate_exchange(position)
            errors.extend(exchange_errors)

        # Validate position size
        size_errors, size_warnings = await self._validate_position_size(position)
        errors.extend(size_errors)
        warnings.extend(size_warnings)

        # Validate entry price
        price_errors, price_warnings = await self._validate_entry_price(position)
        errors.extend(price_errors)
        warnings.extend(price_warnings)

        # Validate timestamps
        if self.validate_timestamps:
            timestamp_errors, timestamp_warnings = await self._validate_timestamps(position)
            errors.extend(timestamp_errors)
            warnings.extend(timestamp_warnings)

        # Cross-field validation
        cross_errors, cross_warnings = await self._validate_cross_fields(position)
        errors.extend(cross_errors)
        warnings.extend(cross_warnings)

        is_valid = len(errors) == 0

        # Update statistics
        self._update_validation_stats(is_valid, len(errors), len(warnings))

        if not is_valid:
            logger.warning(
                "position_validation_failed",
                symbol=position.symbol,
                exchange_id=position.exchange,
                error_count=len(errors),
                warning_count=len(warnings),
            )
        elif warnings:
            logger.info(
                "position_validation_warnings",
                symbol=position.symbol,
                exchange_id=position.exchange,
                warning_count=len(warnings),
            )

        return PositionValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            cleaned_position=position if is_valid else None,
        )

    async def sanitize_position(self, position: DerivativePosition) -> DerivativePosition:
        """Sanitize position data by cleaning common issues.

        Args:
            position: Position object to sanitize

        Returns:
            Sanitized position object
        """
        self._ensure_initialized()

        # Create a copy to avoid modifying the original
        sanitized = position

        # Clean position data directly (DerivativePosition objects have known structure)
        # Clean symbol
        if sanitized.symbol:
            sanitized.symbol = sanitized.symbol.strip().upper()

        # Clean exchange
        if sanitized.exchange:
            sanitized.exchange = sanitized.exchange.strip().lower()

        # Ensure decimal precision for size
        sanitized.size = Decimal(str(sanitized.size))

        # Clean entry_price if present
        if sanitized.entry_price is not None:
            sanitized.entry_price = Decimal(str(sanitized.entry_price))

        logger.debug(
            "position_sanitized",
            symbol=sanitized.symbol,
            exchange_id=sanitized.exchange,
        )

        return sanitized

    async def _validate_required_fields(self, position: DerivativePosition | None) -> list[str]:
        """Validate that required fields are present.

        Args:
            position: Position object

        Returns:
            List of validation errors
        """
        if position is None:
            return ["Position object is None"]

        errors: list[str] = []

        # Validate required DerivativePosition model fields directly
        if not position.symbol or not position.symbol.strip():
            errors.append("Required field symbol is missing or empty")

        # DerivativePosition model guarantees size is not None and is Decimal
        # Only validate business logic constraints

        if not position.exchange or not position.exchange.strip():
            errors.append("Required field exchange is missing or empty")

        # entry_price can be None (it's Optional in the model), no string validation needed
        # (model ensures proper types)

        return errors

    async def _validate_position_id(self, position: DerivativePosition | None) -> list[str]:
        """Validate position ID if present.

        Args:
            position: Position object

        Returns:
            List of validation errors
        """
        if position is None:
            return ["Position object is None"]

        errors: list[str] = []

        # DerivativePosition model has symbol field as required ID
        # No separate position_id field needed
        # Symbol field is validated by the model itself

        return errors

    async def _validate_symbol(
        self, position: DerivativePosition | None
    ) -> tuple[list[str], list[str]]:
        """Validate trading symbol format.

        Args:
            position: Position object

        Returns:
            Tuple of (errors, warnings)
        """
        if position is None:
            return (["Position object is None"], [])
        errors: list[str] = []
        warnings: list[str] = []

        # Check symbol (DerivativePosition model has 'symbol' field)
        if not position.symbol:
            errors.append("Symbol is required")
            return errors, warnings

        symbol = str(position.symbol).strip().upper()

        # Basic format validation
        if len(symbol) < MIN_SYMBOL_LENGTH:
            errors.append("Symbol too short (minimum 2 characters)")
        elif len(symbol) > MAX_SYMBOL_LENGTH:
            errors.append("Symbol too long (maximum 20 characters)")

        # Character validation
        if not symbol.replace("-", "").replace("/", "").replace("_", "").isalnum():
            errors.append("Symbol contains invalid characters")

        # Allowed symbols validation
        if self.allowed_symbols and symbol not in self.allowed_symbols:
            errors.append(f"Symbol {symbol} not in allowed list")

        # Common format warnings
        if not any(sep in symbol for sep in ["-", "/", "_"]):
            warnings.append("Symbol may not be a trading pair (no separator found)")

        return errors, warnings

    async def _validate_exchange(self, position: DerivativePosition | None) -> list[str]:
        """Validate exchange ID.

        Args:
            position: Position object

        Returns:
            List of validation errors
        """
        if position is None:
            return ["Position object is None"]

        errors: list[str] = []

        # Check exchange (DerivativePosition model has 'exchange' field)
        if not position.exchange:
            errors.append("Exchange is required")
            return errors

        exchange_id = str(position.exchange).strip().lower()

        # Allowed exchanges validation
        if self.allowed_exchanges and exchange_id not in self.allowed_exchanges:
            errors.append(f"Exchange {exchange_id} not in allowed list: {self.allowed_exchanges}")

        # Basic format validation
        if len(exchange_id) < MIN_EXCHANGE_ID_LENGTH:
            errors.append("Exchange ID too short")
        elif len(exchange_id) > MAX_EXCHANGE_ID_LENGTH:
            errors.append("Exchange ID too long")

        return errors

    async def _validate_position_size(
        self, position: DerivativePosition | None
    ) -> tuple[list[str], list[str]]:
        """Validate position size.

        Args:
            position: Position object

        Returns:
            Tuple of (errors, warnings)
        """
        if position is None:
            return (["Position object is None"], [])

        errors: list[str] = []
        warnings: list[str] = []

        # Check size (DerivativePosition model guarantees size is not None and is Decimal)
        # size is already a Decimal from the DerivativePosition model
        size = position.size

        # Zero position validation
        if size == 0 and not self.allow_zero_positions:
            errors.append("Zero positions are not allowed")

        # Range validation
        if abs(size) > self.max_position_size:
            errors.append(f"Position size {size} exceeds maximum {self.max_position_size}")

        # Precision warnings
        exponent = size.as_tuple().exponent
        if isinstance(exponent, int) and exponent < DECIMAL_PRECISION_OFFSET:
            warnings.append("Position size has very high precision (>8 decimal places)")

        return errors, warnings

    async def _validate_entry_price(
        self, position: DerivativePosition | None
    ) -> tuple[list[str], list[str]]:
        """Validate entry price.

        Args:
            position: Position object

        Returns:
            Tuple of (errors, warnings)
        """
        if position is None:
            return (["Position object is None"], [])

        errors: list[str] = []
        warnings: list[str] = []

        # Check entry_price (DerivativePosition model has 'entry_price' field)
        if position.entry_price is None:
            errors.append("Entry price is required")
            return errors, warnings

        try:
            price = Decimal(str(position.entry_price))
        except (ValueError, TypeError):
            errors.append("Entry price must be a valid decimal number")
            return errors, warnings

        # Zero price validation
        if price == 0:
            errors.append("Entry price cannot be zero")

        # Negative price validation
        if price < 0 and not self.allow_negative_entry_prices:
            errors.append("Negative entry prices are not allowed")

        # Range validation
        if price > 0 and price < self.min_entry_price:
            errors.append(f"Entry price {price} below minimum {self.min_entry_price}")
        elif price > self.max_entry_price:
            errors.append(f"Entry price {price} above maximum {self.max_entry_price}")

        # Precision warnings
        exponent = price.as_tuple().exponent
        if isinstance(exponent, int) and exponent < DECIMAL_PRECISION_OFFSET:
            warnings.append("Entry price has very high precision (>8 decimal places)")

        return errors, warnings

    async def _validate_timestamps(
        self, position: DerivativePosition | None
    ) -> tuple[list[str], list[str]]:
        """Validate position timestamps.

        Args:
            position: Position object

        Returns:
            Tuple of (errors, warnings)
        """
        if position is None:
            return (["Position object is None"], [])

        errors: list[str] = []
        warnings: list[str] = []

        # DerivativePosition model has timestamp field (datetime) - validate that
        current_time = time.time()
        one_year_ago = current_time - (365 * 24 * 60 * 60)
        one_day_future = current_time + (24 * 60 * 60)

        # Convert datetime to timestamp for comparison
        position_timestamp = position.timestamp.timestamp()

        if position_timestamp < one_year_ago:
            warnings.append("Position timestamp is more than one year old")
        elif position_timestamp > one_day_future:
            warnings.append("Position timestamp is more than one day in the future")

        return errors, warnings

    async def _validate_cross_fields(
        self, position: DerivativePosition | None
    ) -> tuple[list[str], list[str]]:
        """Validate relationships between fields.

        Args:
            position: Position object

        Returns:
            Tuple of (errors, warnings)
        """
        if position is None:
            return (["Position object is None"], [])

        errors: list[str] = []
        warnings: list[str] = []

        # Validate position value for position objects
        try:
            size = Decimal(str(position.size))
            if position.entry_price is not None:
                price = Decimal(str(position.entry_price))

                if size != 0 and price > 0:
                    position_value = abs(size) * price

                    # Check for extremely large position values
                    if position_value > self.max_position_value:
                        errors.append(
                            f"Position value {position_value} exceeds max {self.max_position_value}"
                        )

                    # Check for extremely small position values
                    if position_value < Decimal("0.01"):  # 1 cent
                        warnings.append(f"Very small position value: {position_value}")

        except (ValueError, TypeError):
            # Already handled in individual field validation
            pass

        # DerivativePosition model only has one timestamp field - no consistency validation needed

        return errors, warnings

    async def validate_position_list(self, positions: list[DerivativePosition]) -> dict[str, Any]:
        """Validate a list of positions and return aggregate results.

        Args:
            positions: List of positions to validate

        Returns:
            Dictionary with aggregate validation results
        """
        if not positions:
            return {
                "total_positions": 0,
                "valid_positions": 0,
                "invalid_positions": 0,
                "total_errors": 0,
                "total_warnings": 0,
                "validation_results": [],
            }

        results: list[PositionValidationResult] = []
        total_errors = 0
        total_warnings = 0
        valid_count = 0

        for position in positions:
            result = await self.validate_position(position)
            results.append(result)

            total_errors += len(result.errors)
            total_warnings += len(result.warnings)

            if result.is_valid:
                valid_count += 1

        return {
            "total_positions": len(positions),
            "valid_positions": valid_count,
            "invalid_positions": len(positions) - valid_count,
            "total_errors": total_errors,
            "total_warnings": total_warnings,
            "validation_results": results,
        }

    def get_validation_stats(self) -> dict[str, Any]:
        """Get validation statistics.

        Returns:
            Dictionary with validation statistics
        """
        base_stats = super().get_validation_stats()

        return {
            **base_stats,
            "allow_zero_positions": self.allow_zero_positions,
            "allow_negative_entry_prices": self.allow_negative_entry_prices,
            "max_position_size": str(self.max_position_size),
            "max_entry_price": str(self.max_entry_price),
            "min_entry_price": str(self.min_entry_price),
            "max_position_value": str(self.max_position_value),
            "symbol_validation_enabled": self.symbol_validation_enabled,
            "require_exchange_id": self.require_exchange_id,
            "require_position_id": self.require_position_id,
            "validate_timestamps": self.validate_timestamps,
            "allowed_symbols": self.allowed_symbols,
            "allowed_exchanges": self.allowed_exchanges,
        }
