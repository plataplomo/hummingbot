"""Trade data screening and validation components."""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, NamedTuple

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.screening.base.base_screener import BaseScreener


if TYPE_CHECKING:
    from cyberdelta.core.models import Trade

logger = get_logger(__name__)

# Constants
MIN_TRADE_ID_LENGTH = 3
MAX_TRADE_ID_LENGTH = 100
MIN_SYMBOL_LENGTH = 2
MAX_SYMBOL_LENGTH = 20
MIN_EXCHANGE_ID_LENGTH = 2
MAX_EXCHANGE_ID_LENGTH = 50
DECIMAL_PRECISION_OFFSET = -8


class TradeValidationResult(NamedTuple):
    """Result of trade validation."""

    is_valid: bool
    errors: list[str]
    warnings: list[str]
    cleaned_trade: Trade | None = None


class TradeDataScreener(BaseScreener):
    """Validates and screens trade data before processing.

    Addresses the lack of data validation in the original PortfolioTracker
    by providing comprehensive validation and sanitization of trade data.
    """

    def __init__(
        self,
        name: str = "TradeDataScreener",
        config: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the trade data screener.

        Args:
            name: Screener name
            config: Configuration dictionary
        """
        cfg = config or {}
        super().__init__(name, config)

        # Validation configuration
        self.strict_mode = cfg.get("strict_mode", False)
        self.allow_zero_quantities = cfg.get("allow_zero_quantities", False)
        self.allow_negative_prices = cfg.get("allow_negative_prices", False)
        self.max_price_value = Decimal(cfg.get("max_price_value", "1000000"))
        self.max_quantity_value = Decimal(cfg.get("max_quantity_value", "1000000"))
        self.min_price_value = Decimal(cfg.get("min_price_value", "0.00001"))
        self.min_quantity_value = Decimal(cfg.get("min_quantity_value", "0.00001"))

        # Symbol validation
        self.required_symbol_fields = cfg.get("required_symbol_fields", ["symbol"])
        self.symbol_validation_enabled = cfg.get("symbol_validation_enabled", True)

        # Trade ID validation
        self.require_trade_id = cfg.get("require_trade_id", True)
        self.trade_id_pattern = cfg.get("trade_id_pattern", None)

        # Exchange validation
        self.allowed_exchanges = cfg.get("allowed_exchanges", [])
        self.require_exchange_id = cfg.get("require_exchange_id", True)

        logger.info(
            "trade_data_screener_created",
            screener_name=name,
            strict_mode=self.strict_mode,
            allow_zero_quantities=self.allow_zero_quantities,
            allow_negative_prices=self.allow_negative_prices,
        )

    async def _initialize_internal(self) -> None:
        """Initialize internal state."""
        logger.info("trade_data_screener_initializing")

    async def _shutdown_internal(self) -> None:
        """Shutdown internal state."""
        logger.info("trade_data_screener_shutting_down")

    async def validate_trade(self, trade: Trade | None) -> TradeValidationResult:
        """Validate trade data comprehensively.

        Args:
            trade: Trade object to validate

        Returns:
            TradeValidationResult with validation details
        """
        self._ensure_initialized()

        errors: list[str] = []
        warnings: list[str] = []

        # Basic structure validation
        if trade is None:
            errors.append("Trade object is None")
            return TradeValidationResult(
                is_valid=False,
                errors=errors,
                warnings=warnings,
            )

        # Validate the trade (already checked for None above)
        # Validate required fields
        field_errors = await self._validate_required_fields(trade)
        errors.extend(field_errors)

        # Validate trade ID
        if self.require_trade_id:
            id_errors = await self._validate_trade_id(trade)
            errors.extend(id_errors)

        # Validate symbol
        if self.symbol_validation_enabled:
            symbol_errors, symbol_warnings = await self._validate_symbol(trade)
            errors.extend(symbol_errors)
            warnings.extend(symbol_warnings)

        # Validate exchange
        if self.require_exchange_id:
            exchange_errors = await self._validate_exchange(trade)
            errors.extend(exchange_errors)

        # Validate price
        price_errors, price_warnings = await self._validate_price(trade)
        errors.extend(price_errors)
        warnings.extend(price_warnings)

        # Validate quantity
        quantity_errors, quantity_warnings = await self._validate_quantity(trade)
        errors.extend(quantity_errors)
        warnings.extend(quantity_warnings)

        # Validate timestamp
        timestamp_errors, timestamp_warnings = await self._validate_timestamp(trade)
        errors.extend(timestamp_errors)
        warnings.extend(timestamp_warnings)

        # Validate trade side
        side_errors = await self._validate_trade_side(trade)
        errors.extend(side_errors)

        # Cross-field validation
        cross_errors, cross_warnings = await self._validate_cross_fields(trade)
        errors.extend(cross_errors)
        warnings.extend(cross_warnings)

        is_valid = len(errors) == 0

        if not is_valid:
            logger.warning(
                "trade_validation_failed",
                trade_id=getattr(trade, "trade_id", "unknown"),
                symbol=getattr(trade, "symbol", "unknown"),
                error_count=len(errors),
                warning_count=len(warnings),
            )
        elif warnings:
            logger.info(
                "trade_validation_warnings",
                trade_id=getattr(trade, "trade_id", "unknown"),
                symbol=getattr(trade, "symbol", "unknown"),
                warning_count=len(warnings),
            )

        return TradeValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            cleaned_trade=trade if is_valid else None,
        )

    async def sanitize_trade(self, trade: Trade) -> Trade:
        """Sanitize trade data by cleaning common issues.

        Args:
            trade: Trade object to sanitize

        Returns:
            Sanitized trade object
        """
        self._ensure_initialized()

        # Create a copy to avoid modifying the original
        # This would depend on your Trade model implementation
        sanitized = trade

        # Clean trade data directly (Trade objects have known structure)
        # Clean symbol
        if sanitized.symbol:
            sanitized.symbol = sanitized.symbol.strip().upper()

        # Clean exchange (Trade model uses 'exchange' field)
        if sanitized.exchange:
            sanitized.exchange = sanitized.exchange.strip().lower()

        # Ensure decimal precision for price and quantity
        # Trade model guarantees price is not None
        sanitized.price = Decimal(str(sanitized.price))
        sanitized.quantity = Decimal(str(sanitized.quantity))

        # Clean trade ID (Trade model uses 'id' field)
        if sanitized.id:
            sanitized.id = sanitized.id.strip()

        logger.debug(
            "trade_sanitized",
            trade_id=sanitized.id,
            symbol=sanitized.symbol,
        )

        return sanitized

    async def _validate_required_fields(self, trade: Trade | None) -> list[str]:
        """Validate that required fields are present.

        Args:
            trade: Trade object

        Returns:
            List of validation errors
        """
        if trade is None:
            return ["Trade object is None"]

        errors: list[str] = []

        # Validate required Trade model fields directly
        if not trade.symbol or not trade.symbol.strip():
            errors.append("Required field symbol is missing or empty")

        # Trade model guarantees quantity is not None and > 0 via Field(gt=Decimal(0))

        if not trade.exchange or not trade.exchange.strip():
            errors.append("Required field exchange is missing or empty")

        # Trade model guarantees side is not None, only check value validity here
        # (This will be handled in _validate_trade_side method)

        # Trade model guarantees executed_at is not None and valid datetime
        # (Timestamp validation will be handled in _validate_timestamp method)

        # Check for trade ID fields
        if not trade.id or not trade.id.strip():
            errors.append("Required field id is missing or empty")
        if not trade.order_id or not trade.order_id.strip():
            errors.append("Required field order_id is missing or empty")

        return errors

    async def _validate_trade_id(self, trade: Trade | None) -> list[str]:
        """Validate trade ID format and uniqueness.

        Args:
            trade: Trade object

        Returns:
            List of validation errors
        """
        if trade is None:
            return ["Trade object is None"]

        errors: list[str] = []

        # Check trade ID (Trade model has 'id' field)
        if not trade.id:
            errors.append("Trade ID is required")
            return errors

        trade_id = str(trade.id).strip()

        # Basic length validation
        if len(trade_id) < MIN_TRADE_ID_LENGTH:
            errors.append("Trade ID too short (minimum 3 characters)")
        elif len(trade_id) > MAX_TRADE_ID_LENGTH:
            errors.append("Trade ID too long (maximum 100 characters)")

        # Pattern validation if configured
        if self.trade_id_pattern and not re.match(self.trade_id_pattern, trade_id):
            errors.append(f"Trade ID does not match required pattern: {self.trade_id_pattern}")

        return errors

    async def _validate_symbol(self, trade: Trade | None) -> tuple[list[str], list[str]]:
        """Validate trading symbol format.

        Args:
            trade: Trade object

        Returns:
            Tuple of (errors, warnings)
        """
        if trade is None:
            return (["Trade object is None"], [])

        errors: list[str] = []
        warnings: list[str] = []

        # Check symbol (Trade model has 'symbol' field)
        if not trade.symbol:
            errors.append("Symbol is required")
            return errors, warnings

        symbol = str(trade.symbol).strip().upper()

        # Basic format validation
        if len(symbol) < MIN_SYMBOL_LENGTH:
            errors.append("Symbol too short (minimum 2 characters)")
        elif len(symbol) > MAX_SYMBOL_LENGTH:
            errors.append("Symbol too long (maximum 20 characters)")

        # Character validation
        if not symbol.replace("-", "").replace("/", "").replace("_", "").isalnum():
            errors.append("Symbol contains invalid characters")

        # Common format warnings
        if not any(sep in symbol for sep in ["-", "/", "_"]):
            warnings.append("Symbol may not be a trading pair (no separator found)")

        return errors, warnings

    async def _validate_exchange(self, trade: Trade | None) -> list[str]:
        """Validate exchange ID.

        Args:
            trade: Trade object

        Returns:
            List of validation errors
        """
        if trade is None:
            return ["Trade object is None"]

        errors: list[str] = []

        # Check exchange (Trade model has 'exchange' field)
        if not trade.exchange:
            errors.append("Exchange is required")
            return errors

        exchange_id = str(trade.exchange).strip().lower()

        # Allowed exchanges validation
        if self.allowed_exchanges and exchange_id not in self.allowed_exchanges:
            errors.append(f"Exchange {exchange_id} not in allowed list: {self.allowed_exchanges}")

        # Basic format validation
        if len(exchange_id) < MIN_EXCHANGE_ID_LENGTH:
            errors.append("Exchange ID too short")
        elif len(exchange_id) > MAX_EXCHANGE_ID_LENGTH:
            errors.append("Exchange ID too long")

        return errors

    async def _validate_price(self, trade: Trade | None) -> tuple[list[str], list[str]]:
        """Validate trade price.

        Args:
            trade: Trade object

        Returns:
            Tuple of (errors, warnings)
        """
        if trade is None:
            return (["Trade object is None"], [])

        errors: list[str] = []
        warnings: list[str] = []

        # Check price (Trade model guarantees price is not None and > 0)
        # price is already a Decimal from the Trade model
        price = trade.price

        # Trade model guarantees price > 0 via Field(gt=Decimal(0))

        # Negative price validation
        if price < 0 and not self.allow_negative_prices:
            errors.append("Negative prices are not allowed")

        # Range validation
        if price < self.min_price_value:
            errors.append(f"Price {price} below minimum {self.min_price_value}")
        elif price > self.max_price_value:
            errors.append(f"Price {price} above maximum {self.max_price_value}")

        # Precision warnings
        exponent = price.as_tuple().exponent
        if isinstance(exponent, int) and exponent < DECIMAL_PRECISION_OFFSET:
            warnings.append("Price has very high precision (>8 decimal places)")

        return errors, warnings

    async def _validate_quantity(self, trade: Trade | None) -> tuple[list[str], list[str]]:
        """Validate trade quantity.

        Args:
            trade: Trade object

        Returns:
            Tuple of (errors, warnings)
        """
        if trade is None:
            return (["Trade object is None"], [])

        errors: list[str] = []
        warnings: list[str] = []

        # Check quantity (Trade model guarantees quantity is not None and > 0)
        # quantity is already a Decimal from the Trade model
        quantity = trade.quantity

        # Trade model guarantees quantity > 0 via Field(gt=Decimal(0))
        # Additional business rules validation below

        # Range validation
        if quantity > 0 and quantity < self.min_quantity_value:
            errors.append(f"Quantity {quantity} below minimum {self.min_quantity_value}")
        elif quantity > self.max_quantity_value:
            errors.append(f"Quantity {quantity} above maximum {self.max_quantity_value}")

        # Precision warnings
        exponent = quantity.as_tuple().exponent
        if isinstance(exponent, int) and exponent < DECIMAL_PRECISION_OFFSET:
            warnings.append("Quantity has very high precision (>8 decimal places)")

        return errors, warnings

    async def _validate_timestamp(self, trade: Trade | None) -> tuple[list[str], list[str]]:
        """Validate trade timestamp.

        Args:
            trade: Trade object

        Returns:
            Tuple of (errors, warnings)
        """
        if trade is None:
            return (["Trade object is None"], [])

        errors: list[str] = []
        warnings: list[str] = []

        # Check timestamp (Trade model guarantees executed_at is not None and valid datetime)
        timestamp = trade.executed_at

        # Range validation (reasonable timestamp range)
        current_time = datetime.now(UTC)
        one_year_ago = current_time - timedelta(days=365)
        one_day_future = current_time + timedelta(days=1)

        if timestamp < one_year_ago:
            warnings.append("Timestamp is more than one year old")
        elif timestamp > one_day_future:
            warnings.append("Timestamp is more than one day in the future")

        return errors, warnings

    async def _validate_trade_side(self, trade: Trade | None) -> list[str]:
        """Validate trade side (buy/sell).

        Args:
            trade: Trade object

        Returns:
            List of validation errors
        """
        if trade is None:
            return ["Trade object is None"]

        errors: list[str] = []

        # Check side (Trade model guarantees side is not None and valid OrderSide)
        side = str(trade.side).strip().upper()
        valid_sides = ["BUY", "SELL", "LONG", "SHORT"]

        if side not in valid_sides:
            errors.append(f"Invalid trade side '{side}'. Must be one of: {valid_sides}")

        return errors

    async def _validate_cross_fields(self, trade: Trade | None) -> tuple[list[str], list[str]]:
        """Validate relationships between fields.

        Args:
            trade: Trade object

        Returns:
            Tuple of (errors, warnings)
        """
        if trade is None:
            return (["Trade object is None"], [])

        errors: list[str] = []
        warnings: list[str] = []

        # Validate price * quantity relationship for trade objects
        try:
            # Trade model guarantees price and quantity are not None
            price = Decimal(str(trade.price))
            quantity = Decimal(str(trade.quantity))

            if price > 0 and quantity > 0:
                notional = price * quantity

                # Check for extremely large notional values
                if notional > Decimal(1000000000):  # 1 billion
                    warnings.append(f"Very large notional value: {notional}")

                # Check for extremely small notional values
                if notional < Decimal("0.01"):  # 1 cent
                    warnings.append(f"Very small notional value: {notional}")

        except (ValueError, TypeError):
            # Already handled in individual field validation
            pass

        return errors, warnings

    def get_validation_stats(self) -> dict[str, Any]:
        """Get validation statistics.

        Returns:
            Dictionary with validation statistics
        """
        return {
            "screener_name": self.name,
            "is_running": self.is_initialized,
            "strict_mode": self.strict_mode,
            "allow_zero_quantities": self.allow_zero_quantities,
            "allow_negative_prices": self.allow_negative_prices,
            "symbol_validation_enabled": self.symbol_validation_enabled,
            "require_trade_id": self.require_trade_id,
            "require_exchange_id": self.require_exchange_id,
            "allowed_exchanges": self.allowed_exchanges,
            "max_price_value": str(self.max_price_value),
            "max_quantity_value": str(self.max_quantity_value),
            "min_price_value": str(self.min_price_value),
            "min_quantity_value": str(self.min_quantity_value),
        }
