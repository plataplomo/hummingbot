"""Financial data screening and validation components."""

from __future__ import annotations

import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, NamedTuple

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.screening.base.base_screener import BaseScreener


if TYPE_CHECKING:
    from cyberdelta.core.models import MarginAccountSummary, Ticker
else:
    # Import at runtime for isinstance checks
    from cyberdelta.core.models import MarginAccountSummary, Ticker

logger = get_logger(__name__)

# Constants
MIN_SYMBOL_LENGTH = 2
MAX_SYMBOL_LENGTH = 50
MAX_SYMBOL_LENGTH_TRADING_PAIR = 20
MIN_SYMBOL_PARTS = 2
MAX_SYMBOL_PARTS = 20
MIN_EXCHANGE_ID_LENGTH = 2
MAX_EXCHANGE_ID_LENGTH = 50
DECIMAL_PRECISION_OFFSET = -8


class FinancialValidationResult(NamedTuple):
    """Result of financial data validation."""

    is_valid: bool
    errors: list[str]
    warnings: list[str]
    cleaned_data: Any | None = None


class FinancialDataScreener(BaseScreener):
    """Validates and screens financial data before processing.

    Provides comprehensive validation of financial data including
    margin account summaries, ticker data, and financial metrics.
    """

    def __init__(
        self,
        name: str = "FinancialDataScreener",
        config: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the financial data screener.

        Args:
            name: Screener name
            config: Configuration dictionary
        """
        cfg = config or {}
        super().__init__(name, config)

        # Financial validation configuration
        self.allow_negative_balances = cfg.get("allow_negative_balances", False)
        self.allow_zero_prices = cfg.get("allow_zero_prices", False)
        self.max_price_value = Decimal(cfg.get("max_price_value", "10000000"))
        self.max_balance_value = Decimal(cfg.get("max_balance_value", "1000000000"))
        self.min_price_value = Decimal(cfg.get("min_price_value", "0.000001"))
        self.min_balance_value = Decimal(cfg.get("min_balance_value", "0"))

        # Margin validation
        self.max_leverage = Decimal(cfg.get("max_leverage", "100"))
        self.min_margin_ratio = Decimal(cfg.get("min_margin_ratio", "0.01"))
        self.max_margin_ratio = Decimal(cfg.get("max_margin_ratio", "10"))

        # Ticker validation
        self.require_bid_ask = cfg.get("require_bid_ask", True)
        self.max_spread_percentage = Decimal(cfg.get("max_spread_percentage", "50"))
        self.volume_validation_enabled = cfg.get("volume_validation_enabled", True)

        # Exchange validation
        self.require_exchange_id = cfg.get("require_exchange_id", True)
        self.allowed_exchanges = cfg.get("allowed_exchanges", [])

        # Timestamp validation
        self.validate_timestamps = cfg.get("validate_timestamps", True)
        self.max_data_age_seconds = cfg.get("max_data_age_seconds", 300)  # 5 minutes

        logger.info(
            "financial_data_screener_created",
            screener_name=name,
            allow_negative_balances=self.allow_negative_balances,
            allow_zero_prices=self.allow_zero_prices,
            max_leverage=self.max_leverage,
        )

    async def _initialize_internal(self) -> None:
        """Initialize internal state."""
        logger.info("financial_data_screener_initializing")

    async def _shutdown_internal(self) -> None:
        """Shutdown internal state."""
        logger.info("financial_data_screener_shutting_down")

    async def validate_margin_account_summary(
        self, summary: MarginAccountSummary
    ) -> FinancialValidationResult:
        """Validate margin account summary data.

        Args:
            summary: Margin account summary to validate

        Returns:
            FinancialValidationResult with validation details
        """
        self._ensure_initialized()

        errors: list[str] = []
        warnings: list[str] = []

        # MarginAccountSummary is a concrete type, no None check needed

        # Validate required fields
        field_errors = await self._validate_margin_summary_fields(summary)
        errors.extend(field_errors)

        # Validate exchange
        if self.require_exchange_id:
            exchange_errors = await self._validate_exchange(summary)
            errors.extend(exchange_errors)

        # Validate balance amounts
        balance_errors, balance_warnings = await self._validate_margin_balances(summary)
        errors.extend(balance_errors)
        warnings.extend(balance_warnings)

        # Validate margin ratios
        ratio_errors, ratio_warnings = await self._validate_margin_ratios(summary)
        errors.extend(ratio_errors)
        warnings.extend(ratio_warnings)

        # Validate timestamps
        if self.validate_timestamps:
            timestamp_errors, timestamp_warnings = await self._validate_timestamps(summary)
            errors.extend(timestamp_errors)
            warnings.extend(timestamp_warnings)

        # Cross-field validation
        cross_errors, cross_warnings = await self._validate_margin_cross_fields(summary)
        errors.extend(cross_errors)
        warnings.extend(cross_warnings)

        is_valid = len(errors) == 0

        # Update statistics
        self._update_validation_stats(is_valid, len(errors), len(warnings))

        if not is_valid:
            logger.warning(
                "margin_summary_validation_failed",
                exchange_id=summary.exchange or "unknown",
                error_count=len(errors),
                warning_count=len(warnings),
            )
        elif warnings:
            logger.info(
                "margin_summary_validation_warnings",
                exchange_id=summary.exchange or "unknown",
                warning_count=len(warnings),
            )

        return FinancialValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            cleaned_data=summary if is_valid else None,
        )

    async def validate_ticker(self, ticker: Ticker) -> FinancialValidationResult:
        """Validate ticker data.

        Args:
            ticker: Ticker object to validate

        Returns:
            FinancialValidationResult with validation details
        """
        self._ensure_initialized()

        errors: list[str] = []
        warnings: list[str] = []

        # Ticker is a concrete type, no None check needed

        # Validate required fields
        field_errors = await self._validate_ticker_fields(ticker)
        errors.extend(field_errors)

        # Validate symbol
        symbol_errors, symbol_warnings = await self._validate_symbol(ticker)
        errors.extend(symbol_errors)
        warnings.extend(symbol_warnings)

        # Validate exchange
        if self.require_exchange_id:
            exchange_errors = await self._validate_exchange(ticker)
            errors.extend(exchange_errors)

        # Validate prices
        price_errors, price_warnings = await self._validate_ticker_prices(ticker)
        errors.extend(price_errors)
        warnings.extend(price_warnings)

        # Validate volume
        if self.volume_validation_enabled:
            volume_errors, volume_warnings = await self._validate_ticker_volume(ticker)
            errors.extend(volume_errors)
            warnings.extend(volume_warnings)

        # Validate timestamps
        if self.validate_timestamps:
            timestamp_errors, timestamp_warnings = await self._validate_timestamps(ticker)
            errors.extend(timestamp_errors)
            warnings.extend(timestamp_warnings)

        # Cross-field validation
        cross_errors, cross_warnings = await self._validate_ticker_cross_fields(ticker)
        errors.extend(cross_errors)
        warnings.extend(cross_warnings)

        is_valid = len(errors) == 0

        # Update statistics
        self._update_validation_stats(is_valid, len(errors), len(warnings))

        if not is_valid:
            logger.warning(
                "ticker_validation_failed",
                symbol=ticker.symbol or "unknown",
                exchange_id=ticker.exchange,
                error_count=len(errors),
                warning_count=len(warnings),
            )
        elif warnings:
            logger.info(
                "ticker_validation_warnings",
                symbol=ticker.symbol or "unknown",
                exchange_id=ticker.exchange,
                warning_count=len(warnings),
            )

        return FinancialValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            cleaned_data=ticker if is_valid else None,
        )

    async def _validate_margin_summary_fields(self, summary: MarginAccountSummary) -> list[str]:
        """Validate margin account summary required fields.

        Args:
            summary: Margin account summary

        Returns:
            List of validation errors
        """
        errors: list[str] = []

        # MarginAccountSummary is a concrete type with all required fields
        # No protocol check needed

        # Validate required protocol fields
        if not summary.exchange or not summary.exchange.strip():
            errors.append("Required field exchange is missing or empty")

        # total_equity is required by MarginAccountSummary model - cannot be None

        # Business validation: require margin fields even though model allows None
        if summary.total_initial_margin_required is None:
            errors.append("Business rule violation: total_initial_margin_required is required")

        if summary.total_maintenance_margin_required is None:
            errors.append("Business rule violation: total_maintenance_margin_required is required")

        return errors

    async def _validate_ticker_fields(self, ticker: Ticker) -> list[str]:
        """Validate ticker required fields.

        Args:
            ticker: Ticker object

        Returns:
            List of validation errors
        """
        errors: list[str] = []

        # Ticker is a concrete type with all required fields
        # No protocol check needed

        # Validate required protocol fields
        if not ticker.symbol or not ticker.symbol.strip():
            errors.append("Required field symbol is missing or empty")

        if ticker.price is None:
            errors.append("Required field price is None")

        # Validate bid/ask if required
        if self.require_bid_ask:
            if ticker.bid is None:
                errors.append("Required field bid is None")
            if ticker.ask is None:
                errors.append("Required field ask is None")

        return errors

    async def _validate_exchange(self, data: object) -> list[str]:
        """Validate exchange ID.

        Args:
            data: Data object with exchange_id

        Returns:
            List of validation errors
        """
        errors: list[str] = []

        # Type-safe exchange field access based on known model types
        exchange_id = None
        if isinstance(data, (MarginAccountSummary, Ticker)):
            exchange_id = data.exchange

        if not exchange_id:
            errors.append("Exchange ID is required")
            return errors

        exchange_id = str(exchange_id).strip().lower()

        # Allowed exchanges validation
        if self.allowed_exchanges and exchange_id not in self.allowed_exchanges:
            errors.append(f"Exchange {exchange_id} not in allowed list: {self.allowed_exchanges}")

        # Basic format validation
        if len(exchange_id) < MIN_EXCHANGE_ID_LENGTH:
            errors.append("Exchange ID too short")
        elif len(exchange_id) > MAX_EXCHANGE_ID_LENGTH:
            errors.append("Exchange ID too long")

        return errors

    async def _validate_symbol(self, data: object) -> tuple[list[str], list[str]]:
        """Validate trading symbol format.

        Args:
            data: Data object with symbol

        Returns:
            Tuple of (errors, warnings)
        """
        errors: list[str] = []
        warnings: list[str] = []

        # Get symbol from the data object - Ticker has symbol field
        symbol = None
        if isinstance(data, Ticker):
            symbol = data.symbol

        if not symbol:
            errors.append("Symbol is required")
            return errors, warnings

        symbol = str(symbol).strip().upper()

        # Basic format validation
        if len(symbol) < MIN_SYMBOL_LENGTH:
            errors.append("Symbol too short (minimum 2 characters)")
        elif len(symbol) > MAX_SYMBOL_LENGTH_TRADING_PAIR:
            errors.append("Symbol too long (maximum 20 characters)")

        # Character validation
        if not symbol.replace("-", "").replace("/", "").replace("_", "").isalnum():
            errors.append("Symbol contains invalid characters")

        # Common format warnings
        if not any(sep in symbol for sep in ["-", "/", "_"]):
            warnings.append("Symbol may not be a trading pair (no separator found)")

        return errors, warnings

    async def _validate_margin_balances(
        self, summary: MarginAccountSummary
    ) -> tuple[list[str], list[str]]:
        """Validate margin account balance amounts.

        Args:
            summary: Margin account summary

        Returns:
            Tuple of (errors, warnings)
        """
        errors: list[str] = []
        warnings: list[str] = []

        # MarginAccountSummary is a concrete type with all required fields
        # No protocol check needed

        # Validate required protocol fields
        balance_fields = [
            ("total_equity", summary.total_equity),
            ("total_initial_margin_required", summary.total_initial_margin_required),
            ("total_maintenance_margin_required", summary.total_maintenance_margin_required),
            ("available_equity", summary.available_equity),
        ]

        for field_name, value in balance_fields:
            if value is not None:
                try:
                    amount = Decimal(str(value))
                except (ValueError, TypeError):
                    errors.append(f"{field_name} must be a valid decimal number")
                    continue

                # Negative balance validation
                if amount < 0:
                    if (
                        field_name in {"total_equity", "available_equity"}
                        and not self.allow_negative_balances
                    ):
                        errors.append(f"Negative {field_name} is not allowed")
                    elif field_name in {
                        "total_initial_margin_required",
                        "total_maintenance_margin_required",
                    }:
                        errors.append(f"{field_name} cannot be negative")

                # Range validation
                if amount > self.max_balance_value:
                    errors.append(f"{field_name} {amount} exceeds maximum {self.max_balance_value}")
                elif amount < self.min_balance_value and amount != 0:
                    errors.append(f"{field_name} {amount} below minimum {self.min_balance_value}")

                # Warning for very large amounts
                if amount > self.max_balance_value / 10:
                    warnings.append(f"{field_name} has a very large value: {amount}")

        return errors, warnings

    async def _validate_margin_ratios(
        self, summary: MarginAccountSummary
    ) -> tuple[list[str], list[str]]:
        """Validate margin ratios.

        Args:
            summary: Margin account summary

        Returns:
            Tuple of (errors, warnings)
        """
        errors: list[str] = []
        warnings: list[str] = []

        # MarginAccountSummary validation - no additional optional ratio fields needed
        # The model already has all required margin fields defined

        return errors, warnings

    async def _validate_ticker_prices(self, ticker: Ticker) -> tuple[list[str], list[str]]:
        """Validate ticker price data.

        Args:
            ticker: Ticker object

        Returns:
            Tuple of (errors, warnings)
        """
        errors: list[str] = []
        warnings: list[str] = []

        # Ticker is a concrete type with all required fields
        # No protocol check needed, warnings

        # Validate protocol-defined price fields
        price_fields = [("price", ticker.price), ("bid", ticker.bid), ("ask", ticker.ask)]

        for field_name, value in price_fields:
            if value is not None:
                try:
                    price = Decimal(str(value))
                except (ValueError, TypeError):
                    errors.append(f"{field_name} must be a valid decimal number")
                    continue

                # Zero price validation
                if price == 0 and not self.allow_zero_prices:
                    errors.append(f"{field_name} cannot be zero")

                # Negative price validation
                if price < 0:
                    errors.append(f"{field_name} cannot be negative")

                # Range validation
                if price > 0 and price < self.min_price_value:
                    errors.append(f"{field_name} {price} below minimum {self.min_price_value}")
                elif price > self.max_price_value:
                    errors.append(f"{field_name} {price} above maximum {self.max_price_value}")

                # Precision warnings
                exponent = price.as_tuple().exponent
                if isinstance(exponent, int) and exponent < DECIMAL_PRECISION_OFFSET:
                    warnings.append(f"{field_name} has very high precision (>8 decimal places)")

        return errors, warnings

    async def _validate_ticker_volume(self, ticker: Ticker) -> tuple[list[str], list[str]]:
        """Validate ticker volume data.

        Args:
            ticker: Ticker object

        Returns:
            Tuple of (errors, warnings)
        """
        errors: list[str] = []
        warnings: list[str] = []

        # Validate ticker volume field
        if ticker.volume is not None:
            try:
                volume = Decimal(str(ticker.volume))
            except (ValueError, TypeError):
                errors.append("volume must be a valid decimal number")
            else:
                # Negative volume validation
                if volume < 0:
                    errors.append("volume cannot be negative")

                # Warning for zero volume
                if volume == 0:
                    warnings.append("volume is zero - possible inactive market")

        return errors, warnings

    async def _validate_timestamps(self, data: object) -> tuple[list[str], list[str]]:
        """Validate timestamp data.

        Args:
            data: Data object with timestamps

        Returns:
            Tuple of (errors, warnings)
        """
        errors: list[str] = []
        warnings: list[str] = []

        # Validate timestamp field for known model types
        if isinstance(data, (MarginAccountSummary, Ticker)):
            timestamp_value = data.timestamp

            # Convert datetime to timestamp for validation
            try:
                timestamp_float = timestamp_value.timestamp()
            except (AttributeError, ValueError):
                errors.append("timestamp must be a valid datetime")
            else:
                # Range validation
                current_time = time.time()
                max_age = current_time - self.max_data_age_seconds
                one_hour_future = current_time + (60 * 60)

                if timestamp_float < max_age:
                    warnings.append(f"timestamp is older than {self.max_data_age_seconds} seconds")
                elif timestamp_float > one_hour_future:
                    warnings.append("timestamp is more than one hour in the future")

        return errors, warnings

    async def _validate_margin_cross_fields(
        self, summary: MarginAccountSummary
    ) -> tuple[list[str], list[str]]:
        """Validate margin account cross-field relationships.

        Args:
            summary: Margin account summary

        Returns:
            Tuple of (errors, warnings)
        """
        errors: list[str] = []
        warnings: list[str] = []

        # MarginAccountSummary is a concrete type with all required fields
        # No protocol check needed

        try:
            # Validate margin requirements consistency
            if (
                summary.total_initial_margin_required is not None
                and summary.total_maintenance_margin_required is not None
            ):
                initial_margin = Decimal(str(summary.total_initial_margin_required))
                maintenance_margin = Decimal(str(summary.total_maintenance_margin_required))

                if initial_margin < maintenance_margin:
                    errors.append("Initial margin cannot be less than maintenance margin")

            # Validate account value and margins
            account_value = Decimal(str(summary.total_equity))
            initial_margin = Decimal(str(summary.total_initial_margin_required))

            if account_value > 0 and initial_margin > account_value:
                warnings.append("Initial margin exceeds account value")

            # Validate available balance
            account_value = Decimal(str(summary.total_equity))
            initial_margin = Decimal(str(summary.total_initial_margin_required))
            available_balance = Decimal(str(summary.available_equity))

            expected_available = account_value - initial_margin
            tolerance = Decimal("0.01")

            if abs(available_balance - expected_available) > tolerance:
                warnings.append("Available balance doesn't match expected calculation")

        except (ValueError, TypeError, AttributeError):
            # Error in calculation - individual field validation will catch this
            pass

        return errors, warnings

    async def _validate_ticker_cross_fields(self, ticker: Ticker) -> tuple[list[str], list[str]]:
        """Validate ticker cross-field relationships.

        Args:
            ticker: Ticker object

        Returns:
            Tuple of (errors, warnings)
        """
        errors: list[str] = []
        warnings: list[str] = []

        # Ticker is a concrete type with all required fields
        # No protocol check needed

        try:
            # Validate bid/ask spread
            bid_price = Decimal(str(ticker.bid)) if ticker.bid else None
            ask_price = Decimal(str(ticker.ask)) if ticker.ask else None

            if bid_price and ask_price:
                if bid_price >= ask_price:
                    errors.append("Bid price cannot be greater than or equal to ask price")
                else:
                    spread = ask_price - bid_price
                    spread_pct = (spread / ask_price) * 100

                    if spread_pct > self.max_spread_percentage:
                        warnings.append(f"Very wide spread: {spread_pct:.2f}%")

            # Note: Ticker model doesn't have high_price/low_price fields
            # No high/low price validation needed

            # Validate last price against bid/ask
            last_price = Decimal(str(ticker.price)) if ticker.price else None

            if (
                last_price
                and bid_price
                and ask_price
                and (last_price < bid_price or last_price > ask_price)
            ):
                warnings.append("Last price is outside bid/ask spread")

        except (ValueError, TypeError, AttributeError):
            # Error in calculation - individual field validation will catch this
            pass

        return errors, warnings

    def get_validation_stats(self) -> dict[str, Any]:
        """Get validation statistics.

        Returns:
            Dictionary with validation statistics
        """
        base_stats = super().get_validation_stats()

        return {
            **base_stats,
            "allow_negative_balances": self.allow_negative_balances,
            "allow_zero_prices": self.allow_zero_prices,
            "max_price_value": str(self.max_price_value),
            "max_balance_value": str(self.max_balance_value),
            "min_price_value": str(self.min_price_value),
            "min_balance_value": str(self.min_balance_value),
            "max_leverage": str(self.max_leverage),
            "min_margin_ratio": str(self.min_margin_ratio),
            "max_margin_ratio": str(self.max_margin_ratio),
            "require_bid_ask": self.require_bid_ask,
            "max_spread_percentage": str(self.max_spread_percentage),
            "volume_validation_enabled": self.volume_validation_enabled,
            "require_exchange_id": self.require_exchange_id,
            "validate_timestamps": self.validate_timestamps,
            "max_data_age_seconds": self.max_data_age_seconds,
            "allowed_exchanges": self.allowed_exchanges,
        }
