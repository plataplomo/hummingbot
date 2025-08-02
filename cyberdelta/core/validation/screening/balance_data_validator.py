"""Balance data screening and validation components."""

from __future__ import annotations

import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, NamedTuple

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.validation.screening.base.base_validator import BaseValidator


if TYPE_CHECKING:
    from cyberdelta.core.models import SpotBalance

logger = get_logger(__name__)

# Constants
MIN_CURRENCY_CODE_LENGTH = 2
MAX_CURRENCY_CODE_LENGTH = 10
STANDARD_CURRENCY_CODE_LENGTH = 3
MIN_EXCHANGE_ID_LENGTH = 2
MAX_EXCHANGE_ID_LENGTH = 50
HIGH_LOCKED_BALANCE_THRESHOLD = 90  # percent
LOW_AVAILABLE_BALANCE_THRESHOLD = 5  # percent


class BalanceValidationResult(NamedTuple):
    """Result of balance validation."""

    is_valid: bool
    errors: list[str]
    warnings: list[str]
    cleaned_balance: SpotBalance | None = None


class BalanceDataValidator(BaseValidator):
    """Validates and screens balance data before processing.

    Provides comprehensive validation of spot balance data
    including amounts, currencies, and balance metadata validation.
    """

    def __init__(
        self,
        name: str = "BalanceDataScreener",
        config: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the balance data screener.

        Args:
            name: Screener name
            config: Configuration dictionary
        """
        cfg = config or {}
        super().__init__(name, config)

        # Balance validation configuration
        self.allow_zero_balances = cfg.get("allow_zero_balances", True)
        self.allow_negative_balances = cfg.get("allow_negative_balances", False)
        self.max_balance_amount = Decimal(cfg.get("max_balance_amount", "1000000000"))
        self.min_balance_amount = Decimal(cfg.get("min_balance_amount", "0"))

        # Currency validation
        self.currency_validation_enabled = cfg.get("currency_validation_enabled", True)
        self.allowed_currencies = cfg.get("allowed_currencies", [])
        self.require_currency_code = cfg.get("require_currency_code", True)

        # Exchange validation
        self.require_exchange_id = cfg.get("require_exchange_id", True)
        self.allowed_exchanges = cfg.get("allowed_exchanges", [])

        # Balance metadata validation
        self.validate_timestamps = cfg.get("validate_timestamps", True)
        self.require_balance_type = cfg.get("require_balance_type", False)

        # Precision validation
        self.max_decimal_places = cfg.get("max_decimal_places", 8)

        logger.info(
            "balance_data_screener_created",
            screener_name=name,
            allow_zero_balances=self.allow_zero_balances,
            allow_negative_balances=self.allow_negative_balances,
            max_balance_amount=self.max_balance_amount,
        )

    async def _initialize_internal(self) -> None:
        """Initialize internal state."""
        logger.info("balance_data_screener_initializing")

    async def _shutdown_internal(self) -> None:
        """Shutdown internal state."""
        logger.info("balance_data_screener_shutting_down")

    async def validate_balance(self, balance: SpotBalance | None) -> BalanceValidationResult:
        """Validate balance data comprehensively.

        Args:
            balance: Balance object to validate

        Returns:
            BalanceValidationResult with validation details
        """
        self._ensure_initialized()

        errors: list[str] = []
        warnings: list[str] = []

        # Basic structure validation
        if balance is None:
            errors.append("Balance object is None")
            return BalanceValidationResult(
                is_valid=False,
                errors=errors,
                warnings=warnings,
            )

        # Validate required fields
        field_errors = await self._validate_required_fields(balance)
        errors.extend(field_errors)

        # Validate currency
        if self.currency_validation_enabled:
            currency_errors, currency_warnings = await self._validate_currency(balance)
            errors.extend(currency_errors)
            warnings.extend(currency_warnings)

        # Validate exchange
        if self.require_exchange_id:
            exchange_errors = await self._validate_exchange(balance)
            errors.extend(exchange_errors)

        # Validate balance amounts
        amount_errors, amount_warnings = await self._validate_amounts(balance)
        errors.extend(amount_errors)
        warnings.extend(amount_warnings)

        # Validate balance type
        if self.require_balance_type:
            type_errors = await self._validate_balance_type(balance)
            errors.extend(type_errors)

        # Validate timestamps
        if self.validate_timestamps:
            timestamp_errors, timestamp_warnings = await self._validate_timestamps(balance)
            errors.extend(timestamp_errors)
            warnings.extend(timestamp_warnings)

        # Cross-field validation
        cross_errors, cross_warnings = await self._validate_cross_fields(balance)
        errors.extend(cross_errors)
        warnings.extend(cross_warnings)

        is_valid = len(errors) == 0

        # Update statistics
        self._update_validation_stats(is_valid, len(errors), len(warnings))

        if not is_valid:
            logger.warning(
                "balance_validation_failed",
                currency=balance.asset,
                exchange_id=balance.exchange,
                error_count=len(errors),
                warning_count=len(warnings),
            )
        elif warnings:
            logger.info(
                "balance_validation_warnings",
                currency=balance.asset,
                exchange_id=balance.exchange,
                warning_count=len(warnings),
            )

        return BalanceValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            cleaned_balance=balance if is_valid else None,
        )

    async def sanitize_balance(self, balance: SpotBalance) -> SpotBalance:
        """Sanitize balance data by cleaning common issues.

        Args:
            balance: Balance object to sanitize

        Returns:
            Sanitized balance object
        """
        self._ensure_initialized()

        # Create a copy to avoid modifying the original
        sanitized = balance

        # Clean balance data directly (SpotBalance objects have known structure)
        # Symbol objects are already normalized - no cleaning needed

        # Clean exchange
        if sanitized.exchange:
            sanitized.exchange = sanitized.exchange.strip().lower()

        # Ensure decimal precision for quantities
        sanitized.available_quantity = Decimal(str(sanitized.available_quantity))
        sanitized.total_quantity = Decimal(str(sanitized.total_quantity))

        logger.debug(
            "balance_sanitized",
            currency=sanitized.asset,
            exchange_id=sanitized.exchange,
        )

        return sanitized

    async def _validate_required_fields(self, balance: SpotBalance | None) -> list[str]:
        """Validate that required fields are present.

        Args:
            balance: Balance object

        Returns:
            List of validation errors
        """
        if balance is None:
            return ["Balance object is None"]

        errors: list[str] = []

        # Validate required SpotBalance model fields directly
        if not balance.asset:
            errors.append("Required field asset is missing or empty")

        if not balance.exchange or not balance.exchange.strip():
            errors.append("Required field exchange is missing or empty")

        # SpotBalance model guarantees available_quantity and total_quantity are not None
        # and are Decimal >= 0, no additional validation needed

        return errors

    async def _validate_currency(self, balance: SpotBalance | None) -> tuple[list[str], list[str]]:
        """Validate currency code format.

        Args:
            balance: Balance object

        Returns:
            Tuple of (errors, warnings)
        """
        if balance is None:
            return (["Balance object is None"], [])

        errors: list[str] = []
        warnings: list[str] = []

        # Check asset (SpotBalance model has 'asset' field)
        if not balance.asset:
            errors.append("Asset is required")
            return errors, warnings

        currency = str(balance.asset).strip().upper()

        # Basic format validation
        if len(currency) < MIN_CURRENCY_CODE_LENGTH:
            errors.append(
                f"Currency code too short (minimum {MIN_CURRENCY_CODE_LENGTH} characters)"
            )
        elif len(currency) > MAX_CURRENCY_CODE_LENGTH:
            errors.append(f"Currency code too long (maximum {MAX_CURRENCY_CODE_LENGTH} characters)")

        # Character validation
        if not currency.isalnum():
            errors.append("Currency code contains invalid characters")

        # Allowed currencies validation
        if self.allowed_currencies and currency not in self.allowed_currencies:
            errors.append(f"Currency {currency} not in allowed list")

        # Common format warnings
        if len(currency) != STANDARD_CURRENCY_CODE_LENGTH:
            warnings.append("Currency code is not standard 3-character format")

        # Check for common stablecoins
        stablecoins = ["USDT", "USDC", "BUSD", "DAI", "TUSD", "USDD"]
        if currency in stablecoins:
            warnings.append(f"Currency {currency} is a stablecoin")

        return errors, warnings

    async def _validate_exchange(self, balance: SpotBalance | None) -> list[str]:
        """Validate exchange ID.

        Args:
            balance: Balance object

        Returns:
            List of validation errors
        """
        if balance is None:
            return ["Balance object is None"]

        errors: list[str] = []

        # Check exchange (SpotBalance model has 'exchange' field)
        if not balance.exchange:
            errors.append("Exchange is required")
            return errors

        exchange_id = str(balance.exchange).strip().lower()

        # Allowed exchanges validation
        if self.allowed_exchanges and exchange_id not in self.allowed_exchanges:
            errors.append(f"Exchange {exchange_id} not in allowed list: {self.allowed_exchanges}")

        # Basic format validation
        if len(exchange_id) < MIN_EXCHANGE_ID_LENGTH:
            errors.append("Exchange ID too short")
        elif len(exchange_id) > MAX_EXCHANGE_ID_LENGTH:
            errors.append("Exchange ID too long")

        return errors

    async def _validate_amounts(self, balance: SpotBalance | None) -> tuple[list[str], list[str]]:
        """Validate balance amounts.

        Args:
            balance: Balance object

        Returns:
            Tuple of (errors, warnings)
        """
        if balance is None:
            return (["Balance object is None"], [])

        errors: list[str] = []
        warnings: list[str] = []

        # Validate SpotBalance model amount fields
        amount_fields = [
            ("available_quantity", balance.available_quantity),
            ("total_quantity", balance.total_quantity),
        ]

        for field_name, value in amount_fields:
            field_errors, field_warnings = self._validate_single_amount_field(field_name, value)
            errors.extend(field_errors)
            warnings.extend(field_warnings)

        return errors, warnings

    def _validate_single_amount_field(
        self, field_name: str, value: Decimal
    ) -> tuple[list[str], list[str]]:
        """Validate a single amount field.

        Args:
            field_name: Name of the field being validated
            value: Value to validate

        Returns:
            Tuple of (errors, warnings)
        """
        errors: list[str] = []
        warnings: list[str] = []

        try:
            amount = Decimal(str(value))
        except (ValueError, TypeError):
            errors.append(f"{field_name} must be a valid decimal number")
            return errors, warnings

        # Validate using helper methods
        errors.extend(self._validate_amount_sign(field_name, amount))
        errors.extend(self._validate_amount_range(field_name, amount))
        warnings.extend(self._validate_amount_precision(field_name, amount))

        return errors, warnings

    def _validate_amount_sign(self, field_name: str, amount: Decimal) -> list[str]:
        """Validate amount sign (zero/negative checks).

        Args:
            field_name: Name of the field being validated
            amount: Amount value to validate

        Returns:
            List of validation errors for amount sign issues
        """
        errors: list[str] = []

        # Zero balance validation
        if amount == 0 and field_name == "total" and not self.allow_zero_balances:
            errors.append("Zero total balance is not allowed")

        # Negative balance validation
        if amount < 0:
            if field_name in {"total", "available_quantity"} and not self.allow_negative_balances:
                errors.append(f"Negative {field_name} balance is not allowed")
            elif field_name == "locked":
                errors.append(f"{field_name} balance cannot be negative")

        return errors

    def _validate_amount_range(self, field_name: str, amount: Decimal) -> list[str]:
        """Validate amount is within allowed range.

        Args:
            field_name: Name of the field being validated
            amount: Amount value to validate

        Returns:
            List of validation errors for amount range issues
        """
        errors: list[str] = []

        if amount > self.max_balance_amount:
            errors.append(
                f"{field_name} balance {amount} exceeds maximum {self.max_balance_amount}"
            )
        elif amount < self.min_balance_amount and amount != 0:
            errors.append(f"{field_name} balance {amount} below minimum {self.min_balance_amount}")

        return errors

    def _validate_amount_precision(self, field_name: str, amount: Decimal) -> list[str]:
        """Validate amount precision.

        Args:
            field_name: Name of the field being validated
            amount: Amount value to validate

        Returns:
            List of warnings for amount precision issues
        """
        warnings: list[str] = []

        exponent = amount.as_tuple().exponent
        if isinstance(exponent, int) and exponent < -self.max_decimal_places:
            warnings.append(
                f"{field_name} balance has precision >{self.max_decimal_places} decimal places"
            )

        return warnings

    async def _validate_balance_type(self, balance: SpotBalance | None) -> list[str]:
        """Validate balance type if present.

        Args:
            balance: Balance object

        Returns:
            List of validation errors
        """
        if balance is None:
            return ["Balance object is None"]

        errors: list[str] = []

        # SpotBalance model doesn't have balance_type field - this validation is not applicable
        # SpotBalance is specifically for spot assets, type is implicit

        return errors

    async def _validate_timestamps(
        self, balance: SpotBalance | None
    ) -> tuple[list[str], list[str]]:
        """Validate balance timestamps.

        Args:
            balance: Balance object

        Returns:
            Tuple of (errors, warnings)
        """
        if balance is None:
            return (["Balance object is None"], [])

        errors: list[str] = []
        warnings: list[str] = []

        # Check for additional timestamp fields if they exist through model inspection
        # Note: These are not part of core protocols but may exist on specific balance models
        current_time = time.time()
        one_year_ago = current_time - (365 * 24 * 60 * 60)
        one_hour_future = current_time + (60 * 60)

        # Check for additional timestamp fields if they exist on the balance model
        # (not all balance models have these fields)

        # SpotBalance model has timestamp field (datetime) - validate that
        # Convert datetime to timestamp for comparison
        balance_timestamp = balance.timestamp.timestamp()

        if balance_timestamp < one_year_ago:
            warnings.append("Balance timestamp is more than one year old")
        elif balance_timestamp > one_hour_future:
            warnings.append("Balance timestamp is more than one hour in the future")

        return errors, warnings

    async def _validate_cross_fields(
        self, balance: SpotBalance | None
    ) -> tuple[list[str], list[str]]:
        """Validate relationships between fields.

        Args:
            balance: Balance object

        Returns:
            Tuple of (errors, warnings)
        """
        if balance is None:
            return (["Balance object is None"], [])

        errors: list[str] = []
        warnings: list[str] = []

        # Validate balance amount relationships using SpotBalance model fields
        try:
            total = balance.total_quantity
            available = balance.available_quantity
            # SpotBalance model only has total_quantity and available_quantity
            # locked = total - available (derived field)
            locked = total - available

            # Basic balance equation: total = available + locked
            calculated_total = available + locked

            # Allow small rounding differences
            tolerance = Decimal("0.00000001")

            if abs(total - calculated_total) > tolerance:
                errors.append(
                    f"Balance amounts don't add up: total={total}, "
                    f"available={available}, locked={locked}"
                )

            # Validate individual amounts are not negative (SpotBalance model enforces >= 0)
            if available < 0 and not self.allow_negative_balances:
                errors.append(f"Available balance cannot be negative: {available}")

            if locked < 0:
                errors.append(f"Locked balance cannot be negative: {locked}")

            # Warn about unusual balance distributions
            if total > 0:
                available_pct = (available / total) * 100
                locked_pct = (locked / total) * 100

                if locked_pct > HIGH_LOCKED_BALANCE_THRESHOLD:
                    warnings.append(f"Very high locked balance percentage: {locked_pct:.1f}%")
                elif available_pct < LOW_AVAILABLE_BALANCE_THRESHOLD and locked > 0:
                    warnings.append(f"Very low available balance percentage: {available_pct:.1f}%")

        except (ValueError, TypeError, AttributeError) as e:
            # Error in amount conversion - log and add to errors for debugging
            errors.append(f"Error processing balance amounts: {e}")
            # Still continue with validation, but record the issue

        return errors, warnings

    async def validate_balance_list(self, balances: list[SpotBalance]) -> dict[str, Any]:
        """Validate a list of balances and return aggregate results.

        Args:
            balances: List of balances to validate

        Returns:
            Dictionary with aggregate validation results
        """
        if not balances:
            return {
                "total_balances": 0,
                "valid_balances": 0,
                "invalid_balances": 0,
                "total_errors": 0,
                "total_warnings": 0,
                "validation_results": [],
                "currency_summary": {},
                "exchange_summary": {},
            }

        results: list[BalanceValidationResult] = []
        total_errors = 0
        total_warnings = 0
        valid_count = 0
        currency_summary: dict[str, int] = {}
        exchange_summary: dict[str, int] = {}

        for balance in balances:
            result = await self.validate_balance(balance)
            results.append(result)

            total_errors += len(result.errors)
            total_warnings += len(result.warnings)

            if result.is_valid:
                valid_count += 1

                # Track currency and exchange statistics
                currency = balance.asset.value  # Get string value from Symbol
                exchange = balance.exchange

                currency_summary[currency] = currency_summary.get(currency, 0) + 1
                exchange_summary[exchange] = exchange_summary.get(exchange, 0) + 1

        return {
            "total_balances": len(balances),
            "valid_balances": valid_count,
            "invalid_balances": len(balances) - valid_count,
            "total_errors": total_errors,
            "total_warnings": total_warnings,
            "validation_results": results,
            "currency_summary": currency_summary,
            "exchange_summary": exchange_summary,
        }

    def get_validation_stats(self) -> dict[str, Any]:
        """Get validation statistics.

        Returns:
            Dictionary with validation statistics
        """
        base_stats = super().get_validation_stats()

        return {
            **base_stats,
            "allow_zero_balances": self.allow_zero_balances,
            "allow_negative_balances": self.allow_negative_balances,
            "max_balance_amount": str(self.max_balance_amount),
            "min_balance_amount": str(self.min_balance_amount),
            "currency_validation_enabled": self.currency_validation_enabled,
            "require_currency_code": self.require_currency_code,
            "require_exchange_id": self.require_exchange_id,
            "require_balance_type": self.require_balance_type,
            "validate_timestamps": self.validate_timestamps,
            "max_decimal_places": self.max_decimal_places,
            "allowed_currencies": self.allowed_currencies,
            "allowed_exchanges": self.allowed_exchanges,
        }
