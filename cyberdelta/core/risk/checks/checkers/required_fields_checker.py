"""Required fields checker implementation with direct configuration access."""

from decimal import Decimal
from typing import Final

from cyberdelta.config import AppSettings
from cyberdelta.core.risk.checks.checkers.typed_base_checker import TypedBaseChecker
from cyberdelta.core.risk.checks.models.check_result import CheckContext, CheckResult
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.validation.funding_data import ArbitrageOpportunity


class RequiredFieldsChecker(TypedBaseChecker[CheckResult]):
    """Checker that validates required fields are present and valid."""

    CHECKER_NAME: Final[str] = "required_fields"

    def __init__(self, app_settings: AppSettings) -> None:
        """Initialize the required fields checker with direct AppSettings access.

        Args:
            app_settings: The application settings instance
        """
        super().__init__(app_settings, self.CHECKER_NAME)

        # Required fields configuration (hardcoded as not in new config)
        self.required_fields: list[str] = [
            "long_exchange",
            "short_exchange",
            "symbol",
            "long_price",
            "short_price",
            "spread_percentage",
            "long_side",
            "short_side",
        ]

        # Numeric field validation
        self.numeric_fields: list[str] = [
            "long_price",
            "short_price",
            "spread_percentage",
        ]

        # Minimum values for numeric fields
        self.min_values: dict[str, Decimal] = {
            "long_price": Decimal(0),
            "short_price": Decimal(0),
            "spread_percentage": Decimal(0),
        }

        # Maximum values for numeric fields
        self.max_values: dict[str, Decimal] = {
            "spread_percentage": Decimal("1.0"),  # 100%
        }

    @property
    def name(self) -> str:
        """Name of the checker."""
        return "required_fields"

    async def _perform_check(
        self,
        opportunity: ArbitrageOpportunity,
        context: CheckContext,
    ) -> CheckResult:
        """Check that all required fields are present and valid.

        Args:
            opportunity: The arbitrage opportunity to check
            context: Context information for the check

        Returns:
            CheckResult indicating success/failure
        """
        missing_fields = self._check_missing_fields(opportunity)
        invalid_fields = self._check_invalid_fields(opportunity, missing_fields)

        details = {
            "required_fields": self.required_fields,
            "checked_fields": len(self.required_fields),
            "missing_fields": missing_fields,
            "invalid_fields": invalid_fields,
        }

        if missing_fields or invalid_fields:
            error_messages = self._build_error_messages(missing_fields, invalid_fields)
            return CheckResult.failure(
                message="; ".join(error_messages),
                details=details,
            )

        return CheckResult.success(
            message=f"All {len(self.required_fields)} required fields are valid",
            details=details,
        )

    def _check_missing_fields(self, opportunity: ArbitrageOpportunity) -> list[str]:
        """Check for missing or empty business logic fields.

        Args:
            opportunity: The arbitrage opportunity to check.

        Returns:
            List of missing or invalid field descriptions.
        """
        missing_fields: list[str] = []

        # Core required fields are enforced by Pydantic model validation
        # Only check for business logic requirements here

        # Symbol validation is redundant - Symbol objects are already validated at creation
        if not opportunity.long_exchange.strip():
            missing_fields.append("long_exchange (empty)")
        if not opportunity.short_exchange.strip():
            missing_fields.append("short_exchange (empty)")

        # Check for business logic requirements
        if opportunity.long_price <= 0:
            missing_fields.append("long_price (must be positive)")
        if opportunity.short_price <= 0:
            missing_fields.append("short_price (must be positive)")

        return missing_fields

    def _check_invalid_fields(
        self, opportunity: ArbitrageOpportunity, missing_fields: list[str]
    ) -> list[str]:
        """Check for invalid field values.

        Args:
            opportunity: The arbitrage opportunity to check.
            missing_fields: List of fields already identified as missing.

        Returns:
            List of invalid field descriptions.
        """
        invalid_fields: list[str] = []

        # Check numeric field validity
        invalid_fields.extend(self._validate_numeric_fields(opportunity, missing_fields))

        # Check exchange fields are different
        invalid_fields.extend(self._validate_exchange_fields(opportunity))

        # Check that sides are valid
        invalid_fields.extend(self._validate_side_fields(opportunity))

        return invalid_fields

    def _validate_numeric_fields(
        self, opportunity: ArbitrageOpportunity, missing_fields: list[str]
    ) -> list[str]:
        """Validate numeric field values.

        Args:
            opportunity: The arbitrage opportunity to check.
            missing_fields: List of fields already identified as missing.

        Returns:
            List of invalid numeric field descriptions.
        """
        invalid_fields: list[str] = []

        for field in self.numeric_fields:
            if field in missing_fields:
                continue

            value = getattr(opportunity, field, None)
            if value is None:
                continue

            try:
                decimal_value = self._convert_to_decimal(value, field)

                # Check minimum and maximum values
                if field in self.min_values and decimal_value < self.min_values[field]:
                    invalid_fields.append(
                        f"{field}: value {decimal_value} < minimum {self.min_values[field]}"
                    )
                if field in self.max_values and decimal_value > self.max_values[field]:
                    invalid_fields.append(
                        f"{field}: value {decimal_value} > maximum {self.max_values[field]}"
                    )
            except (ValueError, TypeError) as e:
                invalid_fields.append(f"{field}: invalid numeric value - {e!s}")

        return invalid_fields

    def _convert_to_decimal(self, value: str | float | Decimal, field: str) -> Decimal:
        """Convert value to Decimal.

        Args:
            value: The value to convert.
            field: The field name for error reporting.

        Returns:
            The value as a Decimal.

        Raises:
            TypeFieldError: If the value type cannot be converted.
        """
        if isinstance(value, str):
            return Decimal(value)
        if isinstance(value, Decimal):
            return value
        if isinstance(value, float):
            return Decimal(str(value))
        raise TypeFieldError(field, "str, int, float, or Decimal", type(value).__name__, value)

    def _validate_exchange_fields(self, opportunity: ArbitrageOpportunity) -> list[str]:
        """Validate exchange fields are different.

        Args:
            opportunity: The arbitrage opportunity to check.

        Returns:
            List of invalid exchange field descriptions.
        """
        invalid_fields: list[str] = []

        # Fields are guaranteed to exist by Pydantic model
        if (
            opportunity.long_exchange
            and opportunity.short_exchange
            and opportunity.long_exchange == opportunity.short_exchange
        ):
            invalid_fields.append("long_exchange and short_exchange must be different")

        return invalid_fields

    def _validate_side_fields(self, opportunity: ArbitrageOpportunity) -> list[str]:
        """Validate side field values.

        Args:
            opportunity: The arbitrage opportunity to check.

        Returns:
            List of invalid side field descriptions.
        """
        invalid_fields: list[str] = []
        valid_sides = ["buy", "sell", "long", "short"]

        # These fields are optional in the ArbitrageOpportunity model
        # Only validate if they exist in the model definition
        side_fields = [
            field
            for field in ["long_side", "short_side"]
            if field in type(opportunity).model_fields
        ]

        for side_field in side_fields:
            side_value = getattr(opportunity, side_field, None)
            if side_value and side_value.lower() not in valid_sides:
                invalid_fields.append(f"{side_field}: invalid side '{side_value}'")

        return invalid_fields

    def _build_error_messages(
        self, missing_fields: list[str], invalid_fields: list[str]
    ) -> list[str]:
        """Build error messages from missing and invalid fields.

        Args:
            missing_fields: List of missing field descriptions.
            invalid_fields: List of invalid field descriptions.

        Returns:
            List of formatted error messages.
        """
        error_messages: list[str] = []
        if missing_fields:
            error_messages.append(f"Missing required fields: {', '.join(missing_fields)}")
        if invalid_fields:
            error_messages.append(f"Invalid fields: {', '.join(invalid_fields)}")
        return error_messages

    def add_required_field(self, field: str) -> None:
        """Add a required field to the checker.

        Args:
            field: Name of the field to require
        """
        if field not in self.required_fields:
            self.required_fields.append(field)
            self.logger.info("Added required field", field=field)

    def remove_required_field(self, field: str) -> None:
        """Remove a required field from the checker.

        Args:
            field: Name of the field to remove
        """
        if field in self.required_fields:
            self.required_fields.remove(field)
            self.logger.info("Removed required field", field=field)

    def set_min_value(self, field: str, min_value: Decimal) -> None:
        """Set minimum value for a numeric field.

        Args:
            field: Name of the field
            min_value: Minimum allowed value
        """
        self.min_values[field] = min_value
        self.logger.info("Set minimum value", field=field, min_value=str(min_value))

    def set_max_value(self, field: str, max_value: Decimal) -> None:
        """Set maximum value for a numeric field.

        Args:
            field: Name of the field
            max_value: Maximum allowed value
        """
        self.max_values[field] = max_value
        self.logger.info("Set maximum value", field=field, max_value=str(max_value))

    def _create_skip_result(self) -> CheckResult:
        """Create result for skipped check.

        Returns:
            CheckResult indicating the check was skipped.
        """
        return CheckResult.skip(
            message=f"{self.CHECKER_NAME} check skipped (disabled)",
        )

    def _create_error_result(self, error: Exception, execution_time: float) -> CheckResult:
        """Create result for failed check.

        Args:
            error: The exception that occurred.
            execution_time: Time taken for execution in seconds.

        Returns:
            CheckResult indicating the check encountered an error.
        """
        return CheckResult.error(
            message=f"{self.CHECKER_NAME} check error: {error}",
            details={"execution_time_ms": execution_time},
        )
