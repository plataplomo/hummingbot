"""Hyperliquid Request Builder Base Class.

This module provides shared utilities for all Hyperliquid request builders,
preventing code duplication and ensuring consistent decimal handling across
trading, account, and market data operations.
"""

from __future__ import annotations

from decimal import Decimal

from cyberdelta.apis.exceptions import (
    DecimalFormatError,
    DecimalRangeError,
    PrecisionLossError,
)


# Constants
PRECISION_TOLERANCE = 1e-12  # Tolerance for floating point precision checks


class HyperliquidRequestBuilderBase:
    """Base class for Hyperliquid request builders with shared utilities.

    This class provides common functionality used across all Hyperliquid
    request builders, particularly decimal formatting and validation.
    """

    @staticmethod
    def _validate_decimal_input(value: Decimal) -> None:
        """Validate decimal input for wire format conversion.
        
        Args:
            value: Decimal value to validate
            
        Raises:
            DecimalFormatError: If the decimal value is not finite
            DecimalRangeError: If the decimal value is outside acceptable range
        """
        if not value.is_finite():
            raise DecimalFormatError(value, "must be finite")

        if abs(value) > Decimal("1e18"):
            raise DecimalRangeError(
                value,
                "too large for wire format",
                max_value=Decimal("1e18"),
            )

        if value != 0 and abs(value) < Decimal("1e-8"):
            raise DecimalRangeError(
                value,
                "too small for wire format precision",
                min_value=Decimal("1e-8"),
            )

    @staticmethod
    def _format_and_validate_precision(value: Decimal) -> str:
        """Format decimal with precision validation.
        
        Args:
            value: Decimal value to format
            
        Returns:
            str: Formatted decimal string with 8 decimal places
            
        Raises:
            DecimalFormatError: If the value cannot be converted to float
            PrecisionLossError: If formatting results in precision loss
        """
        try:
            x = float(value)
        except (ValueError, OverflowError) as e:
            raise DecimalFormatError(value, f"cannot convert to float: {e}") from e

        rounded = f"{x:.8f}"

        # Check for rounding errors
        precision_loss = abs(float(rounded) - x)
        if precision_loss >= PRECISION_TOLERANCE:
            raise PrecisionLossError(
                value=Decimal(str(value)),
                precision_loss=precision_loss,
                tolerance=PRECISION_TOLERANCE,
            )

        return rounded

    @staticmethod
    def _normalize_wire_format(rounded: str, original_value: Decimal) -> str:
        """Normalize wire format string with proper decimal handling.
        
        Args:
            rounded: Formatted decimal string
            original_value: Original decimal value for error reporting
            
        Returns:
            str: Normalized wire format string ready for API transmission
            
        Raises:
            DecimalFormatError: If normalization fails
        """
        # Handle negative zero
        if rounded == "-0.00000000":
            rounded = "0.00000000"

        try:
            normalized = Decimal(rounded).normalize()
            result = f"{normalized:f}"

            # Ensure at least one decimal place for Hyperliquid API compatibility
            if "." not in result:
                result += ".0"

            # Final validation - ensure result is parseable
            _ = Decimal(result)
        except Exception as e:
            raise DecimalFormatError(
                original_value,
                f"failed to normalize wire format: {e}",
            ) from e
        else:
            return result

    @staticmethod
    def _decimal_to_wire_format(value: Decimal | None) -> str:
        """Convert a Decimal to the string wire format with validation.

        This is the authoritative conversion method that ensures all decimal values
        sent to Hyperliquid API are properly formatted. The wire format must be
        decimal-represented for financial precision.

        Args:
            value: Decimal value to convert to wire format, or None for market orders

        Returns:
            str: String representation ready for API transmission
        """
        if value is None:
            return "0"

        HyperliquidRequestBuilderBase._validate_decimal_input(value)
        rounded = HyperliquidRequestBuilderBase._format_and_validate_precision(value)
        return HyperliquidRequestBuilderBase._normalize_wire_format(rounded, value)
