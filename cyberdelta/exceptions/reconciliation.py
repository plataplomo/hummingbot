"""Position reconciliation exceptions for CyberDelta.

These exceptions handle errors during position reconciliation and validation processes.
"""

from decimal import Decimal
from typing import Any


class ReconciliationError(Exception):
    """Base class for reconciliation-related errors."""

    def __init__(
        self,
        message: str,
        *,
        exchange_id: str | None = None,
        symbol: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Initialize reconciliation error.

        Args:
            message: Human-readable error description
            exchange_id: Exchange identifier
            symbol: Trading symbol
            metadata: Additional metadata for debugging
        """
        super().__init__(message)
        self.exchange_id = exchange_id
        self.symbol = symbol
        self.metadata = metadata or {}


class PositionFieldError(ReconciliationError):
    """Position field validation errors during reconciliation."""

    def __init__(
        self,
        field_name: str,
        value: object,
        reason: str,
        *,
        exchange_id: str | None = None,
        symbol: str | None = None,
    ) -> None:
        """Initialize position field error.

        Args:
            field_name: Name of the position field that failed validation
            value: The value that failed validation
            reason: Reason for validation failure
            exchange_id: Exchange identifier
            symbol: Trading symbol
        """
        message = f"Position field '{field_name}' invalid: {reason}"
        if exchange_id and symbol:
            message = f"{message} for {exchange_id}/{symbol}"

        super().__init__(
            message,
            exchange_id=exchange_id,
            symbol=symbol,
            metadata={"field_name": field_name, "value": str(value), "reason": reason},
        )
        self.field_name = field_name
        self.value = value
        self.reason = reason


class NonFinitePositionValueError(PositionFieldError):
    """Raised when a position decimal field contains non-finite values (NaN, Inf)."""

    def __init__(
        self,
        field_name: str,
        value: Decimal,
        *,
        exchange_id: str | None = None,
        symbol: str | None = None,
    ) -> None:
        """Initialize non-finite position value error.

        Args:
            field_name: Name of the field with non-finite value
            value: The non-finite decimal value
            exchange_id: Exchange identifier
            symbol: Trading symbol
        """
        super().__init__(
            field_name=field_name,
            value=value,
            reason=f"Invalid or non-finite {field_name}: {value}",
            exchange_id=exchange_id,
            symbol=symbol,
        )


class PositionDiscrepancyError(ReconciliationError):
    """Errors when position discrepancies exceed thresholds."""

    def __init__(
        self,
        discrepancy_type: str,
        source1_name: str,
        source1_value: object,
        source2_name: str,
        source2_value: object,
        *,
        exchange_id: str | None = None,
        symbol: str | None = None,
        threshold: float | None = None,
    ) -> None:
        """Initialize position discrepancy error.

        Args:
            discrepancy_type: Type of discrepancy found (e.g., 'size', 'pnl')
            source1_name: Name of first source
            source1_value: Value from first source
            source2_name: Name of second source
            source2_value: Value from second source
            exchange_id: Exchange identifier
            symbol: Trading symbol
            threshold: Reconciliation threshold exceeded
        """
        message = (
            f"Position {discrepancy_type} discrepancy: "
            f"{source1_name}={source1_value} vs {source2_name}={source2_value}"
        )
        if threshold is not None:
            message = f"{message} (exceeds {threshold}% threshold)"
        if exchange_id and symbol:
            message = f"{message} for {exchange_id}/{symbol}"

        super().__init__(
            message,
            exchange_id=exchange_id,
            symbol=symbol,
            metadata={
                "discrepancy_type": discrepancy_type,
                "source1_name": source1_name,
                "source1_value": source1_value,
                "source2_name": source2_name,
                "source2_value": source2_value,
                "threshold": threshold,
            },
        )
        self.discrepancy_type = discrepancy_type
        self.source1_name = source1_name
        self.source1_value = source1_value
        self.source2_name = source2_name
        self.source2_value = source2_value
        self.threshold = threshold
