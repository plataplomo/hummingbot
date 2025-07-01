"""Trading data transformation exceptions for CyberDelta.

These exceptions handle transformation errors specific to trading data
such as orders, trades, and trading-related enums.
"""

from cyberdelta.apis.exceptions.data_transformation import MappingError, UnknownEnumError


class UnknownOrderSideError(UnknownEnumError):
    """Raised when an unknown order side value is encountered."""

    def __init__(self, side: str, exchange: str | None = None) -> None:
        """Initialize unknown order side error.

        Args:
            side: The unknown side value
            exchange: Optional exchange name
        """
        prefix = f"[{exchange}] " if exchange else ""
        super().__init__(
            enum_type="order side",
            value=side,
            valid_values=["BUY", "SELL", "Bid", "Ask"],
        )
        self.message = f"{prefix}Unknown Backpack order side: '{side}'"
        self.side = side
        self.exchange = exchange


class MissingQuantityError(MappingError):
    """Raised when required quantity field is missing or invalid."""

    def __init__(
        self,
        field_name: str = "quantity_requested",
        order_id: str | None = None,
        reason: str | None = None,
    ) -> None:
        """Initialize missing quantity error.

        Args:
            field_name: Name of the quantity field
            order_id: Optional order ID for context
            reason: Optional specific reason
        """
        message = f"{field_name} is required"
        if reason:
            message = f"{message} {reason}"
        if order_id:
            message = f"{message} (order: {order_id})"

        super().__init__(
            message=message,
            field_name=field_name,
            details={"order_id": order_id} if order_id else {},
        )
        self.order_id = order_id


class OrderTransformationFailedError(MappingError):
    """Raised when order transformation fails."""

    def __init__(
        self,
        source_type: str,
        reason: str | Exception,
        order_id: str | None = None,
        exchange: str | None = None,
    ) -> None:
        """Initialize order transformation error.

        Args:
            source_type: Type of source order (e.g., "Backpack order data", "BackpackRawOrder")
            reason: Reason for transformation failure
            order_id: Optional order ID
            exchange: Optional exchange name
        """
        self.source_type = source_type
        self.reason = reason
        self.order_id = order_id
        self.exchange = exchange

        prefix = f"[{exchange}] " if exchange else ""
        id_part = f" {order_id}" if order_id else ""
        message = f"{prefix}Failed to transform {source_type}{id_part} to Order: {reason}"

        super().__init__(
            message=message,
            source_type=source_type,
            target_type="Order",
            details={
                "order_id": order_id,
                "exchange": exchange,
            },
            original_exception=reason if isinstance(reason, Exception) else None,
        )


class InvalidQuantityError(MappingError):
    """Raised when quantity value is invalid."""

    def __init__(
        self,
        field_name: str,
        value: object,
        constraint: str = "must be > 0",
    ) -> None:
        """Initialize invalid quantity error.

        Args:
            field_name: Name of the quantity field
            value: The invalid value
            constraint: Description of the constraint
        """
        super().__init__(
            message=f"{field_name} is required and {constraint}",
            field_name=field_name,
            source_value=value,
        )
        self.constraint = constraint


class MissingTimestampError(MappingError):
    """Raised when required timestamp field is missing."""

    def __init__(
        self,
        field_name: str,
        order_id: str | None = None,
    ) -> None:
        """Initialize missing timestamp error.

        Args:
            field_name: Name of the timestamp field
            order_id: Optional order ID for context
        """
        message = f"{field_name} is required"
        if order_id:
            message = f"{message} (order: {order_id})"

        super().__init__(
            message=message,
            field_name=field_name,
            details={"order_id": order_id} if order_id else {},
        )
        self.order_id = order_id
