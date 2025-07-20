"""Trading data transformation exceptions for CyberDelta.

These exceptions handle transformation errors specific to trading data
such as orders, trades, and trading-related enums.
"""

from cyberdelta.apis.common.api_error import TransformationError
from cyberdelta.apis.exceptions.data_transformation import UnknownEnumError


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


class MissingQuantityError(TransformationError):
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
        )
        self.order_id = order_id
        self.details = {"order_id": order_id} if order_id else {}


class InvalidQuantityError(TransformationError):
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


class MissingTimestampError(TransformationError):
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
        )
        self.order_id = order_id
        self.details = {"order_id": order_id} if order_id else {}
