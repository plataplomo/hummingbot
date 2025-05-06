"""
Hyperliquid Raw Order Status Model
"""

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from .hl_raw_open_orders import HyperliquidRawOrder  # Assuming structure is similar


class HyperliquidRawOrderStatusResponse(BaseModel):
    """
    Pydantic model for the response structure from Hyperliquid's /info endpoint
    when querying order status (type='orderStatus').

    Ensures the presence of the 'order' field and that it conforms to the
    HyperliquidRawOrder model. Enforces immutability and forbids extra fields.
    """

    # Define fields based on expected response - assuming a single order object
    # Adjust based on actual API response if different.
    order: HyperliquidRawOrder = Field(..., description="The details of the queried order.")

    # Add other potential top-level fields if observed
    # e.g., state: str = Field(...) ?

    model_config = ConfigDict(
        extra="forbid",  # Reject unexpected fields
        frozen=True,  # Ensure immutability
        validate_assignment=True,
    )

    @field_validator("order", mode="before")
    @classmethod
    def validate_order_is_dict(cls, v: object, info: ValidationInfo) -> object:
        """
        Validates that the raw input for the 'order' field is a dictionary.
        This check runs before Pydantic attempts to validate it against HyperliquidRawOrder.

        Args:
            v (object): The raw input value for the 'order' field.
            info (ValidationInfo): Pydantic validation context.

        Returns:
            object: The input value `v` if it is a dictionary (Pydantic will then process it).

        Raises:
            TypeError: If `v` is not a dictionary.
        """
        if not isinstance(v, dict):
            # Use info.field_name to make the error message more specific
            field_name = info.field_name or "order"
            raise TypeError(f"Field '{field_name}' must be a dictionary, got {type(v).__name__}.")
        return v
