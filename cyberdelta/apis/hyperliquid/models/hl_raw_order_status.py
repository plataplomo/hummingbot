"""
Hyperliquid Raw Order Status Model
"""

from pydantic import BaseModel, ConfigDict, Field

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
