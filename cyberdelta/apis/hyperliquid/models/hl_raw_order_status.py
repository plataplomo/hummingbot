"""
Hyperliquid Raw Order Status Model
"""

from pydantic import BaseModel, Field

from .hl_raw_open_orders import HyperliquidRawOrder  # Assuming structure is similar


class HyperliquidRawOrderStatusResponse(BaseModel):
    """
    Represents the expected response structure for an orderStatus query.
    NOTE: This structure is assumed based on SDK usage and common patterns,
    as it's not explicitly detailed in the synthesized OpenAPI spec.
    It likely returns a single order object.
    """

    # Define fields based on expected response - assuming a single order object
    # Adjust based on actual API response if different.
    order: HyperliquidRawOrder = Field(..., description="The details of the queried order.")

    # Add other potential top-level fields if observed
    # e.g., state: str = Field(...) ?
