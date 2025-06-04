"""CyberDeltaEngine: Hyperliquid API Raw Models (Builder Fee Approval)
-----------------------------------------------------------------

Strict boundary validation models for the Hyperliquid 'checkBuilderFeeApproval' info endpoint.
Validates the raw structure only.
Never use for internal business logic.
"""

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawBuilderFeeApprovalResponse(BaseModel):
    """Raw boundary model for the builder fee approval status.
    NOTE: Response structure is assumed based on endpoint name, as docs lack example.
    Assuming a simple boolean status.
    Test test_builder_fee_approved_various_inputs requires coercion from "true"/"false".
    """

    approved: bool = Field(..., alias="approved")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)
