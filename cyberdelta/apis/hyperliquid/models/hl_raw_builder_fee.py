"""
CyberDeltaEngine: Hyperliquid API Raw Models (Builder Fee Approval)
-----------------------------------------------------------------

Strict boundary validation models for the Hyperliquid 'checkBuilderFeeApproval' info endpoint.
Validates the raw structure only.
Never use for internal business logic.
"""

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.apis.hyperliquid.models.common_raw_types import RawStrictBool


class HyperliquidRawBuilderFeeApprovalResponse(BaseModel):
    """
    Raw boundary model for the builder fee approval status.
    NOTE: Response structure is assumed based on endpoint name, as docs lack example.
    Assuming a simple boolean status.
    """

    approved: RawStrictBool = Field(..., alias="approved")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)
