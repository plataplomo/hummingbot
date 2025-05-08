"""
CyberDeltaEngine: Hyperliquid API Raw Models (Builder Fee Approval)
-----------------------------------------------------------------

Strict boundary validation models for the Hyperliquid 'checkBuilderFeeApproval' info endpoint.
Validates the raw structure only.
Never use for internal business logic.
"""

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
)


class HyperliquidRawBuilderFeeApprovalResponse(BaseModel):
    """
    Raw boundary model for the builder fee approval status.
    NOTE: Response structure is assumed based on endpoint name, as docs lack example.
    Assuming a simple boolean status.
    """

    approved: bool = Field(..., alias="approved")  # Assuming field name is 'approved'

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("approved", mode="before")
    @classmethod
    def validate_approved_bool(cls, v: object, info: ValidationInfo) -> bool:
        field_name = info.field_name or "approved"
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            if v.lower() == "true":
                return True
            if v.lower() == "false":
                return False
        raise ValueError(f"{field_name}: Expected boolean, got {type(v).__name__}")
