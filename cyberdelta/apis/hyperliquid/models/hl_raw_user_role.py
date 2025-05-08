"""
CyberDeltaEngine: Hyperliquid API Raw Models (User Role)
-------------------------------------------------------

Strict boundary validation models for the Hyperliquid 'userRole' info endpoint.
Validates the raw structure only.
Never use for internal business logic.
"""

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)

from cyberdelta.apis.hyperliquid.models.common_raw_types import (
    RawEthereumAddressStr,
    RawUserRoleString,
)


class HyperliquidRawUserRoleData(BaseModel):
    """
    Raw boundary model for the optional 'data' field in the userRole response.
    Structure varies based on the role.
    """

    user: RawEthereumAddressStr | None = Field(None, alias="user")
    master: RawEthereumAddressStr | None = Field(None, alias="master")

    model_config = ConfigDict(populate_by_name=True, extra="allow", frozen=True)


class HyperliquidRawUserRoleResponse(BaseModel):
    """
    Raw boundary model for the user role response.
    """

    role: RawUserRoleString = Field(..., alias="role")
    data: HyperliquidRawUserRoleData | None = Field(None, alias="data")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)
