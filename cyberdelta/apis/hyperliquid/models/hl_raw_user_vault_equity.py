"""
CyberDeltaEngine: Hyperliquid API Raw Models (User Vault Equity)
---------------------------------------------------------------

Strict boundary validation models for the Hyperliquid 'userVaultEquities' info endpoint.
Validates the raw structure only.
Never use for internal business logic.
"""

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)

from cyberdelta.apis.hyperliquid.models.common_raw_types import (
    RawFiniteDecimalStr,
    RawLaxEthereumAddressStrHL,
)


class HyperliquidRawUserVaultEquityItem(BaseModel):
    """Raw boundary model for a single user vault equity entry."""

    vault_address: RawLaxEthereumAddressStrHL = Field(..., alias="vaultAddress")
    equity: RawFiniteDecimalStr = Field(..., alias="equity")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# The overall response is a list of these items
# Using RootModel might be overkill if it's just a list.
# We can validate the list structure in the handler if needed.
# For now, defining the item model is sufficient for boundary validation of items.
