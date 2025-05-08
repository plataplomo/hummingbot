"""
CyberDeltaEngine: Hyperliquid API Raw Models (Subaccounts)
----------------------------------------------------------

Strict boundary validation models for the Hyperliquid 'subAccounts' info endpoint.
Validates the raw structure only (a list of addresses).
Never use for internal business logic.
"""

from pydantic import (
    ConfigDict,
    RootModel,
    ValidationInfo,
    field_validator,
)

from cyberdelta.apis.hyperliquid.models.common_raw_types import RawEthereumAddressStr


class HyperliquidRawSubAccountsResponse(RootModel[list[RawEthereumAddressStr]]):
    """
    Raw boundary model for the subaccounts list response.
    The root object is expected to be a list of strings (validated Ethereum addresses).
    """

    root: list[RawEthereumAddressStr]
    model_config = ConfigDict(frozen=True)

    @field_validator("root", mode="before")
    @classmethod
    def validate_address_list_structure(cls, v: object, info: ValidationInfo) -> object:
        """Ensures the root input is a list. Pydantic handles address validation."""
        field_name = info.field_name or "subaccounts_list"
        if not isinstance(v, list):
            raise ValueError(f"Field '{field_name}': Expected a list, got {type(v).__name__}.")

        return v
