"""CyberDeltaEngine: Hyperliquid API Raw Models (Subaccounts).

----------------------------------------------------------

Strict boundary validation models for the Hyperliquid 'subAccounts' info endpoint.
Validates the raw structure only (a list of addresses).
Never use for internal business logic.
"""

from typing import cast

from pydantic import (
    ConfigDict,
    RootModel,
    ValidationInfo,
    field_validator,
)

from cyberdelta.apis.hyperliquid.models.hl_common_raw_types import RawLaxEthereumAddressStrHL


class HyperliquidRawSubAccountsResponse(RootModel[list[RawLaxEthereumAddressStrHL]]):
    """Raw boundary model for the subaccounts list response.

    The root object is expected to be a list of strings (validated Ethereum addresses).
    """

    root: list[RawLaxEthereumAddressStrHL]
    model_config = ConfigDict(frozen=True)

    @field_validator("root", mode="before")
    @classmethod
    def validate_address_list_structure(cls, v: object, info: ValidationInfo) -> list[object]:
        """Ensure the root input is a list. Pydantic handles address validation."""
        field_name = info.field_name or "subaccounts_list"
        if not isinstance(v, list):
            raise TypeError(f"Field '{field_name}': Expected a list, got {type(v).__name__}.")

        return cast("list[object]", v)
