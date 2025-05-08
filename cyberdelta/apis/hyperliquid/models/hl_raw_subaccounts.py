"""
CyberDeltaEngine: Hyperliquid API Raw Models (Subaccounts)
----------------------------------------------------------

Strict boundary validation models for the Hyperliquid 'subAccounts' info endpoint.
Validates the raw structure only (a list of addresses).
Never use for internal business logic.
"""

from pydantic import (
    RootModel,
    field_validator,
)

from cyberdelta.utils.parsing import validate_str_field


class HyperliquidRawSubAccountsResponse(RootModel[list[str]]):
    """
    Raw boundary model for the subaccounts list response.
    The root object is expected to be a list of strings (addresses).
    """

    root: list[str]

    @field_validator("root", mode="before")
    @classmethod
    def validate_address_list(cls, v: object) -> list[str]:
        if not isinstance(v, list):
            raise ValueError("Expected a list of addresses")

        validated_list: list[str] = []
        for item_idx, item in enumerate(v):
            if not isinstance(item, str):
                raise ValueError(
                    f"Item {item_idx}: Expected string address, got {type(item).__name__}"
                )

            field_name = f"address[{item_idx}]"
            s = validate_str_field(item, field_name=field_name, max_length=42)
            if len(s) != 42:
                raise ValueError(f"{field_name}: Expected length 42, got {len(s)}")
            if not s.startswith("0x"):
                raise ValueError(f"{field_name}: Must start with 0x")
            validated_list.append(s)
        return validated_list
