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
    ValidationInfo,
    field_validator,
)

from cyberdelta.utils.parsing import (
    parse_decimal_value,
    validate_str_field,
)


class HyperliquidRawUserVaultEquityItem(BaseModel):
    """Raw boundary model for a single user vault equity entry."""

    vault_address: str = Field(..., alias="vaultAddress")
    equity: str = Field(..., alias="equity")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("vault_address", mode="before")
    @classmethod
    def validate_vault_address(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "vault_address_field"
        s = validate_str_field(v, field_name=field_name, max_length=42)
        if len(s) != 42:
            raise ValueError(f"{field_name}: Expected length 42, got {len(s)}")
        if not s.startswith("0x"):
            raise ValueError(f"{field_name}: Must start with 0x")
        return s

    @field_validator("equity", mode="before")
    @classmethod
    def validate_equity_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "equity_field"
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal")
        return s


# The overall response is a list of these items
# Using RootModel might be overkill if it's just a list.
# We can validate the list structure in the handler if needed.
# For now, defining the item model is sufficient for boundary validation of items.
