"""
CyberDeltaEngine: Hyperliquid API Raw Models (Vault Details)
-----------------------------------------------------------

Strict boundary validation models for the Hyperliquid 'vaultDetails' info endpoint.
Validates the raw structure only, enforcing type and format constraints.
Never use for internal business logic.
"""

from typing import Any

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


class HyperliquidRawVaultPerformanceHistoryItem(BaseModel):
    """Raw boundary model for a single performance history entry."""

    time: int = Field(..., alias="time")
    pnl: str = Field(..., alias="pnl")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("time", mode="before")
    @classmethod
    def validate_timestamp_ms(cls, v: object, info: ValidationInfo) -> int:
        field_name = info.field_name or "time_field"
        if isinstance(v, str):
            try:
                v_int = int(v)
            except ValueError:
                raise ValueError(
                    f"{field_name}: Expected int or int-like string, got {type(v).__name__}"
                ) from None
        elif isinstance(v, int):
            v_int = v
        else:
            raise ValueError(
                f"{field_name}: Expected int or int-like string, got {type(v).__name__}"
            )
        if v_int < 0:
            raise ValueError(f"{field_name}: Timestamp cannot be negative")
        return v_int

    @field_validator("pnl", mode="before")
    @classmethod
    def validate_pnl_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "pnl_field"
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal")
        return s


class HyperliquidRawVaultUserEquity(BaseModel):
    """Raw boundary model for a user's equity details within a vault."""

    user: str = Field(..., alias="user")
    equity: str = Field(..., alias="equity")
    all_time_pnl: str = Field(..., alias="allTimePnl")
    days_following: int = Field(..., alias="daysFollowing")
    vault_entry_time: int = Field(..., alias="vaultEntryTime")
    lockup_until: int = Field(..., alias="lockupUntil")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("user", mode="before")
    @classmethod
    def validate_user_address(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "user_address_field"
        s = validate_str_field(v, field_name=field_name, max_length=42)
        if len(s) != 42:
            raise ValueError(f"{field_name}: Expected length 42, got {len(s)}")
        if not s.startswith("0x"):
            raise ValueError(f"{field_name}: Must start with 0x")
        return s

    @field_validator("equity", "all_time_pnl", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "decimal_str_field"
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal")
        return s

    @field_validator("days_following", "vault_entry_time", "lockup_until", mode="before")
    @classmethod
    def validate_int_or_timestamp_ms(cls, v: object, info: ValidationInfo) -> int:
        field_name = info.field_name or "int_timestamp_field"
        if isinstance(v, str):
            try:
                val = int(v)
            except ValueError:
                raise ValueError(
                    f"{field_name}: Expected int or int-like string, got {type(v).__name__}"
                ) from None
        elif isinstance(v, int):
            val = v
        else:
            raise ValueError(
                f"{field_name}: Expected int or int-like string, got {type(v).__name__}"
            )

        if val < 0:
            raise ValueError(f"{field_name}: Must be non-negative")
        return val


class HyperliquidRawVaultRelationshipData(BaseModel):
    """Raw boundary model for the 'data' field within the 'relationship' structure."""

    # This structure varies based on relationship type ('parent', 'child', etc.)
    # Handling explicitly for 'parent' type shown in docs.
    # Use a union or generics if other types need support.
    child_addresses: list[str] | None = Field(None, alias="childAddresses")
    master: str | None = Field(None, alias="master")  # If type is 'child'? Not in example

    model_config = ConfigDict(populate_by_name=True, extra="allow", frozen=True)
    # Using extra='allow' as the structure might vary significantly based on type
    # but forbidding extra fields on parent models helps contain this.

    @field_validator("child_addresses", mode="before")
    @classmethod
    def validate_child_addresses(cls, v: object, info: ValidationInfo) -> list[str] | None:
        field_name = info.field_name or "child_addresses_field"
        if v is None:
            return None
        if not isinstance(v, list):
            raise ValueError(f"{field_name}: Expected list or None")

        validated_list: list[str] = []
        # Explicitly type v after check for Pyright's benefit during iteration
        current_list: list[object] = v
        for item_idx, item_obj in enumerate(current_list):
            if not isinstance(item_obj, str):
                raise ValueError(
                    f"{field_name}[{item_idx}]: Expected string item, got {type(item_obj).__name__}"
                )
            # item_obj is now known to be str
            item_field_name = f"{field_name}[{item_idx}]"
            s = validate_str_field(item_obj, field_name=item_field_name, max_length=42)
            if len(s) != 42:
                raise ValueError(f"{item_field_name}: Expected length 42, got {len(s)}")
            validated_list.append(s)
        return validated_list

    @field_validator("master", mode="before")
    @classmethod
    def validate_master_address(cls, v: object, info: ValidationInfo) -> str | None:
        field_name = info.field_name or "master_address_field"
        if v is None:
            return None
        s = validate_str_field(v, field_name=field_name, max_length=42)
        if len(s) != 42:
            raise ValueError(f"{field_name}: Expected length 42, got {len(s)}")
        if not s.startswith("0x"):
            raise ValueError(f"{field_name}: Must start with 0x")
        return s


class HyperliquidRawVaultRelationship(BaseModel):
    """Raw boundary model for the vault relationship structure."""

    type: str = Field(..., alias="type")  # e.g., "parent", "child"
    data: HyperliquidRawVaultRelationshipData = Field(..., alias="data")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("type", mode="before")
    @classmethod
    def validate_type_str(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "type_field"
        # Basic validation, could restrict to known enum values if stable
        return validate_str_field(v, field_name=field_name, max_length=32)


class HyperliquidRawVaultDetailsResponse(BaseModel):
    """
    Strict boundary model for the Hyperliquid 'vaultDetails' info endpoint response.
    Validates the raw structure only.
    """

    name: str = Field(..., alias="name")
    description: str = Field(..., alias="description")
    allow_deposits: bool = Field(..., alias="allowDeposits")
    always_close_on_withdraw: bool = Field(..., alias="alwaysCloseOnWithdraw")
    creator: str = Field(..., alias="creator")
    vault_address: str = Field(..., alias="vaultAddress")
    max_balance: str | None = Field(None, alias="maxBalance")  # Nullable decimal string
    curr_balance: str = Field(..., alias="currBalance")  # Decimal string
    total_pnl: str = Field(..., alias="totalPnl")  # Decimal string
    all_time_pnl: str = Field(..., alias="allTimePnl")  # Decimal string
    performance_history: list[HyperliquidRawVaultPerformanceHistoryItem] = Field(
        ..., alias="performanceHistory"
    )
    user_equities: list[HyperliquidRawVaultUserEquity] = Field(..., alias="userEquities")
    max_distributable: str = Field(..., alias="maxDistributable")  # Decimal string
    max_withdrawable: str = Field(..., alias="maxWithdrawable")  # Decimal string
    is_closed: bool = Field(..., alias="isClosed")
    relationship: HyperliquidRawVaultRelationship = Field(..., alias="relationship")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("name", "description", mode="before")
    @classmethod
    def validate_name_desc(cls, v: object, info: ValidationInfo) -> str:
        # Allow potentially longer descriptions, adjust max_length if needed
        return validate_str_field(
            v, field_name=(info.field_name or "name_description_field"), max_length=1024
        )

    @field_validator("allow_deposits", "always_close_on_withdraw", "is_closed", mode="before")
    @classmethod
    def validate_bools(cls, v: object, info: ValidationInfo) -> bool:
        field_name = info.field_name or "bool_field"
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            if v.lower() == "true":
                return True
            if v.lower() == "false":
                return False
        raise ValueError(f"{field_name}: Expected boolean, got {type(v).__name__}")

    @field_validator("creator", "vault_address", mode="before")
    @classmethod
    def validate_address(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "address_field"
        s = validate_str_field(v, field_name=field_name, max_length=42)
        if len(s) != 42:
            raise ValueError(f"{field_name}: Expected length 42, got {len(s)}")
        if not s.startswith("0x"):
            raise ValueError(f"{field_name}: Must start with 0x")
        return s

    @field_validator(
        "curr_balance",
        "total_pnl",
        "all_time_pnl",
        "max_distributable",
        "max_withdrawable",
        mode="before",
    )
    @classmethod
    def validate_required_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "req_decimal_str_field"
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal")
        return s

    @field_validator("max_balance", mode="before")
    @classmethod
    def validate_optional_decimal_str(cls, v: object, info: ValidationInfo) -> str | None:
        field_name = info.field_name or "opt_decimal_str_field"
        if v is None:
            return None
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal")
        return s

    @field_validator("performance_history", "user_equities", mode="before")
    @classmethod
    def validate_list_structure(cls, v: object, info: ValidationInfo) -> list[dict[str, Any]]:
        field_name = info.field_name or "list_field"
        if not isinstance(v, list):
            raise ValueError(f"{field_name}: Expected list")

        # Explicitly type v after check for Pyright's benefit during iteration
        # and build a new list to satisfy the return type strictly.
        validated_items: list[dict[str, Any]] = []
        source_list: list[object] = v

        for item_idx, item_obj in enumerate(source_list):
            if not isinstance(item_obj, dict):
                raise ValueError(
                    f"{field_name}[{item_idx}]: Expected dict item, got {type(item_obj).__name__}"
                )
            # item_obj is now known to be a dict.
            # Pydantic expects dicts that can be parsed into the target model types.
            # We assume it's Dict[str, Any] for the purpose of this raw validator stage.
            validated_items.append(item_obj)
        return validated_items
