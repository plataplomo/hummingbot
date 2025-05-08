"""
CyberDeltaEngine: Hyperliquid API Raw Models (Staking Info)
----------------------------------------------------------

Strict boundary validation models for the Hyperliquid staking-related info endpoints:
- delegations
- delegatorSummary
- delegatorHistory
- delegatorRewards
Validates the raw structure only.
Never use for internal business logic.
"""

from typing import Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    RootModel,
)

from cyberdelta.utils.parsing import (
    parse_decimal_value,
    parse_int_value,
    validate_bool_field,
    validate_str_field,
)

# --- Delegations --- #

class HyperliquidRawDelegationItem(BaseModel):
    """Raw boundary model for a single delegation entry."""

    validator: str = Field(..., alias="validator")
    amount: str = Field(..., alias="amount")
    locked_until_timestamp: int = Field(..., alias="lockedUntilTimestamp")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("validator", mode="before")
    @classmethod
    def validate_validator_address(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "validator"
        s = validate_str_field(v, field_name=field_name, max_length=42)
        if len(s) != 42: raise ValueError(f"{field_name}: Expected length 42")
        if not s.startswith("0x"): raise ValueError(f"{field_name}: Must start with 0x")
        return s

    @field_validator("amount", mode="before")
    @classmethod
    def validate_amount_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "amount"
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite(): raise ValueError(f"{field_name}: Finite decimal required")
        return s

    @field_validator("locked_until_timestamp", mode="before")
    @classmethod
    def validate_timestamp_ms(cls, v: object, info: ValidationInfo) -> int:
        field_name = info.field_name or "locked_until_timestamp"
        if isinstance(v, str): try: v_int = int(v)
        except ValueError: raise ValueError(f"{field_name}: Expected int or int-like string") from None
        elif isinstance(v, int): v_int = v
        else: raise ValueError(f"{field_name}: Expected int or int-like string")
        if v_int < 0: raise ValueError(f"{field_name}: Timestamp cannot be negative")
        return v_int

# Response for 'delegations' is RootModel[list[HyperliquidRawDelegationItem]]
class HyperliquidRawDelegationsResponse(RootModel[list[HyperliquidRawDelegationItem]]):
    """Raw boundary model for the 'delegations' list response."""
    root: list[HyperliquidRawDelegationItem]

    @field_validator("root", mode="before")
    @classmethod
    def validate_delegations_list(cls, v: object) -> list[Any]:
        if not isinstance(v, list):
            raise ValueError("Expected a list of delegations")
        for item_idx, item in enumerate(v):
            if not isinstance(item, dict):
                raise ValueError(f"Item {item_idx}: Expected dict delegation")
        return v


# --- Delegator Summary --- #

class HyperliquidRawDelegatorSummaryResponse(BaseModel):
    """Raw boundary model for the 'delegatorSummary' response."""

    delegated: str = Field(..., alias="delegated")
    undelegated: str = Field(..., alias="undelegated")
    total_pending_withdrawal: str = Field(..., alias="totalPendingWithdrawal")
    n_pending_withdrawals: int = Field(..., alias="nPendingWithdrawals")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("delegated", "undelegated", "total_pending_withdrawal", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "decimal_str_field"
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite(): raise ValueError(f"{field_name}: Finite decimal required")
        return s

    @field_validator("n_pending_withdrawals", mode="before")
    @classmethod
    def validate_non_negative_int(cls, v: object, info: ValidationInfo) -> int:
        field_name = info.field_name or "n_pending_withdrawals"
        if isinstance(v, str): try: v_int = int(v)
        except ValueError: raise ValueError(f"{field_name}: Expected int or int-like string") from None
        elif isinstance(v, int): v_int = v
        else: raise ValueError(f"{field_name}: Expected int or int-like string")
        if v_int < 0: raise ValueError(f"{field_name}: Must be non-negative")
        return v_int

# --- Delegator History --- #

class HyperliquidRawDelegatorHistoryDelegateDelta(BaseModel):
    """Raw boundary model for the 'delegate' details within history delta."""
    validator: str = Field(..., alias="validator")
    amount: str = Field(..., alias="amount")
    is_undelegate: bool = Field(..., alias="isUndelegate")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("validator", mode="before")
    @classmethod
    def validate_validator_address(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "validator"
        s = validate_str_field(v, field_name=field_name, max_length=42)
        if len(s) != 42: raise ValueError(f"{field_name}: Expected length 42")
        if not s.startswith("0x"): raise ValueError(f"{field_name}: Must start with 0x")
        return s

    @field_validator("amount", mode="before")
    @classmethod
    def validate_amount_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "amount"
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite(): raise ValueError(f"{field_name}: Finite decimal required")
        return s

    @field_validator("is_undelegate", mode="before")
    @classmethod
    def validate_is_undelegate_bool(cls, v: object, info: ValidationInfo) -> bool:
        field_name = info.field_name or "is_undelegate"
        if isinstance(v, bool): return v
        if isinstance(v, str):
            if v.lower() == "true": return True
            if v.lower() == "false": return False
        raise ValueError(f"{field_name}: Expected boolean")

class HyperliquidRawDelegatorHistoryDelta(BaseModel):
    """Raw boundary model for the 'delta' object within history items."""
    # Structure depends on the type of history event, handle 'delegate' type.
    delegate: HyperliquidRawDelegatorHistoryDelegateDelta | None = Field(None, alias="delegate")
    # Add other delta types if needed, e.g., withdraw

    model_config = ConfigDict(populate_by_name=True, extra="allow", frozen=True)

class HyperliquidRawDelegatorHistoryItem(BaseModel):
    """Raw boundary model for a single delegator history entry."""
    time: int = Field(..., alias="time")
    hash: str = Field(..., alias="hash")
    delta: HyperliquidRawDelegatorHistoryDelta = Field(..., alias="delta")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("time", mode="before")
    @classmethod
    def validate_timestamp_ms(cls, v: object, info: ValidationInfo) -> int:
        field_name = info.field_name or "time"
        if isinstance(v, str): try: v_int = int(v)
        except ValueError: raise ValueError(f"{field_name}: Expected int or int-like string") from None
        elif isinstance(v, int): v_int = v
        else: raise ValueError(f"{field_name}: Expected int or int-like string")
        if v_int < 0: raise ValueError(f"{field_name}: Timestamp cannot be negative")
        return v_int

    @field_validator("hash", mode="before")
    @classmethod
    def validate_hash_str(cls, v: object, info: ValidationInfo) -> str:
        # Assume standard 0x prefixed hash, length 66
        field_name = info.field_name or "hash"
        s = validate_str_field(v, field_name=field_name, max_length=66)
        if len(s) != 66: raise ValueError(f"{field_name}: Expected length 66")
        if not s.startswith("0x"): raise ValueError(f"{field_name}: Must start with 0x")
        return s

# Response for 'delegatorHistory' is RootModel[list[HyperliquidRawDelegatorHistoryItem]]
class HyperliquidRawDelegatorHistoryResponse(RootModel[list[HyperliquidRawDelegatorHistoryItem]]):
    """Raw boundary model for the 'delegatorHistory' list response."""
    root: list[HyperliquidRawDelegatorHistoryItem]

    @field_validator("root", mode="before")
    @classmethod
    def validate_history_list(cls, v: object) -> list[Any]:
        if not isinstance(v, list):
            raise ValueError("Expected a list of history items")
        for item_idx, item in enumerate(v):
            if not isinstance(item, dict):
                raise ValueError(f"Item {item_idx}: Expected dict history item")
        return v

# --- Delegator Rewards --- #

class HyperliquidRawDelegatorRewardItem(BaseModel):
    """Raw boundary model for a single delegator reward entry."""
    time: int = Field(..., alias="time")
    source: str = Field(..., alias="source") # e.g., "delegation", "commission"
    total_amount: str = Field(..., alias="totalAmount")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("time", mode="before")
    @classmethod
    def validate_timestamp_ms(cls, v: object, info: ValidationInfo) -> int:
        field_name = info.field_name or "time"
        if isinstance(v, str): try: v_int = int(v)
        except ValueError: raise ValueError(f"{field_name}: Expected int or int-like string") from None
        elif isinstance(v, int): v_int = v
        else: raise ValueError(f"{field_name}: Expected int or int-like string")
        if v_int < 0: raise ValueError(f"{field_name}: Timestamp cannot be negative")
        return v_int

    @field_validator("source", mode="before")
    @classmethod
    def validate_source_str(cls, v: object, info: ValidationInfo) -> str:
        # Could validate against known sources if necessary
        return validate_str_field(v, field_name="source", max_length=32)

    @field_validator("total_amount", mode="before")
    @classmethod
    def validate_amount_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "total_amount"
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite(): raise ValueError(f"{field_name}: Finite decimal required")
        return s

# Response for 'delegatorRewards' is RootModel[list[HyperliquidRawDelegatorRewardItem]]
class HyperliquidRawDelegatorRewardsResponse(RootModel[list[HyperliquidRawDelegatorRewardItem]]):
    """Raw boundary model for the 'delegatorRewards' list response."""
    root: list[HyperliquidRawDelegatorRewardItem]

    @field_validator("root", mode="before")
    @classmethod
    def validate_rewards_list(cls, v: object) -> list[Any]:
        if not isinstance(v, list):
            raise ValueError("Expected a list of reward items")
        for item_idx, item in enumerate(v):
            if not isinstance(item, dict):
                raise ValueError(f"Item {item_idx}: Expected dict reward item")
        return v 