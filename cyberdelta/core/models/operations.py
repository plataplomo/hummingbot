"""Operations Models for CyberDeltaEngine.

This module contains models representing financial operations such as transfers
and withdrawals. These models follow the "Core + Typed Extension Slots" pattern
to support exchange-specific enrichment while maintaining a unified interface
for the core application logic.

The models are immutable (frozen=True) to ensure data integrity and represent
completed or in-progress operations with their current status and metadata.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.core.enums import InternalTransferStatus, InternalWithdrawalStatus


# --- Transfer Details Models (Immutable) ---


class HyperliquidTransferDetails(BaseModel):
    """Hyperliquid-specific transfer enrichment fields. Immutable."""

    from_user: str | None = Field(default=None, description="Source user for Hyperliquid transfer.")
    to_user: str | None = Field(
        default=None,
        description="Destination user for Hyperliquid transfer.",
    )

    model_config = ConfigDict(extra="ignore", frozen=True)


class BackpackTransferDetails(BaseModel):
    """Backpack-specific transfer enrichment fields. Immutable."""

    client_id: str | None = Field(
        default=None,
        description="Client ID provided in the Backpack transfer request.",
    )
    from_account_type: str | None = Field(
        default=None,
        description="Account type transferred from (Backpack specific).",
    )
    to_account_type: str | None = Field(
        default=None,
        description="Account type transferred to (Backpack specific).",
    )

    model_config = ConfigDict(extra="ignore", frozen=True)


# --- Core Transfer Model (Immutable) ---


class Transfer(BaseModel):
    """Core internal model for a funds transfer operation. Immutable.

    Represents the state or result of a transfer.
    """

    id: str = Field(description="Unique identifier for the transfer.")
    exchange: str = Field(description="Name of the exchange where the transfer occurred.")
    status: InternalTransferStatus = Field(description="Internal status of the transfer.")
    asset: str = Field(description="The asset symbol that was transferred (e.g., 'USDC', 'ETH').")
    quantity: Decimal = Field(description="The amount of the asset transferred.")
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="Timestamp of the transfer event or creation (UTC).",
    )
    response_message: str | None = Field(
        default=None,
        description="Optional message from the exchange regarding the operation.",
    )

    # Extension Slots
    hl_details: HyperliquidTransferDetails | None = Field(default=None)
    bp_details: BackpackTransferDetails | None = Field(default=None)

    model_config = ConfigDict(extra="forbid", frozen=True, validate_assignment=True)


# --- Withdrawal Details Models (Immutable) ---


class HyperliquidWithdrawalDetails(BaseModel):
    """Hyperliquid-specific withdrawal enrichment fields. Immutable."""

    usd_value: Decimal | None = Field(
        default=None,
        description="USD value of the withdrawal on Hyperliquid.",
    )

    model_config = ConfigDict(extra="ignore", frozen=True)


class BackpackWithdrawalDetails(BaseModel):
    """Backpack-specific withdrawal enrichment fields. Immutable."""

    blockchain: str | None = Field(
        default=None,
        description="Blockchain network used for the Backpack withdrawal.",
    )
    is_internal: bool | None = Field(
        default=None,
        description="Flag indicating if the Backpack withdrawal was internal.",
    )
    client_id: str | None = Field(
        default=None,
        description="Client ID for the Backpack withdrawal.",
    )
    identifier: str | None = Field(
        default=None,
        description="Identifier for fiat withdrawals on Backpack.",
    )
    fiat_fee: Decimal | None = Field(
        default=None,
        description="Fee in fiat currency for Backpack withdrawals.",
    )
    fiat_state: str | None = Field(
        default=None,
        description="State of fiat withdrawal on Backpack.",
    )
    fiat_symbol: str | None = Field(
        default=None,
        description="Fiat currency symbol for Backpack withdrawals.",
    )
    provider_id: str | None = Field(
        default=None,
        description="Provider ID for Backpack withdrawals.",
    )
    subaccount_id: int | None = Field(
        default=None,
        description="Subaccount ID for Backpack withdrawals.",
    )
    bank_name: str | None = Field(
        default=None,
        description="Bank name for Backpack fiat withdrawals.",
    )
    bank_identifier: str | None = Field(
        default=None,
        description="Bank identifier for Backpack fiat withdrawals.",
    )
    account_identifier: str | None = Field(
        default=None,
        description="Account identifier for Backpack fiat withdrawals.",
    )

    model_config = ConfigDict(extra="ignore", frozen=True)


# --- Core Withdrawal Model (Immutable) ---


class Withdrawal(BaseModel):
    """Core internal model for a withdrawal operation. Immutable.

    Represents the state or result of a withdrawal.
    """

    id: str = Field(description="Unique identifier for the withdrawal.")
    exchange: str = Field(description="Name of the exchange from which the withdrawal was made.")
    status: InternalWithdrawalStatus = Field(description="Internal status of the withdrawal.")
    asset: str = Field(description="The asset symbol that was withdrawn (e.g., 'USDC', 'BTC').")
    quantity: Decimal = Field(description="The amount of the asset withdrawn.")
    address: str = Field(description="Destination address for the withdrawal.")
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="Timestamp of the withdrawal event or creation (UTC).",
    )
    fee: Decimal | None = Field(default=None, description="Fee paid for the withdrawal, if any.")
    tx_hash: str | None = Field(
        default=None,
        description="Blockchain transaction hash for the withdrawal, if available.",
    )
    response_message: str | None = Field(
        default=None,
        description="Optional message from the exchange regarding the operation.",
    )

    # Extension Slots
    hl_details: HyperliquidWithdrawalDetails | None = Field(default=None)
    bp_details: BackpackWithdrawalDetails | None = Field(default=None)

    model_config = ConfigDict(extra="forbid", frozen=True, validate_assignment=True)


__all__ = [
    "BackpackTransferDetails",
    "BackpackWithdrawalDetails",
    "HyperliquidTransferDetails",
    "HyperliquidWithdrawalDetails",
    "Transfer",
    "Withdrawal",
]
