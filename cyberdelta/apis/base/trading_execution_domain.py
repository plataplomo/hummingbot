"""Trading execution domain objects with Pydantic validation.

This module provides comprehensive domain objects to replace boolean traps
in trading execution parameters. These objects include built-in validation
and cross-field business rule validation to prevent trading errors.
"""

from __future__ import annotations

import warnings
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from cyberdelta.apis.exceptions.configuration_validation import (
    LeverageRiskError,
    TradingExecutionError,
)


# Leverage thresholds for risk management
HIGH_LEVERAGE_THRESHOLD = 20
VERY_HIGH_LEVERAGE_THRESHOLD = 50


class LiquidityRequirement(Enum):
    """Liquidity requirement for order execution.

    Replaces the boolean `post_only` parameter with explicit liquidity policies.
    """

    ANY = "any"
    """Can be taker or maker - allows any execution (was post_only=False)."""

    POST_ONLY = "post_only"
    """Post-only execution - must be maker (was post_only=True)."""

    IMMEDIATE_OR_CANCEL = "immediate_or_cancel"
    """Taker-only execution - cancel remainder if not immediately filled."""

    FILL_OR_KILL = "fill_or_kill"
    """All-or-nothing execution - cancel entire order if not completely filled."""


class PositionIntent(Enum):
    """Position intent for order execution.

    Replaces the boolean `reduce_only` parameter with explicit position policies.
    """

    OPEN_OR_INCREASE = "open_increase"
    """Normal orders that can open new positions or increase existing ones.

    Was reduce_only=False.
    """

    REDUCE_ONLY = "reduce_only"
    """Risk management orders that only reduce existing positions (was reduce_only=True)."""

    CLOSE_POSITION = "close_position"
    """Orders specifically designed to close an entire position."""


class MarginPolicy(Enum):
    """Margin policy for automated financial operations.

    Replaces multiple boolean auto_* parameters with explicit margin policies.
    """

    MANUAL_ONLY = "manual"
    """No automatic operations - all margin management is manual (was all auto_*=False)."""

    AUTO_LEND_ENABLED = "auto_lend"
    """Enable automatic lending when needed (was auto_lend=True)."""

    AUTO_BORROW_ENABLED = "auto_borrow"
    """Enable automatic borrowing when needed (was auto_borrow=True)."""

    FULL_AUTO = "full_auto"
    """Both automatic lending and borrowing enabled (was auto_lend=True, auto_borrow=True)."""


class OrderExecution(BaseModel):
    """Order execution configuration with comprehensive validation.

    This domain object replaces multiple boolean parameters with explicit,
    validated configuration that prevents dangerous trading combinations.
    """

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        validate_assignment=True,
        str_strip_whitespace=True,
    )

    liquidity_requirement: LiquidityRequirement = Field(
        default=LiquidityRequirement.ANY, description="Liquidity requirement for order execution"
    )

    position_intent: PositionIntent = Field(
        default=PositionIntent.OPEN_OR_INCREASE, description="Position intent for risk management"
    )

    margin_policy: MarginPolicy = Field(
        default=MarginPolicy.MANUAL_ONLY, description="Margin policy for automated operations"
    )

    @model_validator(mode="after")
    def validate_execution_compatibility(self) -> OrderExecution:
        """Validate that execution parameters are compatible.

        Prevents dangerous combinations that could cause trading errors or
        uncontrolled financial operations.
        """
        # Prevent high-risk combinations
        if (
            self.liquidity_requirement == LiquidityRequirement.IMMEDIATE_OR_CANCEL
            and self.margin_policy == MarginPolicy.FULL_AUTO
        ):
            raise TradingExecutionError(
                liquidity_requirement=self.liquidity_requirement.value,
                margin_policy=self.margin_policy.value,
                risk_description="creates risk of uncontrolled borrowing",
            )

        # Reduce-only orders should generally not need aggressive margin policies
        if (
            self.position_intent == PositionIntent.REDUCE_ONLY
            and self.margin_policy == MarginPolicy.FULL_AUTO
        ):
            warnings.warn(
                "REDUCE_ONLY orders with FULL_AUTO margin may be unnecessary - "
                "consider MANUAL_ONLY for position reduction",
                UserWarning,
                stacklevel=2,
            )

        # Fill-or-kill with reduce-only can be problematic
        if (
            self.liquidity_requirement == LiquidityRequirement.FILL_OR_KILL
            and self.position_intent == PositionIntent.REDUCE_ONLY
        ):
            warnings.warn(
                "FILL_OR_KILL with REDUCE_ONLY may fail unnecessarily - "
                "consider IMMEDIATE_OR_CANCEL for more flexibility",
                UserWarning,
                stacklevel=2,
            )

        return self

    def model_dump_for_api(self) -> dict[str, Any]:
        """Serialize for external API consumption.

        Returns:
            Dictionary formatted for API requests with enum values.
        """
        return {
            "liquidity_requirement": self.liquidity_requirement.value,
            "position_intent": self.position_intent.value,
            "margin_policy": self.margin_policy.value,
        }


class AccountSettingsPolicy(Enum):
    """Account settings policy for automated account operations.

    Replaces multiple boolean auto_* parameters in account settings.
    """

    MANUAL_CONTROL = "manual_control"
    """All operations require manual approval."""

    AUTO_SETTLEMENTS = "auto_settlements"
    """Enable automatic borrow settlements only."""

    AUTO_LENDING = "auto_lending"
    """Enable automatic lending only."""

    AUTO_PNL_REALIZATION = "auto_pnl"
    """Enable automatic P&L realization only."""

    AUTO_REPAY = "auto_repay"
    """Enable automatic borrow repayment only."""

    FULL_AUTOMATION = "full_automation"
    """Enable all automatic operations."""

    CONSERVATIVE_AUTO = "conservative_auto"
    """Enable safe automatic operations (settlements + repay)."""


class AccountSettings(BaseModel):
    """Account settings configuration with validation.

    Replaces multiple boolean flags with validated policy objects.
    """

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        validate_assignment=True,
    )

    leverage_limit: int = Field(
        default=10, ge=1, le=100, description="Maximum leverage allowed for the account"
    )

    automation_policy: AccountSettingsPolicy = Field(
        default=AccountSettingsPolicy.MANUAL_CONTROL,
        description="Policy for automated account operations",
    )

    @model_validator(mode="after")
    def validate_account_safety(self) -> AccountSettings:
        """Validate account settings for financial safety."""
        # High leverage with full automation is dangerous
        if (
            self.leverage_limit > HIGH_LEVERAGE_THRESHOLD
            and self.automation_policy == AccountSettingsPolicy.FULL_AUTOMATION
        ):
            raise LeverageRiskError(
                leverage_limit=self.leverage_limit,
                automation_policy=self.automation_policy.value,
            )

        # Warn about high leverage with any automation
        if (
            self.leverage_limit > VERY_HIGH_LEVERAGE_THRESHOLD
            and self.automation_policy != AccountSettingsPolicy.MANUAL_CONTROL
        ):
            warnings.warn(
                f"Very high leverage ({self.leverage_limit}x) with automation "
                "creates significant financial risk",
                UserWarning,
                stacklevel=2,
            )

        return self

    def to_api_fields(self) -> dict[str, bool]:
        """Convert to API format for request payload."""
        return {
            "autoLend": self.automation_policy
            in {
                AccountSettingsPolicy.AUTO_LENDING,
                AccountSettingsPolicy.FULL_AUTOMATION,
            },
        }
