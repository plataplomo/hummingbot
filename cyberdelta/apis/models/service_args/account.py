"""Account service argument models.

This module contains Pydantic models for account operations including transfers,
withdrawals, and account settings updates.
"""

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from cyberdelta.apis.exceptions.field_validation import TypeFieldError
from cyberdelta.apis.models.service_args.common import validate_api_str_field
from cyberdelta.exceptions.field_validation import DecimalFieldError
from cyberdelta.exceptions.service_validation import TransferAccountError
from cyberdelta.utils.parsing import parse_decimal_safely
from cyberdelta.utils.typing import PotentialDecimalInput, is_potential_decimal_input


class TransferArgs(BaseModel):
    """Encapsulates arguments for internal fund transfers between account types within an exchange.

    This model centralizes validation for transfer operations, ensuring consistent
    handling of asset, amount, and account type parameters.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    asset: str
    amount: Decimal = Field(gt=Decimal(0))
    from_account_type: str  # Specific validation might depend on exchange
    to_account_type: str  # Specific validation might depend on exchange
    client_transfer_id: str | None = Field(default=None)

    @field_validator("asset", "from_account_type", "to_account_type", mode="before")
    @classmethod
    def validate_required_strings(cls, v: str, info: ValidationInfo) -> str:
        """Validate required string fields are non-empty with max length 64.

        Returns:
            Validated string value.
        """
        return validate_api_str_field(
            v,
            field_name=str(info.field_name),
            max_length=64,
            allow_empty=False,
        )

    @field_validator("client_transfer_id", mode="before")
    @classmethod
    def validate_optional_string(cls, v: str | None, info: ValidationInfo) -> str | None:
        """Validate optional string fields.

        Returns:
            Validated string value or None if input was None.
        """
        if v is None:
            return None
        return validate_api_str_field(
            v,
            field_name=str(info.field_name),
            max_length=128,
            allow_empty=False,
        )

    @field_validator("amount", mode="before")
    @classmethod
    def parse_amount_decimal(cls, v: PotentialDecimalInput, info: ValidationInfo) -> Decimal:
        """Parse and validate amount as a positive finite decimal.

        Returns:
            Parsed and validated Decimal value.

        Raises:
            TypeFieldError: If input is not a valid decimal type.
            DecimalFieldError: If value is not finite or cannot be parsed.
        """
        field_name = str(info.field_name)

        # Use TypeGuard for better type safety
        if not is_potential_decimal_input(v):
            raise TypeFieldError(
                field_name=field_name,
                expected_type="string, int, float, or Decimal",
                actual_type=type(v).__name__,
            )

        parsed = parse_decimal_safely(v, field_name=field_name, allow_none=False)
        if not parsed.is_finite():
            raise DecimalFieldError(
                field_name=field_name,
                value=v,
                reason="must be a finite decimal",
            )
        # Positivity (gt=0) is handled by Field constraint.
        return parsed

    @model_validator(mode="after")
    def check_account_types_differ(self) -> "TransferArgs":
        """Ensure from and to account types are different.

        Returns:
            Self for method chaining.

        Raises:
            TransferAccountError: If from and to account types are the same.
        """
        if self.from_account_type == self.to_account_type:
            raise TransferAccountError(
                from_account=self.from_account_type,
                to_account=self.to_account_type,
            )
        # Note: Exchange-specific validation for from/to_account_type values would ideally
        # be handled by derived Args models or within the service implementation.
        return self


class WithdrawArgs(BaseModel):
    """Encapsulates arguments for fund withdrawals.

    This model handles withdrawal parameters including asset, amount, address, network,
    and optional tags or IDs, with support for exchange-specific extra parameters.
    """

    model_config = ConfigDict(extra="allow", validate_assignment=True)  # extra="allow" for **kwargs

    asset: str
    amount: Decimal = Field(gt=Decimal(0))
    address: str
    network: str | None = Field(default=None)  # Optional for some exchanges
    tag: str | None = Field(default=None)  # e.g., memo for XRP, destination tag for others
    client_withdrawal_id: str | None = Field(default=None)
    two_factor_token: str | None = Field(default=None)  # If 2FA is handled at this level

    @field_validator("asset", "address", mode="before")
    @classmethod
    def validate_required_strings(cls, v: str, info: ValidationInfo) -> str:
        """Validate required string fields.

        Returns:
            Validated string value.
        """
        return validate_api_str_field(
            v,
            field_name=str(info.field_name),
            max_length=128,
            allow_empty=False,
        )

    @field_validator("network", "tag", "client_withdrawal_id", "two_factor_token", mode="before")
    @classmethod
    def validate_optional_strings(cls, v: str | None, info: ValidationInfo) -> str | None:
        """Validate optional string fields.

        Returns:
            Validated string value or None if input was None.
        """
        if v is None:
            return None
        # Shorter max_length for network/tag unless specific exchanges require longer
        return validate_api_str_field(
            v,
            field_name=str(info.field_name),
            max_length=64,
            allow_empty=False,
        )

    @field_validator("amount", mode="before")
    @classmethod
    def parse_amount_decimal(cls, v: PotentialDecimalInput, info: ValidationInfo) -> Decimal:
        """Parse and validate amount as a positive finite decimal.

        Returns:
            Parsed and validated Decimal value.

        Raises:
            TypeFieldError: If input is not a valid decimal type.
            DecimalFieldError: If value is not finite or cannot be parsed.
        """
        field_name = str(info.field_name)

        # Use TypeGuard for better type safety
        if not is_potential_decimal_input(v):
            raise TypeFieldError(
                field_name=field_name,
                expected_type="string, int, float, or Decimal",
                actual_type=type(v).__name__,
            )

        parsed = parse_decimal_safely(v, field_name=field_name, allow_none=False)
        if not parsed.is_finite():
            raise DecimalFieldError(
                field_name=field_name,
                value=v,
                reason="must be a finite decimal",
            )
        # Positivity (gt=0) is handled by Field constraint.
        return parsed

    # No model_validator needed for basic WithdrawArgs unless inter-dependencies
    # are identified that are universal. Exchange-specific checks would go into
    # the service method or a derived model.


class UpdateAccountSettingsArgs(BaseModel):
    """Arguments for updating account settings."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    auto_borrow_settlements: bool | None = Field(default=None)
    auto_lend: bool | None = Field(default=None)
    auto_realize_pnl: bool | None = Field(default=None)
    auto_repay_borrows: bool | None = Field(default=None)
    leverage_limit: Decimal | None = Field(default=None, gt=Decimal(0))
