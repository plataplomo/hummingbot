"""
Service argument models for internal service layer interfaces.

This module contains Pydantic models that encapsulate arguments for various service methods,
centralizing validation logic and improving API clarity.
"""

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.utils.parsing import parse_decimal_value, validate_str_field


class PlaceOrderArgs(BaseModel):
    """
    Encapsulates all arguments for placing an order.
    
    This model centralizes input validation for order placement across all exchanges,
    including type checks, value constraints, and inter-parameter dependencies.
    """
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: Decimal = Field(gt=Decimal("0"))
    time_in_force: TimeInForce
    price: Decimal | None = Field(default=None, gt=Decimal("0"))
    stop_price: Decimal | None = Field(default=None, gt=Decimal("0"))
    client_order_id: str | None = Field(default=None)
    reduce_only: bool = Field(default=False)
    post_only: bool = Field(default=False)

    @field_validator("symbol", mode="before")
    @classmethod
    def validate_symbol_str(cls, v: str, info: ValidationInfo) -> str:
        """Validate symbol is a non-empty string with max length 64."""
        # field_name is guaranteed by Pydantic to be correct here.
        return validate_str_field(
            v, field_name=str(info.field_name), max_length=64, allow_empty=False
        )

    @field_validator("client_order_id", mode="before")
    @classmethod
    def validate_client_order_id_str(cls, v: str | None, info: ValidationInfo) -> str | None:
        """Validate client_order_id is None or a non-empty string with max length 64."""
        if v is None:
            return None
        return validate_str_field(
            v, field_name=str(info.field_name), max_length=64, allow_empty=False
        )

    @field_validator("quantity", "price", "stop_price", mode="before")
    @classmethod
    def parse_decimal_fields(
        cls, v: str | int | float | Decimal | None, info: ValidationInfo
    ) -> Decimal | None:
        """Parse decimal fields and ensure they are finite."""
        field_name = str(info.field_name)
        is_required = field_name == "quantity"
        parsed = parse_decimal_value(v, field_name=field_name, allow_none=not is_required)
        if parsed is None and is_required:
            raise ValueError(f"Field '{field_name}' is required and cannot be None or invalid.")
        if parsed is not None:
            if not parsed.is_finite():
                raise ValueError(f"Field '{field_name}' must be a finite decimal, got {v}.")
            # Positivity (gt=0) is handled by Field constraint AFTER this validator.
        return parsed

    @model_validator(mode="after")
    def check_parameter_dependencies(self) -> "PlaceOrderArgs":
        """Validate inter-parameter dependencies."""
        if self.order_type in [OrderType.LIMIT, OrderType.STOP_LIMIT] and self.price is None:
            raise ValueError(f"A positive price is required for {self.order_type.value} orders.")
        if (
            self.order_type in [OrderType.STOP_MARKET, OrderType.STOP_LIMIT]
            and self.stop_price is None
        ):
            raise ValueError(
                f"A positive stop_price is required for {self.order_type.value} orders."
            )
        if self.post_only and self.order_type != OrderType.LIMIT:
            raise ValueError("Post-only (post_only=True) is only applicable to LIMIT orders.")
        # Note: Specific client_order_id format checks (e.g., Backpack int conversion)
        # should be handled within the exchange-specific RequestBuilder or service,
        # not in this generic Args model.
        return self


class TransferArgs(BaseModel):
    """
    Encapsulates arguments for internal fund transfers between account types within an exchange.
    
    This model centralizes validation for transfer operations, ensuring consistent
    handling of asset, amount, and account type parameters.
    """
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    asset: str
    amount: Decimal = Field(gt=Decimal("0"))
    from_account_type: str  # Specific validation might depend on exchange
    to_account_type: str    # Specific validation might depend on exchange
    client_transfer_id: str | None = Field(default=None)

    @field_validator("asset", "from_account_type", "to_account_type", mode="before")
    @classmethod
    def validate_required_strings(cls, v: str, info: ValidationInfo) -> str:
        """Validate required string fields are non-empty with max length 64."""
        return validate_str_field(
            v, field_name=str(info.field_name), max_length=64, allow_empty=False
        )

    @field_validator("client_transfer_id", mode="before")
    @classmethod
    def validate_optional_string(cls, v: str | None, info: ValidationInfo) -> str | None:
        """Validate optional string fields."""
        if v is None:
            return None
        return validate_str_field(
            v, field_name=str(info.field_name), max_length=128, allow_empty=False
        )

    @field_validator("amount", mode="before")
    @classmethod
    def parse_amount_decimal(
        cls, v: str | int | float | Decimal, info: ValidationInfo
    ) -> Decimal:
        """Parse and validate amount as a positive finite decimal."""
        field_name = str(info.field_name)
        parsed = parse_decimal_value(v, field_name=field_name, allow_none=False)
        if parsed is None:  # Should be caught by parse_decimal_value
            raise ValueError(f"Field '{field_name}' is required and cannot be None or invalid.")
        if not parsed.is_finite():
            raise ValueError(f"Field '{field_name}' must be a finite decimal, got {v}.")
        # Positivity (gt=0) is handled by Field constraint.
        return parsed

    @model_validator(mode="after")
    def check_account_types_differ(self) -> "TransferArgs":
        """Ensure from and to account types are different."""
        if self.from_account_type == self.to_account_type:
            raise ValueError("from_account_type and to_account_type cannot be the same.")
        # Note: Exchange-specific validation for from/to_account_type values would ideally
        # be handled by derived Args models or within the service implementation.
        return self


class WithdrawArgs(BaseModel):
    """
    Encapsulates arguments for fund withdrawals.
    
    This model handles withdrawal parameters including asset, amount, address, network,
    and optional tags or IDs, with support for exchange-specific extra parameters.
    """
    model_config = ConfigDict(extra="allow", validate_assignment=True)  # extra="allow" for **kwargs

    asset: str
    amount: Decimal = Field(gt=Decimal("0"))
    address: str
    network: str | None = Field(default=None)  # Optional for some exchanges
    tag: str | None = Field(default=None)  # e.g., memo for XRP, destination tag for others
    client_withdrawal_id: str | None = Field(default=None)
    two_factor_token: str | None = Field(default=None)  # If 2FA is handled at this level

    @field_validator("asset", "address", mode="before")
    @classmethod
    def validate_required_strings(cls, v: str, info: ValidationInfo) -> str:
        """Validate required string fields."""
        return validate_str_field(
            v, field_name=str(info.field_name), max_length=128, allow_empty=False
        )

    @field_validator("network", "tag", "client_withdrawal_id", "two_factor_token", mode="before")
    @classmethod
    def validate_optional_strings(cls, v: str | None, info: ValidationInfo) -> str | None:
        """Validate optional string fields."""
        if v is None:
            return None
        # Shorter max_length for network/tag unless specific exchanges require longer
        return validate_str_field(
            v, field_name=str(info.field_name), max_length=64, allow_empty=False
        )

    @field_validator("amount", mode="before")
    @classmethod
    def parse_amount_decimal(
        cls, v: str | int | float | Decimal, info: ValidationInfo
    ) -> Decimal:
        """Parse and validate amount as a positive finite decimal."""
        field_name = str(info.field_name)
        parsed = parse_decimal_value(v, field_name=field_name, allow_none=False)
        if parsed is None:  # Should be caught by parse_decimal_value
            raise ValueError(f"Field '{field_name}' is required and cannot be None or invalid.")
        if not parsed.is_finite():
            raise ValueError(f"Field '{field_name}' must be a finite decimal, got {v}.")
        # Positivity (gt=0) is handled by Field constraint.
        return parsed

    # No model_validator needed for basic WithdrawArgs unless inter-dependencies
    # are identified that are universal. Exchange-specific checks would go into
    # the service method or a derived model.


__all__ = [
    "PlaceOrderArgs",
    "TransferArgs", 
    "WithdrawArgs",
]