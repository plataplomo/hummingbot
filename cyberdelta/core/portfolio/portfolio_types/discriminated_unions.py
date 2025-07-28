"""Discriminated unions for portfolio types providing type-safe polymorphism."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Discriminator, Field


# Order discriminated unions
class SpotOrderData(BaseModel):
    """Spot order specific data."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    order_type: Literal["spot"] = "spot"
    asset_pair: str = Field(..., description="Asset pair for spot trading")
    is_market_order: bool = Field(default=False)


class DerivativeOrderData(BaseModel):
    """Derivative order specific data."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    order_type: Literal["derivative"] = "derivative"
    contract_symbol: str = Field(..., description="Derivative contract symbol")
    leverage: float = Field(default=1.0, gt=0)
    margin_requirement: float = Field(default=0.0, ge=0)


class OptionOrderData(BaseModel):
    """Option order specific data."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    order_type: Literal["option"] = "option"
    underlying_asset: str = Field(..., description="Underlying asset")
    strike_price: float = Field(..., gt=0)
    expiration_date: str = Field(..., description="Option expiration date")
    option_type: Literal["call", "put"] = Field(..., description="Option type")


# Discriminated union for all order types
OrderUnion = Annotated[
    SpotOrderData | DerivativeOrderData | OptionOrderData, Discriminator("order_type")
]


# Position discriminated unions
class SpotPositionData(BaseModel):
    """Spot position specific data."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    position_type: Literal["spot"] = "spot"
    asset: str = Field(..., description="Asset symbol")
    total_quantity: float = Field(..., description="Total quantity held")
    available_quantity: float = Field(..., description="Available quantity")


class DerivativePositionData(BaseModel):
    """Derivative position specific data."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    position_type: Literal["derivative"] = "derivative"
    contract_symbol: str = Field(..., description="Contract symbol")
    size: float = Field(..., description="Position size (positive for long, negative for short)")
    entry_price: float = Field(..., gt=0, description="Average entry price")
    mark_price: float | None = Field(default=None, description="Current mark price")
    unrealized_pnl: float = Field(default=0.0, description="Unrealized PnL")
    leverage: float = Field(default=1.0, gt=0, description="Position leverage")


class OptionPositionData(BaseModel):
    """Option position specific data."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    position_type: Literal["option"] = "option"
    underlying_asset: str = Field(..., description="Underlying asset")
    strike_price: float = Field(..., gt=0)
    expiration_date: str = Field(..., description="Option expiration date")
    option_type: Literal["call", "put"] = Field(..., description="Option type")
    contracts: int = Field(..., description="Number of contracts")
    premium_paid: float = Field(default=0.0, description="Premium paid for the option")


# Discriminated union for all position types
PositionUnion = Annotated[
    SpotPositionData | DerivativePositionData | OptionPositionData, Discriminator("position_type")
]


# Trade discriminated unions
class SpotTradeData(BaseModel):
    """Spot trade specific data."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    trade_type: Literal["spot"] = "spot"
    base_asset: str = Field(..., description="Base asset")
    quote_asset: str = Field(..., description="Quote asset")
    is_maker: bool = Field(default=False, description="Whether this was a maker trade")


class DerivativeTradeData(BaseModel):
    """Derivative trade specific data."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    trade_type: Literal["derivative"] = "derivative"
    contract_symbol: str = Field(..., description="Contract symbol")
    leverage: float = Field(..., gt=0, description="Leverage used")
    funding_rate: float | None = Field(default=None, description="Funding rate at trade time")
    is_liquidation: bool = Field(default=False, description="Whether this was a liquidation")


class OptionTradeData(BaseModel):
    """Option trade specific data."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    trade_type: Literal["option"] = "option"
    underlying_asset: str = Field(..., description="Underlying asset")
    strike_price: float = Field(..., gt=0)
    expiration_date: str = Field(..., description="Option expiration date")
    option_type: Literal["call", "put"] = Field(..., description="Option type")
    premium: float = Field(..., description="Option premium")
    is_exercise: bool = Field(default=False, description="Whether this was an exercise")


# Discriminated union for all trade types
TradeUnion = Annotated[
    SpotTradeData | DerivativeTradeData | OptionTradeData, Discriminator("trade_type")
]


# Exchange-specific discriminated unions
class HyperliquidExchangeData(BaseModel):
    """Hyperliquid exchange specific data."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    exchange_type: Literal["hyperliquid"] = "hyperliquid"
    user_address: str = Field(..., description="User wallet address")
    vault_address: str | None = Field(default=None, description="Vault address if applicable")
    is_mainnet: bool = Field(default=True, description="Whether this is mainnet")


class BackpackExchangeData(BaseModel):
    """Backpack exchange specific data."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    exchange_type: Literal["backpack"] = "backpack"
    username: str = Field(..., description="Backpack username")
    sub_account_id: str | None = Field(default=None, description="Sub-account ID if applicable")


# Discriminated union for exchange-specific data
ExchangeUnion = Annotated[
    HyperliquidExchangeData | BackpackExchangeData, Discriminator("exchange_type")
]


# Error discriminated unions
class ValidationErrorData(BaseModel):
    """Validation error specific data."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    error_type: Literal["validation"] = "validation"
    field_name: str = Field(..., description="Field that failed validation")
    constraint: str = Field(..., description="Validation constraint that was violated")
    actual_value: str = Field(..., description="Actual value that caused the error")


class NetworkErrorData(BaseModel):
    """Network error specific data."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    error_type: Literal["network"] = "network"
    status_code: int | None = Field(default=None, description="HTTP status code if applicable")
    endpoint: str | None = Field(default=None, description="API endpoint that failed")
    retry_count: int = Field(default=0, description="Number of retries attempted")


class BusinessLogicErrorData(BaseModel):
    """Business logic error specific data."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    error_type: Literal["business_logic"] = "business_logic"
    rule_name: str = Field(..., description="Business rule that was violated")
    context: dict[str, str] = Field(default_factory=dict, description="Additional context")


# Discriminated union for error types
ErrorUnion = Annotated[
    ValidationErrorData | NetworkErrorData | BusinessLogicErrorData, Discriminator("error_type")
]


# Type guards for discriminated unions
def is_spot_order(order_data: OrderUnion) -> bool:
    """Type guard for spot orders.

    Args:
        order_data: Order data to check

    Returns:
        True if the order is a spot order, False otherwise
    """
    return order_data.order_type == "spot"


def is_derivative_order(order_data: OrderUnion) -> bool:
    """Type guard for derivative orders.

    Args:
        order_data: Order data to check

    Returns:
        True if the order is a derivative order, False otherwise
    """
    return order_data.order_type == "derivative"


def is_option_order(order_data: OrderUnion) -> bool:
    """Type guard for option orders.

    Args:
        order_data: Order data to check

    Returns:
        True if the order is an option order, False otherwise
    """
    return order_data.order_type == "option"


def is_spot_position(position_data: PositionUnion) -> bool:
    """Type guard for spot positions.

    Args:
        position_data: Position data to check

    Returns:
        True if the position is a spot position, False otherwise
    """
    return position_data.position_type == "spot"


def is_derivative_position(position_data: PositionUnion) -> bool:
    """Type guard for derivative positions.

    Args:
        position_data: Position data to check

    Returns:
        True if the position is a derivative position, False otherwise
    """
    return position_data.position_type == "derivative"


def is_option_position(position_data: PositionUnion) -> bool:
    """Type guard for option positions.

    Args:
        position_data: Position data to check

    Returns:
        True if the position is an option position, False otherwise
    """
    return position_data.position_type == "option"


def is_spot_trade(trade_data: TradeUnion) -> bool:
    """Type guard for spot trades.

    Args:
        trade_data: Trade data to check

    Returns:
        True if the trade is a spot trade, False otherwise
    """
    return trade_data.trade_type == "spot"


def is_derivative_trade(trade_data: TradeUnion) -> bool:
    """Type guard for derivative trades.

    Args:
        trade_data: Trade data to check

    Returns:
        True if the trade is a derivative trade, False otherwise
    """
    return trade_data.trade_type == "derivative"


def is_option_trade(trade_data: TradeUnion) -> bool:
    """Type guard for option trades.

    Args:
        trade_data: Trade data to check

    Returns:
        True if the trade is an option trade, False otherwise
    """
    return trade_data.trade_type == "option"


def is_hyperliquid_exchange(exchange_data: ExchangeUnion) -> bool:
    """Type guard for Hyperliquid exchange.

    Args:
        exchange_data: Exchange data to check

    Returns:
        True if the exchange is Hyperliquid, False otherwise
    """
    return exchange_data.exchange_type == "hyperliquid"


def is_backpack_exchange(exchange_data: ExchangeUnion) -> bool:
    """Type guard for Backpack exchange.

    Args:
        exchange_data: Exchange data to check

    Returns:
        True if the exchange is Backpack, False otherwise
    """
    return exchange_data.exchange_type == "backpack"


def is_validation_error(error_data: ErrorUnion) -> bool:
    """Type guard for validation errors.

    Args:
        error_data: Error data to check

    Returns:
        True if the error is a validation error, False otherwise
    """
    return error_data.error_type == "validation"


def is_network_error(error_data: ErrorUnion) -> bool:
    """Type guard for network errors.

    Args:
        error_data: Error data to check

    Returns:
        True if the error is a network error, False otherwise
    """
    return error_data.error_type == "network"


def is_business_logic_error(error_data: ErrorUnion) -> bool:
    """Type guard for business logic errors.

    Args:
        error_data: Error data to check

    Returns:
        True if the error is a business logic error, False otherwise
    """
    return error_data.error_type == "business_logic"
