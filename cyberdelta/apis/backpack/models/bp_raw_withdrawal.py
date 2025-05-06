"""
CyberDeltaEngine: Backpack API Raw Models (Withdrawal)
-----------------------------------------------------

This module defines Pydantic models for validating the *raw* structure of
Backpack Exchange API requests and responses related to withdrawals.
"""

from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from cyberdelta.utils.parsing import (
    parse_datetime_utc,
    parse_decimal_value,
    validate_str_field,
)

# --- Enums based on OpenAPI spec --- #
Blockchain = Literal[
    "Arbitrum",
    "Base",
    "Berachain",
    "Bitcoin",
    "BitcoinCash",
    "Bsc",
    "Cardano",
    "Dogecoin",
    "EqualsMoney",
    "Ethereum",
    "Hyperliquid",
    "Litecoin",
    "Polygon",
    "Sui",
    "Solana",
    "Story",
    "XRP",
]

Asset = Literal[
    "BTC",
    "ETH",
    "SOL",
    "USDC",
    "USDT",
    "PYTH",
    "JTO",
    "BONK",
    "HNT",
    "MOBILE",
    "WIF",
    "JUP",
    "RENDER",
    "WEN",
    "W",
    "TNSR",
    "PRCL",
    "SHARK",
    "KMNO",
    "MEW",
    "BOME",
    "RAY",
    "HONEY",
    "SHFL",
    "BODEN",
    "IO",
    "DRIFT",
    "PEPE",
    "SHIB",
    "LINK",
    "UNI",
    "ONDO",
    "FTM",
    "MATIC",
    "STRK",
    "BLUR",
    "WLD",
    "GALA",
    "NYAN",
    "HLG",
    "MON",
    "ZKJ",
    "MANEKI",
    "HABIBI",
    "UNA",
    "ZRO",
    "ZEX",
    "AAVE",
    "LDO",
    "MOTHER",
    "CLOUD",
    "MAX",
    "POL",
    "TRUMPWIN",
    "HARRISWIN",
    "MOODENG",
    "DBR",
    "GOAT",
    "ACT",
    "DOGE",
    "BCH",
    "LTC",
    "APE",
    "ENA",
    "ME",
    "EIGEN",
    "CHILLGUY",
    "PENGU",
    "EUR",
    "SONIC",
    "J",
    "TRUMP",
    "MELANIA",
    "ANIME",
    "XRP",
    "SUI",
    "VINE",
    "ADA",
    "MOVE",
    "BERA",
    "IP",
    "HYPE",
    "BNB",
    "KAITO",
    "PEPE1000",
    "BONK1000",
    "SHIB1000",
    "AVAX",
    "S",
    "POINTS",
    "ROAM",
    "AI16Z",
    "LAYER",
    "FARTCOIN",
    "NEAR",
    "PNUT",
    "ARB",
    "DOT",
    "APT",
    "OP",
]

EqualsMoneyWithdrawalState = Literal[
    "initialized", "pending", "processing", "complete", "declined", "cancelled", "review"
]

FiatAsset = Literal[
    "AED",
    "AUD",
    "BGN",
    "BHD",
    "CAD",
    "CHF",
    "CNH",
    "CNY",
    "CZK",
    "DKK",
    "EUR",
    "GBP",
    "HKD",
    "HUF",
    "ILS",
    "JOD",
    "JPY",
    "KES",
    "KWD",
    "MUR",
    "MXN",
    "NOK",
    "NZD",
    "OMR",
    "PLN",
    "QAR",
    "RON",
    "SAR",
    "SEK",
    "SGD",
    "THB",
    "TND",
    "TRY",
    "USD",
    "ZAR",
    "ZMW",
]

WithdrawalStatus = Literal["confirmed", "pending"]

# --- Request Model --- #


class BackpackRawWithdrawalRequest(BaseModel):
    """
    Pydantic model for the raw withdrawal request payload to Backpack.
    Corresponds to `AccountWithdrawalPayload` in OpenAPI.
    """

    address: str
    blockchain: Blockchain
    quantity: Decimal
    symbol: Asset
    client_id: str | None = Field(default=None, alias="clientId")
    two_factor_token: str | None = Field(default=None, alias="twoFactorToken")
    auto_borrow: bool | None = Field(default=None, alias="autoBorrow")
    auto_lend_redeem: bool | None = Field(default=None, alias="autoLendRedeem")

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    # Field validators for request model (mostly string formats)
    @field_validator(
        "address", "client_id", "two_factor_token", "symbol", "blockchain", mode="before"
    )
    @classmethod
    def _validate_request_strings(cls, v: Any, info: ValidationInfo) -> str:
        if not isinstance(v, str):
            raise ValueError(f"Field {info.field_name} must be a string, got {type(v)}")
        return validate_str_field(v, field_name=str(info.field_name))  # Ensure field_name is str

    @field_validator("quantity", mode="before")
    @classmethod
    def _validate_request_quantity(cls, v: Any, info: ValidationInfo) -> Decimal:
        if not isinstance(v, str):
            raise ValueError(f"Field {info.field_name} (quantity) must be a string, got {type(v)}")
        validated_str = validate_str_field(v, field_name=str(info.field_name))
        parsed_decimal = parse_decimal_value(validated_str, field_name=str(info.field_name))
        if parsed_decimal is None:
            raise ValueError(f"Field {info.field_name} could not be parsed to a valid Decimal.")
        if parsed_decimal <= Decimal(0):
            raise ValueError(f"Field {info.field_name} (quantity) must be positive.")
        return parsed_decimal


# --- Response Model --- #


class BackpackRawWithdrawalResponse(BaseModel):
    """
    Pydantic model for the raw withdrawal response from Backpack.
    Corresponds to `Withdrawal` schema in OpenAPI.
    """

    id: int
    blockchain: Blockchain
    quantity: Decimal
    fee: Decimal
    symbol: Asset
    status: WithdrawalStatus
    to_address: str = Field(..., alias="toAddress")
    created_at: datetime = Field(..., alias="createdAt")
    is_internal: bool = Field(..., alias="isInternal")

    # Optional fields
    client_id: str | None = Field(default=None, alias="clientId")
    identifier: str | None = Field(default=None)  # tx hash if sent
    fiat_fee: Decimal | None = Field(default=None, alias="fiatFee")
    fiat_state: EqualsMoneyWithdrawalState | None = Field(default=None, alias="fiatState")
    fiat_symbol: FiatAsset | None = Field(default=None, alias="fiatSymbol")
    provider_id: str | None = Field(default=None, alias="providerId")
    subaccount_id: int | None = Field(default=None, alias="subaccountId")
    transaction_hash: str | None = Field(default=None, alias="transactionHash")
    bank_name: str | None = Field(default=None, alias="bankName")
    bank_identifier: str | None = Field(default=None, alias="bankIdentifier")
    account_identifier: str | None = Field(default=None, alias="accountIdentifier")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("quantity", "fee", "fiat_fee", mode="before")
    @classmethod
    def _validate_response_decimals(cls, v: Any, info: ValidationInfo) -> Decimal | None:
        if v is None:  # Allow optional Decimal fields to be None if not provided
            return None
        if not isinstance(v, str):
            raise ValueError(f"Field {info.field_name} must be a string, got {type(v)}")
        # Ensure field_name is str for parsing utilities
        field_name_str = str(info.field_name)
        validated_str = validate_str_field(v, field_name=field_name_str)
        parsed_decimal = parse_decimal_value(validated_str, field_name=field_name_str)
        # parse_decimal_value is expected to raise on error, but if it could return None:
        if parsed_decimal is None:
            raise ValueError(f"Field {field_name_str} could not be parsed to a valid Decimal.")
        return parsed_decimal

    @field_validator("created_at", mode="before")
    @classmethod
    def _validate_created_at(cls, v: Any, info: ValidationInfo) -> datetime:
        if not isinstance(v, str):
            raise ValueError(
                f"Field {info.field_name} (created_at) must be a string, got {type(v)}"
            )
        validated_str = validate_str_field(v, field_name=str(info.field_name))
        dt = parse_datetime_utc(validated_str, field_name=str(info.field_name))
        if dt is None:
            raise ValueError(
                f"Field {info.field_name} (created_at) could not be parsed to datetime: {v}"
            )
        return dt

    @field_validator(
        "blockchain",
        "symbol",
        "status",
        "to_address",
        "client_id",
        "identifier",
        "fiat_state",
        "fiat_symbol",
        "provider_id",
        "transaction_hash",
        "bank_name",
        "bank_identifier",
        "account_identifier",
        mode="before",
    )
    @classmethod
    def _validate_response_strings(cls, v: Any, info: ValidationInfo) -> str | None:
        if v is None:  # Allow optional string fields to be None
            return None
        if not isinstance(v, str):
            raise ValueError(f"Field {info.field_name} must be a string, got {type(v)}")
        return validate_str_field(v, field_name=str(info.field_name))
