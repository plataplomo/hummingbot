"""CyberDeltaEngine: Backpack API Raw Models (Withdrawal).

-----------------------------------------------------

This module defines Pydantic models for validating the *raw* structure of
Backpack Exchange API requests and responses related to withdrawals.
"""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

# Removed ValidationInfo, field_validator and specific parsing utils
from cyberdelta.apis.backpack.models.bp_common_raw_types import (
    RawBpNonEmptyString,  # For required strings with no specific max_len in original validator
    RawBpOptionalNonEmptyString,  # For optional strings with no specific max_len
    RawBpOptionalParsableFiniteDecimalString,
    RawBpOptionalStrictBool,
    RawBpParsableFiniteDecimalString,
    RawBpParsablePositiveFiniteDecimalString,  # For request quantity
    RawBpStrictBool,
    RawBpStringToDatetime,
    RawBpWithdrawalConfirmedPendingStatusString,  # For response status
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
    "initialized",
    "pending",
    "processing",
    "complete",
    "declined",
    "cancelled",
    "review",
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
    """Pydantic model for the raw withdrawal request payload to Backpack.

    Corresponds to `AccountWithdrawalPayload` in OpenAPI.
    """

    address: RawBpNonEmptyString
    blockchain: Blockchain
    quantity: RawBpParsablePositiveFiniteDecimalString
    symbol: Asset
    client_id: RawBpOptionalNonEmptyString = Field(default=None, alias="clientId")
    two_factor_token: RawBpOptionalNonEmptyString = Field(default=None, alias="twoFactorToken")
    auto_borrow: RawBpOptionalStrictBool = Field(default=None, alias="autoBorrow")
    auto_lend_redeem: RawBpOptionalStrictBool = Field(default=None, alias="autoLendRedeem")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# --- Response Model --- #


class BackpackRawWithdrawalResponse(BaseModel):
    """Pydantic model for the raw withdrawal response from Backpack.

    Corresponds to `Withdrawal` schema in OpenAPI.
    """

    id: int
    blockchain: Blockchain
    quantity: RawBpParsableFiniteDecimalString
    fee: RawBpParsableFiniteDecimalString
    symbol: Asset
    status: RawBpWithdrawalConfirmedPendingStatusString
    to_address: RawBpNonEmptyString = Field(..., alias="toAddress")
    created_at: RawBpStringToDatetime = Field(..., alias="createdAt")
    is_internal: RawBpStrictBool = Field(..., alias="isInternal")

    # Optional fields
    client_id: RawBpOptionalNonEmptyString = Field(default=None, alias="clientId")
    identifier: RawBpOptionalNonEmptyString = Field(default=None)
    fiat_fee: RawBpOptionalParsableFiniteDecimalString = Field(default=None, alias="fiatFee")
    fiat_state: EqualsMoneyWithdrawalState | None = Field(default=None, alias="fiatState")
    fiat_symbol: FiatAsset | None = Field(default=None, alias="fiatSymbol")
    provider_id: RawBpOptionalNonEmptyString = Field(default=None, alias="providerId")
    subaccount_id: int | None = Field(default=None, alias="subaccountId")
    transaction_hash: RawBpOptionalNonEmptyString = Field(default=None, alias="transactionHash")
    bank_name: RawBpOptionalNonEmptyString = Field(default=None, alias="bankName")
    bank_identifier: RawBpOptionalNonEmptyString = Field(default=None, alias="bankIdentifier")
    account_identifier: RawBpOptionalNonEmptyString = Field(default=None, alias="accountIdentifier")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)
