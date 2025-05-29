"""
CyberDeltaEngine: Backpack API Raw Request Payload Models
---------------------------------------------------------

This module defines Pydantic models for request payloads sent to Backpack Exchange API.
These models strictly validate the external contract as defined in the OpenAPI spec,
using Literal types for fixed string values and common raw types for other fields.

Following the exact same pattern as Hyperliquid request models:
1. Use Literal types for fields with fixed string values
2. Use RawBp* annotated types from bp_common_raw_types.py
3. NO business logic validators in these raw models
4. model_config with populate_by_name=True, extra="forbid", frozen=True
"""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.apis.backpack.models.bp_common_raw_types import (
    RawBpNonEmptyStringMax64,
    RawBpNonEmptyStringMax128,
    RawBpNonEmptyStringMax255,
    RawBpOptionalStrictBool,
    RawBpParsableFiniteDecimalString,
    RawBpUint32,
)


# --- Order Execute Request Payload ---
class BackpackRawOrderExecuteRequest(BaseModel):
    """Raw request payload for placing an order on Backpack Exchange.

    Maps to OpenAPI schema: OrderExecutePayload
    """

    # Required fields
    orderType: Literal["Market", "Limit"] = Field(alias="orderType")
    side: Literal["Bid", "Ask"] = Field(alias="side")
    symbol: RawBpNonEmptyStringMax64 = Field(alias="symbol")

    # Optional fields
    clientId: RawBpUint32 | None = Field(default=None, alias="clientId")
    postOnly: RawBpOptionalStrictBool | None = Field(default=None, alias="postOnly")
    price: RawBpParsableFiniteDecimalString | None = Field(default=None, alias="price")
    quantity: RawBpParsableFiniteDecimalString | None = Field(default=None, alias="quantity")
    quoteQuantity: RawBpParsableFiniteDecimalString | None = Field(
        default=None, alias="quoteQuantity"
    )
    reduceOnly: RawBpOptionalStrictBool | None = Field(default=None, alias="reduceOnly")
    selfTradePrevention: Literal["RejectTaker", "RejectMaker", "RejectBoth"] | None = Field(
        default=None, alias="selfTradePrevention"
    )
    timeInForce: Literal["GTC", "IOC", "FOK"] | None = Field(default=None, alias="timeInForce")

    # Spot margin specific fields
    autoLend: RawBpOptionalStrictBool | None = Field(default=None, alias="autoLend")
    autoLendRedeem: RawBpOptionalStrictBool | None = Field(default=None, alias="autoLendRedeem")
    autoBorrow: RawBpOptionalStrictBool | None = Field(default=None, alias="autoBorrow")
    autoBorrowRepay: RawBpOptionalStrictBool | None = Field(default=None, alias="autoBorrowRepay")

    # Stop loss fields
    stopLossTriggerPrice: RawBpParsableFiniteDecimalString | None = Field(
        default=None, alias="stopLossTriggerPrice"
    )
    stopLossTriggerBy: Literal["LastPrice", "MarkPrice", "IndexPrice"] | None = Field(
        default=None, alias="stopLossTriggerBy"
    )
    stopLossLimitPrice: RawBpParsableFiniteDecimalString | None = Field(
        default=None, alias="stopLossLimitPrice"
    )

    # Take profit fields
    takeProfitTriggerPrice: RawBpParsableFiniteDecimalString | None = Field(
        default=None, alias="takeProfitTriggerPrice"
    )
    takeProfitTriggerBy: Literal["LastPrice", "MarkPrice", "IndexPrice"] | None = Field(
        default=None, alias="takeProfitTriggerBy"
    )
    takeProfitLimitPrice: RawBpParsableFiniteDecimalString | None = Field(
        default=None, alias="takeProfitLimitPrice"
    )

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# --- Order Cancel Request Payload ---
class BackpackRawOrderCancelRequest(BaseModel):
    """Raw request payload for cancelling a single order.

    Maps to OpenAPI schema: OrderCancelPayload
    """

    symbol: RawBpNonEmptyStringMax64 = Field(alias="symbol")
    orderId: RawBpNonEmptyStringMax64 | None = Field(default=None, alias="orderId")
    clientId: RawBpUint32 | None = Field(default=None, alias="clientId")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# --- Order Cancel All Request Payload ---
class BackpackRawOrderCancelAllRequest(BaseModel):
    """Raw request payload for cancelling all orders.

    Maps to OpenAPI schema: OrderCancelAllPayload
    """

    symbol: RawBpNonEmptyStringMax64 = Field(alias="symbol")
    orderType: Literal["RestingLimitOrder", "ConditionalOrder"] | None = Field(
        default=None, alias="orderType"
    )

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# --- Account Withdrawal Request Payload ---
class BackpackRawAccountWithdrawalRequest(BaseModel):
    """Raw request payload for withdrawing assets.

    Maps to OpenAPI schema: AccountWithdrawalPayload
    """

    # Required fields
    address: RawBpNonEmptyStringMax128 = Field(alias="address")
    blockchain: Literal[
        "Arbitrum",
        "Base",
        "Bitcoin",
        "BitcoinCash",
        "BNBSmartChain",
        "Cardano",
        "Dogecoin",
        "Ethereum",
        "Litecoin",
        "Polygon",
        "Solana",
        "Story",
        "Sui",
        "XRP",
    ] = Field(alias="blockchain")
    quantity: RawBpParsableFiniteDecimalString = Field(alias="quantity")
    symbol: Literal[
        "BTC",
        "ETH",
        "SOL",
        "USDC",
        "USDT",
        "PYTH",
        "JTO",
        "JUP",
        "RNDR",
        "TNSR",
        "W",
        "INF",
        "MOBILE",
        "KMNO",
        "MEW",
        "DRIFT",
        "WIF",
        "CLOUD",
        "MICHI",
        "TRUMP",
        "TOLY",
        "BONK",
        "RAY",
        "WEN",
        "BODEN",
        "SAMO",
        "BOME",
        "HNT",
        "IO",
        "DJT",
        "MATIC",
        "BNB",
        "HYPE",
        "VIRTUAL",
        "AI16Z",
        "PENGU",
        "ME",
        "GRASS",
        "MOVE",
        "DOGE",
        "SUI",
        "BNSOL",
        "JITOSOL",
        "MOODENG",
        "LESTER",
        "DARAM",
        "DEGENAI",
        "ELIZA",
        "AIXBT",
        "VVAIFU",
        "FARTCOIN",
        "SWARMS",
        "FATHA",
        "ADA",
        "WGF",
        "UBC",
        "GRIFFAIN",
        "XRP",
        "GOAT",
        "OM",
        "AVA",
        "OPUS",
        "GOON",
        "PNUT",
        "FWOG",
        "MBAPPE",
        "ZEREBRO",
        "DINO",
        "EVAN",
        "HAMMY",
        "LTC",
        "BCH",
        "MUMU",
        "OPAIUM",
        "SLERF",
        "MOTHER",
        "RLB",
        "BLINK",
        "UNI",
        "LINK",
        "TRX",
        "AAVE",
        "CRV",
        "MKR",
        "ENA",
        "POPCAT",
        "MYRO",
        "PONKE",
        "ZRO",
        "GIGA",
        "NOT",
        "DOGS",
        "WLD",
        "BFUSD",
        "CORGIAI",
        "MANEKI",
        "BERA",
        "SPX",
        "LUNA2",
        "HONEY",
        "FTM",
        "PEPE",
        "APT",
        "AVAX",
        "AR",
        "TON",
        "SWELL",
        "BUCK",
        "FWOG2",
        "MAVIA",
    ] = Field(alias="symbol")

    # Optional fields
    clientId: RawBpNonEmptyStringMax255 | None = Field(default=None, alias="clientId")
    twoFactorToken: RawBpNonEmptyStringMax64 | None = Field(default=None, alias="twoFactorToken")
    addressTag: RawBpNonEmptyStringMax128 | None = Field(default=None, alias="addressTag")
    autoBorrow: RawBpOptionalStrictBool | None = Field(default=None, alias="autoBorrow")
    autoLendRedeem: RawBpOptionalStrictBool | None = Field(default=None, alias="autoLendRedeem")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# --- Update Account Settings Request Payload ---
class BackpackRawUpdateAccountSettingsRequest(BaseModel):
    """Raw request payload for updating account settings.

    Maps to OpenAPI schema: UpdateAccountSettingsRequest
    """

    autoBorrowSettlements: RawBpOptionalStrictBool | None = Field(
        default=None, alias="autoBorrowSettlements"
    )
    autoLend: RawBpOptionalStrictBool | None = Field(default=None, alias="autoLend")
    autoRealizePnl: RawBpOptionalStrictBool | None = Field(default=None, alias="autoRealizePnl")
    autoRepayBorrows: RawBpOptionalStrictBool | None = Field(default=None, alias="autoRepayBorrows")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# --- Account Convert Dust Request Payload ---
class BackpackRawAccountConvertDustRequest(BaseModel):
    """Raw request payload for converting dust balances to USDC.

    Maps to OpenAPI schema: AccountConvertDustPayload
    """

    symbol: Literal[
        "BTC",
        "ETH",
        "SOL",
        "USDT",
        "PYTH",
        "JTO",
        "JUP",
        "RNDR",
        "TNSR",
        "W",
        "INF",
        "MOBILE",
        "KMNO",
        "MEW",
        "DRIFT",
        "WIF",
        "CLOUD",
        "MICHI",
        "TRUMP",
        "TOLY",
        "BONK",
        "RAY",
        "WEN",
        "BODEN",
        "SAMO",
        "BOME",
        "HNT",
        "IO",
        "DJT",
        "MATIC",
        "BNB",
        "HYPE",
        "VIRTUAL",
        "AI16Z",
        "PENGU",
        "ME",
        "GRASS",
        "MOVE",
        "DOGE",
        "SUI",
        "BNSOL",
        "JITOSOL",
        "MOODENG",
        "LESTER",
        "DARAM",
        "DEGENAI",
        "ELIZA",
        "AIXBT",
        "VVAIFU",
        "FARTCOIN",
        "SWARMS",
        "FATHA",
        "ADA",
        "WGF",
        "UBC",
        "GRIFFAIN",
        "XRP",
        "GOAT",
        "OM",
        "AVA",
        "OPUS",
        "GOON",
        "PNUT",
        "FWOG",
        "MBAPPE",
        "ZEREBRO",
        "DINO",
        "EVAN",
        "HAMMY",
        "LTC",
        "BCH",
        "MUMU",
        "OPAIUM",
        "SLERF",
        "MOTHER",
        "RLB",
        "BLINK",
        "UNI",
        "LINK",
        "TRX",
        "AAVE",
        "CRV",
        "MKR",
        "ENA",
        "POPCAT",
        "MYRO",
        "PONKE",
        "ZRO",
        "GIGA",
        "NOT",
        "DOGS",
        "WLD",
        "BFUSD",
        "CORGIAI",
        "MANEKI",
        "BERA",
        "SPX",
        "LUNA2",
        "HONEY",
        "FTM",
        "PEPE",
        "APT",
        "AVAX",
        "AR",
        "TON",
        "SWELL",
        "BUCK",
        "FWOG2",
        "MAVIA",
    ] = Field(alias="symbol")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# --- Borrow Lend Execute Request Payload ---
class BackpackRawBorrowLendExecuteRequest(BaseModel):
    """Raw request payload for borrowing or lending operations.

    Maps to OpenAPI schema: BorrowLendExecutePayload
    """

    quantity: RawBpParsableFiniteDecimalString = Field(alias="quantity")
    side: Literal["Borrow", "Lend", "Repay", "Redeem"] = Field(alias="side")
    symbol: Literal[
        "BTC",
        "ETH",
        "SOL",
        "USDC",
        "USDT",
        "PYTH",
        "JTO",
        "JUP",
        "RNDR",
        "TNSR",
        "W",
        "INF",
        "MOBILE",
        "KMNO",
        "MEW",
        "DRIFT",
        "WIF",
        "CLOUD",
        "MICHI",
        "TRUMP",
        "TOLY",
        "BONK",
        "RAY",
        "WEN",
        "BODEN",
        "SAMO",
        "BOME",
        "HNT",
        "IO",
        "DJT",
        "MATIC",
        "BNB",
        "HYPE",
        "VIRTUAL",
        "AI16Z",
        "PENGU",
        "ME",
        "GRASS",
        "MOVE",
        "DOGE",
        "SUI",
        "BNSOL",
        "JITOSOL",
        "MOODENG",
        "LESTER",
        "DARAM",
        "DEGENAI",
        "ELIZA",
        "AIXBT",
        "VVAIFU",
        "FARTCOIN",
        "SWARMS",
        "FATHA",
        "ADA",
        "WGF",
        "UBC",
        "GRIFFAIN",
        "XRP",
        "GOAT",
        "OM",
        "AVA",
        "OPUS",
        "GOON",
        "PNUT",
        "FWOG",
        "MBAPPE",
        "ZEREBRO",
        "DINO",
        "EVAN",
        "HAMMY",
        "LTC",
        "BCH",
        "MUMU",
        "OPAIUM",
        "SLERF",
        "MOTHER",
        "RLB",
        "BLINK",
        "UNI",
        "LINK",
        "TRX",
        "AAVE",
        "CRV",
        "MKR",
        "ENA",
        "POPCAT",
        "MYRO",
        "PONKE",
        "ZRO",
        "GIGA",
        "NOT",
        "DOGS",
        "WLD",
        "BFUSD",
        "CORGIAI",
        "MANEKI",
        "BERA",
        "SPX",
        "LUNA2",
        "HONEY",
        "FTM",
        "PEPE",
        "APT",
        "AVAX",
        "AR",
        "TON",
        "SWELL",
        "BUCK",
        "FWOG2",
        "MAVIA",
    ] = Field(alias="symbol")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# --- Request For Quote Payload ---
class BackpackRawRequestForQuoteRequest(BaseModel):
    """Raw request payload for submitting a Request For Quote (RFQ).

    Maps to OpenAPI schema: RequestForQuotePayload
    """

    symbol: RawBpNonEmptyStringMax64 = Field(alias="symbol")
    quantity: RawBpParsableFiniteDecimalString | None = Field(default=None, alias="quantity")
    quoteQuantity: RawBpParsableFiniteDecimalString | None = Field(
        default=None, alias="quoteQuantity"
    )
    autoAcceptThreshold: RawBpParsableFiniteDecimalString | None = Field(
        default=None, alias="autoAcceptThreshold"
    )
    submissionTimeMs: RawBpUint32 | None = Field(default=None, alias="submissionTimeMs")
    expiryTimeMs: RawBpUint32 | None = Field(default=None, alias="expiryTimeMs")
    clientId: RawBpNonEmptyStringMax64 | None = Field(default=None, alias="clientId")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# --- Quote Submit Payload ---
class BackpackRawQuoteSubmitRequest(BaseModel):
    """Raw request payload for submitting a quote in response to an RFQ.

    Maps to OpenAPI schema: QuotePayload
    """

    rfqId: RawBpNonEmptyStringMax64 = Field(alias="rfqId")
    side: Literal["Bid", "Ask"] = Field(alias="side")
    price: RawBpParsableFiniteDecimalString = Field(alias="price")
    clientQuoteId: RawBpNonEmptyStringMax64 | None = Field(default=None, alias="clientQuoteId")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# --- Quote Accept Payload ---
class BackpackRawQuoteAcceptRequest(BaseModel):
    """Raw request payload for accepting a quote.

    Maps to OpenAPI schema: QuoteAcceptPayload
    """

    rfqId: RawBpNonEmptyStringMax64 = Field(alias="rfqId")
    quoteId: RawBpNonEmptyStringMax64 = Field(alias="quoteId")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# --- RFQ Cancel Payload ---
class BackpackRawRequestForQuoteCancelRequest(BaseModel):
    """Raw request payload for cancelling an RFQ.

    Maps to OpenAPI schema: RequestForQuoteCancelPayload
    """

    rfqId: RawBpNonEmptyStringMax64 = Field(alias="rfqId")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# --- RFQ Refresh Payload ---
class BackpackRawRequestForQuoteRefreshRequest(BaseModel):
    """Raw request payload for refreshing an RFQ.

    Maps to OpenAPI schema: RequestForQuoteRefreshPayload
    """

    rfqId: RawBpNonEmptyStringMax64 = Field(alias="rfqId")
    submissionTimeMs: RawBpUint32 | None = Field(default=None, alias="submissionTimeMs")
    expiryTimeMs: RawBpUint32 | None = Field(default=None, alias="expiryTimeMs")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# --- Internal Transfer Payload ---
class BackpackRawInternalTransferRequest(BaseModel):
    """Raw request payload for internal capital transfers.

    Maps to OpenAPI schema: InternalTransferPayload
    """

    # Required fields
    symbol: RawBpNonEmptyStringMax64 = Field(alias="symbol")
    quantity: RawBpParsableFiniteDecimalString = Field(alias="quantity")
    fromAccount: Literal["SPOT", "MARGIN", "FUTURES"] = Field(alias="fromAccount")
    toAccount: Literal["SPOT", "MARGIN", "FUTURES"] = Field(alias="toAccount")

    # Optional fields
    clientId: RawBpNonEmptyStringMax64 | None = Field(default=None, alias="clientId")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)
