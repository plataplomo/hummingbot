"""CyberDeltaEngine: Backpack API Raw Query Parameter Models.

This module defines Pydantic models for validating query parameters in Backpack API requests.
These models represent the exact structure and raw data types expected by Backpack endpoints.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.apis.backpack.models.bp_common_raw_types import (
    RawBpNonEmptyStringMax64,
    RawBpNonNegativeInt,
)


class BackpackRawGetTickerParams(BaseModel):
    """Query parameters for GET /api/v1/ticker endpoint.

    This model validates the query parameters required to retrieve ticker data
    for a specific trading symbol from the Backpack API.

    Attributes:
        symbol: The trading symbol to get ticker data for (e.g., "SOL_USDC").
                Must be a non-empty string with maximum 64 characters.

    """

    symbol: RawBpNonEmptyStringMax64 = Field(alias="symbol")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class BackpackRawGetOrderBookParams(BaseModel):
    """Query parameters for GET /api/v1/depth endpoint.

    This model validates the query parameters for retrieving order book depth data
    from the Backpack API.

    Attributes:
        symbol: The trading symbol to get order book data for (e.g., "SOL_USDC").
                Must be a non-empty string with maximum 64 characters.
        limit: Optional maximum number of bids and asks to retrieve.
               Must be a non-negative integer if provided.

    """

    symbol: RawBpNonEmptyStringMax64 = Field(alias="symbol")
    limit: RawBpNonNegativeInt | None = Field(default=None, alias="limit")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class BackpackRawGetRecentTradesParams(BaseModel):
    """Query parameters for GET /api/v1/trades endpoint.

    This model validates the query parameters for retrieving recent trade data
    from the Backpack API.

    Attributes:
        symbol: The trading symbol to get recent trades for (e.g., "SOL_USDC").
                Must be a non-empty string with maximum 64 characters.
        limit: Optional maximum number of trades to retrieve.
               Must be a non-negative integer if provided.

    """

    symbol: RawBpNonEmptyStringMax64 = Field(alias="symbol")
    limit: RawBpNonNegativeInt | None = Field(default=None, alias="limit")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class BackpackRawGetBalancesParams(BaseModel):
    """Query parameters for GET /api/v1/capital endpoint.

    This model represents query parameters for retrieving account balance data.
    This endpoint typically requires no query parameters.
    """

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class BackpackRawGetPositionsParams(BaseModel):
    """Query parameters for GET /api/v1/position endpoint.

    This model represents query parameters for retrieving position data.
    This endpoint typically requires no query parameters when fetching all positions.
    """

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class BackpackRawGetOpenOrdersParams(BaseModel):
    """Query parameters for GET /api/v1/orders endpoint.

    This model validates the query parameters for retrieving open orders
    from the Backpack API.

    Attributes:
        symbol: Optional trading symbol to filter orders by (e.g., "SOL_USDC").
                If None, returns orders for all symbols.
                Must be a non-empty string with maximum 64 characters if provided.

    """

    symbol: RawBpNonEmptyStringMax64 | None = Field(default=None, alias="symbol")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class BackpackRawGetFundingRateParams(BaseModel):
    """Query parameters for GET /api/v1/funding endpoint.

    This model validates the query parameters for retrieving current funding rate
    data for a specific symbol from the Backpack API.

    Attributes:
        symbol: The trading symbol to get funding rate for (e.g., "SOL_USDC").
                Must be a non-empty string with maximum 64 characters.

    """

    symbol: RawBpNonEmptyStringMax64 = Field(alias="symbol")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class BackpackRawGetHistoricalFundingRatesParams(BaseModel):
    """Query parameters for GET /api/v1/fundingRates endpoint.

    This model validates the query parameters for retrieving historical funding rate
    data from the Backpack API.

    Attributes:
        symbol: The trading symbol to get funding rates for (e.g., "SOL_USDC").
                Must be a non-empty string with maximum 64 characters.
        startTime: Optional start time in seconds since Unix epoch.
                   Raw integer timestamp for filtering results (converted from milliseconds).
        endTime: Optional end time in seconds since Unix epoch.
                 Raw integer timestamp for filtering results (converted from milliseconds).
        limit: Optional maximum number of funding rate records to retrieve.
               Must be a non-negative integer if provided.

    """

    symbol: RawBpNonEmptyStringMax64 = Field(alias="symbol")
    startTime: int | None = Field(default=None, alias="startTime")
    endTime: int | None = Field(default=None, alias="endTime")
    limit: RawBpNonNegativeInt | None = Field(default=None, alias="limit")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class BackpackRawGetAccountInfoParams(BaseModel):
    """Query parameters for GET /api/v1/account endpoint.

    This model represents query parameters for retrieving account information.
    This endpoint typically requires no query parameters.
    """

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class BackpackRawGetMarketsParams(BaseModel):
    """Query parameters for GET /api/v1/markets endpoint.

    This model represents query parameters for retrieving markets metadata.
    This endpoint typically requires no query parameters.
    """

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class BackpackRawGetMarketParams(BaseModel):
    """Query parameters for GET /api/v1/market endpoint.

    This model validates the query parameters for retrieving a specific market
    by symbol from the Backpack API.

    Attributes:
        symbol: The trading symbol for the market (e.g., "SOL_USDC").
                Must be a non-empty string with maximum 64 characters.
                Required as a query parameter for this endpoint.

    """

    symbol: RawBpNonEmptyStringMax64 = Field(alias="symbol")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class BackpackRawGetOrderHistoryParams(BaseModel):
    """Query parameters for GET /api/v1/orderHistory endpoint.

    This model validates the query parameters for retrieving order history
    from the Backpack API.

    Attributes:
        symbol: Optional trading symbol to filter orders by (e.g., "SOL_USDC").
                Must be a non-empty string with maximum 64 characters if provided.
        orderId: Optional specific order ID to filter by.
                 Must be a non-empty string with maximum 64 characters if provided.
        clientId: Optional client order ID to filter by.
                  Must be a non-empty string with maximum 64 characters if provided.
        limit: Optional maximum number of orders to retrieve.
               Must be a non-negative integer if provided.
        start_time: Optional start time in milliseconds since Unix epoch.
                    Raw integer timestamp for filtering results.
        end_time: Optional end time in milliseconds since Unix epoch.
                  Raw integer timestamp for filtering results.

    """

    symbol: RawBpNonEmptyStringMax64 | None = Field(default=None, alias="symbol")
    orderId: RawBpNonEmptyStringMax64 | None = Field(default=None, alias="orderId")
    clientId: RawBpNonEmptyStringMax64 | None = Field(default=None, alias="clientId")
    limit: RawBpNonNegativeInt | None = Field(default=None, alias="limit")
    start_time: int | None = Field(default=None, alias="from")
    end_time: int | None = Field(default=None, alias="to")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class BackpackRawGetTradeHistoryParams(BaseModel):
    """Query parameters for GET /api/v1/fills endpoint.

    This model validates the query parameters for retrieving trade history (fills)
    from the Backpack API.

    Attributes:
        symbol: Optional trading symbol to filter trades by (e.g., "SOL_USDC").
                Must be a non-empty string with maximum 64 characters if provided.
        limit: Optional maximum number of trades to retrieve.
               Must be a non-negative integer if provided.
        start_time: Optional start time in milliseconds since Unix epoch.
                    Raw integer timestamp for filtering results.
        end_time: Optional end time in milliseconds since Unix epoch.
                  Raw integer timestamp for filtering results.
        fromId: Optional trade ID to start pagination from.
                Must be a non-empty string with maximum 64 characters if provided.

    """

    symbol: RawBpNonEmptyStringMax64 | None = Field(default=None, alias="symbol")
    limit: RawBpNonNegativeInt | None = Field(default=None, alias="limit")
    start_time: int | None = Field(default=None, alias="from")
    end_time: int | None = Field(default=None, alias="to")
    fromId: RawBpNonEmptyStringMax64 | None = Field(default=None, alias="fromId")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class BackpackRawGetMarketDataParams(BaseModel):
    """Query parameters for GET /api/v1/klines endpoint.

    This model validates the query parameters for retrieving candlestick/kline data
    from the Backpack API.

    Attributes:
        symbol: The trading symbol to get kline data for (e.g., "SOL_USDC").
                Must be a non-empty string with maximum 64 characters.
        interval: The candlestick interval. Must be one of the supported intervals:
                  "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d",
                  "1w".
        startTime: Optional start time in seconds since Unix epoch.
                   Raw integer timestamp for filtering results (converted from milliseconds).
        endTime: Optional end time in seconds since Unix epoch.
                 Raw integer timestamp for filtering results (converted from milliseconds).
        limit: Optional maximum number of candlesticks to retrieve.
               Must be a non-negative integer if provided.

    """

    symbol: RawBpNonEmptyStringMax64 = Field(alias="symbol")
    interval: Literal[
        "1m",
        "3m",
        "5m",
        "15m",
        "30m",
        "1h",
        "2h",
        "4h",
        "6h",
        "8h",
        "12h",
        "1d",
        "3d",
        "1w",
        "1M",
    ] = Field(alias="interval")
    startTime: int | None = Field(default=None, alias="startTime")
    endTime: int | None = Field(default=None, alias="endTime")
    limit: RawBpNonNegativeInt | None = Field(default=None, alias="limit")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class BackpackRawGetHistoricalTradesParams(BaseModel):
    """Query parameters for GET /api/v1/trades/history endpoint.

    This model validates the query parameters for retrieving historical public trade data
    from the Backpack API.

    Attributes:
        symbol: The trading symbol to get historical trades for (e.g., "SOL_USDC").
                Must be a non-empty string with maximum 64 characters.
        limit: Optional maximum number of trades to retrieve.
               Must be a non-negative integer if provided.
        fromId: Optional trade ID to start pagination from.
                Must be a non-empty string with maximum 64 characters if provided.

    """

    symbol: RawBpNonEmptyStringMax64 = Field(alias="symbol")
    limit: RawBpNonNegativeInt | None = Field(default=None, alias="limit")
    fromId: RawBpNonEmptyStringMax64 | None = Field(default=None, alias="fromId")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class BackpackRawGetOrderParams(BaseModel):
    """Query parameters for GET /api/v1/orders/{orderIdOrClientId} endpoint.

    This model validates the query parameters for retrieving a specific order
    by its ID or client ID from the Backpack API.

    Attributes:
        symbol: The trading symbol for the order (e.g., "SOL_USDC").
                Must be a non-empty string with maximum 64 characters.
                Required as a query parameter for this endpoint.

    """

    symbol: RawBpNonEmptyStringMax64 = Field(alias="symbol")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# --- Account Limits Query Parameter Models (INTERNAL USE ONLY) ---


class BackpackRawMaxBorrowQuantityParams(BaseModel):
    """Query parameters for GET /api/v1/account/limits/borrow endpoint.

    INTERNAL USE ONLY: This model is for risk calculation validation
    and reconciliation purposes within BackpackAccountService.

    Attributes:
        symbol: The asset symbol to check borrowing limits for.
    """

    symbol: str = Field(..., alias="symbol")

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class BackpackRawMaxOrderQuantityParams(BaseModel):
    """Query parameters for GET /api/v1/account/limits/order endpoint.

    INTERNAL USE ONLY: This model is for risk calculation validation
    and reconciliation purposes within BackpackAccountService.

    Attributes:
        symbol: The trading symbol for the order.
        side: The order side ("Bid" or "Ask").
        price: Optional price for the order calculation.
        reduce_only: Optional flag for reduce-only orders.
        auto_borrow: Optional flag for auto-borrow feature.
        auto_borrow_repay: Optional flag for auto-borrow repay feature.
        auto_lend_redeem: Optional flag for auto-lend redeem feature.
    """

    symbol: str = Field(..., alias="symbol")
    side: str = Field(..., alias="side")
    price: str | None = Field(default=None, alias="price")
    reduce_only: bool | None = Field(default=None, alias="reduceOnly")
    auto_borrow: bool | None = Field(default=None, alias="autoBorrow")
    auto_borrow_repay: bool | None = Field(default=None, alias="autoBorrowRepay")
    auto_lend_redeem: bool | None = Field(default=None, alias="autoLendRedeem")

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class BackpackRawMaxWithdrawalQuantityParams(BaseModel):
    """Query parameters for GET /api/v1/account/limits/withdrawal endpoint.

    INTERNAL USE ONLY: This model is for risk calculation validation
    and reconciliation purposes within BackpackAccountService.

    Attributes:
        symbol: The asset symbol for withdrawal limits.
        auto_borrow: Optional flag for auto-borrow feature.
        auto_lend_redeem: Optional flag for auto-lend redeem feature.
    """

    symbol: str = Field(..., alias="symbol")
    auto_borrow: bool | None = Field(default=None, alias="autoBorrow")
    auto_lend_redeem: bool | None = Field(default=None, alias="autoLendRedeem")

    model_config = ConfigDict(extra="forbid", populate_by_name=True)
