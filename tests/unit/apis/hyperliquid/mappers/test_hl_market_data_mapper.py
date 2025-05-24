"""
Unit tests for the Hyperliquid Market Data Mapper.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

import pytest
from _pytest.logging import LogCaptureFixture
from pydantic import ValidationError

# Third-party imports for type checking only
if TYPE_CHECKING:
    from pytest_mock import MockerFixture

# Project-specific imports
from cyberdelta.apis.exchange_names import ExchangeName
from cyberdelta.apis.hyperliquid.mappers.hl_market_data_mapper import HyperliquidMarketDataMapper
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetCtx,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import (
    HyperliquidRawBookLevel,
    HyperliquidRawL2Book,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import (
    HyperliquidRawPublicTrade,
)
from cyberdelta.core.models import OrderBook, Ticker, Trade
from cyberdelta.core.models.enums import OrderSide
from cyberdelta.core.models.market.funding_rate import (
    FundingRate,
    HyperliquidFundingDetails,
)
from cyberdelta.core.models.market.trade import HyperliquidTradeDetails
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value

# Fixtures for Market Data Mapper tests


@pytest.fixture
def market_data_mapper() -> HyperliquidMarketDataMapper:
    """Provide an instance of HyperliquidMarketDataMapper."""
    return HyperliquidMarketDataMapper()


# Keep the old mapper fixture for backward compatibility in tests that don't specify domain
@pytest.fixture
def mapper() -> HyperliquidMarketDataMapper:
    """Provide an instance of HyperliquidMarketDataMapper for backward compatibility."""
    return HyperliquidMarketDataMapper()


# --- Fixtures for HyperliquidRawAssetCtx ---


@pytest.fixture
def hyperliquid_raw_asset_ctx_eth_fixture() -> HyperliquidRawAssetCtx:
    """Provides a valid HyperliquidRawAssetCtx for ETH-PERP."""
    return HyperliquidRawAssetCtx(
        name="ETH-PERP",
        funding="0.00001234",
        markPx="3010.75",  # Instantiation uses alias
        prevDayPx="2950.00",  # Instantiation uses alias
        dayNtlVlm="50000000.00",  # Instantiation uses alias
        impactPx="3009.50",  # Instantiation uses alias
    )


@pytest.fixture
def hyperliquid_raw_asset_ctx_btc_no_impact_px_fixture() -> HyperliquidRawAssetCtx:
    """Provides a valid HyperliquidRawAssetCtx for BTC-PERP with no impactPx."""
    return HyperliquidRawAssetCtx(
        name="BTC-PERP",
        funding="-0.00000567",
        markPx="60200.50",  # Instantiation uses alias
        prevDayPx="61000.00",  # Instantiation uses alias
        dayNtlVlm="120000000.00",  # Instantiation uses alias
        impactPx=None,
    )


# --- Fixtures for HyperliquidRawL2Book ---


@pytest.fixture
def hyperliquid_raw_book_level_fixture_bid() -> HyperliquidRawBookLevel:
    """Provides a valid HyperliquidRawBookLevel for a bid."""
    return HyperliquidRawBookLevel(px="2999.50", sz="10.5", n=2)


@pytest.fixture
def hyperliquid_raw_book_level_fixture_ask() -> HyperliquidRawBookLevel:
    """Provides a valid HyperliquidRawBookLevel for an ask."""
    return HyperliquidRawBookLevel(px="3000.50", sz="5.25", n=3)


@pytest.fixture
def hyperliquid_raw_l2_book_eth_fixture(
    # hyperliquid_raw_book_level_fixture_bid: HyperliquidRawBookLevel, # ARG001: Removed
    # hyperliquid_raw_book_level_fixture_ask: HyperliquidRawBookLevel, # ARG001: Removed
) -> HyperliquidRawL2Book:
    """Provides a valid HyperliquidRawL2Book for ETH-PERP."""
    # Create more levels for a more realistic book
    bid_levels = [
        HyperliquidRawBookLevel(px="2999.50", sz="10.5", n=2),
        HyperliquidRawBookLevel(px="2999.00", sz="20.0", n=5),
        HyperliquidRawBookLevel(px="2998.50", sz="15.0", n=3),
    ]
    ask_levels = [
        HyperliquidRawBookLevel(px="3000.50", sz="5.25", n=3),
        HyperliquidRawBookLevel(px="3001.00", sz="12.0", n=4),
        HyperliquidRawBookLevel(px="3001.50", sz="8.0", n=2),
    ]
    return HyperliquidRawL2Book(
        coin="ETH-PERP",
        levels=[bid_levels, ask_levels],
        time=int(datetime.now(UTC).timestamp() * 1000 - 2000),  # 2 seconds ago
    )


@pytest.fixture
def hyperliquid_raw_l2_book_empty_fixture() -> HyperliquidRawL2Book:
    """Provides an empty HyperliquidRawL2Book."""
    return HyperliquidRawL2Book(
        coin="BTC-PERP",
        levels=[[], []],  # Empty bids and asks
        time=int(datetime.now(UTC).timestamp() * 1000 - 1000),  # 1 second ago
    )


# --- Fixtures for HyperliquidRawPublicTrade ---


@pytest.fixture
def hyperliquid_raw_public_trade_buy_fixture() -> HyperliquidRawPublicTrade:
    """Provides a valid HyperliquidRawPublicTrade for a BUY trade."""
    return HyperliquidRawPublicTrade(
        coin="ETH-PERP",
        side="B",
        px="3002.00",
        sz="1.5",
        time=int(datetime.now(UTC).timestamp() * 1000 - 3000),  # 3 seconds ago
        hash="0xghi789",
    )


@pytest.fixture
def hyperliquid_raw_public_trade_sell_fixture() -> HyperliquidRawPublicTrade:
    """Provides a valid HyperliquidRawPublicTrade for a SELL trade."""
    return HyperliquidRawPublicTrade(
        coin="BTC-PERP",
        side="A",  # Sell
        px="60100.75",
        sz="0.02",
        time=int(datetime.now(UTC).timestamp() * 1000 - 1500),  # 1.5 seconds ago
        hash="0xjkl012",
    )


# --- Tests for map_raw_ctx_to_ticker ---


def test_map_raw_ctx_to_ticker_eth(
    mapper: HyperliquidMarketDataMapper,
    hyperliquid_raw_asset_ctx_eth_fixture: HyperliquidRawAssetCtx,
) -> None:
    """Test mapping a raw asset context for ETH-PERP to an internal Ticker."""
    raw_ctx = hyperliquid_raw_asset_ctx_eth_fixture
    ticker = mapper.transform_raw_asset_ctx_to_ticker(raw_ctx)

    assert isinstance(ticker, Ticker)
    assert ticker.symbol == raw_ctx.name  # Access uses Python name
    assert isinstance(ticker.timestamp, datetime)
    assert (datetime.now(UTC) - ticker.timestamp) < timedelta(seconds=5)

    assert ticker.price == Decimal(raw_ctx.mark_px)  # Access uses Python name mark_px
    assert ticker.volume == Decimal(raw_ctx.day_ntl_vlm)  # Access uses Python name day_ntl_vlm


def test_map_raw_ctx_to_ticker_btc_no_impact(
    mapper: HyperliquidMarketDataMapper,
    hyperliquid_raw_asset_ctx_btc_no_impact_px_fixture: HyperliquidRawAssetCtx,
) -> None:
    """Test mapping for BTC-PERP with no impact price and negative funding."""
    raw_ctx = hyperliquid_raw_asset_ctx_btc_no_impact_px_fixture
    ticker = mapper.transform_raw_asset_ctx_to_ticker(raw_ctx)

    assert isinstance(ticker, Ticker)
    assert ticker.symbol == raw_ctx.name  # Access uses Python name
    assert ticker.price == Decimal(raw_ctx.mark_px)  # Access uses Python name mark_px
    assert ticker.volume == Decimal(raw_ctx.day_ntl_vlm)  # Access uses Python name day_ntl_vlm


# --- Tests for map_raw_order_book ---


def test_map_raw_order_book_eth(
    mapper: HyperliquidMarketDataMapper,
    hyperliquid_raw_l2_book_eth_fixture: HyperliquidRawL2Book,
) -> None:
    """Test mapping a raw L2 book for ETH-PERP to an internal OrderBook."""
    raw_book = hyperliquid_raw_l2_book_eth_fixture
    order_book = mapper.transform_raw_order_book_to_internal(raw_book)

    assert isinstance(order_book, OrderBook)
    assert order_book.symbol == raw_book.coin
    assert isinstance(order_book.timestamp, datetime)
    assert order_book.timestamp.timestamp() * 1000 == raw_book.time

    assert len(order_book.bids) == 3
    assert order_book.bids[0] == (Decimal("2999.50"), Decimal("10.5"))
    assert order_book.bids[1] == (Decimal("2999.00"), Decimal("20.0"))
    assert order_book.bids[2] == (Decimal("2998.50"), Decimal("15.0"))

    assert len(order_book.asks) == 3
    assert order_book.asks[0] == (Decimal("3000.50"), Decimal("5.25"))
    assert order_book.asks[1] == (Decimal("3001.00"), Decimal("12.0"))
    assert order_book.asks[2] == (Decimal("3001.50"), Decimal("8.0"))


def test_map_raw_order_book_with_depth_limit(
    mapper: HyperliquidMarketDataMapper,
    hyperliquid_raw_l2_book_eth_fixture: HyperliquidRawL2Book,
) -> None:
    """Test mapping with a depth limit."""
    raw_book = hyperliquid_raw_l2_book_eth_fixture
    order_book = mapper.transform_raw_order_book_to_internal(raw_book, depth=2)

    assert isinstance(order_book, OrderBook)
    assert len(order_book.bids) == 2
    assert order_book.bids[0] == (Decimal("2999.50"), Decimal("10.5"))
    assert order_book.bids[1] == (Decimal("2999.00"), Decimal("20.0"))

    assert len(order_book.asks) == 2
    assert order_book.asks[0] == (Decimal("3000.50"), Decimal("5.25"))
    assert order_book.asks[1] == (Decimal("3001.00"), Decimal("12.0"))


def test_map_raw_order_book_empty(
    mapper: HyperliquidMarketDataMapper,
    hyperliquid_raw_l2_book_empty_fixture: HyperliquidRawL2Book,
) -> None:
    """Test mapping an empty raw L2 book."""
    raw_book = hyperliquid_raw_l2_book_empty_fixture
    order_book = mapper.transform_raw_order_book_to_internal(raw_book)

    assert isinstance(order_book, OrderBook)
    assert order_book.symbol == raw_book.coin
    assert isinstance(order_book.timestamp, datetime)
    assert not order_book.bids
    assert not order_book.asks


def test_map_raw_order_book_malformed_levels_structure(
    # mapper: HyperliquidMarketDataMapper, # ARG001: Removed
) -> None:
    """Test mapping when raw_book.levels has an unexpected structure
    (e.g., not a list of 2 lists).
    """
    # The HyperliquidRawL2Book model itself has a validator for levels structure.
    # This test ensures the mapper handles it gracefully if such data somehow passed.
    # (Pydantic should prevent this, so this is more of a conceptual check on mapper robustness).

    # Case 1: levels is not a list (should be caught by Pydantic on HyperliquidRawL2Book)
    with pytest.raises(ValidationError):
        raw_data_invalid_levels_type = {
            "coin": "MALFORMED-PERP",
            "levels": "not_a_list",
            "time": 123,
        }
        HyperliquidRawL2Book.model_validate(raw_data_invalid_levels_type)

    # Case 2: levels is a list, but not of length 2 (should be caught by Pydantic)
    with pytest.raises(ValidationError):
        raw_data_invalid_levels_item_type = {
            "coin": "MALFORMED-PERP",
            "levels": ["not_list_1", "not_list_2"],
            "time": 123,
        }
        HyperliquidRawL2Book.model_validate(raw_data_invalid_levels_item_type)

    # The mapper logic itself also logs warnings if levels structure is bad after
    # Pydantic validation.
    # We can simulate a raw_book object that bypasses Pydantic for the levels field
    # to test this.
    # If a non-finite decimal somehow got past RawFiniteDecimalStr for px:
    # This scenario is unlikely due to RawFiniteDecimalStr but tests mapper's
    # direct use of parse_decimal_value
    # For Pydantic v2, direct instantiation with invalid types is harder if
    # validators are robust.
    # The mapper directly calls parse_decimal_value, which would raise ValueError.


# --- Tests for transform_raw_public_trade_to_internal ---


def test_transform_raw_public_trade_to_internal_buy(
    mapper: HyperliquidMarketDataMapper,
    hyperliquid_raw_public_trade_buy_fixture: HyperliquidRawPublicTrade,
) -> None:
    """Test transforming a raw public BUY trade to an internal Trade model."""
    raw_trade = hyperliquid_raw_public_trade_buy_fixture
    trade = mapper.transform_raw_public_trade_to_internal(raw_trade)

    assert isinstance(trade, Trade)
    assert trade.id == raw_trade.hash  # Public trades use hash as ID
    assert trade.symbol == raw_trade.coin
    assert isinstance(trade.executed_at, datetime)
    assert trade.executed_at.timestamp() * 1000 == raw_trade.time
    assert trade.side == OrderSide.BUY
    # Public trades don't have order_id, client_order_id, fee, is_maker directly
    assert trade.order_id == "UNKNOWN_PUBLIC_TRADE"  # Mapper default
    assert trade.exchange == ExchangeName.HYPERLIQUID.value
    assert trade.client_order_id is None
    assert trade.price == Decimal(raw_trade.px)
    assert trade.quantity == Decimal(raw_trade.sz)
    assert trade.fee == Decimal("0")  # Mapper default for public
    assert trade.fee_asset is None  # Mapper default for public
    assert trade.is_maker is None  # Not available in public trades

    assert trade.hl_details is not None
    assert isinstance(trade.hl_details, HyperliquidTradeDetails)
    assert trade.hl_details.trade_hash == raw_trade.hash
    assert trade.hl_details.liquidation_mark_px is None
    assert trade.hl_details.start_position is None
    assert trade.hl_details.dir is None
    assert trade.bp_details is None


def test_transform_raw_public_trade_to_internal_sell(
    mapper: HyperliquidMarketDataMapper,
    hyperliquid_raw_public_trade_sell_fixture: HyperliquidRawPublicTrade,
) -> None:
    """Test transforming a raw public SELL trade to an internal Trade model."""
    raw_trade = hyperliquid_raw_public_trade_sell_fixture
    trade = mapper.transform_raw_public_trade_to_internal(raw_trade)

    assert isinstance(trade, Trade)
    assert trade.id == raw_trade.hash
    assert trade.symbol == raw_trade.coin
    assert isinstance(trade.executed_at, datetime)
    assert trade.executed_at.timestamp() * 1000 == raw_trade.time
    assert trade.side == OrderSide.SELL
    assert trade.order_id == "UNKNOWN_PUBLIC_TRADE"
    assert trade.price == Decimal(raw_trade.px)
    assert trade.quantity == Decimal(raw_trade.sz)
    assert trade.fee == Decimal("0")
    assert trade.is_maker is None

    assert trade.hl_details is not None
    assert trade.hl_details.trade_hash == raw_trade.hash


def test_transform_raw_public_trade_invalid_data() -> None:
    """Test transform_raw_public_trade with data that would fail parsing (e.g. price)."""
    invalid_raw_data = {
        "coin": "XYZ-PERP",
        "side": "B",
        "px": "not_a_price",
        "sz": "1",
        "time": int(datetime.now(UTC).timestamp() * 1000),
        "hash": "0xbad",
    }
    with pytest.raises(ValidationError):
        HyperliquidRawPublicTrade.model_validate(invalid_raw_data)

    # If a non-finite decimal somehow got past RawFiniteDecimalStr for px:
    # This scenario is unlikely due to RawFiniteDecimalStr but tests mapper's
    # direct use of parse_decimal_value
    # For Pydantic v2, direct instantiation with invalid types is harder if
    # validators are robust.
    # The mapper directly calls parse_decimal_value, which would raise ValueError.


# --- Tests for map_raw_trades ---


def test_map_raw_trades_empty_list(mapper: HyperliquidMarketDataMapper) -> None:
    """Test mapping an empty list of raw public trades."""
    trades = mapper.transform_raw_trades([])
    assert isinstance(trades, list)
    assert not trades


def test_map_raw_trades_populated_list(
    mapper: HyperliquidMarketDataMapper,
    hyperliquid_raw_public_trade_buy_fixture: HyperliquidRawPublicTrade,
    hyperliquid_raw_public_trade_sell_fixture: HyperliquidRawPublicTrade,
) -> None:
    """Test mapping a list of raw public trades."""
    raw_trades_list = [
        hyperliquid_raw_public_trade_buy_fixture,
        hyperliquid_raw_public_trade_sell_fixture,
    ]
    trades = mapper.transform_raw_trades(raw_trades_list)

    assert isinstance(trades, list)
    assert len(trades) == 2

    # Check first trade (buy)
    assert trades[0].id == hyperliquid_raw_public_trade_buy_fixture.hash
    assert trades[0].symbol == hyperliquid_raw_public_trade_buy_fixture.coin
    assert trades[0].side == OrderSide.BUY
    assert trades[0].price == Decimal(hyperliquid_raw_public_trade_buy_fixture.px)

    # Check second trade (sell)
    assert trades[1].id == hyperliquid_raw_public_trade_sell_fixture.hash
    assert trades[1].symbol == hyperliquid_raw_public_trade_sell_fixture.coin
    assert trades[1].side == OrderSide.SELL
    assert trades[1].price == Decimal(hyperliquid_raw_public_trade_sell_fixture.px)


def test_map_raw_trades_with_limit(
    mapper: HyperliquidMarketDataMapper,
    hyperliquid_raw_public_trade_buy_fixture: HyperliquidRawPublicTrade,
    hyperliquid_raw_public_trade_sell_fixture: HyperliquidRawPublicTrade,
) -> None:
    """Test mapping with a limit applied."""
    # Create more fixtures if needed, or reuse existing ones for a list > limit
    raw_trade_3 = HyperliquidRawPublicTrade(
        coin="ADA-PERP",
        side="B",
        px="0.5",
        sz="1000",
        time=int(datetime.now(UTC).timestamp() * 1000 - 500),
        hash="0xmno345",
    )
    raw_trades_list = [
        hyperliquid_raw_public_trade_buy_fixture,  # ETH
        hyperliquid_raw_public_trade_sell_fixture,  # BTC
        raw_trade_3,  # ADA
    ]
    trades = mapper.transform_raw_trades(raw_trades_list, limit=2)

    assert isinstance(trades, list)
    assert len(trades) == 2
    # Ensure it took the first two
    assert trades[0].symbol == "ETH-PERP"
    assert trades[1].symbol == "BTC-PERP"


def test_map_raw_trades_limit_greater_than_list_size(
    mapper: HyperliquidMarketDataMapper,
    hyperliquid_raw_public_trade_buy_fixture: HyperliquidRawPublicTrade,
) -> None:
    """Test mapping when limit is larger than the number of trades."""
    raw_trades_list = [hyperliquid_raw_public_trade_buy_fixture]
    trades = mapper.transform_raw_trades(raw_trades_list, limit=5)
    assert isinstance(trades, list)
    assert len(trades) == 1
    assert trades[0].symbol == hyperliquid_raw_public_trade_buy_fixture.coin


def test_map_raw_trades_with_transformation_error(
    mapper: HyperliquidMarketDataMapper,
    hyperliquid_raw_public_trade_buy_fixture: HyperliquidRawPublicTrade,
    mocker: MockerFixture,  # For pytest-mock
    caplog: LogCaptureFixture,  # Added caplog fixture
) -> None:
    """Test that errors during individual trade transformation are handled gracefully."""
    mapper = HyperliquidMarketDataMapper()  # Use a fresh mapper instance

    # Prepare one trade that will succeed transformation by the mock,
    # and one that will cause the mock to raise an error.
    raw_trade_success = hyperliquid_raw_public_trade_buy_fixture.model_copy(deep=True)
    # Use .hash for trade ID and .sz for quantity as per HyperliquidRawPublicTrade model
    modified_data_success = raw_trade_success.model_dump()
    modified_data_success["hash"] = "success_hash_id"
    modified_data_success["coin"] = "ETH"
    modified_data_success["px"] = "2000.0"
    modified_data_success["sz"] = "1.0"
    modified_data_success["side"] = "B"
    modified_data_success["time"] = int(datetime.now(UTC).timestamp() * 1000)
    raw_trade_success = HyperliquidRawPublicTrade.model_validate(modified_data_success)

    raw_trade_fail = hyperliquid_raw_public_trade_buy_fixture.model_copy(deep=True)
    modified_data_fail = raw_trade_fail.model_dump()
    modified_data_fail["hash"] = "problematic_hash_id"
    modified_data_fail["coin"] = "BTC"
    modified_data_fail["px"] = "30000.0"
    modified_data_fail["sz"] = "0.1"
    modified_data_fail["side"] = "A"  # Corrected from "S" to "A" for sell
    modified_data_fail["time"] = int(datetime.now(UTC).timestamp() * 1000) + 1000
    raw_trade_fail = HyperliquidRawPublicTrade.model_validate(modified_data_fail)

    # This mock will be the side_effect for the patched method
    def mock_transform_side_effect(raw_trade_arg: HyperliquidRawPublicTrade) -> Trade | None:
        if raw_trade_arg.hash == "problematic_hash_id":  # Check .hash
            error_msg = "Simulated transformation error for problematic_hash_id"  # EM101
            raise ValueError(error_msg)

        side = OrderSide.BUY if raw_trade_arg.side == "B" else OrderSide.SELL
        # Public trades don't have a direct order_id, fee, or maker status.
        # The mapper sets defaults for these.
        return Trade(
            id=str(raw_trade_arg.hash),  # Trade.id is from hash for public trades
            symbol=raw_trade_arg.coin,
            executed_at=parse_datetime_utc(raw_trade_arg.time, field_name="time"),  # type: ignore[arg-type]
            side=side,
            order_id="UNKNOWN_PUBLIC_TRADE",  # Default from mapper
            exchange=ExchangeName.HYPERLIQUID.value,
            price=parse_decimal_value(raw_trade_arg.px, field_name="price"),  # type: ignore[arg-type]
            quantity=parse_decimal_value(raw_trade_arg.sz, field_name="quantity"),  # type: ignore[arg-type]
            client_order_id=None,  # Default
            fee=Decimal("0"),  # Default
            fee_asset=None,  # Default
            is_maker=None,  # Default
            hl_details=HyperliquidTradeDetails(
                trade_hash=raw_trade_arg.hash,
                # These are None for public trades, as per HyperliquidMarketDataMapper logic
                liquidation_mark_px=None,
                start_position=None,
                dir=None,
            ),
            bp_details=None,  # Default
        )

    patched_method = mocker.patch.object(
        HyperliquidMarketDataMapper,
        "transform_raw_public_trade_to_internal",
        side_effect=mock_transform_side_effect,
    )

    mapped_trades = mapper.transform_raw_trades(
        [raw_trade_success, raw_trade_fail],
    )  # Corrected method name

    assert len(mapped_trades) == 1, "Only the successful trade should be mapped"
    assert mapped_trades[0].id == "success_hash_id"  # Check 'id' now
    assert mapped_trades[0].symbol == "ETH"

    expected_log_message_part1 = (
        "Skipping public trade due to transformation error: "
        "Simulated transformation error for problematic_hash_id."
    )
    expected_log_message_part2 = "Raw: {'coin': 'BTC', 'side': 'A', 'px': '30000.0', "
    expected_log_message_part3 = "'sz': '0.1'"  # Broke the long line

    assert any(
        expected_log_message_part1 in record.message
        and expected_log_message_part2 in record.message
        and expected_log_message_part3 in record.message
        and "'hash': 'problematic_hash_id'"
        in record.message  # Ensure problematic_hash_id is in raw
        and record.levelno == logging.WARNING
        for record in caplog.records
    ), f"Warning for transformation error not found or doesn't match. Logs: {caplog.text}"

    patched_method.assert_any_call(raw_trade_success)
    patched_method.assert_any_call(raw_trade_fail)


# --- Tests for map_raw_ctx_to_funding_rate ---


def test_map_raw_ctx_to_funding_rate_eth(
    mapper: HyperliquidMarketDataMapper,
    hyperliquid_raw_asset_ctx_eth_fixture: HyperliquidRawAssetCtx,
) -> None:
    """Test mapping asset context to FundingRate for ETH with positive funding."""
    raw_ctx = hyperliquid_raw_asset_ctx_eth_fixture
    fr = mapper.transform_raw_asset_ctx_to_funding_rate(raw_ctx)

    assert fr is not None, "FundingRate object should be created"
    assert isinstance(fr, FundingRate)
    assert fr.symbol == raw_ctx.name  # Access uses Python name
    assert isinstance(fr.timestamp, datetime)
    assert (datetime.now(UTC) - fr.timestamp) < timedelta(seconds=10)

    assert fr.hl_details is not None
    expected_hourly_rate = Decimal(raw_ctx.funding)  # Access uses Python name
    assert fr.hl_details.hl_funding_hourly == expected_hourly_rate

    # map_raw_ctx_to_funding_rate calculates 8hr rate for fr.funding_rate
    assert fr.funding_rate == expected_hourly_rate * Decimal("8")

    assert fr.predicted_rate is None  # Set to None by mapper

    assert fr.next_funding_time is not None
    # Check that next_funding_time is roughly the start of the next hour UTC
    now_utc = datetime.now(UTC)
    expected_next_funding_time_approx = now_utc.replace(
        minute=0,
        second=0,
        microsecond=0,
    ) + timedelta(hours=1)
    assert abs((fr.next_funding_time - expected_next_funding_time_approx).total_seconds()) < 120, (
        f"Next funding time {fr.next_funding_time} not close to {expected_next_funding_time_approx}"
    )

    assert fr.mark_price == Decimal(raw_ctx.mark_px)  # Access uses Python name
    assert fr.index_price is None  # Set to None by mapper

    assert isinstance(fr.hl_details, HyperliquidFundingDetails)
    assert fr.hl_details.hl_impact_px == (
        Decimal(raw_ctx.impact_px) if raw_ctx.impact_px else None
    )  # Access uses Python name
    assert fr.bp_details is None


def test_map_raw_ctx_to_funding_rate_btc_negative_funding(
    mapper: HyperliquidMarketDataMapper,
    hyperliquid_raw_asset_ctx_btc_no_impact_px_fixture: HyperliquidRawAssetCtx,
) -> None:
    """Test mapping for BTC with negative funding and no impact price."""
    raw_ctx = hyperliquid_raw_asset_ctx_btc_no_impact_px_fixture
    fr = mapper.transform_raw_asset_ctx_to_funding_rate(raw_ctx)

    assert fr is not None, "FundingRate object should be created"
    assert isinstance(fr, FundingRate)
    assert fr.symbol == raw_ctx.name  # Access uses Python name

    assert fr.hl_details is not None
    expected_hourly_rate = Decimal(raw_ctx.funding)  # Access uses Python name
    assert fr.hl_details.hl_funding_hourly == expected_hourly_rate
    assert fr.funding_rate == expected_hourly_rate * Decimal("8")  # 8hr rate

    assert fr.mark_price == Decimal(raw_ctx.mark_px)  # Access uses Python name

    assert isinstance(fr.hl_details, HyperliquidFundingDetails)
    assert fr.hl_details.hl_impact_px is None  # Based on fixture
    assert fr.bp_details is None  # Ensure bp_details is None for HL


def test_map_raw_ctx_to_funding_rate_parsing_error_returns_funding_rate_with_none(
    mapper: HyperliquidMarketDataMapper,
    mocker: MockerFixture,
    # Not strictly needed if creating specific context:
    # hyperliquid_raw_asset_ctx_eth_fixture: HyperliquidRawAssetCtx
) -> None:
    """Test that if parsing raw_ctx.funding fails internally, the method returns a
    FundingRate object with funding_rate=None."""
    # Instantiate with a funding value that is valid for HyperliquidRawAssetCtx itself,
    # but we will mock parse_decimal_value to fail for this specific input.
    raw_ctx_problematic_funding = HyperliquidRawAssetCtx(  # Instantiation uses alias
        name="ERR-FUNDING-PERP",
        funding="0.0000999",  # Valid raw string, but parsing will be mocked to fail
        markPx="100",  # Alias for mark_px
        prevDayPx="99",  # Alias for prev_day_px
        dayNtlVlm="10000",  # Alias for day_ntl_vlm
        impactPx=None,  # Optional, can be None, alias for impact_px
    )

    original_parse_decimal = parse_decimal_value  # Save original for delegation

    # Define a side effect function for the mock
    def side_effect_for_funding_parse(
        value: str,  # Changed from Any to str, as raw_ctx.funding is str
        *,  # FBT001/FBT002: Make allow_none keyword-only
        allow_none: bool = False,
        field_name: str | None = None,
    ) -> Decimal | None:
        # Check if this call is for the 'funding' field and the specific value
        if field_name == "funding" and value == "0.0000999":
            # Raise ValueError to simulate a parsing failure more accurately than returning None
            # because parse_decimal_value is expected to raise on failure if allow_none=False
            error_message = (
                f"Simulated parse_decimal_value failure for field '{field_name}' "
                f"with value '{value}'"
            )
            raise ValueError(error_message)
        # For all other calls, delegate to the original parse_decimal_value
        return original_parse_decimal(
            value,
            allow_none=allow_none,
            field_name=field_name or "unknown_field",
        )

    # Mock parse_decimal_value within the scope of the mapper module
    mocked_parser = mocker.patch(
        "cyberdelta.apis.hyperliquid.mappers.hl_market_data_mapper.parse_decimal_value",
        side_effect=side_effect_for_funding_parse,
    )

    result = mapper.transform_raw_asset_ctx_to_funding_rate(raw_ctx_problematic_funding)
    assert result is not None
    assert result.funding_rate is None
    assert result.symbol == "ERR-FUNDING-PERP"
    assert result.mark_price == Decimal("100")  # Ensure other fields are still mapped

    # Verify that parse_decimal_value was called for 'funding' (and failed)
    # and also for 'markPx' (which should have succeeded via the original parser).
    funding_call_made = False
    mark_px_call_made = False
    for _args, kwargs in mocked_parser.call_args_list:
        field_name_arg = kwargs.get("field_name")
        if field_name_arg == "funding":
            funding_call_made = True
        elif field_name_arg in ("markPx", "mark_px"):  # HyperliquidRawAssetCtx uses alias
            mark_px_call_made = True

    assert funding_call_made, "parse_decimal_value was not called for 'funding'"
    assert mark_px_call_made, "parse_decimal_value was not called for 'mark_px' (or alias)"
