"""Integration tests for Hyperliquid public endpoints using real API client.

These tests validate the complete data pipeline from API client public methods
to final internal domain models using pytest-recording (VCR) for deterministic tests.
"""

from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.core.models import FundingRate, OrderBook, Ticker, Trade
from cyberdelta.core.models.market.candle import Candle


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_ticker_integration(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() method returns valid Ticker internal model.
    
    This validates the complete pipeline:
    - API method call (get_ticker)
    - Request building (metaAndAssetCtxs endpoint)
    - Response handling and validation
    - Mapping to internal Ticker model
    """
    ticker = await hl_api_for_test_env.get_ticker("BTC")
    
    # Validate internal model structure
    assert ticker is not None, "Ticker should not be None for BTC"
    assert isinstance(ticker, Ticker), f"Expected Ticker, got {type(ticker)}"
    
    # Validate core ticker fields
    assert ticker.symbol == "BTC", f"Expected symbol 'BTC', got '{ticker.symbol}'"
    assert isinstance(ticker.price, Decimal), f"Price should be Decimal, got {type(ticker.price)}"
    assert ticker.price > Decimal("0"), f"Price should be positive, got {ticker.price}"
    
    # Validate price precision (should be reasonable for BTC)
    assert ticker.price > Decimal("1000"), f"BTC price seems too low: {ticker.price}"
    assert ticker.price < Decimal("1000000"), f"BTC price seems too high: {ticker.price}"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_ticker_integration_eth(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() method with ETH symbol."""
    ticker = await hl_api_for_test_env.get_ticker("ETH")
    
    assert ticker is not None, "Ticker should not be None for ETH"
    assert isinstance(ticker, Ticker), f"Expected Ticker, got {type(ticker)}"
    assert ticker.symbol == "ETH", f"Expected symbol 'ETH', got '{ticker.symbol}'"
    assert isinstance(ticker.price, Decimal), f"Price should be Decimal, got {type(ticker.price)}"
    assert ticker.price > Decimal("0"), f"Price should be positive, got {ticker.price}"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_order_book_integration(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_order_book() method returns valid OrderBook internal model.
    
    This validates the complete pipeline:
    - API method call (get_order_book)
    - Request building (l2Book endpoint)
    - Response handling and validation
    - Mapping to internal OrderBook model
    """
    order_book = await hl_api_for_test_env.get_order_book("BTC")
    
    # Validate internal model structure
    assert order_book is not None, "OrderBook should not be None for BTC"
    assert isinstance(order_book, OrderBook), f"Expected OrderBook, got {type(order_book)}"
    
    # Validate core order book fields
    assert order_book.symbol == "BTC", f"Expected symbol 'BTC', got '{order_book.symbol}'"
    
    # Validate bids structure
    assert isinstance(order_book.bids, list), f"Bids should be list, got {type(order_book.bids)}"
    assert len(order_book.bids) > 0, "Should have at least one bid for liquid BTC market"
    
    # Validate first bid structure
    first_bid = order_book.bids[0]
    assert isinstance(first_bid, tuple), f"Bid should be tuple, got {type(first_bid)}"
    assert len(first_bid) == 2, f"Bid tuple should have 2 elements, got {len(first_bid)}"
    
    bid_price, bid_size = first_bid
    assert isinstance(bid_price, Decimal), f"Bid price should be Decimal, got {type(bid_price)}"
    assert isinstance(bid_size, Decimal), f"Bid size should be Decimal, got {type(bid_size)}"
    assert bid_price > Decimal("0"), f"Bid price should be positive, got {bid_price}"
    assert bid_size > Decimal("0"), f"Bid size should be positive, got {bid_size}"
    
    # Validate asks structure
    assert isinstance(order_book.asks, list), f"Asks should be list, got {type(order_book.asks)}"
    assert len(order_book.asks) > 0, "Should have at least one ask for liquid BTC market"
    
    # Validate first ask structure
    first_ask = order_book.asks[0]
    assert isinstance(first_ask, tuple), f"Ask should be tuple, got {type(first_ask)}"
    assert len(first_ask) == 2, f"Ask tuple should have 2 elements, got {len(first_ask)}"
    
    ask_price, ask_size = first_ask
    assert isinstance(ask_price, Decimal), f"Ask price should be Decimal, got {type(ask_price)}"
    assert isinstance(ask_size, Decimal), f"Ask size should be Decimal, got {type(ask_size)}"
    assert ask_price > Decimal("0"), f"Ask price should be positive, got {ask_price}"
    assert ask_size > Decimal("0"), f"Ask size should be positive, got {ask_size}"
    
    # Validate spread (ask should be higher than bid)
    assert ask_price > bid_price, f"Ask price {ask_price} should be higher than bid price {bid_price}"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_recent_trades_integration(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_recent_trades() method returns valid Trade internal models.
    
    This validates the complete pipeline:
    - API method call (get_recent_trades)
    - Request building (recentTrades endpoint)
    - Response handling and validation
    - Mapping to internal Trade models
    """
    trades = await hl_api_for_test_env.get_recent_trades("BTC", limit=10)
    
    # Validate return type
    assert isinstance(trades, list), f"Expected list of trades, got {type(trades)}"
    
    # For liquid market like BTC, we expect some trades (though this depends on timing)
    if len(trades) > 0:
        # Validate each trade is a valid Trade model
        for i, trade in enumerate(trades):
            assert isinstance(trade, Trade), f"Trade {i} should be Trade model, got {type(trade)}"
            
            # Validate core trade fields
            assert hasattr(trade, "symbol"), f"Trade {i} should have symbol attribute"
            assert hasattr(trade, "price"), f"Trade {i} should have price attribute"
            assert hasattr(trade, "size"), f"Trade {i} should have size attribute"
            assert hasattr(trade, "timestamp"), f"Trade {i} should have timestamp attribute"
            
            # Validate data types
            assert isinstance(trade.price, Decimal), f"Trade {i} price should be Decimal, got {type(trade.price)}"
            assert isinstance(trade.size, Decimal), f"Trade {i} size should be Decimal, got {type(trade.size)}"
            
            # Validate positive values
            assert trade.price > Decimal("0"), f"Trade {i} price should be positive, got {trade.price}"
            assert trade.size > Decimal("0"), f"Trade {i} size should be positive, got {trade.size}"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_historical_funding_rates_integration(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_historical_funding_rates() method returns valid FundingRate internal models.
    
    This validates the complete pipeline:
    - API method call (get_historical_funding_rates)
    - Request building (fundingHistory endpoint)
    - Response handling and validation
    - Mapping to internal FundingRate models
    """
    from cyberdelta.apis.models.service_args_models import GetHistoricalFundingRatesArgs
    
    # Use fixed timestamps for VCR consistency
    end_time = 1640995200  # Fixed timestamp
    start_time = end_time - 86400  # 24 hours earlier
    
    args = GetHistoricalFundingRatesArgs(
        symbol="BTC",
        start_time=start_time,
        end_time=end_time,
    )
    
    funding_rates = await hl_api_for_test_env.get_historical_funding_rates(args)
    
    # Validate return type
    assert isinstance(funding_rates, list), f"Expected list of funding rates, got {type(funding_rates)}"
    
    # Validate each funding rate (if any exist in the time range)
    if len(funding_rates) > 0:
        for i, funding_rate in enumerate(funding_rates):
            assert isinstance(funding_rate, FundingRate), (
                f"Funding rate {i} should be FundingRate model, got {type(funding_rate)}"
            )
            
            # Validate core funding rate fields
            assert hasattr(funding_rate, "symbol"), f"Funding rate {i} should have symbol attribute"
            assert hasattr(funding_rate, "rate"), f"Funding rate {i} should have rate attribute"
            assert hasattr(funding_rate, "timestamp"), f"Funding rate {i} should have timestamp attribute"
            
            # Validate data types
            assert isinstance(funding_rate.rate, Decimal), (
                f"Funding rate {i} rate should be Decimal, got {type(funding_rate.rate)}"
            )
            
            # Validate symbol
            assert funding_rate.symbol == "BTC", (
                f"Funding rate {i} symbol should be 'BTC', got '{funding_rate.symbol}'"
            )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_market_data_integration(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_market_data() method returns valid Candle internal models.
    
    This validates the complete pipeline:
    - API method call (get_market_data)
    - Request building (candleSnapshot endpoint)
    - Response handling and validation
    - Mapping to internal Candle models
    """
    from cyberdelta.apis.models.service_args_models import GetMarketDataArgs
    
    # Use fixed timestamps for VCR consistency
    end_time = 1640995200  # Fixed timestamp
    start_time = end_time - 3600  # 1 hour earlier
    
    args = GetMarketDataArgs(
        symbol="BTC",
        interval="1h",
        start_time=start_time,
        end_time=end_time,
    )
    
    candles = await hl_api_for_test_env.get_market_data(args)
    
    # Validate return type
    assert isinstance(candles, list), f"Expected list of candles, got {type(candles)}"
    
    # Validate each candle (if any exist in the time range)
    if len(candles) > 0:
        for i, candle in enumerate(candles):
            assert isinstance(candle, Candle), f"Candle {i} should be Candle model, got {type(candle)}"
            
            # Validate core candle fields
            assert hasattr(candle, "symbol"), f"Candle {i} should have symbol attribute"
            assert hasattr(candle, "open"), f"Candle {i} should have open attribute"
            assert hasattr(candle, "high"), f"Candle {i} should have high attribute"
            assert hasattr(candle, "low"), f"Candle {i} should have low attribute"
            assert hasattr(candle, "close"), f"Candle {i} should have close attribute"
            assert hasattr(candle, "volume"), f"Candle {i} should have volume attribute"
            assert hasattr(candle, "open_time"), f"Candle {i} should have open_time attribute"
            
            # Validate data types
            assert isinstance(candle.open, Decimal), (
                f"Candle {i} open should be Decimal, got {type(candle.open)}"
            )
            assert isinstance(candle.high, Decimal), (
                f"Candle {i} high should be Decimal, got {type(candle.high)}"
            )
            assert isinstance(candle.low, Decimal), (
                f"Candle {i} low should be Decimal, got {type(candle.low)}"
            )
            assert isinstance(candle.close, Decimal), (
                f"Candle {i} close should be Decimal, got {type(candle.close)}"
            )
            assert isinstance(candle.volume, Decimal), (
                f"Candle {i} volume should be Decimal, got {type(candle.volume)}"
            )
            
            # Validate positive values
            assert candle.open > Decimal("0"), (
                f"Candle {i} open should be positive, got {candle.open}"
            )
            assert candle.high > Decimal("0"), (
                f"Candle {i} high should be positive, got {candle.high}"
            )
            assert candle.low > Decimal("0"), (
                f"Candle {i} low should be positive, got {candle.low}"
            )
            assert candle.close > Decimal("0"), (
                f"Candle {i} close should be positive, got {candle.close}"
            )
            assert candle.volume >= Decimal("0"), (
                f"Candle {i} volume should be non-negative, got {candle.volume}"
            )
            
            # Validate OHLC relationships
            assert candle.high >= candle.open, (
                f"Candle {i} high {candle.high} should be >= open {candle.open}"
            )
            assert candle.high >= candle.close, (
                f"Candle {i} high {candle.high} should be >= close {candle.close}"
            )
            assert candle.low <= candle.open, (
                f"Candle {i} low {candle.low} should be <= open {candle.open}"
            )
            assert candle.low <= candle.close, (
                f"Candle {i} low {candle.low} should be <= close {candle.close}"
            )
            
            # Validate symbol
            assert candle.symbol == "BTC", f"Candle {i} symbol should be 'BTC', got '{candle.symbol}'"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_ticker_nonexistent_symbol(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_ticker() with non-existent symbol returns None."""
    ticker = await hl_api_for_test_env.get_ticker("NONEXISTENT")
    
    # Should return None for non-existent symbols
    assert ticker is None, f"Expected None for non-existent symbol, got {ticker}"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/hyperliquid/public"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_hl_get_order_book_nonexistent_symbol(
    hl_api_for_test_env: HyperliquidAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test HyperliquidAPI.get_order_book() with non-existent symbol returns None."""
    order_book = await hl_api_for_test_env.get_order_book("NONEXISTENT")
    
    # Should return None for non-existent symbols
    assert order_book is None, f"Expected None for non-existent symbol, got {order_book}"