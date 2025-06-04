"""Unit tests for BackpackRequestBuilder market data methods."""

from typing import Any, Literal

import pytest

from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder
from cyberdelta.apis.backpack.models.bp_raw_query_params import (
    BackpackRawGetHistoricalTradesParams,
    BackpackRawGetMarketDataParams,
    BackpackRawGetOrderBookParams,
    BackpackRawGetRecentTradesParams,
    BackpackRawGetTickerParams,
)


class TestBuildGetTickerParams:
    """Tests for build_get_ticker_params method."""

    def test_build_get_ticker_params_spot_symbol(self, symbol_spot: str) -> None:
        """Test build_get_ticker_params with spot symbol."""
        params = BackpackRequestBuilder.build_get_ticker_params(symbol_spot)
        assert isinstance(params, BackpackRawGetTickerParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == {"symbol": symbol_spot}

    def test_build_get_ticker_params_perp_symbol(self, symbol_perp: str) -> None:
        """Test build_get_ticker_params with perp symbol."""
        params = BackpackRequestBuilder.build_get_ticker_params(symbol_perp)
        assert isinstance(params, BackpackRawGetTickerParams)
        expected_symbol = symbol_perp.replace("-", "_").upper()
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == {"symbol": expected_symbol}

    def test_build_get_ticker_params_formats_symbol(self) -> None:
        """Test build_get_ticker_params formats symbol correctly."""
        params = BackpackRequestBuilder.build_get_ticker_params("SOL-USDC")
        assert isinstance(params, BackpackRawGetTickerParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == {"symbol": "SOL_USDC"}

    @pytest.mark.parametrize(
        "input_symbol, expected_symbol",
        [
            ("sol-usdc", "SOL_USDC"),
            ("BTC-PERP", "BTC_PERP"),
            ("eth_usdt", "ETH_USDT"),
        ],
    )
    def test_build_get_ticker_params_parametrized(
        self,
        input_symbol: str,
        expected_symbol: str,
    ) -> None:
        """Test build_get_ticker_params with various symbol formats."""
        params = BackpackRequestBuilder.build_get_ticker_params(input_symbol)
        assert isinstance(params, BackpackRawGetTickerParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == {"symbol": expected_symbol}


class TestBuildGetOrderBookParams:
    """Tests for build_get_order_book_params method."""

    def test_build_get_order_book_params_no_limit(self, symbol_btc_spot: str) -> None:
        """Test build_get_order_book_params without limit."""
        params = BackpackRequestBuilder.build_get_order_book_params(symbol_btc_spot, None)
        assert isinstance(params, BackpackRawGetOrderBookParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == {"symbol": symbol_btc_spot}

    def test_build_get_order_book_params_with_limit(self, symbol_btc_spot: str) -> None:
        """Test build_get_order_book_params with limit."""
        params = BackpackRequestBuilder.build_get_order_book_params(symbol_btc_spot, 10)
        assert isinstance(params, BackpackRawGetOrderBookParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == {"symbol": symbol_btc_spot, "limit": 10}

    def test_build_get_order_book_params_formats_symbol(self) -> None:
        """Test build_get_order_book_params formats symbol correctly."""
        params = BackpackRequestBuilder.build_get_order_book_params("BTC-USDT", 20)
        assert isinstance(params, BackpackRawGetOrderBookParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == {"symbol": "BTC_USDT", "limit": 20}

    @pytest.mark.parametrize(
        "limit, expected_params",
        [
            (None, {"symbol": "SOL_USDC"}),
            (5, {"symbol": "SOL_USDC", "limit": 5}),
            (100, {"symbol": "SOL_USDC", "limit": 100}),
            (1000, {"symbol": "SOL_USDC", "limit": 1000}),
        ],
    )
    def test_build_get_order_book_params_parametrized(
        self,
        limit: int | None,
        expected_params: dict[str, Any],
    ) -> None:
        """Test build_get_order_book_params with various limits."""
        params = BackpackRequestBuilder.build_get_order_book_params("SOL_USDC", limit)
        assert isinstance(params, BackpackRawGetOrderBookParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == expected_params


class TestBuildGetRecentTradesParams:
    """Tests for build_get_recent_trades_params method."""

    def test_build_get_recent_trades_params_no_limit(self, symbol_eth_spot: str) -> None:
        """Test build_get_recent_trades_params without limit."""
        params = BackpackRequestBuilder.build_get_recent_trades_params(symbol_eth_spot, None)
        assert isinstance(params, BackpackRawGetRecentTradesParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == {"symbol": symbol_eth_spot}

    def test_build_get_recent_trades_params_with_limit(self, symbol_eth_spot: str) -> None:
        """Test build_get_recent_trades_params with limit."""
        params = BackpackRequestBuilder.build_get_recent_trades_params(symbol_eth_spot, 50)
        assert isinstance(params, BackpackRawGetRecentTradesParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == {"symbol": symbol_eth_spot, "limit": 50}

    def test_build_get_recent_trades_params_formats_symbol(self) -> None:
        """Test build_get_recent_trades_params formats symbol correctly."""
        params = BackpackRequestBuilder.build_get_recent_trades_params("ETH-USDC", 25)
        assert isinstance(params, BackpackRawGetRecentTradesParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == {"symbol": "ETH_USDC", "limit": 25}

    @pytest.mark.parametrize(
        "symbol, limit, expected_params",
        [
            ("SOL_USDC", None, {"symbol": "SOL_USDC"}),
            ("BTC_USDT", 10, {"symbol": "BTC_USDT", "limit": 10}),
            ("eth-perp", 100, {"symbol": "ETH_PERP", "limit": 100}),
        ],
    )
    def test_build_get_recent_trades_params_parametrized(
        self,
        symbol: str,
        limit: int | None,
        expected_params: dict[str, Any],
    ) -> None:
        """Test build_get_recent_trades_params with various combinations."""
        params = BackpackRequestBuilder.build_get_recent_trades_params(symbol, limit)
        assert isinstance(params, BackpackRawGetRecentTradesParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == expected_params


class TestBuildGetMarketDataParams:
    """Tests for build_get_market_data_params method (klines)."""

    def test_build_get_market_data_params_basic(self, symbol_spot: str) -> None:
        """Test build_get_market_data_params with basic parameters."""
        params = BackpackRequestBuilder.build_get_market_data_params(
            symbol_spot,
            "1h",
            None,
            None,
            100,
        )
        assert isinstance(params, BackpackRawGetMarketDataParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        expected = {"symbol": symbol_spot, "interval": "1h", "limit": 100}
        assert params_dict == expected

    def test_build_get_market_data_params_with_times(
        self,
        symbol_spot: str,
        current_timestamp_ms: int,
        past_timestamp_ms: int,
    ) -> None:
        """Test build_get_market_data_params with start and end times."""
        params = BackpackRequestBuilder.build_get_market_data_params(
            symbol_spot,
            "5m",
            past_timestamp_ms,
            current_timestamp_ms,
            50,
        )
        assert isinstance(params, BackpackRawGetMarketDataParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        expected = {
            "symbol": symbol_spot,
            "interval": "5m",
            "limit": 50,
            "startTime": past_timestamp_ms,
            "endTime": current_timestamp_ms,
        }
        assert params_dict == expected

    def test_build_get_market_data_params_formats_symbol(self) -> None:
        """Test build_get_market_data_params formats symbol correctly."""
        params = BackpackRequestBuilder.build_get_market_data_params(
            "SOL-USDC",
            "1m",
            None,
            None,
            200,
        )
        assert isinstance(params, BackpackRawGetMarketDataParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        expected = {"symbol": "SOL_USDC", "interval": "1m", "limit": 200}
        assert params_dict == expected

    @pytest.mark.parametrize(
        "timeframe, limit, expected_interval",
        [
            ("1m", 100, "1m"),
            ("5m", 200, "5m"),
            ("1h", 24, "1h"),
            ("1d", 30, "1d"),
        ],
    )
    def test_build_get_market_data_params_timeframes(
        self,
        symbol_spot: str,
        timeframe: Literal["1m", "5m", "1h", "1d"],
        limit: int,
        expected_interval: str,
    ) -> None:
        """Test build_get_market_data_params with various timeframes."""
        params = BackpackRequestBuilder.build_get_market_data_params(
            symbol_spot,
            timeframe,
            None,
            None,
            limit,
        )
        assert isinstance(params, BackpackRawGetMarketDataParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        expected = {"symbol": symbol_spot, "interval": expected_interval, "limit": limit}
        assert params_dict == expected


class TestBuildGetHistoricalTradesParams:
    """Tests for build_get_historical_trades_params method."""

    def test_build_get_historical_trades_params_basic(self, symbol_eth_spot: str) -> None:
        """Test build_get_historical_trades_params with basic parameters."""
        params = BackpackRequestBuilder.build_get_historical_trades_params(
            symbol_eth_spot,
            50,
            None,
        )
        assert isinstance(params, BackpackRawGetHistoricalTradesParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        expected = {"symbol": symbol_eth_spot, "limit": 50}
        assert params_dict == expected

    def test_build_get_historical_trades_params_with_from_id(self, symbol_perp: str) -> None:
        """Test build_get_historical_trades_params with from_id."""
        params = BackpackRequestBuilder.build_get_historical_trades_params(
            symbol_perp,
            50,
            "trade123",
        )
        assert isinstance(params, BackpackRawGetHistoricalTradesParams)
        expected_symbol = symbol_perp.replace("-", "_").upper()
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        expected = {"symbol": expected_symbol, "limit": 50, "fromId": "trade123"}
        assert params_dict == expected

    def test_build_get_historical_trades_params_formats_symbol(self) -> None:
        """Test build_get_historical_trades_params formats symbol correctly."""
        params = BackpackRequestBuilder.build_get_historical_trades_params(
            "ETH-PERP",
            100,
            "trade456",
        )
        assert isinstance(params, BackpackRawGetHistoricalTradesParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        expected = {"symbol": "ETH_PERP", "limit": 100, "fromId": "trade456"}
        assert params_dict == expected

    @pytest.mark.parametrize(
        "symbol, limit, from_id, expected_params",
        [
            ("SOL_USDC", 25, None, {"symbol": "SOL_USDC", "limit": 25}),
            ("BTC_USDT", 50, "id123", {"symbol": "BTC_USDT", "limit": 50, "fromId": "id123"}),
            ("eth-perp", 100, "id456", {"symbol": "ETH_PERP", "limit": 100, "fromId": "id456"}),
        ],
    )
    def test_build_get_historical_trades_params_parametrized(
        self,
        symbol: str,
        limit: int,
        from_id: str | None,
        expected_params: dict[str, Any],
    ) -> None:
        """Test build_get_historical_trades_params with various combinations."""
        params = BackpackRequestBuilder.build_get_historical_trades_params(symbol, limit, from_id)
        assert isinstance(params, BackpackRawGetHistoricalTradesParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == expected_params
