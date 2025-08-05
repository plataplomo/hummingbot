"""Unit tests for BackpackMarketDataRequestBuilder market data methods."""

from typing import Any, Literal

import pytest

from cyberdelta.apis.backpack.models.bp_raw_query_params import (
    BackpackRawGetHistoricalTradesParams,
    BackpackRawGetMarketDataParams,
    BackpackRawGetMarketParams,
    BackpackRawGetMarketsParams,
    BackpackRawGetOrderBookParams,
    BackpackRawGetRecentTradesParams,
    BackpackRawGetTickerParams,
)
from cyberdelta.apis.backpack.request_builders.bp_market_data_request_builder import (
    BackpackMarketDataRequestBuilder,
)
from cyberdelta.core.symbols import exchanges
from cyberdelta.core.symbols.models import Symbol
from tests.common_symbols import (
    BTC_BP,
    BTC_USDC_BP,
    BTC_USDT_BP,
    ETH_BP,
    ETH_USDC_BP,
    ETH_USDT_BP,
    SOL_USDC_BP,
)


class TestBuildGetTickerParams:
    """Tests for build_get_ticker_params method."""

    def test_build_get_ticker_params_spot_symbol(self, symbol_spot: Symbol) -> None:
        """Test build_get_ticker_params with spot symbol."""
        params = BackpackMarketDataRequestBuilder.build_get_ticker_params(symbol_spot)
        assert isinstance(params, BackpackRawGetTickerParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == {"symbol": symbol_spot}

    def test_build_get_ticker_params_perp_symbol(self, symbol_perp: Symbol) -> None:
        """Test build_get_ticker_params with perp symbol."""
        params = BackpackMarketDataRequestBuilder.build_get_ticker_params(symbol_perp)
        assert isinstance(params, BackpackRawGetTickerParams)
        expected_symbol = symbol_perp.value.replace("-", "_").upper()
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == {"symbol": expected_symbol}

    def test_build_get_ticker_params_formats_symbol(self) -> None:
        """Test build_get_ticker_params formats symbol correctly."""
        params = BackpackMarketDataRequestBuilder.build_get_ticker_params(SOL_USDC_BP)
        assert isinstance(params, BackpackRawGetTickerParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == {"symbol": SOL_USDC_BP.value}

    @pytest.mark.parametrize(
        ("input_symbol", "expected_symbol"),
        [
            (SOL_USDC_BP, SOL_USDC_BP.value),
            (BTC_BP, BTC_BP.value.replace("-", "_")),
            (ETH_USDT_BP, ETH_USDT_BP.value),
        ],
    )
    def test_build_get_ticker_params_parametrized(
        self,
        input_symbol: Symbol,
        expected_symbol: str,
    ) -> None:
        """Test build_get_ticker_params with various symbol formats."""
        params = BackpackMarketDataRequestBuilder.build_get_ticker_params(input_symbol)
        assert isinstance(params, BackpackRawGetTickerParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == {"symbol": expected_symbol}


class TestBuildGetOrderBookParams:
    """Tests for build_get_order_book_params method."""

    def test_build_get_order_book_params_no_limit(self, symbol_btc_spot: Symbol) -> None:
        """Test build_get_order_book_params without limit."""
        params = BackpackMarketDataRequestBuilder.build_get_order_book_params(symbol_btc_spot, None)
        assert isinstance(params, BackpackRawGetOrderBookParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == {"symbol": symbol_btc_spot}

    def test_build_get_order_book_params_with_limit(self, symbol_btc_spot: Symbol) -> None:
        """Test build_get_order_book_params with limit."""
        params = BackpackMarketDataRequestBuilder.build_get_order_book_params(symbol_btc_spot, 10)
        assert isinstance(params, BackpackRawGetOrderBookParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == {"symbol": symbol_btc_spot, "limit": 10}

    def test_build_get_order_book_params_formats_symbol(self) -> None:
        """Test build_get_order_book_params formats symbol correctly."""
        params = BackpackMarketDataRequestBuilder.build_get_order_book_params(BTC_USDT_BP, 20)
        assert isinstance(params, BackpackRawGetOrderBookParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == {"symbol": BTC_USDT_BP.value, "limit": 20}

    @pytest.mark.parametrize(
        ("limit", "expected_params"),
        [
            (None, {"symbol": SOL_USDC_BP.value}),
            (5, {"symbol": SOL_USDC_BP.value, "limit": 5}),
            (100, {"symbol": SOL_USDC_BP.value, "limit": 100}),
            (1000, {"symbol": SOL_USDC_BP.value, "limit": 1000}),
        ],
    )
    def test_build_get_order_book_params_parametrized(
        self,
        limit: int | None,
        expected_params: dict[str, Any],
    ) -> None:
        """Test build_get_order_book_params with various limits."""
        params = BackpackMarketDataRequestBuilder.build_get_order_book_params(SOL_USDC_BP, limit)
        assert isinstance(params, BackpackRawGetOrderBookParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == expected_params


class TestBuildGetRecentTradesParams:
    """Tests for build_get_recent_trades_params method."""

    def test_build_get_recent_trades_params_no_limit(self, symbol_eth_spot: Symbol) -> None:
        """Test build_get_recent_trades_params without limit."""
        params = BackpackMarketDataRequestBuilder.build_get_recent_trades_params(
            symbol_eth_spot, None
        )
        assert isinstance(params, BackpackRawGetRecentTradesParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == {"symbol": symbol_eth_spot}

    def test_build_get_recent_trades_params_with_limit(self, symbol_eth_spot: Symbol) -> None:
        """Test build_get_recent_trades_params with limit."""
        params = BackpackMarketDataRequestBuilder.build_get_recent_trades_params(
            symbol_eth_spot, 50
        )
        assert isinstance(params, BackpackRawGetRecentTradesParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == {"symbol": symbol_eth_spot, "limit": 50}

    def test_build_get_recent_trades_params_formats_symbol(self) -> None:
        """Test build_get_recent_trades_params formats symbol correctly."""
        params = BackpackMarketDataRequestBuilder.build_get_recent_trades_params(ETH_USDC_BP, 25)
        assert isinstance(params, BackpackRawGetRecentTradesParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == {"symbol": ETH_USDC_BP.value, "limit": 25}

    @pytest.mark.parametrize(
        ("symbol", "limit", "expected_params"),
        [
            (SOL_USDC_BP, None, {"symbol": SOL_USDC_BP.value}),
            (BTC_USDT_BP, 10, {"symbol": BTC_USDT_BP.value, "limit": 10}),
            (ETH_BP, 100, {"symbol": ETH_BP.value.replace("-", "_"), "limit": 100}),
        ],
    )
    def test_build_get_recent_trades_params_parametrized(
        self,
        symbol: Symbol,
        limit: int | None,
        expected_params: dict[str, Any],
    ) -> None:
        """Test build_get_recent_trades_params with various combinations."""
        params = BackpackMarketDataRequestBuilder.build_get_recent_trades_params(symbol, limit)
        assert isinstance(params, BackpackRawGetRecentTradesParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == expected_params


class TestBuildGetMarketDataParams:
    """Tests for build_get_market_data_params method (klines)."""

    def test_build_get_market_data_params_basic(self, symbol_spot: Symbol) -> None:
        """Test build_get_market_data_params with basic parameters."""
        params = BackpackMarketDataRequestBuilder.build_get_market_data_params(
            symbol_spot,
            "1h",
            1609459200,  # Sample timestamp
            None,
            100,
        )
        assert isinstance(params, BackpackRawGetMarketDataParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        expected = {"symbol": symbol_spot, "interval": "1h", "startTime": 1609459, "limit": 100}
        assert params_dict == expected

    def test_build_get_market_data_params_with_times(
        self,
        symbol_spot: Symbol,
        current_timestamp_ms: int,
        past_timestamp_ms: int,
    ) -> None:
        """Test build_get_market_data_params with start and end times."""
        params = BackpackMarketDataRequestBuilder.build_get_market_data_params(
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
            "startTime": past_timestamp_ms // 1000,  # Convert to seconds
            "endTime": current_timestamp_ms // 1000,  # Convert to seconds
        }
        assert params_dict == expected

    def test_build_get_market_data_params_formats_symbol(self) -> None:
        """Test build_get_market_data_params formats symbol correctly."""
        params = BackpackMarketDataRequestBuilder.build_get_market_data_params(
            SOL_USDC_BP,
            "1m",
            1609459200,  # Sample timestamp
            None,
            200,
        )
        assert isinstance(params, BackpackRawGetMarketDataParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        expected = {
            "symbol": SOL_USDC_BP.value,
            "interval": "1m",
            "startTime": 1609459,
            "limit": 200,
        }
        assert params_dict == expected

    @pytest.mark.parametrize(
        ("timeframe", "limit", "expected_interval"),
        [
            ("1m", 100, "1m"),
            ("5m", 200, "5m"),
            ("1h", 24, "1h"),
            ("1d", 30, "1d"),
        ],
    )
    def test_build_get_market_data_params_timeframes(
        self,
        symbol_spot: Symbol,
        timeframe: Literal["1m", "5m", "1h", "1d"],
        limit: int,
        expected_interval: str,
    ) -> None:
        """Test build_get_market_data_params with various timeframes."""
        params = BackpackMarketDataRequestBuilder.build_get_market_data_params(
            symbol_spot,
            timeframe,
            1609459200,  # Valid timestamp
            None,
            limit,
        )
        assert isinstance(params, BackpackRawGetMarketDataParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        expected = {
            "symbol": symbol_spot,
            "interval": expected_interval,
            "startTime": 1609459,
            "limit": limit,
        }
        assert params_dict == expected


class TestBuildGetHistoricalTradesParams:
    """Tests for build_get_historical_trades_params method."""

    def test_build_get_historical_trades_params_basic(self, symbol_eth_spot: Symbol) -> None:
        """Test build_get_historical_trades_params with basic parameters."""
        params = BackpackMarketDataRequestBuilder.build_get_historical_trades_params(
            symbol_eth_spot,
            50,
            None,
        )
        assert isinstance(params, BackpackRawGetHistoricalTradesParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        expected = {"symbol": symbol_eth_spot, "limit": 50}
        assert params_dict == expected

    def test_build_get_historical_trades_params_with_from_id(self, symbol_perp: Symbol) -> None:
        """Test build_get_historical_trades_params with from_id."""
        params = BackpackMarketDataRequestBuilder.build_get_historical_trades_params(
            symbol_perp,
            50,
            "trade123",
        )
        assert isinstance(params, BackpackRawGetHistoricalTradesParams)
        expected_symbol = symbol_perp.value.replace("-", "_").upper()
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        expected = {"symbol": expected_symbol, "limit": 50, "fromId": "trade123"}
        assert params_dict == expected

    def test_build_get_historical_trades_params_formats_symbol(self) -> None:
        """Test build_get_historical_trades_params formats symbol correctly."""
        params = BackpackMarketDataRequestBuilder.build_get_historical_trades_params(
            ETH_BP,
            100,
            "trade456",
        )
        assert isinstance(params, BackpackRawGetHistoricalTradesParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        expected = {"symbol": ETH_BP.value.replace("-", "_"), "limit": 100, "fromId": "trade456"}
        assert params_dict == expected

    @pytest.mark.parametrize(
        ("symbol", "limit", "from_id", "expected_params"),
        [
            (SOL_USDC_BP, 25, None, {"symbol": SOL_USDC_BP.value, "limit": 25}),
            (
                BTC_USDT_BP,
                50,
                "id123",
                {"symbol": BTC_USDT_BP.value, "limit": 50, "fromId": "id123"},
            ),
            (
                ETH_BP,
                100,
                "id456",
                {"symbol": ETH_BP.value.replace("-", "_"), "limit": 100, "fromId": "id456"},
            ),
        ],
    )
    def test_build_get_historical_trades_params_parametrized(
        self,
        symbol: Symbol,
        limit: int,
        from_id: str | None,
        expected_params: dict[str, Any],
    ) -> None:
        """Test build_get_historical_trades_params with various combinations."""
        params = BackpackMarketDataRequestBuilder.build_get_historical_trades_params(
            symbol, limit, from_id
        )
        assert isinstance(params, BackpackRawGetHistoricalTradesParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == expected_params


class TestBuildGetMarketsParams:
    """Tests for build_get_markets_params method."""

    def test_build_get_markets_params_basic(self) -> None:
        """Test build_get_markets_params returns valid model."""
        params = BackpackMarketDataRequestBuilder.build_get_markets_params()
        assert isinstance(params, BackpackRawGetMarketsParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        # This endpoint requires no query parameters
        assert params_dict == {}

    def test_build_get_markets_params_empty_params(self) -> None:
        """Test build_get_markets_params returns empty params dict."""
        params = BackpackMarketDataRequestBuilder.build_get_markets_params()
        assert isinstance(params, BackpackRawGetMarketsParams)
        # Should be a valid model but with no required fields
        assert params is not None


class TestBuildGetMarketParams:
    """Tests for build_get_market_params method."""

    def test_build_get_market_params_basic(self, symbol_spot: Symbol) -> None:
        """Test build_get_market_params with spot symbol."""
        params = BackpackMarketDataRequestBuilder.build_get_market_params(symbol_spot)
        assert isinstance(params, BackpackRawGetMarketParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == {"symbol": symbol_spot}

    def test_build_get_market_params_perp_symbol(self, symbol_perp: Symbol) -> None:
        """Test build_get_market_params with perp symbol."""
        params = BackpackMarketDataRequestBuilder.build_get_market_params(symbol_perp)
        assert isinstance(params, BackpackRawGetMarketParams)
        expected_symbol = symbol_perp.value.replace("-", "_").upper()
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == {"symbol": expected_symbol}

    def test_build_get_market_params_formats_symbol(self) -> None:
        """Test build_get_market_params formats symbol correctly."""
        params = BackpackMarketDataRequestBuilder.build_get_market_params(SOL_USDC_BP)
        assert isinstance(params, BackpackRawGetMarketParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == {"symbol": SOL_USDC_BP.value}

    @pytest.mark.parametrize(
        ("input_symbol", "expected_symbol"),
        [
            (SOL_USDC_BP, SOL_USDC_BP.value),
            (BTC_BP, BTC_BP.value.replace("-", "_")),
            (ETH_USDT_BP, ETH_USDT_BP.value),
            (exchanges.backpack("AVAX_USDC"), "AVAX_USDC"),
            (exchanges.backpack("LINK-PERP"), "LINK_PERP"),
        ],
    )
    def test_build_get_market_params_parametrized(
        self,
        input_symbol: Symbol,
        expected_symbol: str,
    ) -> None:
        """Test build_get_market_params with various symbol formats."""
        params = BackpackMarketDataRequestBuilder.build_get_market_params(input_symbol)
        assert isinstance(params, BackpackRawGetMarketParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == {"symbol": expected_symbol}

    def test_build_get_market_params_symbol_consistency(self) -> None:
        """Test build_get_market_params symbol formatting consistency."""
        # Test that the method consistently handles Symbol objects
        test_symbols = [BTC_USDC_BP, ETH_USDT_BP, SOL_USDC_BP, exchanges.backpack("AVAX-PERP")]
        for symbol in test_symbols:
            params = BackpackMarketDataRequestBuilder.build_get_market_params(symbol)
            params_dict = params.model_dump(by_alias=True, exclude_none=True)
            # The symbol should be formatted correctly by the request builder
            assert "symbol" in params_dict
            assert isinstance(params_dict["symbol"], str)
