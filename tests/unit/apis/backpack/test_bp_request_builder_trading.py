"""Unit tests for BackpackRequestBuilder trading methods."""

from typing import Any

import pytest

from cyberdelta.apis.backpack.models.bp_raw_query_params import (
    BackpackRawGetTradeHistoryParams,
)
from cyberdelta.apis.backpack.request_builders.bp_trading_request_builder import (
    BackpackTradingRequestBuilder,
)


class TestBuildGetTradeHistoryParams:
    """Tests for build_get_trade_history_params method (fills)."""

    def test_build_get_trade_history_params_basic(
        self,
        symbol_btc_spot: str,
        current_timestamp_ms: int,
        past_timestamp_ms: int,
    ) -> None:
        """Test build_get_trade_history_params with basic parameters."""
        params = BackpackTradingRequestBuilder.build_get_trade_history_params(
            symbol=symbol_btc_spot,
            limit=25,
            start_time=past_timestamp_ms,
            end_time=None,
            from_id="fillIdStart",
        )
        assert isinstance(params, BackpackRawGetTradeHistoryParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        expected = {
            "symbol": symbol_btc_spot,
            "limit": 25,
            "from": past_timestamp_ms,
            "fromId": "fillIdStart",
        }
        assert params_dict == expected

    def test_build_get_trade_history_params_all_fields(
        self,
        symbol_spot: str,
        current_timestamp_ms: int,
        past_timestamp_ms: int,
    ) -> None:
        """Test build_get_trade_history_params with all fields."""
        params = BackpackTradingRequestBuilder.build_get_trade_history_params(
            symbol=symbol_spot,
            limit=50,
            start_time=past_timestamp_ms,
            end_time=current_timestamp_ms,
            from_id="fillId123",
        )
        assert isinstance(params, BackpackRawGetTradeHistoryParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        expected = {
            "symbol": symbol_spot,
            "limit": 50,
            "from": past_timestamp_ms,
            "to": current_timestamp_ms,
            "fromId": "fillId123",
        }
        assert params_dict == expected

    def test_build_get_trade_history_params_minimal(self, symbol_eth_spot: str) -> None:
        """Test build_get_trade_history_params with minimal parameters."""
        params = BackpackTradingRequestBuilder.build_get_trade_history_params(
            symbol=symbol_eth_spot
        )
        assert isinstance(params, BackpackRawGetTradeHistoryParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        expected = {"symbol": symbol_eth_spot}
        assert params_dict == expected

    def test_build_get_trade_history_params_formats_symbol(self) -> None:
        """Test build_get_trade_history_params formats symbol correctly."""
        params = BackpackTradingRequestBuilder.build_get_trade_history_params(
            symbol="BTC-USDT", limit=100
        )
        assert isinstance(params, BackpackRawGetTradeHistoryParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        expected = {"symbol": "BTC_USDT", "limit": 100}
        assert params_dict == expected

    def test_build_get_trade_history_params_only_limit(self, symbol_spot: str) -> None:
        """Test build_get_trade_history_params with only limit."""
        params = BackpackTradingRequestBuilder.build_get_trade_history_params(
            symbol=symbol_spot, limit=75
        )
        assert isinstance(params, BackpackRawGetTradeHistoryParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        expected = {"symbol": symbol_spot, "limit": 75}
        assert params_dict == expected

    def test_build_get_trade_history_params_only_from_id(self, symbol_spot: str) -> None:
        """Test build_get_trade_history_params with only from_id."""
        params = BackpackTradingRequestBuilder.build_get_trade_history_params(
            symbol=symbol_spot, from_id="startFillId"
        )
        assert isinstance(params, BackpackRawGetTradeHistoryParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        expected = {"symbol": symbol_spot, "fromId": "startFillId"}
        assert params_dict == expected

    def test_build_get_trade_history_params_time_range_only(
        self,
        symbol_spot: str,
        current_timestamp_ms: int,
        past_timestamp_ms: int,
    ) -> None:
        """Test build_get_trade_history_params with time range only."""
        params = BackpackTradingRequestBuilder.build_get_trade_history_params(
            symbol=symbol_spot, start_time=past_timestamp_ms, end_time=current_timestamp_ms
        )
        assert isinstance(params, BackpackRawGetTradeHistoryParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        expected = {
            "symbol": symbol_spot,
            "from": past_timestamp_ms,
            "to": current_timestamp_ms,
        }
        assert params_dict == expected

    @pytest.mark.parametrize(
        ("symbol", "limit", "from_id", "expected_base"),
        [
            ("SOL_USDC", None, None, {"symbol": "SOL_USDC"}),
            ("BTC_USDT", 50, None, {"symbol": "BTC_USDT", "limit": 50}),
            ("eth-perp", None, "fillId", {"symbol": "ETH_PERP", "fromId": "fillId"}),
            ("sol-usdc", 25, "id123", {"symbol": "SOL_USDC", "limit": 25, "fromId": "id123"}),
        ],
    )
    def test_build_get_trade_history_params_parametrized(
        self,
        symbol: str,
        limit: int | None,
        from_id: str | None,
        expected_base: dict[str, Any],
    ) -> None:
        """Test build_get_trade_history_params with various combinations."""
        if limit is None:
            params = BackpackTradingRequestBuilder.build_get_trade_history_params(
                symbol=symbol,
                from_id=from_id,
            )
        else:
            params = BackpackTradingRequestBuilder.build_get_trade_history_params(
                symbol=symbol,
                limit=limit,
                from_id=from_id,
            )
        assert isinstance(params, BackpackRawGetTradeHistoryParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        assert params_dict == expected_base

    @pytest.mark.parametrize(
        ("start_time", "end_time", "expected_time_params"),
        [
            (None, None, {}),
            (1000000, None, {"from": 1000000}),
            (None, 2000000, {"to": 2000000}),
            (1000000, 2000000, {"from": 1000000, "to": 2000000}),
        ],
    )
    def test_build_get_trade_history_params_time_combinations(
        self,
        symbol_spot: str,
        start_time: int | None,
        end_time: int | None,
        expected_time_params: dict[str, Any],
    ) -> None:
        """Test build_get_trade_history_params with various time combinations."""
        params = BackpackTradingRequestBuilder.build_get_trade_history_params(
            symbol=symbol_spot, start_time=start_time, end_time=end_time
        )
        assert isinstance(params, BackpackRawGetTradeHistoryParams)
        params_dict = params.model_dump(by_alias=True, exclude_none=True)
        expected: dict[str, Any] = {"symbol": symbol_spot}
        expected.update(expected_time_params)
        assert params_dict == expected
