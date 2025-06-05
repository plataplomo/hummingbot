"""Unit tests for HyperliquidResponseHandler market data and info response functionality."""

from typing import Any, cast

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.hl_response_handler import (
    HyperliquidResponseHandler,
    RawJsonResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import HyperliquidRawCandleSnapshot
from cyberdelta.apis.hyperliquid.models.hl_raw_funding_history_info import (
    HyperliquidRawFundingHistoryItem,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawMetaAndAssetCtxsResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import HyperliquidRawL2Book
from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import HyperliquidRawPublicTrade
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode

# Import fixtures from the shared conftest
pytest_plugins = ["tests.unit.apis.hyperliquid.conftest_response_handler"]


class TestHandleInfoMetaAndAssetCtxsResponse:
    """Tests for HyperliquidResponseHandler.handle_info_meta_and_asset_ctxs_response."""

    def test_valid(self, valid_raw_meta_and_asset_ctxs: list[Any]) -> None:
        """Test handling a valid meta and asset contexts response."""
        raw_data = valid_raw_meta_and_asset_ctxs
        response: HyperliquidRawMetaAndAssetCtxsResponse = (
            HyperliquidResponseHandler.handle_info_meta_and_asset_ctxs_response(
                cast("RawJsonResponse", raw_data),
            )
        )
        assert isinstance(response, HyperliquidRawMetaAndAssetCtxsResponse)
        assert len(response.meta.universe) == 2  # BTC and ETH
        assert response.meta.universe[0].name == "BTC"
        assert response.meta.universe[1].name == "ETH"
        assert len(response.asset_ctxs) == 2
        assert response.asset_ctxs[0].name == "BTC"
        assert response.asset_ctxs[1].name == "ETH"

    def test_validation_error_wrong_structure(self) -> None:
        """Test meta and asset contexts response with wrong structure."""
        raw_data: list[dict[str, str]] = [{"invalid": "structure"}]  # Missing required fields
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_meta_and_asset_ctxs_response(
                cast("RawJsonResponse", raw_data),
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            "Unexpected info (MetaAndAssetCtxs) response format: expected 2-element list, "
            "got 1 elements" in exc_info.value.message
        )

    def test_validation_error_missing_universe(self) -> None:
        """Test meta and asset contexts response missing universe."""
        raw_data: list[dict[str, str] | list[Any]] = [
            {"name": "ETH-PERP"},
            [],
        ]  # Missing universe field in meta
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_meta_and_asset_ctxs_response(
                cast("RawJsonResponse", raw_data),
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Invalid info (MetaAndAssetCtxs) response from exchange:" in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)

    def test_invalid_top_level_type(self) -> None:
        """Test meta and asset contexts response with wrong top-level type."""
        raw_data = {"invalid": "data"}
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_meta_and_asset_ctxs_response(
                cast("RawJsonResponse", raw_data),
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Unexpected info (MetaAndAssetCtxs) response format:" in exc_info.value.message
        assert "expected list, got dict" in exc_info.value.message


class TestHandleInfoFundingRateResponse:
    """Tests for HyperliquidResponseHandler.handle_info_funding_rate_response."""

    def test_valid(self, valid_raw_asset_ctx: dict[str, Any], symbol: str) -> None:
        """Test handling a valid funding rate response."""
        raw_data = valid_raw_asset_ctx
        response = HyperliquidResponseHandler.handle_info_funding_rate_response(
            cast("RawJsonResponse", raw_data),
            symbol=symbol,
        )
        assert response.name == "ETH-PERP"
        assert response.funding == "0.00015"

    def test_validation_error_missing_funding(self, symbol: str) -> None:
        """Test funding rate response missing funding field."""
        raw_data = {"name": "ETH-PERP", "markPx": "3000.0"}  # Missing funding
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_funding_rate_response(
                cast("RawJsonResponse", raw_data),
                symbol=symbol,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Invalid info (funding rate for {symbol}) response from exchange:"
            in exc_info.value.message
        )
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "funding" in str(exc_info.value.original_exception)

    def test_invalid_top_level_type(self, symbol: str) -> None:
        """Test funding rate response with wrong top-level type."""
        raw_data = ["invalid"]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_funding_rate_response(
                cast("RawJsonResponse", raw_data),
                symbol=symbol,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Unexpected info (funding rate for {symbol}) response format:"
            in exc_info.value.message
        )
        assert "expected dict, got list" in exc_info.value.message


class TestHandleInfoL2BookResponse:
    """Tests for HyperliquidResponseHandler.handle_info_l2_book_response."""

    def test_valid(self, valid_raw_l2_book: dict[str, Any], symbol: str) -> None:
        """Test handling a valid L2 book response."""
        raw_data = valid_raw_l2_book
        response: HyperliquidRawL2Book = HyperliquidResponseHandler.handle_info_l2_book_response(
            cast("RawJsonResponse", raw_data),
            symbol=symbol,
        )
        assert isinstance(response, HyperliquidRawL2Book)
        assert response.coin == "ETH-PERP"
        assert len(response.levels) == 2  # Bids and asks
        assert response.time == 1678889300000

    def test_validation_error_missing_levels(self, symbol: str) -> None:
        """Test L2 book response missing levels field."""
        raw_data = {"coin": "ETH-PERP", "time": 1678889300000}  # Missing levels
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_l2_book_response(
                cast("RawJsonResponse", raw_data),
                symbol=symbol,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Invalid info (l2 book for {symbol}) response from exchange:" in exc_info.value.message
        )
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "levels" in str(exc_info.value.original_exception)

    def test_invalid_top_level_type(self, symbol: str) -> None:
        """Test L2 book response with wrong top-level type."""
        raw_data = "invalid"
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_l2_book_response(
                cast("RawJsonResponse", raw_data),
                symbol=symbol,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert f"Unexpected info (l2 book for {symbol}) response format:" in exc_info.value.message
        assert "expected dict, got str" in exc_info.value.message

    def test_l2_book_response_empty_levels(self, symbol: str) -> None:
        """Test L2 book response with empty levels arrays."""
        raw_data: dict[str, Any] = {
            "coin": "ETH-PERP",
            "levels": [[], []],  # Empty bids and asks
            "time": 1678889300000,
        }
        response = HyperliquidResponseHandler.handle_info_l2_book_response(
            cast("RawJsonResponse", raw_data),
            symbol=symbol,
        )
        assert response.coin == "ETH-PERP"
        assert len(response.levels) == 2
        assert len(response.levels[0]) == 0  # Empty bids
        assert len(response.levels[1]) == 0  # Empty asks


class TestHandleInfoRecentTradesResponse:
    """Tests for HyperliquidResponseHandler.handle_info_recent_trades_response."""

    def test_valid(self, valid_raw_public_trade: dict[str, Any], symbol: str) -> None:
        """Test handling a valid recent trades response."""
        raw_data = [valid_raw_public_trade, valid_raw_public_trade.copy()]
        response_list: list[HyperliquidRawPublicTrade] = (
            HyperliquidResponseHandler.handle_info_recent_trades_response(
                cast("RawJsonResponse", raw_data),
                symbol=symbol,
            )
        )
        assert isinstance(response_list, list)
        assert len(response_list) == 2
        assert isinstance(response_list[0], HyperliquidRawPublicTrade)
        assert response_list[0].coin == "ETH-PERP"
        assert response_list[0].side == "B"

    def test_validation_error_invalid_trade_item(self, symbol: str) -> None:
        """Test recent trades response with invalid trade item."""
        invalid_trade = {"coin": "ETH-PERP"}  # Missing required fields
        raw_data = [invalid_trade]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_recent_trades_response(
                cast("RawJsonResponse", raw_data),
                symbol=symbol,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Invalid single recent trade item (index 0) in info (recent trades for {symbol}) "
            f"response from exchange" in exc_info.value.message
        )
        assert isinstance(exc_info.value.original_exception, ValidationError)

    def test_invalid_item_type_in_list(self, symbol: str) -> None:
        """Test recent trades response with non-dict item in list."""
        raw_data = ["not_a_trade_dict"]
        # Handler should skip invalid items
        response_list = HyperliquidResponseHandler.handle_info_recent_trades_response(
            cast("RawJsonResponse", raw_data),
            symbol=symbol,
        )
        assert isinstance(response_list, list)
        assert len(response_list) == 0  # Invalid item skipped

    def test_invalid_top_level_type(self, symbol: str) -> None:
        """Test recent trades response with wrong top-level type."""
        raw_data = {"invalid": "data"}
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_recent_trades_response(
                cast("RawJsonResponse", raw_data),
                symbol=symbol,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Unexpected info (recent trades for {symbol}) response format:"
            in exc_info.value.message
        )
        assert "expected list, got dict" in exc_info.value.message


class TestHandleInfoCandleSnapshotResponse:
    """Tests for HyperliquidResponseHandler.handle_info_candle_snapshot_response."""

    def test_valid(self, valid_raw_candle_snapshot: dict[str, Any], symbol: str) -> None:
        """Test handling a valid candle snapshot response."""
        raw_data = valid_raw_candle_snapshot
        response: HyperliquidRawCandleSnapshot = (
            HyperliquidResponseHandler.handle_info_candle_snapshot_response(
                cast("RawJsonResponse", raw_data),
                symbol=symbol,
                interval="1m",
            )
        )
        assert isinstance(response, HyperliquidRawCandleSnapshot)
        assert len(response.t) == 2  # Two timestamps
        assert response.s == "ok"

    def test_validation_error_missing_timestamps(self, symbol: str) -> None:
        """Test candle snapshot response missing timestamps."""
        raw_data = {"o": ["1200.0"], "h": ["1250.0"], "s": "ok"}  # Missing 't'
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_candle_snapshot_response(
                cast("RawJsonResponse", raw_data),
                symbol=symbol,
                interval="1m",
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Invalid info (candle snapshot for {symbol}) response from exchange:"
            in exc_info.value.message
        )
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "t" in str(exc_info.value.original_exception)

    def test_invalid_top_level_type(self, symbol: str) -> None:
        """Test candle snapshot response with wrong top-level type."""
        raw_data = ["invalid"]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_candle_snapshot_response(
                cast("RawJsonResponse", raw_data),
                symbol=symbol,
                interval="1m",
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Unexpected info (candle snapshot for {symbol}) response format:"
            in exc_info.value.message
        )
        assert "expected dict, got list" in exc_info.value.message

    def test_candle_snapshot_mismatched_arrays(self, symbol: str) -> None:
        """Test candle snapshot with mismatched array lengths causes APIError."""
        raw_data = {
            "t": [1672531200000, 1672531260000],
            "o": ["1200.0"],  # Only one element vs two timestamps
            "h": ["1250.0", "1205.0"],
            "l": ["1190.0", "1198.0"],
            "c": ["1240.0", "1202.0"],
            "v": ["1000.0", "500.0"],
            "s": "ok",
        }
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_candle_snapshot_response(
                cast("RawJsonResponse", raw_data),
                symbol=symbol,
                interval="1m",
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value


class TestHandleHistoricalFundingRatesResponse:
    """Tests for HyperliquidResponseHandler.handle_historical_funding_rates_response."""

    def test_valid(self, valid_raw_historical_funding_rates_data: list[dict[str, Any]]) -> None:
        """Test handling a valid historical funding rates response."""
        raw_data = valid_raw_historical_funding_rates_data
        response_list: list[HyperliquidRawFundingHistoryItem] = (
            HyperliquidResponseHandler.handle_historical_funding_rates_response(
                cast("RawJsonResponse", raw_data),
            )
        )
        assert isinstance(response_list, list)
        assert len(response_list) == 2
        assert isinstance(response_list[0], HyperliquidRawFundingHistoryItem)
        assert response_list[0].coin == "ETH"
        assert response_list[1].coin == "BTC"

    def test_validation_error_invalid_funding_item(self) -> None:
        """Test historical funding rates response with invalid funding item."""
        invalid_funding = {"coin": "ETH"}  # Missing required fields
        raw_data = [invalid_funding]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_historical_funding_rates_response(
                cast("RawJsonResponse", raw_data),
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            "Invalid single funding history item (index 0) in historical_funding_rates "
            "response from exchange" in exc_info.value.message
        )
        assert isinstance(exc_info.value.original_exception, ValidationError)

    def test_invalid_item_type_in_list(self) -> None:
        """Test historical funding rates response with non-dict item in list."""
        raw_data = ["not_a_funding_dict"]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_historical_funding_rates_response(
                cast("RawJsonResponse", raw_data),
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Expected dict for historical funding rate item" in exc_info.value.message

    def test_invalid_top_level_type(self) -> None:
        """Test historical funding rates response with wrong top-level type."""
        raw_data = {"invalid": "data"}
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_historical_funding_rates_response(
                cast("RawJsonResponse", raw_data),
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Unexpected historical_funding_rates response format:" in exc_info.value.message
        assert "expected list, got dict" in exc_info.value.message

    def test_empty_list_response(self) -> None:
        """Test historical funding rates response with empty list."""
        raw_data: list[Any] = []
        response_list = HyperliquidResponseHandler.handle_historical_funding_rates_response(
            cast("RawJsonResponse", raw_data),
        )
        assert isinstance(response_list, list)
        assert len(response_list) == 0


class TestMarketDataEdgeCases:
    """Tests for additional edge cases in market data response handling."""

    def test_funding_rate_response_extra_fields(self, symbol: str) -> None:
        """Test that funding rate response with extra fields causes ValidationError.

        This test verifies that the model validation properly rejects responses with
        unexpected fields due to the extra='forbid' configuration.
        """
        raw_data = {
            "name": "ETH-PERP",
            "funding": "0.00015",
            "markPx": "3000.0",
            "extraField": "ignored",  # Should cause ValidationError
        }
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_funding_rate_response(
                cast("RawJsonResponse", raw_data),
                symbol=symbol,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert isinstance(exc_info.value.original_exception, ValidationError)

    def test_l2_book_response_empty_levels(self, symbol: str) -> None:
        """Test L2 book response with empty levels arrays."""
        raw_data: dict[str, Any] = {
            "coin": "ETH-PERP",
            "levels": [[], []],  # Empty bids and asks
            "time": 1678889300000,
        }
        response = HyperliquidResponseHandler.handle_info_l2_book_response(
            cast("RawJsonResponse", raw_data),
            symbol=symbol,
        )
        assert response.coin == "ETH-PERP"
        assert len(response.levels) == 2
        assert len(response.levels[0]) == 0  # Empty bids
        assert len(response.levels[1]) == 0  # Empty asks

    def test_recent_trades_mixed_valid_invalid_items(
        self,
        valid_raw_public_trade: dict[str, Any],
        symbol: str,
    ) -> None:
        """Test recent trades response with mix of valid and invalid items."""
        invalid_trade = {"coin": "ETH-PERP"}  # Missing required fields
        raw_data = [
            valid_raw_public_trade.copy(),  # Valid
            invalid_trade,  # Invalid - should cause error
            valid_raw_public_trade.copy(),  # Valid but won't be processed due to error
        ]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_recent_trades_response(
                cast("RawJsonResponse", raw_data),
                symbol=symbol,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "(index 1)" in exc_info.value.message  # Should fail on second item
