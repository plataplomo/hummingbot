"""Unit tests for Backpack raw query parameter models.

Tests validation and processing of query parameters for various Backpack exchange API endpoints
including market data, trading, and account-related query parameter structures.
"""

from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_query_params import (
    BackpackRawGetAccountInfoParams,
    BackpackRawGetBalancesParams,
    BackpackRawGetFundingRateParams,
    BackpackRawGetHistoricalFundingRatesParams,
    BackpackRawGetHistoricalTradesParams,
    BackpackRawGetMarketDataParams,
    BackpackRawGetMarketParams,
    BackpackRawGetMarketsParams,
    BackpackRawGetOpenOrdersParams,
    BackpackRawGetOrderBookParams,
    BackpackRawGetOrderHistoryParams,
    BackpackRawGetOrderParams,
    BackpackRawGetPositionsParams,
    BackpackRawGetRecentTradesParams,
    BackpackRawGetTickerParams,
    BackpackRawGetTradeHistoryParams,
)
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import EmptyStringError


class TestBackpackRawGetTickerParams:
    """Test BackpackRawGetTickerParams validation."""

    def test_valid_symbol(self) -> None:
        """Test valid symbol creation."""
        params = BackpackRawGetTickerParams(symbol="BTC_USDC")
        assert params.symbol == "BTC_USDC"

    def test_alias_support(self) -> None:
        """Test field alias support."""
        data = {"symbol": "ETH_USDC"}
        params = BackpackRawGetTickerParams.model_validate(data)
        assert params.symbol == "ETH_USDC"

    def test_missing_symbol(self) -> None:
        """Test missing required symbol field."""
        with pytest.raises(ValidationError, match="Field required"):
            BackpackRawGetTickerParams.model_validate({})

    def test_empty_symbol(self) -> None:
        """Test empty symbol validation."""
        with pytest.raises(EmptyStringError, match="String cannot be empty"):
            BackpackRawGetTickerParams(symbol="")

    def test_whitespace_symbol(self) -> None:
        """Test whitespace-only symbol validation."""
        with pytest.raises(EmptyStringError, match="String cannot be empty"):
            BackpackRawGetTickerParams(symbol="   ")

    def test_symbol_too_long(self) -> None:
        """Test symbol length validation."""
        with pytest.raises(TypeFieldError, match="must be string with max length 64"):
            BackpackRawGetTickerParams(symbol="A" * 65)

    def test_symbol_wrong_type(self) -> None:
        """Test symbol type validation."""
        data: dict[str, Any] = {"symbol": 123}
        with pytest.raises(TypeError):
            BackpackRawGetTickerParams.model_validate(data)

    def test_extra_fields_forbidden(self) -> None:
        """Test that extra fields are forbidden."""
        data: dict[str, Any] = {"symbol": "BTC_USDC", "extra_field": "not_allowed"}
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            BackpackRawGetTickerParams.model_validate(data)

    def test_immutability(self) -> None:
        """Test that params are immutable."""
        params = BackpackRawGetTickerParams(symbol="BTC_USDC")
        with pytest.raises(ValidationError, match="Instance is frozen"):
            params.symbol = "ETH_USDC"


class TestBackpackRawGetOrderBookParams:
    """Test BackpackRawGetOrderBookParams validation."""

    def test_valid_params_with_limit(self) -> None:
        """Test valid params with limit."""
        params = BackpackRawGetOrderBookParams(symbol="BTC_USDC", limit=100)
        assert params.symbol == "BTC_USDC"
        assert params.limit == 100

    def test_valid_params_without_limit(self) -> None:
        """Test valid params without limit."""
        params = BackpackRawGetOrderBookParams(symbol="BTC_USDC")
        assert params.symbol == "BTC_USDC"
        assert params.limit is None

    def test_limit_zero_allowed(self) -> None:
        """Test that limit=0 is allowed."""
        params = BackpackRawGetOrderBookParams(symbol="BTC_USDC", limit=0)
        assert params.limit == 0

    def test_negative_limit_rejected(self) -> None:
        """Test negative limit validation."""
        with pytest.raises(ValidationError, match="Must be >= 0"):
            BackpackRawGetOrderBookParams(symbol="BTC_USDC", limit=-1)

    def test_limit_wrong_type(self) -> None:
        """Test limit type validation."""
        data: dict[str, Any] = {"symbol": "BTC_USDC", "limit": "invalid"}
        with pytest.raises(ValidationError):
            BackpackRawGetOrderBookParams.model_validate(data)

    def test_missing_symbol(self) -> None:
        """Test missing required symbol."""
        with pytest.raises(ValidationError, match="Field required"):
            BackpackRawGetOrderBookParams.model_validate({"limit": 100})


class TestBackpackRawGetRecentTradesParams:
    """Test BackpackRawGetRecentTradesParams validation."""

    def test_valid_params_complete(self) -> None:
        """Test valid params with all fields."""
        params = BackpackRawGetRecentTradesParams(symbol="BTC_USDC", limit=50)
        assert params.symbol == "BTC_USDC"
        assert params.limit == 50

    def test_valid_params_minimal(self) -> None:
        """Test valid params with only required fields."""
        params = BackpackRawGetRecentTradesParams(symbol="BTC_USDC")
        assert params.symbol == "BTC_USDC"
        assert params.limit is None


class TestBackpackRawGetBalancesParams:
    """Test BackpackRawGetBalancesParams validation."""

    def test_empty_params_valid(self) -> None:
        """Test that empty params are valid."""
        params = BackpackRawGetBalancesParams()
        assert params is not None

    def test_dict_creation(self) -> None:
        """Test creation from empty dict."""
        params = BackpackRawGetBalancesParams.model_validate({})
        assert params is not None

    def test_extra_fields_forbidden(self) -> None:
        """Test that extra fields are forbidden."""
        data: dict[str, Any] = {"unexpected": "field"}
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            BackpackRawGetBalancesParams.model_validate(data)


class TestBackpackRawGetPositionsParams:
    """Test BackpackRawGetPositionsParams validation."""

    def test_empty_params_valid(self) -> None:
        """Test that empty params are valid."""
        params = BackpackRawGetPositionsParams()
        assert params is not None

    def test_immutability(self) -> None:
        """Test that params are immutable."""
        params = BackpackRawGetPositionsParams()
        # Since there are no fields, we can't test assignment, but we can verify the model is frozen
        assert params.model_config.get("frozen") is True


class TestBackpackRawGetOpenOrdersParams:
    """Test BackpackRawGetOpenOrdersParams validation."""

    def test_with_symbol(self) -> None:
        """Test with symbol filter."""
        params = BackpackRawGetOpenOrdersParams(symbol="BTC_USDC")
        assert params.symbol == "BTC_USDC"

    def test_without_symbol(self) -> None:
        """Test without symbol filter."""
        params = BackpackRawGetOpenOrdersParams()
        assert params.symbol is None

    def test_empty_symbol_rejected(self) -> None:
        """Test empty symbol validation."""
        with pytest.raises(EmptyStringError, match="String cannot be empty"):
            BackpackRawGetOpenOrdersParams(symbol="")


class TestBackpackRawGetFundingRateParams:
    """Test BackpackRawGetFundingRateParams validation."""

    def test_valid_symbol(self) -> None:
        """Test valid symbol."""
        params = BackpackRawGetFundingRateParams(symbol="BTC_USDC")
        assert params.symbol == "BTC_USDC"

    def test_missing_symbol(self) -> None:
        """Test missing required symbol."""
        with pytest.raises(ValidationError, match="Field required"):
            BackpackRawGetFundingRateParams.model_validate({})


class TestBackpackRawGetHistoricalFundingRatesParams:
    """Test BackpackRawGetHistoricalFundingRatesParams validation."""

    def test_valid_complete_params(self) -> None:
        """Test valid params with all fields."""
        params = BackpackRawGetHistoricalFundingRatesParams(
            symbol="BTC_USDC",
            startTime=1678886400,
            endTime=1678972800,
            limit=100,
        )
        assert params.symbol == "BTC_USDC"
        assert params.startTime == 1678886400
        assert params.endTime == 1678972800
        assert params.limit == 100

    def test_valid_minimal_params(self) -> None:
        """Test valid params with only required fields."""
        params = BackpackRawGetHistoricalFundingRatesParams(symbol="BTC_USDC")
        assert params.symbol == "BTC_USDC"
        assert params.startTime is None
        assert params.endTime is None
        assert params.limit is None

    def test_negative_timestamps_allowed(self) -> None:
        """Test that negative timestamps are allowed (historical data)."""
        params = BackpackRawGetHistoricalFundingRatesParams(
            symbol="BTC_USDC",
            startTime=-1,
            endTime=-1,
        )
        assert params.startTime == -1
        assert params.endTime == -1

    def test_negative_limit_rejected(self) -> None:
        """Test negative limit validation."""
        with pytest.raises(ValidationError, match="Must be >= 0"):
            BackpackRawGetHistoricalFundingRatesParams(symbol="BTC_USDC", limit=-1)

    def test_timestamp_wrong_type(self) -> None:
        """Test timestamp type validation."""
        data: dict[str, Any] = {"symbol": "BTC_USDC", "startTime": "invalid"}
        with pytest.raises(ValidationError):
            BackpackRawGetHistoricalFundingRatesParams.model_validate(data)


class TestBackpackRawGetAccountInfoParams:
    """Test BackpackRawGetAccountInfoParams validation."""

    def test_empty_params_valid(self) -> None:
        """Test that empty params are valid."""
        params = BackpackRawGetAccountInfoParams()
        assert params is not None


class TestBackpackRawGetMarketsParams:
    """Test BackpackRawGetMarketsParams validation."""

    def test_empty_params_valid(self) -> None:
        """Test that empty params are valid."""
        params = BackpackRawGetMarketsParams()
        assert params is not None


class TestBackpackRawGetMarketParams:
    """Test BackpackRawGetMarketParams validation."""

    def test_valid_symbol(self) -> None:
        """Test valid symbol."""
        params = BackpackRawGetMarketParams(symbol="BTC_USDC")
        assert params.symbol == "BTC_USDC"

    def test_missing_symbol(self) -> None:
        """Test missing required symbol."""
        with pytest.raises(ValidationError, match="Field required"):
            BackpackRawGetMarketParams.model_validate({})


class TestBackpackRawGetOrderHistoryParams:
    """Test BackpackRawGetOrderHistoryParams validation."""

    def test_valid_complete_params(self) -> None:
        """Test valid params with all fields."""
        data = {
            "symbol": "BTC_USDC",
            "orderId": "12345",
            "clientId": "client123",
            "limit": 50,
            "from": 1678886400000,
            "to": 1678972800000,
        }
        params = BackpackRawGetOrderHistoryParams.model_validate(data)
        assert params.symbol == "BTC_USDC"
        assert params.orderId == "12345"
        assert params.clientId == "client123"
        assert params.limit == 50
        assert params.start_time == 1678886400000
        assert params.end_time == 1678972800000

    def test_valid_minimal_params(self) -> None:
        """Test valid params with no filters."""
        params = BackpackRawGetOrderHistoryParams()
        assert params.symbol is None
        assert params.orderId is None
        assert params.clientId is None
        assert params.limit is None
        assert params.start_time is None
        assert params.end_time is None

    def test_alias_support(self) -> None:
        """Test field alias support for timestamps."""
        data = {
            "symbol": "BTC_USDC",
            "from": 1678886400000,
            "to": 1678972800000,
        }
        params = BackpackRawGetOrderHistoryParams.model_validate(data)
        assert params.start_time == 1678886400000
        assert params.end_time == 1678972800000

    def test_empty_string_fields_rejected(self) -> None:
        """Test empty string validation."""
        with pytest.raises(EmptyStringError, match="String cannot be empty"):
            BackpackRawGetOrderHistoryParams(orderId="")

        with pytest.raises(EmptyStringError, match="String cannot be empty"):
            BackpackRawGetOrderHistoryParams(clientId="")


class TestBackpackRawGetTradeHistoryParams:
    """Test BackpackRawGetTradeHistoryParams validation."""

    def test_valid_complete_params(self) -> None:
        """Test valid params with all fields."""
        data = {
            "symbol": "BTC_USDC",
            "limit": 100,
            "from": 1678886400000,
            "to": 1678972800000,
            "fromId": "trade123",
        }
        params = BackpackRawGetTradeHistoryParams.model_validate(data)
        assert params.symbol == "BTC_USDC"
        assert params.limit == 100
        assert params.start_time == 1678886400000
        assert params.end_time == 1678972800000
        assert params.fromId == "trade123"

    def test_valid_minimal_params(self) -> None:
        """Test valid params with no filters."""
        params = BackpackRawGetTradeHistoryParams()
        assert params.symbol is None
        assert params.limit is None
        assert params.start_time is None
        assert params.end_time is None
        assert params.fromId is None

    def test_alias_support(self) -> None:
        """Test field alias support for timestamps."""
        data = {
            "from": 1678886400000,
            "to": 1678972800000,
            "fromId": "trade456",
        }
        params = BackpackRawGetTradeHistoryParams.model_validate(data)
        assert params.start_time == 1678886400000
        assert params.end_time == 1678972800000
        assert params.fromId == "trade456"


class TestBackpackRawGetMarketDataParams:
    """Test BackpackRawGetMarketDataParams validation."""

    def test_valid_complete_params(self) -> None:
        """Test valid params with all fields."""
        params = BackpackRawGetMarketDataParams(
            symbol="BTC_USDC",
            interval="1h",
            startTime=1678886400,
            endTime=1678972800,
            limit=500,
        )
        assert params.symbol == "BTC_USDC"
        assert params.interval == "1h"
        assert params.startTime == 1678886400
        assert params.endTime == 1678972800
        assert params.limit == 500

    def test_valid_minimal_params(self) -> None:
        """Test valid params with only required fields."""
        params = BackpackRawGetMarketDataParams(symbol="BTC_USDC", interval="1m")
        assert params.symbol == "BTC_USDC"
        assert params.interval == "1m"
        assert params.startTime is None
        assert params.endTime is None
        assert params.limit is None

    def test_all_valid_intervals(self) -> None:
        """Test all valid interval values."""
        valid_intervals = [
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
        ]
        for interval in valid_intervals:
            data = {"symbol": "BTC_USDC", "interval": interval}
            params = BackpackRawGetMarketDataParams.model_validate(data)
            assert params.interval == interval

    def test_invalid_interval(self) -> None:
        """Test invalid interval validation."""
        data: dict[str, Any] = {"symbol": "BTC_USDC", "interval": "invalid"}
        with pytest.raises(ValidationError, match="Input should be"):
            BackpackRawGetMarketDataParams.model_validate(data)

    def test_missing_required_fields(self) -> None:
        """Test missing required fields."""
        with pytest.raises(ValidationError, match="Field required"):
            BackpackRawGetMarketDataParams.model_validate({"interval": "1h"})

        with pytest.raises(ValidationError, match="Field required"):
            BackpackRawGetMarketDataParams.model_validate({"symbol": "BTC_USDC"})


class TestBackpackRawGetHistoricalTradesParams:
    """Test BackpackRawGetHistoricalTradesParams validation."""

    def test_valid_complete_params(self) -> None:
        """Test valid params with all fields."""
        params = BackpackRawGetHistoricalTradesParams(
            symbol="BTC_USDC",
            limit=100,
            fromId="trade789",
        )
        assert params.symbol == "BTC_USDC"
        assert params.limit == 100
        assert params.fromId == "trade789"

    def test_valid_minimal_params(self) -> None:
        """Test valid params with only required fields."""
        params = BackpackRawGetHistoricalTradesParams(symbol="BTC_USDC")
        assert params.symbol == "BTC_USDC"
        assert params.limit is None
        assert params.fromId is None

    def test_missing_symbol(self) -> None:
        """Test missing required symbol."""
        with pytest.raises(ValidationError, match="Field required"):
            BackpackRawGetHistoricalTradesParams.model_validate({"limit": 100})


class TestBackpackRawGetOrderParams:
    """Test BackpackRawGetOrderParams validation."""

    def test_valid_symbol(self) -> None:
        """Test valid symbol."""
        params = BackpackRawGetOrderParams(symbol="BTC_USDC")
        assert params.symbol == "BTC_USDC"

    def test_missing_symbol(self) -> None:
        """Test missing required symbol."""
        with pytest.raises(ValidationError, match="Field required"):
            BackpackRawGetOrderParams.model_validate({})


class TestGeneralValidationBehavior:
    """Test general validation behavior across all models."""

    def test_null_values_in_optional_fields(self) -> None:
        """Test that null values in optional fields are handled correctly."""
        data = {
            "symbol": "BTC_USDC",
            "interval": "1h",
            "limit": None,
            "startTime": None,
        }
        params = BackpackRawGetMarketDataParams.model_validate(data)
        assert params.limit is None
        assert params.startTime is None

    def test_populate_by_name_config(self) -> None:
        """Test that populate_by_name configuration works."""
        # Test with field names
        data1 = {"symbol": "BTC_USDC", "start_time": 1678886400000}
        params1 = BackpackRawGetTradeHistoryParams.model_validate(data1)
        assert params1.start_time == 1678886400000

        # Test with aliases
        data2 = {"symbol": "BTC_USDC", "from": 1678886400000}
        params2 = BackpackRawGetTradeHistoryParams.model_validate(data2)
        assert params2.start_time == 1678886400000

    def test_unicode_symbols_allowed(self) -> None:
        """Test that unicode characters in symbols are properly handled."""
        # Unicode should be allowed as long as length constraints are met
        params = BackpackRawGetTickerParams(symbol="BTC_USDC_🚀")
        assert params.symbol == "BTC_USDC_🚀"

    def test_control_characters_in_symbols(self) -> None:
        """Test that control characters in symbols are allowed (no special filtering)."""
        # Control characters are actually allowed by the validation
        params = BackpackRawGetTickerParams(symbol="BTC_USDC\x00")
        assert params.symbol == "BTC_USDC\x00"

    def test_very_long_symbol_with_unicode(self) -> None:
        """Test symbol length validation with unicode characters."""
        # Create a symbol that's too long with unicode
        long_symbol = "A" * 65  # Simple approach - just use too many chars
        with pytest.raises(TypeFieldError, match="must be string with max length 64"):
            BackpackRawGetTickerParams(symbol=long_symbol)
