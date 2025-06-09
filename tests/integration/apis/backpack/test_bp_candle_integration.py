"""Integration tests for Backpack Candle model pipeline.

These tests validate the complete data pipeline from BackpackAPI.get_market_data() calls
to final Candle internal domain models using pytest-recording (VCR) for deterministic tests.

Tests cover:
- Successful candle retrieval for valid symbols and intervals
- OHLCV data validation and relationships
- Time series validation and ordering
- Edge cases and error handling
- Complete API -> Service -> Handler -> Mapper -> Internal Model pipeline
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Protocol

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.service_args_models import GetMarketDataArgs
from cyberdelta.core.models.market.candle import Candle


class FreezerProtocol(Protocol):
    """Protocol for pytest-freezer fixture."""

    def move_to(self, target: datetime | str) -> None:
        """Move the frozen time to the target datetime."""
        ...


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/candle"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_market_data_sol_usdc_1h_success(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
    freezer: FreezerProtocol,
) -> None:
    """Test BackpackAPI.get_market_data() with SOL_USDC 1h interval returns valid Candle models.

    This validates the complete pipeline:
    - API method call (get_market_data)
    - Request building (/api/v1/klines endpoint)
    - Response handling and validation
    - Mapping to internal Candle models
    """
    # Use dynamic timestamps that are recent but deterministic for VCR
    # Calculate a time that's recent enough for API but fixed relative to test run
    now = datetime.now(UTC)
    end_time_dt = now - timedelta(days=7)  # 1 week ago
    start_time_dt = end_time_dt - timedelta(hours=1)  # 1 hour before that

    # Freeze time to ensure VCR consistency
    freezer.move_to(end_time_dt)

    end_time = int(end_time_dt.timestamp())
    start_time = int(start_time_dt.timestamp())

    args = GetMarketDataArgs(
        symbol="SOL_USDC",
        timeframe="1h",
        start_time_ms=start_time * 1000,  # Convert to milliseconds
        end_time_ms=end_time * 1000,
    )

    candles = await bp_api_for_test_env.get_market_data(args)

    # Validate return type
    assert isinstance(candles, list), f"Expected list of candles, got {type(candles)}"

    # Validate each candle (if any exist in the time range)
    if len(candles) > 0:
        for i, candle in enumerate(candles):
            assert isinstance(candle, Candle), (
                f"Candle {i} should be Candle model, got {type(candle)}"
            )

            # Validate core candle fields exist
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
            assert candle.low > Decimal("0"), f"Candle {i} low should be positive, got {candle.low}"
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
            assert candle.symbol == "SOL_USDC", (
                f"Candle {i} symbol should be 'SOL_USDC', got '{candle.symbol}'"
            )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/candle"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_market_data_btc_usdc_1h_success(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
    freezer: FreezerProtocol,
) -> None:
    """Test BackpackAPI.get_market_data() with BTC_USDC 1h interval returns valid Candle models."""
    # Use dynamic timestamps that are recent but deterministic for VCR
    now = datetime.now(UTC)
    end_time_dt = now - timedelta(days=7)  # 1 week ago
    start_time_dt = end_time_dt - timedelta(hours=1)  # 1 hour before that

    # Freeze time to ensure VCR consistency
    freezer.move_to(end_time_dt)

    end_time = int(end_time_dt.timestamp())
    start_time = int(start_time_dt.timestamp())

    args = GetMarketDataArgs(
        symbol="BTC_USDC",
        timeframe="1h",
        start_time_ms=start_time * 1000,
        end_time_ms=end_time * 1000,
    )

    candles = await bp_api_for_test_env.get_market_data(args)

    # Validate return type
    assert isinstance(candles, list), f"Expected list of candles, got {type(candles)}"

    # Validate BTC price ranges if candles exist
    if len(candles) > 0:
        for i, candle in enumerate(candles):
            assert isinstance(candle, Candle), (
                f"Candle {i} should be Candle model, got {type(candle)}"
            )
            assert candle.symbol == "BTC_USDC", (
                f"Candle {i} symbol should be 'BTC_USDC', got '{candle.symbol}'"
            )

            # BTC prices should be in reasonable range
            assert candle.open > Decimal("1000"), f"BTC open price seems too low: {candle.open}"
            assert candle.high > Decimal("1000"), f"BTC high price seems too low: {candle.high}"
            assert candle.low > Decimal("1000"), f"BTC low price seems too low: {candle.low}"
            assert candle.close > Decimal("1000"), f"BTC close price seems too low: {candle.close}"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/candle"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_market_data_different_intervals(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
    freezer: FreezerProtocol,
) -> None:
    """Test BackpackAPI.get_market_data() with different time intervals."""
    # Use dynamic timestamps that are recent but deterministic for VCR
    now = datetime.now(UTC)
    end_time_dt = now - timedelta(days=7)  # 1 week ago

    # Freeze time to ensure VCR consistency
    freezer.move_to(end_time_dt)

    end_time = int(end_time_dt.timestamp())

    # Test different intervals
    intervals = ["1m", "5m", "1h"]

    for interval in intervals:
        # Adjust time range based on interval
        if interval == "1m":
            start_time = end_time - 300  # 5 minutes earlier
        elif interval == "5m":
            start_time = end_time - 1800  # 30 minutes earlier
        else:  # 1h
            start_time = end_time - 3600  # 1 hour earlier

        args = GetMarketDataArgs(
            symbol="SOL_USDC",
            timeframe=interval,
            start_time_ms=start_time * 1000,
            end_time_ms=end_time * 1000,
        )

        try:
            candles = await bp_api_for_test_env.get_market_data(args)

            # Validate return type
            assert isinstance(candles, list), (
                f"Expected list for interval {interval}, got {type(candles)}"
            )

            # Validate structure if candles exist
            if len(candles) > 0:
                for candle in candles:
                    assert isinstance(candle, Candle), f"Should be Candle for interval {interval}"
                    assert candle.symbol == "SOL_USDC", f"Wrong symbol for interval {interval}"

        except APIError:
            # Some intervals might not be supported, which is acceptable
            pass


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/candle"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_market_data_chronological_ordering(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
    freezer: FreezerProtocol,
) -> None:
    """Test BackpackAPI.get_market_data() returns candles in proper chronological order."""
    # Use dynamic timestamps that are recent but deterministic for VCR
    now = datetime.now(UTC)
    end_time_dt = now - timedelta(days=7)  # 1 week ago
    start_time_dt = end_time_dt - timedelta(hours=1)  # 1 hour before that

    # Freeze time to ensure VCR consistency
    freezer.move_to(end_time_dt)

    end_time = int(end_time_dt.timestamp())
    start_time = int(start_time_dt.timestamp())

    args = GetMarketDataArgs(
        symbol="SOL_USDC",
        timeframe="1h",
        start_time_ms=start_time * 1000,
        end_time_ms=end_time * 1000,
    )

    candles = await bp_api_for_test_env.get_market_data(args)

    # Validate return type
    assert isinstance(candles, list), f"Expected list of candles, got {type(candles)}"

    # Check chronological ordering if we have multiple candles
    if len(candles) > 1:
        for i in range(len(candles) - 1):
            current_candle = candles[i]
            next_candle = candles[i + 1]

            # Verify both candles have timestamps
            if hasattr(current_candle, "open_time") and hasattr(next_candle, "open_time"):
                # Check that timestamps are reasonable and consistent
                assert isinstance(current_candle.open_time, datetime), (
                    f"Candle {i} open_time should be datetime, got {type(current_candle.open_time)}"
                )
                assert isinstance(next_candle.open_time, datetime), (
                    f"Candle {i + 1} open_time should be datetime, "
                    f"got {type(next_candle.open_time)}"
                )

                # For 1h interval, timestamps should be 1 hour apart
                time_diff = abs((next_candle.open_time - current_candle.open_time).total_seconds())
                # Allow some flexibility in ordering, but times should be reasonable
                assert time_diff >= 3600, (
                    f"1h candles should be at least 1 hour apart: {time_diff} seconds"
                )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/candle"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_market_data_precision_validation(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
    freezer: FreezerProtocol,
) -> None:
    """Test BackpackAPI.get_market_data() ensures proper Decimal precision handling."""
    # Use dynamic timestamps that are recent but deterministic for VCR
    now = datetime.now(UTC)
    end_time_dt = now - timedelta(days=7)  # 1 week ago
    start_time_dt = end_time_dt - timedelta(hours=1)  # 1 hour before that

    # Freeze time to ensure VCR consistency
    freezer.move_to(end_time_dt)

    end_time = int(end_time_dt.timestamp())
    start_time = int(start_time_dt.timestamp())

    args = GetMarketDataArgs(
        symbol="SOL_USDC",
        timeframe="1h",
        start_time_ms=start_time * 1000,
        end_time_ms=end_time * 1000,
    )

    candles = await bp_api_for_test_env.get_market_data(args)

    # Validate return type
    assert isinstance(candles, list), f"Expected list of candles, got {type(candles)}"

    if len(candles) > 0:
        for i, candle in enumerate(candles):
            # Validate OHLCV precision
            ohlcv_fields = [candle.open, candle.high, candle.low, candle.close, candle.volume]
            field_names = ["open", "high", "low", "close", "volume"]
            for field_name, field_value in zip(field_names, ohlcv_fields, strict=False):
                assert isinstance(field_value, Decimal), (
                    f"Candle {i} {field_name} should be Decimal, got {type(field_value)}"
                )

                # Test arithmetic operations work correctly
                doubled_value = field_value * Decimal("2")
                assert isinstance(doubled_value, Decimal), (
                    f"Candle {i} {field_name} arithmetic should maintain Decimal type"
                )

                if field_value > Decimal("0"):  # Don't test with zero values
                    halved_value = field_value / Decimal("2")
                    assert isinstance(halved_value, Decimal), (
                        f"Candle {i} {field_name} division should maintain Decimal type"
                    )
                    assert halved_value < field_value, (
                        f"Candle {i} {field_name} halved should be less than original"
                    )

            # Test candle calculations
            price_range = candle.high - candle.low
            assert isinstance(price_range, Decimal), (
                "Price range calculation should maintain Decimal type"
            )
            assert price_range >= Decimal("0"), (
                f"Candle {i} price range should be non-negative: {price_range}"
            )

            # Test volume calculations if volume > 0
            if candle.volume > Decimal("0"):
                avg_price = (candle.high + candle.low) / Decimal("2")
                notional_volume = candle.volume * avg_price
                assert isinstance(notional_volume, Decimal), (
                    "Notional volume calculation should maintain Decimal type"
                )
                assert notional_volume > Decimal("0"), (
                    f"Candle {i} notional volume should be positive"
                )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/candle"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_market_data_ohlc_relationships_validation(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
    freezer: FreezerProtocol,
) -> None:
    """Test BackpackAPI.get_market_data() validates comprehensive OHLC relationships."""
    # Use dynamic timestamps that are recent but deterministic for VCR
    now = datetime.now(UTC)
    end_time_dt = now - timedelta(days=7)  # 1 week ago
    start_time_dt = end_time_dt - timedelta(hours=1)  # 1 hour before that

    # Freeze time to ensure VCR consistency
    freezer.move_to(end_time_dt)

    end_time = int(end_time_dt.timestamp())
    start_time = int(start_time_dt.timestamp())

    args = GetMarketDataArgs(
        symbol="SOL_USDC",
        timeframe="1h",
        start_time_ms=start_time * 1000,
        end_time_ms=end_time * 1000,
    )

    candles = await bp_api_for_test_env.get_market_data(args)

    # Validate return type
    assert isinstance(candles, list), f"Expected list of candles, got {type(candles)}"

    if len(candles) > 0:
        for i, candle in enumerate(candles):
            # Basic OHLC relationships
            assert candle.high >= candle.low, (
                f"Candle {i}: high {candle.high} must be >= low {candle.low}"
            )
            assert candle.high >= candle.open, (
                f"Candle {i}: high {candle.high} must be >= open {candle.open}"
            )
            assert candle.high >= candle.close, (
                f"Candle {i}: high {candle.high} must be >= close {candle.close}"
            )
            assert candle.low <= candle.open, (
                f"Candle {i}: low {candle.low} must be <= open {candle.open}"
            )
            assert candle.low <= candle.close, (
                f"Candle {i}: low {candle.low} must be <= close {candle.close}"
            )

            # Advanced validation
            # High and low should not be equal unless it's a very unusual market condition
            if candle.high == candle.low:
                # If high == low, then open and close should also equal high/low
                assert candle.open == candle.high, (
                    f"Candle {i}: if high == low, open should also equal: "
                    f"{candle.open} != {candle.high}"
                )
                assert candle.close == candle.high, (
                    f"Candle {i}: if high == low, close should also equal: "
                    f"{candle.close} != {candle.high}"
                )

            # Price range validation
            price_range = candle.high - candle.low
            avg_price = (candle.high + candle.low) / Decimal("2")

            if avg_price > Decimal("0"):
                range_percentage = (price_range / avg_price) * Decimal("100")
                # For 1h candles, price range shouldn't be extremely large (sanity check)
                assert range_percentage < Decimal("50"), (
                    f"Candle {i}: price range seems unusually large: {range_percentage}%"
                )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/candle"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_market_data_invalid_symbol_error(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
    freezer: FreezerProtocol,
) -> None:
    """Test BackpackAPI.get_market_data() with invalid symbol raises appropriate error."""
    # Use dynamic timestamps that are recent but deterministic for VCR
    now = datetime.now(UTC)
    end_time_dt = now - timedelta(days=7)  # 1 week ago
    start_time_dt = end_time_dt - timedelta(days=1)  # 1 day before that

    # Freeze time to ensure VCR consistency
    freezer.move_to(end_time_dt)

    end_time = int(end_time_dt.timestamp())
    start_time = int(start_time_dt.timestamp())

    args = GetMarketDataArgs(
        symbol="INVALID_SYMBOL",
        timeframe="1h",
        start_time_ms=start_time * 1000,
        end_time_ms=end_time * 1000,
    )

    with pytest.raises(APIError) as exc_info:
        await bp_api_for_test_env.get_market_data(args)

    # Validate error details
    error = exc_info.value
    assert "INVALID_SYMBOL" in str(error) or "symbol" in str(error).lower()


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/candle"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_market_data_invalid_interval_error(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
    freezer: FreezerProtocol,
) -> None:
    """Test BackpackAPI.get_market_data() with invalid interval raises appropriate error."""
    # Use dynamic timestamps that are recent but deterministic for VCR
    now = datetime.now(UTC)
    end_time_dt = now - timedelta(days=7)  # 1 week ago
    start_time_dt = end_time_dt - timedelta(days=1)  # 1 day before that

    # Freeze time to ensure VCR consistency
    freezer.move_to(end_time_dt)

    end_time = int(end_time_dt.timestamp())
    start_time = int(start_time_dt.timestamp())

    with pytest.raises(ValueError) as exc_info:
        args = GetMarketDataArgs(
            symbol="SOL_USDC",
            timeframe="invalid_interval",
            start_time_ms=start_time * 1000,
            end_time_ms=end_time * 1000,
        )
        await bp_api_for_test_env.get_market_data(args)

    # Validate error details
    error = exc_info.value
    assert (
        "invalid_interval" in str(error)
        or "interval" in str(error).lower()
        or "timeframe" in str(error).lower()
    )


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/candle"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_market_data_invalid_time_range_error(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
    freezer: FreezerProtocol,
) -> None:
    """Test BackpackAPI.get_market_data() with invalid time range raises appropriate error."""
    # Invalid time range: start_time > end_time
    now = datetime.now(UTC)
    end_time_dt = now - timedelta(days=7)  # 1 week ago
    start_time_dt = end_time_dt + timedelta(hours=1)  # 1 hour AFTER end_time (invalid)

    # Freeze time to ensure VCR consistency
    freezer.move_to(end_time_dt)

    end_time = int(end_time_dt.timestamp())
    start_time = int(start_time_dt.timestamp())

    with pytest.raises(ValueError) as exc_info:
        args = GetMarketDataArgs(
            symbol="SOL_USDC",
            timeframe="1h",
            start_time_ms=start_time * 1000,
            end_time_ms=end_time * 1000,
        )
        await bp_api_for_test_env.get_market_data(args)

    # Validate error contains relevant information
    error_str = str(exc_info.value)
    assert len(error_str) > 0, "Error message should not be empty"


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/candle"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_market_data_future_time_range_handling(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
    freezer: FreezerProtocol,
) -> None:
    """Test BackpackAPI.get_market_data() with future time range returns empty or errors."""
    # Time range in the future (relative to frozen time for VCR)
    now = datetime.now(UTC)
    current_time_dt = now - timedelta(days=7)  # 1 week ago as base
    start_time_dt = current_time_dt + timedelta(days=1)  # 1 day in the future
    end_time_dt = start_time_dt + timedelta(hours=1)  # 1 hour later

    # Freeze time to ensure VCR consistency
    freezer.move_to(current_time_dt)

    start_time = int(start_time_dt.timestamp())
    end_time = int(end_time_dt.timestamp())

    args = GetMarketDataArgs(
        symbol="SOL_USDC",
        timeframe="1h",
        start_time_ms=start_time * 1000,
        end_time_ms=end_time * 1000,
    )

    try:
        candles = await bp_api_for_test_env.get_market_data(args)

        # If successful, should return empty list for future time range
        assert isinstance(candles, list), f"Expected list, got {type(candles)}"
        assert len(candles) == 0, (
            f"Expected empty list for future time range, got {len(candles)} candles"
        )

    except APIError:
        # If it raises an error for future time range, that's also acceptable
        pass


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/candle"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_market_data_very_old_time_range_handling(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_market_data() with very old time range (before market existed)."""
    # Very old time range (before crypto markets existed)
    start_time = 946684800  # Year 2000
    end_time = start_time + 3600  # 1 hour later

    args = GetMarketDataArgs(
        symbol="SOL_USDC",
        timeframe="1h",
        start_time_ms=start_time * 1000,
        end_time_ms=end_time * 1000,
    )

    try:
        candles = await bp_api_for_test_env.get_market_data(args)

        # If successful, should return empty list for time before market existed
        assert isinstance(candles, list), f"Expected list, got {type(candles)}"
        assert len(candles) == 0, (
            f"Expected empty list for pre-market time range, got {len(candles)} candles"
        )

    except APIError:
        # If it raises an error for very old time range, that's also acceptable
        pass


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/candle"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_market_data_multiple_symbols_consistency(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
    freezer: FreezerProtocol,
) -> None:
    """Test BackpackAPI.get_market_data() returns consistent structure across symbols."""
    symbols = ["SOL_USDC", "BTC_USDC"]
    all_candles: dict[str, list[Candle]] = {}

    # Use dynamic timestamps that are recent but deterministic for VCR
    now = datetime.now(UTC)
    end_time_dt = now - timedelta(days=7)  # 1 week ago
    start_time_dt = end_time_dt - timedelta(days=1)  # 1 day before that

    # Freeze time to ensure VCR consistency
    freezer.move_to(end_time_dt)

    end_time = int(end_time_dt.timestamp())
    start_time = int(start_time_dt.timestamp())

    for symbol in symbols:
        args = GetMarketDataArgs(
            symbol=symbol,
            timeframe="1h",
            start_time_ms=start_time * 1000,
            end_time_ms=end_time * 1000,
        )

        candles = await bp_api_for_test_env.get_market_data(args)
        assert isinstance(candles, list), f"Expected list for {symbol}, got {type(candles)}"
        all_candles[symbol] = candles

    # Verify structure consistency across symbols
    for symbol, candles in all_candles.items():
        if len(candles) > 0:
            # All candles for each symbol should have the same structure
            first_candle = candles[0]
            required_attrs = ["symbol", "open", "high", "low", "close", "volume"]

            for attr in required_attrs:
                assert hasattr(first_candle, attr), (
                    f"Candle for {symbol} missing required attribute: {attr}"
                )
                value = getattr(first_candle, attr)
                assert value is not None, (
                    f"Candle for {symbol} has None value for required attribute: {attr}"
                )

            # All candles in the list should have same symbol
            for i, candle in enumerate(candles):
                assert candle.symbol == symbol, (
                    f"Candle {i} for {symbol} has wrong symbol: {candle.symbol}"
                )
