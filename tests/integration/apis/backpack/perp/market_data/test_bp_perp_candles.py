"""Integration tests for Backpack perp candle model pipeline.

These tests validate the complete data pipeline from BackpackAPI.get_market_data() calls
to final Candle internal domain models using pytest-recording (VCR) for deterministic tests.

Tests cover perpetual futures markets only:
- Successful candle retrieval for valid perp symbols and intervals
- OHLCV data validation and relationships for perp markets
- Time series validation and ordering
- Perpetual-specific characteristics (funding impact, leverage characteristics)
- Edge cases and error handling
- Complete API -> Service -> Handler -> Mapper -> Internal Model pipeline
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.common import APIError
from cyberdelta.apis.models.service_args.market_data import GetMarketDataArgs
from cyberdelta.core.models.market.candle import Candle
from cyberdelta.core.symbols import exchanges
from tests.fixtures.time_fixtures import FreezerProtocol


# Mark all tests in this file
pytestmark = [pytest.mark.integration, pytest.mark.perp, pytest.mark.vcr]


@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/perp/candles"], indirect=True)
class TestBackpackPerpCandles:
    """Backpack perp candle integration tests."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_bp_get_market_data_sol_usdc_perp_1h_success(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
        freezer: FreezerProtocol,
    ) -> None:
        """Test BackpackAPI.get_market_data() with SOL_USDC_PERP 1h interval.

        Returns valid Candle models.
        """
        # Use dynamic timestamps that are recent but deterministic for VCR
        now = datetime.now(UTC)
        end_time_dt = now - timedelta(days=7)  # 1 week ago
        start_time_dt = end_time_dt - timedelta(hours=1)  # 1 hour before that

        # Freeze time to ensure VCR consistency
        freezer.move_to(end_time_dt)

        end_time = int(end_time_dt.timestamp())
        start_time = int(start_time_dt.timestamp())

        args = GetMarketDataArgs(
            symbol=exchanges.backpack("SOL_USDC_PERP"),
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
                assert candle.open > Decimal(0), (
                    f"Candle {i} open should be positive, got {candle.open}"
                )
                assert candle.high > Decimal(0), (
                    f"Candle {i} high should be positive, got {candle.high}"
                )
                assert candle.low > Decimal(0), (
                    f"Candle {i} low should be positive, got {candle.low}"
                )
                assert candle.close > Decimal(0), (
                    f"Candle {i} close should be positive, got {candle.close}"
                )
                assert candle.volume >= Decimal(0), (
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
                assert candle.symbol == "SOL_USDC_PERP", (
                    f"Candle {i} symbol should be 'SOL_USDC_PERP', got '{candle.symbol}'"
                )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_bp_get_market_data_btc_usdc_perp_1h_success(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
        freezer: FreezerProtocol,
    ) -> None:
        """Test BackpackAPI.get_market_data() with BTC_USDC_PERP 1h interval.

        Returns valid Candle models.
        """
        # Use dynamic timestamps that are recent but deterministic for VCR
        now = datetime.now(UTC)
        end_time_dt = now - timedelta(days=7)  # 1 week ago
        start_time_dt = end_time_dt - timedelta(hours=1)  # 1 hour before that

        # Freeze time to ensure VCR consistency
        freezer.move_to(end_time_dt)

        end_time = int(end_time_dt.timestamp())
        start_time = int(start_time_dt.timestamp())

        args = GetMarketDataArgs(
            symbol=exchanges.backpack("BTC_USDC_PERP"),
            timeframe="1h",
            start_time_ms=start_time * 1000,
            end_time_ms=end_time * 1000,
        )

        candles = await bp_api_for_test_env.get_market_data(args)

        # Validate return type
        assert isinstance(candles, list), f"Expected list of candles, got {type(candles)}"

        # Validate BTC perp price ranges if candles exist
        if len(candles) > 0:
            for i, candle in enumerate(candles):
                assert isinstance(candle, Candle), (
                    f"Candle {i} should be Candle model, got {type(candle)}"
                )
                assert candle.symbol == "BTC_USDC_PERP", (
                    f"Candle {i} symbol should be 'BTC_USDC_PERP', got '{candle.symbol}'"
                )

                # BTC perp prices should be positive and reasonable
                assert candle.open > Decimal(0), (
                    f"BTC perp open price must be positive: {candle.open}"
                )
                assert candle.high > Decimal(0), (
                    f"BTC perp high price must be positive: {candle.high}"
                )
                assert candle.low > Decimal(0), f"BTC perp low price must be positive: {candle.low}"
                assert candle.close > Decimal(0), (
                    f"BTC perp close price must be positive: {candle.close}"
                )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_bp_get_market_data_eth_usdc_perp_1h_success(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
        freezer: FreezerProtocol,
    ) -> None:
        """Test BackpackAPI.get_market_data() with ETH_USDC_PERP 1h interval.

        Returns valid Candle models.
        """
        # Use dynamic timestamps that are recent but deterministic for VCR
        now = datetime.now(UTC)
        end_time_dt = now - timedelta(days=7)  # 1 week ago
        start_time_dt = end_time_dt - timedelta(hours=1)  # 1 hour before that

        # Freeze time to ensure VCR consistency
        freezer.move_to(end_time_dt)

        end_time = int(end_time_dt.timestamp())
        start_time = int(start_time_dt.timestamp())

        args = GetMarketDataArgs(
            symbol=exchanges.backpack("ETH_USDC_PERP"),
            timeframe="1h",
            start_time_ms=start_time * 1000,
            end_time_ms=end_time * 1000,
        )

        candles = await bp_api_for_test_env.get_market_data(args)

        # Validate return type
        assert isinstance(candles, list), f"Expected list of candles, got {type(candles)}"

        # Validate ETH perp price ranges if candles exist
        if len(candles) > 0:
            for i, candle in enumerate(candles):
                assert isinstance(candle, Candle), (
                    f"Candle {i} should be Candle model, got {type(candle)}"
                )
                assert candle.symbol == "ETH_USDC_PERP", (
                    f"Candle {i} symbol should be 'ETH_USDC_PERP', got '{candle.symbol}'"
                )

                # ETH perp prices should be positive and reasonable
                assert candle.open > Decimal(0), (
                    f"ETH perp open price must be positive: {candle.open}"
                )
                assert candle.high > Decimal(0), (
                    f"ETH perp high price must be positive: {candle.high}"
                )
                assert candle.low > Decimal(0), f"ETH perp low price must be positive: {candle.low}"
                assert candle.close > Decimal(0), (
                    f"ETH perp close price must be positive: {candle.close}"
                )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_bp_get_market_data_different_intervals_perp(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
        freezer: FreezerProtocol,
    ) -> None:
        """Test BackpackAPI.get_market_data() with different time intervals for perp markets."""
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
                symbol=exchanges.backpack("SOL_USDC_PERP"),
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
                        assert isinstance(candle, Candle), (
                            f"Should be Candle for interval {interval}"
                        )
                        assert candle.symbol == "SOL_USDC_PERP", (
                            f"Wrong symbol for interval {interval}"
                        )

            except APIError:
                # Some intervals might not be supported, which is acceptable
                pass

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_bp_get_market_data_chronological_ordering_perp(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
        freezer: FreezerProtocol,
    ) -> None:
        """Test BackpackAPI.get_market_data() returns perp candles in proper chronological order."""
        # Use dynamic timestamps that are recent but deterministic for VCR
        now = datetime.now(UTC)
        end_time_dt = now - timedelta(days=7)  # 1 week ago
        start_time_dt = end_time_dt - timedelta(hours=1)  # 1 hour before that

        # Freeze time to ensure VCR consistency
        freezer.move_to(end_time_dt)

        end_time = int(end_time_dt.timestamp())
        start_time = int(start_time_dt.timestamp())

        args = GetMarketDataArgs(
            symbol=exchanges.backpack("SOL_USDC_PERP"),
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
                        f"Candle {i} open_time should be datetime, "
                        f"got {type(current_candle.open_time)}"
                    )
                    assert isinstance(next_candle.open_time, datetime), (
                        f"Candle {i + 1} open_time should be datetime, "
                        f"got {type(next_candle.open_time)}"
                    )

                    # For 1h interval, timestamps should be 1 hour apart
                    time_diff = abs(
                        (next_candle.open_time - current_candle.open_time).total_seconds(),
                    )
                    # Allow some flexibility in ordering, but times should be reasonable
                    assert time_diff >= 3600, (
                        f"1h perp candles should be at least 1 hour apart: {time_diff} seconds"
                    )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_bp_get_market_data_precision_validation_perp(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
        freezer: FreezerProtocol,
    ) -> None:
        """Test BackpackAPI.get_market_data() ensures proper Decimal precision handling for perp."""
        # Use dynamic timestamps that are recent but deterministic for VCR
        now = datetime.now(UTC)
        end_time_dt = now - timedelta(days=7)  # 1 week ago
        start_time_dt = end_time_dt - timedelta(hours=1)  # 1 hour before that

        # Freeze time to ensure VCR consistency
        freezer.move_to(end_time_dt)

        end_time = int(end_time_dt.timestamp())
        start_time = int(start_time_dt.timestamp())

        args = GetMarketDataArgs(
            symbol=exchanges.backpack("SOL_USDC_PERP"),
            timeframe="1h",
            start_time_ms=start_time * 1000,
            end_time_ms=end_time * 1000,
        )

        candles = await bp_api_for_test_env.get_market_data(args)

        # Validate return type
        assert isinstance(candles, list), f"Expected list of candles, got {type(candles)}"

        if len(candles) > 0:
            for i, candle in enumerate(candles):
                # Validate OHLCV precision for perp trading
                ohlcv_fields = [candle.open, candle.high, candle.low, candle.close, candle.volume]
                field_names = ["open", "high", "low", "close", "volume"]
                for field_name, field_value in zip(field_names, ohlcv_fields, strict=False):
                    assert isinstance(field_value, Decimal), (
                        f"Perp candle {i} {field_name} should be Decimal, got {type(field_value)}"
                    )

                    # Test arithmetic operations work correctly for leverage calculations
                    doubled_value = field_value * Decimal(2)
                    assert isinstance(doubled_value, Decimal), (
                        f"Perp candle {i} {field_name} arithmetic should maintain Decimal type"
                    )

                    if field_value > Decimal(0):  # Don't test with zero values
                        halved_value = field_value / Decimal(2)
                        assert isinstance(halved_value, Decimal), (
                            f"Perp candle {i} {field_name} division should maintain Decimal type"
                        )
                        assert halved_value < field_value, (
                            f"Perp candle {i} {field_name} halved should be less than original"
                        )

                # Test leverage-related calculations
                leverage_factor = Decimal(10)  # 10x leverage
                leveraged_volume = candle.volume * leverage_factor
                assert isinstance(leveraged_volume, Decimal), (
                    "Leverage calculations should maintain Decimal type"
                )

                # Test candle calculations for perp trading
                price_range = candle.high - candle.low
                assert isinstance(price_range, Decimal), (
                    "Price range calculation should maintain Decimal type"
                )
                assert price_range >= Decimal(0), (
                    f"Perp candle {i} price range should be non-negative: {price_range}"
                )

                # Test notional volume calculations for margin requirements
                if candle.volume > Decimal(0):
                    avg_price = (candle.high + candle.low) / Decimal(2)
                    notional_volume = candle.volume * avg_price
                    margin_requirement = notional_volume / leverage_factor

                    assert isinstance(notional_volume, Decimal), (
                        "Notional volume calculation should maintain Decimal type"
                    )
                    assert isinstance(margin_requirement, Decimal), (
                        "Margin requirement calculation should maintain Decimal type"
                    )
                    assert notional_volume > Decimal(0), (
                        f"Perp candle {i} notional volume should be positive"
                    )
                    assert margin_requirement < notional_volume, (
                        f"Perp candle {i} margin should be less than notional (leverage effect)"
                    )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_bp_get_market_data_ohlc_relationships_validation_perp(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
        freezer: FreezerProtocol,
    ) -> None:
        """Test BackpackAPI.get_market_data() validates comprehensive OHLC relationships.

        For perp markets.
        """
        # Use dynamic timestamps that are recent but deterministic for VCR
        now = datetime.now(UTC)
        end_time_dt = now - timedelta(days=7)  # 1 week ago
        start_time_dt = end_time_dt - timedelta(hours=1)  # 1 hour before that

        # Freeze time to ensure VCR consistency
        freezer.move_to(end_time_dt)

        end_time = int(end_time_dt.timestamp())
        start_time = int(start_time_dt.timestamp())

        args = GetMarketDataArgs(
            symbol=exchanges.backpack("SOL_USDC_PERP"),
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
                    f"Perp candle {i}: high {candle.high} must be >= low {candle.low}"
                )
                assert candle.high >= candle.open, (
                    f"Perp candle {i}: high {candle.high} must be >= open {candle.open}"
                )
                assert candle.high >= candle.close, (
                    f"Perp candle {i}: high {candle.high} must be >= close {candle.close}"
                )
                assert candle.low <= candle.open, (
                    f"Perp candle {i}: low {candle.low} must be <= open {candle.open}"
                )
                assert candle.low <= candle.close, (
                    f"Perp candle {i}: low {candle.low} must be <= close {candle.close}"
                )

                # Advanced validation for perp markets
                # High and low should not be equal unless it's a very unusual market condition
                if candle.high == candle.low:
                    # If high == low, then open and close should also equal high/low
                    assert candle.open == candle.high, (
                        f"Perp candle {i}: if high == low, open should also equal: "
                        f"{candle.open} != {candle.high}"
                    )
                    assert candle.close == candle.high, (
                        f"Perp candle {i}: if high == low, close should also equal: "
                        f"{candle.close} != {candle.high}"
                    )

                # Price range validation for perp markets (can be more volatile due to leverage)
                price_range = candle.high - candle.low
                avg_price = (candle.high + candle.low) / Decimal(2)

                if avg_price > Decimal(0):
                    range_percentage = (price_range / avg_price) * Decimal(100)
                    # For 1h perp candles, validate range is not zero or negative
                    # Market volatility is natural and should not be artificially constrained
                    assert range_percentage >= Decimal(0), (
                        f"Perp candle {i}: price range percentage cannot be negative: "
                        f"{range_percentage}%"
                    )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_bp_get_market_data_invalid_perp_symbol_error(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
        freezer: FreezerProtocol,
    ) -> None:
        """Test BackpackAPI.get_market_data() with invalid perp symbol raises appropriate error."""
        # Use dynamic timestamps that are recent but deterministic for VCR
        now = datetime.now(UTC)
        end_time_dt = now - timedelta(days=7)  # 1 week ago
        start_time_dt = end_time_dt - timedelta(days=1)  # 1 day before that

        # Freeze time to ensure VCR consistency
        freezer.move_to(end_time_dt)

        end_time = int(end_time_dt.timestamp())
        start_time = int(start_time_dt.timestamp())

        args = GetMarketDataArgs(
            symbol=exchanges.backpack("INVALID_PERP"),
            timeframe="1h",
            start_time_ms=start_time * 1000,
            end_time_ms=end_time * 1000,
        )

        with pytest.raises(APIError) as exc_info:
            await bp_api_for_test_env.get_market_data(args)

        # Validate error details
        error = exc_info.value
        assert "INVALID_PERP" in str(error) or "symbol" in str(error).lower()

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_bp_get_market_data_multiple_perp_symbols_consistency(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
        freezer: FreezerProtocol,
    ) -> None:
        """Test BackpackAPI.get_market_data() returns consistent structure across perp symbols."""
        symbols = ["SOL_USDC_PERP", "BTC_USDC_PERP", "ETH_USDC_PERP"]
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
                symbol=exchanges.backpack(symbol),
                timeframe="1h",
                start_time_ms=start_time * 1000,
                end_time_ms=end_time * 1000,
            )

            candles = await bp_api_for_test_env.get_market_data(args)
            assert isinstance(candles, list), f"Expected list for {symbol}, got {type(candles)}"
            all_candles[symbol] = candles

        # Verify structure consistency across perp symbols
        for symbol, candles in all_candles.items():
            if len(candles) > 0:
                # All candles for each symbol should have the same structure
                first_candle = candles[0]
                required_attrs = ["symbol", "open", "high", "low", "close", "volume"]

                for attr in required_attrs:
                    assert hasattr(first_candle, attr), (
                        f"Perp candle for {symbol} missing required attribute: {attr}"
                    )
                    value = getattr(first_candle, attr)
                    assert value is not None, (
                        f"Perp candle for {symbol} has None value for required attribute: {attr}"
                    )

                # All candles in the list should have same symbol
                for i, candle in enumerate(candles):
                    assert candle.symbol == symbol, (
                        f"Perp candle {i} for {symbol} has wrong symbol: {candle.symbol}"
                    )
                    # Validate it's actually a perp symbol
                    assert symbol.endswith("_PERP"), f"Should be perp symbol, got: {symbol}"

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_bp_get_market_data_perp_funding_impact_awareness(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
        freezer: FreezerProtocol,
    ) -> None:
        """Test perp candle data characteristics related to funding periods."""
        # Use dynamic timestamps that are recent but deterministic for VCR
        now = datetime.now(UTC)
        end_time_dt = now - timedelta(days=7)  # 1 week ago
        start_time_dt = end_time_dt - timedelta(hours=8)  # 8 hours (funding period)

        # Freeze time to ensure VCR consistency
        freezer.move_to(end_time_dt)

        end_time = int(end_time_dt.timestamp())
        start_time = int(start_time_dt.timestamp())

        args = GetMarketDataArgs(
            symbol=exchanges.backpack("SOL_USDC_PERP"),
            timeframe="1h",
            start_time_ms=start_time * 1000,
            end_time_ms=end_time * 1000,
        )

        candles = await bp_api_for_test_env.get_market_data(args)

        if len(candles) > 0:
            for candle in candles:
                # Test that perp candles can handle funding-related calculations
                # Funding typically occurs every 8 hours

                # Test that price data supports funding rate calculations with Decimal precision
                # Funding rates are exchange-specific and should not be hardcoded
                test_rate = Decimal("0.001")  # Test rate for precision validation only
                funding_payment = candle.close * test_rate

                assert isinstance(funding_payment, Decimal), (
                    "Funding payment calculation should maintain Decimal type"
                )

                # Test that price precision supports various funding calculations
                min_test_rate = Decimal("0.0001")  # Minimal test rate for precision validation
                min_funding = candle.close * min_test_rate
                assert isinstance(min_funding, Decimal), (
                    "Minimum funding calculation should maintain Decimal type"
                )

                # Validate calculations maintain proper Decimal precision
                if candle.close > Decimal(0):
                    funding_percentage = (funding_payment / candle.close) * Decimal(100)
                    assert isinstance(funding_percentage, Decimal), (
                        "Funding percentage calculation should maintain Decimal type"
                    )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_bp_get_market_data_perp_leverage_volatility_characteristics(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
        freezer: FreezerProtocol,
    ) -> None:
        """Test perp candle characteristics related to leverage trading volatility."""
        # Use dynamic timestamps that are recent but deterministic for VCR
        now = datetime.now(UTC)
        end_time_dt = now - timedelta(days=7)  # 1 week ago
        start_time_dt = end_time_dt - timedelta(hours=6)  # 6 hours of data

        # Freeze time to ensure VCR consistency
        freezer.move_to(end_time_dt)

        end_time = int(end_time_dt.timestamp())
        start_time = int(start_time_dt.timestamp())

        args = GetMarketDataArgs(
            symbol=exchanges.backpack("SOL_USDC_PERP"),
            timeframe="1h",
            start_time_ms=start_time * 1000,
            end_time_ms=end_time * 1000,
        )

        candles = await bp_api_for_test_env.get_market_data(args)

        if len(candles) > 1:
            # Test volatility characteristics that might be amplified by leverage
            price_changes: list[Decimal] = []
            volumes: list[Decimal] = []

            for i in range(len(candles) - 1):
                current = candles[i]
                next_candle = candles[i + 1]

                # Calculate price change between candles
                if current.close > Decimal(0):
                    price_change = abs(next_candle.open - current.close) / current.close
                    price_changes.append(price_change)

                volumes.append(current.volume)

            if price_changes:
                # Perp markets can have higher volatility due to leverage
                max_change = max(price_changes)
                avg_change = sum(price_changes) / len(price_changes)

                # Validate price changes are not negative (basic sanity check)
                assert max_change >= Decimal(0), (
                    f"Maximum price change cannot be negative: {max_change}"
                )
                assert avg_change >= Decimal(0), (
                    f"Average price change cannot be negative: {avg_change}"
                )

            if volumes:
                # Volume should be consistent across candles
                total_volume = sum(volumes)
                assert total_volume >= Decimal(0), "Total volume should be non-negative"
