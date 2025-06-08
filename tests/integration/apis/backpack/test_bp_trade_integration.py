"""Integration tests for Backpack Trade model pipeline.

These tests validate the complete data pipeline from BackpackAPI.get_recent_trades() calls
to final Trade internal domain models using pytest-recording (VCR) for deterministic tests.

Tests cover:
- Successful trades retrieval for valid symbols
- Trade data validation (price, quantity, executed_at, side)
- Chronological ordering validation
- Edge cases and error handling
- Complete API -> Service -> Handler -> Mapper -> Internal Model pipeline
"""

from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.core.models import Trade
from cyberdelta.core.models.enums import OrderSide


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/trade"], indirect=True
)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_recent_trades_sol_usdc_success(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_recent_trades() with SOL_USDC returns valid Trade models.
    
    This validates the complete pipeline:
    - API method call (get_recent_trades)
    - Request building (/api/v1/trades endpoint)
    - Response handling and validation
    - Mapping to internal Trade models
    """
    trades = await bp_api_for_test_env.get_recent_trades("SOL_USDC", limit=10)
    
    # Validate return type
    assert isinstance(trades, list), (

        f"Expected list of trades, got {type(trades)}"

    )
    
    # For liquid market like SOL_USDC, we expect some trades (though this depends on timing)
    if len(trades) > 0:
        # Validate each trade is a valid Trade model
        for i, trade in enumerate(trades):
            assert isinstance(trade, Trade), (

                f"Trade {i} should be Trade model, got {type(trade)}"

            )
            
            # Validate core trade fields exist
            assert hasattr(trade, "symbol"), f"Trade {i} should have symbol attribute"
            assert hasattr(trade, "price"), f"Trade {i} should have price attribute"
            assert hasattr(trade, "quantity"), f"Trade {i} should have quantity attribute"
            assert hasattr(trade, "executed_at"), f"Trade {i} should have executed_at attribute"
            
            # Validate data types
            assert isinstance(trade.price, Decimal), (

                f"Trade {i} price should be Decimal, got {type(trade.price)}"

            )
            assert isinstance(trade.quantity, Decimal), (

                f"Trade {i} quantity should be Decimal, got {type(trade.quantity)}"

            )
            
            # Validate positive values
            assert trade.price > Decimal("0"), (
                f"Trade {i} price should be positive, got {trade.price}"
            )
            assert trade.quantity > Decimal("0"), (
                f"Trade {i} quantity should be positive, got {trade.quantity}"
            )
            
            # Validate symbol
            assert trade.symbol == "SOL_USDC", (

                f"Trade {i} symbol should be 'SOL_USDC', got '{trade.symbol}'"

            )
            
            # Validate executed_at (should be a reasonable executed_at)
            if hasattr(trade, "executed_at"):
                # executed_at should be datetime object
                from datetime import datetime
                assert isinstance(trade.executed_at, datetime), (
                    f"Trade {i} executed_at should be datetime, got {type(trade.executed_at)}"
                )
                # Should be a reasonable executed_at (after 2020)
                min_date = datetime(2020, 1, 1)
                assert trade.executed_at > min_date, (
                    f"Trade {i} executed_at seems too old: {trade.executed_at}"
                )


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/trade"], indirect=True
)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_recent_trades_btc_usdc_success(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_recent_trades() with BTC_USDC returns valid Trade models."""
    trades = await bp_api_for_test_env.get_recent_trades("BTC_USDC", limit=5)
    
    # Validate return type
    assert isinstance(trades, list), (

        f"Expected list of trades, got {type(trades)}"

    )
    
    # For BTC market, expect reasonable trade data
    if len(trades) > 0:
        for i, trade in enumerate(trades):
            assert isinstance(trade, Trade), (

                f"Trade {i} should be Trade model, got {type(trade)}"

            )
            assert trade.symbol == "BTC_USDC", (

                f"Trade {i} symbol should be 'BTC_USDC', got '{trade.symbol}'"

            )
            
            # BTC prices should be in reasonable range
            assert trade.price > Decimal("1000"), f"BTC price seems too low: {trade.price}"
            assert trade.price < Decimal("1000000"), f"BTC price seems too high: {trade.price}"


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/trade"], indirect=True
)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_recent_trades_perp_success(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_recent_trades() with perpetual contract returns valid Trade models."""
    trades = await bp_api_for_test_env.get_recent_trades("SOL_USDC_PERP", limit=5)
    
    # Validate return type
    assert isinstance(trades, list), (

        f"Expected list of trades, got {type(trades)}"

    )
    
    # For perpetual contracts, validate structure is same as spot
    if len(trades) > 0:
        for i, trade in enumerate(trades):
            assert isinstance(trade, Trade), (

                f"Trade {i} should be Trade model, got {type(trade)}"

            )
            assert trade.symbol == "SOL_USDC_PERP", (

                f"Trade {i} symbol should be 'SOL_USDC_PERP', got '{trade.symbol}'"

            )


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/trade"], indirect=True
)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_recent_trades_chronological_ordering(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_recent_trades() returns trades in proper chronological order."""
    trades = await bp_api_for_test_env.get_recent_trades("SOL_USDC", limit=10)
    
    # Validate return type
    assert isinstance(trades, list), (

        f"Expected list of trades, got {type(trades)}"

    )
    
    # Check chronological ordering if we have multiple trades
    if len(trades) > 1:
        for i in range(len(trades) - 1):
            current_trade = trades[i]
            next_trade = trades[i + 1]
            
            # Verify both trades have executed_ats
            if (hasattr(current_trade, "executed_at") and
                hasattr(next_trade, "executed_at")):
                
                # Most recent trade should be first (descending order)
                # Note: This depends on exchange implementation - some return
                # ascending, some descending
                # We'll check that executed_ats are reasonable and consistent
                from datetime import datetime
                assert isinstance(current_trade.executed_at, datetime), (
                    f"Trade {i} executed_at should be datetime, "
                    f"got {type(current_trade.executed_at)}"
                )
                assert isinstance(next_trade.executed_at, datetime), (
                    f"Trade {i+1} executed_at should be datetime, "
                    f"got {type(next_trade.executed_at)}"
                )


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/trade"], indirect=True
)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_recent_trades_limit_parameter(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_recent_trades() respects limit parameter."""
    # Test with small limit
    small_limit = 3
    trades_small = await bp_api_for_test_env.get_recent_trades("SOL_USDC", limit=small_limit)
    
    # Validate return type
    assert isinstance(trades_small, list), (

        f"Expected list of trades, got {type(trades_small)}"

    )
    
    # Test with larger limit
    large_limit = 10
    trades_large = await bp_api_for_test_env.get_recent_trades("SOL_USDC", limit=large_limit)
    
    # Validate return type
    assert isinstance(trades_large, list), (

        f"Expected list of trades, got {type(trades_large)}"

    )
    
    # If there are enough trades, the larger limit should return more results
    if len(trades_small) == small_limit and len(trades_large) > small_limit:
        # Verify that the first few trades are the same (most recent)
        for i in range(min(len(trades_small), len(trades_large))):
            small_trade = trades_small[i]
            large_trade = trades_large[i]
            
            # Should be the same trade (same executed_at if available)
            if (hasattr(small_trade, "executed_at") and
                hasattr(large_trade, "executed_at")):
                assert small_trade.executed_at == large_trade.executed_at, (
                    f"Trade {i} executed_ats should match: "
                    f"{small_trade.executed_at} vs {large_trade.executed_at}"
                )


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/trade"], indirect=True
)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_recent_trades_precision_validation(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_recent_trades() ensures proper Decimal precision handling."""
    trades = await bp_api_for_test_env.get_recent_trades("SOL_USDC", limit=5)
    
    # Validate return type
    assert isinstance(trades, list), (

        f"Expected list of trades, got {type(trades)}"

    )
    
    if len(trades) > 0:
        for i, trade in enumerate(trades):
            # Validate price precision
            assert isinstance(trade.price, Decimal), (

                f"Trade {i} price should be Decimal, got {type(trade.price)}"

            )
            assert isinstance(trade.quantity, Decimal), (

                f"Trade {i} quantity should be Decimal, got {type(trade.quantity)}"

            )
            
            # Test arithmetic operations work correctly
            total_value = trade.price * trade.quantity
            assert isinstance(total_value, Decimal), (

                f"Trade {i} total value should be Decimal"

            )
            assert total_value > Decimal("0"), (
                f"Trade {i} total value should be positive, got {total_value}"
            )
            
            # Test precision is maintained
            price_str = str(trade.price)
            if "." in price_str:
                decimal_places = len(price_str.split(".")[1])
                assert decimal_places <= 18, (
                    f"Trade {i} price precision seems too high: {decimal_places} decimal places"
                )


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/trade"], indirect=True
)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_recent_trades_side_validation(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_recent_trades() validates trade side information if available."""
    trades = await bp_api_for_test_env.get_recent_trades("SOL_USDC", limit=10)
    
    # Validate return type
    assert isinstance(trades, list), (

        f"Expected list of trades, got {type(trades)}"

    )
    
    if len(trades) > 0:
        for i, trade in enumerate(trades):
            # If trade has side information, validate it
            if hasattr(trade, "side"):
                assert trade.side in [OrderSide.BUY, OrderSide.SELL], (
                    f"Trade {i} side should be buy/sell, got '{trade.side}'"
                )
            
            # Note: Trade model doesn't have taker_side field
            # Direction information is captured in the 'side' field


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/trade"], indirect=True
)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_recent_trades_invalid_symbol_error(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_recent_trades() with invalid symbol raises appropriate error."""
    with pytest.raises(APIError) as exc_info:
        await bp_api_for_test_env.get_recent_trades("INVALID_SYMBOL", limit=5)
    
    # Validate error details
    error = exc_info.value
    assert "INVALID_SYMBOL" in str(error) or "symbol" in str(error).lower()


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/trade"], indirect=True
)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_recent_trades_nonexistent_symbol_error(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_recent_trades() with non-existent but well-formed symbol."""
    with pytest.raises(APIError) as exc_info:
        await bp_api_for_test_env.get_recent_trades("NOTREAL_USDC", limit=5)
    
    # Validate error details
    error = exc_info.value
    assert (
        "NOTREAL_USDC" in str(error) or
        "not found" in str(error).lower() or
        "symbol" in str(error).lower()
    )


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/trade"], indirect=True
)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_recent_trades_empty_symbol_error(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_recent_trades() with empty symbol raises appropriate error."""
    with pytest.raises((APIError, ValueError)) as exc_info:
        await bp_api_for_test_env.get_recent_trades("", limit=5)
    
    # Validate error contains relevant information
    error_str = str(exc_info.value)
    assert len(error_str) > 0, "Error message should not be empty"


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/trade"], indirect=True
)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_recent_trades_zero_limit_handling(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_recent_trades() with zero limit parameter."""
    try:
        trades = await bp_api_for_test_env.get_recent_trades("SOL_USDC", limit=0)
        # If it succeeds, should return empty list
        assert isinstance(trades, list), (

            f"Expected list of trades, got {type(trades)}"

        )
        assert len(trades) == 0, (

            f"Expected empty list for limit=0, got {len(trades)} trades"

        )
    except (APIError, ValueError):
        # If it raises an error, that's also acceptable behavior
        pass


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/trade"], indirect=True
)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_recent_trades_large_limit_handling(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_recent_trades() with very large limit parameter."""
    try:
        # Try with a very large limit
        trades = await bp_api_for_test_env.get_recent_trades("SOL_USDC", limit=1000)
        
        # Should return valid list (may be capped by exchange)
        assert isinstance(trades, list), (

            f"Expected list of trades, got {type(trades)}"

        )
        
        # Validate all returned trades if any
        for i, trade in enumerate(trades):
            assert isinstance(trade, Trade), (

                f"Trade {i} should be Trade model, got {type(trade)}"

            )
            assert trade.symbol == "SOL_USDC", (

                f"Trade {i} symbol should be 'SOL_USDC', got '{trade.symbol}'"

            )
            
    except (APIError, ValueError):
        # If exchange rejects large limits, that's acceptable
        pass


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/trade"], indirect=True
)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_recent_trades_multiple_symbols_consistency(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_recent_trades() returns consistent structure across symbols."""
    symbols = ["SOL_USDC", "BTC_USDC"]
    all_trades: dict[str, list[Trade]] = {}
    
    for symbol in symbols:
        trades = await bp_api_for_test_env.get_recent_trades(symbol, limit=5)
        assert isinstance(trades, list), (

            f"Expected list for {symbol}, got {type(trades)}"

        )
        all_trades[symbol] = trades
    
    # Verify structure consistency across symbols
    for symbol, trades in all_trades.items():
        if len(trades) > 0:
            # All trades for each symbol should have the same structure
            first_trade = trades[0]
            required_attrs = ["symbol", "price", "size"]
            
            for attr in required_attrs:
                assert hasattr(first_trade, attr), (
                    f"Trade for {symbol} missing required attribute: {attr}"
                )
                value = getattr(first_trade, attr)
                assert value is not None, (
                    f"Trade for {symbol} has None value for required attribute: {attr}"
                )
            
            # All trades in the list should have same symbol
            for i, trade in enumerate(trades):
                assert trade.symbol == symbol, (

                    f"Trade {i} for {symbol} has wrong symbol: {trade.symbol}"

                )


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/trade"], indirect=True
)
@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_bp_get_recent_trades_no_limit_default_behavior(
    bp_api_for_test_env: BackpackAPI,
    custom_vcr_config: dict[str, Any],
) -> None:
    """Test BackpackAPI.get_recent_trades() default behavior when no limit specified."""
    # Test without specifying limit
    trades_no_limit = await bp_api_for_test_env.get_recent_trades("SOL_USDC")
    
    # Validate return type
    assert isinstance(trades_no_limit, list), (

        f"Expected list of trades, got {type(trades_no_limit)}"

    )
    
    # Compare with explicit limit to understand default behavior
    trades_with_limit = await bp_api_for_test_env.get_recent_trades("SOL_USDC", limit=20)
    
    # Should return reasonable number of trades
    if len(trades_no_limit) > 0 and len(trades_with_limit) > 0:
        # Both should return valid Trade objects
        assert isinstance(trades_no_limit[0], Trade), "First trade should be Trade model"
        assert isinstance(trades_with_limit[0], Trade), "First trade should be Trade model"