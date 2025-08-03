"""Integration tests for Backpack spot trade model pipeline.

These tests validate the complete data pipeline from BackpackAPI.get_recent_trades() calls
to final Trade internal domain models using pytest-recording (VCR) for deterministic tests.

Tests cover spot markets only:
- Successful trades retrieval for valid spot symbols (SOL_USDC, BTC_USDC, ETH_USDC)
- Trade data validation (price, quantity, executed_at, side)
- Chronological ordering validation
- Edge cases and error handling
- Complete API -> Service -> Handler -> Mapper -> Internal Model pipeline
"""

from datetime import datetime
from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.common import APIError
from cyberdelta.enums import OrderSide
from cyberdelta.models import Trade
from cyberdelta.core.symbols.models import Symbol
from tests.common_symbols import COMMON_SPOT_SYMBOLS_BP, SOL_USDC_BP, BTC_USDC_BP, ETH_USDC_BP
from cyberdelta.core.symbols import exchanges


# Mark all tests in this file
pytestmark = [pytest.mark.integration, pytest.mark.spot, pytest.mark.vcr]


class TestBackpackSpotTrades:
    """Backpack spot trade integration tests."""

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir",
        ["apis/backpack/spot/trades"],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_get_sol_usdc_recent_trades_success(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test BackpackAPI.get_recent_trades() with SOL_USDC returns valid Trade models."""
        trades = await bp_api_for_test_env.get_recent_trades(SOL_USDC_BP, limit=10)

        assert isinstance(trades, list), f"Expected list of trades, got {type(trades)}"

        if len(trades) > 0:
            for i, trade in enumerate(trades):
                assert isinstance(trade, Trade), (
                    f"Trade {i} should be Trade model, got {type(trade)}"
                )

                assert hasattr(trade, "symbol"), f"Trade {i} should have symbol attribute"
                assert hasattr(trade, "price"), f"Trade {i} should have price attribute"
                assert hasattr(trade, "quantity"), f"Trade {i} should have quantity attribute"
                assert hasattr(trade, "executed_at"), f"Trade {i} should have executed_at attribute"

                assert isinstance(trade.price, Decimal), (
                    f"Trade {i} price should be Decimal, got {type(trade.price)}"
                )
                assert isinstance(trade.quantity, Decimal), (
                    f"Trade {i} quantity should be Decimal, got {type(trade.quantity)}"
                )

                assert trade.price > Decimal(0), (
                    f"Trade {i} price should be positive, got {trade.price}"
                )
                assert trade.quantity > Decimal(0), (
                    f"Trade {i} quantity should be positive, got {trade.quantity}"
                )

                assert trade.symbol == SOL_USDC_BP, (
                    f"Trade {i} symbol should be SOL_USDC_BP, got '{trade.symbol}'"
                )

                assert isinstance(trade.executed_at, datetime), (
                    f"Trade {i} executed_at should be datetime, got {type(trade.executed_at)}"
                )

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir",
        ["apis/backpack/spot/trades"],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_get_btc_usdc_recent_trades_success(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test BackpackAPI.get_recent_trades() with BTC_USDC returns valid Trade models."""
        trades = await bp_api_for_test_env.get_recent_trades(BTC_USDC_BP, limit=5)

        assert isinstance(trades, list), f"Expected list of trades, got {type(trades)}"

        if len(trades) > 0:
            for i, trade in enumerate(trades):
                assert isinstance(trade, Trade), (
                    f"Trade {i} should be Trade model, got {type(trade)}"
                )
                assert trade.symbol == BTC_USDC_BP, (
                    f"Trade {i} symbol should be BTC_USDC_BP, got '{trade.symbol}'"
                )

                # BTC prices should be in reasonable range
                assert trade.price > Decimal(1000), f"BTC trade price seems too low: {trade.price}"
                assert trade.price < Decimal(1000000), (
                    f"BTC trade price seems too high: {trade.price}"
                )

    @pytest.mark.parametrize("symbol", [s.value for s in COMMON_SPOT_SYMBOLS_BP])
    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir",
        ["apis/backpack/spot/trades"],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_spot_trade_chronological_ordering(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
        symbol: Symbol,
    ) -> None:
        """Test spot trade chronological ordering across symbols."""
        trades = await bp_api_for_test_env.get_recent_trades(symbol, limit=20)

        assert isinstance(trades, list), f"Expected list for {symbol}"

        if len(trades) > 1:
            for i in range(len(trades) - 1):
                current_trade = trades[i]
                next_trade = trades[i + 1]

                if (
                    hasattr(current_trade, "executed_at") and hasattr(next_trade, "executed_at")
                ) and (current_trade.executed_at and next_trade.executed_at):
                    # Trades should be in reverse chronological order (newest first)
                    assert current_trade.executed_at >= next_trade.executed_at, (
                        f"Trades not in reverse chronological order for {symbol}"
                    )

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir",
        ["apis/backpack/spot/trades"],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_spot_trade_side_validation(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test spot trade side validation and distribution."""
        trades = await bp_api_for_test_env.get_recent_trades(SOL_USDC_BP, limit=50)

        if len(trades) > 0:
            buy_trades = 0
            sell_trades = 0

            for trade in trades:
                assert trade.side in [OrderSide.BUY, OrderSide.SELL], (
                    f"Trade side should be BUY or SELL, got {trade.side}"
                )

                if trade.side == OrderSide.BUY:
                    buy_trades += 1
                else:
                    sell_trades += 1

            # In active markets, we expect both buy and sell trades
            total_sided_trades = buy_trades + sell_trades
            if total_sided_trades > 10:  # Only check if we have enough trades
                assert buy_trades > 0, "Should have some buy trades in active market"
                assert sell_trades > 0, "Should have some sell trades in active market"

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir",
        ["apis/backpack/spot/trades"],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_spot_trade_volume_analysis(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test spot trade volume and size distribution."""
        trades = await bp_api_for_test_env.get_recent_trades(SOL_USDC_BP, limit=30)

        if len(trades) > 0:
            total_volume = Decimal(0)
            trade_sizes: list[Decimal] = []

            for trade in trades:
                volume = trade.price * trade.quantity
                total_volume += volume
                trade_sizes.append(trade.quantity)

            assert total_volume > Decimal(0), "Total volume should be positive"

            # Check trade size distribution
            if len(trade_sizes) > 5:
                min_size = min(trade_sizes)
                max_size = max(trade_sizes)
                assert min_size > Decimal(0), "Minimum trade size should be positive"
                assert max_size >= min_size, "Maximum should be >= minimum"

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir",
        ["apis/backpack/spot/trades"],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_get_recent_trades_invalid_spot_symbol_error(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test BackpackAPI.get_recent_trades() with invalid spot symbol.

        Raises appropriate error.
        """
        with pytest.raises(APIError) as exc_info:
            await bp_api_for_test_env.get_recent_trades(exchanges.backpack("INVALID_SPOT_SYMBOL"), limit=5)

        error = exc_info.value
        assert "INVALID_SPOT_SYMBOL" in str(error) or "symbol" in str(error).lower()

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir",
        ["apis/backpack/spot/trades"],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_spot_trade_precision_validation(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test spot trade decimal precision handling."""
        trades = await bp_api_for_test_env.get_recent_trades(SOL_USDC_BP, limit=10)

        if len(trades) > 0:
            for trade in trades:
                assert isinstance(trade.price, Decimal), "Price should be Decimal type"
                assert isinstance(trade.quantity, Decimal), "Quantity should be Decimal type"

                # Test arithmetic operations
                volume = trade.price * trade.quantity
                assert isinstance(volume, Decimal), (
                    "Volume calculation should maintain Decimal type"
                )
                assert volume > Decimal(0), "Volume should be positive"

                # Test precision preservation
                doubled_price = trade.price * Decimal(2)
                assert isinstance(doubled_price, Decimal), (
                    "Price arithmetic should maintain Decimal type"
                )

    @pytest.mark.parametrize("limit", [1, 5, 10, 50])
    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir",
        ["apis/backpack/spot/trades"],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_spot_trade_limit_parameter(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
        limit: int,
    ) -> None:
        """Test spot trade limit parameter functionality."""
        trades = await bp_api_for_test_env.get_recent_trades(SOL_USDC_BP, limit=limit)

        assert isinstance(trades, list), "Should return list"
        assert len(trades) <= limit, f"Should not exceed requested limit of {limit}"

        # For very active markets, we often get the requested number of trades
        if len(trades) > 0:
            assert len(trades) >= min(1, limit), "Should have at least one trade if any exist"
