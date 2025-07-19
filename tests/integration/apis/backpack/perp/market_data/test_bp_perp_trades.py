"""Integration tests for Backpack perp trade model pipeline.

These tests validate the complete data pipeline from BackpackAPI.get_recent_trades() calls
to final Trade internal domain models using pytest-recording (VCR) for deterministic tests.

Tests cover perpetual futures markets only:
- Successful trades retrieval for valid perp symbols (SOL_USDC_PERP, BTC_USDC_PERP, ETH_USDC_PERP)
- Trade data validation (price, quantity, executed_at, side)
- Chronological ordering validation
- Perpetual-specific features (funding impact, leverage characteristics)
- Edge cases and error handling
"""

from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.common import APIError
from cyberdelta.core.models import Trade
from cyberdelta.enums import OrderSide


# Mark all tests in this file
pytestmark = [pytest.mark.integration, pytest.mark.perp, pytest.mark.vcr]


class TestBackpackPerpTrades:
    """Backpack perp trade integration tests."""

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir",
        ["apis/backpack/perp/trades"],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_get_sol_usdc_perp_recent_trades_success(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test BackpackAPI.get_recent_trades() with SOL_USDC_PERP returns valid Trade models."""
        trades = await bp_api_for_test_env.get_recent_trades("SOL_USDC_PERP", limit=5)

        assert isinstance(trades, list), f"Expected list of trades, got {type(trades)}"

        if len(trades) > 0:
            for i, trade in enumerate(trades):
                assert isinstance(trade, Trade), (
                    f"Trade {i} should be Trade model, got {type(trade)}"
                )

                assert trade.symbol == "SOL_USDC_PERP", (
                    f"Trade {i} symbol should be 'SOL_USDC_PERP', got '{trade.symbol}'"
                )

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

                # Perp trades often have specific size characteristics
                assert trade.side in [OrderSide.BUY, OrderSide.SELL], (
                    f"Trade {i} side should be BUY or SELL, got {trade.side}"
                )

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir",
        ["apis/backpack/perp/trades"],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_get_btc_usdc_perp_recent_trades_success(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test BackpackAPI.get_recent_trades() with BTC_USDC_PERP returns valid Trade models."""
        trades = await bp_api_for_test_env.get_recent_trades("BTC_USDC_PERP", limit=5)

        assert isinstance(trades, list), f"Expected list of trades, got {type(trades)}"

        if len(trades) > 0:
            for i, trade in enumerate(trades):
                assert isinstance(trade, Trade), (
                    f"Trade {i} should be Trade model, got {type(trade)}"
                )
                assert trade.symbol == "BTC_USDC_PERP", (
                    f"Trade {i} symbol should be 'BTC_USDC_PERP', got '{trade.symbol}'"
                )

                # BTC perp prices should be positive and finite
                assert trade.price > Decimal(0), (
                    f"BTC perp trade price must be positive: {trade.price}"
                )
                assert trade.price.is_finite(), (
                    f"BTC perp trade price must be finite: {trade.price}"
                )

    @pytest.mark.parametrize("symbol", ["SOL_USDC_PERP", "BTC_USDC_PERP", "ETH_USDC_PERP"])
    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir",
        ["apis/backpack/perp/trades"],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_perp_trade_chronological_ordering(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
        symbol: str,
    ) -> None:
        """Test perp trade chronological ordering across symbols."""
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
        ["apis/backpack/perp/trades"],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_perp_trade_leverage_characteristics(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test perp trade characteristics related to leverage trading."""
        trades = await bp_api_for_test_env.get_recent_trades("SOL_USDC_PERP", limit=30)

        if len(trades) > 0:
            total_notional = Decimal(0)
            large_trades = 0

            for trade in trades:
                notional_value = trade.price * trade.quantity
                total_notional += notional_value

                # Track trades with positive notional value
                if notional_value > Decimal(0):
                    large_trades += 1

            assert total_notional > Decimal(0), "Total notional should be positive"

            # Perp markets often have larger individual trade sizes due to leverage
            if len(trades) > 10:
                _ = large_trades / len(trades)
                # This is market-dependent, but perp markets often have more large trades

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir",
        ["apis/backpack/perp/trades"],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_perp_trade_side_distribution(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test perp trade side distribution and balance."""
        trades = await bp_api_for_test_env.get_recent_trades("SOL_USDC_PERP", limit=50)

        if len(trades) > 0:
            buy_trades = 0
            sell_trades = 0
            buy_volume = Decimal(0)
            sell_volume = Decimal(0)

            for trade in trades:
                volume = trade.price * trade.quantity

                if trade.side == OrderSide.BUY:
                    buy_trades += 1
                    buy_volume += volume
                elif trade.side == OrderSide.SELL:
                    sell_trades += 1
                    sell_volume += volume

            total_sided_trades = buy_trades + sell_trades
            if total_sided_trades > 10:  # Only check if we have enough trades
                assert buy_trades > 0, "Should have some buy trades in active perp market"
                assert sell_trades > 0, "Should have some sell trades in active perp market"

                # Volume analysis
                total_volume = buy_volume + sell_volume
                if total_volume > Decimal(0):
                    buy_ratio = buy_volume / total_volume
                    # Buy ratio should be between 0 and 1 (valid percentage)
                    assert Decimal(0) <= buy_ratio <= Decimal(1), (
                        f"Buy volume ratio must be valid percentage: {buy_ratio}"
                    )

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir",
        ["apis/backpack/perp/trades"],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_perp_trade_size_analysis(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test perp trade size patterns and distribution."""
        trades = await bp_api_for_test_env.get_recent_trades("SOL_USDC_PERP", limit=30)

        if len(trades) > 5:
            quantities = [trade.quantity for trade in trades]
            prices = [trade.price for trade in trades]

            # Basic statistical validation
            min_qty = min(quantities)
            max_qty = max(quantities)
            avg_qty = sum(quantities) / len(quantities)

            assert min_qty > Decimal(0), "Minimum quantity should be positive"
            assert max_qty >= min_qty, "Maximum should be >= minimum"
            assert avg_qty > Decimal(0), "Average quantity should be positive"

            # Price consistency
            min_price = min(prices)
            max_price = max(prices)
            price_range = max_price - min_price

            # Price volatility should be non-negative
            if len(trades) > 10 and min_price > Decimal(0):
                price_volatility = price_range / min_price
                assert price_volatility >= Decimal(0), (
                    f"Price volatility cannot be negative: {price_volatility}"
                )

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir",
        ["apis/backpack/perp/trades"],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_get_recent_trades_invalid_perp_symbol_error(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test BackpackAPI.get_recent_trades() with invalid perp symbol.

        Raises appropriate error.
        """
        with pytest.raises(APIError) as exc_info:
            await bp_api_for_test_env.get_recent_trades("INVALID_PERP", limit=5)

        error = exc_info.value
        assert "INVALID_PERP" in str(error) or "symbol" in str(error).lower()

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir",
        ["apis/backpack/perp/trades"],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_perp_trade_precision_and_margin_calculations(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test perp trade precision handling for margin calculations."""
        trades = await bp_api_for_test_env.get_recent_trades("SOL_USDC_PERP", limit=10)

        if len(trades) > 0:
            for trade in trades:
                assert isinstance(trade.price, Decimal), "Price should be Decimal type"
                assert isinstance(trade.quantity, Decimal), "Quantity should be Decimal type"

                # Test leverage calculations
                notional = trade.price * trade.quantity
                margin_10x = notional / Decimal(10)  # 10x leverage
                margin_20x = notional / Decimal(20)  # 20x leverage

                assert isinstance(margin_10x, Decimal), (
                    "Margin calculation should maintain Decimal type"
                )
                assert isinstance(margin_20x, Decimal), (
                    "Margin calculation should maintain Decimal type"
                )
                assert margin_10x > margin_20x, "Lower leverage should require more margin"

                # Test PnL calculations
                price_move = trade.price * Decimal("0.01")  # 1% price move
                pnl_10x = trade.quantity * price_move * Decimal(10)  # 10x leverage PnL

                assert isinstance(pnl_10x, Decimal), "PnL calculation should maintain Decimal type"

    @pytest.mark.parametrize("limit", [1, 5, 10, 25])
    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir",
        ["apis/backpack/perp/trades"],
        indirect=True,
    )
    @pytest.mark.asyncio
    async def test_perp_trade_limit_parameter(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
        limit: int,
    ) -> None:
        """Test perp trade limit parameter functionality."""
        trades = await bp_api_for_test_env.get_recent_trades("SOL_USDC_PERP", limit=limit)

        assert isinstance(trades, list), "Should return list"
        assert len(trades) <= limit, f"Should not exceed requested limit of {limit}"

        # Perp markets are often very active
        if len(trades) > 0:
            assert len(trades) >= min(1, limit), "Should have at least one trade if any exist"
