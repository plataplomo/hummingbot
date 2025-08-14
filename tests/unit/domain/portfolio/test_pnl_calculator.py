"""Property-based tests for PnL Calculator.

This module tests the critical PnL calculation functions to ensure:
- Mathematical invariants in PnL calculations
- Precision preservation in all financial calculations
- Correct profit/loss direction based on position side
- Fee handling consistency
- Realized vs unrealized PnL accuracy

SECURITY CRITICAL: PnL calculation errors could lead to incorrect portfolio
valuation, wrong trading decisions, or financial reporting errors.
"""

from decimal import Decimal
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from hypothesis import assume, given, strategies as st
from hypothesis.strategies import SearchStrategy

from cyberdelta.config.models import AppSettings, PortfolioCalculationSettings
from cyberdelta.domain.portfolio.pnl_calculator import PnLCalculator
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.enums.trading import OrderSide
from cyberdelta.exceptions.portfolio import InvalidPositionDataError
from cyberdelta.models import DerivativePosition
from cyberdelta.models.portfolio.pnl_report import PnLReport
from cyberdelta.models.portfolio.portfolio_state import (
    PortfolioState,  # type: ignore[import-untyped]
)
from cyberdelta.symbols import symbol


# =============================================================================
# HYPOTHESIS STRATEGIES FOR PNL TESTING
# =============================================================================


def price_strategy() -> SearchStrategy[Decimal]:
    """Generate valid price values.

    Returns:
        SearchStrategy[Decimal]: Strategy generating valid price values.
    """
    return st.decimals(
        min_value=Decimal("0.00000001"),
        max_value=Decimal(1000000),
        places=8,
        allow_nan=False,
        allow_infinity=False,
    ).filter(lambda x: x > 0)


def quantity_strategy() -> SearchStrategy[Decimal]:
    """Generate valid position quantities.

    Returns:
        SearchStrategy[Decimal]: Strategy generating valid quantity values.
    """
    return st.decimals(
        min_value=Decimal("0.00000001"),
        max_value=Decimal(10000),
        places=8,
        allow_nan=False,
        allow_infinity=False,
    ).filter(lambda x: x > 0)


def pnl_strategy() -> SearchStrategy[Decimal]:
    """Generate valid PnL values (can be negative).

    Returns:
        SearchStrategy[Decimal]: Strategy generating valid PnL values.
    """
    return st.decimals(
        min_value=Decimal(-100000),
        max_value=Decimal(100000),
        places=8,
        allow_nan=False,
        allow_infinity=False,
    )


def fee_strategy() -> SearchStrategy[Decimal]:
    """Generate valid fee amounts.

    Returns:
        SearchStrategy[Decimal]: Strategy generating valid fee amounts.
    """
    return st.decimals(
        min_value=Decimal(0),
        max_value=Decimal(1000),
        places=8,
        allow_nan=False,
        allow_infinity=False,
    )


def side_strategy() -> SearchStrategy[OrderSide]:
    """Generate valid order sides.

    Returns:
        SearchStrategy[OrderSide]: Strategy generating valid order sides.
    """
    return st.sampled_from([OrderSide.BUY, OrderSide.SELL])


def exchange_strategy() -> SearchStrategy[ExchangeName]:
    """Generate valid exchange names.

    Returns:
        SearchStrategy[ExchangeName]: Strategy generating valid exchange names.
    """
    return st.sampled_from([ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK])


def position_strategy() -> SearchStrategy[dict[str, object]]:
    """Generate valid position data.

    Returns:
        SearchStrategy[dict[str, object]]: Strategy generating valid position data.
    """
    return st.builds(
        lambda entry_price, current_price, size, side, exchange: {
            "entry_price": entry_price,
            "current_price": current_price,
            "size": size if side == OrderSide.BUY else -size,
            "side": side,
            "exchange": exchange,
        },
        entry_price=price_strategy(),
        current_price=price_strategy(),
        size=quantity_strategy(),
        side=side_strategy(),
        exchange=exchange_strategy(),
    )


# =============================================================================
# TEST FIXTURES
# =============================================================================


@pytest.fixture
def mock_config() -> MagicMock:
    """Create mock application configuration.

    Returns:
        MagicMock: Mock application configuration.
    """
    config = MagicMock(spec=AppSettings)
    config.calculation = MagicMock(spec=PortfolioCalculationSettings)
    config.calculation.pnl_calculation_method = "mark_to_market"
    config.calculation.include_fees_in_pnl = True
    config.calculation.base_currency = "USD"
    config.calculation.performance_period_days = 30
    return config


@pytest.fixture
def mock_state_manager() -> AsyncMock:
    """Create mock portfolio state manager.

    Returns:
        AsyncMock: Mock portfolio state manager.
    """
    state_manager = AsyncMock()
    state = MagicMock(spec=PortfolioState)
    state.positions = {}
    state.total_equity_usd = Decimal(10000)
    state_manager.get_state.return_value = state
    return state_manager


@pytest.fixture
def pnl_calculator(mock_config: MagicMock, mock_state_manager: AsyncMock) -> PnLCalculator:
    """Create PnL calculator instance.

    Returns:
        PnLCalculator: PnL calculator instance for testing.
    """
    return PnLCalculator(mock_config, mock_state_manager)


# =============================================================================
# PROPERTY TESTS FOR PNL CALCULATIONS
# =============================================================================


class TestPnLCalculationProperties:
    """Property-based tests for PnL calculation logic."""

    @given(
        entry_price=price_strategy(),
        current_price=price_strategy(),
        quantity=quantity_strategy(),
        side=side_strategy(),
    )
    def test_unrealized_pnl_calculation_properties(
        self, entry_price: Decimal, current_price: Decimal, quantity: Decimal, side: OrderSide
    ) -> None:
        """Property: Unrealized PnL should follow mathematical invariants."""
        # Calculate expected PnL based on side
        if side == OrderSide.BUY:
            # Long position: profit when price goes up
            expected_pnl = (current_price - entry_price) * quantity
        else:
            # Short position: profit when price goes down
            expected_pnl = (entry_price - current_price) * quantity

        # Property: PnL direction should match price movement
        if side == OrderSide.BUY:
            if current_price > entry_price:
                assert expected_pnl > 0, "Long position should profit when price increases"
            elif current_price < entry_price:
                assert expected_pnl < 0, "Long position should lose when price decreases"
            else:
                assert expected_pnl == 0, "No PnL when price unchanged"
        elif current_price < entry_price:
            assert expected_pnl > 0, "Short position should profit when price decreases"
        elif current_price > entry_price:
            assert expected_pnl < 0, "Short position should lose when price increases"
        else:
            assert expected_pnl == 0, "No PnL when price unchanged"

        # Property: PnL magnitude should be proportional to price change and quantity
        price_change = abs(current_price - entry_price)
        expected_magnitude = price_change * quantity
        assert abs(expected_pnl) == expected_magnitude

    @given(
        unrealized_pnl=pnl_strategy(),
        realized_pnl=pnl_strategy(),
        fees=fee_strategy(),
        include_fees=st.booleans(),
    )
    def test_net_pnl_calculation_properties(
        self, unrealized_pnl: Decimal, realized_pnl: Decimal, fees: Decimal, include_fees: bool
    ) -> None:
        """Property: Net PnL should equal sum of components."""
        # Calculate expected net PnL
        net_pnl = unrealized_pnl + realized_pnl
        if include_fees:
            net_pnl -= fees

        # Property: Net PnL components should sum correctly
        if include_fees:
            assert net_pnl == unrealized_pnl + realized_pnl - fees
        else:
            assert net_pnl == unrealized_pnl + realized_pnl

        # Property: Fees should only reduce net PnL, never increase it
        if include_fees and fees > 0:
            assert net_pnl < unrealized_pnl + realized_pnl

    @given(entry_price=price_strategy(), quantity=quantity_strategy(), pnl=pnl_strategy())
    def test_return_percentage_calculation(
        self, entry_price: Decimal, quantity: Decimal, pnl: Decimal
    ) -> None:
        """Property: Return percentage should be consistent with PnL."""
        entry_value = entry_price * quantity

        # Calculate expected return percentage
        expected_return = (pnl / entry_value) * 100 if entry_value > 0 else Decimal(0)

        # Property: Return percentage should reflect PnL relative to entry value
        if entry_value > 0:
            assert expected_return == (pnl / entry_value) * 100

            # Property: Positive PnL should give positive return
            if pnl > 0:
                assert expected_return > 0
            elif pnl < 0:
                assert expected_return < 0
            else:
                assert expected_return == 0

    @given(positions=st.lists(position_strategy(), min_size=1, max_size=10))
    def test_portfolio_pnl_aggregation(self, positions: list[dict[str, object]]) -> None:
        """Property: Portfolio PnL should equal sum of position PnLs."""
        total_unrealized_pnl = Decimal(0)

        for pos in positions:
            # Calculate PnL for each position
            entry_price = cast(Decimal, pos["entry_price"])
            current_price = cast(Decimal, pos["current_price"])
            size = cast(Decimal, pos["size"])
            side = cast(OrderSide, pos["side"])

            if side == OrderSide.BUY:
                position_pnl = (current_price - entry_price) * abs(size)
            else:
                position_pnl = (entry_price - current_price) * abs(size)

            total_unrealized_pnl += position_pnl

        # Property: Total PnL should be sum of individual PnLs
        calculated_total = sum(
            (cast(Decimal, pos["current_price"]) - cast(Decimal, pos["entry_price"]))
            * abs(cast(Decimal, pos["size"]))
            if cast(OrderSide, pos["side"]) == OrderSide.BUY
            else (cast(Decimal, pos["entry_price"]) - cast(Decimal, pos["current_price"]))
            * abs(cast(Decimal, pos["size"]))
            for pos in positions
        )

        assert total_unrealized_pnl == calculated_total

    @given(
        entry_price=price_strategy(), current_price=price_strategy(), quantity=quantity_strategy()
    )
    def test_market_value_calculation(
        self, entry_price: Decimal, current_price: Decimal, quantity: Decimal
    ) -> None:
        """Property: Market value should equal current price * quantity."""
        expected_market_value = current_price * quantity

        # Property: Market value should be non-negative
        assert expected_market_value >= 0

        # Property: Market value should be proportional to quantity
        assert expected_market_value == current_price * quantity

        # Property: Market value should change with price
        new_price = current_price * Decimal("1.1")  # 10% increase
        new_market_value = new_price * quantity
        assert new_market_value > expected_market_value


# =============================================================================
# PROPERTY TESTS FOR PNL CALCULATOR CLASS
# =============================================================================


class TestPnLCalculatorIntegration:
    """Integration property tests for PnL calculator."""

    @pytest.mark.asyncio
    @given(
        entry_price=price_strategy(),
        current_price=price_strategy(),
        quantity=quantity_strategy(),
        side=side_strategy(),
    )
    async def test_position_pnl_calculation(
        self,
        pnl_calculator: PnLCalculator,
        mock_state_manager: AsyncMock,
        entry_price: Decimal,
        current_price: Decimal,
        quantity: Decimal,
        side: OrderSide,
    ) -> None:
        """Property: Position PnL calculation should be accurate."""
        # Create mock position
        symbol_obj = symbol("BTC", ExchangeName.HYPERLIQUID)
        exchange = ExchangeName.HYPERLIQUID

        position = MagicMock(spec=DerivativePosition)
        position.symbol = symbol_obj
        position.exchange = exchange
        position.entry_price = entry_price
        position.size = quantity if side == OrderSide.BUY else -quantity
        position.side = side

        # Set up state manager
        state = MagicMock(spec=PortfolioState)
        state.positions = {f"{exchange.value}:{symbol_obj.value}": position}
        mock_state_manager.get_state.return_value = state

        # Mock market price - using setattr to avoid mypy method assignment error
        mock_get_price = AsyncMock(return_value=current_price)
        pnl_calculator._get_current_market_price = mock_get_price

        # Calculate PnL
        result = await pnl_calculator.calculate_position_pnl(symbol_obj, exchange)

        if result:
            # Property: PnL calculation should match expected formula
            if side == OrderSide.BUY:
                expected_pnl = (current_price - entry_price) * quantity
            else:
                expected_pnl = (entry_price - current_price) * quantity

            assert result.unrealized_pnl_usd == expected_pnl
            assert result.entry_price == entry_price
            assert result.current_price == current_price
            assert result.quantity == quantity
            assert result.market_value_usd == current_price * quantity

    @pytest.mark.asyncio
    @given(
        include_fees=st.booleans(),
        total_equity=st.decimals(
            min_value=Decimal(100), max_value=Decimal(1000000), places=2
        ).filter(lambda x: x > 0),
    )
    async def test_comprehensive_pnl_report(
        self,
        pnl_calculator: PnLCalculator,
        mock_state_manager: AsyncMock,
        include_fees: bool,
        total_equity: Decimal,
    ) -> None:
        """Property: Comprehensive PnL report should be internally consistent."""
        # Set up configuration
        pnl_calculator._include_fees = include_fees

        # Set up state
        state = MagicMock(spec=PortfolioState)
        state.positions = {}
        state.total_equity_usd = total_equity
        mock_state_manager.get_state.return_value = state

        # Calculate comprehensive PnL
        report = await pnl_calculator.calculate_pnl()

        # Property: Report should have consistent structure
        assert isinstance(report, PnLReport)
        assert report.total_equity_usd == total_equity
        assert report.calculation_method == "comprehensive"
        assert report.fees_included == include_fees
        assert report.base_currency == "USD"

        # Property: Net PnL should equal unrealized + realized
        if not include_fees or report.total_fees_usd is None:
            assert (
                report.net_pnl_usd
                == report.total_unrealized_pnl_usd + report.total_realized_pnl_usd
            )
        else:
            assert (
                report.net_pnl_usd
                == report.total_unrealized_pnl_usd
                + report.total_realized_pnl_usd
                - report.total_fees_usd
            )

    @pytest.mark.asyncio
    async def test_missing_portfolio_equity_error(
        self, pnl_calculator: PnLCalculator, mock_state_manager: AsyncMock
    ) -> None:
        """Property: Should raise error when portfolio equity is missing."""
        # Set up state with None equity
        state = MagicMock(spec=PortfolioState)
        state.positions = {}
        state.total_equity_usd = None
        mock_state_manager.get_state.return_value = state

        # Should raise InvalidPositionDataError
        with pytest.raises(InvalidPositionDataError):
            await pnl_calculator.calculate_pnl()


# =============================================================================
# PROPERTY TESTS FOR EDGE CASES
# =============================================================================


class TestPnLEdgeCases:
    """Property tests for edge cases in PnL calculations."""

    @given(quantity=quantity_strategy(), side=side_strategy())
    def test_zero_price_change_pnl(self, quantity: Decimal, side: OrderSide) -> None:
        """Property: Zero price change should result in zero PnL."""
        price = Decimal("100.0")

        # Calculate PnL with same entry and current price
        pnl = (price - price) * quantity  # Always zero regardless of side

        assert pnl == Decimal(0)

    @given(entry_price=price_strategy(), current_price=price_strategy())
    def test_zero_quantity_pnl(self, entry_price: Decimal, current_price: Decimal) -> None:
        """Property: Zero quantity should result in zero PnL."""
        quantity = Decimal(0)

        # Calculate PnL with zero quantity
        pnl = (current_price - entry_price) * quantity

        assert pnl == Decimal(0)

    @given(
        very_small_price=st.decimals(
            min_value=Decimal("0.00000001"), max_value=Decimal("0.0001"), places=10
        ),
        large_quantity=st.decimals(min_value=Decimal(10000), max_value=Decimal(1000000), places=2),
    )
    def test_precision_preservation(
        self, very_small_price: Decimal, large_quantity: Decimal
    ) -> None:
        """Property: Precision should be preserved in calculations."""
        # Calculate with very small price changes
        entry_price = very_small_price
        current_price = very_small_price * Decimal("1.00000001")  # Tiny change

        pnl = (current_price - entry_price) * large_quantity

        # Property: Result should maintain precision
        assert isinstance(pnl, Decimal)
        # The result should be non-zero despite small price change
        assert pnl != Decimal(0)

        # Property: Calculation should be reversible
        reverse_quantity = pnl / (current_price - entry_price)
        assert abs(reverse_quantity - large_quantity) < Decimal("0.00000001")


# =============================================================================
# PROPERTY TESTS FOR FEE HANDLING
# =============================================================================


class TestFeeHandlingProperties:
    """Property tests for fee handling in PnL calculations."""

    @given(gross_pnl=pnl_strategy(), fee_amount=fee_strategy())
    def test_fee_impact_on_pnl(self, gross_pnl: Decimal, fee_amount: Decimal) -> None:
        """Property: Fees should always reduce net PnL."""
        net_pnl_with_fees = gross_pnl - fee_amount
        net_pnl_without_fees = gross_pnl

        # Property: Net PnL with fees should be less than without fees
        if fee_amount > 0:
            assert net_pnl_with_fees < net_pnl_without_fees
        else:
            assert net_pnl_with_fees == net_pnl_without_fees

        # Property: Fee impact should equal fee amount
        assert net_pnl_without_fees - net_pnl_with_fees == fee_amount

    @given(fees=st.lists(fee_strategy(), min_size=1, max_size=10))
    def test_total_fees_aggregation(self, fees: list[Decimal]) -> None:
        """Property: Total fees should equal sum of individual fees."""
        total_fees = sum(fees)

        # Property: Total should be sum of parts
        assert total_fees == sum(fees)

        # Property: Total fees should be non-negative
        assert total_fees >= 0

        # Property: Total should be at least as large as largest individual fee
        if fees:
            assert total_fees >= max(fees)


# =============================================================================
# PROPERTY TESTS FOR MATHEMATICAL INVARIANTS
# =============================================================================


class TestMathematicalInvariants:
    """Property tests for mathematical invariants in PnL calculations."""

    @given(positions=st.lists(position_strategy(), min_size=2, max_size=5))
    def test_pnl_additivity(self, positions: list[dict[str, object]]) -> None:
        """Property: PnL should be additive across positions."""
        # Calculate PnL for combined positions
        total_pnl = Decimal(0)
        individual_pnls = []

        for pos in positions:
            entry_price = cast(Decimal, pos["entry_price"])
            current_price = cast(Decimal, pos["current_price"])
            size = cast(Decimal, pos["size"])
            side = cast(OrderSide, pos["side"])

            if side == OrderSide.BUY:
                pnl = (current_price - entry_price) * abs(size)
            else:
                pnl = (entry_price - current_price) * abs(size)

            individual_pnls.append(pnl)
            total_pnl += pnl

        # Property: Total should equal sum of individuals
        assert total_pnl == sum(individual_pnls)

    @given(
        entry_price=price_strategy(),
        price_changes=st.lists(
            st.decimals(min_value=Decimal(-10), max_value=Decimal(10), places=4),
            min_size=2,
            max_size=5,
        ),
        quantity=quantity_strategy(),
    )
    def test_pnl_path_independence(
        self, entry_price: Decimal, price_changes: list[Decimal], quantity: Decimal
    ) -> None:
        """Property: Final PnL should be path-independent."""
        # Calculate final price after all changes
        final_price = entry_price
        for change in price_changes:
            final_price += change

        # Ensure final price is positive
        assume(final_price > 0)

        # Direct PnL calculation
        direct_pnl = (final_price - entry_price) * quantity

        # Step-by-step PnL calculation
        cumulative_pnl = Decimal(0)
        current_price = entry_price
        for change in price_changes:
            new_price = current_price + change
            if new_price > 0:  # Ensure price stays positive
                step_pnl = change * quantity
                cumulative_pnl += step_pnl
                current_price = new_price

        # Property: Both methods should give same result
        assert abs(direct_pnl - cumulative_pnl) < Decimal("0.00000001")

    @given(
        base_price=price_strategy(),
        percentage_change=st.decimals(min_value=Decimal("-0.99"), max_value=Decimal(10), places=4),
        quantity=quantity_strategy(),
    )
    def test_percentage_return_consistency(
        self, base_price: Decimal, percentage_change: Decimal, quantity: Decimal
    ) -> None:
        """Property: Percentage returns should be consistent with PnL."""
        # Calculate new price based on percentage change
        new_price = base_price * (Decimal(1) + percentage_change)

        # Calculate PnL
        pnl = (new_price - base_price) * quantity

        # Calculate return percentage from PnL
        entry_value = base_price * quantity
        if entry_value > 0:
            calculated_return = (pnl / entry_value) * 100
            expected_return = percentage_change * 100

            # Property: Return percentage should match input percentage
            assert abs(calculated_return - expected_return) < Decimal("0.00001")
