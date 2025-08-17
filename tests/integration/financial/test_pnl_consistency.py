"""Cross-validation tests to ensure all PnL calculations are consistent."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from cyberdelta.config import AppSettings
from cyberdelta.domain.financial.calculators.mark_to_market_calculator import (
    MarkToMarketCalculator,
)
from cyberdelta.enums import ExchangeName, OrderSide
from cyberdelta.models.derivative_position import DerivativePosition
from tests.factories.symbol_test_factory import SymbolTestFactory


class TestPnLCalculationConsistency:
    """Ensure all PnL calculation methods return identical results."""

    @pytest.fixture
    def pnl_calculator(self) -> MarkToMarketCalculator:
        """Create a PnL calculator for testing.

        Returns:
            MarkToMarketCalculator instance configured for testing
        """
        # Create minimal valid AppSettings using model_validate
        config = AppSettings.model_validate({})
        return MarkToMarketCalculator(config, fee_calculator=None)

    @pytest.mark.parametrize(
        ("side", "size", "entry_price", "mark_price", "expected_pnl"),
        [
            # Long positions
            (OrderSide.BUY, Decimal(100), Decimal(50000), Decimal(55000), Decimal(500000)),
            (OrderSide.BUY, Decimal(1), Decimal(100), Decimal(110), Decimal(10)),
            # Short positions - negative size representation
            (OrderSide.SELL, Decimal(-100), Decimal(50000), Decimal(45000), Decimal(500000)),
            # Edge cases
            (OrderSide.BUY, Decimal("0.1"), Decimal(1000), Decimal(1100), Decimal(10)),
            (OrderSide.SELL, Decimal("-0.1"), Decimal(1000), Decimal(900), Decimal(10)),
        ],
    )
    def test_pnl_calculation_consistency_after_fix(
        self,
        pnl_calculator: MarkToMarketCalculator,
        side: OrderSide,
        size: Decimal,
        entry_price: Decimal,
        mark_price: Decimal,
        expected_pnl: Decimal,
    ) -> None:
        """CRITICAL: Test that PnL calculation is consistent regardless of size sign."""
        # Create test position
        position = DerivativePosition(
            exchange=ExchangeName.BACKPACK,
            symbol=SymbolTestFactory.create_with_metadata("BTC-USDC", ExchangeName.BACKPACK),
            side=side,
            size=size,
            entry_price=entry_price,
            timestamp=datetime.now(UTC),
        )

        # Test centralized calculator
        pnl_result = pnl_calculator.calculate_unrealized_pnl(
            position=position, mark_price=mark_price, include_fees=False
        )
        position_pnl = pnl_result.amount

        # Should always calculate same PnL regardless of size sign
        assert position_pnl == expected_pnl, (
            f"PnL calculation failed for {side} position with size {size}. "
            f"Expected {expected_pnl}, got {position_pnl}"
        )

    def test_short_position_bug_fix_specific(self) -> None:
        """CRITICAL: Specific test for the short position calculation bug fix."""
        # Create short position that was previously calculated incorrectly
        short_position = DerivativePosition(
            exchange=ExchangeName.BACKPACK,
            symbol=SymbolTestFactory.create_with_metadata("BTC-USDC", ExchangeName.BACKPACK),
            side=OrderSide.SELL,
            size=Decimal(-100),  # Negative size
            entry_price=Decimal(50000),
            timestamp=datetime.now(UTC),
        )

        # Price drops $5000 - should be $500K profit for short
        mark_price = Decimal(45000)

        # Create calculator for this test
        config = AppSettings.model_validate({})
        calculator = MarkToMarketCalculator(config, fee_calculator=None)

        pnl_result = calculator.calculate_unrealized_pnl(
            position=short_position, mark_price=mark_price, include_fees=False
        )
        result = pnl_result.amount

        # CRITICAL: Should show profit, not loss
        expected_profit = Decimal(500000)  # 100 * ($50K - $45K)
        assert result == expected_profit

        # Test that our fix works for the standard case too
        long_position = DerivativePosition(
            exchange=ExchangeName.BACKPACK,
            symbol=SymbolTestFactory.create_with_metadata("BTC-USDC", ExchangeName.BACKPACK),
            side=OrderSide.BUY,
            size=Decimal(100),  # Positive size for long
            entry_price=Decimal(50000),
            timestamp=datetime.now(UTC),
        )

        # Price drops $5000 - should be $500K loss for long
        result_long = long_position.calculate_unrealized_pnl(mark_price)
        expected_loss = Decimal(-500000)  # 100 * ($45K - $50K)
        assert result_long == expected_loss

    def test_zero_position_handling(self) -> None:
        """Test that zero positions return None."""
        zero_position = DerivativePosition(
            exchange=ExchangeName.BACKPACK,
            symbol=SymbolTestFactory.create_with_metadata("BTC-USDC", ExchangeName.BACKPACK),
            side=OrderSide.BUY,
            size=Decimal(0),
            entry_price=None,
            timestamp=datetime.now(UTC),
        )

        result = zero_position.calculate_unrealized_pnl(Decimal(50000))
        assert result is None
