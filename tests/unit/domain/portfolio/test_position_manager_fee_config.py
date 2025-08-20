"""Test that PositionManager properly uses configuration for fee inclusion.

This test verifies the fix for the critical hard-coded fee configuration issue.
Previously, include_fees was hard-coded to False at lines 267 and 538.
Now it should use config.financial.pnl.include_fees_in_pnl.

Following TESTING_SECURITY_RULES.md and CLAUDE.md:
- Tests behavior via public methods, not private members
- Tests that the correct config value is passed to PnL calculator
"""

from decimal import Decimal
from unittest.mock import MagicMock, call

from cyberdelta.domain.portfolio.position_manager import PositionManager
from cyberdelta.enums import OrderSide
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models.derivative_position import DerivativePosition
from tests.common_symbols import BTC_HL


class TestPositionManagerFeeConfiguration:
    """Test suite for verifying PositionManager fee configuration usage."""

    async def test_position_manager_uses_fee_config_true(self) -> None:
        """Test that fee configuration True is passed to PnL calculator."""
        # Setup mock config with fees enabled
        config = MagicMock()
        config.validation.position_size_tolerance = Decimal("0.01")
        config.validation.position_closure_threshold = Decimal("0.0001")
        config.validation.max_position_age_seconds = 86400
        config.financial.pnl.include_fees_in_pnl = True

        # Setup other mocks
        state_manager = MagicMock()
        pnl_calculator = MagicMock()
        event_bus = MagicMock()

        # Create position manager
        position_manager = PositionManager(
            config=config,
            state_manager=state_manager,
            pnl_calculator=pnl_calculator,
            event_bus=event_bus,
        )

        # Create a test position
        test_position = DerivativePosition(
            exchange=ExchangeName.HYPERLIQUID,
            symbol=BTC_HL,
            side=OrderSide.BUY,
            size=Decimal("1.0"),
            entry_price=Decimal(50000),
            timestamp=MagicMock(),
        )

        # Mock state manager to return the position
        state_manager.get_position_for_symbol.return_value = test_position

        # Trigger calculate_position_pnl which should use the config
        current_price = Decimal(51000)
        await position_manager.calculate_position_pnl(
            symbol=BTC_HL,
            exchange=ExchangeName.HYPERLIQUID,
            current_price=current_price,
        )

        # Verify the PnL calculator was called with include_fees=True from config
        pnl_calculator.calculate_unrealized.assert_called_once_with(
            position=test_position,
            mark_price=current_price,
            include_fees=True,  # Should match config value
        )

    async def test_position_manager_uses_fee_config_false(self) -> None:
        """Test that fee configuration False is passed to PnL calculator."""
        # Setup mock config with fees disabled
        config = MagicMock()
        config.validation.position_size_tolerance = Decimal("0.01")
        config.validation.position_closure_threshold = Decimal("0.0001")
        config.validation.max_position_age_seconds = 86400
        config.financial.pnl.include_fees_in_pnl = False

        # Setup other mocks
        state_manager = MagicMock()
        pnl_calculator = MagicMock()
        event_bus = MagicMock()

        # Create position manager
        position_manager = PositionManager(
            config=config,
            state_manager=state_manager,
            pnl_calculator=pnl_calculator,
            event_bus=event_bus,
        )

        # Create a test position
        test_position = DerivativePosition(
            exchange=ExchangeName.HYPERLIQUID,
            symbol=BTC_HL,
            side=OrderSide.BUY,
            size=Decimal("1.0"),
            entry_price=Decimal(50000),
            timestamp=MagicMock(),
        )

        # Mock state manager to return the position
        state_manager.get_position_for_symbol.return_value = test_position

        # Trigger calculate_position_pnl which should use the config
        current_price = Decimal(51000)
        await position_manager.calculate_position_pnl(
            symbol=BTC_HL,
            exchange=ExchangeName.HYPERLIQUID,
            current_price=current_price,
        )

        # Verify the PnL calculator was called with include_fees=False from config
        pnl_calculator.calculate_unrealized.assert_called_once_with(
            position=test_position,
            mark_price=current_price,
            include_fees=False,  # Should match config value
        )

    async def test_config_value_not_affected_by_later_changes(self) -> None:
        """Test that configuration value is cached and not affected by later config changes."""
        # Setup mock config with fees enabled
        config = MagicMock()
        config.validation.position_size_tolerance = Decimal("0.01")
        config.validation.position_closure_threshold = Decimal("0.0001")
        config.validation.max_position_age_seconds = 86400
        config.financial.pnl.include_fees_in_pnl = True

        # Setup other mocks
        state_manager = MagicMock()
        pnl_calculator = MagicMock()
        event_bus = MagicMock()

        # Create position manager
        position_manager = PositionManager(
            config=config,
            state_manager=state_manager,
            pnl_calculator=pnl_calculator,
            event_bus=event_bus,
        )

        # Create a test position
        test_position = DerivativePosition(
            exchange=ExchangeName.HYPERLIQUID,
            symbol=BTC_HL,
            side=OrderSide.BUY,
            size=Decimal("1.0"),
            entry_price=Decimal(50000),
            timestamp=MagicMock(),
        )

        # Mock state manager to return the position
        state_manager.get_position_for_symbol.return_value = test_position

        # First call - should use original config value (True)
        await position_manager.calculate_position_pnl(
            symbol=BTC_HL,
            exchange=ExchangeName.HYPERLIQUID,
            current_price=Decimal(51000),
        )

        # Change the config after initialization
        config.financial.pnl.include_fees_in_pnl = False

        # Second call - should still use cached value (True)
        await position_manager.calculate_position_pnl(
            symbol=BTC_HL,
            exchange=ExchangeName.HYPERLIQUID,
            current_price=Decimal(52000),
        )

        # Verify both calls used include_fees=True (cached value)
        calls = pnl_calculator.calculate_unrealized.call_args_list
        assert len(calls) == 2
        assert calls[0] == call(
            position=test_position,
            mark_price=Decimal(51000),
            include_fees=True,  # Original cached value
        )
        assert calls[1] == call(
            position=test_position,
            mark_price=Decimal(52000),
            include_fees=True,  # Still using cached value, not the changed config
        )
