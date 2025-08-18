"""Test that PositionManager properly uses configuration for fee inclusion.

This test verifies the fix for the critical hard-coded fee configuration issue.
Previously, include_fees was hard-coded to False at lines 267 and 538.
Now it should use config.financial.pnl.include_fees_in_pnl.
"""

from decimal import Decimal
from unittest.mock import MagicMock

from cyberdelta.domain.portfolio.position_manager import PositionManager


class TestPositionManagerFeeConfiguration:
    """Test suite for verifying PositionManager fee configuration usage."""

    def test_position_manager_caches_fee_config_true(self) -> None:
        """Test that fee configuration is cached during initialization when True."""
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

        # Verify the fee configuration was cached correctly
        assert position_manager._include_fees_in_pnl is True

    def test_position_manager_caches_fee_config_false(self) -> None:
        """Test that fee configuration is cached during initialization when False."""
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

        # Verify the fee configuration was cached correctly
        assert position_manager._include_fees_in_pnl is False

    def test_cached_config_not_affected_by_later_changes(self) -> None:
        """Test that cached configuration is not affected by later config changes."""
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

        # Verify initial cached value
        assert position_manager._include_fees_in_pnl is True

        # Change the config after initialization
        config.financial.pnl.include_fees_in_pnl = False

        # The cached value should still be True (not affected by config change)
        assert position_manager._include_fees_in_pnl is True
