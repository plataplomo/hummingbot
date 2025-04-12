#!/usr/bin/env python
"""Tests for RiskManager opportunity validation logic."""

from decimal import Decimal
from unittest.mock import MagicMock

from cyberdelta.core.models import ArbitrageOpportunity
from cyberdelta.core.risk_manager import RiskManager, SizedOpportunity

# Note: Fixtures risk_manager, sample_opportunity
#       are provided by tests/unit/risk/conftest.py


class TestRiskManagerValidation:
    """Test suite for RiskManager validate_opportunities."""

    def test_validate_opportunities(
        self, risk_manager: RiskManager, sample_opportunity: ArbitrageOpportunity
    ) -> None:
        """Test validating opportunities."""
        # Test with a valid opportunity (needs size_opportunity to return something)
        valid_sized = SizedOpportunity(
            sample_opportunity, Decimal("100"), Decimal("100"), 0.1, Decimal("1"), 0.01, 0.1
        )
        risk_manager.size_opportunity = MagicMock(return_value=valid_sized)

        valid_opportunities = [sample_opportunity]
        validated = risk_manager.validate_opportunities(valid_opportunities)
        assert len(validated) == 1
        assert validated[0] == valid_sized

        # Test with an invalid opportunity (mock size_opportunity returning None)
        risk_manager.size_opportunity = MagicMock(return_value=None)
        # Add necessary attributes to the mock for logging format string
        invalid_opportunity = MagicMock(spec=ArbitrageOpportunity)
        invalid_opportunity.symbol = "INVALID_SYM"
        invalid_opportunity.long_exchange = "invalid_long"
        invalid_opportunity.short_exchange = "invalid_short"

        invalid_opportunities = [invalid_opportunity]
        validated_invalid = risk_manager.validate_opportunities(invalid_opportunities)
        assert len(validated_invalid) == 0, (
            "Validate opportunities should return empty list when sizing fails"
        )

        # Test mixed list
        def size_side_effect(opp):
            if opp == sample_opportunity:
                return valid_sized
            else:
                return None

        risk_manager.size_opportunity = MagicMock(side_effect=size_side_effect)
        mixed_opportunities = [
            invalid_opportunity,
            sample_opportunity,
        ]  # Order reversed for sort check
        validated_mixed = risk_manager.validate_opportunities(mixed_opportunities)
        assert len(validated_mixed) == 1
        assert validated_mixed[0] == valid_sized  # Should still be the valid one

    # TODO: Add tests for _get_validation_metrics if needed (currently implicitly tested via controls)
    # TODO: Add tests for is_opportunity_profitable helper
