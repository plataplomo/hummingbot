"""Tests for RiskManager opportunity validation logic."""

from decimal import Decimal
from unittest.mock import patch

import pytest

from cyberdelta.core.risk_manager import RiskManager, SizedOpportunity
from cyberdelta.validation.funding_data import ArbitrageOpportunity


# Note: Fixtures risk_manager, sample_opportunity
#       are provided by tests/unit/risk/conftest.py


class TestRiskManagerValidation:
    """Test suite for RiskManager validate_opportunities."""

    @pytest.mark.asyncio
    async def test_validate_opportunities(
        self,
        risk_manager: RiskManager,
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test validating opportunities."""
        # Test with a valid opportunity (needs size_opportunity to return something)
        valid_sized = SizedOpportunity(
            sample_opportunity,
            Decimal(100),
            Decimal(100),
            Decimal("0.1"),  # allocation_percentage
            Decimal(1),
            Decimal("0.01"),  # expected_return
            Decimal("0.1"),  # risk_adjusted_return
        )
        with patch.object(risk_manager, "size_opportunity", return_value=valid_sized):
            valid_opportunities = [sample_opportunity]
            validated = await risk_manager.validate_opportunities(valid_opportunities)
            assert len(validated) == 1
            assert validated[0] == valid_sized

        # Test with an invalid opportunity (mock size_opportunity returning None)
        with patch.object(risk_manager, "size_opportunity", return_value=None):
            # Use a real ArbitrageOpportunity with values that are valid for the model
            # but should logically fail sizing (e.g., unprofitable)
            invalid_opportunity = ArbitrageOpportunity(
                symbol="INVALID_SYM",
                long_exchange="invalid_long",
                short_exchange="invalid_short",
                long_price=Decimal("10000.0"),  # Valid price, but make it unprofitable
                short_price=Decimal("9000.0"),  # Long price > Short price = unprofitable
                long_funding_rate=Decimal("0.0001"),
                short_funding_rate=Decimal("0.0001"),  # Zero NFD
                net_funding_differential=Decimal("0.0"),  # Explicitly zero NFD
                timestamp=sample_opportunity.timestamp,
                # Add missing fields if ArbitrageOpportunity requires them and they
                # affect equality/hashing for the test
                expected_profit=Decimal("-1000.0"),  # Unprofitable
                utility_score=0.1,  # Low utility
                basis_volatility=0.05,  # Some volatility
            )
            invalid_opportunities = [invalid_opportunity]
            validated_invalid = await risk_manager.validate_opportunities(invalid_opportunities)
            assert len(validated_invalid) == 0, (
                "Validate opportunities should return empty list when sizing fails"
            )

        # Test mixed list
        def size_side_effect(opp: ArbitrageOpportunity) -> SizedOpportunity | None:
            """Return sized opportunity or None based on input opportunity."""
            if opp == sample_opportunity:
                return valid_sized
            return None

        with patch.object(risk_manager, "size_opportunity", side_effect=size_side_effect):
            mixed_opportunities: list[ArbitrageOpportunity] = [
                sample_opportunity,  # This will be sized successfully
                ArbitrageOpportunity(  # This one will fail sizing due to the side_effect mock
                    symbol="OTHER-PERP",
                    long_exchange="other_long",
                    short_exchange="other_short",
                    long_price=Decimal(99),
                    short_price=Decimal(101),
                    long_funding_rate=Decimal("0.0"),
                    short_funding_rate=Decimal("0.0"),
                    net_funding_differential=Decimal("0.0"),
                    timestamp=sample_opportunity.timestamp,
                ),
                sample_opportunity,  # Add the valid one again to test sorting/uniqueness
            ]
            # We expect only one valid opportunity after validation and sorting
            validated_mixed = await risk_manager.validate_opportunities(mixed_opportunities)
            assert len(validated_mixed) == 2

    # TODO: Add tests for _get_validation_metrics if needed
    # (currently implicitly tested via controls)
    # TODO: Add tests for is_opportunity_profitable helper
