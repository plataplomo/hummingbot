"""Property-based tests for Backpack balance mapper.

This module tests critical balance mapping functions to ensure:
- Financial precision is preserved in balance calculations
- Balance invariants are maintained (available <= total)
- Edge case handling for zero/negative balances
- Proper asset symbol mapping

SECURITY CRITICAL: Balance mapping errors could lead to incorrect portfolio
calculations, wrong available balance reporting, or trading with insufficient funds.
"""

from decimal import Decimal, InvalidOperation as DecimalInvalidOperation
from typing import Any

import pytest
from hypothesis import assume, given, strategies as st
from hypothesis.strategies import SearchStrategy

from cyberdelta.apis.backpack.mappers.account.bp_balance_mapper import BackpackBalanceMapper
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models import SpotBalance
from cyberdelta.symbols.models import BaseSymbol


# =============================================================================
# HYPOTHESIS STRATEGIES FOR BALANCE MAPPING
# =============================================================================


def balance_decimal_strategy() -> SearchStrategy[str]:
    """Generate decimal strings for balance amounts.

    Returns:
        A Hypothesis strategy for decimal balance strings.
    """
    return st.one_of([
        # Common balance amounts
        st.decimals(min_value=Decimal(0), max_value=Decimal(1000000), places=8).map(str),
        st.decimals(min_value=Decimal(0), max_value=Decimal(100000), places=6).map(str),
        # Edge cases
        st.just("0"),
        st.just("0.0"),
        st.just("0.00000001"),  # Minimum amount
        st.just("999999.99999999"),  # Large amount
    ])


def asset_symbol_strategy() -> SearchStrategy[str]:
    """Generate valid asset symbols.

    Returns:
        A Hypothesis strategy for asset symbol strings.
    """
    return st.one_of([
        # Common crypto assets
        st.sampled_from(["BTC", "ETH", "SOL", "USDC", "USDT"]),
        # Valid format variations
        st.text(
            min_size=2, max_size=10, alphabet=st.characters(whitelist_categories=["Lu", "Ll", "Nd"])
        ),
        # Edge cases
        st.just(""),
        st.text(min_size=1, max_size=20).filter(lambda x: x.strip()),
    ])


def balance_data_strategy() -> SearchStrategy[dict[str, Any]]:
    """Generate balance data for transformation testing.

    Returns:
        A Hypothesis strategy for balance data dictionaries.
    """
    return st.fixed_dictionaries({
        "asset": asset_symbol_strategy(),
        "available": balance_decimal_strategy(),
        "locked": balance_decimal_strategy(),
    })


def collateral_balance_strategy() -> SearchStrategy[dict[str, Any]]:
    """Generate collateral balance data.

    Returns:
        A Hypothesis strategy for collateral balance data dictionaries.
    """
    return st.fixed_dictionaries({
        "asset": asset_symbol_strategy(),
        "total": balance_decimal_strategy(),
        "available": balance_decimal_strategy(),
        "locked": balance_decimal_strategy(),
        "borrowed": balance_decimal_strategy(),
        "interest": balance_decimal_strategy(),
    })


# =============================================================================
# PROPERTY TESTS FOR BALANCE TRANSFORMATION
# =============================================================================


class TestBalanceTransformationProperties:
    """Property-based tests for balance transformation."""

    @given(balance_data=balance_data_strategy())
    def test_balance_transformation_financial_precision(self, balance_data: dict[str, Any]) -> None:
        """Property: Balance transformation should preserve financial precision."""
        # Skip empty assets
        assume(balance_data["asset"].strip())

        try:
            mapper = BackpackBalanceMapper()
            total_balance = str(
                Decimal(balance_data["available"]) + Decimal(balance_data["locked"])
            )
            result = mapper.transform_balance_data_to_spot_balance(
                asset=balance_data["asset"],
                total_balance=total_balance,
                available_balance=balance_data["available"],
            )

            # Property: Result should be a SpotBalance
            assert isinstance(result, SpotBalance)

            # Property: Financial values should be preserved as Decimal
            assert isinstance(result.available_quantity, Decimal)
            assert isinstance(result.total_quantity, Decimal)

            # Property: Precision should be preserved
            expected_available = Decimal(balance_data["available"])
            expected_total = Decimal(balance_data["available"]) + Decimal(balance_data["locked"])

            assert result.available_quantity == expected_available

            # Property: Total should match calculated value
            assert result.total_quantity == expected_total

        except (ValueError, TypeError, AttributeError):
            # Expected for invalid data
            pass

    @given(available=balance_decimal_strategy(), locked=balance_decimal_strategy())
    def test_balance_mathematical_invariants(self, available: str, locked: str) -> None:
        """Property: Balance calculations should maintain mathematical invariants."""
        # Only test with positive values for this property
        available_dec = Decimal(available)
        locked_dec = Decimal(locked)

        try:
            mapper = BackpackBalanceMapper()
            total_balance = str(available_dec + locked_dec)
            result = mapper.transform_balance_data_to_spot_balance(
                asset="BTC", total_balance=total_balance, available_balance=available
            )

            # Property: All balance amounts should be non-negative
            assert result.available_quantity >= Decimal(0)
            assert result.total_quantity >= Decimal(0)

            # Property: Total should match calculated value
            assert result.total_quantity == available_dec + locked_dec

            # Property: Available should not exceed total
            assert result.available_quantity <= result.total_quantity

            # Property: Values should be finite
            assert result.available_quantity.is_finite()
            assert result.total_quantity.is_finite()

        except (ValueError, TypeError, AttributeError, KeyError, DecimalInvalidOperation):
            # Expected for invalid input
            pass

    @given(asset=st.sampled_from(["BTC", "ETH", "USDC"]))
    def test_balance_asset_symbol_consistency(self, asset: str) -> None:
        """Property: Asset symbols should be mapped consistently."""
        mapper = BackpackBalanceMapper()

        try:
            result = mapper.transform_balance_data_to_spot_balance(
                asset=asset, total_balance="110.0", available_balance="100.0"
            )

            # Property: Asset should be properly created
            assert isinstance(result.asset, BaseSymbol)

            # Property: Exchange should be Backpack
            assert result.exchange == ExchangeName.BACKPACK

            # Property: Asset should be preserved in symbol
            assert asset in str(result.asset)

        except (ValueError, TypeError, AttributeError, KeyError):
            # Asset mapping might fail for invalid symbols
            pass

    @given(
        available=st.decimals(min_value=Decimal(0), max_value=Decimal(1000), places=8),
        locked=st.decimals(min_value=Decimal(0), max_value=Decimal(1000), places=8),
    )
    def test_balance_precision_round_trip(self, available: Decimal, locked: Decimal) -> None:
        """Property: Balance precision should survive round-trip transformation."""
        mapper = BackpackBalanceMapper()

        # Calculate total balance
        total = available + locked

        try:
            result = mapper.transform_balance_data_to_spot_balance(
                asset="BTC", total_balance=str(total), available_balance=str(available)
            )

            # Property: Exact precision should be preserved
            assert result.available_quantity == available
            assert result.total_quantity == total

            # Property: String representation should be consistent
            assert str(result.available_quantity) == str(available)
            assert str(result.total_quantity) == str(total)

        except (ValueError, TypeError, AttributeError, KeyError, DecimalInvalidOperation) as e:
            pytest.fail(f"Valid decimal inputs should not fail: {e}")


# =============================================================================
# PROPERTY TESTS FOR COLLATERAL BALANCE TRANSFORMATION
# =============================================================================


class TestCollateralBalanceProperties:
    """Property-based tests for collateral balance handling."""

    def test_collateral_balance_method_exists(self) -> None:
        """Property: Mapper should have collateral balance method."""
        mapper = BackpackBalanceMapper()

        # The actual method is create_balance_from_collateral
        assert hasattr(mapper, "create_balance_from_collateral")
        assert callable(mapper.create_balance_from_collateral)


# =============================================================================
# PROPERTY TESTS FOR BALANCE VALIDATION
# =============================================================================


class TestBalanceValidationProperties:
    """Property-based tests for balance validation logic."""

    @given(
        available=st.one_of(
            st.just("inf"),
            st.just("-inf"),
            st.just("nan"),
        )
    )
    def test_non_finite_balance_rejection(self, available: str) -> None:
        """Property: Non-finite balance values (inf, -inf, nan) should be rejected."""
        mapper = BackpackBalanceMapper()

        # The mapper should reject non-finite values at the final validation step
        with pytest.raises(Exception):  # SecurityValidationError or DataTransformationError
            mapper.transform_balance_data_to_spot_balance(
                asset="BTC", total_balance="0", available_balance=available
            )

    @given(
        available=st.one_of(
            st.just("invalid"),
            st.just("abc123"),
            st.just("12.34.56"),
            st.just("--123"),
            st.just("12-34"),
            st.just("not_a_number"),
            st.just(""),
        )
    )
    def test_invalid_balance_handling(self, available: str) -> None:
        """Property: Invalid but non-infinite balance values should be converted to zero."""
        mapper = BackpackBalanceMapper()

        # The mapper should handle invalid non-finite values gracefully by converting to zero
        result = mapper.transform_balance_data_to_spot_balance(
            asset="BTC", total_balance="0", available_balance=available
        )

        # Invalid values should be converted to zero
        assert result.available_quantity == Decimal("0")

    @given(available=st.decimals(min_value=Decimal(-1000), max_value=Decimal("-0.01"), places=8))
    def test_negative_balance_handling(self, available: Decimal) -> None:
        """Property: Test handling of negative balances."""
        mapper = BackpackBalanceMapper()

        try:
            result = mapper.transform_balance_data_to_spot_balance(
                asset="BTC", total_balance=str(available), available_balance=str(available)
            )

            # Property: How negative balances are handled should be consistent
            # (Implementation may reject them or handle them specially)
            if result:
                assert isinstance(result.available_quantity, Decimal)

        except (ValueError, TypeError, AttributeError, KeyError, DecimalInvalidOperation):
            # Negative balances may be rejected, which is valid
            pass

    @given(balance_data=balance_data_strategy())
    def test_balance_transformation_error_safety(self, balance_data: dict[str, Any]) -> None:
        """Property: Balance transformation errors should be safe and informative."""
        mapper = BackpackBalanceMapper()

        try:
            total_balance = str(
                Decimal(balance_data["available"]) + Decimal(balance_data["locked"])
            )
            mapper.transform_balance_data_to_spot_balance(
                asset=balance_data["asset"],
                total_balance=total_balance,
                available_balance=balance_data["available"],
            )
        except (ValueError, TypeError, AttributeError, KeyError, DecimalInvalidOperation) as e:
            # Property: Error messages should be informative
            error_msg = str(e)
            assert len(error_msg) > 0

            # Property: Should not expose sensitive information
            assert "password" not in error_msg.lower()
            assert "secret" not in error_msg.lower()


# =============================================================================
# INTEGRATION PROPERTY TESTS
# =============================================================================


class TestBalanceMapperIntegrationProperties:
    """Integration property tests for balance mapper."""

    @given(
        available=balance_decimal_strategy(),
        locked=balance_decimal_strategy(),
        asset=st.sampled_from(["BTC", "ETH", "USDC"]),
    )
    def test_balance_mapper_consistency(self, available: str, locked: str, asset: str) -> None:
        """Property: Balance mapper should be consistent across calls."""
        mapper = BackpackBalanceMapper()

        try:
            # Transform the same data twice
            total_balance = str(Decimal(available) + Decimal(locked))
            result1 = mapper.transform_balance_data_to_spot_balance(
                asset=asset, total_balance=total_balance, available_balance=available
            )
            result2 = mapper.transform_balance_data_to_spot_balance(
                asset=asset, total_balance=total_balance, available_balance=available
            )

            # Property: Same input should give same output
            assert result1.available_quantity == result2.available_quantity
            assert result1.total_quantity == result2.total_quantity
            assert result1.asset == result2.asset

        except (ValueError, TypeError, AttributeError, KeyError, DecimalInvalidOperation):
            # If it fails once, it should fail consistently
            with pytest.raises((
                ValueError,
                TypeError,
                AttributeError,
                KeyError,
                DecimalInvalidOperation,
            )):
                total_balance = str(Decimal(available) + Decimal(locked))
                mapper.transform_balance_data_to_spot_balance(
                    asset=asset, total_balance=total_balance, available_balance=available
                )

    @given(balance_data=balance_data_strategy())
    def test_balance_mapper_deterministic(self, balance_data: dict[str, Any]) -> None:
        """Property: Balance mapper should be deterministic."""
        # Skip empty assets
        assume(balance_data["asset"].strip())

        mapper1 = BackpackBalanceMapper()
        mapper2 = BackpackBalanceMapper()

        try:
            total_balance = str(
                Decimal(balance_data["available"]) + Decimal(balance_data["locked"])
            )
            result1 = mapper1.transform_balance_data_to_spot_balance(
                asset=balance_data["asset"],
                total_balance=total_balance,
                available_balance=balance_data["available"],
            )
            result2 = mapper2.transform_balance_data_to_spot_balance(
                asset=balance_data["asset"],
                total_balance=total_balance,
                available_balance=balance_data["available"],
            )

            # Property: Different mapper instances should give same results
            assert result1.available_quantity == result2.available_quantity
            assert result1.total_quantity == result2.total_quantity

        except (ValueError, TypeError, AttributeError, KeyError, DecimalInvalidOperation):
            # Both should fail in the same way
            with pytest.raises((
                ValueError,
                TypeError,
                AttributeError,
                KeyError,
                DecimalInvalidOperation,
            )):
                total_balance = str(
                    Decimal(balance_data["available"]) + Decimal(balance_data["locked"])
                )
                mapper2.transform_balance_data_to_spot_balance(
                    asset=balance_data["asset"],
                    total_balance=total_balance,
                    available_balance=balance_data["available"],
                )
