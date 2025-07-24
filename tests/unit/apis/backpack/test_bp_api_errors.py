"""Unit tests for Backpack API error codes enum.

Tests validation, enum completeness, and string representation.
"""

from __future__ import annotations

import pytest

from cyberdelta.apis.backpack.bp_api_errors import BackpackAPIErrorCode


pytestmark = pytest.mark.timing


class TestBackpackAPIErrorCode:
    """Unit tests for BackpackAPIErrorCode enum."""

    def test_all_error_codes_exist(self) -> None:
        """Test that all expected error codes are defined in the enum."""
        expected_codes = [
            "FORBIDDEN",
            "INVALID_CLIENT_REQUEST",
            "INVALID_SIGNATURE",
            "SERVER_ERROR",
            "UNAUTHORIZED",
            "TIMEOUT",
            "TOO_MANY_REQUESTS",
            "RESOURCE_NOT_FOUND",
            "MAINTENANCE",
            "INVALID_QUANTITY",
            "ORDER_LIMIT",
            "INVALID_ORDER",
            "INVALID_PRICE",
            "INVALID_MARKET",
            "INVALID_SOURCE",
            "INSUFFICIENT_FUNDS",
            "INSUFFICIENT_MARGIN",
            "POSITION_LIMIT",
            "ACCOUNT_LIQUIDATING",
            "TRADING_PAUSED",
            "INVALID_ASSET",
            "INVALID_SYMBOL",
            "INVALID_POSITION_ID",
            "BORROW_REQUIRES_LEND_REDEEM",
            "LEND_REQUIRES_BORROW_REPAY",
            "INSUFFICIENT_SUPPLY",
            "BORROW_LIMIT",
            "LEND_LIMIT",
            "MAX_LEVERAGE_REACHED",
            "PRECONDITION_FAILED",
            "NOT_IMPLEMENTED",
        ]

        # Check that all expected codes exist
        for code in expected_codes:
            assert hasattr(BackpackAPIErrorCode, code), f"Missing error code: {code}"

        # Check that we have the expected number of codes
        assert len(BackpackAPIErrorCode) == len(expected_codes)

    def test_error_code_values_match_names(self) -> None:
        """Test that error code values match their enum names."""
        for error_code in BackpackAPIErrorCode:
            assert error_code.value == error_code.name

    def test_enum_members_are_strings(self) -> None:
        """Test that all enum values are strings."""
        for error_code in BackpackAPIErrorCode:
            assert isinstance(error_code.value, str)
            assert len(error_code.value) > 0

    @pytest.mark.parametrize("error_code", list(BackpackAPIErrorCode))
    def test_individual_error_codes(self, error_code: BackpackAPIErrorCode) -> None:
        """Test individual error code properties."""
        # Test that the error code has a valid name
        assert isinstance(error_code.name, str)
        assert len(error_code.name) > 0

        # Test that the error code has a valid value
        assert isinstance(error_code.value, str)
        assert len(error_code.value) > 0

        # Test that name and value match
        assert error_code.name == error_code.value

    def test_authentication_related_errors(self) -> None:
        """Test authentication and authorization related error codes."""
        auth_errors = [
            BackpackAPIErrorCode.UNAUTHORIZED,
            BackpackAPIErrorCode.FORBIDDEN,
            BackpackAPIErrorCode.INVALID_SIGNATURE,
        ]

        for error in auth_errors:
            assert isinstance(error, BackpackAPIErrorCode)
            assert error.value in ["UNAUTHORIZED", "FORBIDDEN", "INVALID_SIGNATURE"]

    def test_rate_limiting_errors(self) -> None:
        """Test rate limiting related error codes."""
        rate_limit_errors = [
            BackpackAPIErrorCode.TOO_MANY_REQUESTS,
            BackpackAPIErrorCode.TIMEOUT,
        ]

        for error in rate_limit_errors:
            assert isinstance(error, BackpackAPIErrorCode)
            assert error.value in ["TOO_MANY_REQUESTS", "TIMEOUT"]

    def test_trading_related_errors(self) -> None:
        """Test trading operation related error codes."""
        trading_errors = [
            BackpackAPIErrorCode.INVALID_QUANTITY,
            BackpackAPIErrorCode.INVALID_PRICE,
            BackpackAPIErrorCode.INVALID_ORDER,
            BackpackAPIErrorCode.ORDER_LIMIT,
            BackpackAPIErrorCode.INSUFFICIENT_FUNDS,
            BackpackAPIErrorCode.INSUFFICIENT_MARGIN,
            BackpackAPIErrorCode.POSITION_LIMIT,
            BackpackAPIErrorCode.TRADING_PAUSED,
            BackpackAPIErrorCode.ACCOUNT_LIQUIDATING,
        ]

        for error in trading_errors:
            assert isinstance(error, BackpackAPIErrorCode)

    def test_market_data_errors(self) -> None:
        """Test market data related error codes."""
        market_errors = [
            BackpackAPIErrorCode.INVALID_MARKET,
            BackpackAPIErrorCode.INVALID_SYMBOL,
            BackpackAPIErrorCode.INVALID_ASSET,
            BackpackAPIErrorCode.RESOURCE_NOT_FOUND,
        ]

        for error in market_errors:
            assert isinstance(error, BackpackAPIErrorCode)

    def test_lending_borrowing_errors(self) -> None:
        """Test lending and borrowing related error codes."""
        lending_errors = [
            BackpackAPIErrorCode.BORROW_REQUIRES_LEND_REDEEM,
            BackpackAPIErrorCode.LEND_REQUIRES_BORROW_REPAY,
            BackpackAPIErrorCode.INSUFFICIENT_SUPPLY,
            BackpackAPIErrorCode.BORROW_LIMIT,
            BackpackAPIErrorCode.LEND_LIMIT,
            BackpackAPIErrorCode.MAX_LEVERAGE_REACHED,
        ]

        for error in lending_errors:
            assert isinstance(error, BackpackAPIErrorCode)

    def test_system_errors(self) -> None:
        """Test system and infrastructure related error codes."""
        system_errors = [
            BackpackAPIErrorCode.SERVER_ERROR,
            BackpackAPIErrorCode.MAINTENANCE,
            BackpackAPIErrorCode.NOT_IMPLEMENTED,
            BackpackAPIErrorCode.PRECONDITION_FAILED,
        ]

        for error in system_errors:
            assert isinstance(error, BackpackAPIErrorCode)

    def test_client_errors(self) -> None:
        """Test client request related error codes."""
        client_errors = [
            BackpackAPIErrorCode.INVALID_CLIENT_REQUEST,
            BackpackAPIErrorCode.INVALID_SOURCE,
            BackpackAPIErrorCode.INVALID_POSITION_ID,
        ]

        for error in client_errors:
            assert isinstance(error, BackpackAPIErrorCode)

    def test_enum_iteration(self) -> None:
        """Test that the enum can be iterated over."""
        error_codes = list(BackpackAPIErrorCode)
        assert len(error_codes) > 0

        # Test that all items are BackpackAPIErrorCode instances
        for error_code in error_codes:
            assert isinstance(error_code, BackpackAPIErrorCode)

    def test_enum_membership(self) -> None:
        """Test enum membership checks."""
        # Test that values exist in enum
        assert BackpackAPIErrorCode.FORBIDDEN in BackpackAPIErrorCode
        assert BackpackAPIErrorCode.SERVER_ERROR in BackpackAPIErrorCode
        assert BackpackAPIErrorCode.UNAUTHORIZED in BackpackAPIErrorCode

    def test_enum_lookup_by_value(self) -> None:
        """Test that enum members can be looked up by value."""
        # Test successful lookups
        assert BackpackAPIErrorCode("FORBIDDEN") == BackpackAPIErrorCode.FORBIDDEN
        assert BackpackAPIErrorCode("SERVER_ERROR") == BackpackAPIErrorCode.SERVER_ERROR
        assert BackpackAPIErrorCode("UNAUTHORIZED") == BackpackAPIErrorCode.UNAUTHORIZED

        # Test lookup failure
        with pytest.raises(
            ValueError, match="'NONEXISTENT_ERROR' is not a valid BackpackAPIErrorCode"
        ):
            BackpackAPIErrorCode("NONEXISTENT_ERROR")

    def test_enum_string_representation(self) -> None:
        """Test string representation of enum members."""
        error_code = BackpackAPIErrorCode.FORBIDDEN

        # Test str() representation
        assert str(error_code) == "BackpackAPIErrorCode.FORBIDDEN"

        # Test repr() representation
        assert repr(error_code) == "<BackpackAPIErrorCode.FORBIDDEN: 'FORBIDDEN'>"

    def test_enum_comparison(self) -> None:
        """Test comparison operations on enum members."""
        # Test equality
        assert BackpackAPIErrorCode.FORBIDDEN == BackpackAPIErrorCode.FORBIDDEN
        # Test inequality with different enum members
        forbidden = BackpackAPIErrorCode.FORBIDDEN
        unauthorized = BackpackAPIErrorCode.UNAUTHORIZED
        assert forbidden != unauthorized

        # Test identity
        forbidden1 = BackpackAPIErrorCode.FORBIDDEN
        forbidden2 = BackpackAPIErrorCode.FORBIDDEN
        assert forbidden1 is forbidden2

    def test_enum_hashing(self) -> None:
        """Test that enum members are hashable."""
        error_codes_set = {
            BackpackAPIErrorCode.FORBIDDEN,
            BackpackAPIErrorCode.UNAUTHORIZED,
            BackpackAPIErrorCode.SERVER_ERROR,
        }

        assert len(error_codes_set) == 3
        assert BackpackAPIErrorCode.FORBIDDEN in error_codes_set
