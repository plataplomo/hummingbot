"""Unit tests for BackpackResponseHandler funding rates response functionality."""

from typing import Any, cast

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_response_handler import BackpackResponseHandler
from cyberdelta.apis.backpack.models.bp_raw_funding import BackpackRawFundingRate
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.utils.typing import ParsedJsonResponse


# Import fixtures from the shared conftest
pytest_plugins = ["tests.unit.apis.backpack.conftest_response_handler"]

# Type aliases for clarity
type RawJsonPrim = str | int | float | bool | None
type RawJson = dict[str, RawJson] | list[RawJson] | RawJsonPrim
type RawJsonResponse = RawJson


class TestHandleGetFundingRateResponse:
    """Tests for BackpackResponseHandler.handle_get_funding_rate_response."""

    def test_valid(self, valid_raw_funding_rate: dict[str, Any], symbol_perp: str) -> None:
        """Test handling a valid raw funding rate response."""
        funding_rate: BackpackRawFundingRate = (
            BackpackResponseHandler.handle_get_funding_rate_response(
                cast("ParsedJsonResponse", valid_raw_funding_rate),
                symbol_perp,
                200,
                {},
            )
        )
        assert isinstance(funding_rate, BackpackRawFundingRate)
        assert funding_rate.symbol == symbol_perp
        assert funding_rate.funding_rate == "0.000123"
        assert funding_rate.mark_price == "140.00"
        assert funding_rate.index_price == "139.90"
        assert funding_rate.time == 1678887000000

    def test_validation_error_missing_field(self, symbol_perp: str) -> None:
        """Test funding rate response missing required field."""
        raw_data = {
            "symbol": symbol_perp,
            # Missing 'rate' field
            "markPrice": "140.00",
            "indexPrice": "139.90",
            "time": 1678887000000,
        }
        with pytest.raises(APIError) as exc_info:
            BackpackResponseHandler.handle_get_funding_rate_response(
                cast("ParsedJsonResponse", raw_data),
                symbol_perp,
                200,
                {},
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert f"funding rate ({symbol_perp})" in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)

    def test_validation_error_invalid_rate_format(self, symbol_perp: str) -> None:
        """Test funding rate response with invalid rate format."""
        raw_data = {
            "symbol": symbol_perp,
            "rate": "invalid_rate",
            "markPrice": "140.00",
            "indexPrice": "139.90",
            "time": 1678887000000,
        }
        with pytest.raises(APIError) as exc_info:
            BackpackResponseHandler.handle_get_funding_rate_response(
                cast("ParsedJsonResponse", raw_data),
                symbol_perp,
                200,
                {},
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert isinstance(exc_info.value.original_exception, ValidationError)

    def test_invalid_top_level_type(self, symbol_perp: str) -> None:
        """Test funding rate response with wrong top-level type."""
        raw_data = ["invalid"]
        with pytest.raises(APIError) as exc_info:
            BackpackResponseHandler.handle_get_funding_rate_response(
                cast("ParsedJsonResponse", raw_data),
                symbol_perp,
                400,
                {},
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "expected dict" in exc_info.value.message
        assert "got str" in exc_info.value.message

    def test_extra_fields_ignored(self, symbol_perp: str) -> None:
        """Test that extra fields in funding rate response cause ValidationError.

        This is due to extra='forbid' in the validation model.
        """
        raw_data = {
            "symbol": symbol_perp,
            "rate": "0.000123",
            "markPrice": "140.00",
            "indexPrice": "139.90",
            "time": 1678887000000,
            "extraField": "should_be_ignored",
        }
        with pytest.raises(APIError) as exc_info:
            BackpackResponseHandler.handle_get_funding_rate_response(
                cast("ParsedJsonResponse", raw_data),
                symbol_perp,
                200,
                {},
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert isinstance(exc_info.value.original_exception, ValidationError)


class TestFundingRateEdgeCases:
    """Tests for additional edge cases in funding rate response handling."""

    def test_funding_rate_with_zero_rate(self, symbol_perp: str) -> None:
        """Test funding rate response with zero rate."""
        raw_data = {
            "symbol": symbol_perp,
            "rate": "0.0",
            "markPrice": "140.00",
            "indexPrice": "139.90",
            "time": 1678887000000,
        }
        funding_rate = BackpackResponseHandler.handle_get_funding_rate_response(
            cast("ParsedJsonResponse", raw_data),
            symbol_perp,
            200,
            {},
        )
        assert funding_rate.funding_rate == "0.0"

    def test_funding_rate_with_negative_rate(self, symbol_perp: str) -> None:
        """Test funding rate response with negative rate."""
        raw_data = {
            "symbol": symbol_perp,
            "rate": "-0.000456",
            "markPrice": "140.00",
            "indexPrice": "139.90",
            "time": 1678887000000,
        }
        funding_rate = BackpackResponseHandler.handle_get_funding_rate_response(
            cast("ParsedJsonResponse", raw_data),
            symbol_perp,
            200,
            {},
        )
        assert funding_rate.funding_rate == "-0.000456"

    def test_funding_rate_with_high_precision(self, symbol_perp: str) -> None:
        """Test funding rate response with high precision decimal values."""
        raw_data = {
            "symbol": symbol_perp,
            "rate": "0.000123456789",
            "markPrice": "140.123456789",
            "indexPrice": "139.987654321",
            "time": 1678887000000,
        }
        funding_rate = BackpackResponseHandler.handle_get_funding_rate_response(
            cast("ParsedJsonResponse", raw_data),
            symbol_perp,
            200,
            {},
        )
        assert funding_rate.funding_rate == "0.000123456789"
        assert funding_rate.mark_price == "140.123456789"
        assert funding_rate.index_price == "139.987654321"
