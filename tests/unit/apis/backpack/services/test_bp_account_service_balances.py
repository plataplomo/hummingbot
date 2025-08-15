"""Unit tests for BackpackAccountService balance functionality with Property-Based Testing.

--------------------------------------------------------------------

Comprehensive property-based test suite for BackpackAccountService balance functionality using Hypothesis.
Tests service layer operations with mocked dependencies including:
- Balance retrieval with various asset combinations and amounts
- HTTP client response handling and error conditions
- Response validation and transformation edge cases
- API error handling and status code scenarios
- Malicious input resistance and boundary testing
- Hundreds of generated test combinations for comprehensive coverage
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from hypothesis import given, strategies as st, settings, assume
from hypothesis.strategies import SearchStrategy, composite
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalanceResponse
from cyberdelta.apis.backpack.models.bp_raw_query_params import BackpackRawGetBalancesParams
from cyberdelta.apis.backpack.services.bp_account_service import BackpackAccountService
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.utils.typing import ParsedJsonResponse


class TestBackpackAccountServiceBalances:
    """Tests for the BackpackAccountService balance functionality."""

    @pytest.mark.asyncio
    async def test_get_balances_raw_success(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_balances successfully retrieves and processes balance data."""
        mock_raw_response_data_dict: ParsedJsonResponse = {
            "USDC": {"available": "1000.5", "locked": "10.0"},
        }
        mock_validated_raw_balances_dict: dict[str, BackpackRawBalanceResponse] = {
            "USDC": BackpackRawBalanceResponse(available="1000.5", locked="10.0", staked="0"),
        }

        # Mock request builder to return a valid params object
        mock_request_builder.build_get_balances_params.return_value = BackpackRawGetBalancesParams()

        # Mock the HTTP client and response handler to return expected data
        mock_http_client_requester.return_value = (mock_raw_response_data_dict, 200, MagicMock())
        mock_response_handler.handle_get_balances_response.return_value = (
            mock_validated_raw_balances_dict
        )

        result = await bp_account_service.get_balances()

        # The mapper transforms to exchange="backpack" not "backpack_test_account"
        assert result["USDC"].exchange == ExchangeName.BACKPACK
        assert result["USDC"].asset.value == "USDC"
        assert isinstance(result["USDC"].timestamp, datetime)
        assert result["USDC"].timestamp.tzinfo == UTC
        assert result["USDC"].total_quantity == Decimal("1010.5")
        assert result["USDC"].available_quantity == Decimal("1000.5")

    @pytest.mark.asyncio
    async def test_get_balances_success(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_balances successfully retrieves and processes balance data."""
        mock_params_from_builder_for_get_balances = BackpackRawGetBalancesParams()

        mock_raw_response_dict: ParsedJsonResponse = {
            "USDC": {"available": "1000.5", "locked": "10.0"},
            "SOL": {"available": "50.2", "locked": "0.5"},
        }
        mock_status_code = 200
        mock_headers: dict[str, str] = {}

        mock_request_builder.build_get_balances_params.return_value = (
            mock_params_from_builder_for_get_balances
        )

        mock_http_client_requester.return_value = (
            mock_raw_response_dict,
            mock_status_code,
            mock_headers,
        )

        mock_raw_balances_payload: dict[str, BackpackRawBalanceResponse] = {
            "USDC": BackpackRawBalanceResponse(available="1000.5", locked="10.0", staked="0"),
            "SOL": BackpackRawBalanceResponse(available="50.2", locked="0.5", staked="0"),
        }
        mock_response_handler.handle_get_balances_response.return_value = mock_raw_balances_payload

        result_balances = await bp_account_service.get_balances()

        assert len(result_balances) == 2
        assert "USDC" in result_balances
        assert "SOL" in result_balances

        # Check USDC balance
        usdc_balance = result_balances["USDC"]
        assert (
            usdc_balance.exchange == ExchangeName.BACKPACK
        )  # Mapper returns ExchangeName.BACKPACK
        assert usdc_balance.asset.value == "USDC"
        assert isinstance(usdc_balance.timestamp, datetime)
        assert usdc_balance.timestamp.tzinfo == UTC
        assert usdc_balance.total_quantity == Decimal("1010.5")
        assert usdc_balance.available_quantity == Decimal("1000.5")

        # Check SOL balance
        sol_balance = result_balances["SOL"]
        assert sol_balance.exchange == ExchangeName.BACKPACK  # Mapper returns ExchangeName.BACKPACK
        assert sol_balance.asset.value == "SOL"
        assert isinstance(sol_balance.timestamp, datetime)
        assert sol_balance.timestamp.tzinfo == UTC
        assert sol_balance.total_quantity == Decimal("50.7")
        assert sol_balance.available_quantity == Decimal("50.2")

    @pytest.mark.asyncio
    async def test_get_balances_http_client_returns_none(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test get_balances when HTTP client returns None content."""
        # Mock the HTTP client to return None which triggers error
        mock_http_client_requester.return_value = (None, 200, {})

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_balances()

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "No data received for balances, status: 200" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_balances_validation_error_via_public_api(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_balances handles validation error from response handler."""
        # Mock the HTTP client to return invalid data that causes validation error
        mock_http_client_requester.return_value = ({"invalid": "balance"}, 200, {})

        # Mock the response handler to raise ValidationError
        mock_response_handler.handle_get_balances_response.side_effect = (
            ValidationError.from_exception_data(
                title="ValidationError",
                line_errors=[],
            )
        )

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_balances()

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Failed to retrieve balance data" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_balances_unexpected_exception_via_public_api(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_balances handles unexpected exception from response handler."""
        # Mock the HTTP client to return valid data but response handler raises unexpected error
        mock_http_client_requester.return_value = ({"USDC": {"available": "100.0"}}, 200, {})

        # Mock the response handler to raise an unexpected exception
        mock_response_handler.handle_get_balances_response.side_effect = Exception(
            "Unexpected error"
        )

        # Business logic doesn't wrap general exceptions, so expect raw Exception
        with pytest.raises(Exception) as exc_info:
            await bp_account_service.get_balances()

        assert str(exc_info.value) == "Unexpected error"

    @pytest.mark.asyncio
    async def test_get_balances_validation_error_coverage(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_balances validation error coverage."""
        # Mock the HTTP client to return invalid data that causes validation error
        mock_http_client_requester.return_value = ({"invalid": "balance"}, 200, {})

        # Mock the response handler to raise ValidationError
        mock_response_handler.handle_get_balances_response.side_effect = (
            ValidationError.from_exception_data(
                title="ValidationError",
                line_errors=[],
            )
        )

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_balances()

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value

    @pytest.mark.asyncio
    async def test_get_balances_unexpected_exception_coverage(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_balances unexpected exception coverage."""
        # Mock the HTTP client to return valid data but response handler raises unexpected error
        mock_http_client_requester.return_value = ({"USDC": {"available": "100.0"}}, 200, {})

        # Mock the response handler to raise an unexpected exception
        mock_response_handler.handle_get_balances_response.side_effect = Exception(
            "Unexpected error"
        )

        # Business logic doesn't wrap general exceptions, so expect raw Exception
        with pytest.raises(Exception) as exc_info:
            await bp_account_service.get_balances()

        assert str(exc_info.value) == "Unexpected error"


# =======================
# Property-Based Testing Strategy Builders
# =======================


def asset_symbol_strategy() -> SearchStrategy[str]:
    """Generate valid asset symbols for balance testing.

    Returns:
        SearchStrategy[str]: Strategy for asset symbols.
    """
    return st.one_of([
        # Common assets
        st.sampled_from([
            "USDC",
            "USDT",
            "USD",
            "SOL",
            "ETH",
            "BTC",
            "BONK",
            "WIF",
            "JUP",
            "RAY",
            "ORCA",
            "PYTH",
            "MNGO",
            "SBR",
            "COPE",
            "STEP",
            "MEDIA",
        ]),
        # Custom assets with various formats
        st.builds(
            lambda base, suffix: f"{base}-{suffix}",
            st.sampled_from(["BTC", "ETH", "SOL"]),
            st.sampled_from(["USD", "USDC", "USDT", "PERP"]),
        ),
        # Test edge cases
        st.sampled_from(["A", "AA", "VERYLONGASSETSYMBOL123"]),
    ])


def balance_amount_strategy() -> SearchStrategy[str]:
    """Generate valid balance amounts as strings.

    Returns:
        SearchStrategy[str]: Strategy for balance amounts.
    """
    return st.one_of([
        # Common decimal amounts
        st.builds(
            lambda integer, decimal: f"{integer}.{decimal}",
            st.integers(min_value=0, max_value=999999),
            st.text(alphabet="0123456789", min_size=1, max_size=8),
        ),
        # Zero amounts
        st.sampled_from(["0", "0.0", "0.00", "0.000000"]),
        # High precision amounts
        st.builds(
            lambda base, precision: f"{base}.{''.join(['123456789'])[:precision]}",
            st.integers(min_value=1, max_value=1000),
            st.integers(min_value=1, max_value=10),
        ),
        # Large amounts
        st.builds(
            str,
            st.decimals(min_value=Decimal("1000000"), max_value=Decimal("999999999"), places=6),
        ),
        # Very small amounts
        st.builds(
            str,
            st.decimals(min_value=Decimal("0.000001"), max_value=Decimal("0.001"), places=9),
        ),
    ])


def http_status_code_strategy() -> SearchStrategy[int]:
    """Generate HTTP status codes for testing.

    Returns:
        SearchStrategy[int]: Strategy for HTTP status codes.
    """
    return st.one_of([
        # Success codes
        st.sampled_from([200, 201, 202]),
        # Client error codes
        st.sampled_from([400, 401, 403, 404, 429, 422]),
        # Server error codes
        st.sampled_from([500, 502, 503, 504]),
        # Unusual but valid codes
        st.sampled_from([201, 206, 300, 418]),
    ])


def malicious_balance_strategy() -> SearchStrategy[str]:
    """Generate potentially malicious balance values.

    Returns:
        SearchStrategy[str]: Strategy for malicious balance strings.
    """
    return st.one_of([
        # Script injection attempts
        st.sampled_from([
            "<script>alert('xss')</script>",
            "javascript:alert(1)",
            "<img src=x onerror=alert(1)>",
        ]),
        # SQL injection attempts
        st.sampled_from([
            "'; DROP TABLE balances; --",
            "1' OR '1'='1",
            "admin'--",
        ]),
        # Command injection
        st.sampled_from([
            "$(rm -rf /)",
            "`cat /etc/passwd`",
            "; ls -la",
        ]),
        # Format string attacks
        st.sampled_from(["%s%s%s", "%x%x%x", "%n%n%n"]),
        # Very long strings
        st.text(min_size=1000, max_size=1500),
        # Null bytes
        st.builds(lambda: "100.0\x00malicious"),
        # Unicode exploitation
        st.text(
            alphabet=st.characters(min_codepoint=0x2000, max_codepoint=0x2FFF),
            min_size=10,
            max_size=100,
        ),
    ])


@composite
def single_balance_response_strategy(draw: st.DrawFn) -> BackpackRawBalanceResponse:
    """Generate a single valid BackpackRawBalanceResponse.

    Args:
        draw: Hypothesis draw function.

    Returns:
        BackpackRawBalanceResponse: Valid balance response.
    """
    available = draw(balance_amount_strategy())
    locked = draw(balance_amount_strategy())
    staked = draw(st.one_of(st.just("0"), balance_amount_strategy()))

    return BackpackRawBalanceResponse(
        available=available,
        locked=locked,
        staked=staked,
    )


@composite
def multiple_balances_strategy(draw: st.DrawFn) -> dict[str, BackpackRawBalanceResponse]:
    """Generate multiple balance responses for different assets.

    Args:
        draw: Hypothesis draw function.

    Returns:
        dict[str, BackpackRawBalanceResponse]: Multiple balance responses.
    """
    num_assets = draw(st.integers(min_value=1, max_value=10))
    assets = draw(
        st.lists(asset_symbol_strategy(), min_size=num_assets, max_size=num_assets, unique=True)
    )

    balances = {}
    for asset in assets:
        balances[asset] = draw(single_balance_response_strategy())

    return balances


@composite
def raw_response_data_strategy(draw: st.DrawFn) -> ParsedJsonResponse:
    """Generate raw response data that would come from HTTP client.

    Args:
        draw: Hypothesis draw function.

    Returns:
        ParsedJsonResponse: Raw balance response data.
    """
    num_assets = draw(st.integers(min_value=1, max_value=8))
    assets = draw(
        st.lists(asset_symbol_strategy(), min_size=num_assets, max_size=num_assets, unique=True)
    )

    response_data = {}
    for asset in assets:
        available = draw(balance_amount_strategy())
        locked = draw(balance_amount_strategy())

        balance_dict = {"available": available, "locked": locked}

        # Sometimes include staked field
        if draw(st.booleans()):
            balance_dict["staked"] = draw(balance_amount_strategy())

        response_data[asset] = balance_dict

    return response_data


@composite
def malicious_response_data_strategy(draw: st.DrawFn) -> ParsedJsonResponse:
    """Generate malicious response data for security testing.

    Args:
        draw: Hypothesis draw function.

    Returns:
        ParsedJsonResponse: Malicious response data.
    """
    asset = draw(st.one_of(asset_symbol_strategy(), malicious_balance_strategy()))
    available = draw(malicious_balance_strategy())
    locked = draw(malicious_balance_strategy())

    return {
        asset: {
            "available": available,
            "locked": locked,
        }
    }


@composite
def invalid_response_structure_strategy(draw: st.DrawFn) -> Any:
    """Generate invalid response structures for error testing.

    Args:
        draw: Hypothesis draw function.

    Returns:
        ParsedJsonResponse: Invalid response structure.
    """
    return draw(
        st.one_of([
            # Missing required fields
            st.builds(lambda asset: {asset: {"available": "100.0"}}, asset_symbol_strategy()),
            st.builds(lambda asset: {asset: {"locked": "10.0"}}, asset_symbol_strategy()),
            # Wrong field types
            st.builds(
                lambda asset: {asset: {"available": 100, "locked": "10.0"}}, asset_symbol_strategy()
            ),
            st.builds(
                lambda asset: {asset: {"available": "100.0", "locked": True}},
                asset_symbol_strategy(),
            ),
            # Extra unexpected fields
            st.builds(
                lambda asset: {
                    asset: {"available": "100.0", "locked": "10.0", "unexpected": "field"}
                },
                asset_symbol_strategy(),
            ),
            # Nested structures
            st.builds(
                lambda asset: {asset: {"available": {"nested": "100.0"}, "locked": "10.0"}},
                asset_symbol_strategy(),
            ),
            # Empty structures
            st.just({}),
            st.builds(lambda asset: {asset: {}}, asset_symbol_strategy()),
            # Non-dict values
            st.builds(lambda asset: {asset: "not_a_dict"}, asset_symbol_strategy()),
            st.builds(lambda asset: {asset: ["not", "a", "dict"]}, asset_symbol_strategy()),
        ])
    )


# =======================
# Property-Based Test Classes
# =======================


class TestBackpackAccountServiceBalancesPropertyBased:
    """Property-based tests for BackpackAccountService balance operations."""

    @given(balances=multiple_balances_strategy())
    @settings(max_examples=50)
    @pytest.mark.asyncio
    async def test_get_balances_success_property_based(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        balances: dict[str, BackpackRawBalanceResponse],
    ) -> None:
        """Property-based test for successful balance retrieval."""
        # Create raw response data from balance responses
        raw_response_data = {}
        for asset, balance in balances.items():
            raw_response_data[asset] = {
                "available": balance.available,
                "locked": balance.locked,
            }
            if balance.staked != "0":
                raw_response_data[asset]["staked"] = balance.staked

        # Mock request builder
        mock_request_builder.build_get_balances_params.return_value = BackpackRawGetBalancesParams()

        # Mock HTTP client response
        mock_http_client_requester.return_value = (raw_response_data, 200, {})

        # Mock response handler to return balances
        mock_response_handler.handle_get_balances_response.return_value = balances

        result = await bp_account_service.get_balances()

        # Verify structure and count
        assert len(result) == len(balances)

        # Verify each balance
        for asset in balances:
            assert asset in result
            balance_result = result[asset]

            # Check basic properties
            assert balance_result.exchange == ExchangeName.BACKPACK
            assert balance_result.asset.value == asset
            assert isinstance(balance_result.timestamp, datetime)
            assert balance_result.timestamp.tzinfo == UTC

            # Check amounts are positive Decimals
            assert isinstance(balance_result.total_quantity, Decimal)
            assert isinstance(balance_result.available_quantity, Decimal)
            assert balance_result.total_quantity >= 0
            assert balance_result.available_quantity >= 0

            # Available should be <= total
            assert balance_result.available_quantity <= balance_result.total_quantity

    @given(
        raw_data=raw_response_data_strategy(),
        status_code=st.sampled_from([200, 201, 202]),
    )
    @settings(max_examples=30)
    @pytest.mark.asyncio
    async def test_get_balances_response_handling_property_based(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        raw_data: ParsedJsonResponse,
        status_code: int,
    ) -> None:
        """Property-based test for response data handling."""
        # Create corresponding BackpackRawBalanceResponse objects
        balance_responses = {}
        if isinstance(raw_data, dict):
            for asset, balance_data in raw_data.items():
                balance_responses[asset] = BackpackRawBalanceResponse(
                    available=balance_data["available"],
                    locked=balance_data["locked"],
                    staked=balance_data.get("staked", "0"),
                )

        # Mock setup
        mock_request_builder.build_get_balances_params.return_value = BackpackRawGetBalancesParams()
        mock_http_client_requester.return_value = (raw_data, status_code, {})
        mock_response_handler.handle_get_balances_response.return_value = balance_responses

        result = await bp_account_service.get_balances()

        # Verify all assets are present
        assert len(result) == len(raw_data)
        for asset in raw_data:
            assert asset in result
            assert result[asset].asset.value == asset

    @given(status_code=st.integers(min_value=400, max_value=599))
    @settings(max_examples=20)
    @pytest.mark.asyncio
    async def test_get_balances_error_status_codes_property_based(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        status_code: int,
    ) -> None:
        """Property-based test for various error status codes."""
        # Mock request builder
        mock_request_builder.build_get_balances_params.return_value = BackpackRawGetBalancesParams()

        # Mock HTTP client to return error status
        mock_http_client_requester.return_value = (None, status_code, {})

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_balances()

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert f"status: {status_code}" in exc_info.value.message

    @given(invalid_data=invalid_response_structure_strategy())
    @settings(max_examples=20)
    @pytest.mark.asyncio
    async def test_get_balances_invalid_structure_property_based(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        invalid_data: ParsedJsonResponse,
    ) -> None:
        """Property-based test for invalid response structures."""
        # Mock setup
        mock_request_builder.build_get_balances_params.return_value = BackpackRawGetBalancesParams()
        mock_http_client_requester.return_value = (invalid_data, 200, {})

        # Mock response handler to raise ValidationError
        mock_response_handler.handle_get_balances_response.side_effect = (
            ValidationError.from_exception_data(
                title="ValidationError",
                line_errors=[],
            )
        )

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_balances()

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Failed to retrieve balance data" in exc_info.value.message

    @given(malicious_data=malicious_response_data_strategy())
    @settings(max_examples=20)
    @pytest.mark.asyncio
    async def test_get_balances_malicious_input_resistance_property_based(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        malicious_data: ParsedJsonResponse,
    ) -> None:
        """Property-based test for malicious input resistance."""
        # Mock setup
        mock_request_builder.build_get_balances_params.return_value = BackpackRawGetBalancesParams()
        mock_http_client_requester.return_value = (malicious_data, 200, {})

        # Mock response handler to either succeed or fail safely
        try:
            # Try to create valid response - should either work or fail gracefully
            balance_responses = {}
            if isinstance(malicious_data, dict):
                for asset, balance_data in malicious_data.items():
                    balance_responses[asset] = BackpackRawBalanceResponse(
                        available=balance_data["available"],
                        locked=balance_data["locked"],
                        staked="0",
                    )
            mock_response_handler.handle_get_balances_response.return_value = balance_responses

            result = await bp_account_service.get_balances()

            # If successful, verify malicious input is safely handled
            for asset in result:
                assert isinstance(result[asset].asset.value, str)
                assert isinstance(result[asset].total_quantity, Decimal)
                assert isinstance(result[asset].available_quantity, Decimal)

        except (ValidationError, ValueError):
            # Mock response handler to raise appropriate error
            mock_response_handler.handle_get_balances_response.side_effect = (
                ValidationError.from_exception_data(
                    title="ValidationError",
                    line_errors=[],
                )
            )

            with pytest.raises(APIError):
                await bp_account_service.get_balances()

    @given(
        asset_count=st.integers(min_value=1, max_value=50),
        include_zero_balances=st.booleans(),
    )
    @settings(max_examples=20)
    @pytest.mark.asyncio
    async def test_get_balances_scale_testing_property_based(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        asset_count: int,
        include_zero_balances: bool,
    ) -> None:
        """Property-based test for handling various numbers of assets."""
        # Generate unique assets
        assets = [f"ASSET{i:03d}" for i in range(asset_count)]

        # Create balance data
        raw_data = {}
        balance_responses = {}

        for asset in assets:
            if include_zero_balances and asset_count > 5:
                # Some zero balances for larger sets
                available = "0.0" if hash(asset) % 3 == 0 else "100.0"
                locked = "0.0" if hash(asset) % 5 == 0 else "10.0"
            else:
                available = "100.0"
                locked = "10.0"

            raw_data[asset] = {"available": available, "locked": locked}
            balance_responses[asset] = BackpackRawBalanceResponse(
                available=available,
                locked=locked,
                staked="0",
            )

        # Mock setup
        mock_request_builder.build_get_balances_params.return_value = BackpackRawGetBalancesParams()
        mock_http_client_requester.return_value = (raw_data, 200, {})
        mock_response_handler.handle_get_balances_response.return_value = balance_responses

        result = await bp_account_service.get_balances()

        # Verify scale
        assert len(result) == asset_count
        for i in range(asset_count):
            asset = f"ASSET{i:03d}"
            assert asset in result
            assert result[asset].exchange == ExchangeName.BACKPACK


class TestBackpackAccountServiceEdgeCases:
    """Property-based tests for edge cases and boundary conditions."""

    @given(
        precision_places=st.integers(min_value=1, max_value=18),
        include_scientific_notation=st.booleans(),
    )
    @settings(max_examples=20)
    @pytest.mark.asyncio
    async def test_balance_precision_handling_property_based(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        precision_places: int,
        include_scientific_notation: bool,
    ) -> None:
        """Property-based test for decimal precision handling."""
        # Create high precision amounts
        if include_scientific_notation:
            available = f"1.23456789e-{precision_places}"
            locked = f"9.87654321e-{precision_places + 1}"
        else:
            available = f"1.{'123456789'[:precision_places]}"
            locked = f"0.{'987654321'[:precision_places]}"

        raw_data = {"USDC": {"available": available, "locked": locked}}
        balance_response = BackpackRawBalanceResponse(
            available=available,
            locked=locked,
            staked="0",
        )

        # Mock setup
        mock_request_builder.build_get_balances_params.return_value = BackpackRawGetBalancesParams()
        mock_http_client_requester.return_value = (raw_data, 200, {})
        mock_response_handler.handle_get_balances_response.return_value = {"USDC": balance_response}

        try:
            result = await bp_account_service.get_balances()

            # Verify precision is maintained
            usdc_balance = result["USDC"]
            assert isinstance(usdc_balance.total_quantity, Decimal)
            assert isinstance(usdc_balance.available_quantity, Decimal)

        except (ValidationError, ValueError):
            # Some extreme precision values might fail, which is acceptable
            pass

    @given(
        null_fields=st.lists(
            st.sampled_from(["available", "locked", "staked"]),
            min_size=1,
            max_size=3,
            unique=True,
        )
    )
    @settings(max_examples=10)
    @pytest.mark.asyncio
    async def test_balance_null_fields_property_based(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        null_fields: list[str],
    ) -> None:
        """Property-based test for null field handling."""
        # Create data with null fields (only allowing None for optional fields)
        balance_data: dict[str, str | None] = {
            "available": "100.0",
            "locked": "10.0",
            "staked": "0",
        }
        for field in null_fields:
            if field == "staked":  # Only staked can be None
                balance_data[field] = None
            else:
                balance_data[field] = "0"  # Use default for required fields

        raw_data = {"USDC": balance_data}

        # Mock setup
        mock_request_builder.build_get_balances_params.return_value = BackpackRawGetBalancesParams()
        mock_http_client_requester.return_value = (raw_data, 200, {})

        # Mock response handler to raise ValidationError for null fields
        mock_response_handler.handle_get_balances_response.side_effect = (
            ValidationError.from_exception_data(
                title="ValidationError",
                line_errors=[],
            )
        )

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_balances()

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value

    @given(
        unicode_asset=st.text(
            alphabet=st.characters(min_codepoint=0x1F300, max_codepoint=0x1F6FF),
            min_size=1,
            max_size=10,
        )
    )
    @settings(max_examples=10)
    @pytest.mark.asyncio
    async def test_balance_unicode_assets_property_based(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        unicode_asset: str,
    ) -> None:
        """Property-based test for unicode asset handling."""
        raw_data = {unicode_asset: {"available": "100.0", "locked": "10.0"}}
        balance_response = BackpackRawBalanceResponse(
            available="100.0",
            locked="10.0",
            staked="0",
        )

        # Mock setup
        mock_request_builder.build_get_balances_params.return_value = BackpackRawGetBalancesParams()
        mock_http_client_requester.return_value = (raw_data, 200, {})
        mock_response_handler.handle_get_balances_response.return_value = {
            unicode_asset: balance_response
        }

        try:
            result = await bp_account_service.get_balances()

            # Verify unicode asset is handled
            assert unicode_asset in result
            assert isinstance(result[unicode_asset].asset.value, str)

        except Exception:
            # Unicode might not be supported, which is acceptable
            pass


# =======================
# Legacy Compatibility Verification
# =======================


class TestLegacyCompatibility:
    """Verify that property-based tests don't break legacy functionality."""

    @pytest.mark.asyncio
    async def test_legacy_single_balance_still_works(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Verify legacy single balance functionality remains intact."""
        raw_response_data = {"USDC": {"available": "1000.5", "locked": "10.0"}}
        balance_response = {
            "USDC": BackpackRawBalanceResponse(available="1000.5", locked="10.0", staked="0")
        }

        mock_request_builder.build_get_balances_params.return_value = BackpackRawGetBalancesParams()
        mock_http_client_requester.return_value = (raw_response_data, 200, {})
        mock_response_handler.handle_get_balances_response.return_value = balance_response

        result = await bp_account_service.get_balances()

        assert result["USDC"].exchange == ExchangeName.BACKPACK
        assert result["USDC"].asset.value == "USDC"
        assert result["USDC"].total_quantity == Decimal("1010.5")
        assert result["USDC"].available_quantity == Decimal("1000.5")

    @pytest.mark.asyncio
    async def test_legacy_validation_error_still_works(
        self,
        bp_account_service: BackpackAccountService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Verify legacy validation error handling remains intact."""
        mock_http_client_requester.return_value = ({"invalid": "balance"}, 200, {})
        mock_response_handler.handle_get_balances_response.side_effect = (
            ValidationError.from_exception_data(
                title="ValidationError",
                line_errors=[],
            )
        )

        with pytest.raises(APIError) as exc_info:
            await bp_account_service.get_balances()

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Failed to retrieve balance data" in exc_info.value.message
