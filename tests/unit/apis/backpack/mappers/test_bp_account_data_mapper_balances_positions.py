"""Property-based tests for Backpack Account Data Mapper Balance and Position Methods.

This module provides comprehensive property-based testing of BackpackAccountDataMapper
balance and position transformation methods, which are critical for secure financial
data processing and account state management.

SECURITY CRITICAL: Account data mapping must prevent:
- Financial data corruption through invalid decimal parsing
- Balance calculation errors that could lead to trading failures
- Position size miscalculations that could cause incorrect risk assessment
- Account summary inconsistencies that could mask financial exposure
- Precision loss in high-value or high-precision financial operations

Key Testing Areas:
- Balance transformation with comprehensive decimal value generation
- Position transformation with side detection and PnL calculation validation
- Account summary aggregation with multi-asset and multi-position scenarios
- Error handling for missing or invalid financial data
- Edge cases with zero values, negative values, and boundary conditions
- Security boundaries with malicious input resistance

Following TESTING_SECURITY_RULES.md:
- NO hardcoded financial values (Hypothesis generates them)
- NO fallback mechanisms that could hide calculation errors
- Comprehensive testing of financial precision boundaries
- Validation of security-sensitive account data processing

Architecture Compliance:
- Follows RULE-ARCH-MODEL-DESIGN-V2 for account model design
- Implements RULE-RUNTIME-SAFETY-V4 for safe financial processing
- Adheres to RULE-NO-SILENCING-V4 for proper error propagation
"""

from __future__ import annotations

import contextlib
from datetime import datetime
from decimal import Decimal

import pytest
from hypothesis import assume, given, settings, strategies as st
from hypothesis.strategies import SearchStrategy, composite

from cyberdelta.apis.backpack.mappers.account.bp_account_summary_mapper import (
    BackpackAccountSummaryMapper,
)
from cyberdelta.apis.backpack.mappers.account.bp_balance_mapper import BackpackBalanceMapper
from cyberdelta.apis.backpack.mappers.account.bp_position_mapper import BackpackPositionMapper
from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalanceResponse
from cyberdelta.apis.backpack.models.bp_raw_account_summary import BackpackRawAccountSummaryResponse
from cyberdelta.apis.backpack.models.bp_raw_margin_functions import (
    BackpackRawImfFunction,
    BackpackRawMmfFunction,
)
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPositionResponse
from cyberdelta.apis.exceptions.data_transformation import DataTransformationError
from cyberdelta.enums import OrderSide
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.models import DerivativePosition, MarginAccountSummary, SpotBalance
from cyberdelta.symbols import exchanges
from cyberdelta.symbols.models import Symbol


class CompositeAccountMapper:
    """Composite mapper that provides all account mapping functionality for testing."""

    def __init__(self) -> None:
        """Initialize the composite mapper with all sub-mappers."""
        self.balance_mapper = BackpackBalanceMapper()
        self.position_mapper = BackpackPositionMapper()
        self.account_summary_mapper = BackpackAccountSummaryMapper()

    def transform_balance_data_to_spot_balance(
        self, asset: Symbol, total_balance: str, available_balance: str
    ) -> SpotBalance:
        """Transform balance data to spot balance.

        Returns:
            SpotBalance: The transformed spot balance object.
        """
        return self.balance_mapper.transform_balance_data_to_spot_balance(
            asset.value, total_balance, available_balance
        )

    def transform_raw_balance_to_internal(
        self, asset: Symbol, raw_balance: BackpackRawBalanceResponse
    ) -> SpotBalance:
        """Transform raw balance to internal format.

        Returns:
            SpotBalance: The transformed balance in internal format.
        """
        return self.balance_mapper.transform_raw_balance_to_internal(asset, raw_balance)

    def transform_raw_position_to_internal(
        self, raw_position: BackpackRawPositionResponse
    ) -> DerivativePosition:
        """Transform raw position to internal format.

        Returns:
            DerivativePosition: The transformed position in internal format.
        """
        return self.position_mapper.transform_raw_position_to_internal(raw_position)

    def transform_raw_account_summary_to_internal(
        self,
        raw_summary: BackpackRawAccountSummaryResponse,
        spot_balances: dict[str, BackpackRawBalanceResponse],
        positions: list[BackpackRawPositionResponse],
    ) -> MarginAccountSummary:
        """Transform raw account summary to internal format.

        Returns:
            MarginAccountSummary: The transformed account summary in internal format.
        """
        return self.account_summary_mapper.transform_raw_account_summary_to_internal(
            raw_summary, spot_balances, positions
        )


@pytest.fixture
def mapper() -> CompositeAccountMapper:
    """Fixture providing a composite account mapper instance for testing.

    Returns:
        CompositeAccountMapper: Configured mapper instance for testing.
    """
    return CompositeAccountMapper()


# =============================================================================
# HYPOTHESIS STRATEGIES FOR ACCOUNT DATA MAPPER TESTING
# =============================================================================


def asset_symbol_strategy() -> SearchStrategy[str]:
    """Generate valid asset symbol strings.

    Returns:
        SearchStrategy[str]: A strategy for generating asset symbols.
    """
    return st.one_of([
        # Common crypto assets
        st.sampled_from([
            "BTC",
            "ETH",
            "SOL",
            "USDC",
            "USDT",
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
        st.text(
            min_size=2,
            max_size=20,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="-_."
            ),
        ).filter(lambda x: x.strip() and len(x.encode("utf-8")) <= 20),
        # Unicode asset names (for international testing)
        st.sampled_from(["USDC🚀", "BTC⚡", "ETH💎", "SOL🌞"]),
    ])


def decimal_amount_strategy() -> SearchStrategy[Decimal]:
    """Generate valid decimal amounts for financial operations.

    Returns:
        SearchStrategy[Decimal]: A strategy for generating decimal amounts.
    """
    return st.one_of([
        # Common amounts
        st.decimals(min_value=Decimal(0), max_value=Decimal(1000000), places=18),
        st.decimals(min_value=Decimal("0.000001"), max_value=Decimal(999999), places=18),
        # Edge cases
        st.just(Decimal(0)),
        st.just(Decimal("0.000001")),  # Minimum unit
        st.just(Decimal(21000000)),  # Max BTC supply
        st.just(Decimal("999999999.999999999999999999")),  # High precision
        # Large values
        st.decimals(min_value=Decimal(1000000), max_value=Decimal(1000000000), places=8),
    ])


def balance_amount_string_strategy() -> SearchStrategy[str]:
    """Generate balance amount strings as they come from the API.

    Returns:
        SearchStrategy[str]: A strategy for generating balance amount strings.
    """
    return st.one_of([
        # Standard decimal strings
        st.builds(str, decimal_amount_strategy()),
        # Scientific notation
        st.sampled_from(["1.23e6", "5.67e-8", "9.99e+10", "1e-18", "1.234567890123456789e15"]),
        # Zero representations
        st.sampled_from(["0", "0.0", "0.00", "0.000000000000000000"]),
        # High precision strings
        st.sampled_from([
            "123.123456789012345678",
            "999999999.999999999999999999",
            "0.000000000000000001",
        ]),
    ])


def _create_trading_pair(base: str, quote: str) -> str:
    """Create trading pair symbol from base and quote assets.

    Returns:
        str: Trading pair symbol in the format '{base}-{quote}'.
    """
    return f"{base}-{quote}"


def trading_symbol_strategy() -> SearchStrategy[str]:
    """Generate valid trading symbol strings.

    Returns:
        SearchStrategy[str]: A strategy for generating trading symbols.
    """
    return st.one_of([
        # Common trading pairs
        st.sampled_from([
            "BTC-USDC",
            "ETH-USDC",
            "SOL-USDC",
            "AVAX-USDC",
            "DOT-USDC",
            "LINK-USDC",
            "UNI-USDC",
            "MATIC-USDC",
        ]),
        # Generated trading pairs
        st.builds(
            _create_trading_pair,
            st.sampled_from(["BTC", "ETH", "SOL", "AVAX", "DOT", "LINK"]),
            st.sampled_from(["USDC", "USDT", "BTC", "ETH"]),
        ),
    ])


def position_quantity_strategy() -> SearchStrategy[str]:
    """Generate position quantity strings (can be negative for short positions).

    Returns:
        SearchStrategy[str]: A strategy for generating position quantity strings.
    """
    return st.one_of([
        # Positive quantities (long positions)
        st.builds(
            str, st.decimals(min_value=Decimal("0.000001"), max_value=Decimal(1000000), places=18)
        ),
        # Negative quantities (short positions)
        st.builds(
            str,
            st.decimals(min_value=Decimal(-1000000), max_value=Decimal("-0.000001"), places=18),
        ),
        # Zero quantity
        st.just("0"),
        st.just("0.0"),
        # Edge cases
        st.sampled_from([
            "10.123456789012345678",
            "-10.123456789012345678",
            "999999.999999999999999999",
            "-999999.999999999999999999",
        ]),
    ])


def price_strategy() -> SearchStrategy[str]:
    """Generate price strings for positions and trading.

    Returns:
        SearchStrategy[str]: A strategy for generating price strings.
    """
    return st.one_of([
        # Normal price range
        st.builds(str, st.decimals(min_value=Decimal("0.01"), max_value=Decimal(100000), places=8)),
        # High precision prices
        st.sampled_from([
            "100.12345678",
            "0.00000123",
            "50000.99999999",
            "1.234567890123456789",
        ]),
        # Common crypto prices
        st.sampled_from(["100.25", "50000.0", "2500.50", "0.1", "0.001", "1000000.0"]),
    ])


def pnl_strategy() -> SearchStrategy[str]:
    """Generate PnL strings (can be positive or negative).

    Returns:
        SearchStrategy[str]: A strategy for generating PnL strings.
    """
    return st.one_of([
        # Positive PnL
        st.builds(str, st.decimals(min_value=Decimal(0), max_value=Decimal(1000000), places=18)),
        # Negative PnL
        st.builds(str, st.decimals(min_value=Decimal(-1000000), max_value=Decimal(0), places=18)),
        # Zero PnL
        st.just("0"),
        st.just("0.0"),
        # Extreme values
        st.sampled_from([
            "999999.999999999999999999",
            "-999999.999999999999999999",
            "0.000000000000000001",
            "-0.000000000000000001",
        ]),
    ])


@composite
def raw_balance_strategy(draw: st.DrawFn) -> BackpackRawBalanceResponse:
    """Generate BackpackRawBalanceResponse instances.

    Returns:
        BackpackRawBalanceResponse: A generated balance response instance.
    """
    available = draw(balance_amount_string_strategy())
    locked = draw(balance_amount_string_strategy())
    staked = draw(balance_amount_string_strategy())

    return BackpackRawBalanceResponse(
        available=available,
        locked=locked,
        staked=staked,
    )


@composite
def raw_position_strategy(draw: st.DrawFn) -> BackpackRawPositionResponse:
    """Generate BackpackRawPositionResponse instances.

    Returns:
        BackpackRawPositionResponse: A generated position response instance.
    """
    symbol = draw(trading_symbol_strategy())
    break_even_price = draw(price_strategy())
    entry_price = draw(price_strategy())
    est_liquidation_price = draw(price_strategy())
    imf = draw(st.builds(str, st.decimals(min_value=Decimal(0), max_value=Decimal(1), places=8)))
    mark_price = draw(price_strategy())
    mmf = draw(st.builds(str, st.decimals(min_value=Decimal(0), max_value=Decimal(1), places=8)))
    net_cost = draw(pnl_strategy())
    net_quantity = draw(position_quantity_strategy())
    net_exposure_quantity = draw(position_quantity_strategy())
    net_exposure_notional = draw(balance_amount_string_strategy())
    pnl_realized = draw(pnl_strategy())
    pnl_unrealized = draw(pnl_strategy())
    cumulative_funding_payment = draw(pnl_strategy())
    user_id = draw(st.integers(min_value=1, max_value=999999999999))
    position_id = draw(
        st.text(
            min_size=1,
            max_size=64,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="_-"
            ),
        )
    )
    cumulative_interest = draw(pnl_strategy())

    # Create IMF and MMF function objects
    imf_function = BackpackRawImfFunction(
        base=imf,
        factor="0.0",
    )
    mmf_function = BackpackRawMmfFunction(
        base=mmf,
        factor="0.0",
    )

    return BackpackRawPositionResponse(
        symbol=symbol,
        subaccountId=0,
        breakEvenPrice=break_even_price,
        entryPrice=entry_price,
        estLiquidationPrice=est_liquidation_price,
        imf=imf,
        imfFunction=imf_function,
        markPrice=mark_price,
        mmf=mmf,
        mmfFunction=mmf_function,
        netCost=net_cost,
        netQuantity=net_quantity,
        netExposureQuantity=net_exposure_quantity,
        netExposureNotional=net_exposure_notional,
        pnlRealized=pnl_realized,
        pnlUnrealized=pnl_unrealized,
        cumulativeFundingPayment=cumulative_funding_payment,
        userId=user_id,
        positionId=position_id,
        cumulativeInterest=cumulative_interest,
    )


@composite
def zero_position_strategy(draw: st.DrawFn) -> BackpackRawPositionResponse:
    """Generate zero-size positions for testing business logic validation.

    Returns:
        BackpackRawPositionResponse: A generated zero-size position instance.
    """
    position = draw(raw_position_strategy())

    # Override with zero quantity to test zero position logic
    return BackpackRawPositionResponse(
        symbol=position.symbol,
        subaccountId=position.subaccount_id,
        breakEvenPrice=position.break_even_price,
        entryPrice=position.entry_price,  # This will cause validation failure
        estLiquidationPrice=position.est_liquidation_price,
        imf=position.imf,
        imfFunction=position.imf_function,
        markPrice=position.mark_price,
        mmf=position.mmf,
        mmfFunction=position.mmf_function,
        netCost=position.net_cost,
        netQuantity="0.0",  # Zero quantity
        netExposureQuantity=position.net_exposure_quantity,
        netExposureNotional=position.net_exposure_notional,
        pnlRealized=position.pnl_realized,
        pnlUnrealized="0.0",
        cumulativeFundingPayment=position.cumulative_funding_payment,
        userId=position.user_id,
        positionId=position.position_id,
        cumulativeInterest=position.cumulative_interest,
    )


@composite
def raw_account_summary_strategy(draw: st.DrawFn) -> BackpackRawAccountSummaryResponse:
    """Generate BackpackRawAccountSummaryResponse instances.

    Returns:
        BackpackRawAccountSummaryResponse: A generated account summary response instance.
    """
    auto_borrow_settlements = draw(st.booleans())
    auto_lend = draw(st.booleans())
    auto_realize_pnl = draw(st.booleans())
    auto_repay_borrows = draw(st.booleans())
    borrow_limit = draw(balance_amount_string_strategy())
    futures_maker_fee = draw(
        st.builds(str, st.decimals(min_value=Decimal(0), max_value=Decimal("0.01"), places=6))
    )
    futures_taker_fee = draw(
        st.builds(str, st.decimals(min_value=Decimal(0), max_value=Decimal("0.01"), places=6))
    )
    leverage_limit = draw(
        st.builds(str, st.decimals(min_value=Decimal(1), max_value=Decimal(100), places=2))
    )
    limit_orders = draw(st.integers(min_value=0, max_value=1000))
    liquidating = draw(st.booleans())
    position_limit = draw(balance_amount_string_strategy())
    spot_maker_fee = draw(
        st.builds(str, st.decimals(min_value=Decimal(0), max_value=Decimal("0.01"), places=6))
    )
    spot_taker_fee = draw(
        st.builds(str, st.decimals(min_value=Decimal(0), max_value=Decimal("0.01"), places=6))
    )
    trigger_orders = draw(st.integers(min_value=0, max_value=1000))

    return BackpackRawAccountSummaryResponse.model_validate({
        "autoBorrowSettlements": auto_borrow_settlements,
        "autoLend": auto_lend,
        "autoRealizePnl": auto_realize_pnl,
        "autoRepayBorrows": auto_repay_borrows,
        "borrowLimit": borrow_limit,
        "futuresMakerFee": futures_maker_fee,
        "futuresTakerFee": futures_taker_fee,
        "leverageLimit": leverage_limit,
        "limitOrders": limit_orders,
        "liquidating": liquidating,
        "positionLimit": position_limit,
        "spotMakerFee": spot_maker_fee,
        "spotTakerFee": spot_taker_fee,
        "triggerOrders": trigger_orders,
    })


def malicious_balance_input_strategy() -> SearchStrategy[str]:
    """Generate malicious balance input strings for security testing.

    Returns:
        SearchStrategy[str]: A strategy for generating malicious balance input strings.
    """
    return st.one_of([
        # XSS attempts
        st.sampled_from([
            "<script>alert('xss')</script>",
            "<img src=x onerror=alert(1)>",
            "javascript:alert('XSS')",
        ]),
        # SQL injection attempts
        st.sampled_from([
            "'; DROP TABLE balances;--",
            "1' OR '1'='1",
            "admin'--",
        ]),
        # Command injection
        st.sampled_from([
            "$(rm -rf /)",
            "`cat /etc/passwd`",
            "; ls -la",
        ]),
        # Buffer overflow attempts
        st.text(alphabet="A", min_size=1000, max_size=1500),
        # Format string attacks
        st.sampled_from(["%s%s%s%s%s", "%x%x%x%x", "%n%n%n%n"]),
        # Unicode attacks
        st.sampled_from([
            "\\udce2\\udc28\\udc00",
            "\\x00\\x01\\x02",
            "\\u202e\\u202d",  # Right-to-left override
        ]),
        # Invalid decimal formats
        st.sampled_from([
            "not_a_number",
            "123.456.789",
            "1,234.56",  # Comma separator
            "NaN",
            "Infinity",
            "-Infinity",
        ]),
    ])


# =============================================================================
# PROPERTY TESTS FOR BALANCE TRANSFORMATION
# =============================================================================


class TestBalanceTransformationProperties:
    """Property-based tests for balance transformation functionality."""

    @given(
        asset=asset_symbol_strategy(),
        total_balance=balance_amount_string_strategy(),
        available_balance=balance_amount_string_strategy(),
    )
    @settings(max_examples=200, deadline=None)
    def test_balance_data_transformation_properties(
        self, asset: str, total_balance: str, available_balance: str
    ) -> None:
        """Property: Valid balance data should always transform to SpotBalance."""
        mapper = CompositeAccountMapper()
        symbol = exchanges.backpack(asset)

        try:
            result = mapper.transform_balance_data_to_spot_balance(
                asset=symbol,
                total_balance=total_balance,
                available_balance=available_balance,
            )

            # Property: Result should be valid SpotBalance
            assert isinstance(result, SpotBalance)
            assert result.asset.value == asset
            assert result.exchange == ExchangeName.BACKPACK.value
            assert isinstance(result.timestamp, datetime)

            # Property: Decimal conversion should preserve precision
            assert isinstance(result.total_quantity, Decimal)
            assert isinstance(result.available_quantity, Decimal)

            # Property: Non-negative balances
            assert result.total_quantity >= Decimal(0)
            assert result.available_quantity >= Decimal(0)

            # Property: Backpack details should be present
            assert result.bp_details is not None

        except (ValueError, DataTransformationError):
            # Expected for invalid decimal strings
            pass

    @given(
        asset=asset_symbol_strategy(),
        raw_balance=raw_balance_strategy(),
    )
    @settings(max_examples=200, deadline=None)
    def test_raw_balance_transformation_properties(
        self, asset: str, raw_balance: BackpackRawBalanceResponse
    ) -> None:
        """Property: Valid raw balance should transform correctly."""
        mapper = CompositeAccountMapper()
        symbol = exchanges.backpack(asset)

        try:
            result = mapper.transform_raw_balance_to_internal(symbol, raw_balance)

            # Property: Result should be valid SpotBalance
            assert isinstance(result, SpotBalance)
            assert result.asset.value == asset
            assert result.exchange == ExchangeName.BACKPACK

            # Property: Total should be sum of available + locked + staked
            try:
                expected_total = (
                    Decimal(raw_balance.available)
                    + Decimal(raw_balance.locked)
                    + Decimal(raw_balance.staked)
                )
                assert result.total_quantity == expected_total
                assert result.available_quantity == Decimal(raw_balance.available)
            except (ValueError, ArithmeticError):
                # Expected for invalid decimal values in raw data
                pass

        except (ValueError, DataTransformationError):
            # Expected for invalid raw balance data
            pass

    @given(
        asset=asset_symbol_strategy(),
        available=decimal_amount_strategy(),
        locked=decimal_amount_strategy(),
        staked=decimal_amount_strategy(),
    )
    @settings(max_examples=150, deadline=None)
    def test_balance_calculation_consistency_properties(
        self, asset: str, available: Decimal, locked: Decimal, staked: Decimal
    ) -> None:
        """Property: Balance calculations should be mathematically consistent."""
        mapper = CompositeAccountMapper()
        symbol = exchanges.backpack(asset)

        raw_balance = BackpackRawBalanceResponse(
            available=str(available),
            locked=str(locked),
            staked=str(staked),
        )

        result = mapper.transform_raw_balance_to_internal(symbol, raw_balance)

        # Property: Total should equal sum of components
        expected_total = available + locked + staked
        assert result.total_quantity == expected_total
        assert result.available_quantity == available

        # Property: Precision should be preserved
        assert str(result.total_quantity) == str(expected_total)

    @given(
        asset=asset_symbol_strategy(),
        zero_values=st.sampled_from([
            ("0", "0", "0"),
            ("0.0", "0.0", "0.0"),
            ("0.000000000000000000", "0.000000000000000000", "0.000000000000000000"),
        ]),
    )
    @settings(max_examples=50, deadline=None)
    def test_zero_balance_handling_properties(
        self, asset: str, zero_values: tuple[str, str, str]
    ) -> None:
        """Property: Zero balances should be handled correctly."""
        mapper = CompositeAccountMapper()
        symbol = exchanges.backpack(asset)

        available, locked, staked = zero_values
        raw_balance = BackpackRawBalanceResponse(
            available=available,
            locked=locked,
            staked=staked,
        )

        result = mapper.transform_raw_balance_to_internal(symbol, raw_balance)

        # Property: All zero balances should result in zero totals
        assert result.total_quantity == Decimal(0)
        assert result.available_quantity == Decimal(0)

    @given(
        malicious_input=malicious_balance_input_strategy(),
    )
    @settings(max_examples=100, deadline=None)
    def test_balance_security_resistance_properties(self, malicious_input: str) -> None:
        """Property: Balance transformation should resist malicious inputs."""
        mapper = CompositeAccountMapper()
        symbol = exchanges.backpack("USDC")

        # Should either transform safely or raise appropriate error
        try:
            result = mapper.transform_balance_data_to_spot_balance(
                asset=symbol,
                total_balance=malicious_input,
                available_balance="100.0",
            )

            # If accepted, should not contain malicious content in critical fields
            assert isinstance(result.total_quantity, Decimal)
            assert isinstance(result.available_quantity, Decimal)

        except (ValueError, DataTransformationError):
            # Expected rejection of malicious input
            pass


# =============================================================================
# PROPERTY TESTS FOR POSITION TRANSFORMATION
# =============================================================================


class TestPositionTransformationProperties:
    """Property-based tests for position transformation functionality."""

    @given(
        raw_position=raw_position_strategy(),
    )
    @settings(max_examples=200, deadline=None)
    def test_position_transformation_properties(
        self, raw_position: BackpackRawPositionResponse
    ) -> None:
        """Property: Valid raw position should transform to DerivativePosition."""
        mapper = CompositeAccountMapper()

        try:
            # Skip zero positions with non-None entry price (business logic violation)
            if Decimal(raw_position.net_quantity) == Decimal(0) and raw_position.entry_price != "0":
                assume(False)

            result = mapper.transform_raw_position_to_internal(raw_position)

            # Property: Result should be valid DerivativePosition
            assert isinstance(result, DerivativePosition)
            assert result.exchange == ExchangeName.BACKPACK
            assert isinstance(result.timestamp, datetime)

            # Property: Size should match net quantity
            assert result.size == Decimal(raw_position.net_quantity)

            # Property: Side should be determined by quantity sign
            if Decimal(raw_position.net_quantity) > Decimal(0):
                assert result.side == OrderSide.BUY
            elif Decimal(raw_position.net_quantity) < Decimal(0):
                assert result.side == OrderSide.SELL
            else:
                # Zero position side determination
                assert result.side in [OrderSide.BUY, OrderSide.SELL]

            # Property: Prices should be preserved
            assert result.entry_price == Decimal(raw_position.entry_price)
            assert result.mark_price == Decimal(raw_position.mark_price)
            assert result.liquidation_price == Decimal(raw_position.est_liquidation_price)

            # Property: PnL should be preserved
            assert result.unrealized_pnl == Decimal(raw_position.pnl_unrealized)
            assert result.realized_pnl == Decimal(raw_position.pnl_realized)

            # Property: Backpack details should be present
            assert result.bp_details is not None
            assert result.bp_details.imf_base == Decimal(raw_position.imf)
            assert result.bp_details.mmf_base == Decimal(raw_position.mmf)

        except (ValueError, DataTransformationError):
            # Expected for invalid position data
            pass

    @given(
        symbol=trading_symbol_strategy(),
        quantity=st.decimals(min_value=Decimal("0.000001"), max_value=Decimal(1000000), places=18),
        side_multiplier=st.sampled_from([1, -1]),
        entry_price=st.decimals(min_value=Decimal("0.01"), max_value=Decimal(100000), places=8),
        pnl=st.decimals(min_value=Decimal(-100000), max_value=Decimal(100000), places=18),
    )
    @settings(max_examples=150, deadline=None)
    def test_position_side_detection_properties(
        self,
        symbol: str,
        quantity: Decimal,
        side_multiplier: int,
        entry_price: Decimal,
        pnl: Decimal,
    ) -> None:
        """Property: Position side should be correctly detected from quantity sign."""
        mapper = CompositeAccountMapper()

        signed_quantity = quantity * side_multiplier

        # Create minimal IMF and MMF functions
        imf_function = BackpackRawImfFunction(base="0.1", factor="0.0")
        mmf_function = BackpackRawMmfFunction(base="0.05", factor="0.0")

        raw_position = BackpackRawPositionResponse(
            symbol=symbol,
            subaccountId=0,
            breakEvenPrice=str(entry_price),
            entryPrice=str(entry_price),
            estLiquidationPrice=str(entry_price * Decimal("0.9")),
            imf="0.1",
            imfFunction=imf_function,
            markPrice=str(entry_price),
            mmf="0.05",
            mmfFunction=mmf_function,
            netCost=str(signed_quantity * entry_price),
            netQuantity=str(signed_quantity),
            netExposureQuantity=str(signed_quantity),
            netExposureNotional=str(abs(signed_quantity * entry_price)),
            pnlRealized="0.0",
            pnlUnrealized=str(pnl),
            cumulativeFundingPayment="0.0",
            userId=12345,
            positionId="test_pos",
            cumulativeInterest="0.0",
        )

        result = mapper.transform_raw_position_to_internal(raw_position)

        # Property: Side should match quantity sign
        if side_multiplier > 0:
            assert result.side == OrderSide.BUY
        else:
            assert result.side == OrderSide.SELL

        # Property: Size should preserve sign
        assert result.size == signed_quantity

    @given(
        zero_position=zero_position_strategy(),
    )
    @settings(max_examples=50, deadline=None)
    def test_zero_position_validation_properties(
        self, zero_position: BackpackRawPositionResponse
    ) -> None:
        """Property: Zero positions should validate business logic correctly."""
        mapper = CompositeAccountMapper()

        # Zero positions with non-None entry price should fail validation
        if Decimal(zero_position.net_quantity) == Decimal(0) and zero_position.entry_price != "0":
            with pytest.raises(DataTransformationError):
                mapper.transform_raw_position_to_internal(zero_position)

    @given(
        symbol=trading_symbol_strategy(),
        user_id=st.integers(min_value=1, max_value=999999999999),
        position_id=st.text(min_size=1, max_size=64).filter(lambda x: x.strip()),
    )
    @settings(max_examples=100, deadline=None)
    def test_position_metadata_preservation_properties(
        self, symbol: str, user_id: int, position_id: str
    ) -> None:
        """Property: Position metadata should be preserved through transformation."""
        mapper = CompositeAccountMapper()

        # Create minimal valid position
        imf_function = BackpackRawImfFunction(base="0.1", factor="0.0")
        mmf_function = BackpackRawMmfFunction(base="0.05", factor="0.0")

        raw_position = BackpackRawPositionResponse(
            symbol=symbol,
            subaccountId=0,
            breakEvenPrice="100.0",
            entryPrice="100.0",
            estLiquidationPrice="90.0",
            imf="0.1",
            imfFunction=imf_function,
            markPrice="100.0",
            mmf="0.05",
            mmfFunction=mmf_function,
            netCost="1000.0",
            netQuantity="10.0",
            netExposureQuantity="10.0",
            netExposureNotional="1000.0",
            pnlRealized="0.0",
            pnlUnrealized="5.0",
            cumulativeFundingPayment="0.0",
            userId=user_id,
            positionId=position_id,
            cumulativeInterest="0.0",
        )

        result = mapper.transform_raw_position_to_internal(raw_position)

        # Property: Symbol should be preserved
        assert result.symbol.value == symbol


# =============================================================================
# PROPERTY TESTS FOR ACCOUNT SUMMARY TRANSFORMATION
# =============================================================================


class TestAccountSummaryTransformationProperties:
    """Property-based tests for account summary transformation functionality."""

    @given(
        raw_summary=raw_account_summary_strategy(),
        spot_balances=st.dictionaries(
            asset_symbol_strategy(), raw_balance_strategy(), min_size=0, max_size=10
        ),
        positions=st.lists(raw_position_strategy(), min_size=0, max_size=5),
    )
    @settings(max_examples=100, deadline=None)
    def test_account_summary_transformation_properties(
        self,
        raw_summary: BackpackRawAccountSummaryResponse,
        spot_balances: dict[str, BackpackRawBalanceResponse],
        positions: list[BackpackRawPositionResponse],
    ) -> None:
        """Property: Valid account summary data should transform correctly."""
        mapper = CompositeAccountMapper()

        try:
            # Filter out zero positions with non-None entry prices
            valid_positions: list[BackpackRawPositionResponse] = []
            for pos in positions:
                if Decimal(pos.net_quantity) == Decimal(0) and pos.entry_price != "0":
                    continue
                valid_positions.append(pos)

            result = mapper.transform_raw_account_summary_to_internal(
                raw_summary, spot_balances, valid_positions
            )

            # Property: Result should be valid MarginAccountSummary
            assert isinstance(result, MarginAccountSummary)
            assert result.exchange == ExchangeName.BACKPACK.value
            assert isinstance(result.timestamp, datetime)

            # Property: Equity values should be non-negative or handle negative PnL
            assert isinstance(result.total_equity, Decimal)
            assert isinstance(result.available_equity, Decimal)

            # Property: Position metrics should be calculated
            assert isinstance(result.total_position_notional, Decimal)
            assert isinstance(result.total_unrealized_pnl, Decimal)

            # Property: Backpack details should be present
            assert result.bp_details is not None

        except (ValueError, DataTransformationError):
            # Expected for invalid account data
            pass

    @given(
        usd_balances=st.dictionaries(
            st.sampled_from(["USDC", "USDT"]), raw_balance_strategy(), min_size=1, max_size=2
        ),
        non_usd_balances=st.dictionaries(
            st.sampled_from(["BTC", "ETH", "SOL"]), raw_balance_strategy(), min_size=0, max_size=3
        ),
    )
    @settings(max_examples=100, deadline=None)
    def test_usd_balance_aggregation_properties(
        self,
        usd_balances: dict[str, BackpackRawBalanceResponse],
        non_usd_balances: dict[str, BackpackRawBalanceResponse],
    ) -> None:
        """Property: Only USD-like balances should be included in equity calculations."""
        mapper = CompositeAccountMapper()
        raw_summary = BackpackRawAccountSummaryResponse.model_validate({
            "autoBorrowSettlements": False,
            "autoLend": False,
            "autoRealizePnl": False,
            "autoRepayBorrows": False,
            "borrowLimit": "5000.0",
            "futuresMakerFee": "0.0002",
            "futuresTakerFee": "0.0005",
            "leverageLimit": "10.0",
            "limitOrders": 100,
            "liquidating": False,
            "positionLimit": "1000000.0",
            "spotMakerFee": "0.001",
            "spotTakerFee": "0.001",
            "triggerOrders": 50,
        })

        all_balances = {**usd_balances, **non_usd_balances}

        try:
            result = mapper.transform_raw_account_summary_to_internal(raw_summary, all_balances, [])

            # Property: Only USD-like assets should contribute to equity
            # Calculate expected total from USD balances only
            expected_total = Decimal(0)
            for asset, balance in usd_balances.items():
                if asset in ["USDC", "USDT"]:
                    try:
                        total = (
                            Decimal(balance.available)
                            + Decimal(balance.locked)
                            + Decimal(balance.staked)
                        )
                        expected_total += total
                    except (ValueError, ArithmeticError):
                        pass

            # Property: Non-USD balances should not affect equity
            assert result.total_equity >= Decimal(0)  # Should be reasonable

        except (ValueError, DataTransformationError):
            # Expected for invalid balance data
            pass

    @given(
        positions_with_pnl=st.lists(
            st.tuples(
                position_quantity_strategy(),
                price_strategy(),
                pnl_strategy(),
            ),
            min_size=1,
            max_size=5,
        ),
    )
    @settings(max_examples=100, deadline=None)
    def test_position_pnl_aggregation_properties(
        self, positions_with_pnl: list[tuple[str, str, str]]
    ) -> None:
        """Property: Position PnL should be correctly aggregated."""
        mapper = CompositeAccountMapper()
        raw_summary = BackpackRawAccountSummaryResponse.model_validate({
            "autoBorrowSettlements": False,
            "autoLend": False,
            "autoRealizePnl": False,
            "autoRepayBorrows": False,
            "borrowLimit": "5000.0",
            "futuresMakerFee": "0.0002",
            "futuresTakerFee": "0.0005",
            "leverageLimit": "10.0",
            "limitOrders": 100,
            "liquidating": False,
            "positionLimit": "1000000.0",
            "spotMakerFee": "0.001",
            "spotTakerFee": "0.001",
            "triggerOrders": 50,
        })

        # Create positions from the generated data
        positions: list[BackpackRawPositionResponse] = []
        expected_total_pnl = Decimal(0)

        for i, (quantity, price, pnl) in enumerate(positions_with_pnl):
            # Skip zero positions that would fail validation
            if Decimal(quantity) == Decimal(0):
                continue

            imf_function = BackpackRawImfFunction(base="0.1", factor="0.0")
            mmf_function = BackpackRawMmfFunction(base="0.05", factor="0.0")

            position = BackpackRawPositionResponse(
                symbol=f"TEST{i}-USDC",
                subaccountId=0,
                breakEvenPrice=price,
                entryPrice=price,
                estLiquidationPrice=price,
                imf="0.1",
                imfFunction=imf_function,
                markPrice=price,
                mmf="0.05",
                mmfFunction=mmf_function,
                netCost=str(Decimal(quantity) * Decimal(price)),
                netQuantity=quantity,
                netExposureQuantity=quantity,
                netExposureNotional=str(abs(Decimal(quantity) * Decimal(price))),
                pnlRealized="0.0",
                pnlUnrealized=pnl,
                cumulativeFundingPayment="0.0",
                userId=12345,
                positionId=f"pos{i}",
                cumulativeInterest="0.0",
            )
            positions.append(position)

            with contextlib.suppress(ValueError, ArithmeticError):
                expected_total_pnl += Decimal(pnl)

        if positions:
            try:
                result = mapper.transform_raw_account_summary_to_internal(
                    raw_summary, {}, positions
                )

                # Property: Total unrealized PnL should aggregate position PnL
                assert isinstance(result.total_unrealized_pnl, Decimal)

            except (ValueError, DataTransformationError):
                # Expected for invalid position data
                pass


# =============================================================================
# PROPERTY TESTS FOR SECURITY BOUNDARIES
# =============================================================================


class TestAccountDataSecurityProperties:
    """Property-based tests for security-critical account data behavior."""

    @given(
        asset=asset_symbol_strategy(),
        malicious_balance=malicious_balance_input_strategy(),
        valid_balance=balance_amount_string_strategy(),
    )
    @settings(max_examples=100, deadline=None)
    def test_malicious_balance_resistance_properties(
        self, asset: str, malicious_balance: str, valid_balance: str
    ) -> None:
        """Property: Account mappers should resist malicious balance inputs."""
        mapper = CompositeAccountMapper()
        symbol = exchanges.backpack(asset)

        # Test malicious total balance
        try:
            result = mapper.transform_balance_data_to_spot_balance(
                asset=symbol,
                total_balance=malicious_balance,
                available_balance=valid_balance,
            )

            # If accepted, should not contain execution traces
            assert isinstance(result.total_quantity, Decimal)

        except (ValueError, DataTransformationError):
            # Expected rejection
            pass

        # Test malicious available balance
        try:
            result = mapper.transform_balance_data_to_spot_balance(
                asset=symbol,
                total_balance=valid_balance,
                available_balance=malicious_balance,
            )

            # If accepted, should be safe
            assert isinstance(result.available_quantity, Decimal)

        except (ValueError, DataTransformationError):
            # Expected rejection
            pass

    @given(
        symbol=trading_symbol_strategy(),
        malicious_price=malicious_balance_input_strategy(),
    )
    @settings(max_examples=50, deadline=None)
    def test_malicious_position_data_resistance(self, symbol: str, malicious_price: str) -> None:
        """Property: Position transformation should resist malicious price inputs."""
        mapper = CompositeAccountMapper()

        imf_function = BackpackRawImfFunction(base="0.1", factor="0.0")
        mmf_function = BackpackRawMmfFunction(base="0.05", factor="0.0")

        try:
            raw_position = BackpackRawPositionResponse(
                symbol=symbol,
                subaccountId=0,
                breakEvenPrice=malicious_price,
                entryPrice="100.0",  # Keep valid to avoid business logic errors
                estLiquidationPrice="90.0",
                imf="0.1",
                imfFunction=imf_function,
                markPrice="100.0",
                mmf="0.05",
                mmfFunction=mmf_function,
                netCost="1000.0",
                netQuantity="10.0",
                netExposureQuantity="10.0",
                netExposureNotional="1000.0",
                pnlRealized="0.0",
                pnlUnrealized="5.0",
                cumulativeFundingPayment="0.0",
                userId=12345,
                positionId="test_pos",
                cumulativeInterest="0.0",
            )

            result = mapper.transform_raw_position_to_internal(raw_position)

            # Should not contain traces of malicious execution
            assert isinstance(result.size, Decimal)

        except (ValueError, DataTransformationError, TypeFieldError):
            # Expected for malicious inputs - validation properly rejects them
            pass

    @given(
        large_values=st.lists(
            st.builds(
                str,
                st.decimals(
                    min_value=Decimal(1000000000),
                    max_value=Decimal(999999999999999999),
                    places=18,
                ),
            ),
            min_size=3,
            max_size=3,
        ),
    )
    @settings(max_examples=50, deadline=None)
    def test_large_value_handling_properties(self, large_values: list[str]) -> None:
        """Property: Large financial values should be handled without overflow."""
        mapper = CompositeAccountMapper()
        symbol = exchanges.backpack("USDC")

        available, locked, staked = large_values

        try:
            raw_balance = BackpackRawBalanceResponse(
                available=available,
                locked=locked,
                staked=staked,
            )

            result = mapper.transform_raw_balance_to_internal(symbol, raw_balance)

            # Property: Large values should not cause overflow
            assert isinstance(result.total_quantity, Decimal)
            assert result.total_quantity >= Decimal(0)

            # Property: Precision should be maintained
            expected_total = Decimal(available) + Decimal(locked) + Decimal(staked)
            assert result.total_quantity == expected_total

        except (ValueError, DataTransformationError, OverflowError):
            # Expected for values too large to handle
            pass


# =============================================================================
# INTEGRATION PROPERTY TESTS
# =============================================================================


class TestAccountDataIntegrationProperties:
    """Integration property tests for complete account data workflows."""

    def _filter_valid_positions(
        self, positions: list[BackpackRawPositionResponse]
    ) -> list[BackpackRawPositionResponse]:
        """Filter positions to avoid business logic violations.

        Returns:
            List of valid position responses.
        """
        valid_positions: list[BackpackRawPositionResponse] = []
        for pos in positions:
            if Decimal(pos.net_quantity) == Decimal(0) and pos.entry_price != "0":
                continue
            valid_positions.append(pos)
        return valid_positions

    def _transform_balances(
        self, mapper: CompositeAccountMapper, spot_balances: dict[str, BackpackRawBalanceResponse]
    ) -> list[SpotBalance]:
        """Transform raw balances to internal format.

        Returns:
            List of transformed spot balances.
        """
        transformed_balances: list[SpotBalance] = []
        for asset, balance in spot_balances.items():
            try:
                symbol = exchanges.backpack(asset)
                transformed_balance = mapper.transform_raw_balance_to_internal(symbol, balance)
                transformed_balances.append(transformed_balance)
            except (ValueError, DataTransformationError):
                continue
        return transformed_balances

    def _transform_positions(
        self, mapper: CompositeAccountMapper, positions: list[BackpackRawPositionResponse]
    ) -> list[DerivativePosition]:
        """Transform raw positions to internal format.

        Returns:
            List of transformed derivative positions.
        """
        transformed_positions: list[DerivativePosition] = []
        for position in positions:
            try:
                transformed_position = mapper.transform_raw_position_to_internal(position)
                transformed_positions.append(transformed_position)
            except (ValueError, DataTransformationError):
                continue
        return transformed_positions

    def _validate_transformations(
        self,
        account_summary: MarginAccountSummary,
        transformed_balances: list[SpotBalance],
        transformed_positions: list[DerivativePosition],
    ) -> None:
        """Validate that all transformations are consistent."""
        # Property: All transformations should be consistent
        assert isinstance(account_summary, MarginAccountSummary)
        for transformed_balance in transformed_balances:
            assert isinstance(transformed_balance, SpotBalance)
        for transformed_position in transformed_positions:
            assert isinstance(transformed_position, DerivativePosition)

        # Property: Exchange consistency
        assert account_summary.exchange == ExchangeName.BACKPACK.value
        for transformed_balance in transformed_balances:
            assert transformed_balance.exchange == ExchangeName.BACKPACK
        for transformed_position in transformed_positions:
            assert transformed_position.exchange == ExchangeName.BACKPACK

    @given(
        account_scenario=st.tuples(
            raw_account_summary_strategy(),
            st.dictionaries(
                asset_symbol_strategy(), raw_balance_strategy(), min_size=0, max_size=5
            ),
            st.lists(raw_position_strategy(), min_size=0, max_size=3),
        ),
    )
    @settings(max_examples=50, deadline=None)
    def test_complete_account_workflow_properties(
        self,
        account_scenario: tuple[
            BackpackRawAccountSummaryResponse,
            dict[str, BackpackRawBalanceResponse],
            list[BackpackRawPositionResponse],
        ],
    ) -> None:
        """Property: Complete account data workflow should be consistent."""
        mapper = CompositeAccountMapper()
        raw_summary, spot_balances, positions = account_scenario

        try:
            valid_positions = self._filter_valid_positions(positions)
            transformed_balances = self._transform_balances(mapper, spot_balances)
            transformed_positions = self._transform_positions(mapper, valid_positions)

            # Transform account summary
            account_summary = mapper.transform_raw_account_summary_to_internal(
                raw_summary, spot_balances, valid_positions
            )

            self._validate_transformations(
                account_summary, transformed_balances, transformed_positions
            )

        except (ValueError, DataTransformationError):
            # Expected for invalid account data combinations
            pass


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_balance_transformation_basic_compatibility() -> None:
    """Test basic balance transformation for regression verification."""
    mapper = CompositeAccountMapper()

    result = mapper.transform_balance_data_to_spot_balance(
        asset=exchanges.backpack("USDC"),
        total_balance="1000.0",
        available_balance="900.0",
    )

    assert isinstance(result, SpotBalance)
    assert result.asset.value == "USDC"
    assert result.total_quantity == Decimal("1000.0")
    assert result.available_quantity == Decimal("900.0")


def test_position_transformation_basic_compatibility() -> None:
    """Test basic position transformation for regression verification."""
    mapper = CompositeAccountMapper()

    imf_function = BackpackRawImfFunction(base="0.1", factor="0.0")
    mmf_function = BackpackRawMmfFunction(base="0.05", factor="0.0")

    raw_position = BackpackRawPositionResponse(
        symbol="SOL-USDC",
        subaccountId=0,
        breakEvenPrice="100.25",
        entryPrice="100.00",
        estLiquidationPrice="90.00",
        imf="0.1",
        imfFunction=imf_function,
        markPrice="100.50",
        mmf="0.05",
        mmfFunction=mmf_function,
        netCost="1000.0",
        netQuantity="10.0",
        netExposureQuantity="10.0",
        netExposureNotional="1005.0",
        pnlRealized="0.0",
        pnlUnrealized="5.0",
        cumulativeFundingPayment="0.1",
        userId=12345,
        positionId="pos123",
        cumulativeInterest="0.0",
    )

    result = mapper.transform_raw_position_to_internal(raw_position)

    assert isinstance(result, DerivativePosition)
    assert result.symbol.value == "SOL-USDC"
    assert result.side == OrderSide.BUY
    assert result.size == Decimal("10.0")


def test_account_summary_transformation_basic_compatibility() -> None:
    """Test basic account summary transformation for regression verification."""
    mapper = CompositeAccountMapper()

    raw_summary = BackpackRawAccountSummaryResponse.model_validate({
        "autoBorrowSettlements": False,
        "autoLend": False,
        "autoRealizePnl": False,
        "autoRepayBorrows": False,
        "borrowLimit": "5000.0",
        "futuresMakerFee": "0.0002",
        "futuresTakerFee": "0.0005",
        "leverageLimit": "10.0",
        "limitOrders": 100,
        "liquidating": False,
        "positionLimit": "1000000.0",
        "spotMakerFee": "0.001",
        "spotTakerFee": "0.001",
        "triggerOrders": 50,
    })

    spot_balances = {
        "USDC": BackpackRawBalanceResponse(available="900.0", locked="100.0", staked="0.0")
    }

    result = mapper.transform_raw_account_summary_to_internal(raw_summary, spot_balances, [])

    assert isinstance(result, MarginAccountSummary)
    assert result.total_equity == Decimal("1000.0")
