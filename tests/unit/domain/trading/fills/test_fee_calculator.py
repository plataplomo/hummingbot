"""Property-based tests for Fee Calculator.

This module tests the critical fee calculation logic to ensure:
- Fee calculations are mathematically consistent
- Maker/taker fee rates are correctly applied
- Fee limits (minimum and maximum) are properly enforced
- Exchange-specific fee structures are handled correctly
- Precision is preserved in all calculations
- Fee asset determination follows configured rules
- Percentage-based fees are accurate

SECURITY CRITICAL: Fee calculation errors directly impact profitability
and could lead to incorrect PnL calculations, wrong position sizing,
or failure to detect unprofitable trades.
"""

from decimal import Decimal
from typing import TypedDict
from unittest.mock import MagicMock

import pytest
from hypothesis import HealthCheck, assume, given, settings, strategies as st
from hypothesis.strategies import SearchStrategy

from cyberdelta.config.models.exchange_config import ExchangeSpecificConfig
from cyberdelta.config.models.fee_config import FeeStructureConfig
from cyberdelta.domain.trading.fills.fee_calculator import FeeCalculator
from cyberdelta.enums import ExchangeName, MakerTaker, OrderSide, OrderType
from cyberdelta.models.market.fill import Fill
from cyberdelta.models.market.order import Order


# =============================================================================
# TYPE DEFINITIONS
# =============================================================================


class FeeStructureData(TypedDict):
    """Type definition for fee structure configuration data."""
    maker_fee_rate: Decimal
    taker_fee_rate: Decimal
    fee_calculation_method: str
    minimum_fee: Decimal | None
    maximum_fee: Decimal | None
    fee_asset: str | None


# =============================================================================
# HYPOTHESIS STRATEGIES FOR FEE CALCULATIONS
# =============================================================================


def price_strategy() -> SearchStrategy[Decimal]:
    """Generate valid price values."""
    return st.decimals(
        min_value=Decimal("0.00001"),
        max_value=Decimal(1000000),
        places=8,
        allow_nan=False,
        allow_infinity=False,
    ).filter(lambda x: x > 0)


def quantity_strategy() -> SearchStrategy[Decimal]:
    """Generate valid quantity values."""
    return st.decimals(
        min_value=Decimal("0.00001"),
        max_value=Decimal(10000),
        places=8,
        allow_nan=False,
        allow_infinity=False,
    ).filter(lambda x: x > 0)


def fee_rate_strategy() -> SearchStrategy[Decimal]:
    """Generate valid fee rate values (as decimals, e.g., 0.001 = 0.1%)."""
    return st.decimals(
        min_value=Decimal(0),
        max_value=Decimal("0.01"),  # Max 1% fee
        places=6,
        allow_nan=False,
        allow_infinity=False,
    )


def fee_limit_strategy() -> SearchStrategy[Decimal | None]:
    """Generate valid fee limit values."""
    return st.one_of(
        st.none(),
        st.decimals(
            min_value=Decimal("0.0001"),
            max_value=Decimal(100),
            places=4,
            allow_nan=False,
            allow_infinity=False,
        ),
    )


def maker_taker_strategy() -> SearchStrategy[MakerTaker]:
    """Generate valid maker/taker values."""
    return st.sampled_from([MakerTaker.MAKER, MakerTaker.TAKER])


def fee_method_strategy() -> SearchStrategy[str]:
    """Generate valid fee calculation methods."""
    return st.sampled_from(["percentage", "fixed"])


def fee_asset_strategy() -> SearchStrategy[str | None]:
    """Generate valid fee asset values."""
    return st.one_of(st.none(), st.sampled_from(["USDC", "USD", "BTC", "ETH", "SOL"]))


def exchange_name_strategy() -> SearchStrategy[ExchangeName]:
    """Generate valid exchange names."""
    return st.sampled_from([ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK])


def order_side_strategy() -> SearchStrategy[OrderSide]:
    """Generate valid order sides."""
    return st.sampled_from([OrderSide.BUY, OrderSide.SELL])


def order_type_strategy() -> SearchStrategy[OrderType]:
    """Generate valid order types."""
    return st.sampled_from([
        OrderType.LIMIT,
        OrderType.MARKET,
        OrderType.STOP_LIMIT,
        OrderType.STOP_MARKET,
    ])


def fee_structure_strategy() -> SearchStrategy[FeeStructureData]:
    """Generate valid fee structure configuration data."""
    
    def _build_fee_structure(
        maker_rate: Decimal,
        taker_rate: Decimal,
        method: str,
        min_fee: Decimal | None,
        max_fee: Decimal | None,
        asset: str | None,
    ) -> FeeStructureData:
        return {
            "maker_fee_rate": maker_rate,
            "taker_fee_rate": taker_rate,
            "fee_calculation_method": method,
            "minimum_fee": min_fee,
            "maximum_fee": max_fee,
            "fee_asset": asset,
        }

    return st.builds(
        _build_fee_structure,
        maker_rate=fee_rate_strategy(),
        taker_rate=fee_rate_strategy(),
        method=fee_method_strategy(),
        min_fee=fee_limit_strategy(),
        max_fee=fee_limit_strategy(),
        asset=fee_asset_strategy(),
    )


# =============================================================================
# TEST FIXTURES
# =============================================================================


def create_mock_order(
    symbol: str = "BTC",
    exchange: ExchangeName = ExchangeName.HYPERLIQUID,
    side: OrderSide = OrderSide.BUY,
    order_type: OrderType = OrderType.LIMIT,
    price: Decimal = Decimal(50000),
    quantity: Decimal = Decimal("1.0"),
) -> Order:
    """Create a mock Order object for testing."""
    order = MagicMock(spec=Order)

    # Create mock symbol with configurable value
    mock_symbol = MagicMock()
    mock_symbol.value = symbol

    order.symbol = mock_symbol
    order.exchange = exchange
    order.side = side
    order.order_type = order_type
    order.price = price
    order.quantity_requested = quantity
    return order


def create_mock_fill(
    maker_taker: MakerTaker | None = MakerTaker.TAKER,
    fee: Decimal | None = None,
    fee_asset: str | None = None,
) -> Fill:
    """Create a mock Fill object for testing."""
    fill = MagicMock(spec=Fill)
    fill.maker_taker = maker_taker
    fill.fee = fee
    fill.fee_asset = fee_asset
    return fill


def create_mock_exchange_config(fee_structure_data: FeeStructureData) -> ExchangeSpecificConfig:
    """Create a mock exchange configuration."""
    config = MagicMock(spec=ExchangeSpecificConfig)

    # Create fee structure from data
    fee_structure = MagicMock(spec=FeeStructureConfig)
    fee_structure.maker_fee_rate = fee_structure_data["maker_fee_rate"]
    fee_structure.taker_fee_rate = fee_structure_data["taker_fee_rate"]
    fee_structure.fee_calculation_method = fee_structure_data["fee_calculation_method"]
    fee_structure.minimum_fee = fee_structure_data.get("minimum_fee")
    fee_structure.maximum_fee = fee_structure_data.get("maximum_fee")
    fee_structure.fee_asset = fee_structure_data.get("fee_asset")

    config.fee_structure = fee_structure
    return config


# =============================================================================
# PROPERTY TESTS FOR FEE CALCULATION
# =============================================================================


class TestFeeCalculationProperties:
    """Property-based tests for core fee calculation logic."""

    @given(
        fill_price=price_strategy(),
        fill_quantity=quantity_strategy(),
        fee_structure_data=fee_structure_strategy(),
        maker_taker=maker_taker_strategy(),
    )
    def test_percentage_fee_calculation_accuracy(
        self,
        fill_price: Decimal,
        fill_quantity: Decimal,
        fee_structure_data: FeeStructureData,
        maker_taker: MakerTaker,
    ) -> None:
        """Property: Percentage-based fees should be mathematically accurate."""
        # Force percentage method
        fee_structure_data["fee_calculation_method"] = "percentage"

        order = create_mock_order(price=fill_price, quantity=fill_quantity)
        fill = create_mock_fill(maker_taker=maker_taker)
        exchange_config = create_mock_exchange_config(fee_structure_data)

        fee_amount, _ = FeeCalculator.calculate_fee(
            order, fill_price, fill_quantity, fill, exchange_config
        )

        # Determine expected fee rate
        expected_rate = (
            fee_structure_data["maker_fee_rate"]
            if maker_taker == MakerTaker.MAKER
            else fee_structure_data["taker_fee_rate"]
        )

        # Calculate expected fee
        trade_value = fill_price * fill_quantity
        expected_fee = trade_value * expected_rate

        # Apply limits if configured
        if fee_structure_data["minimum_fee"] is not None:
            expected_fee = max(expected_fee, fee_structure_data["minimum_fee"])
        if fee_structure_data["maximum_fee"] is not None:
            expected_fee = min(expected_fee, fee_structure_data["maximum_fee"])

        # Property: Fee should match expected calculation
        assert fee_amount == expected_fee

        # Property: Fee should be Decimal type
        assert isinstance(fee_amount, Decimal)

        # Property: Fee should be non-negative
        assert fee_amount >= Decimal(0)

    @given(
        fill_price=price_strategy(), fill_quantity=quantity_strategy(), fee_rate=fee_rate_strategy()
    )
    def test_fixed_fee_calculation(
        self, fill_price: Decimal, fill_quantity: Decimal, fee_rate: Decimal
    ) -> None:
        """Property: Fixed fees should not depend on trade value."""
        fee_structure_data: FeeStructureData = {
            "maker_fee_rate": fee_rate,
            "taker_fee_rate": fee_rate,
            "fee_calculation_method": "fixed",
            "minimum_fee": None,
            "maximum_fee": None,
            "fee_asset": "USDC",
        }

        order = create_mock_order(price=fill_price, quantity=fill_quantity)
        fill = create_mock_fill(maker_taker=MakerTaker.TAKER)
        exchange_config = create_mock_exchange_config(fee_structure_data)

        fee_amount, _ = FeeCalculator.calculate_fee(
            order, fill_price, fill_quantity, fill, exchange_config
        )

        # Property: Fixed fee should equal the fee rate directly
        assert fee_amount == fee_rate

        # Property: Fixed fee should not depend on trade value
        # Test with different trade value
        order2 = create_mock_order(
            price=fill_price * Decimal(2), quantity=fill_quantity * Decimal(2)
        )
        fee_amount2, _ = FeeCalculator.calculate_fee(
            order2, fill_price * Decimal(2), fill_quantity * Decimal(2), fill, exchange_config
        )

        assert fee_amount == fee_amount2  # Same fixed fee regardless of trade value

    @given(
        fill_price=price_strategy(),
        fill_quantity=quantity_strategy(),
        maker_rate=fee_rate_strategy(),
        taker_rate=fee_rate_strategy(),
    )
    def test_maker_taker_rate_distinction(
        self,
        fill_price: Decimal,
        fill_quantity: Decimal,
        maker_rate: Decimal,
        taker_rate: Decimal,
    ) -> None:
        """Property: Maker and taker rates should be correctly applied."""
        assume(maker_rate != taker_rate)  # Only test when rates differ

        fee_structure_data: FeeStructureData = {
            "maker_fee_rate": maker_rate,
            "taker_fee_rate": taker_rate,
            "fee_calculation_method": "percentage",
            "minimum_fee": None,
            "maximum_fee": None,
            "fee_asset": "USDC",
        }

        order = create_mock_order(price=fill_price, quantity=fill_quantity)
        exchange_config = create_mock_exchange_config(fee_structure_data)

        # Calculate fee as maker
        fill_maker = create_mock_fill(maker_taker=MakerTaker.MAKER)
        fee_maker, _ = FeeCalculator.calculate_fee(
            order, fill_price, fill_quantity, fill_maker, exchange_config
        )

        # Calculate fee as taker
        fill_taker = create_mock_fill(maker_taker=MakerTaker.TAKER)
        fee_taker, _ = FeeCalculator.calculate_fee(
            order, fill_price, fill_quantity, fill_taker, exchange_config
        )

        trade_value = fill_price * fill_quantity

        # Property: Maker fee should use maker rate
        assert fee_maker == trade_value * maker_rate

        # Property: Taker fee should use taker rate
        assert fee_taker == trade_value * taker_rate

        # Property: When rates differ, fees should differ
        if maker_rate < taker_rate:
            assert fee_maker < fee_taker
        else:
            assert fee_maker > fee_taker


# =============================================================================
# PROPERTY TESTS FOR FEE LIMITS
# =============================================================================


class TestFeeLimitProperties:
    """Property-based tests for fee limit enforcement."""

    @given(
        fill_price=price_strategy(),
        fill_quantity=quantity_strategy(),
        fee_rate=fee_rate_strategy(),
        minimum_fee=st.decimals(min_value=Decimal("0.01"), max_value=Decimal(1), places=4),
    )
    def test_minimum_fee_enforcement(
        self,
        fill_price: Decimal,
        fill_quantity: Decimal,
        fee_rate: Decimal,
        minimum_fee: Decimal,
    ) -> None:
        """Property: Minimum fee should always be enforced."""
        # Create a scenario where calculated fee would be less than minimum
        trade_value = fill_price * fill_quantity
        calculated_fee = trade_value * fee_rate

        # Only test when calculated fee would be less than minimum
        assume(calculated_fee < minimum_fee)

        fee_structure_data: FeeStructureData = {
            "maker_fee_rate": fee_rate,
            "taker_fee_rate": fee_rate,
            "fee_calculation_method": "percentage",
            "minimum_fee": minimum_fee,
            "maximum_fee": None,
            "fee_asset": "USDC",
        }

        order = create_mock_order(price=fill_price, quantity=fill_quantity)
        fill = create_mock_fill(maker_taker=MakerTaker.TAKER)
        exchange_config = create_mock_exchange_config(fee_structure_data)

        fee_amount, _ = FeeCalculator.calculate_fee(
            order, fill_price, fill_quantity, fill, exchange_config
        )

        # Property: Fee should be at least the minimum
        assert fee_amount >= minimum_fee

        # Property: Fee should equal minimum when calculated fee is less
        assert fee_amount == minimum_fee

    @settings(suppress_health_check=[HealthCheck.filter_too_much])
    @given(
        fill_price=st.decimals(min_value=Decimal(1000), max_value=Decimal(100000), places=2),
        fill_quantity=st.decimals(min_value=Decimal(10), max_value=Decimal(100), places=2),
        fee_rate=st.decimals(min_value=Decimal("0.005"), max_value=Decimal("0.01"), places=4),
        maximum_fee=st.decimals(min_value=Decimal("0.01"), max_value=Decimal(10), places=4),
    )
    def test_maximum_fee_enforcement(
        self,
        fill_price: Decimal,
        fill_quantity: Decimal,
        fee_rate: Decimal,
        maximum_fee: Decimal,
    ) -> None:
        """Property: Maximum fee should always be enforced."""
        # Create a scenario where calculated fee would exceed maximum
        trade_value = fill_price * fill_quantity
        calculated_fee = trade_value * fee_rate

        # Only test when calculated fee would exceed maximum
        assume(calculated_fee > maximum_fee)

        fee_structure_data: FeeStructureData = {
            "maker_fee_rate": fee_rate,
            "taker_fee_rate": fee_rate,
            "fee_calculation_method": "percentage",
            "minimum_fee": None,
            "maximum_fee": maximum_fee,
            "fee_asset": "USDC",
        }

        order = create_mock_order(price=fill_price, quantity=fill_quantity)
        fill = create_mock_fill(maker_taker=MakerTaker.TAKER)
        exchange_config = create_mock_exchange_config(fee_structure_data)

        fee_amount, _ = FeeCalculator.calculate_fee(
            order, fill_price, fill_quantity, fill, exchange_config
        )

        # Property: Fee should not exceed maximum
        assert fee_amount <= maximum_fee

        # Property: Fee should equal maximum when calculated fee exceeds it
        assert fee_amount == maximum_fee

    @given(
        fill_price=price_strategy(),
        fill_quantity=quantity_strategy(),
        fee_rate=fee_rate_strategy(),
        minimum_fee=st.decimals(min_value=Decimal("0.01"), max_value=Decimal(1), places=4),
        maximum_fee=st.decimals(min_value=Decimal(1), max_value=Decimal(10), places=4),
    )
    def test_fee_limit_consistency(
        self,
        fill_price: Decimal,
        fill_quantity: Decimal,
        fee_rate: Decimal,
        minimum_fee: Decimal,
        maximum_fee: Decimal,
    ) -> None:
        """Property: Fee should always be within configured limits."""
        assume(minimum_fee < maximum_fee)  # Ensure valid configuration

        fee_structure_data: FeeStructureData = {
            "maker_fee_rate": fee_rate,
            "taker_fee_rate": fee_rate,
            "fee_calculation_method": "percentage",
            "minimum_fee": minimum_fee,
            "maximum_fee": maximum_fee,
            "fee_asset": "USDC",
        }

        order = create_mock_order(price=fill_price, quantity=fill_quantity)
        fill = create_mock_fill(maker_taker=MakerTaker.TAKER)
        exchange_config = create_mock_exchange_config(fee_structure_data)

        fee_amount, _ = FeeCalculator.calculate_fee(
            order, fill_price, fill_quantity, fill, exchange_config
        )

        # Property: Fee should be within limits
        assert minimum_fee <= fee_amount <= maximum_fee


# =============================================================================
# PROPERTY TESTS FOR FEE ASSET DETERMINATION
# =============================================================================


class TestFeeAssetProperties:
    """Property-based tests for fee asset determination."""

    @given(
        fill_price=price_strategy(),
        fill_quantity=quantity_strategy(),
        fee_asset=fee_asset_strategy(),
        symbol=st.sampled_from(["BTC_USDC", "ETH_USD", "SOL_USDT"]),
    )
    def test_fee_asset_configuration(
        self, fill_price: Decimal, fill_quantity: Decimal, fee_asset: str | None, symbol: str
    ) -> None:
        """Property: Configured fee asset should be used when specified."""
        fee_structure_data: FeeStructureData = {
            "maker_fee_rate": Decimal("0.001"),
            "taker_fee_rate": Decimal("0.001"),
            "fee_calculation_method": "percentage",
            "minimum_fee": None,
            "maximum_fee": None,
            "fee_asset": fee_asset,
        }

        # Create order with custom symbol
        order = create_mock_order(symbol=symbol, price=fill_price, quantity=fill_quantity)

        fill = create_mock_fill(maker_taker=MakerTaker.TAKER)
        exchange_config = create_mock_exchange_config(fee_structure_data)

        _, returned_fee_asset = FeeCalculator.calculate_fee(
            order, fill_price, fill_quantity, fill, exchange_config
        )

        if fee_asset is not None:
            # Property: When configured, fee asset should match configuration
            assert returned_fee_asset == fee_asset
        else:
            # Property: When not configured, should use quote currency
            symbol_parts = symbol.split("_")
            expected_asset = symbol_parts[-1] if len(symbol_parts) > 1 else "USDC"
            assert returned_fee_asset == expected_asset

    def test_fee_asset_default_behavior(self) -> None:
        """Property: Default fee asset should be quote currency."""
        fee_structure_data: FeeStructureData = {
            "maker_fee_rate": Decimal("0.001"),
            "taker_fee_rate": Decimal("0.001"),
            "fee_calculation_method": "percentage",
            "minimum_fee": None,
            "maximum_fee": None,
            "fee_asset": None,  # No configured fee asset
        }

        test_cases = [
            ("BTC_USDC", "USDC"),
            ("ETH_USD", "USD"),
            ("SOL_USDT", "USDT"),
            ("BTC", "USDC"),  # Single part symbol defaults to USDC
        ]

        for symbol, expected_asset in test_cases:
            order = create_mock_order(symbol=symbol)

            fill = create_mock_fill(maker_taker=MakerTaker.TAKER)
            exchange_config = create_mock_exchange_config(fee_structure_data)

            _, fee_asset = FeeCalculator.calculate_fee(
                order, Decimal(50000), Decimal(1), fill, exchange_config
            )

            assert fee_asset == expected_asset


# =============================================================================
# PROPERTY TESTS FOR PRECISION PRESERVATION
# =============================================================================


class TestPrecisionPreservation:
    """Property tests for decimal precision preservation."""

    @given(
        fill_price=st.decimals(
            min_value=Decimal("0.00000001"), max_value=Decimal("999999.99999999"), places=8
        ).filter(lambda x: x > 0),
        fill_quantity=st.decimals(
            min_value=Decimal("0.00000001"), max_value=Decimal("9999.99999999"), places=8
        ).filter(lambda x: x > 0),
        fee_rate=st.decimals(min_value=Decimal("0.000001"), max_value=Decimal("0.01"), places=6),
    )
    def test_decimal_precision_maintained(
        self, fill_price: Decimal, fill_quantity: Decimal, fee_rate: Decimal
    ) -> None:
        """Property: Decimal precision should be preserved throughout calculations."""
        fee_structure_data: FeeStructureData = {
            "maker_fee_rate": fee_rate,
            "taker_fee_rate": fee_rate,
            "fee_calculation_method": "percentage",
            "minimum_fee": None,
            "maximum_fee": None,
            "fee_asset": "USDC",
        }

        order = create_mock_order(price=fill_price, quantity=fill_quantity)
        fill = create_mock_fill(maker_taker=MakerTaker.TAKER)
        exchange_config = create_mock_exchange_config(fee_structure_data)

        fee_amount, _ = FeeCalculator.calculate_fee(
            order, fill_price, fill_quantity, fill, exchange_config
        )

        # Property: Fee should be a Decimal
        assert isinstance(fee_amount, Decimal)

        # Property: Fee should be finite
        assert fee_amount.is_finite()

        # Property: Fee calculation should be exact
        expected_fee = fill_price * fill_quantity * fee_rate
        assert fee_amount == expected_fee

        # Property: No floating point errors
        # Verify by converting to string and back
        fee_str = str(fee_amount)
        fee_from_str = Decimal(fee_str)
        assert fee_amount == fee_from_str

    @given(
        prices=st.lists(
            st.decimals(min_value=Decimal("0.01"), max_value=Decimal(100000), places=6).filter(
                lambda x: x > 0
            ),
            min_size=5,
            max_size=20,
        ),
        quantities=st.lists(
            st.decimals(min_value=Decimal("0.001"), max_value=Decimal(100), places=6).filter(
                lambda x: x > 0
            ),
            min_size=5,
            max_size=20,
        ),
        fee_rate=fee_rate_strategy(),
    )
    def test_cumulative_fee_precision(
        self, prices: list[Decimal], quantities: list[Decimal], fee_rate: Decimal
    ) -> None:
        """Property: Cumulative fees should maintain precision."""
        assume(len(prices) == len(quantities))

        fee_structure_data: FeeStructureData = {
            "maker_fee_rate": fee_rate,
            "taker_fee_rate": fee_rate,
            "fee_calculation_method": "percentage",
            "minimum_fee": None,
            "maximum_fee": None,
            "fee_asset": "USDC",
        }

        exchange_config = create_mock_exchange_config(fee_structure_data)
        total_fees = Decimal(0)
        total_trade_value = Decimal(0)

        for price, quantity in zip(prices, quantities, strict=False):
            order = create_mock_order(price=price, quantity=quantity)
            fill = create_mock_fill(maker_taker=MakerTaker.TAKER)

            fee_amount, _ = FeeCalculator.calculate_fee(
                order, price, quantity, fill, exchange_config
            )

            total_fees += fee_amount
            total_trade_value += price * quantity

        # Property: Total fees should equal fee rate times total trade value
        expected_total_fees = total_trade_value * fee_rate
        assert total_fees == expected_total_fees

        # Property: Precision should be maintained
        assert total_fees.is_finite()
        assert isinstance(total_fees, Decimal)


# =============================================================================
# PROPERTY TESTS FOR EXCHANGE-SPECIFIC CONFIGURATIONS
# =============================================================================


class TestExchangeSpecificProperties:
    """Property tests for exchange-specific fee structures."""

    @given(
        fill_price=price_strategy(),
        fill_quantity=quantity_strategy(),
        exchange=exchange_name_strategy(),
        maker_rate=fee_rate_strategy(),
        taker_rate=fee_rate_strategy(),
    )
    def test_exchange_fee_structure_independence(
        self,
        fill_price: Decimal,
        fill_quantity: Decimal,
        exchange: ExchangeName,
        maker_rate: Decimal,
        taker_rate: Decimal,
    ) -> None:
        """Property: Each exchange should use its own fee structure."""
        fee_structure_data: FeeStructureData = {
            "maker_fee_rate": maker_rate,
            "taker_fee_rate": taker_rate,
            "fee_calculation_method": "percentage",
            "minimum_fee": None,
            "maximum_fee": None,
            "fee_asset": "USDC",
        }

        order = create_mock_order(price=fill_price, quantity=fill_quantity, exchange=exchange)
        fill = create_mock_fill(maker_taker=MakerTaker.TAKER)
        exchange_config = create_mock_exchange_config(fee_structure_data)

        fee_amount, _ = FeeCalculator.calculate_fee(
            order, fill_price, fill_quantity, fill, exchange_config
        )

        # Property: Fee should be based on exchange-specific configuration
        expected_fee = fill_price * fill_quantity * taker_rate
        assert fee_amount == expected_fee

        # Property: Exchange name should not affect calculation logic
        # (only the configuration matters)
        order2 = create_mock_order(
            price=fill_price,
            quantity=fill_quantity,
            exchange=ExchangeName.HYPERLIQUID
            if exchange == ExchangeName.BACKPACK
            else ExchangeName.BACKPACK,
        )

        fee_amount2, _ = FeeCalculator.calculate_fee(
            order2, fill_price, fill_quantity, fill, exchange_config
        )

        # Same config should give same fee regardless of exchange enum
        assert fee_amount == fee_amount2

    def test_missing_fee_structure_handling(self) -> None:
        """Property: Missing fee structure should raise an error."""
        order = create_mock_order()
        fill = create_mock_fill()

        # Create config without fee structure
        exchange_config = MagicMock(spec=ExchangeSpecificConfig)
        exchange_config.fee_structure = None

        # Property: Should raise ValueError for missing fee structure
        with pytest.raises(ValueError, match="Fee structure not configured"):
            FeeCalculator.calculate_fee(
                order, Decimal(50000), Decimal(1), fill, exchange_config
            )


# =============================================================================
# PROPERTY TESTS FOR EDGE CASES
# =============================================================================


class TestEdgeCases:
    """Property tests for edge cases in fee calculation."""

    @given(fill_quantity=quantity_strategy(), fee_rate=fee_rate_strategy())
    def test_zero_price_handling(self, fill_quantity: Decimal, fee_rate: Decimal) -> None:
        """Property: Zero price should result in zero fee for percentage method."""
        fee_structure_data: FeeStructureData = {
            "maker_fee_rate": fee_rate,
            "taker_fee_rate": fee_rate,
            "fee_calculation_method": "percentage",
            "minimum_fee": None,
            "maximum_fee": None,
            "fee_asset": "USDC",
        }

        order = create_mock_order(price=Decimal(0), quantity=fill_quantity)
        fill = create_mock_fill(maker_taker=MakerTaker.TAKER)
        exchange_config = create_mock_exchange_config(fee_structure_data)

        fee_amount, _ = FeeCalculator.calculate_fee(
            order, Decimal(0), fill_quantity, fill, exchange_config
        )

        # Property: Zero price should give zero fee (unless minimum fee exists)
        assert fee_amount == Decimal(0)

    @given(fill_price=price_strategy(), fee_rate=fee_rate_strategy())
    def test_zero_quantity_handling(self, fill_price: Decimal, fee_rate: Decimal) -> None:
        """Property: Zero quantity should result in zero fee for percentage method."""
        fee_structure_data: FeeStructureData = {
            "maker_fee_rate": fee_rate,
            "taker_fee_rate": fee_rate,
            "fee_calculation_method": "percentage",
            "minimum_fee": None,
            "maximum_fee": None,
            "fee_asset": "USDC",
        }

        order = create_mock_order(price=fill_price, quantity=Decimal(0))
        fill = create_mock_fill(maker_taker=MakerTaker.TAKER)
        exchange_config = create_mock_exchange_config(fee_structure_data)

        fee_amount, _ = FeeCalculator.calculate_fee(
            order, fill_price, Decimal(0), fill, exchange_config
        )

        # Property: Zero quantity should give zero fee (unless minimum fee exists)
        assert fee_amount == Decimal(0)

    @given(
        fill_price=price_strategy(),
        fill_quantity=quantity_strategy(),
        fee_structure_data=fee_structure_strategy(),
    )
    def test_none_maker_taker_default(
        self,
        fill_price: Decimal,
        fill_quantity: Decimal,
        fee_structure_data: FeeStructureData,
    ) -> None:
        """Property: None maker_taker should default to TAKER."""
        order = create_mock_order(price=fill_price, quantity=fill_quantity)
        fill = create_mock_fill(maker_taker=None)  # None maker_taker
        exchange_config = create_mock_exchange_config(fee_structure_data)

        fee_amount, _ = FeeCalculator.calculate_fee(
            order, fill_price, fill_quantity, fill, exchange_config
        )

        # Calculate expected fee using taker rate (default)
        if fee_structure_data["fee_calculation_method"] == "percentage":
            trade_value = fill_price * fill_quantity
            expected_fee = trade_value * fee_structure_data["taker_fee_rate"]
        else:
            expected_fee = fee_structure_data["taker_fee_rate"]

        # Apply limits
        if fee_structure_data["minimum_fee"] is not None:
            expected_fee = max(expected_fee, fee_structure_data["minimum_fee"])
        if fee_structure_data["maximum_fee"] is not None:
            expected_fee = min(expected_fee, fee_structure_data["maximum_fee"])

        # Property: Should use taker rate when maker_taker is None
        assert fee_amount == expected_fee

    @given(
        very_small_price=st.decimals(
            min_value=Decimal("0.00000001"), max_value=Decimal("0.001"), places=10
        ).filter(lambda x: x > 0),
        very_small_quantity=st.decimals(
            min_value=Decimal("0.00000001"), max_value=Decimal("0.001"), places=10
        ).filter(lambda x: x > 0),
        fee_rate=fee_rate_strategy(),
    )
    def test_very_small_values(
        self, very_small_price: Decimal, very_small_quantity: Decimal, fee_rate: Decimal
    ) -> None:
        """Property: Very small trades should calculate fees correctly."""
        fee_structure_data: FeeStructureData = {
            "maker_fee_rate": fee_rate,
            "taker_fee_rate": fee_rate,
            "fee_calculation_method": "percentage",
            "minimum_fee": None,
            "maximum_fee": None,
            "fee_asset": "USDC",
        }

        order = create_mock_order(price=very_small_price, quantity=very_small_quantity)
        fill = create_mock_fill(maker_taker=MakerTaker.TAKER)
        exchange_config = create_mock_exchange_config(fee_structure_data)

        fee_amount, _ = FeeCalculator.calculate_fee(
            order, very_small_price, very_small_quantity, fill, exchange_config
        )

        # Property: Fee should be calculated correctly even for tiny values
        expected_fee = very_small_price * very_small_quantity * fee_rate
        assert fee_amount == expected_fee

        # Property: Fee should maintain precision
        assert isinstance(fee_amount, Decimal)
        assert fee_amount.is_finite()

    @given(
        very_large_price=st.decimals(
            min_value=Decimal(100000), max_value=Decimal(10000000), places=2
        ).filter(lambda x: x > 0),
        very_large_quantity=st.decimals(
            min_value=Decimal(1000), max_value=Decimal(100000), places=2
        ).filter(lambda x: x > 0),
        fee_rate=fee_rate_strategy(),
    )
    def test_very_large_values(
        self, very_large_price: Decimal, very_large_quantity: Decimal, fee_rate: Decimal
    ) -> None:
        """Property: Very large trades should calculate fees correctly."""
        fee_structure_data: FeeStructureData = {
            "maker_fee_rate": fee_rate,
            "taker_fee_rate": fee_rate,
            "fee_calculation_method": "percentage",
            "minimum_fee": None,
            "maximum_fee": None,
            "fee_asset": "USDC",
        }

        order = create_mock_order(price=very_large_price, quantity=very_large_quantity)
        fill = create_mock_fill(maker_taker=MakerTaker.TAKER)
        exchange_config = create_mock_exchange_config(fee_structure_data)

        fee_amount, _ = FeeCalculator.calculate_fee(
            order, very_large_price, very_large_quantity, fill, exchange_config
        )

        # Property: Fee should be calculated correctly even for large values
        expected_fee = very_large_price * very_large_quantity * fee_rate
        assert fee_amount == expected_fee

        # Property: No overflow should occur
        assert isinstance(fee_amount, Decimal)
        assert fee_amount.is_finite()


# =============================================================================
# INTEGRATION PROPERTY TESTS
# =============================================================================


class TestFeeCalculatorIntegration:
    """Integration property tests for complete fee calculation flow."""

    @given(
        fill_price=price_strategy(),
        fill_quantity=quantity_strategy(),
        fee_structure_data=fee_structure_strategy(),
        maker_taker=maker_taker_strategy(),
    )
    def test_fee_calculation_determinism(
        self,
        fill_price: Decimal,
        fill_quantity: Decimal,
        fee_structure_data: FeeStructureData,
        maker_taker: MakerTaker,
    ) -> None:
        """Property: Fee calculation should be deterministic."""
        order = create_mock_order(price=fill_price, quantity=fill_quantity)
        fill = create_mock_fill(maker_taker=maker_taker)
        exchange_config = create_mock_exchange_config(fee_structure_data)

        # Calculate fee multiple times
        fee1, asset1 = FeeCalculator.calculate_fee(
            order, fill_price, fill_quantity, fill, exchange_config
        )
        fee2, asset2 = FeeCalculator.calculate_fee(
            order, fill_price, fill_quantity, fill, exchange_config
        )
        fee3, asset3 = FeeCalculator.calculate_fee(
            order, fill_price, fill_quantity, fill, exchange_config
        )

        # Property: Same inputs should always give same outputs
        assert fee1 == fee2 == fee3
        assert asset1 == asset2 == asset3

    @given(
        trades=st.lists(
            st.tuples(price_strategy(), quantity_strategy(), maker_taker_strategy()),
            min_size=1,
            max_size=10,
        ),
        fee_structure_data=fee_structure_strategy(),
    )
    def test_multiple_trades_fee_consistency(
        self,
        trades: list[tuple[Decimal, Decimal, MakerTaker]],
        fee_structure_data: FeeStructureData,
    ) -> None:
        """Property: Fees for multiple trades should be additive."""
        exchange_config = create_mock_exchange_config(fee_structure_data)
        total_fees = Decimal(0)
        total_trade_value = Decimal(0)

        for price, quantity, maker_taker in trades:
            order = create_mock_order(price=price, quantity=quantity)
            fill = create_mock_fill(maker_taker=maker_taker)

            fee_amount, _ = FeeCalculator.calculate_fee(
                order, price, quantity, fill, exchange_config
            )

            total_fees += fee_amount
            total_trade_value += price * quantity

        # Property: Total fees should be non-negative
        assert total_fees >= Decimal(0)

        # Property: Fees should be finite
        assert total_fees.is_finite()

        # Property: For percentage fees, total should not exceed max rate * total value
        if fee_structure_data["fee_calculation_method"] == "percentage":
            max_rate = max(
                fee_structure_data["maker_fee_rate"], fee_structure_data["taker_fee_rate"]
            )

            # Account for minimum fees which might increase total
            if fee_structure_data["minimum_fee"] is None:
                assert total_fees <= total_trade_value * max_rate * Decimal("1.01")  # 1% tolerance

    @given(
        fill_price=price_strategy(),
        fill_quantity=quantity_strategy(),
        maker_rate=fee_rate_strategy(),
        taker_rate=fee_rate_strategy(),
        min_fee=fee_limit_strategy(),
        max_fee=fee_limit_strategy(),
    )
    def test_fee_configuration_validity(
        self,
        fill_price: Decimal,
        fill_quantity: Decimal,
        maker_rate: Decimal,
        taker_rate: Decimal,
        min_fee: Decimal | None,
        max_fee: Decimal | None,
    ) -> None:
        """Property: Valid configurations should always produce valid fees."""
        # Skip invalid configurations
        if min_fee is not None and max_fee is not None:
            assume(min_fee <= max_fee)

        fee_structure_data: FeeStructureData = {
            "maker_fee_rate": maker_rate,
            "taker_fee_rate": taker_rate,
            "fee_calculation_method": "percentage",
            "minimum_fee": min_fee,
            "maximum_fee": max_fee,
            "fee_asset": "USDC",
        }

        order = create_mock_order(price=fill_price, quantity=fill_quantity)
        fill = create_mock_fill(maker_taker=MakerTaker.TAKER)
        exchange_config = create_mock_exchange_config(fee_structure_data)

        fee_amount, fee_asset = FeeCalculator.calculate_fee(
            order, fill_price, fill_quantity, fill, exchange_config
        )

        # Property: Fee should always be valid
        assert isinstance(fee_amount, Decimal)
        assert fee_amount >= Decimal(0)
        assert fee_amount.is_finite()

        # Property: Fee asset should always be a string
        assert isinstance(fee_asset, str)
        assert len(fee_asset) > 0
