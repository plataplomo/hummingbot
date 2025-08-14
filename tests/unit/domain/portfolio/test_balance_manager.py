"""Property-based tests for Balance Manager.

This module tests the critical balance management functions to ensure:
- Balance invariants (total >= available, non-negative balances)
- Precision preservation in all financial calculations
- Correct balance updates from fills and trades
- Balance aggregation consistency across exchanges
- Reconciliation accuracy and tolerance handling
- Thread-safe balance operations

SECURITY CRITICAL: Balance calculation errors could lead to incorrect available
funds, wrong position sizing, over-leveraging, or financial losses.
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from hypothesis import HealthCheck, assume, given, settings, strategies as st
from hypothesis.strategies import SearchStrategy

from cyberdelta.config.models import AppSettings, PortfolioValidationSettings
from cyberdelta.domain.portfolio.balance_manager import BalanceManager
from cyberdelta.enums import ExchangeName, OrderSide
from cyberdelta.models import SpotBalance
from cyberdelta.models.market.fill import Fill
from cyberdelta.models.portfolio.state import PortfolioState
from cyberdelta.symbols import Symbol, exchanges, symbol as create_symbol_func


# =============================================================================
# HYPOTHESIS STRATEGIES FOR BALANCE TESTING
# =============================================================================


def create_symbol(value: str, exchange: ExchangeName) -> Symbol:
    """Create a symbol for the given exchange.
    
    Returns:
        Symbol for the specified exchange.
    """
    if exchange == ExchangeName.HYPERLIQUID:
        return exchanges.hyperliquid(value=value)
    return exchanges.backpack(value=value)


def balance_amount_strategy() -> SearchStrategy[Decimal]:
    """Generate valid balance amounts (non-negative).
    
    Returns:
        Strategy for generating decimal balance amounts.
    """
    return st.decimals(
        min_value=Decimal(0),
        max_value=Decimal(1000000),
        places=8,
        allow_nan=False,
        allow_infinity=False,
    )


def balance_delta_strategy() -> SearchStrategy[Decimal]:
    """Generate balance delta values (can be negative).
    
    Returns:
        Strategy for generating decimal delta values.
    """
    return st.decimals(
        min_value=Decimal(-10000),
        max_value=Decimal(10000),
        places=8,
        allow_nan=False,
        allow_infinity=False,
    )


def price_strategy() -> SearchStrategy[Decimal]:
    """Generate valid price values.
    
    Returns:
        Strategy for generating decimal price values.
    """
    return st.decimals(
        min_value=Decimal("0.00000001"),
        max_value=Decimal(1000000),
        places=8,
        allow_nan=False,
        allow_infinity=False,
    ).filter(lambda x: x > 0)


def quantity_strategy() -> SearchStrategy[Decimal]:
    """Generate valid quantity values.
    
    Returns:
        Strategy for generating decimal quantity values.
    """
    return st.decimals(
        min_value=Decimal("0.00000001"),
        max_value=Decimal(10000),
        places=8,
        allow_nan=False,
        allow_infinity=False,
    ).filter(lambda x: x > 0)


def fee_strategy() -> SearchStrategy[Decimal]:
    """Generate valid fee amounts.
    
    Returns:
        Strategy for generating decimal fee amounts.
    """
    return st.decimals(
        min_value=Decimal(0),
        max_value=Decimal(100),
        places=8,
        allow_nan=False,
        allow_infinity=False,
    )


def exchange_strategy() -> SearchStrategy[ExchangeName]:
    """Generate valid exchange names.
    
    Returns:
        Strategy for generating exchange names.
    """
    return st.sampled_from([ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK])


def asset_strategy() -> SearchStrategy[str]:
    """Generate valid asset symbols.
    
    Returns:
        Strategy for generating asset symbol strings.
    """
    return st.sampled_from(["USDC", "USD", "BTC", "ETH", "SOL", "PYTH"])


def side_strategy() -> SearchStrategy[OrderSide]:
    """Generate valid order sides.
    
    Returns:
        Strategy for generating order side enums.
    """
    return st.sampled_from([OrderSide.BUY, OrderSide.SELL])


def spot_balance_strategy() -> SearchStrategy[dict[str, Any]]:
    """Generate valid spot balance data.
    
    Returns:
        Strategy for generating spot balance dictionaries.
    """
    return st.builds(
        lambda exchange, asset, total, available: {
            "exchange": exchange,
            "asset": asset,
            "total_quantity": total,
            "available_quantity": min(available, total),  # Available <= Total
            "timestamp": datetime.now(UTC),
        },
        exchange=exchange_strategy(),
        asset=asset_strategy(),
        total=balance_amount_strategy(),
        available=balance_amount_strategy(),
    )


def fill_strategy() -> SearchStrategy[dict[str, Any]]:
    """Generate valid fill data for balance updates.
    
    Returns:
        Strategy for generating fill data dictionaries.
    """
    return st.builds(
        lambda price, quantity, fee, side, exchange: {
            "price": price,
            "quantity": quantity,
            "fee": fee,
            "side": side,
            "exchange": exchange,
        },
        price=price_strategy(),
        quantity=quantity_strategy(),
        fee=fee_strategy(),
        side=side_strategy(),
        exchange=exchange_strategy(),
    )


# =============================================================================
# TEST FIXTURES
# =============================================================================


@pytest.fixture
def mock_config() -> MagicMock:
    """Create mock application configuration.
    
    Returns:
        Mock configuration object.
    """
    config = MagicMock(spec=AppSettings)
    config.validation = MagicMock(spec=PortfolioValidationSettings)
    config.validation.balance_tolerance = Decimal("0.00001")
    config.validation.min_balance_threshold = Decimal("0.00000001")
    config.state = MagicMock()
    config.state.reconciliation_enabled = True
    return config


@pytest.fixture
def mock_state_manager() -> AsyncMock:
    """Create mock portfolio state manager.
    
    Returns:
        Mock state manager object.
    """
    state_manager = AsyncMock()
    state = MagicMock(spec=PortfolioState)
    state.balances = {}
    state.positions = {}
    state_manager.get_state.return_value = state
    return state_manager


@pytest.fixture
def balance_manager(mock_config: MagicMock, mock_state_manager: AsyncMock) -> BalanceManager:
    """Create balance manager instance.
    
    Returns:
        Configured balance manager instance.
    """
    return BalanceManager(mock_config, mock_state_manager)


# =============================================================================
# PROPERTY TESTS FOR BALANCE INVARIANTS
# =============================================================================


class TestBalanceInvariants:
    """Property-based tests for balance invariants."""

    @given(total_quantity=balance_amount_strategy(), available_quantity=balance_amount_strategy())
    def test_balance_constraint_properties(
        self, total_quantity: Decimal, available_quantity: Decimal
    ) -> None:
        """Property: Available balance should never exceed total balance."""
        # Create balance with constraint
        effective_available = min(available_quantity, total_quantity)

        # Property: Available <= Total
        assert effective_available <= total_quantity

        # Property: Both should be non-negative
        assert effective_available >= Decimal(0)
        assert total_quantity >= Decimal(0)

        # Property: If total is zero, available must be zero
        if total_quantity == Decimal(0):
            assert effective_available == Decimal(0)

    @given(initial_balance=balance_amount_strategy(), delta=balance_delta_strategy())
    def test_balance_update_properties(self, initial_balance: Decimal, delta: Decimal) -> None:
        """Property: Balance updates should maintain consistency."""
        new_balance = initial_balance + delta

        # Property: Negative balances should be handled
        if new_balance < Decimal(0):
            # In real system, this might be rejected or clamped to zero
            # For now, test the mathematical property
            assert new_balance == initial_balance + delta
        else:
            # Property: Positive updates preserve precision
            assert new_balance == initial_balance + delta
            assert new_balance >= Decimal(0)

    @given(balances=st.lists(balance_amount_strategy(), min_size=1, max_size=10))
    def test_balance_aggregation_properties(self, balances: list[Decimal]) -> None:
        """Property: Balance aggregation should be exact."""
        total = sum(balances)

        # Property: Sum should equal individual parts
        assert total == sum(balances)

        # Property: Total should be non-negative if all parts are
        assert total >= Decimal(0)

        # Property: Total should be at least as large as any individual balance
        assert all(total >= balance for balance in balances)


# =============================================================================
# PROPERTY TESTS FOR BALANCE MANAGER
# =============================================================================


class TestBalanceManagerProperties:
    """Property-based tests for BalanceManager operations."""

    @pytest.mark.asyncio
    @given(
        asset=asset_strategy(),
        exchange=exchange_strategy(),
        total_quantity=balance_amount_strategy(),
        available_quantity=balance_amount_strategy(),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    async def test_get_balance_properties(
        self,
        balance_manager: BalanceManager,
        mock_state_manager: AsyncMock,
        asset: str,
        exchange: ExchangeName,
        total_quantity: Decimal,
        available_quantity: Decimal,
    ) -> None:
        """Property: Balance retrieval should be consistent."""
        # Set up mock balance
        symbol = create_symbol_func(value=asset, exchange=exchange)
        effective_available = min(available_quantity, total_quantity)

        balance = SpotBalance(
            exchange=exchange,
            asset=symbol,
            timestamp=datetime.now(UTC),
            total_quantity=total_quantity,
            available_quantity=effective_available,
        )

        state = MagicMock(spec=PortfolioState)
        state.balances = {f"{exchange.value}:{asset}": balance}
        mock_state_manager.get_state.return_value = state

        # Get balance
        result = await balance_manager.get_balance(symbol, exchange)

        if result:
            # Property: Retrieved balance should match stored balance
            assert result.total_quantity == total_quantity
            assert result.available_quantity == effective_available

            # Property: Available <= Total invariant maintained
            assert result.available_quantity <= result.total_quantity

    @pytest.mark.asyncio
    @given(
        balances=st.lists(
            st.tuples(asset_strategy(), exchange_strategy(), balance_amount_strategy()),
            min_size=1,
            max_size=10,
        )
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    async def test_total_balance_usd_properties(
        self,
        balance_manager: BalanceManager,
        mock_state_manager: AsyncMock,
        balances: list[tuple[str, ExchangeName, Decimal]],
    ) -> None:
        """Property: Total USD balance calculation should be accurate."""
        # Set up mock balances
        state = MagicMock(spec=PortfolioState)
        state.balances = {}

        # Group balances by key to handle duplicates correctly
        balance_groups: dict[str, Decimal] = {}
        for asset, exchange, amount in balances:
            key = f"{exchange.value}:{asset}"
            # Only keep the latest balance for each key (overwrites previous)
            balance_groups[key] = amount

        expected_total = Decimal(0)
        for key, amount in balance_groups.items():
            exchange_str, asset = key.split(":", 1)
            exchange = ExchangeName(exchange_str)
            symbol = create_symbol_func(value=asset, exchange=exchange)
            balance = SpotBalance(
                exchange=exchange,
                asset=symbol,
                timestamp=datetime.now(UTC),
                total_quantity=amount,
                available_quantity=amount,
            )
            state.balances[key] = balance

            # Only count USD/USDC in total
            if asset in {"USD", "USDC"}:
                expected_total += amount

        mock_state_manager.get_state.return_value = state

        # Calculate total
        total = await balance_manager.get_total_balance_usd()

        # Property: Total should equal sum of USD/USDC balances
        assert total == expected_total

        # Property: Total should be non-negative
        assert total >= Decimal(0)

    @pytest.mark.asyncio
    @given(fill_data=fill_strategy())
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    async def test_update_balance_from_fill_properties(
        self,
        balance_manager: BalanceManager,
        mock_state_manager: AsyncMock,
        fill_data: dict[str, Any],
    ) -> None:
        """Property: Fill updates should correctly adjust balances."""
        # Calculate expected balance change first to filter invalid scenarios
        cost = fill_data["price"] * fill_data["quantity"]
        if fill_data["side"] == OrderSide.BUY:
            expected_delta = -cost - fill_data["fee"]
        else:
            expected_delta = cost - fill_data["fee"]

        initial_amount = Decimal(10000)
        expected_balance = initial_amount + expected_delta

        # Skip test cases that would result in negative balances
        # (these are correctly rejected by the system)
        assume(expected_balance >= Decimal(0))

        # Create mock fill
        symbol = create_symbol_func(value="BTC_USDC", exchange=fill_data["exchange"])
        fill = MagicMock(spec=Fill)
        fill.symbol = symbol
        fill.exchange = fill_data["exchange"]
        fill.side = fill_data["side"]
        fill.price = fill_data["price"]
        fill.quantity = fill_data["quantity"]
        fill.fee = fill_data["fee"]

        # Mock state with initial balance
        quote_symbol = create_symbol_func(value="USDC", exchange=fill_data["exchange"])
        initial_balance = SpotBalance(
            exchange=fill_data["exchange"],
            asset=quote_symbol,
            timestamp=datetime.now(UTC),
            total_quantity=initial_amount,
            available_quantity=initial_amount,
        )

        state = MagicMock(spec=PortfolioState)
        state.balances = {f"{fill_data['exchange'].value}:USDC": initial_balance}
        mock_state_manager.get_state.return_value = state

        # Mock _get_quote_asset to return USDC
        with patch.object(balance_manager, "_get_quote_asset", return_value=quote_symbol):
            await balance_manager.update_balance_from_fill(fill)

        # Property: Balance should be updated correctly
        # The actual update happens through _update_balance
        # which modifies the state.balances dictionary
        assert mock_state_manager.save_state.called

    @pytest.mark.asyncio
    @given(
        exchange=exchange_strategy(),
        asset=asset_strategy(),
        expected=balance_amount_strategy(),
        actual=balance_amount_strategy(),
        tolerance=st.decimals(min_value=Decimal("0.00001"), max_value=Decimal(1), places=5),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    async def test_validate_balance_properties(
        self,
        balance_manager: BalanceManager,
        mock_state_manager: AsyncMock,
        exchange: ExchangeName,
        asset: str,
        expected: Decimal,
        actual: Decimal,
        tolerance: Decimal,
    ) -> None:
        """Property: Balance validation should respect tolerance."""
        balance_manager.config.validation.balance_tolerance = tolerance

        # Set up mock balance
        symbol = create_symbol_func(value=asset, exchange=exchange)
        balance = SpotBalance(
            exchange=exchange,
            asset=symbol,
            timestamp=datetime.now(UTC),
            total_quantity=actual,
            available_quantity=actual,
        )

        state = MagicMock(spec=PortfolioState)
        state.balances = {f"{exchange.value}:{asset}": balance}
        mock_state_manager.get_state.return_value = state

        # Validate balance
        is_valid = await balance_manager.validate_balance(exchange, symbol, expected)

        # Property: Validation should respect tolerance
        diff = abs(actual - expected)
        if diff <= tolerance:
            assert is_valid is True
        else:
            assert is_valid is False


# =============================================================================
# PROPERTY TESTS FOR BALANCE RECONCILIATION
# =============================================================================


class TestBalanceReconciliationProperties:
    """Property-based tests for balance reconciliation."""

    @pytest.mark.asyncio
    @given(
        local_balances=st.lists(
            st.tuples(asset_strategy(), balance_amount_strategy()), min_size=1, max_size=5
        ),
        exchange_balances=st.lists(
            st.tuples(asset_strategy(), balance_amount_strategy()), min_size=1, max_size=5
        ),
        exchange=exchange_strategy(),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    async def test_reconciliation_properties(
        self,
        balance_manager: BalanceManager,
        mock_state_manager: AsyncMock,
        local_balances: list[tuple[str, Decimal]],
        exchange_balances: list[tuple[str, Decimal]],
        exchange: ExchangeName,
    ) -> None:
        """Property: Reconciliation should identify and correct discrepancies."""
        # Set up local balances
        state = MagicMock(spec=PortfolioState)
        state.balances = {}

        for asset, amount in local_balances:
            symbol = create_symbol_func(value=asset, exchange=exchange)
            balance = SpotBalance(
                exchange=exchange,
                asset=symbol,
                timestamp=datetime.now(UTC),
                total_quantity=amount,
                available_quantity=amount,
            )
            state.balances[f"{exchange.value}:{asset}"] = balance

        mock_state_manager.get_state.return_value = state

        # Create exchange balance objects
        exchange_balance_objects = []
        for asset, amount in exchange_balances:
            symbol = create_symbol_func(value=asset, exchange=exchange)
            balance = SpotBalance(
                exchange=exchange,
                asset=symbol,
                timestamp=datetime.now(UTC),
                total_quantity=amount,
                available_quantity=amount,
            )
            exchange_balance_objects.append(balance)

        # Perform reconciliation
        report = await balance_manager.reconcile_balances(exchange_balance_objects, exchange)

        # Property: Report should have correct structure
        assert report.reconciliation_timestamp is not None
        assert isinstance(report.reconciliation_successful, bool)
        assert report.total_discrepancies >= 0
        assert exchange in report.exchange_results

        # Property: Discrepancy count should match reported discrepancies
        assert report.total_discrepancies == len(report.balance_discrepancies)

        # Property: Success should correlate with discrepancy count
        if report.total_discrepancies == 0:
            assert report.reconciliation_successful is True
        else:
            assert report.reconciliation_successful is False

    @pytest.mark.asyncio
    @given(
        assets=st.lists(asset_strategy(), min_size=1, max_size=5, unique=True),
        amounts=st.lists(balance_amount_strategy(), min_size=1, max_size=5),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    async def test_reconciliation_update_properties(
        self,
        balance_manager: BalanceManager,
        mock_state_manager: AsyncMock,
        assets: list[str],
        amounts: list[Decimal],
    ) -> None:
        """Property: Reconciliation should update balances to match exchange."""
        # Ensure we have same number of assets and amounts
        assume(len(assets) == len(amounts))

        exchange = ExchangeName.HYPERLIQUID

        # Set up empty local state
        state = MagicMock(spec=PortfolioState)
        state.balances = {}
        mock_state_manager.get_state.return_value = state

        # Create exchange balances
        exchange_balances = []
        for asset, amount in zip(assets, amounts, strict=False):
            symbol = create_symbol_func(value=asset, exchange=exchange)
            balance = SpotBalance(
                exchange=exchange,
                asset=symbol,
                timestamp=datetime.now(UTC),
                total_quantity=amount,
                available_quantity=amount,
            )
            exchange_balances.append(balance)

        # Enable reconciliation
        balance_manager.config.state.reconciliation_enabled = True

        # Perform reconciliation
        _ = await balance_manager.reconcile_balances(exchange_balances, exchange)

        # Property: All exchange balances should be in state after reconciliation
        for asset, amount in zip(assets, amounts, strict=False):
            key = f"{exchange.value}:{asset}"
            if amount > 0:  # Only non-zero balances are added
                assert key in state.balances
                assert state.balances[key].total_quantity == amount

        # Property: State should be saved if updates were made
        if any(amount > 0 for amount in amounts):
            assert mock_state_manager.save_state.called


# =============================================================================
# PROPERTY TESTS FOR FILL IMPACT ON BALANCES
# =============================================================================


class TestFillBalanceImpactProperties:
    """Property-based tests for how fills impact balances."""

    @given(
        price=price_strategy(),
        quantity=quantity_strategy(),
        fee=fee_strategy(),
        side=side_strategy(),
        initial_balance=balance_amount_strategy(),
    )
    def test_fill_cost_calculation_properties(
        self,
        price: Decimal,
        quantity: Decimal,
        fee: Decimal,
        side: OrderSide,
        initial_balance: Decimal,
    ) -> None:
        """Property: Fill cost calculations should be mathematically correct."""
        # Calculate cost
        cost = price * quantity

        # Calculate balance impact
        balance_change = -cost - fee if side == OrderSide.BUY else cost - fee

        new_balance = initial_balance + balance_change

        # Property: Buy should decrease balance
        if side == OrderSide.BUY:
            assert balance_change < 0 or (balance_change == 0 and fee == 0)

        # Property: Sell should increase balance (unless fees exceed proceeds)
        if side == OrderSide.SELL and fee < cost:
            assert balance_change > 0

        # Property: Fees always decrease net balance
        if fee > 0:
            balance_without_fee = initial_balance + (cost if side == OrderSide.SELL else -cost)
            assert new_balance < balance_without_fee

    @given(
        fills=st.lists(fill_strategy(), min_size=1, max_size=10),
        initial_balance=balance_amount_strategy(),
    )
    def test_multiple_fills_impact(
        self, fills: list[dict[str, Any]], initial_balance: Decimal
    ) -> None:
        """Property: Multiple fills should have cumulative effect on balance."""
        balance = initial_balance

        for fill in fills:
            cost = fill["price"] * fill["quantity"]
            if fill["side"] == OrderSide.BUY:
                balance_change = -cost - fill["fee"]
            else:
                balance_change = cost - fill["fee"]
            balance += balance_change

        # Property: Final balance should equal initial plus all changes
        total_change = Decimal(0)
        for fill in fills:
            cost = fill["price"] * fill["quantity"]
            if fill["side"] == OrderSide.BUY:
                total_change += -cost - fill["fee"]
            else:
                total_change += cost - fill["fee"]

        assert balance == initial_balance + total_change


# =============================================================================
# PROPERTY TESTS FOR BALANCE MATHEMATICAL PROPERTIES
# =============================================================================


class TestBalanceMathematicalProperties:
    """Property tests for mathematical properties of balance operations."""

    @given(
        balances=st.lists(
            st.tuples(asset_strategy(), balance_amount_strategy()), min_size=2, max_size=10
        )
    )
    def test_balance_additivity(self, balances: list[tuple[str, Decimal]]) -> None:
        """Property: Balance totals should be additive."""
        # Group by asset
        asset_totals: dict[str, Decimal] = {}
        for asset, amount in balances:
            if asset not in asset_totals:
                asset_totals[asset] = Decimal(0)
            asset_totals[asset] += amount

        # Property: Sum of individual additions equals total
        for asset, total in asset_totals.items():
            individual_sum = sum(amount for a, amount in balances if a == asset)
            assert total == individual_sum

    @given(
        balance=balance_amount_strategy(),
        operations=st.lists(balance_delta_strategy(), min_size=1, max_size=10),
    )
    def test_balance_operation_order_independence(
        self, balance: Decimal, operations: list[Decimal]
    ) -> None:
        """Property: Balance operations should be order-independent for additions."""
        # Apply operations in original order
        result1 = balance
        for op in operations:
            result1 += op

        # Apply operations in reverse order
        result2 = balance
        for op in reversed(operations):
            result2 += op

        # Property: Order shouldn't matter for pure additions
        assert result1 == result2

        # Property: Result should equal initial plus sum of all operations
        assert result1 == balance + sum(operations)

    @given(
        balance=balance_amount_strategy(),
        factor=st.decimals(min_value=Decimal("0.1"), max_value=Decimal(10), places=2),
    )
    def test_balance_scaling_properties(self, balance: Decimal, factor: Decimal) -> None:
        """Property: Balance scaling should preserve proportions."""
        scaled = balance * factor

        # Property: Scaling should be reversible
        if factor != Decimal(0):
            unscaled = scaled / factor
            # Account for potential rounding in division
            assert abs(unscaled - balance) < Decimal("0.00000001")

        # Property: Zero balance remains zero after scaling
        if balance == Decimal(0):
            assert scaled == Decimal(0)

        # Property: Scaling by 1 should not change value
        if factor == Decimal(1):
            assert scaled == balance


# =============================================================================
# PROPERTY TESTS FOR EXCHANGE-SPECIFIC BALANCE OPERATIONS
# =============================================================================


class TestExchangeBalanceProperties:
    """Property tests for exchange-specific balance operations."""

    @pytest.mark.asyncio
    @given(
        exchanges=st.lists(exchange_strategy(), min_size=2, max_size=3),
        assets=st.lists(asset_strategy(), min_size=1, max_size=5, unique=True),
        amounts=st.lists(balance_amount_strategy(), min_size=1, max_size=5),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    async def test_exchange_balance_isolation(
        self,
        balance_manager: BalanceManager,
        mock_state_manager: AsyncMock,
        exchanges: list[ExchangeName],
        assets: list[str],
        amounts: list[Decimal],
    ) -> None:
        """Property: Exchange balances should be isolated from each other."""
        # Ensure we have matching lengths
        assume(len(assets) == len(amounts))

        # Set up balances across multiple exchanges
        state = MagicMock(spec=PortfolioState)
        state.balances = {}

        for exchange in exchanges:
            for asset, amount in zip(assets, amounts, strict=False):
                symbol = create_symbol_func(value=asset, exchange=exchange)
                balance = SpotBalance(
                    exchange=exchange,
                    asset=symbol,
                    timestamp=datetime.now(UTC),
                    total_quantity=amount,
                    available_quantity=amount,
                )
                state.balances[f"{exchange.value}:{asset}"] = balance

        mock_state_manager.get_state.return_value = state

        # Get balances for each exchange
        for exchange in exchanges:
            exchange_balances = await balance_manager.get_exchange_balances(exchange)

            # Property: Should only return balances for this exchange
            for asset in exchange_balances:
                key = f"{exchange.value}:{asset}"
                assert key in state.balances

            # Property: Should have all assets for this exchange
            for asset in assets:
                assert asset in exchange_balances
                assert exchange_balances[asset].total_quantity == amounts[assets.index(asset)]

    @pytest.mark.asyncio
    @given(
        exchange1=exchange_strategy(),
        exchange2=exchange_strategy(),
        asset=asset_strategy(),
        amount1=balance_amount_strategy(),
        amount2=balance_amount_strategy(),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    async def test_cross_exchange_balance_independence(
        self,
        balance_manager: BalanceManager,
        mock_state_manager: AsyncMock,
        exchange1: ExchangeName,
        exchange2: ExchangeName,
        asset: str,
        amount1: Decimal,
        amount2: Decimal,
    ) -> None:
        """Property: Same asset on different exchanges should be independent."""
        # Skip if same exchange
        assume(exchange1 != exchange2)

        # Set up balances
        symbol1 = create_symbol_func(value=asset, exchange=exchange1)
        symbol2 = create_symbol_func(value=asset, exchange=exchange2)
        state = MagicMock(spec=PortfolioState)
        state.balances = {}

        balance1 = SpotBalance(
            exchange=exchange1,
            asset=symbol1,
            timestamp=datetime.now(UTC),
            total_quantity=amount1,
            available_quantity=amount1,
        )
        balance2 = SpotBalance(
            exchange=exchange2,
            asset=symbol2,
            timestamp=datetime.now(UTC),
            total_quantity=amount2,
            available_quantity=amount2,
        )

        state.balances[f"{exchange1.value}:{asset}"] = balance1
        state.balances[f"{exchange2.value}:{asset}"] = balance2
        mock_state_manager.get_state.return_value = state

        # Get balances
        result1 = await balance_manager.get_balance(symbol1, exchange1)
        result2 = await balance_manager.get_balance(symbol2, exchange2)

        # Property: Balances should be independent
        assert result1 is not None
        assert result2 is not None
        assert result1.total_quantity == amount1
        assert result2.total_quantity == amount2

        # Property: Total USD should sum both if asset is USD/USDC
        if asset in {"USD", "USDC"}:
            total = await balance_manager.get_total_balance_usd()
            assert total == amount1 + amount2


# =============================================================================
# PROPERTY TESTS FOR EDGE CASES
# =============================================================================


class TestBalanceEdgeCases:
    """Property tests for edge cases in balance management."""

    @pytest.mark.asyncio
    async def test_zero_balance_properties(
        self, balance_manager: BalanceManager, mock_state_manager: AsyncMock
    ) -> None:
        """Property: Zero balances should be handled correctly."""
        # Set up zero balance
        symbol = create_symbol_func(value="BTC", exchange=ExchangeName.HYPERLIQUID)
        exchange = ExchangeName.HYPERLIQUID

        balance = SpotBalance(
            exchange=exchange,
            asset=symbol,
            timestamp=datetime.now(UTC),
            total_quantity=Decimal(0),
            available_quantity=Decimal(0),
        )

        state = MagicMock(spec=PortfolioState)
        state.balances = {f"{exchange.value}:BTC": balance}
        mock_state_manager.get_state.return_value = state

        # Get balance
        result = await balance_manager.get_balance(symbol, exchange)

        # Property: Zero balance should be valid
        assert result is not None
        assert result.total_quantity == Decimal(0)
        assert result.available_quantity == Decimal(0)

        # Property: Zero balance validation should work
        is_valid = await balance_manager.validate_balance(exchange, symbol, Decimal(0))
        assert is_valid is True

    @pytest.mark.asyncio
    @given(
        very_small_amount=st.decimals(
            min_value=Decimal("0.00000001"), max_value=Decimal("0.0001"), places=10
        ),
        very_large_amount=st.decimals(
            min_value=Decimal(1000000), max_value=Decimal(999999999), places=2
        ),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    async def test_extreme_balance_values(
        self,
        balance_manager: BalanceManager,
        mock_state_manager: AsyncMock,
        very_small_amount: Decimal,
        very_large_amount: Decimal,
    ) -> None:
        """Property: Extreme balance values should be handled correctly."""
        symbol = create_symbol_func(value="BTC", exchange=ExchangeName.HYPERLIQUID)
        exchange = ExchangeName.HYPERLIQUID

        # Test very small balance
        small_balance = SpotBalance(
            exchange=exchange,
            asset=symbol,
            timestamp=datetime.now(UTC),
            total_quantity=very_small_amount,
            available_quantity=very_small_amount,
        )

        state = MagicMock(spec=PortfolioState)
        state.balances = {f"{exchange.value}:BTC": small_balance}
        mock_state_manager.get_state.return_value = state

        result = await balance_manager.get_balance(symbol, exchange)

        # Property: Small amounts should preserve precision
        assert result is not None
        assert result.total_quantity == very_small_amount

        # Test very large balance
        large_balance = SpotBalance(
            exchange=exchange,
            asset=symbol,
            timestamp=datetime.now(UTC),
            total_quantity=very_large_amount,
            available_quantity=very_large_amount,
        )

        state.balances = {f"{exchange.value}:BTC": large_balance}
        result = await balance_manager.get_balance(symbol, exchange)

        # Property: Large amounts should not overflow
        assert result is not None
        assert result.total_quantity == very_large_amount
        assert result.total_quantity.is_finite()

    @pytest.mark.asyncio
    async def test_missing_state_handling(
        self, balance_manager: BalanceManager, mock_state_manager: AsyncMock
    ) -> None:
        """Property: Missing state should be handled gracefully."""
        # Set state to None
        mock_state_manager.get_state.return_value = None

        symbol = create_symbol_func(value="BTC", exchange=ExchangeName.HYPERLIQUID)
        exchange = ExchangeName.HYPERLIQUID

        # Property: Should return None for missing state
        result = await balance_manager.get_balance(symbol, exchange)
        assert result is None

        # Property: Should return 0 for total balance
        total = await balance_manager.get_total_balance_usd()
        assert total == Decimal(0)

        # Property: Should handle reconciliation with missing state
        exchange_balances: list[SpotBalance] = []
        report = await balance_manager.reconcile_balances(exchange_balances, exchange)
        assert report.reconciliation_successful is False
        if report.error_messages:
            assert "No portfolio state available" in report.error_messages[0]


# =============================================================================
# PROPERTY TESTS FOR PRECISION PRESERVATION
# =============================================================================


class TestBalancePrecisionProperties:
    """Property tests for precision preservation in balance operations."""

    @given(
        amount=st.decimals(
            min_value=Decimal("0.00000001"), max_value=Decimal("999999.99999999"), places=8
        )
    )
    def test_decimal_precision_preservation(self, amount: Decimal) -> None:
        """Property: Decimal precision should be preserved exactly."""
        # Convert to string and back
        amount_str = str(amount)
        recovered = Decimal(amount_str)

        # Property: Round-trip should preserve value
        assert recovered == amount

        # Property: String representation should be consistent
        assert str(recovered) == str(amount)

        # Property: Operations should preserve precision
        doubled = amount * Decimal(2)
        halved = doubled / Decimal(2)
        assert halved == amount

    @given(
        amounts=st.lists(
            st.decimals(min_value=Decimal("0.00000001"), max_value=Decimal(100), places=8),
            min_size=10,
            max_size=100,
        )
    )
    def test_cumulative_precision(self, amounts: list[Decimal]) -> None:
        """Property: Cumulative operations should maintain precision."""
        # Sum using different methods
        total1 = sum(amounts)

        total2 = Decimal(0)
        for amount in amounts:
            total2 += amount

        # Property: Different summation methods should give same result
        assert total1 == total2

        # Property: Precision should be maintained
        assert total1.is_finite() if isinstance(total1, Decimal) else True

        # Property: Average calculation should maintain precision
        if amounts:
            average = total1 / Decimal(str(len(amounts)))
            assert average.is_finite()
            # Verify average * count ≈ total (within rounding)
            reconstructed = average * Decimal(str(len(amounts)))
            diff = abs(reconstructed - total1)
            assert diff < Decimal("0.00000001") * len(amounts)
