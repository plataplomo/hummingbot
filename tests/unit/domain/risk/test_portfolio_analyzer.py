"""Property-based tests for domain.risk.portfolio_analyzer module.

This module tests the critical portfolio analysis utilities to ensure:
- Exposure calculations maintain mathematical precision
- Portfolio data extraction handles all edge cases correctly
- Decimal operations preserve financial precision exactly
- Business logic invariants are never violated
- All combinations of position states are handled safely

SECURITY CRITICAL: Portfolio analysis errors can lead to incorrect risk
assessments, wrong exposure calculations, or failure to detect dangerous
portfolio states that could result in catastrophic losses.
"""

import operator
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
from hypothesis import assume, given, strategies as st
from hypothesis.strategies import SearchStrategy

from cyberdelta.domain.portfolio.portfolio_service import PortfolioService
from cyberdelta.domain.risk.portfolio_analyzer import PortfolioAnalyzer
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models import DerivativePosition, TradeSignal
from cyberdelta.symbols.models import Symbol


# =============================================================================
# HYPOTHESIS STRATEGIES FOR PORTFOLIO ANALYSIS TESTING
# =============================================================================


def decimal_strategy(
    min_value: Decimal | None = None,
    max_value: Decimal | None = None,
    max_decimal_places: int = 8,
) -> SearchStrategy[Decimal]:
    """Generate Decimal values for financial testing.

    Args:
        min_value: Minimum decimal value (inclusive)
        max_value: Maximum decimal value (inclusive)
        max_decimal_places: Maximum number of decimal places

    Returns:
        Strategy generating valid Decimal values
    """
    min_val = min_value or Decimal(-1000000)
    max_val = max_value or Decimal(1000000)

    return st.decimals(
        min_value=min_val,
        max_value=max_val,
        places=max_decimal_places,
        allow_nan=False,
        allow_infinity=False,
    )


def positive_decimal_strategy() -> SearchStrategy[Decimal]:
    """Generate positive decimal values for financial calculations.
    
    Returns:
        Strategy generating positive Decimal values for testing
    """
    return st.decimals(
        min_value=Decimal("0.00000001"),
        max_value=Decimal(1000000),
        places=8,
        allow_nan=False,
        allow_infinity=False,
    )


def position_size_strategy() -> SearchStrategy[Decimal]:
    """Generate position sizes (can be positive, negative, or zero).
    
    Returns:
        Strategy generating position sizes including zero, positive, and negative values
    """
    return st.one_of([
        st.just(Decimal(0)),  # No position
        positive_decimal_strategy(),  # Long position
        positive_decimal_strategy().map(operator.neg),  # Short position
    ])


def price_strategy() -> SearchStrategy[Decimal]:
    """Generate valid market prices.
    
    Returns:
        Strategy generating valid positive price values
    """
    return st.decimals(
        min_value=Decimal("0.01"),
        max_value=Decimal(100000),
        places=8,
        allow_nan=False,
        allow_infinity=False,
    )


def create_mock_position(size: Decimal) -> DerivativePosition:
    """Create a mock DerivativePosition for testing.

    Args:
        size: Position size (positive for long, negative for short, zero for no position)

    Returns:
        Mock DerivativePosition object
    """
    position = MagicMock(spec=DerivativePosition)
    position.size = size
    return position


def create_mock_trade_signal(
    exchange: ExchangeName = ExchangeName.HYPERLIQUID,
    price: Decimal = Decimal(50000),
) -> TradeSignal:
    """Create a mock TradeSignal for testing.

    Args:
        exchange: Exchange for the signal
        price: Signal price

    Returns:
        Mock TradeSignal object
    """
    signal = MagicMock(spec=TradeSignal)
    signal.exchange = exchange
    signal.price = price

    # Create mock symbol
    mock_symbol = MagicMock(spec=Symbol)
    mock_symbol.value = "BTC"
    signal.symbol = mock_symbol

    return signal


def create_mock_portfolio_service() -> PortfolioService:
    """Create a mock PortfolioService for testing.

    Returns:
        Mock PortfolioService with async methods
    """
    service = MagicMock(spec=PortfolioService)
    service.get_position = AsyncMock()
    service.get_total_equity_usd = AsyncMock()
    return service


# =============================================================================
# PROPERTY TESTS FOR EXPOSURE CALCULATIONS
# =============================================================================


class TestPortfolioAnalyzerExposureProperties:
    """Property-based tests for exposure calculation logic."""

    @given(
        position_size=position_size_strategy(),
        signal_price=price_strategy(),
    )
    def test_exposure_calculation_mathematical_properties(
        self, position_size: Decimal, signal_price: Decimal
    ) -> None:
        """Property: Exposure calculations must follow mathematical invariants."""
        # Create analyzer and test data
        mock_service = create_mock_portfolio_service()
        analyzer = PortfolioAnalyzer(mock_service)

        position = create_mock_position(position_size) if position_size != 0 else None

        # Calculate exposure
        exposure = analyzer.calculate_exposure(position, signal_price)

        # Property: Exposure must always be non-negative (absolute value)
        assert exposure >= Decimal(0)

        # Property: Exposure must be finite
        assert exposure.is_finite()

        # Property: Exposure is zero if no position
        if position_size == Decimal(0) or position is None:
            assert exposure == Decimal(0)
        else:
            # Property: Exposure = |position_size| * price
            expected_exposure = abs(position_size) * signal_price
            assert exposure == expected_exposure

    @given(
        position_size=position_size_strategy(),
    )
    def test_exposure_calculation_zero_price_handling(self, position_size: Decimal) -> None:
        """Property: Zero or None price should result in zero exposure."""
        mock_service = create_mock_portfolio_service()
        analyzer = PortfolioAnalyzer(mock_service)

        position = create_mock_position(position_size) if position_size != 0 else None

        # Test with zero price
        exposure_zero = analyzer.calculate_exposure(position, Decimal(0))
        assert exposure_zero == Decimal(0)

        # Test with None price
        exposure_none = analyzer.calculate_exposure(position, None)
        assert exposure_none == Decimal(0)

    @given(
        signal_price=price_strategy(),
    )
    def test_exposure_calculation_no_position_handling(self, signal_price: Decimal) -> None:
        """Property: No position should result in zero exposure regardless of price."""
        mock_service = create_mock_portfolio_service()
        analyzer = PortfolioAnalyzer(mock_service)

        # Test with None position
        exposure_none = analyzer.calculate_exposure(None, signal_price)
        assert exposure_none == Decimal(0)

        # Test with zero-sized position
        zero_position = create_mock_position(Decimal(0))
        exposure_zero = analyzer.calculate_exposure(zero_position, signal_price)
        assert exposure_zero == Decimal(0)

    @given(
        position_size=st.decimals(
            min_value=Decimal("0.00000001"), max_value=Decimal(1000), places=8
        ),
        price1=price_strategy(),
        price2=price_strategy(),
    )
    def test_exposure_calculation_price_proportionality(
        self, position_size: Decimal, price1: Decimal, price2: Decimal
    ) -> None:
        """Property: Exposure should be proportional to price for the same position."""
        assume(price1 > 0 and price2 > 0)
        assume(price1 != price2)

        mock_service = create_mock_portfolio_service()
        analyzer = PortfolioAnalyzer(mock_service)

        position = create_mock_position(position_size)

        exposure1 = analyzer.calculate_exposure(position, price1)
        exposure2 = analyzer.calculate_exposure(position, price2)

        # Property: Ratio of exposures should equal ratio of prices
        if exposure1 > 0 and exposure2 > 0:
            price_ratio = price2 / price1
            exposure_ratio = exposure2 / exposure1

            # Allow small floating point differences
            assert abs(price_ratio - exposure_ratio) < Decimal("0.000001")

    @given(
        long_size=positive_decimal_strategy(),
        signal_price=price_strategy(),
    )
    def test_exposure_calculation_position_direction_invariance(
        self, long_size: Decimal, signal_price: Decimal
    ) -> None:
        """Property: Exposure should be the same for long and short positions of equal size."""
        mock_service = create_mock_portfolio_service()
        analyzer = PortfolioAnalyzer(mock_service)

        long_position = create_mock_position(long_size)
        short_position = create_mock_position(-long_size)

        long_exposure = analyzer.calculate_exposure(long_position, signal_price)
        short_exposure = analyzer.calculate_exposure(short_position, signal_price)

        # Property: Long and short positions of equal size have equal exposure
        assert long_exposure == short_exposure

        # Property: Both exposures are positive
        assert long_exposure >= Decimal(0)
        assert short_exposure >= Decimal(0)

    @given(
        position_size=position_size_strategy(),
        signal_price=price_strategy(),
    )
    def test_exposure_calculation_precision_preservation(
        self, position_size: Decimal, signal_price: Decimal
    ) -> None:
        """Property: Exposure calculations must preserve decimal precision."""
        mock_service = create_mock_portfolio_service()
        analyzer = PortfolioAnalyzer(mock_service)

        position = create_mock_position(position_size) if position_size != 0 else None

        exposure = analyzer.calculate_exposure(position, signal_price)

        # Property: Result must be a Decimal (not float)
        assert isinstance(exposure, Decimal)

        # Property: Result must be finite
        assert exposure.is_finite()

        # Property: If position exists, exposure should match manual calculation
        if position and signal_price and position_size != 0:
            manual_calculation = abs(position_size) * signal_price
            assert exposure == manual_calculation


# =============================================================================
# PROPERTY TESTS FOR PORTFOLIO DATA EXTRACTION
# =============================================================================


class TestPortfolioAnalyzerDataExtractionProperties:
    """Property-based tests for portfolio data extraction logic."""

    @pytest.mark.asyncio
    @given(
        exchange=st.sampled_from([ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]),
        signal_price=price_strategy(),
        position_size=position_size_strategy(),
        total_equity=positive_decimal_strategy(),
    )
    async def test_portfolio_data_extraction_consistency(
        self,
        exchange: ExchangeName,
        signal_price: Decimal,
        position_size: Decimal,
        total_equity: Decimal,
    ) -> None:
        """Property: Portfolio data extraction should be consistent and complete."""
        # Setup
        mock_service = create_mock_portfolio_service()
        analyzer = PortfolioAnalyzer(mock_service)

        signal = create_mock_trade_signal(exchange, signal_price)
        position = create_mock_position(position_size) if position_size != 0 else None

        # Mock service responses
        mock_service.get_position.return_value = position  # type: ignore[attr-defined]
        mock_service.get_total_equity_usd.return_value = total_equity  # type: ignore[attr-defined]

        # Execute
        (
            extracted_exchange,
            extracted_position,
            extracted_equity,
            extracted_exposure,
        ) = await analyzer.get_portfolio_data(signal)

        # Property: Exchange should match signal
        assert extracted_exchange == exchange

        # Property: Total equity should match service response
        assert extracted_equity == total_equity
        assert extracted_equity >= Decimal(0)

        # Property: Position should match service response
        assert extracted_position == position

        # Property: Exposure should be calculated correctly
        expected_exposure = analyzer.calculate_exposure(position, signal_price)
        assert extracted_exposure == expected_exposure

        # Property: All returned values must be proper types
        assert isinstance(extracted_exchange, ExchangeName)
        assert extracted_position is None or hasattr(extracted_position, "size")
        assert isinstance(extracted_equity, Decimal)
        assert isinstance(extracted_exposure, Decimal)

    @pytest.mark.asyncio
    @given(
        signal_price=price_strategy(),
        total_equity=positive_decimal_strategy(),
    )
    async def test_portfolio_data_extraction_list_exchange_handling(
        self, signal_price: Decimal, total_equity: Decimal
    ) -> None:
        """Property: Should handle signals with list exchanges correctly."""
        # Setup
        mock_service = create_mock_portfolio_service()
        analyzer = PortfolioAnalyzer(mock_service)

        # Create signal with list of exchanges (as can happen in some cases)
        signal = create_mock_trade_signal(ExchangeName.HYPERLIQUID, signal_price)
        signal.exchange = [
            ExchangeName.HYPERLIQUID,
            ExchangeName.BACKPACK,
        ]  # List instead of single value

        mock_service.get_position.return_value = None  # type: ignore[attr-defined]
        mock_service.get_total_equity_usd.return_value = total_equity  # type: ignore[attr-defined]

        # Execute
        (
            extracted_exchange,
            extracted_position,
            extracted_equity,
            extracted_exposure,
        ) = await analyzer.get_portfolio_data(signal)

        # Property: Should extract first exchange from list
        assert extracted_exchange == ExchangeName.HYPERLIQUID

        # Property: Other properties should still be valid
        assert extracted_equity == total_equity
        assert extracted_position is None
        assert extracted_exposure == Decimal(0)

    @pytest.mark.asyncio
    @given(
        exchange=st.sampled_from([ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]),
        signal_price=price_strategy(),
    )
    async def test_portfolio_data_extraction_service_integration(
        self, exchange: ExchangeName, signal_price: Decimal
    ) -> None:
        """Property: Should correctly integrate with portfolio service methods."""
        # Setup
        mock_service = create_mock_portfolio_service()
        analyzer = PortfolioAnalyzer(mock_service)

        signal = create_mock_trade_signal(exchange, signal_price)

        # Setup service to return None position and positive equity
        mock_service.get_position.return_value = None  # type: ignore[attr-defined]
        mock_service.get_total_equity_usd.return_value = Decimal(10000)  # type: ignore[attr-defined]

        # Execute
        await analyzer.get_portfolio_data(signal)

        # Property: Service methods should be called with correct parameters
        mock_service.get_position.assert_called_once_with(signal.symbol, exchange)  # type: ignore[attr-defined]
        mock_service.get_total_equity_usd.assert_called_once()  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    @given(
        position_size_values=st.lists(position_size_strategy(), min_size=2, max_size=5),
        signal_price=price_strategy(),
        total_equity=positive_decimal_strategy(),
    )
    async def test_portfolio_data_extraction_multiple_calls_consistency(
        self, position_size_values: list[Decimal], signal_price: Decimal, total_equity: Decimal
    ) -> None:
        """Property: Multiple calls with different positions should be consistent."""
        mock_service = create_mock_portfolio_service()
        analyzer = PortfolioAnalyzer(mock_service)

        signal = create_mock_trade_signal(ExchangeName.HYPERLIQUID, signal_price)
        mock_service.get_total_equity_usd.return_value = total_equity  # type: ignore[attr-defined]

        results: list[tuple[ExchangeName, DerivativePosition | None, Decimal, Decimal]] = []

        for position_size in position_size_values:
            position = create_mock_position(position_size) if position_size != 0 else None
            mock_service.get_position.return_value = position  # type: ignore[attr-defined]

            result = await analyzer.get_portfolio_data(signal)
            results.append(result)

        # Property: All calls should return consistent structure
        for result in results:
            assert len(result) == 4  # (exchange, position, equity, exposure)
            exchange, _, equity, exposure = result

            assert isinstance(exchange, ExchangeName)
            assert equity == total_equity  # Should be consistent across calls
            assert isinstance(exposure, Decimal)
            assert exposure >= Decimal(0)


# =============================================================================
# INTEGRATION PROPERTY TESTS
# =============================================================================


class TestPortfolioAnalyzerIntegrationProperties:
    """Integration property tests across portfolio analyzer functionality."""

    @given(
        position_sizes=st.lists(position_size_strategy(), min_size=1, max_size=10),
        prices=st.lists(price_strategy(), min_size=1, max_size=10),
    )
    def test_exposure_calculation_batch_consistency(
        self, position_sizes: list[Decimal], prices: list[Decimal]
    ) -> None:
        """Property: Batch exposure calculations should be mathematically consistent."""
        mock_service = create_mock_portfolio_service()
        analyzer = PortfolioAnalyzer(mock_service)

        total_exposure_sum = Decimal(0)
        individual_exposures: list[Decimal] = []

        # Calculate exposures individually
        for position_size in position_sizes:
            for price in prices:
                position = create_mock_position(position_size) if position_size != 0 else None
                exposure = analyzer.calculate_exposure(position, price)
                individual_exposures.append(exposure)
                total_exposure_sum += exposure

        # Property: All individual exposures should be non-negative
        for exposure in individual_exposures:
            assert exposure >= Decimal(0)

        # Property: Sum should equal manual calculation
        manual_sum = Decimal(0)
        for position_size in position_sizes:
            for price in prices:
                if position_size != 0:
                    manual_sum += abs(position_size) * price

        assert total_exposure_sum == manual_sum

    @given(
        position_size=position_size_strategy(),
        price_multipliers=st.lists(positive_decimal_strategy(), min_size=2, max_size=5),
        base_price=price_strategy(),
    )
    def test_exposure_calculation_price_scaling_properties(
        self, position_size: Decimal, price_multipliers: list[Decimal], base_price: Decimal
    ) -> None:
        """Property: Exposure should scale linearly with price."""
        mock_service = create_mock_portfolio_service()
        analyzer = PortfolioAnalyzer(mock_service)

        position = create_mock_position(position_size) if position_size != 0 else None
        base_exposure = analyzer.calculate_exposure(position, base_price)

        for multiplier in price_multipliers:
            scaled_price = base_price * multiplier
            scaled_exposure = analyzer.calculate_exposure(position, scaled_price)

            # Property: Exposure should scale by the same factor as price
            if base_exposure > 0:
                exposure_ratio = scaled_exposure / base_exposure
                # Allow for small rounding differences
                assert abs(exposure_ratio - multiplier) < Decimal("0.000001")
            else:
                # If base exposure is zero, scaled exposure should also be zero
                assert scaled_exposure == Decimal(0)

    @pytest.mark.asyncio
    @given(
        exchanges=st.lists(
            st.sampled_from([ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]),
            min_size=1,
            max_size=3,
        ),
        signal_price=price_strategy(),
        total_equity=positive_decimal_strategy(),
    )
    async def test_portfolio_data_cross_exchange_consistency(
        self, exchanges: list[ExchangeName], signal_price: Decimal, total_equity: Decimal
    ) -> None:
        """Property: Portfolio data should be consistent across different exchanges."""
        mock_service = create_mock_portfolio_service()
        analyzer = PortfolioAnalyzer(mock_service)

        mock_service.get_total_equity_usd.return_value = total_equity  # type: ignore[attr-defined]

        results: list[tuple[ExchangeName, DerivativePosition | None, Decimal, Decimal]] = []

        for exchange in exchanges:
            signal = create_mock_trade_signal(exchange, signal_price)
            mock_service.get_position.return_value = None  # type: ignore[attr-defined]  # No position for simplicity

            result = await analyzer.get_portfolio_data(signal)
            results.append(result)

        # Property: Total equity should be consistent across exchanges
        for result in results:
            _, _, equity, _ = result
            assert equity == total_equity

        # Property: Exposure should be zero for all (no positions)
        for result in results:
            _, _, _, exposure = result
            assert exposure == Decimal(0)

    @given(
        test_scenarios=st.lists(
            st.tuples(position_size_strategy(), price_strategy()), min_size=5, max_size=20
        )
    )
    def test_exposure_calculation_comprehensive_scenarios(
        self, test_scenarios: list[tuple[Decimal, Decimal]]
    ) -> None:
        """Property: Exposure calculations should handle comprehensive test scenarios correctly."""
        mock_service = create_mock_portfolio_service()
        analyzer = PortfolioAnalyzer(mock_service)

        for position_size, price in test_scenarios:
            position = create_mock_position(position_size) if position_size != 0 else None
            exposure = analyzer.calculate_exposure(position, price)

            # Property: All fundamental invariants must hold
            assert isinstance(exposure, Decimal)
            assert exposure.is_finite()
            assert exposure >= Decimal(0)

            # Property: Mathematical correctness
            if position_size == 0 or position is None or price == 0:
                assert exposure == Decimal(0)
            else:
                expected = abs(position_size) * price
                assert exposure == expected
                assert exposure > Decimal(0)  # Should be positive for valid position and price
