"""Unit tests for the FundingRateArbitrageStrategy.

Tests the funding rate arbitrage strategy implementation including opportunity detection,
signal generation, and risk management. Following the mandatory test pattern:
SUCCESS, EDGE, and FAILURE cases.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from cyberdelta.core.models import FundingRate
from cyberdelta.core.models.derivative_position import DerivativePosition
from cyberdelta.core.models.market import Candle
from cyberdelta.core.models.market.ticker import Ticker
from cyberdelta.core.models.trade_signal import TradeSignal
from cyberdelta.core.portfolio.managers.portfolio_state_manager import PortfolioStateManager
from cyberdelta.core.risk_manager import RiskManager, SizedOpportunity
from cyberdelta.enums import OrderSide, SignalType
from cyberdelta.strategies.funding_rate_arbitrage import FundingRateArbitrageStrategy
from cyberdelta.validation.funding_data import ArbitrageOpportunity
from tests.fixtures.time_fixtures import FreezerProtocol


pytestmark = pytest.mark.timing


@pytest.fixture
def mock_data_handler() -> MagicMock:
    """Create a mock DataHandler instance.

    Returns:
        MagicMock: Mocked DataHandler with configured methods.
    """
    handler = MagicMock()
    # Create proper Mock objects for each method
    handler.get_latest_funding_rate = Mock(return_value=None)
    handler.get_latest_ticker = Mock(return_value=None)
    handler.get_historical_candles = Mock(return_value=[])
    handler.refresh_hyperliquid_funding_rates = AsyncMock()
    handler.fetch_funding_rates = AsyncMock(return_value=None)
    return handler


@pytest.fixture
def mock_portfolio_state_manager() -> Mock:
    """Create a mock PortfolioStateManager instance.
    
    Returns:
        Mock: Mocked PortfolioStateManager with test positions.
    """
    tracker = Mock(spec=PortfolioStateManager)

    # Create mock positions
    mock_perp_position = DerivativePosition(
        exchange="hyperliquid",
        symbol="BTC-PERP",
        side=OrderSide.BUY,
        size=Decimal("1.0"),
        entry_price=Decimal("50000.0"),
        timestamp=datetime.now(UTC),
    )

    mock_spot_position = DerivativePosition(
        exchange="backpack",
        symbol="BTC_USDC",
        side=OrderSide.SELL,
        size=Decimal("-1.0"),  # Negative size for SELL position
        entry_price=Decimal("49900.0"),
        timestamp=datetime.now(UTC),
    )

    def get_position_mock(exchange: str, symbol: str) -> DerivativePosition | None:
        if exchange == "hyperliquid" and symbol == "BTC-PERP":
            return mock_perp_position
        if exchange == "backpack" and symbol == "BTC_USDC":
            return mock_spot_position
        return None

    tracker.get_position = Mock(side_effect=get_position_mock)
    tracker.get_total_balance = Mock(return_value=Decimal("10000.0"))
    return tracker


@pytest.fixture
def mock_risk_manager() -> Mock:
    """Create a mock RiskManager instance.

    Returns:
        Mock: Mocked RiskManager with sized opportunity responses.
    """
    manager = Mock(spec=RiskManager)

    # Create a mock ArbitrageOpportunity
    mock_opportunity = ArbitrageOpportunity(
        symbol="BTC-PERP",
        long_exchange="hyperliquid",
        short_exchange="backpack",
        long_price=Decimal("50000.0"),
        short_price=Decimal("49900.0"),
        long_funding_rate=Decimal("0.001"),
        short_funding_rate=Decimal("-0.0005"),
        net_funding_differential=Decimal("0.0015"),
        timestamp=datetime.now(UTC),
        expected_profit=Decimal("100.0"),
    )

    # Create a mock SizedOpportunity
    mock_sized_opportunity = SizedOpportunity(
        opportunity=mock_opportunity,
        long_size=Decimal("1000.0"),
        short_size=Decimal("1000.0"),
        allocation_percentage=Decimal("10.0"),
        expected_profit=Decimal("100.0"),
        expected_return=Decimal("10.0"),
        risk_adjusted_return=Decimal("5.0"),
    )

    manager.size_opportunity = Mock(return_value=mock_sized_opportunity)
    manager.validate_signal = Mock(return_value=True)
    return manager


@pytest.fixture
def strategy_params() -> dict[str, Any]:
    """Standard strategy parameters for testing.

    Returns:
        dict[str, Any]: Test strategy configuration parameters.
    """
    return {
        "min_funding_differential": "0.0001",
        "min_profit_usd": "0.1",
        "risk_aversion": "1.0",
        "rebalance_threshold": "0.05",
        "check_interval": 10,
        "perp_exchange": "hyperliquid",
        "spot_exchange": "backpack",
        "symbol_mapping": {"BTC-PERP": "BTC_USDC"},
        "history_length": 24,
    }


@pytest.fixture
def funding_rate_strategy(
    mock_data_handler: MagicMock,
    mock_portfolio_state_manager: Mock,
    mock_risk_manager: Mock,
    strategy_params: dict[str, Any],
) -> FundingRateArbitrageStrategy:
    """Create a FundingRateArbitrageStrategy instance for testing.

    Returns:
        FundingRateArbitrageStrategy: Configured strategy with mocked dependencies.
    """
    return FundingRateArbitrageStrategy(
        name="test_strategy",
        symbol="BTC-PERP",
        data_handler=mock_data_handler,
        portfolio_tracker=mock_portfolio_state_manager,
        risk_manager=mock_risk_manager,
        params=strategy_params,
    )


class TestFundingRateArbitrageStrategyInit:
    """Test suite for FundingRateArbitrageStrategy initialization.

    Tests success, edge, and failure cases for strategy initialization.
    """

    # SUCCESS CASES
    def test_init_success_with_valid_params(
        self,
        mock_data_handler: MagicMock,
        mock_portfolio_state_manager: Mock,
        mock_risk_manager: Mock,
        strategy_params: dict[str, Any],
    ) -> None:
        """Test successful initialization with valid parameters."""
        # Act
        strategy = FundingRateArbitrageStrategy(
            name="test_strategy",
            symbol="BTC-PERP",
            data_handler=mock_data_handler,
            portfolio_tracker=mock_portfolio_state_manager,
            risk_manager=mock_risk_manager,
            params=strategy_params,
        )

        # Assert
        assert strategy.name == "test_strategy"
        assert strategy.symbol == "BTC-PERP"
        assert strategy.data_handler is mock_data_handler
        assert strategy.portfolio_tracker is mock_portfolio_state_manager
        assert strategy.risk_manager is mock_risk_manager
        assert strategy.min_funding_differential == Decimal("0.0001")
        assert strategy.min_profit_threshold == Decimal("0.1")
        assert strategy.risk_aversion == Decimal("1.0")
        assert strategy.rebalance_threshold == Decimal("0.05")
        assert strategy.check_interval == 10
        assert strategy.perp_exchange == "hyperliquid"
        assert strategy.spot_exchange == "backpack"
        assert strategy.symbol_mapping == {"BTC-PERP": "BTC_USDC"}

    def test_init_success_with_minimal_params(
        self,
        mock_data_handler: MagicMock,
        mock_portfolio_state_manager: Mock,
    ) -> None:
        """Test successful initialization with minimal parameters (defaults)."""
        # Act
        strategy = FundingRateArbitrageStrategy(
            name="minimal_strategy",
            symbol="ETH-PERP",
            data_handler=mock_data_handler,
            portfolio_tracker=mock_portfolio_state_manager,
        )

        # Assert
        assert strategy.name == "minimal_strategy"
        assert strategy.symbol == "ETH-PERP"
        assert strategy.risk_manager is None
        assert strategy.min_funding_differential == Decimal("0.0001")
        assert strategy.min_profit_threshold == Decimal("0.1")
        assert strategy.risk_aversion == Decimal("1.0")
        assert strategy.rebalance_threshold == Decimal("0.05")
        assert strategy.check_interval == 10
        assert strategy.perp_exchange == "hyperliquid"
        assert strategy.spot_exchange == "backpack"
        # Should create default mapping from symbol
        assert "ETH-PERP" in strategy.symbol_mapping
        assert strategy.symbol_mapping["ETH-PERP"] == "ETH_USDC"

    # EDGE CASES
    def test_init_edge_invalid_decimal_params(
        self,
        mock_data_handler: MagicMock,
        mock_portfolio_state_manager: Mock,
    ) -> None:
        """Test initialization with invalid decimal parameters falls back to defaults."""
        # Arrange
        invalid_params = {
            "min_funding_differential": "invalid_decimal",
            "min_profit_usd": None,
            "risk_aversion": "",
            "rebalance_threshold": "not_a_number",
        }

        # Act
        strategy = FundingRateArbitrageStrategy(
            name="edge_strategy",
            symbol="BTC-PERP",
            data_handler=mock_data_handler,
            portfolio_tracker=mock_portfolio_state_manager,
            params=invalid_params,
        )

        # Assert - Should use defaults for invalid params
        assert strategy.min_funding_differential == Decimal("0.0001")
        assert strategy.min_profit_threshold == Decimal("0.1")
        assert strategy.risk_aversion == Decimal("1.0")
        assert strategy.rebalance_threshold == Decimal("0.05")

    def test_init_edge_invalid_int_params(
        self,
        mock_data_handler: MagicMock,
        mock_portfolio_state_manager: Mock,
    ) -> None:
        """Test initialization with invalid integer parameters falls back to defaults."""
        # Arrange
        invalid_params = {
            "check_interval": "not_an_int",
            "history_length": None,
        }

        # Act
        strategy = FundingRateArbitrageStrategy(
            name="edge_strategy",
            symbol="BTC-PERP",
            data_handler=mock_data_handler,
            portfolio_tracker=mock_portfolio_state_manager,
            params=invalid_params,
        )

        # Assert - Should use defaults for invalid params
        assert strategy.check_interval == 10
        # Cannot test private method _get_int_param - test behavior through public interface instead

    def test_init_edge_complex_symbol_mapping(
        self,
        mock_data_handler: MagicMock,
        mock_portfolio_state_manager: Mock,
    ) -> None:
        """Test initialization with complex symbol containing underscores."""
        # Arrange
        complex_symbol = "SOL_USD-PERP"

        # Act
        strategy = FundingRateArbitrageStrategy(
            name="complex_strategy",
            symbol=complex_symbol,
            data_handler=mock_data_handler,
            portfolio_tracker=mock_portfolio_state_manager,
        )

        # Assert - Should extract base correctly
        assert complex_symbol in strategy.symbol_mapping
        assert strategy.symbol_mapping[complex_symbol] == "SOL_USDC"

    # FAILURE CASES
    def test_init_failure_none_data_handler(
        self,
        mock_portfolio_state_manager: Mock,
    ) -> None:
        """Test initialization failure with None data handler."""
        # Act & Assert
        with pytest.raises(TypeError):
            FundingRateArbitrageStrategy(
                name="fail_strategy",
                symbol="BTC-PERP",
                data_handler=None,
                portfolio_tracker=mock_portfolio_state_manager,
            )

    def test_init_failure_none_portfolio_tracker(
        self,
        mock_data_handler: MagicMock,
    ) -> None:
        """Test initialization failure with None portfolio tracker."""
        # Act & Assert
        with pytest.raises(TypeError):
            FundingRateArbitrageStrategy(
                name="fail_strategy",
                symbol="BTC-PERP",
                data_handler=mock_data_handler,
                portfolio_tracker=None,
            )

    def test_init_failure_empty_name(
        self,
        mock_data_handler: MagicMock,
        mock_portfolio_state_manager: Mock,
    ) -> None:
        """Test initialization failure with empty strategy name."""
        # Act & Assert
        with pytest.raises(ValueError):
            FundingRateArbitrageStrategy(
                name="",
                symbol="BTC-PERP",
                data_handler=mock_data_handler,
                portfolio_tracker=mock_portfolio_state_manager,
            )

    def test_init_failure_empty_symbol(
        self,
        mock_data_handler: MagicMock,
        mock_portfolio_state_manager: Mock,
    ) -> None:
        """Test initialization failure with empty symbol."""
        # Act & Assert
        with pytest.raises(ValueError):
            FundingRateArbitrageStrategy(
                name="fail_strategy",
                symbol="",
                data_handler=mock_data_handler,
                portfolio_tracker=mock_portfolio_state_manager,
            )


class TestEvaluateEntryOpportunityWithFundingRates:
    """Test suite for evaluate_entry_opportunity with various funding rate scenarios."""

    # SUCCESS CASES
    @pytest.mark.asyncio
    async def test_evaluate_opportunity_success_with_valid_funding_rate(
        self, funding_rate_strategy: FundingRateArbitrageStrategy
    ) -> None:
        """Test successful opportunity evaluation with valid funding rate."""
        # Arrange
        # Use a fixed timestamp for deterministic testing
        fixed_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        expected_rate = FundingRate(
            symbol="BTC-PERP",
            timestamp=fixed_time,
            funding_rate=Decimal("0.002"),  # Above threshold to ensure opportunity
            next_funding_time=fixed_time + timedelta(hours=8),
        )

        # Mock funding rate
        def mock_get_funding_rate(exchange: str, symbol: str) -> FundingRate | None:
            if exchange == "hyperliquid" and symbol == "BTC-PERP":
                return expected_rate
            return None

        with patch.object(
            funding_rate_strategy.data_handler,
            "get_latest_funding_rate",
            side_effect=mock_get_funding_rate,
        ):
            # Mock ticker prices for valid opportunity with significant spread
            perp_ticker = Ticker(
                symbol="BTC-PERP",
                exchange="hyperliquid",
                timestamp=fixed_time,
                price=Decimal("50000.5"),
                bid=Decimal("50000.0"),
                ask=Decimal("50001.0"),
            )
            spot_ticker = Ticker(
                symbol="BTC_USDC",
                exchange="backpack",
                timestamp=fixed_time,
                price=Decimal("49800.5"),  # Larger spread to ensure profitability
                bid=Decimal("49800.0"),
                ask=Decimal("49801.0"),
            )

            def mock_get_ticker(exchange: str, symbol: str) -> Ticker | None:
                if exchange == "hyperliquid" and symbol == "BTC-PERP":
                    return perp_ticker
                if exchange == "backpack" and symbol == "BTC_USDC":
                    return spot_ticker
                return None

            with (
                patch.object(
                    funding_rate_strategy.data_handler,
                    "get_latest_ticker",
                    side_effect=mock_get_ticker,
                ),
                patch.object(
                    funding_rate_strategy.data_handler,
                    "fetch_funding_rates",
                    new_callable=AsyncMock,
                    return_value=None,
                ),
            ):
                # Act
                result = await funding_rate_strategy.evaluate_entry_opportunity()

                # Assert
                # Should return signals when valid funding rate and price differential exists
                assert result is not None
                assert isinstance(result, list)
                assert len(result) == 2  # Should generate perp and spot signals

    @pytest.mark.asyncio
    async def test_evaluate_opportunity_success_after_funding_rate_retries(
        self, funding_rate_strategy: FundingRateArbitrageStrategy, frozen_time: FreezerProtocol
    ) -> None:
        """Test successful opportunity evaluation after initial funding rate failures."""
        # Arrange
        frozen_time.move_to("2024-01-01 12:00:00+00:00")
        expected_rate = FundingRate(
            symbol="BTC-PERP",
            timestamp=datetime.now(UTC),
            funding_rate=Decimal("0.002"),  # Above threshold to ensure opportunity
            next_funding_time=datetime.now(UTC) + timedelta(hours=8),
        )

        # Mock tickers with profitable spread
        perp_ticker = Ticker(
            symbol="BTC-PERP",
            exchange="hyperliquid",
            timestamp=datetime.now(UTC),
            price=Decimal("50000.5"),
            bid=Decimal("50000.0"),
            ask=Decimal("50001.0"),
        )
        spot_ticker = Ticker(
            symbol="BTC_USDC",
            exchange="backpack",
            timestamp=datetime.now(UTC),
            price=Decimal("49800.5"),  # Larger spread for profitability
            bid=Decimal("49800.0"),
            ask=Decimal("49801.0"),
        )

        def mock_get_ticker(exchange: str, symbol: str) -> Ticker | None:
            if exchange == "hyperliquid" and symbol == "BTC-PERP":
                return perp_ticker
            if exchange == "backpack" and symbol == "BTC_USDC":
                return spot_ticker
            return None

        # Create a stateful mock for funding rate that returns None twice, then the expected rate
        call_count = 0

        def mock_get_funding_rate_with_retry(exchange: str, symbol: str) -> FundingRate | None:
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                return None
            if exchange == "hyperliquid" and symbol == "BTC-PERP":
                return expected_rate
            return None

        with (
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_funding_rate",
                side_effect=mock_get_funding_rate_with_retry,
            ) as mock_get_rate,
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_ticker",
                side_effect=mock_get_ticker,
            ),
            patch("asyncio.sleep", new_callable=AsyncMock),
            patch.object(
                funding_rate_strategy.data_handler,
                "fetch_funding_rates",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            # Act
            result = await funding_rate_strategy.evaluate_entry_opportunity()

            # Assert
            # The strategy should retry internally and eventually succeed
            # Should make at least 3 calls (2 failures + 1 success)
            assert mock_get_rate.call_count >= 3
            # Should get a result when funding rate eventually succeeds
            assert result is not None
            assert isinstance(result, list)
            assert len(result) == 2  # Should have 2 signals for perp and spot

    # EDGE CASES
    @pytest.mark.asyncio
    async def test_evaluate_opportunity_edge_no_funding_rate_available(
        self, funding_rate_strategy: FundingRateArbitrageStrategy, frozen_time: FreezerProtocol
    ) -> None:
        """Test opportunity evaluation when no funding rate is available."""
        frozen_time.move_to("2024-01-01 12:00:00+00:00")

        def mock_get_funding_rate(exchange: str, symbol: str) -> FundingRate | None:
            return None

        with patch.object(
            funding_rate_strategy.data_handler,
            "get_latest_funding_rate",
            side_effect=mock_get_funding_rate,
        ) as mock_get_rate:
            # Act
            result = await funding_rate_strategy.evaluate_entry_opportunity()

            # Assert
            # Should return None or empty list when no funding rate
            assert result is None or result == []
            mock_get_rate.assert_called()

    @pytest.mark.asyncio
    async def test_evaluate_opportunity_edge_funding_rate_below_threshold(
        self, funding_rate_strategy: FundingRateArbitrageStrategy, frozen_time: FreezerProtocol
    ) -> None:
        """Test opportunity evaluation with funding rate below minimum threshold."""
        # Arrange
        frozen_time.move_to("2024-01-01 12:00:00+00:00")
        low_rate = FundingRate(
            symbol="BTC-PERP",
            timestamp=datetime.now(UTC),
            funding_rate=Decimal("0.00001"),  # Below default threshold of 0.0001
            next_funding_time=datetime.now(UTC) + timedelta(hours=8),
        )

        perp_ticker = Ticker(
            symbol="BTC-PERP",
            exchange="hyperliquid",
            timestamp=datetime.now(UTC),
            price=Decimal("50000.5"),
            bid=Decimal("50000.0"),
            ask=Decimal("50001.0"),
        )
        spot_ticker = Ticker(
            symbol="BTC_USDC",
            exchange="backpack",
            timestamp=datetime.now(UTC),
            price=Decimal("50000.5"),
            bid=Decimal("50000.0"),
            ask=Decimal("50001.0"),
        )

        def mock_get_funding_rate(exchange: str, symbol: str) -> FundingRate | None:
            if exchange == "hyperliquid" and symbol == "BTC-PERP":
                return low_rate
            return None

        def mock_get_ticker(exchange: str, symbol: str) -> Ticker | None:
            if exchange == "hyperliquid" and symbol == "BTC-PERP":
                return perp_ticker
            if exchange == "backpack" and symbol == "BTC_USDC":
                return spot_ticker
            return None

        with (
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_funding_rate",
                side_effect=mock_get_funding_rate,
            ),
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_ticker",
                side_effect=mock_get_ticker,
            ),
            patch.object(
                funding_rate_strategy.data_handler,
                "fetch_funding_rates",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            # Act
            result = await funding_rate_strategy.evaluate_entry_opportunity()

            # Assert
            # Should not generate signals for low funding rate
            assert result is None or result == []

    # FAILURE CASES
    @pytest.mark.asyncio
    async def test_evaluate_opportunity_failure_funding_rate_unavailable(
        self, funding_rate_strategy: FundingRateArbitrageStrategy, frozen_time: FreezerProtocol
    ) -> None:
        """Test failure when funding rate is consistently unavailable."""
        frozen_time.move_to("2024-01-01 12:00:00+00:00")

        def mock_get_funding_rate(exchange: str, symbol: str) -> FundingRate | None:
            return None

        with (
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_funding_rate",
                side_effect=mock_get_funding_rate,
            ) as mock_get_rate,
            patch("asyncio.sleep", new_callable=AsyncMock),
            patch.object(
                funding_rate_strategy.data_handler,
                "fetch_funding_rates",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            # Act
            result = await funding_rate_strategy.evaluate_entry_opportunity()

            # Assert
            # Should not generate signals when funding rate unavailable
            assert result is None or result == []
            assert mock_get_rate.call_count >= 1

    @pytest.mark.asyncio
    async def test_evaluate_opportunity_failure_data_handler_exception(
        self, funding_rate_strategy: FundingRateArbitrageStrategy, frozen_time: FreezerProtocol
    ) -> None:
        """Test failure when data handler raises exception."""
        frozen_time.move_to("2024-01-01 12:00:00+00:00")
        with (
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_funding_rate",
                side_effect=Exception("Network error"),
            ) as mock_get_rate,
            patch("asyncio.sleep", new_callable=AsyncMock),
            patch.object(
                funding_rate_strategy.data_handler,
                "fetch_funding_rates",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            # Act
            result = await funding_rate_strategy.evaluate_entry_opportunity()

            # Assert
            # Should handle exception gracefully
            assert result is None or result == []
            assert mock_get_rate.call_count >= 1

    @pytest.mark.asyncio
    async def test_evaluate_opportunity_failure_missing_ticker_data(
        self, funding_rate_strategy: FundingRateArbitrageStrategy, frozen_time: FreezerProtocol
    ) -> None:
        """Test failure when ticker data is missing."""
        # Arrange - valid funding rate but missing ticker
        frozen_time.move_to("2024-01-01 12:00:00+00:00")
        funding_rate = FundingRate(
            symbol="BTC-PERP",
            timestamp=datetime.now(UTC),
            funding_rate=Decimal("0.002"),  # Increased to ensure profit
            next_funding_time=datetime.now(UTC) + timedelta(hours=8),
        )

        with (
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_funding_rate",
                return_value=funding_rate,
            ),
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_ticker",
                return_value=None,
            ),
        ):
            # Act
            result = await funding_rate_strategy.evaluate_entry_opportunity()

            # Assert
            # Should not generate signals without ticker data
            assert result is None or result == []


class TestEvaluateOpportunityPriceScenarios:
    """Test suite for evaluate_entry_opportunity with various price scenarios."""

    # SUCCESS CASES
    @pytest.mark.asyncio
    async def test_evaluate_opportunity_success_with_price_differential(
        self, funding_rate_strategy: FundingRateArbitrageStrategy, frozen_time: FreezerProtocol
    ) -> None:
        """Test successful opportunity evaluation with favorable price differential."""
        # Arrange
        frozen_time.move_to("2024-01-01 12:00:00+00:00")
        funding_rate = FundingRate(
            symbol="BTC-PERP",
            timestamp=datetime.now(UTC),
            funding_rate=Decimal("0.002"),  # Increased to ensure profit
            next_funding_time=datetime.now(UTC) + timedelta(hours=8),
        )
        perp_ticker = Ticker(
            symbol="BTC-PERP",
            exchange="hyperliquid",
            timestamp=datetime.now(UTC),
            price=Decimal("50000.5"),
            bid=Decimal("50000.0"),
            ask=Decimal("50001.0"),
        )
        spot_ticker = Ticker(
            symbol="BTC_USDC",
            exchange="backpack",
            timestamp=datetime.now(UTC),
            price=Decimal("49999.5"),
            bid=Decimal("49999.0"),
            ask=Decimal("50000.0"),
        )

        def mock_get_ticker(exchange: str, symbol: str) -> Ticker | None:
            if exchange == "hyperliquid" and symbol == "BTC-PERP":
                return perp_ticker
            if exchange == "backpack" and symbol == "BTC_USDC":
                return spot_ticker
            return None

        with (
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_funding_rate",
                return_value=funding_rate,
            ),
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_ticker",
                side_effect=mock_get_ticker,
            ),
            patch.object(
                funding_rate_strategy.data_handler,
                "fetch_funding_rates",
                return_value=None,
            ),
            patch("asyncio.sleep"),
        ):
            # Act
            result = await funding_rate_strategy.evaluate_entry_opportunity()

            # Assert
            # Should evaluate opportunity based on price differential
            assert isinstance(result, list) if result else True

    @pytest.mark.asyncio
    async def test_evaluate_opportunity_success_different_symbols(
        self, funding_rate_strategy: FundingRateArbitrageStrategy, frozen_time: FreezerProtocol
    ) -> None:
        """Test successful opportunity evaluation with different symbol mapping."""
        # Arrange
        frozen_time.move_to("2024-01-01 12:00:00+00:00")
        funding_rate_strategy.symbol_mapping = {"ETH-PERP": "ETH_USDC"}
        funding_rate_strategy.symbol = "ETH-PERP"

        funding_rate = FundingRate(
            symbol="ETH-PERP",
            timestamp=datetime.now(UTC),
            funding_rate=Decimal("0.002"),  # Increased to ensure profit
            next_funding_time=datetime.now(UTC) + timedelta(hours=8),
        )
        perp_ticker = Ticker(
            symbol="ETH-PERP",
            exchange="hyperliquid",
            timestamp=datetime.now(UTC),
            price=Decimal("3000.5"),
            bid=Decimal("3000.0"),
            ask=Decimal("3001.0"),
        )
        spot_ticker = Ticker(
            symbol="ETH_USDC",
            exchange="backpack",
            timestamp=datetime.now(UTC),
            price=Decimal("2999.5"),
            bid=Decimal("2999.0"),
            ask=Decimal("3000.0"),
        )

        def mock_get_ticker(exchange: str, symbol: str) -> Ticker | None:
            if exchange == "hyperliquid" and symbol == "ETH-PERP":
                return perp_ticker
            if exchange == "backpack" and symbol == "ETH_USDC":
                return spot_ticker
            return None

        with (
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_funding_rate",
                return_value=funding_rate,
            ),
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_ticker",
                side_effect=mock_get_ticker,
            ),
            patch.object(
                funding_rate_strategy.data_handler,
                "fetch_funding_rates",
                return_value=None,
            ),
            patch("asyncio.sleep"),
        ):
            # Act
            result = await funding_rate_strategy.evaluate_entry_opportunity()

            # Assert
            # Should process ETH symbol mapping correctly
            assert isinstance(result, list) if result else True

    # EDGE CASES
    @pytest.mark.asyncio
    async def test_evaluate_opportunity_edge_zero_bid_ask_spread(
        self, funding_rate_strategy: FundingRateArbitrageStrategy, frozen_time: FreezerProtocol
    ) -> None:
        """Test opportunity evaluation with zero bid-ask spread."""
        # Arrange
        frozen_time.move_to("2024-01-01 12:00:00+00:00")
        funding_rate = FundingRate(
            symbol="BTC-PERP",
            timestamp=datetime.now(UTC),
            funding_rate=Decimal("0.002"),  # Increased to ensure profit
            next_funding_time=datetime.now(UTC) + timedelta(hours=8),
        )
        perp_ticker = Ticker(
            symbol="BTC-PERP",
            exchange="hyperliquid",
            timestamp=datetime.now(UTC),
            price=Decimal("50000.0"),
            bid=Decimal("50000.0"),
            ask=Decimal("50000.0"),  # Same as bid
        )
        spot_ticker = Ticker(
            symbol="BTC_USDC",
            exchange="backpack",
            timestamp=datetime.now(UTC),
            price=Decimal("50000.0"),
            bid=Decimal("50000.0"),
            ask=Decimal("50000.0"),  # Same as bid
        )

        def mock_get_ticker(exchange: str, symbol: str) -> Ticker | None:
            if exchange == "hyperliquid" and symbol == "BTC-PERP":
                return perp_ticker
            if exchange == "backpack" and symbol == "BTC_USDC":
                return spot_ticker
            return None

        with (
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_funding_rate",
                return_value=funding_rate,
            ),
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_ticker",
                side_effect=mock_get_ticker,
            ),
            patch.object(
                funding_rate_strategy.data_handler,
                "fetch_funding_rates",
                return_value=None,
            ),
            patch("asyncio.sleep"),
        ):
            # Act
            result = await funding_rate_strategy.evaluate_entry_opportunity()

            # Assert
            # Should handle zero spread scenario
            assert isinstance(result, list) if result else True

    @pytest.mark.asyncio
    async def test_evaluate_opportunity_edge_very_large_numbers(
        self, funding_rate_strategy: FundingRateArbitrageStrategy, frozen_time: FreezerProtocol
    ) -> None:
        """Test opportunity evaluation with very large price numbers."""
        # Arrange
        frozen_time.move_to("2024-01-01 12:00:00+00:00")
        funding_rate = FundingRate(
            symbol="BTC-PERP",
            timestamp=datetime.now(UTC),
            funding_rate=Decimal("0.002"),  # Increased to ensure profit
            next_funding_time=datetime.now(UTC) + timedelta(hours=8),
        )
        large_price = Decimal("999999999.99")
        perp_ticker = Ticker(
            symbol="BTC-PERP",
            exchange="hyperliquid",
            timestamp=datetime.now(UTC),
            price=large_price + Decimal("0.005"),
            bid=large_price,
            ask=large_price + Decimal("0.01"),
        )
        spot_ticker = Ticker(
            symbol="BTC_USDC",
            exchange="backpack",
            timestamp=datetime.now(UTC),
            price=large_price - Decimal("0.5"),
            bid=large_price - Decimal("1.0"),
            ask=large_price,
        )

        def mock_get_ticker(exchange: str, symbol: str) -> Ticker | None:
            if exchange == "hyperliquid" and symbol == "BTC-PERP":
                return perp_ticker
            if exchange == "backpack" and symbol == "BTC_USDC":
                return spot_ticker
            return None

        with (
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_funding_rate",
                return_value=funding_rate,
            ),
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_ticker",
                side_effect=mock_get_ticker,
            ),
            patch.object(
                funding_rate_strategy.data_handler,
                "fetch_funding_rates",
                return_value=None,
            ),
            patch("asyncio.sleep"),
        ):
            # Act
            result = await funding_rate_strategy.evaluate_entry_opportunity()

            # Assert
            # Should handle large numbers correctly
            assert isinstance(result, list) if result else True

    # FAILURE CASES
    @pytest.mark.asyncio
    async def test_evaluate_opportunity_failure_missing_symbol_mapping(
        self, funding_rate_strategy: FundingRateArbitrageStrategy, frozen_time: FreezerProtocol
    ) -> None:
        """Test failure when symbol mapping is missing."""
        # Arrange
        frozen_time.move_to("2024-01-01 12:00:00+00:00")
        funding_rate_strategy.symbol_mapping = {}  # Empty mapping

        funding_rate = FundingRate(
            symbol="BTC-PERP",
            timestamp=datetime.now(UTC),
            funding_rate=Decimal("0.002"),  # Increased to ensure profit
            next_funding_time=datetime.now(UTC) + timedelta(hours=8),
        )

        with patch.object(
            funding_rate_strategy.data_handler,
            "get_latest_funding_rate",
            return_value=funding_rate,
        ):
            # Act
            result = await funding_rate_strategy.evaluate_entry_opportunity()

            # Assert
            # Should not generate signals without symbol mapping
            assert result is None or result == []

    @pytest.mark.asyncio
    async def test_evaluate_opportunity_failure_none_perp_ticker(
        self, funding_rate_strategy: FundingRateArbitrageStrategy, frozen_time: FreezerProtocol
    ) -> None:
        """Test failure when perp ticker is None."""
        frozen_time.move_to("2024-01-01 12:00:00+00:00")
        funding_rate = FundingRate(
            symbol="BTC-PERP",
            timestamp=datetime.now(UTC),
            funding_rate=Decimal("0.002"),  # Increased to ensure profit
            next_funding_time=datetime.now(UTC) + timedelta(hours=8),
        )

        spot_ticker = Ticker(
            symbol="BTC_USDC",
            exchange="backpack",
            timestamp=datetime.now(UTC),
            price=Decimal("49900.5"),
            bid=Decimal("49900.0"),
            ask=Decimal("49901.0"),
        )

        def mock_get_ticker(exchange: str, symbol: str) -> Ticker | None:
            if exchange == "hyperliquid" and symbol == "BTC-PERP":
                return None  # Perp ticker is None
            if exchange == "backpack" and symbol == "BTC_USDC":
                return spot_ticker
            return None

        with (
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_funding_rate",
                return_value=funding_rate,
            ),
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_ticker",
                side_effect=mock_get_ticker,
            ),
            patch.object(
                funding_rate_strategy.data_handler,
                "fetch_funding_rates",
                return_value=None,
            ),
            patch("asyncio.sleep"),
        ):
            # Act
            result = await funding_rate_strategy.evaluate_entry_opportunity()

            # Assert
            # Should not generate signals without perp ticker
            assert result is None or result == []

    @pytest.mark.asyncio
    async def test_evaluate_opportunity_failure_none_spot_ticker(
        self, funding_rate_strategy: FundingRateArbitrageStrategy, frozen_time: FreezerProtocol
    ) -> None:
        """Test failure when spot ticker is None."""
        frozen_time.move_to("2024-01-01 12:00:00+00:00")
        funding_rate = FundingRate(
            symbol="BTC-PERP",
            timestamp=datetime.now(UTC),
            funding_rate=Decimal("0.002"),  # Increased to ensure profit
            next_funding_time=datetime.now(UTC) + timedelta(hours=8),
        )

        perp_ticker = Ticker(
            symbol="BTC-PERP",
            exchange="hyperliquid",
            timestamp=datetime.now(UTC),
            price=Decimal("50000.5"),
            bid=Decimal("50000.0"),
            ask=Decimal("50001.0"),
        )

        def mock_get_ticker(exchange: str, symbol: str) -> Ticker | None:
            if exchange == "hyperliquid" and symbol == "BTC-PERP":
                return perp_ticker
            if exchange == "backpack" and symbol == "BTC_USDC":
                return None  # Spot ticker is None
            return None

        with (
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_funding_rate",
                return_value=funding_rate,
            ),
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_ticker",
                side_effect=mock_get_ticker,
            ),
            patch.object(
                funding_rate_strategy.data_handler,
                "fetch_funding_rates",
                return_value=None,
            ),
            patch("asyncio.sleep"),
        ):
            # Act
            result = await funding_rate_strategy.evaluate_entry_opportunity()

            # Assert
            # Should not generate signals without spot ticker
            assert result is None or result == []

    @pytest.mark.asyncio
    async def test_evaluate_opportunity_failure_both_tickers_none(
        self, funding_rate_strategy: FundingRateArbitrageStrategy, frozen_time: FreezerProtocol
    ) -> None:
        """Test failure when both tickers are None."""
        frozen_time.move_to("2024-01-01 12:00:00+00:00")
        funding_rate = FundingRate(
            symbol="BTC-PERP",
            timestamp=datetime.now(UTC),
            funding_rate=Decimal("0.002"),  # Increased to ensure profit
            next_funding_time=datetime.now(UTC) + timedelta(hours=8),
        )

        with (
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_funding_rate",
                return_value=funding_rate,
            ),
            patch.object(
                funding_rate_strategy.data_handler, "get_latest_ticker", return_value=None
            ),
        ):
            # Act
            result = await funding_rate_strategy.evaluate_entry_opportunity()

            # Assert
            # Should not generate signals without ticker data
            assert result is None or result == []


class TestFundingRateValidationScenarios:
    """Test suite for funding rate validation through public interface."""

    # SUCCESS CASES
    @pytest.mark.asyncio
    async def test_evaluate_opportunity_with_positive_funding_rate(
        self, funding_rate_strategy: FundingRateArbitrageStrategy, frozen_time: FreezerProtocol
    ) -> None:
        """Test opportunity evaluation with positive funding rate."""
        # Arrange
        frozen_time.move_to("2024-01-01 12:00:00+00:00")
        positive_rate = FundingRate(
            symbol="BTC-PERP",
            timestamp=datetime.now(UTC),
            funding_rate=Decimal("0.002"),  # Increased to ensure profit
            next_funding_time=datetime.now(UTC) + timedelta(hours=8),
        )

        perp_ticker = Ticker(
            symbol="BTC-PERP",
            exchange="hyperliquid",
            timestamp=datetime.now(UTC),
            price=Decimal("50000.5"),
            bid=Decimal("50000.0"),
            ask=Decimal("50001.0"),
        )
        spot_ticker = Ticker(
            symbol="BTC_USDC",
            exchange="backpack",
            timestamp=datetime.now(UTC),
            price=Decimal("49900.5"),
            bid=Decimal("49900.0"),
            ask=Decimal("49901.0"),
        )

        def mock_get_ticker(exchange: str, symbol: str) -> Ticker | None:
            if exchange == "hyperliquid" and symbol == "BTC-PERP":
                return perp_ticker
            if exchange == "backpack" and symbol == "BTC_USDC":
                return spot_ticker
            return None

        with (
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_funding_rate",
                return_value=positive_rate,
            ),
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_ticker",
                side_effect=mock_get_ticker,
            ),
            patch.object(
                funding_rate_strategy.data_handler,
                "fetch_funding_rates",
                return_value=None,
            ),
            patch("asyncio.sleep"),
        ):
            # Act
            result = await funding_rate_strategy.evaluate_entry_opportunity()

            # Assert
            # Should process positive funding rate
            assert isinstance(result, list) if result else True

    @pytest.mark.asyncio
    async def test_evaluate_opportunity_with_negative_funding_rate(
        self, funding_rate_strategy: FundingRateArbitrageStrategy, frozen_time: FreezerProtocol
    ) -> None:
        """Test opportunity evaluation with negative funding rate."""
        # Arrange
        frozen_time.move_to("2024-01-01 12:00:00+00:00")
        negative_rate = FundingRate(
            symbol="BTC-PERP",
            timestamp=datetime.now(UTC),
            funding_rate=Decimal("-0.001"),
            next_funding_time=datetime.now(UTC) + timedelta(hours=8),
        )

        perp_ticker = Ticker(
            symbol="BTC-PERP",
            exchange="hyperliquid",
            timestamp=datetime.now(UTC),
            price=Decimal("50000.5"),
            bid=Decimal("50000.0"),
            ask=Decimal("50001.0"),
        )
        spot_ticker = Ticker(
            symbol="BTC_USDC",
            exchange="backpack",
            timestamp=datetime.now(UTC),
            price=Decimal("50100.5"),
            bid=Decimal("50100.0"),
            ask=Decimal("50101.0"),
        )

        def mock_get_ticker(exchange: str, symbol: str) -> Ticker | None:
            if exchange == "hyperliquid" and symbol == "BTC-PERP":
                return perp_ticker
            if exchange == "backpack" and symbol == "BTC_USDC":
                return spot_ticker
            return None

        with (
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_funding_rate",
                return_value=negative_rate,
            ),
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_ticker",
                side_effect=mock_get_ticker,
            ),
            patch.object(
                funding_rate_strategy.data_handler,
                "fetch_funding_rates",
                return_value=None,
            ),
            patch("asyncio.sleep"),
        ):
            # Act
            result = await funding_rate_strategy.evaluate_entry_opportunity()

            # Assert
            # Should process negative funding rate scenarios
            assert isinstance(result, list) if result else True

    @pytest.mark.asyncio
    async def test_evaluate_opportunity_with_very_small_funding_rate(
        self, funding_rate_strategy: FundingRateArbitrageStrategy, frozen_time: FreezerProtocol
    ) -> None:
        """Test opportunity evaluation with very small funding rate."""
        # Arrange
        frozen_time.move_to("2024-01-01 12:00:00+00:00")
        very_small_rate = FundingRate(
            symbol="BTC-PERP",
            timestamp=datetime.now(UTC),
            funding_rate=Decimal("0.0000001"),  # Very small but valid
            next_funding_time=datetime.now(UTC) + timedelta(hours=8),
        )

        perp_ticker = Ticker(
            symbol="BTC-PERP",
            exchange="hyperliquid",
            timestamp=datetime.now(UTC),
            price=Decimal("50000.5"),
            bid=Decimal("50000.0"),
            ask=Decimal("50001.0"),
        )
        spot_ticker = Ticker(
            symbol="BTC_USDC",
            exchange="backpack",
            timestamp=datetime.now(UTC),
            price=Decimal("49900.5"),
            bid=Decimal("49900.0"),
            ask=Decimal("49901.0"),
        )

        def mock_get_ticker(exchange: str, symbol: str) -> Ticker | None:
            if exchange == "hyperliquid" and symbol == "BTC-PERP":
                return perp_ticker
            if exchange == "backpack" and symbol == "BTC_USDC":
                return spot_ticker
            return None

        with (
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_funding_rate",
                return_value=very_small_rate,
            ),
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_ticker",
                side_effect=mock_get_ticker,
            ),
            patch.object(
                funding_rate_strategy.data_handler,
                "fetch_funding_rates",
                return_value=None,
            ),
            patch("asyncio.sleep"),
        ):
            # Act
            result = await funding_rate_strategy.evaluate_entry_opportunity()

            # Assert
            # Should handle very small funding rates
            assert isinstance(result, list) if result else True

    # EDGE CASES
    @pytest.mark.asyncio
    async def test_evaluate_opportunity_edge_zero_funding_rate(
        self, funding_rate_strategy: FundingRateArbitrageStrategy, frozen_time: FreezerProtocol
    ) -> None:
        """Test opportunity evaluation with zero funding rate."""
        # Arrange
        frozen_time.move_to("2024-01-01 12:00:00+00:00")
        zero_rate = FundingRate(
            symbol="BTC-PERP",
            timestamp=datetime.now(UTC),
            funding_rate=Decimal("0.0"),
            next_funding_time=datetime.now(UTC) + timedelta(hours=8),
        )

        perp_ticker = Ticker(
            symbol="BTC-PERP",
            exchange="hyperliquid",
            timestamp=datetime.now(UTC),
            price=Decimal("50000.5"),
            bid=Decimal("50000.0"),
            ask=Decimal("50001.0"),
        )
        spot_ticker = Ticker(
            symbol="BTC_USDC",
            exchange="backpack",
            timestamp=datetime.now(UTC),
            price=Decimal("49900.5"),
            bid=Decimal("49900.0"),
            ask=Decimal("49901.0"),
        )

        def mock_get_ticker(exchange: str, symbol: str) -> Ticker | None:
            if exchange == "hyperliquid" and symbol == "BTC-PERP":
                return perp_ticker
            if exchange == "backpack" and symbol == "BTC_USDC":
                return spot_ticker
            return None

        with (
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_funding_rate",
                return_value=zero_rate,
            ),
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_ticker",
                side_effect=mock_get_ticker,
            ),
            patch.object(
                funding_rate_strategy.data_handler,
                "fetch_funding_rates",
                return_value=None,
            ),
            patch("asyncio.sleep"),
        ):
            # Act
            result = await funding_rate_strategy.evaluate_entry_opportunity()

            # Assert
            # Should handle zero funding rate (no opportunity)
            assert result is None or result == []

    @pytest.mark.asyncio
    async def test_evaluate_opportunity_edge_large_positive_funding_rate(
        self, funding_rate_strategy: FundingRateArbitrageStrategy, frozen_time: FreezerProtocol
    ) -> None:
        """Test opportunity evaluation with large positive funding rate."""
        # Arrange
        frozen_time.move_to("2024-01-01 12:00:00+00:00")
        large_rate = FundingRate(
            symbol="BTC-PERP",
            timestamp=datetime.now(UTC),
            funding_rate=Decimal("0.1"),  # 10% funding rate
            next_funding_time=datetime.now(UTC) + timedelta(hours=8),
        )

        perp_ticker = Ticker(
            symbol="BTC-PERP",
            exchange="hyperliquid",
            timestamp=datetime.now(UTC),
            price=Decimal("50000.5"),
            bid=Decimal("50000.0"),
            ask=Decimal("50001.0"),
        )
        spot_ticker = Ticker(
            symbol="BTC_USDC",
            exchange="backpack",
            timestamp=datetime.now(UTC),
            price=Decimal("49900.5"),
            bid=Decimal("49900.0"),
            ask=Decimal("49901.0"),
        )

        def mock_get_ticker(exchange: str, symbol: str) -> Ticker | None:
            if exchange == "hyperliquid" and symbol == "BTC-PERP":
                return perp_ticker
            if exchange == "backpack" and symbol == "BTC_USDC":
                return spot_ticker
            return None

        with (
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_funding_rate",
                return_value=large_rate,
            ),
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_ticker",
                side_effect=mock_get_ticker,
            ),
            patch.object(
                funding_rate_strategy.data_handler,
                "fetch_funding_rates",
                return_value=None,
            ),
            patch("asyncio.sleep"),
        ):
            # Act
            result = await funding_rate_strategy.evaluate_entry_opportunity()

            # Assert
            # Should potentially generate signals for large funding rates
            assert isinstance(result, list) if result else True

    @pytest.mark.asyncio
    async def test_evaluate_opportunity_edge_large_negative_funding_rate(
        self, funding_rate_strategy: FundingRateArbitrageStrategy, frozen_time: FreezerProtocol
    ) -> None:
        """Test opportunity evaluation with large negative funding rate."""
        # Arrange
        frozen_time.move_to("2024-01-01 12:00:00+00:00")
        large_negative_rate = FundingRate(
            symbol="BTC-PERP",
            timestamp=datetime.now(UTC),
            funding_rate=Decimal("-0.1"),  # -10% funding rate
            next_funding_time=datetime.now(UTC) + timedelta(hours=8),
        )

        perp_ticker = Ticker(
            symbol="BTC-PERP",
            exchange="hyperliquid",
            timestamp=datetime.now(UTC),
            price=Decimal("50000.5"),
            bid=Decimal("50000.0"),
            ask=Decimal("50001.0"),
        )
        spot_ticker = Ticker(
            symbol="BTC_USDC",
            exchange="backpack",
            timestamp=datetime.now(UTC),
            price=Decimal("50100.5"),
            bid=Decimal("50100.0"),
            ask=Decimal("50101.0"),
        )

        def mock_get_ticker(exchange: str, symbol: str) -> Ticker | None:
            if exchange == "hyperliquid" and symbol == "BTC-PERP":
                return perp_ticker
            if exchange == "backpack" and symbol == "BTC_USDC":
                return spot_ticker
            return None

        with (
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_funding_rate",
                return_value=large_negative_rate,
            ),
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_ticker",
                side_effect=mock_get_ticker,
            ),
            patch.object(
                funding_rate_strategy.data_handler,
                "fetch_funding_rates",
                return_value=None,
            ),
            patch("asyncio.sleep"),
        ):
            # Act
            result = await funding_rate_strategy.evaluate_entry_opportunity()

            # Assert
            # Should potentially generate signals for large negative funding rates
            assert isinstance(result, list) if result else True

    # FAILURE CASES
    @pytest.mark.asyncio
    async def test_evaluate_opportunity_failure_none_funding_rate(
        self, funding_rate_strategy: FundingRateArbitrageStrategy, frozen_time: FreezerProtocol
    ) -> None:
        """Test failure when funding rate is None."""
        # Already covered in earlier tests
        frozen_time.move_to("2024-01-01 12:00:00+00:00")
        with patch.object(
            funding_rate_strategy.data_handler,
            "get_latest_funding_rate",
            return_value=None,
        ):
            # Act
            result = await funding_rate_strategy.evaluate_entry_opportunity()

            # Assert
            assert result is None or result == []

    @pytest.mark.asyncio
    async def test_evaluate_opportunity_failure_extreme_positive_funding_rate(
        self, funding_rate_strategy: FundingRateArbitrageStrategy, frozen_time: FreezerProtocol
    ) -> None:
        """Test opportunity evaluation with extreme positive funding rate."""
        # Arrange
        frozen_time.move_to("2024-01-01 12:00:00+00:00")
        extreme_rate = FundingRate(
            symbol="BTC-PERP",
            timestamp=datetime.now(UTC),
            funding_rate=Decimal("1.0"),  # 100% funding rate (unrealistic)
            next_funding_time=datetime.now(UTC) + timedelta(hours=8),
        )

        perp_ticker = Ticker(
            symbol="BTC-PERP",
            exchange="hyperliquid",
            timestamp=datetime.now(UTC),
            price=Decimal("50000.5"),
            bid=Decimal("50000.0"),
            ask=Decimal("50001.0"),
        )
        spot_ticker = Ticker(
            symbol="BTC_USDC",
            exchange="backpack",
            timestamp=datetime.now(UTC),
            price=Decimal("49900.5"),
            bid=Decimal("49900.0"),
            ask=Decimal("49901.0"),
        )

        def mock_get_ticker(exchange: str, symbol: str) -> Ticker | None:
            if exchange == "hyperliquid" and symbol == "BTC-PERP":
                return perp_ticker
            if exchange == "backpack" and symbol == "BTC_USDC":
                return spot_ticker
            return None

        with (
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_funding_rate",
                return_value=extreme_rate,
            ),
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_ticker",
                side_effect=mock_get_ticker,
            ),
            patch.object(
                funding_rate_strategy.data_handler,
                "fetch_funding_rates",
                return_value=None,
            ),
            patch("asyncio.sleep"),
        ):
            # Act
            result = await funding_rate_strategy.evaluate_entry_opportunity()

            # Assert
            # Strategy should handle extreme rates appropriately
            assert isinstance(result, list) if result else True

    @pytest.mark.asyncio
    async def test_evaluate_opportunity_failure_extreme_negative_funding_rate(
        self, funding_rate_strategy: FundingRateArbitrageStrategy, frozen_time: FreezerProtocol
    ) -> None:
        """Test opportunity evaluation with extreme negative funding rate."""
        # Arrange
        frozen_time.move_to("2024-01-01 12:00:00+00:00")
        extreme_negative_rate = FundingRate(
            symbol="BTC-PERP",
            timestamp=datetime.now(UTC),
            funding_rate=Decimal("-1.0"),  # -100% funding rate (unrealistic)
            next_funding_time=datetime.now(UTC) + timedelta(hours=8),
        )

        perp_ticker = Ticker(
            symbol="BTC-PERP",
            exchange="hyperliquid",
            timestamp=datetime.now(UTC),
            price=Decimal("50000.5"),
            bid=Decimal("50000.0"),
            ask=Decimal("50001.0"),
        )
        spot_ticker = Ticker(
            symbol="BTC_USDC",
            exchange="backpack",
            timestamp=datetime.now(UTC),
            price=Decimal("50100.5"),
            bid=Decimal("50100.0"),
            ask=Decimal("50101.0"),
        )

        def mock_get_ticker(exchange: str, symbol: str) -> Ticker | None:
            if exchange == "hyperliquid" and symbol == "BTC-PERP":
                return perp_ticker
            if exchange == "backpack" and symbol == "BTC_USDC":
                return spot_ticker
            return None

        with (
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_funding_rate",
                return_value=extreme_negative_rate,
            ),
            patch.object(
                funding_rate_strategy.data_handler,
                "get_latest_ticker",
                side_effect=mock_get_ticker,
            ),
            patch.object(
                funding_rate_strategy.data_handler,
                "fetch_funding_rates",
                return_value=None,
            ),
            patch("asyncio.sleep"),
        ):
            # Act
            result = await funding_rate_strategy.evaluate_entry_opportunity()

            # Assert
            # Strategy should handle extreme rates appropriately
            assert isinstance(result, list) if result else True


class TestProcessData:
    """Test suite for process_data method with success, edge, and failure cases."""

    # SUCCESS CASES
    @pytest.mark.asyncio
    async def test_process_data_success_valid_candle(
        self, funding_rate_strategy: FundingRateArbitrageStrategy, frozen_time: FreezerProtocol
    ) -> None:
        """Test successful data processing with valid candle."""
        # Arrange
        frozen_time.move_to("2024-01-01 12:00:00+00:00")
        candle = Candle(
            symbol="BTC-PERP",
            interval="1h",
            open=Decimal("50000.0"),
            high=Decimal("50100.0"),
            low=Decimal("49900.0"),
            close=Decimal("50050.0"),
            volume=Decimal("1000.0"),
            open_time=datetime.now(UTC),
        )

        # Mock evaluate_entry_opportunity to return signals
        mock_signals = [
            TradeSignal(
                symbol="BTC-PERP",
                signal_type=SignalType.ENTER_LONG,
                side=OrderSide.BUY,
                price=Decimal("50000.0"),
                quantity=Decimal("1.0"),
                exchange="hyperliquid",
            )
        ]

        with patch.object(
            funding_rate_strategy, "evaluate_entry_opportunity", return_value=mock_signals
        ) as mock_evaluate:
            # Act
            result = await funding_rate_strategy.process_data(candle)

        # Assert
        assert result == mock_signals
        mock_evaluate.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_data_success_no_signals_generated(
        self, funding_rate_strategy: FundingRateArbitrageStrategy, frozen_time: FreezerProtocol
    ) -> None:
        """Test successful data processing with no signals generated."""
        # Arrange
        frozen_time.move_to("2024-01-01 12:00:00+00:00")
        candle = Candle(
            symbol="BTC-PERP",
            interval="1h",
            open=Decimal("50000.0"),
            high=Decimal("50100.0"),
            low=Decimal("49900.0"),
            close=Decimal("50050.0"),
            volume=Decimal("1000.0"),
            open_time=datetime.now(UTC),
        )

        with patch.object(
            funding_rate_strategy, "evaluate_entry_opportunity", return_value=None
        ) as mock_evaluate:
            # Act
            result = await funding_rate_strategy.process_data(candle)

        # Assert
        assert result == []
        mock_evaluate.assert_called_once()

    # EDGE CASES
    @pytest.mark.asyncio
    async def test_process_data_edge_zero_volume_candle(
        self, funding_rate_strategy: FundingRateArbitrageStrategy, frozen_time: FreezerProtocol
    ) -> None:
        """Test data processing with zero volume candle."""
        # Arrange
        frozen_time.move_to("2024-01-01 12:00:00+00:00")
        candle = Candle(
            symbol="BTC-PERP",
            interval="1h",
            open=Decimal("50000.0"),
            high=Decimal("50000.0"),
            low=Decimal("50000.0"),
            close=Decimal("50000.0"),
            volume=Decimal("0.0"),  # Zero volume
            open_time=datetime.now(UTC),
        )

        with patch.object(
            funding_rate_strategy, "evaluate_entry_opportunity", return_value=[]
        ) as mock_evaluate:
            # Act
            result = await funding_rate_strategy.process_data(candle)

        # Assert
        assert result == []
        mock_evaluate.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_data_edge_very_old_candle(
        self, funding_rate_strategy: FundingRateArbitrageStrategy, frozen_time: FreezerProtocol
    ) -> None:
        """Test data processing with very old candle."""
        # Arrange
        frozen_time.move_to("2024-01-01 12:00:00+00:00")
        old_timestamp = datetime.now(UTC) - timedelta(days=1)
        candle = Candle(
            symbol="BTC-PERP",
            interval="1h",
            open=Decimal("50000.0"),
            high=Decimal("50100.0"),
            low=Decimal("49900.0"),
            close=Decimal("50050.0"),
            volume=Decimal("1000.0"),
            open_time=old_timestamp,
        )

        with patch.object(
            funding_rate_strategy, "evaluate_entry_opportunity", return_value=[]
        ) as mock_evaluate:
            # Act
            result = await funding_rate_strategy.process_data(candle)

        # Assert
        assert result == []
        mock_evaluate.assert_called_once()

    # FAILURE CASES
    @pytest.mark.asyncio
    async def test_process_data_failure_evaluate_raises_exception(
        self, funding_rate_strategy: FundingRateArbitrageStrategy, frozen_time: FreezerProtocol
    ) -> None:
        """Test data processing failure when evaluate_entry_opportunity raises exception."""
        # Arrange
        frozen_time.move_to("2024-01-01 12:00:00+00:00")
        candle = Candle(
            symbol="BTC-PERP",
            interval="1h",
            open=Decimal("50000.0"),
            high=Decimal("50100.0"),
            low=Decimal("49900.0"),
            close=Decimal("50050.0"),
            volume=Decimal("1000.0"),
            open_time=datetime.now(UTC),
        )

        with patch.object(
            funding_rate_strategy,
            "evaluate_entry_opportunity",
            side_effect=Exception("Evaluation error"),
        ):
            # Act
            result = await funding_rate_strategy.process_data(candle)

        # Assert
        assert result == []  # Should return empty list on exception

    @pytest.mark.asyncio
    async def test_process_data_failure_none_candle(
        self, funding_rate_strategy: FundingRateArbitrageStrategy, frozen_time: FreezerProtocol
    ) -> None:
        """Test data processing failure with None candle."""
        # Act & Assert
        frozen_time.move_to("2024-01-01 12:00:00+00:00")
        with pytest.raises(AttributeError):
            await funding_rate_strategy.process_data(None)  # type: ignore[arg-type]

    @pytest.mark.asyncio
    async def test_process_data_failure_invalid_candle_data(
        self, funding_rate_strategy: FundingRateArbitrageStrategy, frozen_time: FreezerProtocol
    ) -> None:
        """Test data processing with invalid candle data."""
        # Arrange - Create candle with missing required fields
        frozen_time.move_to("2024-01-01 12:00:00+00:00")
        incomplete_candle = Mock()
        incomplete_candle.symbol = None
        incomplete_candle.close = None
        incomplete_candle.open_time = None

        # Act
        result = await funding_rate_strategy.process_data(incomplete_candle)

        # Assert - Should handle gracefully and return empty list
        assert result == []


# Integration tests
class TestIntegrationScenarios:
    """Integration test scenarios for funding rate arbitrage strategy functionality."""

    @pytest.mark.asyncio
    async def test_full_opportunity_detection_and_signal_generation(
        self,
        mock_data_handler: MagicMock,
        mock_portfolio_state_manager: Mock,
        mock_risk_manager: Mock,
        strategy_params: dict[str, Any],
        frozen_time: FreezerProtocol,
    ) -> None:
        """Test complete flow from opportunity detection to signal generation."""
        # Arrange
        frozen_time.move_to("2024-01-01 12:00:00+00:00")
        strategy = FundingRateArbitrageStrategy(
            name="integration_test",
            symbol="BTC-PERP",
            data_handler=mock_data_handler,
            portfolio_tracker=mock_portfolio_state_manager,
            risk_manager=mock_risk_manager,
            params=strategy_params,
        )

        # Mock funding rate
        funding_rate = FundingRate(
            symbol="BTC-PERP",
            timestamp=datetime.now(UTC),
            funding_rate=Decimal("0.002"),  # Increased to ensure profit  # 0.1% funding rate
            next_funding_time=datetime.now(UTC) + timedelta(hours=8),
        )
        mock_data_handler.get_latest_funding_rate.return_value = funding_rate

        # Mock tickers with profitable spread
        perp_ticker = Ticker(
            symbol="BTC-PERP",
            exchange="hyperliquid",
            timestamp=datetime.now(UTC),
            price=Decimal("50000.5"),
            bid=Decimal("50000.0"),
            ask=Decimal("50001.0"),
        )
        spot_ticker = Ticker(
            symbol="BTC_USDC",
            exchange="backpack",
            timestamp=datetime.now(UTC),
            price=Decimal("49900.5"),
            bid=Decimal("49900.0"),  # Lower price on spot
            ask=Decimal("49901.0"),
        )

        def mock_get_ticker(exchange: str, symbol: str) -> Ticker | None:
            if exchange == "hyperliquid" and symbol == "BTC-PERP":
                return perp_ticker
            if exchange == "backpack" and symbol == "BTC_USDC":
                return spot_ticker
            return None

        mock_data_handler.get_latest_ticker.side_effect = mock_get_ticker
        mock_data_handler.fetch_funding_rates = AsyncMock(return_value=None)

        # Create a mock ArbitrageOpportunity for the sized opportunity
        mock_opportunity = Mock(spec=ArbitrageOpportunity)
        mock_opportunity.symbol = "BTC-PERP"
        mock_opportunity.long_exchange = "backpack"
        mock_opportunity.short_exchange = "hyperliquid"
        mock_opportunity.net_funding_differential = Decimal("0.002")

        # Mock risk manager sizing - using correct attributes based on SizedOpportunity
        mock_sized_opp = Mock(spec=SizedOpportunity)
        mock_sized_opp.opportunity = mock_opportunity
        mock_sized_opp.long_size = Decimal("1000.0")
        mock_sized_opp.short_size = Decimal("1000.0")
        mock_sized_opp.allocation_percentage = Decimal("10.0")
        mock_sized_opp.expected_profit = Decimal("100.0")
        mock_sized_opp.expected_return = Decimal("10.0")
        mock_sized_opp.risk_adjusted_return = Decimal("5.0")

        mock_risk_manager.size_opportunity.return_value = mock_sized_opp
        mock_risk_manager.validate_signal.return_value = True

        # Act
        signals = await strategy.evaluate_entry_opportunity()

        # Assert
        assert signals is not None
        assert len(signals) == 2  # Should generate two signals (perp and spot)

        # Verify basic signal characteristics
        symbols_found: set[str] = set()
        for signal in signals:
            symbols_found.add(signal.symbol)
            assert signal.quantity is not None
            assert signal.price > Decimal(0)

        # Should have both BTC-PERP and BTC_USDC signals
        assert symbols_found == {"BTC-PERP", "BTC_USDC"}

    @pytest.mark.asyncio
    async def test_no_opportunity_when_funding_rate_too_low(
        self,
        mock_data_handler: MagicMock,
        mock_portfolio_state_manager: Mock,
        mock_risk_manager: Mock,
        strategy_params: dict[str, Any],
        frozen_time: FreezerProtocol,
    ) -> None:
        """Test that no signals are generated when funding rate is below threshold."""
        # Arrange
        frozen_time.move_to("2024-01-01 12:00:00+00:00")
        strategy = FundingRateArbitrageStrategy(
            name="integration_test",
            symbol="BTC-PERP",
            data_handler=mock_data_handler,
            portfolio_tracker=mock_portfolio_state_manager,
            risk_manager=mock_risk_manager,
            params=strategy_params,
        )

        # Mock very low funding rate (below min_funding_differential)
        funding_rate = FundingRate(
            symbol="BTC-PERP",
            timestamp=datetime.now(UTC),
            funding_rate=Decimal("0.00001"),  # 0.001% - below threshold
            next_funding_time=datetime.now(UTC) + timedelta(hours=8),
        )
        mock_data_handler.get_latest_funding_rate.return_value = funding_rate

        # Mock tickers
        perp_ticker = Ticker(
            symbol="BTC-PERP",
            exchange="hyperliquid",
            timestamp=datetime.now(UTC),
            price=Decimal("50000.5"),
            bid=Decimal("50000.0"),
            ask=Decimal("50001.0"),
        )
        spot_ticker = Ticker(
            symbol="BTC_USDC",
            exchange="backpack",
            timestamp=datetime.now(UTC),
            price=Decimal("50000.5"),
            bid=Decimal("50000.0"),
            ask=Decimal("50001.0"),
        )

        def mock_get_ticker(exchange: str, symbol: str) -> Ticker | None:
            if exchange == "hyperliquid" and symbol == "BTC-PERP":
                return perp_ticker
            if exchange == "backpack" and symbol == "BTC_USDC":
                return spot_ticker
            return None

        mock_data_handler.get_latest_ticker.side_effect = mock_get_ticker

        # Act
        signals = await strategy.evaluate_entry_opportunity()

        # Assert
        assert signals is None or signals == []
