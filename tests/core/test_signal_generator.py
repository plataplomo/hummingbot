"""
Tests for the SignalGenerator class.
"""

import logging

# Removed unused asyncio
from collections import deque
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any  # Removed Coroutine
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from cyberdelta.core.data_handler import DataHandler
from cyberdelta.core.models import (
    ArbitrageOpportunity,
    FundingRate,
    OrderBook,
    Ticker,
    # Position, # Removed unused Position import
)
from cyberdelta.core.signal_generator import SignalGenerator
from cyberdelta.core.symbol_mapper import SymbolMapper
from cyberdelta.utils.config import Config

logger = logging.getLogger(__name__)


class TestSignalGenerator:
    """Test suite for the SignalGenerator class."""

    @pytest.fixture
    def mock_config_dict(self) -> dict[str, Any]:
        """Provides a dictionary for simple config mocking."""
        return {
            "exchanges": {
                "hyperliquid": {
                    "enabled": True,
                    "symbols": {"BTC": "BTC-PERP", "ETH": "ETH-PERP"},
                    "fee_rate": "0.0004",
                },
                "backpack": {
                    "enabled": True,
                    "symbols": {"BTC": "BTC_USDC", "ETH": "ETH_USDC"},
                    "fee_rate": "0.0006",
                },
            },
            "strategy.funding_rate.min_funding_differential": "0.0002",
            "strategy.funding_rate.min_profit_threshold": "3.0",
            "strategy.funding_rate.funding_sample_period": 3600,
            "strategy.funding_rate.funding_sample_count": 24,
            "strategy.funding_rate.risk_aversion": 1.0,
            "strategy.funding_rate.default_slippage": "0.001",
            "strategy.funding_rate.slippage_sensitivity": "0.5",
            "strategy.funding_rate.liquidity_threshold_usd": "10000",
            "strategy.funding_rate.max_slippage_percent": "0.01",
        }

    @pytest.fixture
    def config(self, mock_config_dict: dict[str, Any]) -> MagicMock:
        """Create a mock config for testing."""
        mock_config = MagicMock(spec=Config)

        # Define side effect using nested function with type hints
        def config_get_side_effect(key: str, default: Any = None) -> Any:
            if "." in key:
                parts = key.split(".")
                base = parts[0]
                if base == "exchanges" and len(parts) > 2:
                    exchange = parts[1]
                    prop = parts[2]
                    return mock_config_dict.get(base, {}).get(exchange, {}).get(prop, default)
            return mock_config_dict.get(key, default)

        mock_config.get.side_effect = config_get_side_effect
        return mock_config

    @pytest.fixture
    def data_handler(self) -> MagicMock:
        """Fixture for mock DataHandler."""
        handler = MagicMock(spec=DataHandler)

        # Use exchange-specific symbols
        funding_rates: dict[str, dict[str, FundingRate]] = {
            "hyperliquid": {
                "BTC-PERP": FundingRate(
                    symbol="BTC-PERP", funding_rate=Decimal("-0.001"), mark_price=Decimal("30000")
                ),
                "ETH-PERP": FundingRate(
                    symbol="ETH-PERP", funding_rate=Decimal("0.005"), mark_price=Decimal("2000")
                ),
            },
            "backpack": {
                "BTC_USDC": FundingRate(
                    symbol="BTC_USDC", funding_rate=Decimal("0.002"), mark_price=Decimal("30010")
                ),
                "ETH_USDC": FundingRate(
                    symbol="ETH_USDC", funding_rate=Decimal("-0.01"), mark_price=Decimal("2005")
                ),
            },
        }
        tickers: dict[str, dict[str, Ticker]] = {
            "hyperliquid": {
                "BTC-PERP": Ticker(
                    symbol="BTC-PERP",
                    price=Decimal("30000"),
                    bid=Decimal("29999"),
                    ask=Decimal("30001"),
                    volume=Decimal("100"),
                ),
                "ETH-PERP": Ticker(
                    symbol="ETH-PERP",
                    price=Decimal("2000"),
                    bid=Decimal("1999"),
                    ask=Decimal("2001"),
                    volume=Decimal("500"),
                ),
            },
            "backpack": {
                "BTC_USDC": Ticker(
                    symbol="BTC_USDC",
                    price=Decimal("30010"),
                    bid=Decimal("30009"),
                    ask=Decimal("30011"),
                    volume=Decimal("120"),
                ),
                "ETH_USDC": Ticker(
                    symbol="ETH_USDC",
                    price=Decimal("2005"),
                    bid=Decimal("2004"),
                    ask=Decimal("2006"),
                    volume=Decimal("600"),
                ),
            },
        }
        mock_orderbook = MagicMock(spec=OrderBook)
        mock_orderbook.bids = [(Decimal("29999"), Decimal("2.5"))]
        mock_orderbook.asks = [(Decimal("30001"), Decimal("1.5"))]
        orderbooks: dict[str, dict[str, MagicMock]] = {
            "hyperliquid": {"BTC-PERP": mock_orderbook, "ETH-PERP": mock_orderbook},
            "backpack": {"BTC_USDC": mock_orderbook, "ETH_USDC": mock_orderbook},
        }

        # Define side effects using nested functions with type hints
        def get_funding_rate_side_effect(exchange: str, symbol: str) -> FundingRate | None:
            return funding_rates.get(exchange, {}).get(symbol)

        def get_ticker_side_effect(exchange: str, symbol: str) -> Ticker | None:
            return tickers.get(exchange, {}).get(symbol)

        def get_orderbook_side_effect(exchange: str, symbol: str) -> OrderBook | None:
            return orderbooks.get(exchange, {}).get(symbol)

        handler.get_funding_rate.side_effect = get_funding_rate_side_effect
        handler.get_ticker.side_effect = get_ticker_side_effect
        handler.get_orderbook.side_effect = get_orderbook_side_effect

        return handler

    @pytest.fixture
    def symbol_mapper(self, config: MagicMock) -> SymbolMapper:
        """Fixture for a SymbolMapper using the mock config."""
        config_data: dict[str, Any] = {"exchanges": {}}
        exchanges_config = config.get("exchanges")
        if exchanges_config:
            for ex_id in exchanges_config:
                exchange_details = exchanges_config.get(ex_id, {})
                enabled = config.get(f"exchanges.{ex_id}.enabled", False)
                symbols = exchange_details.get("symbols", {})
                if enabled:
                    config_data["exchanges"][ex_id] = {"enabled": enabled, "symbols": symbols}
        return SymbolMapper(config_data)

    @pytest.fixture
    def signal_generator(
        self, config: MagicMock, data_handler: MagicMock, symbol_mapper: SymbolMapper
    ) -> SignalGenerator:
        """Create a SignalGenerator instance for testing."""
        return SignalGenerator(config, data_handler, symbol_mapper)

    def test_init(
        self, signal_generator: SignalGenerator, config: MagicMock, data_handler: MagicMock
    ) -> None:
        """Test initializing the signal generator."""
        assert signal_generator.min_funding_differential == Decimal("0.0002")
        assert signal_generator.min_profit_threshold == Decimal("3.0")
        assert "hyperliquid" in signal_generator.historical_funding_rates
        assert "BTC" in signal_generator.historical_funding_rates["hyperliquid"]
        assert isinstance(signal_generator.historical_funding_rates["hyperliquid"]["BTC"], deque)
        assert "BTC" in signal_generator.historical_basis
        assert isinstance(signal_generator.historical_basis["BTC"], deque)
        assert signal_generator.config == config
        assert signal_generator.data_handler == data_handler

    @patch("cyberdelta.core.signal_generator.datetime")
    def test_update_historical_data(
        self, mock_datetime: MagicMock, signal_generator: SignalGenerator, data_handler: MagicMock
    ) -> None:
        """Test updating historical funding rate and basis data using deque."""
        fixed_now = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
        mock_datetime.now.return_value = fixed_now
        mock_datetime.side_effect = None

        signal_generator.update_historical_data()

        assert len(signal_generator.historical_funding_rates["hyperliquid"]["BTC"]) >= 1
        assert len(signal_generator.historical_basis["BTC"]) >= 1
        assert signal_generator.historical_funding_rates["hyperliquid"]["BTC"][0] == (
            fixed_now,
            Decimal("-0.001"),
        )
        btc_basis = Decimal("30000") - Decimal("30010")
        assert signal_generator.historical_basis["BTC"][0] == (fixed_now, btc_basis)

        data_handler.get_funding_rate.assert_called()
        data_handler.get_ticker.assert_called()

        # Test data trimming
        sample_period_seconds = signal_generator.funding_sample_period
        sample_count = signal_generator.funding_sample_count

        # Define nested functions with type hints for side effects used in loop
        def get_funding_iter(
            exchange: str, symbol: str, rate_chg: Decimal = Decimal(0)
        ) -> FundingRate | None:
            base_rate = Decimal("-0.001") if exchange == "hyperliquid" else Decimal("0.002")
            return FundingRate(symbol=symbol, funding_rate=base_rate + rate_chg)

        def get_ticker_iter(
            exchange: str, symbol: str, price_chg: Decimal = Decimal(0)
        ) -> Ticker | None:
            base_price = Decimal("30000") if exchange == "hyperliquid" else Decimal("30010")
            return Ticker(symbol=symbol, price=base_price + price_chg)

        for i in range(sample_count + 5):
            mock_time = fixed_now - timedelta(seconds=i * sample_period_seconds // 2)
            mock_datetime.now.return_value = mock_time
            rate_change = Decimal(str(i * 0.00001))
            price_change = Decimal(str(i * 5))
            data_handler.get_funding_rate.side_effect = (
                lambda ex, sym, r=rate_change: get_funding_iter(ex, sym, r)
            )
            data_handler.get_ticker.side_effect = lambda ex, sym, p=price_change: get_ticker_iter(
                ex, sym, p
            )
            signal_generator.update_historical_data()

        assert len(signal_generator.historical_funding_rates["hyperliquid"]["BTC"]) <= sample_count
        assert len(signal_generator.historical_basis["BTC"]) <= sample_count

    def test_calculate_basis_volatility(self, signal_generator: SignalGenerator) -> None:
        """Test calculating basis volatility."""
        now = datetime.now(UTC)
        # Provide at least two distinct data points
        timestamps = [now - timedelta(hours=1), now]
        bases = [Decimal("10.0"), Decimal("10.2")]  # Need >= 2 points
        signal_generator.historical_basis["TEST"] = deque(zip(timestamps, bases, strict=False))

        # Calculate expected volatility (use numpy with ddof=1 for sample std dev)
        # Convert Decimals to floats for numpy calculation
        expected_volatility_float = np.std([float(b) for b in bases], ddof=1)
        expected_volatility = Decimal(str(expected_volatility_float)).quantize(Decimal("0.0001"))

        # Calculate volatility
        volatility = signal_generator.calculate_basis_volatility("TEST")

        # Assert the result
        assert isinstance(volatility, Decimal)  # Check type
        assert volatility == pytest.approx(expected_volatility)  # Use approx for float comparison
        assert volatility > Decimal("0.0")  # Ensure calculation happened

        # Test insufficient data (only 1 point, should return 0)
        signal_generator.historical_basis["TEST_INSUFFICIENT"] = deque([(now, Decimal("10.0"))])
        volatility_insufficient = signal_generator.calculate_basis_volatility("TEST_INSUFFICIENT")
        assert volatility_insufficient == Decimal("0.0")

    def test_calculate_funding_rate_volatility(self, signal_generator: SignalGenerator) -> None:
        """Test calculating funding rate volatility."""
        now = datetime.now(UTC)
        # Provide at least two distinct data points
        timestamps = [now - timedelta(hours=1), now]
        rates = [Decimal("0.00010"), Decimal("0.00012")]  # Example rates
        exchange_id = "test_ex"
        internal_symbol = "TEST"

        # Ensure deque exists before appending
        if exchange_id not in signal_generator.historical_funding_rates:
            signal_generator.historical_funding_rates[exchange_id] = {}
        signal_generator.historical_funding_rates[exchange_id].setdefault(
            internal_symbol, deque()
        ).extend(zip(timestamps, rates, strict=False))  # Use extend

        # Expected volatility (sample std dev of 0.00010, 0.00012)
        expected_volatility_float = np.std([float(r) for r in rates], ddof=1)  # Use ddof=1
        expected_volatility = Decimal(str(expected_volatility_float)).quantize(Decimal("0.000001"))

        # Calculate volatility
        volatility = signal_generator.calculate_funding_rate_volatility(
            exchange_id, internal_symbol
        )

        # Assert the result
        assert isinstance(volatility, Decimal)  # Check type
        assert volatility == pytest.approx(expected_volatility)  # Use approx
        assert volatility > Decimal("0.0")  # Ensure not zero

        # Test insufficient data
        signal_generator.historical_funding_rates["test_ex"]["TEST_INSUFFICIENT"] = deque(
            [(now, Decimal("0.0001"))]
        )
        volatility_insufficient = signal_generator.calculate_funding_rate_volatility(
            "test_ex", "TEST_INSUFFICIENT"
        )
        assert volatility_insufficient == Decimal("0.0")

    def test_estimate_slippage(
        self, signal_generator: SignalGenerator, data_handler: MagicMock
    ) -> None:
        """Test estimating slippage (currently hardcoded)."""
        mock_orderbook = MagicMock(spec=OrderBook)
        mock_orderbook.bids = [(Decimal("29999"), Decimal("2.5"))]
        mock_orderbook.asks = [(Decimal("30001"), Decimal("1.5"))]
        data_handler.get_orderbook.return_value = mock_orderbook
        estimated_slippage = signal_generator.estimate_slippage("test_ex", "TEST")
        # TODO: Implement actual slippage estimation logic in SignalGenerator.estimate_slippage
        assert estimated_slippage == Decimal("1E-9")

    def test_generate_opportunities(
        self, signal_generator: SignalGenerator, data_handler: MagicMock, config: MagicMock
    ) -> None:
        """Test generating arbitrage opportunities."""
        opportunities = signal_generator.generate_arbitrage_opportunities()
        assert len(opportunities) > 0, "Expected opportunities based on mocked data and thresholds"
        assert isinstance(opportunities[0], ArbitrageOpportunity)
        opp = opportunities[0]
        assert opp.symbol == "BTC"
        assert opp.long_exchange == "hyperliquid"
        assert opp.short_exchange == "backpack"
        assert opp.net_funding_differential is not None
        assert opp.net_funding_differential.compare(Decimal("0.003")) == Decimal("0")

    def test_generate_opportunities_no_eligible(
        self, signal_generator: SignalGenerator, data_handler: MagicMock, config: MagicMock
    ) -> None:
        """Test when no opportunities meet the eligibility criteria."""

        # Define side effect with type hints
        def mock_low_funding(exchange: str, symbol: str) -> FundingRate | None:
            # Use exchange symbols from mapper if needed
            hl_sym = (
                signal_generator.symbol_mapper.get_exchange_symbol(symbol, "hyperliquid") or symbol
            )
            bp_sym = (
                signal_generator.symbol_mapper.get_exchange_symbol(symbol, "backpack") or symbol
            )
            rates = {
                "hyperliquid": {
                    hl_sym: FundingRate(symbol=hl_sym, funding_rate=Decimal("0.00001"))
                },
                "backpack": {bp_sym: FundingRate(symbol=bp_sym, funding_rate=Decimal("0.00002"))},
            }
            # Return based on the *exchange symbol* passed in the call
            return rates.get(exchange, {}).get(symbol)

        data_handler.get_funding_rate.side_effect = mock_low_funding

        opportunities = signal_generator.generate_arbitrage_opportunities()
        assert len(opportunities) == 0

    def test_generate_opportunities_single_exchange(
        self, signal_generator: SignalGenerator, config: MagicMock, data_handler: MagicMock
    ) -> None:
        """Test scenario with only one exchange configured."""

        # Define side effect with type hints
        def single_exchange_config_get(key: str, default: Any = None) -> Any:
            mock_single_config_dict = {
                "exchanges": {"hyperliquid": {"enabled": True, "symbols": {"BTC": "BTC-PERP"}}},
                "strategy.funding_rate.min_funding_differential": "0.0002",
                # Add other required keys with default values
                "strategy.funding_rate.min_profit_threshold": "3.0",
                "strategy.funding_rate.funding_sample_period": 3600,
                "strategy.funding_rate.funding_sample_count": 24,
                "strategy.funding_rate.risk_aversion": 1.0,
                "strategy.funding_rate.default_slippage": "0.001",
                "strategy.funding_rate.slippage_sensitivity": "0.5",
                "strategy.funding_rate.liquidity_threshold_usd": "10000",
                "strategy.funding_rate.max_slippage_percent": "0.01",
            }
            if key.startswith("exchanges.hyperliquid."):
                prop = key.split(".")[-1]
                return mock_single_config_dict["exchanges"]["hyperliquid"].get(prop, default)
            return mock_single_config_dict.get(key, default)

        config.get.side_effect = single_exchange_config_get
        signal_generator._initialize_data_structures()

        opportunities = signal_generator.generate_arbitrage_opportunities()
        assert len(opportunities) == 0

    def test_arbitrage_opportunity_creation(self, signal_generator: SignalGenerator) -> None:
        """Test the internal creation logic for ArbitrageOpportunity."""
        now = datetime.now(UTC)
        # Correct structure: dict[exchange_id, FundingRate]
        funding_data = {
            "hyperliquid": FundingRate(
                symbol="BTC-PERP", funding_rate=Decimal("-0.001"), timestamp=now
            ),
            "backpack": FundingRate(
                symbol="BTC_USDC", funding_rate=Decimal("0.002"), timestamp=now
            ),
        }
        # Correct structure: dict[exchange_id, Ticker]
        ticker_data = {
            "hyperliquid": Ticker(symbol="BTC-PERP", price=Decimal("41000")),
            "backpack": Ticker(symbol="BTC_USDC", price=Decimal("41100")),
        }

        opportunities = signal_generator._check_funding_rate_opportunities(
            "BTC", funding_data, ticker_data
        )

        assert len(opportunities) == 1
        opp = opportunities[0]
        assert isinstance(opp, ArbitrageOpportunity)
        assert opp.symbol == "BTC"
        assert opp.long_exchange == "hyperliquid"
        assert opp.short_exchange == "backpack"
        assert opp.net_funding_differential == Decimal("0.003")
