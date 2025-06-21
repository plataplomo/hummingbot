"""Tests for the SignalGenerator class."""

import logging

# Removed unused asyncio
from collections import deque
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any  # Removed Coroutine
from unittest.mock import MagicMock

import numpy as np
import pytest

from cyberdelta.config.config_models import AppSettings
from cyberdelta.core.data_handler import DataHandler
from cyberdelta.core.models import (
    FundingRate,
    OrderBook,
    Ticker,
    # Position, # Removed unused Position import
)
from cyberdelta.core.signal_generator import SignalGenerator
from cyberdelta.core.symbol_mapper import SymbolMapper
from cyberdelta.validation.funding_data import ArbitrageOpportunity

logger = logging.getLogger(__name__)


class TestSignalGenerator:
    """Test suite for the SignalGenerator class."""

    @pytest.fixture
    def mock_config_dict(self) -> dict[str, Any]:
        """Provide a dictionary for simple config mocking."""
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
    def config(self, test_app_settings: "AppSettings") -> MagicMock:
        """Create a mock config for testing using the new AppSettings."""
        mock_config = MagicMock()

        # Define side effect using nested function with type hints
        def config_get_side_effect(key: str, default: object | None = None) -> object | None:
            """Handle config get side effect for testing."""
            if "." in key:
                parts = key.split(".")
                base = parts[0]
                if base == "exchanges" and len(parts) > 2:
                    exchange = parts[1]
                    prop = parts[2]
                    # Use the test_app_settings exchanges configuration
                    exchanges = test_app_settings.exchanges
                    if exchange in exchanges:
                        exchange_config = exchanges[exchange]
                        if prop == "enabled":
                            return exchange_config.enabled
                        if prop == "symbols":
                            return exchange_config.symbols
                        if prop == "fee_rate":
                            return "0.0004" if exchange == "hyperliquid" else "0.0006"
                    return default
                if base == "strategy":
                    # Return strategy configuration values
                    strategy_defaults = {
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
                    return strategy_defaults.get(key, default)
            return default

        mock_config.get.side_effect = config_get_side_effect
        return mock_config

    @pytest.fixture
    def data_handler(self) -> MagicMock:
        """Fixture for mock DataHandler."""
        handler = MagicMock(spec=DataHandler)
        now = datetime.now(UTC)

        # Use exchange-specific symbols matching test_config.yaml
        funding_rates: dict[str, dict[str, FundingRate]] = {
            "hyperliquid": {
                "BTC": FundingRate(
                    symbol="BTC",
                    funding_rate=Decimal("-0.001"),
                    mark_price=Decimal("30000"),
                    timestamp=now,
                ),
                "ETH": FundingRate(
                    symbol="ETH",
                    funding_rate=Decimal("0.005"),
                    mark_price=Decimal("2000"),
                    timestamp=now,
                ),
            },
            "backpack": {
                "BTC-USDC": FundingRate(
                    symbol="BTC-USDC",
                    funding_rate=Decimal("0.002"),
                    mark_price=Decimal("30010"),
                    timestamp=now,
                ),
                "ETH-USDC": FundingRate(
                    symbol="ETH-USDC",
                    funding_rate=Decimal("-0.01"),
                    mark_price=Decimal("2005"),
                    timestamp=now,
                ),
            },
        }

        # Mock Ticker data for data_handler.tickers
        mock_hl_btc_ticker = Ticker(
            symbol="BTC",
            price=Decimal("30000"),
            bid=Decimal("29999"),
            ask=Decimal("30001"),
            timestamp=now,
        )
        mock_bp_btc_ticker = Ticker(
            symbol="BTC-USDC",
            price=Decimal("30010"),
            bid=Decimal("30009"),
            ask=Decimal("30011"),
            timestamp=now,
        )
        mock_hl_eth_ticker = Ticker(
            symbol="ETH",
            price=Decimal("2000"),
            bid=Decimal("1999"),
            ask=Decimal("2001"),
            timestamp=now,
        )
        mock_bp_eth_ticker = Ticker(
            symbol="ETH-USDC",
            price=Decimal("2005"),
            bid=Decimal("2004"),
            ask=Decimal("2006"),
            timestamp=now,
        )

        handler.tickers = {
            "hyperliquid": {
                "BTC": mock_hl_btc_ticker,
                "ETH": mock_hl_eth_ticker,
            },
            "backpack": {
                "BTC-USDC": mock_bp_btc_ticker,
                "ETH-USDC": mock_bp_eth_ticker,
            },
        }

        mock_orderbook = MagicMock(spec=OrderBook)
        mock_orderbook.bids = [(Decimal("29999"), Decimal("2.5"))]
        mock_orderbook.asks = [(Decimal("30001"), Decimal("1.5"))]
        # This orderbooks mock might not be strictly necessary if get_orderbook is not called
        # by the tested logic or if estimate_slippage is mocked directly if it uses
        # get_orderbook. For now, keeping it as it was.
        # or if estimate_slippage is mocked directly if it uses get_orderbook.
        # For now, keeping it as it was.
        orderbooks: dict[str, dict[str, MagicMock]] = {
            "hyperliquid": {"BTC": mock_orderbook, "ETH": mock_orderbook},
            "backpack": {"BTC-USDC": mock_orderbook, "ETH-USDC": mock_orderbook},
        }

        def get_funding_rate_side_effect(exchange: str, symbol: str) -> FundingRate | None:
            """Get funding rate side effect for testing."""
            return funding_rates.get(exchange, {}).get(symbol)

        # get_ticker side effect is no longer directly used by SignalGenerator for opportunities,
        # as it uses handler.tickers. However, other parts of tests might still use it.
        # For safety, ensure it returns Ticker if something still calls it.
        def get_ticker_side_effect(exchange: str, symbol: str) -> Ticker | None:
            """Get ticker side effect for testing."""
            ticker = handler.tickers.get(exchange, {}).get(symbol)
            return ticker if ticker is None or isinstance(ticker, Ticker) else None

        def get_orderbook_side_effect(exchange: str, symbol: str) -> OrderBook | None:
            """Get orderbook side effect for testing."""
            return orderbooks.get(exchange, {}).get(symbol)

        handler.get_latest_funding_rate.side_effect = get_funding_rate_side_effect
        handler.get_latest_ticker.side_effect = get_ticker_side_effect
        handler.get_latest_order_book.side_effect = get_orderbook_side_effect

        return handler

    @pytest.fixture
    def symbol_mapper(self, test_app_settings: AppSettings) -> SymbolMapper:
        """Fixture for a SymbolMapper using the test AppSettings."""
        exchanges_map_for_mapper: dict[str, Any] = {}
        for ex_id, exchange_config in test_app_settings.exchanges.items():
            if exchange_config.enabled:
                # SymbolMapper expects the exchange config to have a "symbols" key
                exchanges_map_for_mapper[ex_id] = {"symbols": exchange_config.symbols}
        return SymbolMapper(exchanges_map_for_mapper)

    @pytest.fixture
    def signal_generator(
        self,
        test_app_settings: AppSettings,
        data_handler: MagicMock,
        symbol_mapper: SymbolMapper,
    ) -> SignalGenerator:
        """Create a SignalGenerator instance for testing."""
        return SignalGenerator(test_app_settings, data_handler, symbol_mapper)

    def test_init(
        self,
        signal_generator: SignalGenerator,
        test_app_settings: AppSettings,
        data_handler: MagicMock,
    ) -> None:
        """Test initializing the signal generator."""
        assert signal_generator.min_funding_differential == Decimal("0.0001")
        assert signal_generator.min_profit_threshold == Decimal("1.0")
        assert "hyperliquid" in signal_generator.historical_funding_rates
        assert "BTC" in signal_generator.historical_funding_rates["hyperliquid"]
        assert isinstance(signal_generator.historical_funding_rates["hyperliquid"]["BTC"], deque)
        assert "BTC" in signal_generator.historical_basis
        assert isinstance(signal_generator.historical_basis["BTC"], deque)
        assert signal_generator.app_settings == test_app_settings
        assert signal_generator.data_handler == data_handler

    def test_update_historical_data(
        self,
        signal_generator: SignalGenerator,
        data_handler: MagicMock,
    ) -> None:
        """Test updating historical funding rate and basis data using deque."""
        # No need to mock datetime since we're testing the logic, not the timestamp

        signal_generator.update_historical_data()

        assert len(signal_generator.historical_funding_rates["hyperliquid"]["BTC"]) == 1
        assert len(signal_generator.historical_basis["BTC"]) == 1

        # Check the funding rate value (ignoring timestamp)
        _, funding_rate = signal_generator.historical_funding_rates["hyperliquid"]["BTC"][0]
        assert funding_rate == Decimal("-0.001")

        # Check the basis value (ignoring timestamp)
        _, basis = signal_generator.historical_basis["BTC"][0]
        btc_basis = Decimal("30000") - Decimal("30010")
        assert basis == btc_basis

        data_handler.reset_mock()

        sample_count = signal_generator.funding_sample_count

        def get_funding_iter(
            exchange: str,
            symbol: str,
            rate_chg: Decimal = Decimal(0),
        ) -> FundingRate | None:
            """Get funding iter for testing."""
            base_rate = Decimal("-0.001") if exchange == "hyperliquid" else Decimal("0.002")
            return FundingRate(
                symbol=symbol,
                funding_rate=base_rate + rate_chg,
                timestamp=datetime.now(UTC),
            )

        def get_ticker_iter(
            exchange: str,
            symbol: str,
            price_chg: Decimal = Decimal(0),
        ) -> Ticker | None:
            """Get ticker iter for testing."""
            base_price = Decimal("30000") if exchange == "hyperliquid" else Decimal("30010")
            return Ticker(symbol=symbol, price=base_price + price_chg, timestamp=datetime.now(UTC))

        for i in range(sample_count + 5):
            rate_change = Decimal(str(i * 0.00001))
            price_change = Decimal(str(i * 5))

            # Update side effects to return slightly different data each time
            def _funding_side_effect(
                ex: str, sym: str, r: Decimal = rate_change
            ) -> FundingRate | None:
                return get_funding_iter(ex, sym, r)

            def _ticker_side_effect(ex: str, sym: str, p: Decimal = price_change) -> Ticker | None:
                return get_ticker_iter(ex, sym, p)

            data_handler.get_latest_funding_rate.side_effect = _funding_side_effect
            data_handler.get_latest_ticker.side_effect = _ticker_side_effect

            # Call the update function multiple times
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
        expected_volatility = Decimal(str(expected_volatility_float))

        # Calculate volatility
        volatility = signal_generator.calculate_basis_volatility("TEST")

        # Assert the result
        assert isinstance(volatility, Decimal)  # Check type
        # Compare against the Decimal expected value calculated earlier
        assert_decimal_approx(volatility, expected_volatility)
        assert volatility > Decimal("0.0")  # Ensure calculation happened

        # Test insufficient data (only 1 point, should return default volatility)
        signal_generator.historical_basis["TEST_INSUFFICIENT"] = deque([(now, Decimal("10.0"))])
        volatility_insufficient = signal_generator.calculate_basis_volatility("TEST_INSUFFICIENT")
        assert volatility_insufficient == Decimal("0.01")

        signal_generator.historical_basis["TEST"] = deque()  # Clear for next test

    def test_calculate_funding_rate_volatility(self, signal_generator: SignalGenerator) -> None:
        """Test calculation of funding rate volatility."""
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
            internal_symbol,
            deque(),
        ).extend(zip(timestamps, rates, strict=False))  # Use extend

        # Expected volatility (sample std dev of 0.00010, 0.00012)
        expected_volatility_float = np.std([float(r) for r in rates], ddof=1)  # Use ddof=1
        expected_volatility = Decimal(str(expected_volatility_float))

        # Calculate volatility
        volatility = signal_generator.calculate_funding_rate_volatility(
            exchange_id,
            internal_symbol,
        )

        # Assert the result
        assert isinstance(volatility, Decimal)  # Check type
        # Compare against the Decimal expected value calculated earlier
        assert_decimal_approx(volatility, expected_volatility)
        assert volatility > Decimal("0.0")  # Ensure not zero

        # Test insufficient data (should return default volatility)
        signal_generator.historical_funding_rates["test_ex"]["TEST_INSUFFICIENT"] = deque(
            [(now, Decimal("0.0001"))],
        )
        volatility_insufficient = signal_generator.calculate_funding_rate_volatility(
            "test_ex",
            "TEST_INSUFFICIENT",
        )
        assert volatility_insufficient == Decimal("0.0001")

        signal_generator.historical_basis["TEST"] = deque()  # Clear for next test

    def test_estimate_slippage(
        self,
        signal_generator: SignalGenerator,
        data_handler: MagicMock,
    ) -> None:
        """Test estimating slippage (currently hardcoded)."""
        mock_orderbook = MagicMock(spec=OrderBook)
        mock_orderbook.bids = [(Decimal("29999"), Decimal("2.5"))]
        mock_orderbook.asks = [(Decimal("30001"), Decimal("1.5"))]
        data_handler.get_latest_order_book.return_value = mock_orderbook
        # Use an exchange defined in the mock config for the test
        exchange_id_for_test = "hyperliquid"
        # The estimate_slippage method in SignalGenerator uses data_handler.get_latest_order_book,
        # which is already mocked. We also need to ensure that the Ticker data is available
        # if it's used internally by estimate_slippage or its callees.
        # For now, assuming Ticker is not directly used by estimate_slippage based on its
        # current simple mock.
        # The estimate_slippage method itself might need get_latest_ticker if it uses price
        # data. For this test, we mock get_latest_order_book. If it also needs
        # get_latest_ticker, that should be mocked too.
        data_handler.get_latest_order_book.return_value = mock_orderbook

        estimated_slippage = signal_generator.estimate_slippage(exchange_id_for_test, "TEST")
        # Assert the estimated slippage matches the expected fallback value from config
        expected_fallback_slippage = (
            Decimal(signal_generator.default_slippage) * signal_generator.slippage_sensitivity
        )
        assert estimated_slippage == expected_fallback_slippage  # Expect 0.001 * 0.5 = 0.0005

    @pytest.mark.asyncio
    async def test_generate_opportunities(
        self,
        signal_generator: SignalGenerator,
        data_handler: MagicMock,
        config: MagicMock,
    ) -> None:
        """Test generating arbitrage opportunities."""
        # now = datetime.now(UTC) # Unused variable
        # Prepare mock data arguments based on the method's needs
        # Mock funding_data structure: symbol -> exchange -> FundingRate | None
        mock_funding_data = {
            "BTC": {
                "hyperliquid": data_handler.get_latest_funding_rate("hyperliquid", "BTC"),
                "backpack": data_handler.get_latest_funding_rate("backpack", "BTC-USDC"),
            },
            "ETH": {
                "hyperliquid": data_handler.get_latest_funding_rate("hyperliquid", "ETH"),
                "backpack": data_handler.get_latest_funding_rate("backpack", "ETH-USDC"),
            },
        }
        # SignalGenerator now uses self.data_handler.tickers internally.
        # Ensure data_handler.tickers is populated correctly in the fixture or test setup.
        # The data_handler fixture already populates data_handler.tickers.

        opportunities = await signal_generator.generate_arbitrage_opportunities(
            funding_data=mock_funding_data,
        )
        assert len(opportunities) > 0, "Expected opportunities based on mocked data and thresholds"
        assert isinstance(opportunities[0], ArbitrageOpportunity)
        opp = opportunities[0]
        assert opp.symbol == "BTC"  # Based on mock data, BTC diff is 0.002 - (-0.001) = 0.003
        assert opp.long_exchange == "backpack"  # Higher funding rate (positive NFD source) is long
        assert (
            opp.short_exchange == "hyperliquid"
        )  # Lower funding rate (negative NFD source) is short
        assert opp.net_funding_differential is not None
        # Re-verify expected net_funding_differential based on calculation in source
        # price_a * rate_a = 30000 * -0.001 = -30.0
        # price_b * rate_b = 30010 * 0.002 = 60.02
        # net = 60.02 - (-30.0) = 90.02
        # Expected rate diff: rate_b (0.002) - rate_a (-0.001) = 0.003
        assert opp.net_funding_differential.compare(Decimal("0.003")) == Decimal(
            "0",
        )  # Compare against the rate differential

    @pytest.mark.asyncio
    async def test_generate_opportunities_no_eligible(
        self,
        signal_generator: SignalGenerator,
        data_handler: MagicMock,
        config: MagicMock,
    ) -> None:
        """Test when no opportunities meet the eligibility criteria."""
        now = datetime.now(UTC)  # This 'now' is used for mock_low_funding timestamps

        # Define side effect with type hints, adding mark_price and timestamp
        def mock_low_funding(exchange: str, symbol: str) -> FundingRate | None:
            """Return mock low funding for testing."""
            hl_sym = (
                signal_generator.symbol_mapper.get_exchange_symbol("BTC", "hyperliquid")
                or "BTC-PERP"
            )
            bp_sym = (
                signal_generator.symbol_mapper.get_exchange_symbol("BTC", "backpack") or "BTC_USDC"
            )
            rates = {
                "hyperliquid": {
                    hl_sym: FundingRate(
                        symbol=hl_sym,
                        funding_rate=Decimal("0.00001"),
                        mark_price=Decimal("30000"),
                        timestamp=now,
                    ),
                },
                "backpack": {
                    bp_sym: FundingRate(
                        symbol=bp_sym,
                        funding_rate=Decimal("0.00002"),
                        mark_price=Decimal("30010"),
                        timestamp=now,
                    ),
                },
            }
            return rates.get(exchange, {}).get(symbol)

        data_handler.get_latest_funding_rate.side_effect = mock_low_funding

        # SignalGenerator uses self.data_handler.tickers. Ensure it's set.
        # For this test, we assume the prices are such that the low funding diff doesn't
        # create an opp.
        # The data_handler fixture populates tickers, which should be sufficient.
        # If specific ticker values are needed for this test, mock data_handler.tickers here.
        # Example:
        # data_handler.tickers = { ... specific Ticker objects ... }

        mock_funding_data = {
            "BTC": {
                "hyperliquid": mock_low_funding("hyperliquid", "BTC-PERP"),
                "backpack": mock_low_funding("backpack", "BTC_USDC"),
            },
        }

        opportunities = await signal_generator.generate_arbitrage_opportunities(
            funding_data=mock_funding_data,
        )
        assert len(opportunities) == 0

    @pytest.mark.asyncio
    async def test_generate_opportunities_single_exchange(
        self,
        signal_generator: SignalGenerator,
        config: MagicMock,
        data_handler: MagicMock,
        symbol_mapper: SymbolMapper,
    ) -> None:
        """Test scenario with only one exchange configured."""

        # now = datetime.now(UTC) # Unused variable
        # Define side effect with type hints
        def single_exchange_config_get(key: str, default: object | None = None) -> object | None:
            """Handle single exchange config get for testing."""
            mock_single_config_dict: dict[str, Any] = {
                "exchanges": {"hyperliquid": {"enabled": True, "symbols": {"BTC": "BTC-PERP"}}},
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
            if key.startswith("exchanges.hyperliquid."):
                prop = key.split(".")[-1]
                ex_data = mock_single_config_dict.get("exchanges", {}).get("hyperliquid", {})
                result: object | None = ex_data.get(prop, default)
                return result
            if key.startswith("exchanges.") and key.endswith(".enabled"):
                # For single exchange test, only hyperliquid should be enabled
                return key == "exchanges.hyperliquid.enabled"
            final_result: object | None = mock_single_config_dict.get(key, default)
            return final_result

        config.get.side_effect = single_exchange_config_get
        # Re-initialize by creating a new signal generator instance with the updated config
        signal_generator = SignalGenerator(config, data_handler, symbol_mapper)

        mock_funding_data = {
            "BTC": {
                "hyperliquid": data_handler.get_latest_funding_rate("hyperliquid", "BTC-PERP"),
            },
        }

        # Ensure data_handler.tickers is appropriately set for the single exchange scenario
        # The main data_handler fixture sets up tickers for both. For this specific test,
        # it might be cleaner to re-mock data_handler.tickers if precise control is needed,
        # but SignalGenerator should correctly filter based on enabled exchanges from config.
        # The existing fixture setup for data_handler.tickers should be fine as SignalGenerator
        # will only look for 'hyperliquid' tickers due to the mocked config.

        opportunities = await signal_generator.generate_arbitrage_opportunities(
            funding_data=mock_funding_data,
        )
        assert len(opportunities) == 0

    @pytest.mark.asyncio
    async def test_arbitrage_opportunity_creation(self, signal_generator: SignalGenerator) -> None:
        """Test the internal creation logic for ArbitrageOpportunity."""
        now = datetime.now(UTC)
        _funding_data = {
            "hyperliquid": FundingRate(
                symbol="BTC",
                funding_rate=Decimal("-0.001"),
                timestamp=now,
            ),
            "backpack": FundingRate(
                symbol="BTC-USDC",
                funding_rate=Decimal("0.002"),
                timestamp=now,
            ),
        }
        # Ticker data for test
        _ticker_data: dict[str, Ticker] = {
            "hyperliquid": Ticker(
                symbol="BTC",
                price=Decimal("41000"),
                bid=Decimal("40999"),
                ask=Decimal("41001"),
                timestamp=now,
            ),
            "backpack": Ticker(
                symbol="BTC-USDC",
                price=Decimal("41100"),
                bid=Decimal("41099"),
                ask=Decimal("41101"),
                timestamp=now,
            ),
        }

        # Test opportunity detection through the public signal generation interface
        # Add the mock data to the data handler and generate signals
        # Note: mock_funding_data and ticker_data are defined above but not used
        # This is a mock test that simulates the signal generation interface

        # Generate opportunities directly
        opportunities = await signal_generator.generate_arbitrage_opportunities({})

        assert len(opportunities) == 0  # Empty funding data should return no opportunities

        # Now test with proper funding data structure
        funding_data: dict[str, dict[str, FundingRate | None]] = {
            "BTC": {
                "hyperliquid": _funding_data["hyperliquid"],
                "backpack": _funding_data["backpack"],
            }
        }

        # Need to mock the ticker data in the data handler
        signal_generator.data_handler.tickers = {
            "hyperliquid": {"BTC": _ticker_data["hyperliquid"]},
            "backpack": {"BTC-USDC": _ticker_data["backpack"]},
        }

        opportunities = await signal_generator.generate_arbitrage_opportunities(funding_data)

        assert len(opportunities) == 1
        opp = opportunities[0]
        assert isinstance(opp, ArbitrageOpportunity)
        assert opp.symbol == "BTC"
        assert opp.long_exchange == "backpack"
        assert opp.short_exchange == "hyperliquid"
        # Assert expected_profit, not net_funding_differential directly
        # Based on test inputs (price_a=41000, rate_a=-0.001, price_b=41100, rate_b=0.002):
        # net_funding_usd = (41100 * 0.002) - (41000 * -0.001) = 82.2 - (-41) = 123.2
        # slippage = (0.001 * 0.5) + (0.001 * 0.5) = 0.001 (using default config)
        # expected_profit = 123.2 - 0.001 = 123.199
        correct_expected_profit = Decimal("123.199")
        assert opp.expected_profit == correct_expected_profit


def assert_decimal_approx(
    actual: Decimal,
    expected: Decimal,
    tol: Decimal = Decimal("1e-6"),
) -> None:
    """Assert that two Decimal values are approximately equal within a given tolerance.

    Args:
        actual (Decimal): The actual value.
        expected (Decimal): The expected value.
        tol (Decimal): The allowed tolerance (default: 1e-6).

    Raises:
        AssertionError: If the values differ by more than tol.

    """
    assert abs(actual - expected) <= tol, f"{actual} != {expected} within {tol}"
