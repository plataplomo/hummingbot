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
    def config(self, test_app_settings: "AppSettings") -> MagicMock:
        """Create a mock config for testing using the new AppSettings."""
        mock_config = MagicMock()

        # Define side effect using nested function with type hints
        def config_get_side_effect(key: str, default: object | None = None) -> object | None:
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
                        elif prop == "symbols":
                            return exchange_config.symbols
                        elif prop == "fee_rate":
                            return "0.0004" if exchange == "hyperliquid" else "0.0006"
                    return default
                elif base == "strategy":
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

        # Use exchange-specific symbols
        funding_rates: dict[str, dict[str, FundingRate]] = {
            "hyperliquid": {
                "BTC-PERP": FundingRate(
                    symbol="BTC-PERP",
                    funding_rate=Decimal("-0.001"),
                    mark_price=Decimal("30000"),
                    timestamp=now,
                ),
                "ETH-PERP": FundingRate(
                    symbol="ETH-PERP",
                    funding_rate=Decimal("0.005"),
                    mark_price=Decimal("2000"),
                    timestamp=now,
                ),
            },
            "backpack": {
                "BTC_USDC": FundingRate(
                    symbol="BTC_USDC",
                    funding_rate=Decimal("0.002"),
                    mark_price=Decimal("30010"),
                    timestamp=now,
                ),
                "ETH_USDC": FundingRate(
                    symbol="ETH_USDC",
                    funding_rate=Decimal("-0.01"),
                    mark_price=Decimal("2005"),
                    timestamp=now,
                ),
            },
        }

        # Mock Ticker data for data_handler.tickers
        mock_hl_btc_ticker = Ticker(
            symbol="BTC-PERP",
            price=Decimal("30000"),
            bid=Decimal("29999"),
            ask=Decimal("30001"),
            timestamp=now,
        )
        mock_bp_btc_ticker = Ticker(
            symbol="BTC_USDC",
            price=Decimal("30010"),
            bid=Decimal("30009"),
            ask=Decimal("30011"),
            timestamp=now,
        )
        mock_hl_eth_ticker = Ticker(
            symbol="ETH-PERP",
            price=Decimal("2000"),
            bid=Decimal("1999"),
            ask=Decimal("2001"),
            timestamp=now,
        )
        mock_bp_eth_ticker = Ticker(
            symbol="ETH_USDC",
            price=Decimal("2005"),
            bid=Decimal("2004"),
            ask=Decimal("2006"),
            timestamp=now,
        )

        handler.tickers = {
            "hyperliquid": {
                "BTC-PERP": mock_hl_btc_ticker,
                "ETH-PERP": mock_hl_eth_ticker,
            },
            "backpack": {
                "BTC_USDC": mock_bp_btc_ticker,
                "ETH_USDC": mock_bp_eth_ticker,
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
            "hyperliquid": {"BTC-PERP": mock_orderbook, "ETH-PERP": mock_orderbook},
            "backpack": {"BTC_USDC": mock_orderbook, "ETH_USDC": mock_orderbook},
        }

        def get_funding_rate_side_effect(exchange: str, symbol: str) -> FundingRate | None:
            return funding_rates.get(exchange, {}).get(symbol)

        # get_ticker side effect is no longer directly used by SignalGenerator for opportunities,
        # as it uses handler.tickers. However, other parts of tests might still use it.
        # For safety, ensure it returns Ticker if something still calls it.
        def get_ticker_side_effect(exchange: str, symbol: str) -> Ticker | None:
            ticker = handler.tickers.get(exchange, {}).get(symbol)
            return ticker if ticker is None or isinstance(ticker, Ticker) else None

        def get_orderbook_side_effect(exchange: str, symbol: str) -> OrderBook | None:
            return orderbooks.get(exchange, {}).get(symbol)

        handler.get_latest_funding_rate.side_effect = get_funding_rate_side_effect
        handler.get_latest_ticker.side_effect = get_ticker_side_effect
        handler.get_latest_order_book.side_effect = get_orderbook_side_effect

        return handler

    @pytest.fixture
    def symbol_mapper(self, config: MagicMock) -> SymbolMapper:
        """Fixture for a SymbolMapper using the mock config."""
        exchanges_map_for_mapper: dict[str, Any] = {}
        exchanges_config_from_main = config.get("exchanges")
        if exchanges_config_from_main and isinstance(exchanges_config_from_main, dict):
            for ex_id, exchange_details_any in exchanges_config_from_main.items():
                if isinstance(exchange_details_any, dict):
                    exchange_details: dict[str, Any] = exchange_details_any
                    # Check for enabled status directly from the already fetched exchange_details
                    enabled = exchange_details.get("enabled", False)
                    symbols = exchange_details.get("symbols", {})
                    if enabled and isinstance(symbols, dict):
                        # SymbolMapper expects a dict of exchange_id -> { "symbols": {...},
                        # ...other_keys_if_needed }
                        # We only need to pass the symbols map for each enabled exchange.
                        # The SymbolMapper itself will handle the structure if it gets the
                        # raw exchanges_config part.
                        # Let's simplify to pass the relevant part of exchanges_config_from_main
                        exchanges_map_for_mapper[ex_id] = (
                            exchange_details  # Pass the whole exchange detail if it has symbols
                        )
                    elif enabled:  # Enabled but no symbols dict
                        logger.warning(
                            f"Exchange {ex_id} enabled but no valid 'symbols' map found."
                        )
                else:
                    logger.warning(f"Exchange data for {ex_id} is not a dictionary.")
        # SymbolMapper expects a dictionary where keys are exchange_ids
        # and values are dictionaries containing at least a "symbols" map.
        return SymbolMapper(exchanges_map_for_mapper)

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
        assert signal_generator.app_settings == config
        assert signal_generator.data_handler == data_handler

    @patch("cyberdelta.core.signal_generator.datetime")
    def test_update_historical_data(
        self, mock_datetime: MagicMock, signal_generator: SignalGenerator, data_handler: MagicMock
    ) -> None:
        """Test updating historical funding rate and basis data using deque."""
        fixed_now = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
        mock_datetime.now.return_value = fixed_now

        signal_generator.update_historical_data()

        assert len(signal_generator.historical_funding_rates["hyperliquid"]["BTC"]) == 1
        assert len(signal_generator.historical_basis["BTC"]) == 1
        assert signal_generator.historical_funding_rates["hyperliquid"]["BTC"][0] == (
            fixed_now,
            Decimal("-0.001"),
        )
        btc_basis = Decimal("30000") - Decimal("30010")
        assert signal_generator.historical_basis["BTC"][0] == (fixed_now, btc_basis)

        data_handler.reset_mock()

        sample_count = signal_generator.funding_sample_count

        def get_funding_iter(
            exchange: str, symbol: str, rate_chg: Decimal = Decimal(0)
        ) -> FundingRate | None:
            base_rate = Decimal("-0.001") if exchange == "hyperliquid" else Decimal("0.002")
            return FundingRate(
                symbol=symbol, funding_rate=base_rate + rate_chg, timestamp=fixed_now
            )

        def get_ticker_iter(
            exchange: str, symbol: str, price_chg: Decimal = Decimal(0)
        ) -> Ticker | None:
            base_price = Decimal("30000") if exchange == "hyperliquid" else Decimal("30010")
            return Ticker(symbol=symbol, price=base_price + price_chg, timestamp=fixed_now)

        for i in range(sample_count + 5):
            rate_change = Decimal(str(i * 0.00001))
            price_change = Decimal(str(i * 5))

            # Update side effects to return slightly different data each time
            data_handler.get_latest_funding_rate.side_effect = (
                lambda ex, sym, r=rate_change: get_funding_iter(ex, sym, r)
            )
            data_handler.get_latest_ticker.side_effect = (
                lambda ex, sym, p=price_change: get_ticker_iter(ex, sym, p)
            )

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
            internal_symbol, deque()
        ).extend(zip(timestamps, rates, strict=False))  # Use extend

        # Expected volatility (sample std dev of 0.00010, 0.00012)
        expected_volatility_float = np.std([float(r) for r in rates], ddof=1)  # Use ddof=1
        expected_volatility = Decimal(str(expected_volatility_float))

        # Calculate volatility
        volatility = signal_generator.calculate_funding_rate_volatility(
            exchange_id, internal_symbol
        )

        # Assert the result
        assert isinstance(volatility, Decimal)  # Check type
        # Compare against the Decimal expected value calculated earlier
        assert_decimal_approx(volatility, expected_volatility)
        assert volatility > Decimal("0.0")  # Ensure not zero

        # Test insufficient data (should return default volatility)
        signal_generator.historical_funding_rates["test_ex"]["TEST_INSUFFICIENT"] = deque(
            [(now, Decimal("0.0001"))]
        )
        volatility_insufficient = signal_generator.calculate_funding_rate_volatility(
            "test_ex", "TEST_INSUFFICIENT"
        )
        assert volatility_insufficient == Decimal("0.0001")

        signal_generator.historical_basis["TEST"] = deque()  # Clear for next test

    def test_estimate_slippage(
        self, signal_generator: SignalGenerator, data_handler: MagicMock
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
        self, signal_generator: SignalGenerator, data_handler: MagicMock, config: MagicMock
    ) -> None:
        """Test generating arbitrage opportunities."""
        # now = datetime.now(UTC) # Unused variable
        # Prepare mock data arguments based on the method's needs
        # Mock funding_data structure: symbol -> exchange -> FundingRate | None
        mock_funding_data = {
            "BTC": {
                "hyperliquid": data_handler.get_latest_funding_rate("hyperliquid", "BTC-PERP"),
                "backpack": data_handler.get_latest_funding_rate("backpack", "BTC_USDC"),
            },
            "ETH": {
                "hyperliquid": data_handler.get_latest_funding_rate("hyperliquid", "ETH-PERP"),
                "backpack": data_handler.get_latest_funding_rate("backpack", "ETH_USDC"),
            },
        }
        # SignalGenerator now uses self.data_handler.tickers internally.
        # Ensure data_handler.tickers is populated correctly in the fixture or test setup.
        # The data_handler fixture already populates data_handler.tickers.

        opportunities = await signal_generator.generate_arbitrage_opportunities(
            funding_data=mock_funding_data
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
            "0"
        )  # Compare against the rate differential

    @pytest.mark.asyncio
    async def test_generate_opportunities_no_eligible(
        self, signal_generator: SignalGenerator, data_handler: MagicMock, config: MagicMock
    ) -> None:
        """Test when no opportunities meet the eligibility criteria."""
        now = datetime.now(UTC)  # This 'now' is used for mock_low_funding timestamps

        # Define side effect with type hints, adding mark_price and timestamp
        def mock_low_funding(exchange: str, symbol: str) -> FundingRate | None:
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
                    )
                },
                "backpack": {
                    bp_sym: FundingRate(
                        symbol=bp_sym,
                        funding_rate=Decimal("0.00002"),
                        mark_price=Decimal("30010"),
                        timestamp=now,
                    )
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
            }
        }

        opportunities = await signal_generator.generate_arbitrage_opportunities(
            funding_data=mock_funding_data
        )
        assert len(opportunities) == 0

    @pytest.mark.asyncio
    async def test_generate_opportunities_single_exchange(
        self, signal_generator: SignalGenerator, config: MagicMock, data_handler: MagicMock
    ) -> None:
        """Test scenario with only one exchange configured."""

        # now = datetime.now(UTC) # Unused variable
        # Define side effect with type hints
        def single_exchange_config_get(key: str, default: object | None = None) -> object | None:
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
            elif key.startswith("exchanges.") and key.endswith(".enabled"):
                # For single exchange test, only hyperliquid should be enabled
                return key == "exchanges.hyperliquid.enabled"
            final_result: object | None = mock_single_config_dict.get(key, default)
            return final_result

        config.get.side_effect = single_exchange_config_get
        # Re-initialize by creating a new signal generator instance with the updated config
        signal_generator = SignalGenerator(config)

        mock_funding_data = {
            "BTC": {
                "hyperliquid": data_handler.get_latest_funding_rate("hyperliquid", "BTC-PERP"),
            }
        }

        # Ensure data_handler.tickers is appropriately set for the single exchange scenario
        # The main data_handler fixture sets up tickers for both. For this specific test,
        # it might be cleaner to re-mock data_handler.tickers if precise control is needed,
        # but SignalGenerator should correctly filter based on enabled exchanges from config.
        # The existing fixture setup for data_handler.tickers should be fine as SignalGenerator
        # will only look for 'hyperliquid' tickers due to the mocked config.

        opportunities = await signal_generator.generate_arbitrage_opportunities(
            funding_data=mock_funding_data
        )
        assert len(opportunities) == 0

    def test_arbitrage_opportunity_creation(self, signal_generator: SignalGenerator) -> None:
        """Test the internal creation logic for ArbitrageOpportunity."""
        now = datetime.now(UTC)
        funding_data = {
            "hyperliquid": FundingRate(
                symbol="BTC-PERP",
                funding_rate=Decimal("-0.001"),
                timestamp=now,
            ),
            "backpack": FundingRate(
                symbol="BTC_USDC",
                funding_rate=Decimal("0.002"),
                timestamp=now,
            ),
        }
        # Ensure ticker_data matches the expected type dict[str, Ticker | None]
        ticker_data: dict[str, Ticker | None] = {
            "hyperliquid": Ticker(
                symbol="BTC-PERP",
                price=Decimal("41000"),
                bid=Decimal("40999"),
                ask=Decimal("41001"),
                timestamp=now,
            ),
            "backpack": Ticker(
                symbol="BTC_USDC",
                price=Decimal("41100"),
                bid=Decimal("41099"),
                ask=Decimal("41101"),
                timestamp=now,
            ),
        }

        # Test opportunity detection through the public signal generation interface
        # Add the mock data to the data handler and generate signals
        data_handler.funding_rates = mock_funding_data  # Add funding data
        
        # Generate signals which should internally detect opportunities
        signals = await signal_generator.generate_signals()
        
        # Extract opportunities from generated signals (signals should contain opportunity metadata)
        opportunities = []
        for signal in signals:
            if signal.metadata and "opportunity" in signal.metadata:
                opportunities.append(signal.metadata["opportunity"])

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
    actual: Decimal, expected: Decimal, tol: Decimal = Decimal("1e-6")
) -> None:
    """
    Assert that two Decimal values are approximately equal within a given tolerance.

    Args:
        actual (Decimal): The actual value.
        expected (Decimal): The expected value.
        tol (Decimal): The allowed tolerance (default: 1e-6).

    Raises:
        AssertionError: If the values differ by more than tol.
    """
    assert abs(actual - expected) <= tol, f"{actual} != {expected} within {tol}"
