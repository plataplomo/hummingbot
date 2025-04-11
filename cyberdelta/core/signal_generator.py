from __future__ import annotations # Enable postponed evaluation

import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal, getcontext  # Import Decimal
from typing import TYPE_CHECKING, Any # Added TYPE_CHECKING

import numpy as np
import decimal

from cyberdelta.core.data_handler import DataHandler
# from cyberdelta.core.models import ( # Moved below
#     ArbitrageOpportunity,
# )
from cyberdelta.utils.config import Config

if TYPE_CHECKING:
    from cyberdelta.core.models import ArbitrageOpportunity, MarketData, TradeSignal


logger = logging.getLogger(__name__)

# Set precision for Decimal
getcontext().prec = 28


class SignalGenerator:
    """
    Identifies funding rate arbitrage opportunities between exchanges.

    Responsible for:
    - Monitoring funding rates across exchanges
    - Calculating Net Funding Differential (NFD)
    - Computing expected profit metrics including costs
    - Generating and ranking arbitrage opportunities
    """

    def __init__(self, config: Config, data_handler: DataHandler) -> None:
        """
        Initialize the signal generator.

        Args:
            config: Application configuration
            data_handler: DataHandler for market data
        """
        self.config = config
        self.data_handler = data_handler

        # Parameters from config - ensure Decimal where appropriate
        self.min_funding_differential = Decimal(
            str(config.get("strategy.funding_rate.min_funding_differential", 0.0001))
        )
        self.min_profit_threshold = Decimal(
            str(config.get("strategy.funding_rate.min_profit_threshold", 5.0))
        )

        # Historical funding rates for volatility calculation
        # exchange -> symbol -> List[(timestamp, rate: Decimal)]
        self.historical_funding_rates: dict[str, dict[str, list[tuple[datetime, Decimal]]]] = {}

        # Historical basis data for volatility calculation
        # symbol -> List[(timestamp, basis: Decimal)]
        self.historical_basis: dict[str, list[tuple[datetime, Decimal]]] = {}

        # Sample period and count for funding rate history
        self.funding_sample_period = config.get(
            "strategy.funding_rate.funding_sample_period", 3600
        )  # seconds
        self.funding_sample_count = config.get("strategy.funding_rate.funding_sample_count", 24)

        # Risk aversion parameter (λ) for utility function (float is fine here)
        self.risk_aversion = config.get("strategy.funding_rate.risk_aversion", 1.0)

        # Initialize data structures
        self._initialize_data_structures()

    def _initialize_data_structures(self) -> None:
        """Initialize data structures for historical data."""
        # Get all configured exchanges and symbols
        exchanges = []
        for exchange_id in self.config.get("exchanges", {}).keys():
            if self.config.get(f"exchanges.{exchange_id}.enabled", False):
                exchanges.append(exchange_id)

        # Initialize historical funding rate storage
        for exchange in exchanges:
            self.historical_funding_rates[exchange] = {}
            # Get the symbol map {InternalSymbol: ExchangeSymbol}
            symbol_map = self.config.get(f"exchanges.{exchange}.symbols", {})
            for internal_symbol, exchange_symbol in symbol_map.items():
                # Use exchange_symbol for the key in historical rates
                self.historical_funding_rates[exchange][exchange_symbol] = []

                # Initialize basis history using the internal symbol
                if internal_symbol not in self.historical_basis:
                    self.historical_basis[internal_symbol] = []

    def update_historical_data(self) -> None:
        """
        Update historical funding rate and basis data with latest information.
        Should be called regularly to maintain up-to-date volatility calculations.
        """
        now = datetime.now()

        # Get all configured exchanges and symbols
        exchanges = []
        all_internal_symbols = set()
        exchange_symbol_map: dict[str, dict[str, str]] = {}

        for exchange_id in self.config.get("exchanges", {}).keys():
            if self.config.get(f"exchanges.{exchange_id}.enabled", False):
                exchanges.append(exchange_id)
                symbols_config = self.config.get(f"exchanges.{exchange_id}.symbols", {})
                exchange_symbol_map[exchange_id] = symbols_config
                all_internal_symbols.update(symbols_config.keys())

        # Update funding rate history
        for exchange in exchanges:
            if exchange not in self.historical_funding_rates:
                continue  # Skip if not initialized
            for exchange_symbol in self.historical_funding_rates[exchange].keys():
                # Fetch data using the exchange-specific symbol
                funding_data = self.data_handler.get_funding_rate(exchange, exchange_symbol)
                if funding_data:
                    rate, timestamp = funding_data  # Assuming rate is Decimal
                    # Ensure rate is not None before attempting conversion
                    if rate is not None and not isinstance(rate, Decimal):
                        logger.warning(
                            f"Funding rate for {exchange}/{exchange_symbol} is not Decimal: "
                            f"{rate}. Converting."
                        )
                        try:
                            rate = Decimal(str(rate))
                        except decimal.InvalidOperation:
                            logger.error(f"Could not convert funding rate '{rate}' to Decimal.")
                            continue # Skip this update if conversion fails
                    # Add to historical data (only if rate is valid Decimal)
                    if isinstance(rate, Decimal):
                        history = self.historical_funding_rates[exchange][exchange_symbol]
                        history.append((timestamp, rate))

                    # Trim to keep only recent samples
                    cutoff_time = now - timedelta(
                        seconds=self.funding_sample_period * self.funding_sample_count
                    )
                    while history and history[0][0] < cutoff_time:
                        history.pop(0)

        # Update basis history (price difference between exchanges)
        for internal_symbol in all_internal_symbols:
            # Find exchanges that map this internal symbol
            valid_exchanges_for_symbol = []
            exchange_tickers: dict[str, Any] = {}
            for exchange in exchanges:
                if internal_symbol in exchange_symbol_map.get(exchange, {}):
                    exchange_symbol = exchange_symbol_map[exchange][internal_symbol]
                    ticker = self.data_handler.get_ticker(exchange, exchange_symbol)
                    if ticker and ticker.price is not None:
                        valid_exchanges_for_symbol.append(exchange)
                        exchange_tickers[exchange] = ticker  # Store the valid ticker

            if len(valid_exchanges_for_symbol) >= 2:
                # Calculate basis between the first two valid exchanges
                exchange1, exchange2 = (
                    valid_exchanges_for_symbol[0],
                    valid_exchanges_for_symbol[1],
                )
                ticker1 = exchange_tickers[exchange1]
                ticker2 = exchange_tickers[exchange2]

                # Tickers should have Decimal prices
                price1 = ticker1.price
                price2 = ticker2.price
                # Ensure prices are not None before conversion
                if price1 is not None and not isinstance(price1, Decimal):
                    try:
                        price1 = Decimal(str(price1))
                    except decimal.InvalidOperation:
                        logger.error(f"Could not convert ticker price '{price1}' to Decimal for {exchange1}/{internal_symbol}")
                        price1 = None # Set to None if conversion fails
                if price2 is not None and not isinstance(price2, Decimal):
                    try:
                        price2 = Decimal(str(price2))
                    except decimal.InvalidOperation:
                        logger.error(f"Could not convert ticker price '{price2}' to Decimal for {exchange2}/{internal_symbol}")
                        price2 = None # Set to None if conversion fails

                # Calculate basis (price differential) only if both prices are valid Decimals
                if isinstance(price1, Decimal) and isinstance(price2, Decimal):
                    basis = price1 - price2  # Decimal - Decimal = Decimal

                    # Add to historical data (using internal symbol key)
                    if internal_symbol not in self.historical_basis:
                        self.historical_basis[internal_symbol] = []

                    self.historical_basis[internal_symbol].append((now, basis))

                    # Trim to keep only recent samples
                    cutoff_time = now - timedelta(
                        seconds=self.funding_sample_period * self.funding_sample_count
                    )
                    while (
                        self.historical_basis[internal_symbol]
                        and self.historical_basis[internal_symbol][0][0] < cutoff_time
                    ):
                        self.historical_basis[internal_symbol].pop(0)

    def calculate_funding_rate_volatility(self, exchange: str, symbol: str) -> Decimal:
        """Calculates the volatility of historical funding rates."""
        history = self.historical_funding_rates.get(exchange, {}).get(symbol, [])
        if len(history) < 2:
            return Decimal("0.0")

        # Extract Decimal rates for calculation
        rates = [rate for _, rate in history]
        # Ensure rates are float for numpy calculation, then convert result back to Decimal
        if not rates:
            return Decimal("0.0")
        # Convert list of Decimals to numpy array of floats for std calculation
        rates_float = np.array([float(r) for r in rates])
        volatility_float = np.std(rates_float)
        return Decimal(str(volatility_float))

    def calculate_basis_volatility(self, symbol: str) -> Decimal:
        """
        Calculate the standard deviation of the basis for a symbol.
        Returns Decimal.
        """
        if symbol not in self.historical_basis or len(self.historical_basis[symbol]) < 2:
            return Decimal("0.0")

        # Basis values are already stored as Decimal
        basis_values = [b[1] for b in self.historical_basis[symbol]]

        # Calculate standard deviation (convert to float for numpy, back to Decimal)
        basis_values_float = np.array([float(b) for b in basis_values])
        volatility_float = np.std(basis_values_float)
        return Decimal(str(volatility_float))

    def estimate_slippage(self, symbol: str, order_size: Decimal, exchange: str) -> Decimal:
        """
        Estimate slippage based on order size and liquidity.
        Returns Decimal percentage.

        Args:
            symbol: Trading symbol (exchange-specific)
            order_size: Size of the order in USD (Decimal)
            exchange: Exchange identifier

        Returns:
            Estimated slippage as a Decimal percentage (e.g., 0.001 for 0.1%)
        """
        # Get orderbook from data handler
        orderbook = self.data_handler.get_orderbook(exchange, symbol)
        if not orderbook:
            # Default to a conservative estimate if no orderbook data
            return Decimal("0.001")  # 0.1% default slippage

        # Extract available liquidity from orderbook (assume float, convert to Decimal)
        # This is simplified; real implementation needs bid/ask analysis.
        available_depth_float = orderbook.get("depth", 100000.0)
        available_depth = Decimal(str(available_depth_float))

        # Convert available depth to Decimal for comparison
        available_depth_dec = Decimal(str(available_depth))
        if available_depth_dec <= Decimal("0"):
            logger.warning(
                f"Available depth is zero or negative for {exchange}/{symbol}. "
                f"Using default slippage."
            )
            return Decimal("0.001")

        # Calculate slippage factor (e.g., inverse relationship with depth)
        scaling_factor = Decimal("0.1")  # β parameter as Decimal
        slippage = scaling_factor * (order_size / available_depth)  # Decimal calculation

        # Cap the slippage at reasonable limits (Decimal)
        return min(slippage, Decimal("0.01"))  # Max 1% slippage

    def generate_opportunities(self) -> list["ArbitrageOpportunity"]:
        """
        Generate a list of arbitrage opportunities sorted by utility score.
        """
        opportunities = []
        processed_pairs = set()  # Keep track of processed pairs to avoid duplicates

        # Get configured exchanges and symbols
        exchanges = []
        for exchange_id in self.config.get("exchanges", {}).keys():
            if self.config.get(f"exchanges.{exchange_id}.enabled", False):
                exchanges.append(exchange_id)

        if len(exchanges) < 2:
            logger.info("Need at least two enabled exchanges to generate funding rate opportunities.")
            return [] # Return early if not enough exchanges

        for i in range(len(exchanges)):
            for j in range(i + 1, len(exchanges)):
                exchange1 = exchanges[i]
                exchange2 = exchanges[j]

                # Calculate common symbols based on config
                symbols1 = set(self.config.get(f"exchanges.{exchange1}.symbols", {}).keys())
                symbols2 = set(self.config.get(f"exchanges.{exchange2}.symbols", {}).keys())
                common_symbols = list(symbols1.intersection(symbols2))

                if not common_symbols:
                    continue # Skip if no common symbols

                for symbol in common_symbols:
                    # Avoid processing the same pair twice (e.g., A-B vs B-A)
                    pair_key = tuple(sorted((exchange1, exchange2))) + (symbol,)
                    if pair_key in processed_pairs:
                        continue

                    # Get latest data
                    funding1 = self.data_handler.get_funding_rate(exchange1, symbol)
                    funding2 = self.data_handler.get_funding_rate(exchange2, symbol)
                    ticker1 = self.data_handler.get_ticker(exchange1, symbol)
                    ticker2 = self.data_handler.get_ticker(exchange2, symbol)

                    if not all([funding1, funding2, ticker1, ticker2]):
                        # logger.debug(f"Missing data for {symbol} on {exchange1}/{exchange2}")
                        continue

                    # Ensure correct types (Decimal)
                    rate1 = funding1.funding_rate  # Already Decimal from model
                    rate2 = funding2.funding_rate  # Already Decimal from model
                    ask1 = ticker1.ask  # Already Decimal from model
                    bid1 = ticker1.bid  # Already Decimal from model
                    ask2 = ticker2.ask  # Already Decimal from model
                    bid2 = ticker2.bid  # Already Decimal from model

                    now = datetime.now(UTC)
                    basis_volatility = self.calculate_basis_volatility(symbol)  # Expects Decimal

                    # Opportunity 1: Long on 1, Short on 2
                    if rate1 < rate2:
                        net_differential = rate2 - rate1  # Decimal math
                        if net_differential > self.min_funding_differential:
                            try:
                                # Calculate estimated profit and utility
                                optimal_size_placeholder = Decimal("1000")  # Placeholder USD size
                                expected_profit = (
                                    net_differential * optimal_size_placeholder
                                )  # Simplified
                                # Utility calculation requires floats
                                utility_score = float(expected_profit) - self.risk_aversion * (
                                    float(basis_volatility) ** 2
                                )

                                opportunity = ArbitrageOpportunity(
                                    symbol=symbol,
                                    long_exchange=exchange1,
                                    short_exchange=exchange2,
                                    long_price=ask1,  # Buy at Ask 1
                                    short_price=bid2,  # Sell at Bid 2
                                    long_funding_rate=rate1,
                                    short_funding_rate=rate2,
                                    net_funding_differential=net_differential,
                                    timestamp=now,
                                    # Optional fields
                                    optimal_size=optimal_size_placeholder,
                                    expected_profit=expected_profit,
                                    confidence=None,  # Placeholder confidence
                                    # Store as float if needed by model
                                    basis_volatility=float(basis_volatility),
                                    utility_score=utility_score,
                                )
                                opportunities.append(opportunity)
                            except Exception as e:
                                self.logger.error(
                                    "Error creating opportunity (1)",
                                    symbol=symbol,
                                    error=e,
                                )

                    # Opportunity 2: Long on 2, Short on 1
                    elif rate2 < rate1:
                        net_differential = rate1 - rate2  # Decimal math
                        if net_differential > self.min_funding_differential:
                            try:
                                # Calculate estimated profit and utility
                                optimal_size_placeholder = Decimal("1000")  # Placeholder USD size
                                expected_profit = (
                                    net_differential * optimal_size_placeholder
                                )  # Simplified
                                utility_score = float(expected_profit) - self.risk_aversion * (
                                    float(basis_volatility) ** 2
                                )

                                opportunity = ArbitrageOpportunity(
                                    symbol=symbol,
                                    long_exchange=exchange2,
                                    short_exchange=exchange1,
                                    long_price=ask2,  # Buy at Ask 2
                                    short_price=bid1,  # Sell at Bid 1
                                    long_funding_rate=rate2,
                                    short_funding_rate=rate1,
                                    net_funding_differential=net_differential,
                                    timestamp=now,
                                    # Optional fields
                                    optimal_size=optimal_size_placeholder,
                                    expected_profit=expected_profit,
                                    confidence=None,  # Placeholder confidence
                                    # Store as float if needed by model
                                    basis_volatility=float(basis_volatility),
                                    utility_score=utility_score,
                                )
                                opportunities.append(opportunity)
                            except Exception as e:
                                self.logger.error(
                                    "Error creating opportunity (2)",
                                    symbol=symbol,
                                    error=e,
                                )

                    processed_pairs.add(pair_key)

        # Sort opportunities by utility score (handle None safely)
        opportunities.sort(
            key=lambda x: x.utility_score if x.utility_score is not None else -float("inf"),
            reverse=True,
        )

        return opportunities
