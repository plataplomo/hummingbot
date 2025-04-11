import logging
from datetime import datetime, timedelta
from decimal import Decimal, getcontext  # Import Decimal
from typing import Any

import numpy as np

from cyberdelta.core.data_handler import DataHandler
from cyberdelta.utils.config import Config

logger = logging.getLogger(__name__)

# Set precision for Decimal
getcontext().prec = 28


class ArbitrageOpportunity:
    """
    Represents a funding rate arbitrage opportunity between two exchanges.
    """

    def __init__(
        self,
        symbol: str,
        long_exchange: str,
        short_exchange: str,
        long_funding_rate: Decimal,  # Changed to Decimal
        short_funding_rate: Decimal,  # Changed to Decimal
        net_funding_differential: Decimal,  # Changed to Decimal
        timestamp: datetime,
        expected_profit: Decimal,  # Changed to Decimal
        utility_score: float,  # Keep float
        basis_volatility: float,
    ):  # Keep float
        """
        Initialize an arbitrage opportunity.

        Args:
            symbol: Trading symbol
            long_exchange: Exchange to take long position
            short_exchange: Exchange to take short position
            long_funding_rate: Funding rate on long exchange (Decimal)
            short_funding_rate: Funding rate on short exchange (Decimal)
            net_funding_differential: Difference between funding rates (Decimal)
            timestamp: When the opportunity was identified
            expected_profit: Expected profit after costs (Decimal)
            utility_score: Ranking score considering profit and risk
            basis_volatility: Volatility of the basis between exchanges
        """
        self.symbol = symbol
        self.long_exchange = long_exchange
        self.short_exchange = short_exchange
        self.long_funding_rate = long_funding_rate
        self.short_funding_rate = short_funding_rate
        self.net_funding_differential = net_funding_differential
        self.timestamp = timestamp
        self.expected_profit = expected_profit
        self.utility_score = utility_score
        self.basis_volatility = basis_volatility

        # Add attributes expected by RiskManager Kelly calc if not already present
        # These might be better set during sizing, but ensure they exist
        self.long_entry_price: Decimal | None = None
        self.short_entry_price: Decimal | None = None

    def __str__(self) -> str:
        """String representation of the opportunity."""
        # Format Decimal rates as percentages with higher precision if needed
        return (
            f"ArbitrageOpportunity: {self.symbol} - "
            f"Long: {self.long_exchange} ({self.long_funding_rate * 100:.6f}%), "
            f"Short: {self.short_exchange} ({self.short_funding_rate * 100:.6f}%), "
            f"NFD: {self.net_funding_differential * 100:.6f}%, "
            f"ExpProfit: ${self.expected_profit:.4f}, "  # Increased precision for profit
            f"Utility: {self.utility_score:.4f}"
        )


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
                    if not isinstance(rate, Decimal):
                        logger.warning(
                            f"Funding rate for {exchange}/{exchange_symbol} is not Decimal: {rate}. Converting."
                        )
                        rate = Decimal(str(rate))

                    # Add to historical data
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
                if not isinstance(price1, Decimal):
                    price1 = Decimal(str(price1))
                if not isinstance(price2, Decimal):
                    price2 = Decimal(str(price2))

                # Calculate basis (price differential)
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

    def calculate_basis_volatility(self, symbol: str) -> float:
        """
        Calculate the standard deviation of the basis for a symbol.
        Basis values are stored as Decimal, but std deviation naturally returns float.

        Args:
            symbol: Trading symbol (internal)

        Returns:
            Basis volatility (float) or 0.0 if insufficient data
        """
        if symbol not in self.historical_basis or len(self.historical_basis[symbol]) < 2:
            return 0.0

        # Extract basis values (Decimal) from historical data, convert to float for numpy
        basis_values_float = [float(b[1]) for b in self.historical_basis[symbol]]

        # Calculate standard deviation using numpy (returns float)
        volatility = np.std(basis_values_float)
        return float(volatility)  # Ensure return type is float

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

    def generate_opportunities(self) -> list[ArbitrageOpportunity]:
        """
        Generate a list of arbitrage opportunities sorted by utility score.

        Returns:
            List of arbitrage opportunities
        """
        opportunities = []
        now = datetime.now()

        # Get all configured exchanges
        exchanges = []
        exchange_symbol_map: dict[str, dict[str, str]] = {}
        for exchange_id in self.config.get("exchanges", {}).keys():
            if self.config.get(f"exchanges.{exchange_id}.enabled", False):
                exchanges.append(exchange_id)
                exchange_symbol_map[exchange_id] = self.config.get(
                    f"exchanges.{exchange_id}.symbols", {}
                )

        # Need at least two exchanges for arbitrage
        if len(exchanges) < 2:
            logger.warning("Fewer than 2 exchanges configured. Arbitrage not possible.")
            return []

        # Get common internal symbols across exchanges
        common_internal_symbols = set()
        first_exchange = True
        for exchange in exchanges:
            internal_symbols = set(exchange_symbol_map.get(exchange, {}).keys())
            if first_exchange:
                common_internal_symbols = internal_symbols
                first_exchange = False
            else:
                common_internal_symbols.intersection_update(internal_symbols)

        logger.debug(
            f"Found {len(common_internal_symbols)} common internal symbols across "
            f"configured exchanges: {common_internal_symbols}"
        )

        # Generate opportunities for each common symbol
        for internal_symbol in common_internal_symbols:
            # Get exchanges that support this internal symbol
            supporting_exchanges = [
                ex for ex in exchanges if internal_symbol in exchange_symbol_map.get(ex, {})
            ]
            if len(supporting_exchanges) < 2:
                continue  # Need at least two supporting exchanges

            for i, ex1 in enumerate(supporting_exchanges):
                for ex2 in supporting_exchanges[i + 1 :]:
                    # Get exchange-specific symbols
                    sym1 = exchange_symbol_map[ex1][internal_symbol]
                    sym2 = exchange_symbol_map[ex2][internal_symbol]

                    # Get funding rates (should be Decimal)
                    rate_data1 = self.data_handler.get_funding_rate(ex1, sym1)
                    rate_data2 = self.data_handler.get_funding_rate(ex2, sym2)

                    if not rate_data1 or not rate_data2:
                        logger.debug(
                            f"Missing funding rate for {internal_symbol} on {ex1} or {ex2}"
                        )
                        continue

                    rate1, timestamp1 = rate_data1
                    rate2, timestamp2 = rate_data2

                    # Ensure rates are Decimal
                    if not isinstance(rate1, Decimal):
                        rate1 = Decimal(str(rate1))
                    if not isinstance(rate2, Decimal):
                        rate2 = Decimal(str(rate2))

                    # Determine long/short sides based on rates
                    long_exchange, short_exchange = (ex1, ex2) if rate1 < rate2 else (ex2, ex1)
                    long_sym, short_sym = (sym1, sym2) if rate1 < rate2 else (sym2, sym1)
                    long_rate, short_rate = (rate1, rate2) if rate1 < rate2 else (rate2, rate1)

                    # Calculate NFD (Decimal)
                    nfd = short_rate - long_rate

                    logger.debug(
                        f"Opportunity Check for {internal_symbol}: "
                        f"Long ({long_exchange} vs {short_exchange}), "
                        f"L_Sym={long_sym}, S_Sym={short_sym}, L_Rate={long_rate:.6f}, "
                        f"S_Rate={short_rate:.6f}, NFD={nfd:.6f}, Min_NFD={self.min_funding_differential}"
                    )

                    # Check if NFD exceeds minimum threshold (Decimal comparison)
                    if nfd < self.min_funding_differential:
                        continue

                    # Basis volatility calculation (returns float)
                    basis_volatility = self.calculate_basis_volatility(internal_symbol)

                    # --- Calculate Costs (using Decimal) ---
                    position_size = Decimal("1000.0")  # Reference size for cost estimation

                    # Estimate slippage (returns Decimal %)
                    long_slippage = self.estimate_slippage(long_sym, position_size, long_exchange)
                    short_slippage = self.estimate_slippage(
                        short_sym, position_size, short_exchange
                    )

                    # Get exchange fee rates from config (convert to Decimal)
                    long_fee_rate = Decimal(
                        str(self.config.get(f"exchanges.{long_exchange}.fee_rate", 0.0005))
                    )
                    short_fee_rate = Decimal(
                        str(self.config.get(f"exchanges.{short_exchange}.fee_rate", 0.0005))
                    )

                    # Calculate total costs (Decimal)
                    # Costs = Size * (Slippage_L + Slippage_S + Fee_L + Fee_S)
                    total_costs = position_size * (
                        long_slippage + short_slippage + long_fee_rate + short_fee_rate
                    )

                    # Calculate expected profit based on NFD, adjusted for costs (Decimal)
                    # Profit = (Size * NFD) - Costs
                    expected_profit = (position_size * nfd) - total_costs

                    # Calculate expected profit (after costs)
                    if expected_profit < self.min_profit_threshold:
                        logger.debug(
                            f"Opportunity ({internal_symbol} - {long_exchange} vs {short_exchange}) "
                            f"below profit threshold: ${expected_profit:.4f} < ${self.min_profit_threshold}"
                        )
                        continue

                    # Calculate utility score (remains float based)
                    # Convert basis_volatility (float) to Decimal for the calculation
                    basis_volatility_dec = Decimal(str(basis_volatility))
                    utility_score_denominator = basis_volatility_dec + Decimal(
                        "1e-9"
                    )  # Avoid division by zero

                    # Perform division using Decimal
                    utility_score_dec = (
                        expected_profit / utility_score_denominator
                        if utility_score_denominator > Decimal("0")
                        else Decimal("0")
                    )

                    # Apply risk aversion (float) and convert final score to float
                    utility_score_float = float(
                        utility_score_dec * (Decimal("1.0") - Decimal(str(self.risk_aversion)))
                    )

                    # Fetch current ticker prices to store in opportunity (for RiskManager)
                    ticker_long = self.data_handler.get_ticker(long_exchange, long_sym)
                    ticker_short = self.data_handler.get_ticker(short_exchange, short_sym)
                    long_entry_p = ticker_long.ask if ticker_long else None
                    short_entry_p = ticker_short.bid if ticker_short else None
                    if long_entry_p and not isinstance(long_entry_p, Decimal):
                        long_entry_p = Decimal(str(long_entry_p))
                    if short_entry_p and not isinstance(short_entry_p, Decimal):
                        short_entry_p = Decimal(str(short_entry_p))

                    # Create ArbitrageOpportunity object
                    opportunity = ArbitrageOpportunity(
                        symbol=internal_symbol,
                        long_exchange=long_exchange,
                        short_exchange=short_exchange,
                        long_funding_rate=long_rate,  # Decimal
                        short_funding_rate=short_rate,  # Decimal
                        net_funding_differential=nfd,  # Decimal
                        timestamp=now,
                        expected_profit=expected_profit,  # Decimal
                        utility_score=utility_score_float,  # Float
                        basis_volatility=basis_volatility,  # Float
                    )
                    # Assign prices after creation
                    opportunity.long_entry_price = long_entry_p
                    opportunity.short_entry_price = short_entry_p

                    opportunities.append(opportunity)
                    logger.info(f"Generated opportunity: {opportunity}")

        # Sort opportunities by utility score (descending)
        opportunities.sort(key=lambda o: o.utility_score, reverse=True)

        return opportunities
