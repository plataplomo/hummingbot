from __future__ import annotations

import asyncio
import logging
from datetime import datetime, UTC
from decimal import getcontext, Decimal
from typing import Any

from cyberdelta.core.data_handler import DataHandler
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import RiskManager, SizedOpportunity
from cyberdelta.core.models import ArbitrageOpportunity, MarketData, TradeSignal
from cyberdelta.core.strategy import Strategy

# Set precision for Decimal
getcontext().prec = 28

# Instantiate module-level logger
logger = logging.getLogger(__name__)


class FundingRateArbitrageStrategy(Strategy):
    """
    Implementation of a funding rate arbitrage strategy between exchanges.

    Primary approach for v0.0.1: Hyperliquid-Perp vs Backpack-Spot strategy
    This strategy:
    - Takes a position on Hyperliquid perpetual contracts
    - Hedges with opposite position in Backpack spot markets
    - Profits from funding rate payments while maintaining delta neutrality
    """

    def __init__( 
        self,
        name: str,
        symbol: str,
        data_handler: DataHandler,
        portfolio_tracker: PortfolioTracker,
        risk_manager: RiskManager | None = None,
        params: dict[str, Any] | None = None,
    ):
        """
        Initialize the funding rate arbitrage strategy.

        Args:
            name: Unique name for the strategy
            symbol: Trading symbol this strategy operates on (e.g., "BTC-PERP")
            data_handler: Data handler for market data access
            portfolio_tracker: Portfolio tracker for position management
            risk_manager: Risk manager for position sizing and risk controls
            params: Dictionary of strategy parameters
        """
        super().__init__(name, symbol, params)
        self.data_handler = data_handler
        self.portfolio_tracker = portfolio_tracker
        self.risk_manager = risk_manager

        # Load strategy parameters from config or use defaults
        self.min_funding_differential = self.get_param(
            "min_funding_differential", 0.0001
        )  # 0.01% minimum
        self.min_profit_threshold = self.get_param(
            "min_profit_threshold", 5.0
        )  # $5 minimum expected profit
        self.risk_aversion = self.get_param(
            "risk_aversion", 1.0
        )  # Risk aversion parameter for utility function
        self.rebalance_threshold = self.get_param(
            "rebalance_threshold", 0.05
        )  # 5% threshold for rebalancing

        # Strategy state
        self.last_opportunity_check: datetime | None = None
        self.active_opportunities: list[ArbitrageOpportunity] = []
        self.historical_basis: dict[str, list[tuple[datetime, Decimal]]] = {}
        self.check_interval: int = self.get_param("check_interval", 600)  # 10 minutes by default

        # Exchange mapping
        self.perp_exchange: str = self.get_param("perp_exchange", "hyperliquid")
        self.spot_exchange: str = self.get_param("spot_exchange", "backpack")

        # Market mapping (perp to spot)
        self.symbol_mapping: dict[str, str] = self.get_param("symbol_mapping", {})
        if not self.symbol_mapping:
            # Default mapping if not provided
            base = symbol.split("-")[0] if "-" in symbol else symbol.split("_")[0]
            self.symbol_mapping = {symbol: f"{base}_USDC"}

        # Position sizing info storage
        self.sized_opportunities: dict[str, SizedOpportunity] = {}

        logger.info(
            f"Initialized {self.name} strategy for {self.symbol} "
            f"between {self.perp_exchange} and {self.spot_exchange}"
        )

    async def _check_opportunity(self) -> ArbitrageOpportunity | None:
        """
        Check for funding rate arbitrage opportunity between perp and spot markets.

        Returns:
            ArbitrageOpportunity if found, None otherwise
        """
        # Get funding rate from perp exchange
        funding_rate = await self.data_handler.get_funding_rate(self.perp_exchange, self.symbol)
        if not funding_rate:
            logger.warning(f"Could not get funding rate for {self.symbol} on {self.perp_exchange}")
            return None

        # Get current time
        now = datetime.now(UTC)

        # Get prices for basis calculation
        perp_ticker = await self.data_handler.get_ticker(self.perp_exchange, self.symbol)
        spot_symbol = self.symbol_mapping.get(self.symbol)
        spot_ticker = await self.data_handler.get_ticker(self.spot_exchange, spot_symbol)

        if not perp_ticker or not spot_ticker:
            logger.warning(f"Could not get prices for {self.symbol} or {spot_symbol}")
            return None

        # Calculate basis (price differential)
        basis = perp_ticker.price - spot_ticker.price

        # Update historical basis data
        if self.symbol not in self.historical_basis:
            self.historical_basis[self.symbol] = []
        self.historical_basis[self.symbol].append((now, basis))

        # Keep only recent data points
        max_history = self.get_param("history_length", 24)  # 24 data points by default
        if len(self.historical_basis[self.symbol]) > max_history:
            self.historical_basis[self.symbol] = self.historical_basis[self.symbol][-max_history:]

        # Calculate basis volatility
        basis_volatility = self._calculate_basis_volatility(self.symbol)

        # Calculate Net Funding Differential (NFD)
        # For the perp vs spot strategy, this is just the funding rate
        nfd = funding_rate.funding_rate

        # Skip if NFD is below threshold
        if abs(nfd) < self.min_funding_differential:
            logger.debug(f"NFD ({nfd:.6f}%) below threshold ({self.min_funding_differential:.6f}%)")
            return None

        # Estimate position size (will be refined by risk manager)
        position_size = Decimal("1000.0")  # Use Decimal, will be adjusted by risk manager

        # Estimate trading costs
        perp_slippage = self._estimate_slippage(self.symbol, position_size, self.perp_exchange)
        spot_slippage = self._estimate_slippage(spot_symbol, position_size, self.spot_exchange)

        # Get exchange fee rates as Decimal
        perp_fee_rate = Decimal(str(self.get_param(f"{self.perp_exchange}_fee_rate", 0.0005)))
        spot_fee_rate = Decimal(str(self.get_param(f"{self.spot_exchange}_fee_rate", 0.0005)))

        # Calculate total costs (Decimal math)
        total_costs = position_size * (
            perp_slippage + spot_slippage + perp_fee_rate + spot_fee_rate
        )

        # Calculate expected profit (Decimal math, convert funding rate correctly)
        # Assuming nfd is already Decimal rate (e.g., 0.0001 for 0.01%)
        expected_profit = (position_size * nfd) - total_costs

        # Skip if expected profit is below threshold
        if expected_profit < self.min_profit_threshold:
            logger.debug(
                f"Expected profit (${expected_profit:.2f}) below threshold "
                f"(${self.min_profit_threshold:.2f})"
            )
            return None

        # Calculate utility score (using float conversion for simplicity here)
        # Calculate utility score
        utility_score = expected_profit - (self.risk_aversion * (basis_volatility**2))

        # Determine which side to take based on funding rate
        # If funding is positive, shorts receive payment, so we short the perp and long the spot
        # If funding is negative, longs receive payment, so we long the perp and short the spot
        perp_side = "SHORT" if nfd > 0 else "LONG"
        # spot_side = "LONG" if nfd > 0 else "SHORT" # Unused variable F841

        # Create opportunity object
        opportunity = ArbitrageOpportunity(
            symbol=self.symbol,
            long_exchange=self.perp_exchange if perp_side == "LONG" else self.spot_exchange,
            short_exchange=self.spot_exchange if perp_side == "LONG" else self.perp_exchange,
            long_price=perp_ticker.ask if perp_side == "LONG" else spot_ticker.ask,
            short_price=spot_ticker.bid if perp_side == "LONG" else perp_ticker.bid,
            long_funding_rate=Decimal(str(nfd)) if perp_side == "LONG" else Decimal("0"),
            short_funding_rate=Decimal(str(nfd)) if perp_side == "SHORT" else Decimal("0"),
            net_funding_differential=Decimal(str(nfd)),
            timestamp=now,
            expected_profit=Decimal(str(expected_profit)),
            utility_score=float(utility_score),
            basis_volatility=float(basis_volatility),
            optimal_size=None,
            confidence=None,
        )

        logger.info(f"Found opportunity: {opportunity}")
        return opportunity

    def _calculate_basis_volatility(self, symbol: str) -> Decimal:
        """
        Calculate the volatility of the basis between perp and spot markets.

        Args:
            symbol: Trading symbol

        Returns:
            Basis volatility as standard deviation
        """
        if symbol not in self.historical_basis or len(self.historical_basis[symbol]) < 2:
            # Not enough data, return default volatility
            return Decimal("0.01")  # Default volatility of 1%

        # Extract basis values
        basis_values = [b for _, b in self.historical_basis[symbol]]

        # Calculate standard deviation
        mean = sum(basis_values) / len(basis_values)
        variance = sum((x - mean) ** 2 for x in basis_values) / len(basis_values)
        # Use Decimal for sqrt
        try:
            # Ensure variance is non-negative before sqrt
            if variance < 0:
                logger.warning(f"Calculated negative variance ({variance}) for basis volatility of {symbol}. Returning default.")
                return Decimal("0.01") # Return default Decimal volatility
            return variance.sqrt() # Equivalent to ** Decimal("0.5")
        except Exception as e:
            logger.error(f"Error calculating sqrt of variance {variance} for {symbol}: {e}")
            return Decimal("0.01") # Return default Decimal volatility on error

    def _estimate_slippage(self, symbol: str, size: Decimal, exchange: str) -> Decimal:
        """
        Estimate slippage for a given symbol, size, and exchange.

        Args:
            symbol: Trading symbol
            size: Position size in USD (Decimal)
            exchange: Exchange name

        Returns:
            Estimated slippage as a percentage (Decimal)
        """
        # Simple slippage model
        # More sophisticated models would use orderbook depth
        base_slippage = Decimal("0.0001")  # 0.01% base slippage

        # Scale slippage based on size (use Decimal math)
        ref_size = Decimal("10000")
        if size <= Decimal("0") or ref_size <= Decimal("0"):
            return base_slippage # Avoid math errors for zero/negative size

        # Use Decimal for exponentiation
        try:
            size_ratio = size / ref_size
             # Ensure ratio is non-negative before sqrt
            if size_ratio < 0:
                 logger.warning(f"Calculated negative size ratio ({size_ratio}) for slippage of {symbol}. Using base.")
                 return base_slippage
            slippage_scaling = size_ratio.sqrt() # Equivalent to ** Decimal("0.5")
        except Exception as e:
             logger.error(f"Error calculating sqrt of size_ratio {size_ratio} for {symbol} slippage: {e}")
             return base_slippage # Return base on error

        return base_slippage * slippage_scaling

    def process_data(self, data: MarketData) -> TradeSignal | None:
        """
        Process new market data and generate a trading signal if appropriate.

        Args:
            data: Market data to process

        Returns:
            TradeSignal if a trade should be executed, None otherwise
        """
        # First, update historical data
        self.update_historical_data(data)

        # Check if it's time to check for opportunities
        now = datetime.now(UTC)
        if (
            self.last_opportunity_check is None
            or (now - self.last_opportunity_check).total_seconds() >= self.check_interval
        ):
            # Create task but don't await immediately - it will run in the background
            # This avoids blocking the main strategy loop
            asyncio.create_task(self._check_and_generate_signal())
            self.last_opportunity_check = now

        # Check if we need to rebalance existing positions
        if self._should_rebalance():
            return self._generate_rebalance_signal()

        return None

    async def _check_and_generate_signal(self) -> TradeSignal | None:
        """
        Check for opportunities and generate a signal if one is found.
        This runs asynchronously to avoid blocking the main strategy loop.
        """
        try:
            opportunity = await self._check_opportunity()
            if opportunity:
                # Apply enhanced position sizing via RiskManager if available
                sized_opportunity = None

                if self.risk_manager:
                    sized_opportunity = self.risk_manager.size_opportunity(opportunity)
                    if sized_opportunity:
                        logger.info(f"Sized opportunity: {sized_opportunity}")
                        # Store sized opportunity for reference
                        opportunity_id = str(id(opportunity))
                        self.sized_opportunities[opportunity_id] = sized_opportunity
                    else:
                        logger.warning("Opportunity rejected by risk manager")
                        return None

                self.active_opportunities.append(opportunity)
                signal = self._generate_entry_signal(opportunity, sized_opportunity)
                logger.info(f"Generated trade signal: {signal}")
                return signal
        except Exception as e:
            logger.error(f"Error checking opportunities: {e}", exc_info=True)

        return None

    def _should_rebalance(self) -> bool:
        """
        Check if positions need rebalancing.

        Returns:
            True if rebalancing is needed, False otherwise
        """
        # Check if we have active positions
        perp_position = self.portfolio_tracker.get_position(self.perp_exchange, self.symbol)
        spot_symbol = self.symbol_mapping.get(self.symbol)
        spot_position = self.portfolio_tracker.get_position(self.spot_exchange, spot_symbol)

        if not perp_position or not spot_position:
            return False

        # Get latest prices
        perp_price = self.data_handler.get_latest_price(self.perp_exchange, self.symbol)
        spot_price = self.data_handler.get_latest_price(self.spot_exchange, spot_symbol)

        if not perp_price or not spot_price:
            return False

        # Calculate position values
        perp_value = abs(perp_position.size * perp_price)
        spot_value = abs(spot_position.size * spot_price)

        # Calculate imbalance
        if perp_value == 0 or spot_value == 0:
            return False

        imbalance = abs(perp_value - spot_value) / max(perp_value, spot_value)

        # Rebalance if imbalance exceeds threshold
        return imbalance > self.rebalance_threshold

    def _generate_entry_signal(
        self,
        opportunity: ArbitrageOpportunity,
        sized_opportunity: SizedOpportunity | None = None,
    ) -> TradeSignal:
        """
        Generate a trade signal for a new opportunity.

        Args:
            opportunity: The arbitrage opportunity
            sized_opportunity: Sized opportunity from risk manager (optional)

        Returns:
            TradeSignal for the opportunity
        """
        # Determine sides
        perp_side = "SHORT" if opportunity.net_funding_differential > 0 else "LONG"
        spot_side = "LONG" if opportunity.net_funding_differential > 0 else "SHORT"

        # Map spot symbol
        spot_symbol = self.symbol_mapping.get(self.symbol)

        # Determine position sizes
        default_size = 1000.0

        if sized_opportunity:
            # Use sizes from risk manager
            if opportunity.long_exchange == self.perp_exchange:
                perp_size = sized_opportunity.long_size
                spot_size = sized_opportunity.short_size
            else:
                perp_size = sized_opportunity.short_size
                spot_size = sized_opportunity.long_size

            # Convert to quantity using latest prices
            perp_price = (
                self.data_handler.get_latest_price(self.perp_exchange, self.symbol) or default_size
            )
            spot_price = (
                self.data_handler.get_latest_price(self.spot_exchange, spot_symbol) or default_size
            )

            perp_quantity = perp_size / perp_price
            spot_quantity = spot_size / spot_price
        else:
            # Use default sizes
            perp_quantity = default_size
            spot_quantity = default_size

        self.signals_generated += 1
        self.last_signal_time = datetime.now(UTC)

        # Create trade signal with enhanced metadata
        opportunity_id = str(id(opportunity))

        return TradeSignal(
            strategy_name=self.name,
            signal_type=SignalType.ENTER_LONG if perp_side == "LONG" else SignalType.ENTER_SHORT,
            symbol=self.symbol,
            timestamp=datetime.now(UTC),
            signal_id=f"{self.name}_{self.signals_generated}",
            trades=[
                {
                    "exchange": self.perp_exchange,
                    "symbol": self.symbol,
                    "side": perp_side,
                    "size": perp_quantity,
                    "type": "LIMIT",
                },
                {
                    "exchange": self.spot_exchange,
                    "symbol": spot_symbol,
                    "side": spot_side,
                    "size": spot_quantity,
                    "type": "LIMIT",
                },
            ],
            metadata={
                "opportunity_id": opportunity_id,
                "net_funding_differential": opportunity.net_funding_differential,
                "expected_profit": opportunity.expected_profit,
                "position_sizing": {
                    "enhanced": sized_opportunity is not None,
                    "long_size": sized_opportunity.long_size if sized_opportunity else None,
                    "short_size": sized_opportunity.short_size if sized_opportunity else None,
                    "allocation_percentage": sized_opportunity.allocation_percentage
                    if sized_opportunity
                    else None,
                    "risk_adjusted_return": sized_opportunity.risk_adjusted_return
                    if sized_opportunity
                    else None,
                }
                if sized_opportunity
                else {},
            },
        )

    def _generate_rebalance_signal(self) -> TradeSignal:
        """
        Generate a trade signal for rebalancing positions.

        Returns:
            TradeSignal for rebalancing
        """
        # Get current positions
        perp_position = self.portfolio_tracker.get_position(self.perp_exchange, self.symbol)
        spot_symbol = self.symbol_mapping.get(self.symbol)
        spot_position = self.portfolio_tracker.get_position(self.spot_exchange, spot_symbol)

        # Get latest prices
        perp_price = self.data_handler.get_latest_price(self.perp_exchange, self.symbol)
        spot_price = self.data_handler.get_latest_price(self.spot_exchange, spot_symbol)

        # Calculate position values
        perp_value = abs(perp_position.size * perp_price)
        spot_value = abs(spot_position.size * spot_price)

        # Calculate target delta-neutral position
        target_value = (abs(perp_value) + abs(spot_value)) / 2

        # Calculate adjustments needed
        perp_adjustment = target_value - abs(perp_value)
        spot_adjustment = target_value - abs(spot_value)

        # Determine sides for adjustments
        perp_side = (
            "LONG"
            if (perp_position.size > 0 and perp_adjustment > 0)
            or (perp_position.size < 0 and perp_adjustment < 0)
            else "SHORT"
        )

        spot_side = (
            "LONG"
            if (spot_position.size > 0 and spot_adjustment > 0)
            or (spot_position.size < 0 and spot_adjustment < 0)
            else "SHORT"
        )

        self.signals_generated += 1
        self.last_signal_time = datetime.now(UTC)

        return TradeSignal(
            strategy_name=self.name,
            signal_type=SignalType.REBALANCE,
            symbol=self.symbol,
            timestamp=datetime.now(UTC),
            signal_id=f"{self.name}_rebalance_{self.signals_generated}",
            trades=[
                {
                    "exchange": self.perp_exchange,
                    "symbol": self.symbol,
                    "side": perp_side,
                    "size": abs(perp_adjustment) / perp_price,
                    "type": "LIMIT",
                },
                {
                    "exchange": self.spot_exchange,
                    "symbol": spot_symbol,
                    "side": spot_side,
                    "size": abs(spot_adjustment) / spot_price,
                    "type": "LIMIT",
                },
            ],
            metadata={
                "rebalance_reason": "delta_neutral_adjustment",
                "perp_value_before": perp_value,
                "spot_value_before": spot_value,
                "target_value": target_value,
            },
        )

    def on_start(self) -> None:
        """Called when the strategy is started"""
        logger.info(f"Starting funding rate arbitrage strategy '{self.name}'")
        self.last_opportunity_check = None
        self.active_opportunities = []
        self.sized_opportunities = {}

    def on_stop(self) -> None:
        """Called when the strategy is stopped"""
        logger.info(f"Stopping funding rate arbitrage strategy '{self.name}'")
        # Clean up any resources if needed
