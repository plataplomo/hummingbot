from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from decimal import Decimal, getcontext
from typing import Any

from cyberdelta.core.data_handler import DataHandler
from cyberdelta.core.models import (
    # MarketData, # Removed
    OrderSide,
    SignalType,
    TradeSignal,
)
from cyberdelta.core.models.market import Candle  # Import Candle
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import RiskManager, SizedOpportunity
from cyberdelta.core.strategy import Strategy
from cyberdelta.validation.funding_data import ArbitrageOpportunity

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

        # Use helpers for config-derived parameters
        self.min_funding_differential: Decimal = self._get_decimal_param(
            "min_funding_differential", Decimal("0.0001")
        )
        self.min_profit_threshold: Decimal = self._get_decimal_param(
            "min_profit_threshold", Decimal("5.0")
        )
        self.risk_aversion: Decimal = self._get_decimal_param("risk_aversion", Decimal("1.0"))
        self.rebalance_threshold: Decimal = self._get_decimal_param(
            "rebalance_threshold", Decimal("0.05")
        )
        self.check_interval: int = self._get_int_param("check_interval", 600)

        # Strategy state
        self.last_opportunity_check: datetime | None = None
        self.active_opportunities: list[ArbitrageOpportunity] = []
        self.historical_basis: dict[str, list[tuple[datetime, Decimal]]] = {}
        # Defensive: ensure check_interval is always int
        # max_history = self._get_int_param("history_length", 24)  # Unused, remove

        # Exchange mapping
        self.perp_exchange: str = str(self.get_param("perp_exchange", "hyperliquid"))
        self.spot_exchange: str = str(self.get_param("spot_exchange", "backpack"))

        # Market mapping (perp to spot)
        symbol_mapping_param = self.get_param("symbol_mapping", None)
        if symbol_mapping_param is not None and isinstance(symbol_mapping_param, dict):
            self.symbol_mapping: dict[str, str] = symbol_mapping_param
        else:
            # Default mapping if not provided
            base = symbol.split("-")[0] if "-" in symbol else symbol.split("_")[0]
            self.symbol_mapping = {symbol: f"{base}_USDC"}

        # Position sizing info storage
        self.sized_opportunities: dict[str, SizedOpportunity] = {}

        logger.info(
            f"Initialized {self.name} strategy for {self.symbol} "
            f"between {self.perp_exchange} and {self.spot_exchange}"
        )

    def _get_decimal_param(self, key: str, default: Decimal) -> Decimal:
        value = self.get_param(key, default)
        try:
            return Decimal(str(value))
        except Exception:
            return default

    def _get_int_param(self, key: str, default: int) -> int:
        value = self.get_param(key, default)
        if value is None:
            return default
        if isinstance(value, (int, float, str)):
            try:
                return int(value)
            except Exception:
                return default
        return default

    async def _check_opportunity(self) -> ArbitrageOpportunity | None:
        """
        Check for funding rate arbitrage opportunity between perp and spot markets.

        Returns:
            ArbitrageOpportunity if found, None otherwise
        """
        # Get max_history at the start so it's always defined
        max_history = self._get_int_param("history_length", 24)
        # Get funding rate from perp exchange (synchronous call)
        funding_rate = self.data_handler.get_funding_rate(self.perp_exchange, self.symbol)
        if asyncio.iscoroutine(funding_rate):
            funding_rate = await funding_rate
        if funding_rate is None:
            logger.warning(f"Could not get funding rate for {self.symbol} on {self.perp_exchange}")
            return None

        # Get current time
        now = datetime.now(UTC)

        # Get prices for basis calculation (synchronous calls)
        perp_ticker = self.data_handler.get_ticker(self.perp_exchange, self.symbol)
        spot_symbol = self.symbol_mapping.get(self.symbol)
        if spot_symbol is None:
            logger.warning(f"No spot symbol mapping for {self.symbol}")
            return None
        spot_ticker = self.data_handler.get_ticker(self.spot_exchange, spot_symbol)

        if perp_ticker is None or spot_ticker is None:
            logger.warning(f"Could not get prices for {self.symbol} or {spot_symbol}")
            return None

        # Calculate basis (price differential)
        perp_price = perp_ticker.close
        spot_price = spot_ticker.close
        basis = perp_price - spot_price

        # Update historical basis data
        if self.symbol not in self.historical_basis:
            self.historical_basis[self.symbol] = []
        self.historical_basis[self.symbol].append((now, basis))

        # Keep only recent data points
        if len(self.historical_basis[self.symbol]) > max_history:
            self.historical_basis[self.symbol] = self.historical_basis[self.symbol][-max_history:]

        # Calculate basis volatility
        basis_volatility = self._calculate_basis_volatility(self.symbol)

        # Calculate Net Funding Differential (NFD)
        nfd = funding_rate.funding_rate
        if nfd is None:
            logger.warning(f"Funding rate is None for {self.symbol}")
            return None

        # Skip if NFD is below threshold
        if abs(nfd) < self.min_funding_differential:
            logger.debug(f"NFD ({nfd:.6f}%) below threshold ({self.min_funding_differential:.6f}%)")
            return None

        # Estimate position size (will be refined by risk manager)
        position_size = Decimal("1000.0")

        # Estimate trading costs
        perp_slippage = self._estimate_slippage(self.symbol, position_size, self.perp_exchange)
        spot_slippage = self._estimate_slippage(spot_symbol, position_size, self.spot_exchange)

        perp_fee_rate = self._get_decimal_param(f"{self.perp_exchange}_fee_rate", Decimal("0.0005"))
        spot_fee_rate = self._get_decimal_param(f"{self.spot_exchange}_fee_rate", Decimal("0.0005"))

        # Calculate total costs (Decimal math)
        total_costs = position_size * (
            perp_slippage + spot_slippage + perp_fee_rate + spot_fee_rate
        )

        # Calculate expected profit (Decimal math)
        expected_profit = (position_size * nfd) - total_costs

        # Skip if expected profit is below threshold
        if expected_profit < self.min_profit_threshold:
            logger.debug(
                f"Expected profit (${expected_profit:.2f}) below threshold "
                f"(${self.min_profit_threshold:.2f})"
            )
            return None

        # Calculate utility score
        utility_score = expected_profit - (self.risk_aversion * (basis_volatility**2))

        # Determine which side to take based on funding rate
        perp_side = "SHORT" if nfd > 0 else "LONG"

        # Determine entry prices based on Candle close prices
        # A real strategy might estimate entry better (e.g., mid-price, or consider liquidity)
        # For simplicity, use the close price of the respective tickers
        long_price = perp_ticker.close if perp_side == "LONG" else spot_ticker.close
        short_price = spot_ticker.close if perp_side == "LONG" else perp_ticker.close

        # Create opportunity object
        opportunity = ArbitrageOpportunity(
            symbol=self.symbol,
            long_exchange=self.perp_exchange if perp_side == "LONG" else self.spot_exchange,
            short_exchange=self.spot_exchange if perp_side == "LONG" else self.perp_exchange,
            long_price=long_price,
            short_price=short_price,
            long_funding_rate=nfd if perp_side == "LONG" else Decimal("0"),
            short_funding_rate=nfd if perp_side == "SHORT" else Decimal("0"),
            net_funding_differential=nfd,
            timestamp=now,
            expected_profit=expected_profit,
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
            return Decimal("0.01")
        basis_values = [Decimal(str(b)) for _, b in self.historical_basis[symbol]]
        mean = sum(basis_values) / Decimal(len(basis_values))
        variance = sum((x - mean) ** 2 for x in basis_values) / Decimal(len(basis_values))
        try:
            if variance < 0:
                logger.warning(
                    f"Calculated negative variance ({variance}) for basis volatility of {symbol}. "
                    "Returning default."
                )
                return Decimal("0.01")
            return variance.sqrt()
        except Exception as e:
            logger.error(f"Error calculating sqrt of variance {variance} for {symbol}: {e}")
            return Decimal("0.01")

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
        base_slippage = Decimal("0.0001")
        ref_size = Decimal("10000")
        if size <= Decimal("0") or ref_size <= Decimal("0"):
            return base_slippage
        try:
            size_ratio = size / ref_size
            if size_ratio < 0:
                logger.warning(
                    f"Calculated negative size ratio ({size_ratio}) for slippage of {symbol}. "
                    "Using base."
                )
                return base_slippage
            slippage_scaling = size_ratio.sqrt()
        except Exception as e:
            logger.error(f"Error calculating sqrt of size_ratio for {symbol} slippage: {e}")
            return base_slippage
        return base_slippage * slippage_scaling

    async def process_data(self, data: Candle) -> list[TradeSignal]:
        """
        Process incoming market data (Candle).

        Args:
            data: Market data to process

        Returns:
            List of TradeSignals (multi-leg, e.g., for both perp and spot) if trades should be
            executed, or an empty list if no opportunity is found.
            The returned list may be empty if no valid signals are generated.
        """
        self.update_historical_data(data)
        now = datetime.now(UTC)
        if (
            self.last_opportunity_check is None
            or (now - self.last_opportunity_check).total_seconds() >= self.check_interval
        ):
            asyncio.create_task(self._check_and_generate_signal())
            self.last_opportunity_check = now
        if self._should_rebalance():
            return self._generate_rebalance_signal()
        return []

    async def _check_and_generate_signal(self) -> list[TradeSignal] | None:
        try:
            opportunity = await self._check_opportunity()
            if opportunity:
                sized_opportunity = None
                if self.risk_manager:
                    sized_opportunity = self.risk_manager.size_opportunity(opportunity)
                    if sized_opportunity:
                        logger.info(f"Sized opportunity: {sized_opportunity}")
                        opportunity_id = str(id(opportunity))
                        self.sized_opportunities[opportunity_id] = sized_opportunity
                    else:
                        logger.warning("Opportunity rejected by risk manager")
                        return None
                self.active_opportunities.append(opportunity)
                signals = self._generate_entry_signal(opportunity, sized_opportunity)
                logger.info(f"Generated trade signals: {signals}")
                return signals
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
        perp_position = None
        spot_position = None
        if hasattr(self.portfolio_tracker, "get_position"):
            perp_position = self.portfolio_tracker.get_position(self.perp_exchange, self.symbol)
            spot_symbol = self.symbol_mapping.get(self.symbol)
            if spot_symbol is not None:
                spot_position = self.portfolio_tracker.get_position(self.spot_exchange, spot_symbol)
        if not perp_position or not spot_position:
            return False

        # Get latest prices using get_ticker().close as fallback
        perp_ticker = self.data_handler.get_ticker(self.perp_exchange, self.symbol)
        spot_symbol = self.symbol_mapping.get(self.symbol)
        spot_ticker = None
        if spot_symbol is not None:
            spot_ticker = self.data_handler.get_ticker(self.spot_exchange, spot_symbol)
        perp_price = perp_ticker.close if perp_ticker is not None else None
        spot_price = spot_ticker.close if spot_ticker is not None else None
        if perp_price is None or spot_price is None:
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
    ) -> list[TradeSignal]:
        """Generate entry signals (long/short on perp and spot) if trades should be executed, or an
        empty list if no opportunity is found.
        """
        nfd = opportunity.net_funding_differential
        if nfd is None:
            logger.warning("Opportunity net_funding_differential is None, cannot generate signals.")
            return []
        perp_side = OrderSide.SELL if nfd > 0 else OrderSide.BUY
        spot_symbol = self.symbol_mapping.get(self.symbol)
        default_size = Decimal("1000.0")
        perp_quantity = default_size
        spot_quantity = default_size
        if sized_opportunity:
            if opportunity.long_exchange == self.perp_exchange:
                perp_size = sized_opportunity.long_size
                spot_size = sized_opportunity.short_size
            else:
                perp_size = sized_opportunity.short_size
                spot_size = sized_opportunity.long_size
            perp_ticker = self.data_handler.get_ticker(self.perp_exchange, self.symbol)
            spot_ticker = None
            if spot_symbol is not None:
                spot_ticker = self.data_handler.get_ticker(self.spot_exchange, spot_symbol)
            perp_price = perp_ticker.close if perp_ticker is not None else default_size
            spot_price = spot_ticker.close if spot_ticker is not None else default_size
            perp_quantity = perp_size / perp_price if perp_price else default_size
            spot_quantity = spot_size / spot_price if spot_price else default_size
        else:
            perp_ticker = self.data_handler.get_ticker(self.perp_exchange, self.symbol)
            spot_ticker = None
            if spot_symbol is not None:
                spot_ticker = self.data_handler.get_ticker(self.spot_exchange, spot_symbol)
            perp_price = perp_ticker.close if perp_ticker is not None else default_size
            spot_price = spot_ticker.close if spot_ticker is not None else default_size
        self.signals_generated += 1
        self.last_signal_time = datetime.now(UTC)
        opportunity_id = str(id(opportunity))
        now = datetime.now(UTC)
        signals = [
            TradeSignal(
                symbol=self.symbol,
                signal_type=SignalType.ENTER_SHORT
                if perp_side == OrderSide.SELL
                else SignalType.ENTER_LONG,
                side=perp_side,
                price=perp_price,
                quantity=perp_quantity,
                timestamp=now,
                metadata={
                    "opportunity_id": opportunity_id,
                    "leg": "perp",
                    "net_funding_differential": opportunity.net_funding_differential,
                    "expected_profit": opportunity.expected_profit,
                    "position_sizing": {
                        "long_size": getattr(sized_opportunity, "long_size", None),
                        "short_size": getattr(sized_opportunity, "short_size", None),
                        "allocation_percentage": getattr(
                            sized_opportunity, "allocation_percentage", None
                        ),
                        "risk_adjusted_return": getattr(
                            sized_opportunity, "risk_adjusted_return", None
                        ),
                    }
                    if sized_opportunity
                    else {},
                },
                signal_id=f"{self.name}_{self.signals_generated}_perp",
            ),
        ]
        if spot_symbol is not None:
            signals.append(
                TradeSignal(
                    symbol=spot_symbol,
                    signal_type=SignalType.ENTER_LONG if nfd > 0 else SignalType.ENTER_SHORT,
                    side=OrderSide.BUY if nfd > 0 else OrderSide.SELL,
                    price=spot_price,
                    quantity=spot_quantity,
                    timestamp=now,
                    metadata={
                        "opportunity_id": opportunity_id,
                        "leg": "spot",
                        "net_funding_differential": opportunity.net_funding_differential,
                        "expected_profit": opportunity.expected_profit,
                        "position_sizing": {
                            "long_size": getattr(sized_opportunity, "long_size", None),
                            "short_size": getattr(sized_opportunity, "short_size", None),
                            "allocation_percentage": getattr(
                                sized_opportunity, "allocation_percentage", None
                            ),
                            "risk_adjusted_return": getattr(
                                sized_opportunity, "risk_adjusted_return", None
                            ),
                        }
                        if sized_opportunity
                        else {},
                    },
                    signal_id=f"{self.name}_{self.signals_generated}_spot",
                )
            )
        return signals

    def _generate_rebalance_signal(self) -> list[TradeSignal]:
        """
        Generate trade signals for rebalancing positions (one per leg).
        """
        perp_position = self.portfolio_tracker.get_position(self.perp_exchange, self.symbol)
        spot_symbol = self.symbol_mapping.get(self.symbol)
        if spot_symbol is None:
            return []
        spot_position = self.portfolio_tracker.get_position(self.spot_exchange, spot_symbol)
        perp_ticker = self.data_handler.get_ticker(self.perp_exchange, self.symbol)
        spot_ticker = self.data_handler.get_ticker(self.spot_exchange, spot_symbol)
        perp_price = perp_ticker.close if perp_ticker is not None else Decimal("1")
        spot_price = spot_ticker.close if spot_ticker is not None else Decimal("1")
        perp_value = abs(perp_position.size * perp_price) if perp_position else Decimal("0")
        spot_value = abs(spot_position.size * spot_price) if spot_position else Decimal("0")
        if perp_value == 0 or spot_value == 0:
            target_value = Decimal("0")
        else:
            target_value = (abs(perp_value) + abs(spot_value)) / 2
        perp_adjustment = target_value - abs(perp_value)
        spot_adjustment = target_value - abs(spot_value)
        perp_side = (
            OrderSide.BUY
            if (
                perp_position
                and (
                    (perp_position.size > 0 and perp_adjustment > 0)
                    or (perp_position.size < 0 and perp_adjustment < 0)
                )
            )
            else OrderSide.SELL
        )
        spot_side = (
            OrderSide.BUY
            if (
                spot_position
                and (
                    (spot_position.size > 0 and spot_adjustment > 0)
                    or (spot_position.size < 0 and spot_adjustment < 0)
                )
            )
            else OrderSide.SELL
        )
        self.signals_generated += 1
        self.last_signal_time = datetime.now(UTC)
        now = datetime.now(UTC)
        opportunity_id = f"rebalance_{int(now.timestamp() * 1e6)}"
        signals = [
            TradeSignal(
                symbol=self.symbol,
                signal_type=SignalType.REBALANCE,
                side=perp_side,
                price=perp_price,
                quantity=abs(perp_adjustment) / perp_price if perp_price else Decimal("0"),
                timestamp=now,
                metadata={
                    "opportunity_id": opportunity_id,
                    "rebalance_reason": "delta_neutral_adjustment",
                    "leg": "perp",
                    "perp_value_before": perp_value,
                    "spot_value_before": spot_value,
                    "target_value": target_value,
                },
                signal_id=f"{self.name}_rebalance_{self.signals_generated}_perp",
            ),
            TradeSignal(
                symbol=spot_symbol,
                signal_type=SignalType.REBALANCE,
                side=spot_side,
                price=spot_price,
                quantity=abs(spot_adjustment) / spot_price if spot_price else Decimal("0"),
                timestamp=now,
                metadata={
                    "opportunity_id": opportunity_id,
                    "rebalance_reason": "delta_neutral_adjustment",
                    "leg": "spot",
                    "perp_value_before": perp_value,
                    "spot_value_before": spot_value,
                    "target_value": target_value,
                },
                signal_id=f"{self.name}_rebalance_{self.signals_generated}_spot",
            ),
        ]
        return signals

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
