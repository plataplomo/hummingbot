from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal, getcontext
from typing import Any, cast

from cyberdelta.core.data_handler import DataHandler
from cyberdelta.core.models import (
    # MarketData, # Removed
    OrderSide,
    SignalType,
    Ticker,
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
    ) -> None:
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
        if isinstance(value, int | float | str):
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

        # Get funding rate from perp exchange
        funding_rate = self.data_handler.get_latest_funding_rate(self.perp_exchange, self.symbol)
        if funding_rate is None:
            logger.warning(f"Could not get funding rate for {self.symbol} on {self.perp_exchange}")
            return None

        # Get current time
        now = datetime.now(UTC)

        # Get prices for basis calculation
        perp_ticker = self.data_handler.get_latest_ticker(self.perp_exchange, self.symbol)
        spot_symbol = self.symbol_mapping.get(self.symbol)
        if spot_symbol is None:
            logger.warning(f"No spot symbol mapping for {self.symbol}")
            return None
        spot_ticker = self.data_handler.get_latest_ticker(self.spot_exchange, spot_symbol)

        if perp_ticker is None or spot_ticker is None:
            logger.warning(f"Could not get prices for {self.symbol} or {spot_symbol}")
            return None

        # Ensure .close is accessed only if tickers are not None (already checked)
        current_perp_price = perp_ticker.price
        current_spot_price = spot_ticker.price

        if current_perp_price is None or current_spot_price is None:
            logger.warning(
                f"Ticker price is None for {self.symbol} or {spot_symbol}. "
                f"Perp: {current_perp_price}, Spot: {current_spot_price}"
            )
            return None

        perp_price = current_perp_price
        spot_price = current_spot_price
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
            logger.warning(
                f"Funding rate value is None for {self.symbol} on "
                f"{self.perp_exchange} from {funding_rate}"
            )
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

        # Ensure prices are not None before using them
        entry_perp_price = perp_ticker.price
        entry_spot_price = spot_ticker.price

        if entry_perp_price is None or entry_spot_price is None:
            logger.warning(
                f"Cannot determine entry prices due to None ticker price for "
                f"{self.symbol} or {spot_symbol}."
            )
            return None

        long_price = entry_perp_price if perp_side == "LONG" else entry_spot_price
        short_price = entry_spot_price if perp_side == "LONG" else entry_perp_price

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
        signals: list[TradeSignal] = []

        # Time-based check for funding opportunities
        if (
            self.last_opportunity_check is None
            or (now - self.last_opportunity_check).total_seconds() >= self.check_interval
        ):
            opportunity_signals = await self.evaluate_entry_opportunity()
            if opportunity_signals:
                signals.extend(opportunity_signals)
            # self.last_opportunity_check is updated within evaluate_entry_opportunity

        # Rebalance check (can be independent or related to opportunity checks)
        # Fetch live tickers for rebalance check
        # data_handler is guaranteed by __init__ to be non-None
        perp_ticker_live = self.data_handler.get_latest_ticker(self.perp_exchange, self.symbol)

        spot_symbol_mapped = self.symbol_mapping.get(self.symbol)
        spot_ticker_live: Ticker | None = None
        if spot_symbol_mapped:
            spot_ticker_live = self.data_handler.get_latest_ticker(
                self.spot_exchange, spot_symbol_mapped
            )

        if self._should_rebalance(perp_ticker_live, spot_ticker_live):
            rebalance_signals = self._generate_rebalance_signal(perp_ticker_live, spot_ticker_live)
            if rebalance_signals:
                signals.extend(rebalance_signals)

        return signals

    async def evaluate_entry_opportunity(self) -> list[TradeSignal] | None:
        """Core logic to check for opportunities and generate trading signals."""
        self.last_opportunity_check = datetime.now(UTC)
        signals: list[TradeSignal] = []

        if self.risk_manager is None:
            logger.error(
                f"RiskManager not initialized in {self.name} for evaluate_entry_opportunity"
            )
            return None

        # Fetch current tickers needed by helper methods first
        # data_handler is guaranteed by __init__ to be non-None
        perp_ticker_live = self.data_handler.get_latest_ticker(self.perp_exchange, self.symbol)

        spot_symbol_mapped = self.symbol_mapping.get(self.symbol)
        spot_ticker_live: Ticker | None = None
        if spot_symbol_mapped:
            spot_ticker_live = self.data_handler.get_latest_ticker(
                self.spot_exchange, spot_symbol_mapped
            )

        # Check for rebalancing first
        if self._should_rebalance(perp_ticker_live, spot_ticker_live):
            rebalance_signals = self._generate_rebalance_signal(perp_ticker_live, spot_ticker_live)
            if rebalance_signals:
                signals.extend(rebalance_signals)
                # Potentially return early or manage state to avoid conflicting entry signals
                logger.info(
                    f"Generated {len(rebalance_signals)} rebalance signals for {self.symbol}."
                )
                return signals

        opportunity = await self._check_opportunity()
        if opportunity:
            logger.info(f"Found opportunity: {opportunity}")
            self.active_opportunities.append(opportunity)

            sized_opportunity_raw = self.risk_manager.calculate_position_size(opportunity)  # type: ignore[attr-defined]
            sized_opportunity = cast(SizedOpportunity | None, sized_opportunity_raw)

            if (
                sized_opportunity
                and sized_opportunity.long_size > Decimal(0)
                and sized_opportunity.short_size > Decimal(0)
            ):
                self.sized_opportunities[opportunity.id] = sized_opportunity
                entry_signals = self._generate_entry_signal(
                    opportunity, sized_opportunity, perp_ticker_live, spot_ticker_live
                )
                if entry_signals:
                    signals.extend(entry_signals)
                    logger.info(
                        f"Generated {len(entry_signals)} entry signals for {opportunity.id}."
                    )
            else:
                logger.info(
                    f"Opportunity {opportunity.id} not sized or size is zero, no entry "
                    f"signals generated."
                )
        return signals if signals else None

    def _should_rebalance(
        self, perp_ticker_live: Ticker | None, spot_ticker_live: Ticker | None
    ) -> bool:
        """Determine if rebalancing is needed based on current positions and market prices."""
        # The portfolio_tracker is guaranteed non-None by __init__.
        # The check for has_active_positions was problematic, subsequent logic
        # handles position existence.
        # if not self.portfolio_tracker: # REMOVED
        #    or not self.portfolio_tracker.has_active_positions(self.name): # REMOVED
        #     return False # REMOVED

        # Get current positions
        perp_position = self.portfolio_tracker.get_position(self.perp_exchange, self.symbol)
        spot_symbol = self.symbol_mapping.get(self.symbol)
        if not spot_symbol:
            logger.error(f"Spot symbol not mapped for {self.symbol} in _should_rebalance")
            return False
        spot_position = self.portfolio_tracker.get_position(self.spot_exchange, spot_symbol)

        # Ensure positions are not None before accessing .size or using in calculations
        if perp_position is None or spot_position is None:
            logger.warning("Perp or spot position is None, cannot evaluate rebalance.")
            return False
        # Assuming DerivativePosition.size is Decimal (not Decimal | None)
        # If .size itself can be None, further checks are needed here.

        if (
            perp_ticker_live is None or spot_ticker_live is None
        ):  # Short-circuit before accessing .price
            logger.warning("Live ticker data unavailable for rebalance check (ticker object None).")
            return False

        current_perp_live_price = perp_ticker_live.price
        current_spot_live_price = spot_ticker_live.price

        if current_perp_live_price is None or current_spot_live_price is None:
            logger.warning("Live ticker data unavailable for rebalance check (price is None).")
            return False

        perp_price_val = current_perp_live_price
        spot_price_val = current_spot_live_price

        # Values can now be calculated as positions are confirmed to be not None
        perp_value = perp_position.size * perp_price_val
        spot_value = spot_position.size * spot_price_val

        # Check imbalance (simplified: absolute difference in values)
        imbalance = abs(perp_value + spot_value)  # Assuming one is short, one is long
        total_value = abs(perp_value) + abs(spot_value)

        if total_value == Decimal(0):
            return False  # Avoid division by zero if no value

        imbalance_ratio = imbalance / total_value
        logger.debug(f"Rebalance check: Imbalance ratio {imbalance_ratio:.4f} for {self.symbol}")
        return imbalance_ratio > self.rebalance_threshold

    def _generate_entry_signal(
        self,
        opportunity: ArbitrageOpportunity,
        sized_opportunity: SizedOpportunity | None = None,
        perp_ticker_entry: Ticker | None = None,  # ADDED param
        spot_ticker_entry: Ticker | None = None,  # ADDED param
    ) -> list[TradeSignal]:
        """Generate entry signals for a given opportunity."""
        signals: list[TradeSignal] = []
        default_size = Decimal("0")  # Default for quantity if price is zero

        perp_side = OrderSide.BUY if opportunity.net_funding_differential > 0 else OrderSide.SELL
        spot_side = OrderSide.SELL if opportunity.net_funding_differential > 0 else OrderSide.BUY

        perp_exchange_name = (
            opportunity.long_exchange if perp_side == OrderSide.BUY else opportunity.short_exchange
        )
        spot_exchange_name = (
            opportunity.short_exchange if perp_side == OrderSide.BUY else opportunity.long_exchange
        )

        # Use live tickers passed as arguments
        # perp_ticker_entry was passed
        # spot_ticker_entry was passed

        perp_price_entry = (
            perp_ticker_entry.price
            if perp_ticker_entry is not None and perp_ticker_entry.price is not None
            else default_size
        )
        spot_price_entry = (
            spot_ticker_entry.price
            if spot_ticker_entry is not None and spot_ticker_entry.price is not None
            else default_size
        )

        # Determine quantities
        perp_quantity = default_size
        spot_quantity = default_size

        if sized_opportunity:
            # Use sized opportunity details if available
            perp_size = (
                sized_opportunity.long_size
                if perp_side == OrderSide.BUY
                else sized_opportunity.short_size
            )
            spot_size = (
                sized_opportunity.short_size
                if perp_side == OrderSide.BUY
                else sized_opportunity.long_size
            )

            perp_quantity = (
                perp_size / perp_price_entry if perp_price_entry > Decimal(0) else default_size
            )
            spot_quantity = (
                spot_size / spot_price_entry if spot_price_entry > Decimal(0) else default_size
            )
        else:
            # Fallback if no sized_opportunity (should ideally not happen
            # if risk_manager is used properly)
            # For non-sized opportunity, perhaps use a default quantity or skip
            # if prices are zero
            if perp_price_entry <= Decimal(0) or spot_price_entry <= Decimal(0):
                logger.warning(
                    "Prices for non-sized opportunity are zero/None or invalid, "
                    "cannot determine quantity."
                )
                return []  # For safety, do not proceed if not properly sized.
            # Simplified: use a nominal quantity based on opportunity target if not sized
            # This part needs careful consideration for a real strategy.
            # For now, we assume sizing is always done by risk manager if opportunity is pursued.
            logger.warning(
                f"No sized_opportunity for {opportunity.id}, using fallback "
                f"quantities might be risky."
            )
            # Example fallback (not recommended for live trading without proper sizing logic):
            # nominal_trade_value = Decimal("100") # e.g., $100 USD
            # perp_quantity = (
            #     nominal_trade_value / perp_price_entry # MODIFIED
            #     if perp_price_entry > Decimal(0) else default_size
            # )
            # spot_quantity = (
            #     nominal_trade_value / spot_price_entry # MODIFIED
            #     if spot_price_entry > Decimal(0) else default_size
            # )
            return []  # For safety, do not proceed if not properly sized.

        if perp_quantity > Decimal(0):
            signals.append(
                TradeSignal(
                    source_strategy=self.name,
                    symbol=opportunity.symbol,  # Exchange-specific symbol for perp_exchange_name
                    exchange=perp_exchange_name,  # Use the correct exchange for this leg
                    side=perp_side,
                    price=opportunity.long_price
                    if perp_side == OrderSide.BUY
                    else opportunity.short_price,
                    quantity=perp_quantity,
                    signal_type=SignalType.ENTER_LONG
                    if perp_side == OrderSide.BUY
                    else SignalType.ENTER_SHORT,
                    timestamp=datetime.now(UTC),
                    confidence=opportunity.confidence_score,  # Pass confidence
                    metadata={
                        "strategy_name": self.name,
                        "opportunity_id": opportunity.id,
                    },
                )
            )
        if spot_quantity > Decimal(0):
            # Map to spot symbol for the spot exchange signal
            spot_symbol_for_signal = self.symbol_mapping.get(opportunity.symbol, opportunity.symbol)
            signals.append(
                TradeSignal(
                    source_strategy=self.name,
                    symbol=spot_symbol_for_signal,  # Use the mapped spot symbol
                    exchange=spot_exchange_name,  # Use the correct exchange for this leg
                    side=spot_side,
                    price=opportunity.short_price
                    if perp_side == OrderSide.BUY
                    else opportunity.long_price,
                    quantity=spot_quantity,
                    signal_type=SignalType.ENTER_SHORT
                    if spot_side == OrderSide.SELL
                    else SignalType.ENTER_LONG,
                    timestamp=datetime.now(UTC),
                    confidence=opportunity.confidence_score,  # Pass confidence
                    metadata={
                        "strategy_name": self.name,
                        "opportunity_id": opportunity.id,
                    },
                )
            )
        self.signals_generated += len(signals)
        return signals

    def _generate_rebalance_signal(
        self, perp_ticker_live: Ticker | None, spot_ticker_live: Ticker | None
    ) -> list[TradeSignal]:
        """Generate rebalancing signals based on current positions and target delta."""
        signals: list[TradeSignal] = []
        if not self.portfolio_tracker:
            logger.error(f"PortfolioTracker not set for {self.name}")
            return signals

        perp_position = self.portfolio_tracker.get_position(self.perp_exchange, self.symbol)
        spot_symbol = self.symbol_mapping.get(self.symbol)
        if not spot_symbol:
            logger.error(f"Spot symbol not mapped for {self.symbol} in _generate_rebalance_signal")
            return signals
        spot_position = self.portfolio_tracker.get_position(self.spot_exchange, spot_symbol)

        if (
            perp_ticker_live is None or spot_ticker_live is None
        ):  # Short-circuit before accessing .price
            logger.warning("Live ticker data unavailable for rebalance check (ticker object None).")
            return signals

        current_perp_live_price = perp_ticker_live.price
        current_spot_live_price = spot_ticker_live.price

        if current_perp_live_price is None or current_spot_live_price is None:
            logger.warning("Live ticker data unavailable for rebalance check (price is None).")
            return signals

        perp_price_rebal = current_perp_live_price
        spot_price_rebal = current_spot_live_price

        # Check if positions are None before accessing .size
        if perp_position is None or spot_position is None:
            logger.warning("Perp or spot position is None, cannot rebalance.")
            return signals
        # Assuming DerivativePosition.size is Decimal (not Decimal | None)
        # The original check also had: or perp_position.size is None or spot_position.size is None
        # If .size can be None, that check needs to be here too.
        # For now, assuming .size is non-optional if position object exists.

        # Simplified rebalancing: aim for market-neutral by value
        # Positive size = long, negative size = short
        perp_value = perp_position.size * perp_price_rebal
        spot_value = spot_position.size * spot_price_rebal
        net_exposure = perp_value + spot_value

        # If net exposure is significant, rebalance
        # Example: rebalance if exposure is > 1% of the smaller leg's absolute value
        # This threshold logic can be much more sophisticated
        threshold_value = min(abs(perp_value), abs(spot_value)) * Decimal("0.01")
        if threshold_value == Decimal(0) and net_exposure != Decimal(0):
            logger.info(
                f"Rebalance needed for {self.symbol} due to zero threshold and "
                f"non-zero exposure: {net_exposure}"
            )
        elif abs(net_exposure) > threshold_value and threshold_value > Decimal(0):
            logger.info(
                f"Rebalance triggered for {self.symbol}. Net exposure: {net_exposure}, "
                f"Threshold: {threshold_value}"
            )
        else:
            return signals  # No rebalancing needed

        # Determine which leg to adjust and by how much (simplified)
        # This logic assumes we want to bring net_exposure closer to zero.
        # A more robust approach would consider transaction costs, minimum order sizes etc.

        if abs(net_exposure) < Decimal("0.01"):  # Effectively zero, no rebalance needed
            return signals

        if net_exposure > Decimal(0):  # Net long, need to sell something or buy less
            # Option 1: Sell some of the leg that made it net long
            # (e.g. if perp_value is more positive)
            # Option 2: Increase the short leg
            # Simplified: Adjust the perpetual leg by the net exposure amount
            if perp_position.size > 0:  # If perp is long, sell some
                side = OrderSide.SELL
                exchange_to_adjust = self.perp_exchange
                symbol_to_adjust = self.symbol
                price_to_use = perp_price_rebal
            else:  # Perp is short, sell more of spot (if spot is long)
                side = OrderSide.SELL
                exchange_to_adjust = self.spot_exchange
                symbol_to_adjust = spot_symbol
                price_to_use = spot_price_rebal
        else:  # Net short, need to buy something or sell less
            if perp_position.size < 0:  # If perp is short, buy some back
                side = OrderSide.BUY
                exchange_to_adjust = self.perp_exchange
                symbol_to_adjust = self.symbol
                price_to_use = perp_price_rebal
            else:  # Perp is long, buy more of spot (if spot is short)
                side = OrderSide.BUY
                exchange_to_adjust = self.spot_exchange
                symbol_to_adjust = spot_symbol
                price_to_use = spot_price_rebal

        if price_to_use <= Decimal(0):
            logger.warning(
                f"Price for rebalancing {symbol_to_adjust} is zero or None, "
                f"cannot calculate quantity."
            )
            return signals

        quantity_to_rebalance = abs(net_exposure) / price_to_use

        if quantity_to_rebalance > Decimal("0"):  # Ensure non-zero quantity
            signals.append(
                TradeSignal(
                    source_strategy=self.name,
                    symbol=symbol_to_adjust,
                    exchange=exchange_to_adjust,
                    side=side,
                    price=price_to_use,  # Use current market price for rebalance
                    quantity=quantity_to_rebalance,
                    signal_type=SignalType.REBALANCE,
                    timestamp=datetime.now(UTC),
                    # Confidence for rebalance might be different or not applicable
                    confidence=1.0,  # MODIFIED: Decimal to float
                    metadata={
                        "strategy_name": self.name,
                        "opportunity_id": "REBALANCE-" + datetime.now(UTC).isoformat(),
                    },
                )
            )
            logger.info(
                f"Generated rebalance signal: {side} {quantity_to_rebalance} "
                f"{symbol_to_adjust} on {exchange_to_adjust}"
            )

        return signals

    def on_start(self) -> None:
        """Called when the strategy is started."""
        logger.info(f"Strategy {self.name} started for symbol {self.symbol}.")
        # Potentially load historical data or prime initial state

    def on_stop(self) -> None:
        """Called when the strategy is stopped."""
        logger.info(f"Strategy {self.name} stopped for symbol {self.symbol}.")
        # Perform any cleanup, like cancelling open orders (if strategy manages them directly)
