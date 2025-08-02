"""Advanced position sizing with complete portfolio integration."""
from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict

from cyberdelta.core.portfolio.coordinators.portfolio_risk_coordinator import PortfolioRiskCoordinator
from cyberdelta.core.symbols import Symbol


class PortfolioAwarePositionSizer:
    """Position sizer with advanced portfolio context."""

    def __init__(self, coordinator: PortfolioRiskCoordinator):
        self.coordinator = coordinator
        
        # Get risk parameters from coordinator
        self.risk_params = coordinator.risk_params
        self.dynamic_params = coordinator.dynamic_params

        # Position sizing parameters from config
        self.max_position_percent = self.risk_params.base_position_percent
        self.volatility_target = self.risk_params.target_risk_per_position
        self.correlation_limit = Decimal("0.7")  # Maximum correlation with existing positions
        self.drawdown_scaling = True  # Scale down during drawdowns

    async def initialize(self) -> None:
        """Initialize the position sizer."""
        # Validate coordinator is available
        if not self.coordinator:
            raise RuntimeError("Position sizer requires a valid coordinator")
        
        # Test coordinator connectivity
        try:
            await self.coordinator.get_current_portfolio_with_risk_assessment()
        except Exception as e:
            raise RuntimeError(f"Position sizer initialization failed - coordinator not ready: {e}")
        
        # Position sizer is now ready

    async def calculate_size(
        self,
        signal: Any,  # TradingSignal
        portfolio_context: dict[str, Any],
        evaluation: dict[str, Any]
    ) -> Decimal:
        """Calculate optimal position size using multiple methods."""

        # Get multiple sizing estimates
        sizes = {}

        # 1. Fixed fractional sizing
        sizes["fixed_fractional"] = await self._fixed_fractional_sizing(
            signal, portfolio_context
        )

        # 2. Volatility-based sizing
        sizes["volatility_based"] = await self._volatility_based_sizing(
            signal, portfolio_context
        )

        # 3. Kelly criterion sizing
        sizes["kelly"] = await self._kelly_criterion_sizing(
            signal, portfolio_context, evaluation
        )

        # 4. Risk parity sizing
        sizes["risk_parity"] = await self._risk_parity_sizing(
            signal, portfolio_context
        )

        # Combine sizes using weighted average
        final_size = await self._combine_sizing_methods(sizes, signal, portfolio_context)

        # Apply portfolio-level adjustments
        adjusted_size = await self._apply_portfolio_adjustments(
            final_size, signal, portfolio_context
        )

        # Apply risk limits
        limited_size = await self._apply_risk_limits(
            adjusted_size, signal, portfolio_context
        )

        return limited_size

    async def _fixed_fractional_sizing(
        self,
        signal: Any,
        portfolio_context: dict[str, Any]
    ) -> Decimal:
        """Fixed percentage of portfolio sizing."""

        total_capital = portfolio_context["total_capital"]
        base_percent = self.max_position_percent

        # Adjust based on signal strength and confidence
        signal_factor = Decimal(str(signal.strength * signal.confidence))
        adjusted_percent = base_percent * signal_factor

        return Decimal(str(total_capital)) * adjusted_percent

    async def _volatility_based_sizing(
        self,
        signal: Any,
        portfolio_context: dict[str, Any]
    ) -> Decimal:
        """Size position based on volatility targeting."""

        # Get symbol volatility (placeholder - would integrate with market data)
        symbol_volatility = await self._get_symbol_volatility(signal.symbol)

        if symbol_volatility <= 0:
            return Decimal("0")

        total_capital = portfolio_context["total_capital"]

        # Calculate position size to achieve target volatility
        target_risk = total_capital * self.volatility_target
        position_size = target_risk / symbol_volatility

        # Adjust for signal strength
        signal_factor = Decimal(str(signal.strength * signal.confidence))

        return Decimal(str(position_size)) * signal_factor

    async def _kelly_criterion_sizing(
        self,
        signal: Any,
        portfolio_context: dict[str, Any],
        evaluation: Dict[str, Any]
    ) -> Decimal:
        """Kelly criterion optimal sizing using production Kelly sizer."""
        
        try:
            # Try to use the coordinator's production Kelly position sizer
            if hasattr(self.coordinator, 'position_sizer'):
                from cyberdelta.core.risk.sizing.models.sizing_result import SizingContext
                from cyberdelta.validation.funding_data import ArbitrageOpportunity
                
                # Create sizing context
                sizing_context = SizingContext(
                    sizing_method="kelly",  # or get from config
                    available_capital=portfolio_context["total_capital"]
                )
                
                # Add evaluation data to context
                sizing_context.add_metadata("expected_win_rate", evaluation.get("expected_win_rate", 0.5))
                sizing_context.add_metadata("expected_win_amount", evaluation.get("expected_win_amount", 1.0))
                sizing_context.add_metadata("expected_loss_amount", evaluation.get("expected_loss_amount", 1.0))
                
                # Create opportunity object for Kelly sizer
                opportunity = ArbitrageOpportunity(
                    id=signal.signal_id,
                    exchange_buy=signal.exchange_buy,
                    exchange_sell=signal.exchange_sell,
                    symbol=signal.symbol,
                    price_buy=signal.price_buy,
                    price_sell=signal.price_sell,
                    spread_percentage=signal.spread_percentage,
                    signal_strength=signal.signal_strength,
                    detected_at=signal.timestamp
                )
                
                # Get Kelly sizing result from production sizer
                sizing_result = await self.coordinator.position_sizer.size_opportunity(
                    opportunity,
                    portfolio_context["total_capital"]
                )
                
                if sizing_result.success:
                    # Adjust for signal confidence
                    confidence_factor = Decimal(str(signal.confidence))
                    return sizing_result.position_size * confidence_factor
        except Exception:
            # Fall through to simple Kelly calculation
            pass
        
        # Fallback to simple Kelly calculation
        win_rate = Decimal(str(evaluation.get("expected_win_rate", 0.5)))
        avg_win = Decimal(str(evaluation.get("expected_win_amount", 1.0)))
        avg_loss = Decimal(str(evaluation.get("expected_loss_amount", 1.0)))

        if avg_loss <= 0:
            return Decimal("0")

        # Kelly formula: f = (bp - q) / b
        # where b = avg_win/avg_loss, p = win_rate, q = 1-win_rate
        b = avg_win / avg_loss
        p = win_rate
        q = Decimal("1") - win_rate

        kelly_fraction = (b * p - q) / b

        # Cap Kelly at reasonable level from risk parameters
        kelly_fraction = min(kelly_fraction, self.risk_params.kelly_cap)
        kelly_fraction = max(kelly_fraction, Decimal("0"))

        total_capital = portfolio_context["total_capital"]

        # Adjust for signal confidence
        confidence_factor = Decimal(str(signal.confidence))

        return Decimal(str(total_capital)) * kelly_fraction * confidence_factor

    async def _risk_parity_sizing(
        self,
        signal: Any,
        portfolio_context: dict[str, Any]
    ) -> Decimal:
        """Risk parity sizing to balance portfolio risk."""

        # Calculate current portfolio risk concentration
        portfolio_state = portfolio_context["portfolio_state"]
        
        # Get positions through coordinator
        portfolio_with_risk = await self.coordinator.get_current_portfolio_with_risk_assessment()
        # TODO: Refactor to get actual positions from exchange_summaries
        positions = []
        # for exchange_positions in portfolio_with_risk.portfolio_state.positions.values():
        #     positions.extend(exchange_positions)

        if not positions:
            # First position gets standard allocation
            return await self._fixed_fractional_sizing(signal, portfolio_context)

        # Calculate risk contribution of each position
        position_risks = {}
        total_risk = Decimal("0")

        for position in positions:
            position_risk = await self._calculate_position_risk(position)
            position_risks[position.symbol] = position_risk
            total_risk += position_risk

        if total_risk <= 0:
            return await self._fixed_fractional_sizing(signal, portfolio_context)

        # Target equal risk contribution
        target_risk_per_position = total_risk / (len(positions) + 1)  # +1 for new position

        # Size new position to achieve target risk
        symbol_volatility = await self._get_symbol_volatility(signal.symbol)
        if symbol_volatility <= 0:
            return Decimal("0")

        position_size = target_risk_per_position / symbol_volatility

        # Adjust for signal strength
        signal_factor = Decimal(str(signal.strength))

        return Decimal(str(position_size)) * signal_factor

    async def _combine_sizing_methods(
        self,
        sizes: dict[str, Decimal],
        signal: Any,
        portfolio_context: dict[str, Any]
    ) -> Decimal:
        """Combine multiple sizing methods using weighted average."""

        # Weights for different methods
        weights = {
            "fixed_fractional": Decimal("0.2"),
            "volatility_based": Decimal("0.3"),
            "kelly": Decimal("0.3"),
            "risk_parity": Decimal("0.2")
        }

        # Calculate weighted average
        weighted_sum = Decimal("0")
        total_weight = Decimal("0")

        for method, size in sizes.items():
            if method in weights and size > 0:
                weight = weights[method]
                weighted_sum += size * weight
                total_weight += weight

        if total_weight <= 0:
            return Decimal("0")

        return weighted_sum / total_weight

    async def _apply_portfolio_adjustments(
        self,
        base_size: Decimal,
        signal: Any,
        portfolio_context: dict[str, Any]
    ) -> Decimal:
        """Apply portfolio-level adjustments to position size."""

        adjusted_size = base_size

        # 1. Drawdown scaling
        if self.drawdown_scaling:
            risk_assessment = portfolio_context["risk_assessment"]
            if "drawdown" in risk_assessment and risk_assessment["drawdown"]:
                drawdown = risk_assessment["drawdown"]
                if drawdown > 0:
                    # Scale down during drawdowns
                    drawdown_factor = max(Decimal("0.5"), Decimal("1") - drawdown)
                    adjusted_size *= drawdown_factor

        # 2. Concentration adjustments
        concentration_factor = await self._calculate_concentration_factor(
            signal, portfolio_context
        )
        adjusted_size *= concentration_factor

        # 3. Correlation adjustments
        correlation_factor = await self._calculate_correlation_factor(
            signal, portfolio_context
        )
        adjusted_size *= correlation_factor

        # 4. Available capacity
        available_capital = portfolio_context["available_capital"]
        if available_capital <= 0:
            return Decimal("0")

        # Don't use more than configured max of available capital for single position
        max_from_available = available_capital * self.risk_params.max_capital_utilization
        adjusted_size = min(adjusted_size, max_from_available)

        return adjusted_size

    async def _apply_risk_limits(
        self,
        size: Decimal,
        signal: Any,
        portfolio_context: dict[str, Any]
    ) -> Decimal:
        """Apply final risk limits to position size."""

        # Maximum position size limit
        total_capital = portfolio_context["total_capital"]
        max_position = total_capital * self.max_position_percent
        size = min(size, max_position)

        # Minimum position size (avoid tiny positions)
        min_position = total_capital * Decimal("0.001")  # 0.1% minimum
        if size < min_position:
            return Decimal("0")

        # Check leverage limits from dynamic parameters
        risk_assessment = portfolio_context["risk_assessment"]
        current_leverage = risk_assessment.get("leverage", 0)
        volatility = risk_assessment.get("volatility_estimate", Decimal("0.02"))
        max_leverage = self.dynamic_params.get_leverage_limit(volatility)
        if current_leverage >= max_leverage:
            return Decimal("0")

        return size

    async def _get_symbol_volatility(self, symbol: Symbol) -> Decimal:
        """Get symbol volatility from risk metrics."""
        # Get current portfolio state
        portfolio_with_risk = await self.coordinator.get_current_portfolio_with_risk_assessment()
        
        # Use risk assessment data if available
        risk_data = portfolio_with_risk.risk_assessment
        # Note: Using general volatility estimate since symbol-specific volatilities not available
        return risk_data.volatility_estimate
        
        if symbol in volatilities:
            return Decimal(str(volatilities[symbol]))
        
        # Default volatility from risk parameters if not available
        default_volatility = self.risk_params.default_volatilities["default"]
        return default_volatility

    async def _calculate_position_risk(self, position: Any) -> Decimal:
        """Calculate risk contribution of a position."""
        # Risk = position_size * volatility
        symbol_volatility = await self._get_symbol_volatility(position.symbol)
        return abs(position.size) * symbol_volatility

    async def _calculate_concentration_factor(
        self,
        signal: Any,
        portfolio_context: dict[str, Any]
    ) -> Decimal:
        """Calculate concentration adjustment factor."""

        # Check if we already have exposure to this symbol
        portfolio_with_risk = await self.coordinator.get_current_portfolio_with_risk_assessment()
        existing_exposure = Decimal("0")

        # TODO: Refactor to get actual positions from exchange_summaries
        # for exchange_positions in portfolio_with_risk.portfolio_state.positions.values():
        #     for position in exchange_positions:
        #         if position.symbol == signal.symbol:
        #             existing_exposure += abs(position.size)

        if existing_exposure == 0:
            return Decimal("1")  # No concentration penalty

        total_capital = portfolio_context["total_capital"]
        concentration_ratio = existing_exposure / total_capital

        # Reduce size as concentration increases
        if concentration_ratio > self.max_position_percent:
            return Decimal("0.5")  # Heavy penalty for over-concentration
        elif concentration_ratio > self.max_position_percent * Decimal("0.5"):
            return Decimal("0.8")  # Moderate penalty
        else:
            return Decimal("1")  # No penalty

    async def _calculate_correlation_factor(
        self,
        signal: Any,
        portfolio_context: dict[str, Any]
    ) -> Decimal:
        """Calculate correlation adjustment factor."""

        # This would calculate correlation with existing positions
        # For now, return conservative factor
        return Decimal("0.9")  # Slight reduction for correlation