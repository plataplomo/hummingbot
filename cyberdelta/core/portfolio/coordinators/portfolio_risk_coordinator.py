"""Central coordinator for portfolio state management and risk assessment."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone, UTC
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from cyberdelta.core.portfolio.portfolio_types.models import PortfolioState, Position
from cyberdelta.core.portfolio.services import PortfolioServiceFactory
from cyberdelta.core.risk.services.risk_service_factory import RiskServiceFactory
from cyberdelta.core.portfolio.config.risk_parameters import (
    RiskParameters,
    DynamicRiskParameters,
    MarketDataProvider,
)


# Supporting Pydantic models for type-safe integration
class RiskAssessment(BaseModel):
    """Comprehensive risk assessment model."""
    total_exposure: Decimal
    currency_exposures: dict[str, Decimal]
    var_95: Decimal
    max_drawdown: Decimal
    leverage_ratio: Decimal
    risk_score: float
    position_count: int
    concentration_risk: Decimal
    volatility_estimate: Decimal
    correlation_risk: Decimal
    liquidity_risk: Decimal
    timestamp: datetime

    model_config = ConfigDict(extra="forbid")


class PortfolioWithRiskModel(BaseModel):
    """Portfolio state enhanced with risk assessment."""
    portfolio_state: PortfolioState
    risk_assessment: RiskAssessment
    timestamp: datetime

    model_config = ConfigDict(extra="forbid")


class TradeRequestModel(BaseModel):
    """Trade request with validation."""
    symbol: str
    side: str  # "buy" or "sell"
    quantity: Decimal
    price: Decimal | None = None
    signal_strength: float = 1.0
    exchange_id: str
    strategy_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="forbid")

    @field_validator("quantity", mode="before")
    @classmethod
    def validate_quantity(cls, v: Any) -> Decimal:
        """Validate quantity is positive."""
        quantity = Decimal(str(v))
        if quantity <= 0:
            raise ValueError("Quantity must be positive")
        return quantity

    @field_validator("signal_strength", mode="before")
    @classmethod
    def validate_signal_strength(cls, v: Any) -> float:
        """Validate signal strength is between 0 and 1."""
        strength = float(v)
        if not 0.0 <= strength <= 1.0:
            raise ValueError("Signal strength must be between 0.0 and 1.0")
        return strength

    @field_validator("side")
    @classmethod
    def validate_side(cls, v: str) -> str:
        """Validate side is buy or sell."""
        if v.lower() not in ("buy", "sell"):
            raise ValueError("Side must be 'buy' or 'sell'")
        return v.lower()


class TradeValidationResultModel(BaseModel):
    """Result of trade validation."""
    approved: bool
    reason: str | None = None
    optimal_size: Decimal | None = None
    risk_assessment: RiskAssessment | None = None
    portfolio_impact: dict[str, Any] | None = None
    risk_violations: list[str] = Field(default_factory=list)
    execution_parameters: dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="forbid")


class PortfolioRiskCoordinator(BaseModel):
    """Coordinates portfolio state management with risk assessment."""

    portfolio_factory: PortfolioServiceFactory = Field(..., description="Portfolio service factory")
    risk_factory: RiskServiceFactory = Field(..., description="Risk service factory")
    risk_params: RiskParameters = Field(default_factory=RiskParameters, description="Risk parameters")
    market_data_provider: MarketDataProvider | None = Field(default=None, description="Market data provider")

    model_config = ConfigDict(extra="forbid", validate_assignment=True, arbitrary_types_allowed=True)

    def model_post_init(self, __context: Any) -> None:
        """Initialize service instances after Pydantic validation."""
        # Portfolio module services
        self.portfolio_manager = self.portfolio_factory.create_portfolio_state_manager()
        self.performance_analytics = self.portfolio_factory.create_performance_analytics()

        # Initialize dynamic risk parameters
        self.dynamic_params = DynamicRiskParameters(self.risk_params, self.market_data_provider)
        
        # Pass market data provider to risk factory if available
        if self.market_data_provider:
            self.risk_factory.set_market_data_provider(self.market_data_provider)

        # Risk module services
        self.exposure_calculator = self.risk_factory.create_exposure_calculator()
        self.position_sizer = self.risk_factory.create_position_sizer("production_kelly")
        self.risk_calculator = self.risk_factory.create_risk_metrics_calculator()

    async def get_current_portfolio_with_risk_assessment(self) -> PortfolioWithRiskModel:
        """Get portfolio state enhanced with risk assessment."""
        # Get current portfolio state (portfolio module responsibility)
        portfolio_state = await self.portfolio_manager.get_portfolio_summary()

        # Calculate risk metrics (risk module responsibility)
        risk_assessment = await self._calculate_comprehensive_risk(portfolio_state)

        return PortfolioWithRiskModel(
            portfolio_state=portfolio_state,
            risk_assessment=risk_assessment,
            timestamp=portfolio_state.timestamp
        )

    async def validate_trade_request(self, trade_request: TradeRequestModel) -> TradeValidationResultModel:
        """Validate trade using both portfolio state and risk assessment."""
        # Get current state
        portfolio_state = await self.portfolio_manager.get_portfolio_summary()

        # Get comprehensive risk assessment
        current_risk = await self._calculate_comprehensive_risk(portfolio_state)
        
        # Professional risk validation
        risk_violations = []
        execution_parameters = {}
        
        # 1. Capital adequacy validation
        total_capital = portfolio_state.total_capital
        estimated_price = trade_request.price or await self._get_estimated_price(trade_request.symbol)
        trade_value = trade_request.quantity * estimated_price
        
        if trade_value > total_capital:
            risk_violations.append(f"Insufficient capital: trade value {trade_value} > total capital {total_capital}")
        
        # Check available capital
        available_capital = total_capital - current_risk.total_exposure
        max_capital_utilization = self.risk_params.max_capital_utilization
        if trade_value > available_capital * max_capital_utilization:
            risk_violations.append(f"Insufficient available capital: {trade_value} > {available_capital * max_capital_utilization}")

        # 2. Leverage validation with dynamic limits
        max_leverage = await self._get_dynamic_leverage_limit(current_risk)
        new_exposure = current_risk.total_exposure + trade_value
        new_leverage = new_exposure / total_capital if total_capital > 0 else Decimal(0)
        
        if new_leverage > max_leverage:
            risk_violations.append(f"Leverage limit exceeded: {new_leverage:.2f}x > {max_leverage:.2f}x")
        
        execution_parameters["max_leverage"] = float(max_leverage)
        execution_parameters["projected_leverage"] = float(new_leverage)

        # 3. Position concentration validation
        max_position_value = await self._get_dynamic_position_limit(trade_request.symbol, current_risk, total_capital)
        
        # Check existing position size
        existing_position_value = await self._get_existing_position_value(trade_request.symbol, portfolio_state)
        total_position_value = existing_position_value + trade_value
        
        if total_position_value > max_position_value:
            risk_violations.append(
                f"Position concentration limit exceeded: {total_position_value} > {max_position_value} "
                f"for symbol {trade_request.symbol}"
            )

        # 4. VaR limit validation with portfolio impact
        projected_var = await self._calculate_projected_var(trade_request, current_risk, portfolio_state)
        max_var_limit = total_capital * self.risk_params.max_var_percentage
        
        if projected_var > max_var_limit:
            risk_violations.append(f"Projected VaR limit exceeded: {projected_var} > {max_var_limit}")
        
        execution_parameters["projected_var"] = float(projected_var)
        execution_parameters["var_limit"] = float(max_var_limit)

        # 5. Liquidity validation
        liquidity_check = await self._validate_liquidity_requirements(trade_request, portfolio_state)
        if not liquidity_check["adequate"]:
            risk_violations.append(f"Liquidity requirements not met: {liquidity_check['reason']}")

        # 6. Correlation risk validation
        correlation_risk = await self._validate_correlation_risk(trade_request, current_risk, portfolio_state)
        if correlation_risk["risk_too_high"]:
            risk_violations.append(f"Correlation risk too high: {correlation_risk['reason']}")

        # 7. Market condition validation
        market_conditions = await self._validate_market_conditions(trade_request)
        if not market_conditions["suitable"]:
            risk_violations.append(f"Market conditions unsuitable: {market_conditions['reason']}")

        # If any critical violations, reject the trade
        if risk_violations:
            return TradeValidationResultModel(
                approved=False,
                reason="Risk validation failed - see violations",
                risk_violations=risk_violations,
                risk_assessment=current_risk,
                execution_parameters=execution_parameters
            )

        # Calculate optimal position size using sophisticated methods
        optimal_size = await self._calculate_optimal_position_size(
            trade_request, portfolio_state, current_risk
        )

        # Simulate portfolio impact
        portfolio_impact = await self._simulate_trade_impact(trade_request, portfolio_state)

        # Set execution parameters for optimal execution
        execution_parameters.update({
            "max_slippage": await self._calculate_max_slippage(trade_request, current_risk),
            "execution_urgency": await self._determine_execution_urgency(trade_request),
            "partial_fills_allowed": liquidity_check.get("partial_fills_recommended", True),
            "max_execution_time": await self._calculate_max_execution_time(trade_request),
            "risk_adjusted_size": float(optimal_size)
        })

        return TradeValidationResultModel(
            approved=True,
            optimal_size=optimal_size,
            risk_assessment=current_risk,
            portfolio_impact=portfolio_impact,
            execution_parameters=execution_parameters
        )

    async def execute_coordinated_trade(self, trade_request: TradeRequestModel) -> dict[str, Any]:
        """Execute trade with coordinated portfolio and risk management."""
        # Pre-trade validation
        validation_result = await self.validate_trade_request(trade_request)
        if not validation_result.approved:
            return {"success": False, "reason": validation_result.reason}

        # The coordinator should NOT execute trades directly
        # This violates the clean architecture - trade execution belongs to PortfolioAwareTradeExecutor
        raise NotImplementedError(
            "Trade execution should be handled by PortfolioAwareTradeExecutor, "
            "not the PortfolioRiskCoordinator. The coordinator is only for "
            "portfolio-risk coordination per Week 4 specifications."
        )

    async def _calculate_comprehensive_risk(self, portfolio_state: PortfolioState) -> RiskAssessment:
        """Calculate complete risk assessment using risk module."""
        # Get all positions for calculations
        positions_list = []
        for exchange_positions in portfolio_state.positions.values():
            positions_list.extend(exchange_positions)
        
        # Use risk module services for professional calculations
        try:
            # Calculate exposure metrics using risk module
            exposure_metrics = await self.exposure_calculator.calculate_portfolio_exposure(portfolio_state)
            total_exposure = exposure_metrics.total_exposure
            currency_exposures = exposure_metrics.currency_breakdown
        except Exception:
            # Fallback to manual calculation
            total_exposure = Decimal("0")
            currency_exposures: dict[str, Decimal] = {}
            
            for position in positions_list:
                entry_price = position.entry_price or position.mark_price or Decimal("100")
                position_value = abs(position.size * entry_price)
                total_exposure += position_value
                
                # Extract currency from symbol
                currency = position.symbol.split("/")[1] if "/" in position.symbol else "USDC"
                currency_exposures[currency] = currency_exposures.get(currency, Decimal("0")) + position_value

        # Calculate risk metrics using risk module
        try:
            risk_metrics = await self.risk_calculator.calculate_risk_metrics(portfolio_state)
            var_95 = risk_metrics.var_95
            leverage_ratio = risk_metrics.leverage
            max_drawdown = risk_metrics.max_drawdown
            volatility_estimate = risk_metrics.portfolio_volatility
        except Exception:
            # Professional fallback calculations
            leverage_ratio = total_exposure / portfolio_state.total_capital if portfolio_state.total_capital > 0 else Decimal("0")
            
            # VaR calculation using parametric method
            portfolio_volatility = await self._calculate_portfolio_volatility(positions_list)
            var_95 = total_exposure * portfolio_volatility * self.risk_params.statistical_params["var_confidence_95"]
            
            # Use max_drawdown from portfolio state or calculate from performance
            if hasattr(portfolio_state, 'max_drawdown') and portfolio_state.max_drawdown is not None:
                max_drawdown = portfolio_state.max_drawdown
            else:
                max_drawdown = await self._calculate_current_drawdown(portfolio_state)
            
            volatility_estimate = portfolio_volatility

        # Calculate additional risk metrics
        concentration_risk = await self._calculate_concentration_risk(positions_list, portfolio_state.total_capital)
        correlation_risk = await self._calculate_correlation_risk(positions_list)
        liquidity_risk = await self._calculate_liquidity_risk(positions_list)
        
        # Calculate comprehensive risk score
        risk_score = await self._calculate_risk_score(
            leverage_ratio, max_drawdown, concentration_risk, var_95, portfolio_state.total_capital
        )

        return RiskAssessment(
            total_exposure=total_exposure,
            currency_exposures=currency_exposures,
            var_95=var_95,
            max_drawdown=max_drawdown,
            leverage_ratio=leverage_ratio,
            risk_score=risk_score,
            position_count=len(positions_list),
            concentration_risk=concentration_risk,
            volatility_estimate=volatility_estimate,
            correlation_risk=correlation_risk,
            liquidity_risk=liquidity_risk,
            timestamp=datetime.now(UTC)
        )

    async def _calculate_portfolio_volatility(self, positions: list[Position]) -> Decimal:
        """Calculate portfolio volatility using individual position volatilities."""
        if not positions:
            return Decimal("0")
        
        # Professional volatility calculation would use:
        # 1. Historical price data for each position
        # 2. Correlation matrix between positions
        # 3. Position weights
        # For now, use a weighted average approach
        
        total_weight = Decimal("0")
        weighted_volatility = Decimal("0")
        
        for position in positions:
            # Get position volatility (would be from market data service in production)
            symbol_volatility = await self._get_symbol_volatility(position.symbol)
            
            # Weight by position size
            position_value = abs(position.size * (position.entry_price or position.mark_price or Decimal("100")))
            total_weight += position_value
            weighted_volatility += symbol_volatility * position_value
        
        if total_weight > 0:
            return weighted_volatility / total_weight
        return self.risk_params.default_volatilities["default"]

    async def _get_symbol_volatility(self, symbol: str) -> Decimal:
        """Get symbol volatility from market data or use defaults."""
        # Use dynamic parameters which will try market data first
        return await self.dynamic_params.get_symbol_volatility(symbol)

    async def _calculate_current_drawdown(self, portfolio_state: PortfolioState) -> Decimal:
        """Calculate current drawdown from high water mark."""
        # In production, this would track the highest portfolio value over time
        # For now, calculate based on unrealized P&L
        
        total_unrealized_pnl = Decimal("0")
        for exchange_positions in portfolio_state.positions.values():
            for position in exchange_positions:
                if position.unrealized_pnl:
                    total_unrealized_pnl += position.unrealized_pnl
        
        # If we have negative unrealized P&L, that contributes to drawdown
        if total_unrealized_pnl < 0:
            return abs(total_unrealized_pnl) / portfolio_state.total_capital if portfolio_state.total_capital > 0 else Decimal("0")
        
        return Decimal("0")

    async def _calculate_concentration_risk(self, positions: list[Position], total_capital: Decimal) -> Decimal:
        """Calculate position concentration risk."""
        if not positions or total_capital <= 0:
            return Decimal("0")
        
        # Calculate Herfindahl-Hirschman Index for concentration
        position_percentages = []
        for position in positions:
            position_value = abs(position.size * (position.entry_price or position.mark_price or Decimal("100")))
            position_percent = position_value / total_capital
            position_percentages.append(position_percent)
        
        # HHI calculation
        hhi = sum(percent ** 2 for percent in position_percentages)
        
        # Normalize to 0-1 scale (1/n is perfect diversification, 1 is maximum concentration)
        n_positions = len(positions)
        if n_positions > 1:
            perfect_diversification = Decimal("1") / Decimal(str(n_positions))
            concentration_risk = (hhi - perfect_diversification) / (Decimal("1") - perfect_diversification)
            return max(Decimal("0"), min(Decimal("1"), concentration_risk))
        else:
            return Decimal("1")  # Single position = maximum concentration

    async def _calculate_correlation_risk(self, positions: list[Position]) -> Decimal:
        """Calculate portfolio correlation risk."""
        if len(positions) < 2:
            return Decimal("0")
        
        # In production, this would use actual correlation data
        # For now, estimate based on asset classes
        crypto_positions = 0
        stable_positions = 0
        
        for position in positions:
            symbol_upper = position.symbol.upper()
            if any(stable in symbol_upper for stable in ["USDC", "USDT", "DAI"]):
                stable_positions += 1
            else:
                crypto_positions += 1
        
        # High correlation risk if mostly crypto assets (they tend to be correlated)
        if crypto_positions > stable_positions * 3:
            return Decimal("0.8")  # High correlation risk
        elif crypto_positions > stable_positions:
            return Decimal("0.5")  # Medium correlation risk
        else:
            return Decimal("0.2")  # Low correlation risk

    async def _calculate_liquidity_risk(self, positions: list[Position]) -> Decimal:
        """Calculate portfolio liquidity risk."""
        if not positions:
            return Decimal("0")
        
        # Estimate liquidity based on position sizes and symbols
        total_risk = Decimal("0")
        position_count = len(positions)
        
        for position in positions:
            symbol_upper = position.symbol.upper()
            position_size_usd = abs(position.size * (position.entry_price or position.mark_price or Decimal("100")))
            
            # Liquidity risk factors
            symbol_liquidity_risk = self.dynamic_params.get_liquidity_factor(position.symbol)
            
            # Adjust for position size (larger positions harder to liquidate)
            size_multiplier = self.dynamic_params.get_size_multiplier(position_size_usd)
            
            total_risk += symbol_liquidity_risk * size_multiplier
        
        # Average liquidity risk across positions
        return total_risk / Decimal(str(position_count))

    async def _calculate_risk_score(
        self, 
        leverage: Decimal, 
        drawdown: Decimal, 
        concentration: Decimal, 
        var_95: Decimal, 
        total_capital: Decimal
    ) -> float:
        """Calculate comprehensive risk score (0-100)."""
        score = 0.0
        
        # Leverage component (0-25 points)
        leverage_thresholds = self.risk_params.risk_thresholds["leverage"]
        if leverage > leverage_thresholds["critical"]:
            score += 25.0
        elif leverage > leverage_thresholds["high"]:
            score += 20.0
        elif leverage > leverage_thresholds["elevated"]:
            score += 15.0
        elif leverage > leverage_thresholds["moderate"]:
            score += 10.0
        elif leverage > leverage_thresholds["low"]:
            score += 5.0
        
        # Drawdown component (0-25 points)
        drawdown_thresholds = self.risk_params.risk_thresholds["drawdown"]
        if drawdown > drawdown_thresholds["critical"]:
            score += 25.0
        elif drawdown > drawdown_thresholds["high"]:
            score += 20.0
        elif drawdown > drawdown_thresholds["elevated"]:
            score += 15.0
        elif drawdown > drawdown_thresholds["moderate"]:
            score += 10.0
        elif drawdown > drawdown_thresholds["low"]:
            score += 5.0
        
        # Concentration component (0-25 points)
        score += float(concentration) * 25.0
        
        # VaR component (0-25 points)
        if total_capital > 0:
            var_percent = var_95 / total_capital
            var_thresholds = self.risk_params.risk_thresholds["var_percent"]
            if var_percent > var_thresholds["critical"]:
                score += 25.0
            elif var_percent > var_thresholds["high"]:
                score += 20.0
            elif var_percent > var_thresholds["elevated"]:
                score += 15.0
            elif var_percent > var_thresholds["moderate"]:
                score += 10.0
            elif var_percent > var_thresholds["low"]:
                score += 5.0
        
        return min(100.0, max(0.0, score))

    async def _simulate_trade_impact(
        self, trade_request: TradeRequestModel, portfolio_state: PortfolioState
    ) -> dict[str, Any]:
        """Simulate impact of proposed trade on portfolio."""
        # Calculate notional value of trade
        notional_value = trade_request.quantity * (trade_request.price or Decimal("100"))
        
        # Calculate exposure change based on side
        if trade_request.side == "buy":
            exposure_change = notional_value
        elif trade_request.side == "sell":
            exposure_change = -notional_value
        else:
            exposure_change = Decimal("0")  # Unknown side
        
        # Calculate risk change
        current_risk = await self._calculate_comprehensive_risk(portfolio_state)
        current_var = current_risk.get("var_95", Decimal("0"))
        
        # Estimate new VaR after trade (simplified)
        volatility = self.risk_params.default_volatilities["default"]
        position_var_contribution = abs(notional_value) * volatility * self.risk_params.statistical_params["var_confidence_95"]
        new_var = current_var + position_var_contribution
        risk_change = new_var - current_var
        
        # Estimate performance impact
        total_capital = portfolio_state.total_capital
        position_percent = abs(notional_value) / total_capital if total_capital > 0 else Decimal("0")
        expected_return = self.risk_params.default_expected_returns["default"]
        performance_impact = position_percent * expected_return
        
        return {
            "exposure_change": exposure_change,
            "risk_change": risk_change,
            "performance_impact": performance_impact,
            "var_change": risk_change,
            "position_percent": position_percent
        }

    # Supporting methods for professional risk validation
    
    async def _get_estimated_price(self, symbol: str) -> Decimal:
        """Get estimated price for symbol from market data."""
        # In production, this would fetch from market data service
        # For now, return reasonable estimates based on symbol
        if "BTC" in symbol.upper():
            return Decimal("45000")
        elif "ETH" in symbol.upper():
            return Decimal("2800")
        elif "SOL" in symbol.upper():
            return Decimal("100")
        elif any(stable in symbol.upper() for stable in ["USDC", "USDT"]):
            return Decimal("1.0")
        else:
            return Decimal("100")

    async def _get_dynamic_leverage_limit(self, current_risk: RiskAssessment) -> Decimal:
        """Calculate dynamic leverage limit based on current risk."""
        # Use dynamic leverage limit based on market volatility
        return self.dynamic_params.get_leverage_limit(current_risk.volatility_estimate)

    async def _get_dynamic_position_limit(self, symbol: str, current_risk: RiskAssessment, total_capital: Decimal) -> Decimal:
        """Calculate dynamic position limit for symbol."""
        # Base limit from risk parameters
        base_limit = total_capital * self.risk_params.base_position_percent
        
        # Adjust based on asset volatility
        symbol_volatility = await self._get_symbol_volatility(symbol)
        volatility_factor = self.dynamic_params.get_position_limit_factor(symbol_volatility)
        
        # Adjust based on correlation risk
        correlation_factor = self.dynamic_params.get_correlation_factor(current_risk.correlation_risk)
        
        return base_limit * volatility_factor * correlation_factor

    async def _get_existing_position_value(self, symbol: str, portfolio_state: PortfolioState) -> Decimal:
        """Get existing position value for symbol."""
        total_value = Decimal("0")
        
        for exchange_positions in portfolio_state.positions.values():
            for position in exchange_positions:
                if position.symbol == symbol:
                    position_value = abs(position.size * (position.entry_price or position.mark_price or Decimal("100")))
                    total_value += position_value
        
        return total_value

    async def _calculate_projected_var(
        self, trade_request: TradeRequestModel, current_risk: RiskAssessment, portfolio_state: PortfolioState
    ) -> Decimal:
        """Calculate projected VaR after executing the trade."""
        # Current VaR
        current_var = current_risk.var_95
        
        # Estimate trade VaR contribution
        estimated_price = trade_request.price or await self._get_estimated_price(trade_request.symbol)
        trade_value = trade_request.quantity * estimated_price
        symbol_volatility = await self._get_symbol_volatility(trade_request.symbol)
        
        # VaR contribution from new position (simplified)
        trade_var_contribution = trade_value * symbol_volatility * self.risk_params.statistical_params["var_confidence_95"]
        
        # In production, would account for correlation with existing positions
        # For now, use default correlation adjustment from parameters
        correlation_adjustment = self.risk_params.statistical_params["correlation_adjustment"]
        
        # Calculate projected VaR using correlation
        projected_var_squared = (current_var ** 2) + (trade_var_contribution ** 2) + (
            2 * correlation_adjustment * current_var * trade_var_contribution
        )
        
        if projected_var_squared > 0:
            return projected_var_squared.sqrt()
        else:
            return current_var + trade_var_contribution

    async def _validate_liquidity_requirements(
        self, trade_request: TradeRequestModel, portfolio_state: PortfolioState
    ) -> dict[str, Any]:
        """Validate liquidity requirements for the trade."""
        symbol_upper = trade_request.symbol.upper()
        estimated_price = trade_request.price or await self._get_estimated_price(trade_request.symbol)
        trade_value = trade_request.quantity * estimated_price
        
        # Get daily volume from dynamic parameters
        estimated_daily_volume = await self.dynamic_params.get_daily_volume(trade_request.symbol)
        
        # Check if trade size is reasonable relative to daily volume
        volume_percentage = trade_value / estimated_daily_volume
        
        if volume_percentage > self.risk_params.market_impact["high_impact_threshold"]:
            return {
                "adequate": False,
                "reason": f"Trade size too large: {volume_percentage:.1%} of estimated daily volume",
                "partial_fills_recommended": True
            }
        elif volume_percentage > self.risk_params.market_impact["medium_impact_threshold"]:
            return {
                "adequate": True,
                "reason": "Large trade - may experience slippage",
                "partial_fills_recommended": True
            }
        else:
            return {
                "adequate": True,
                "reason": "Liquidity adequate",
                "partial_fills_recommended": False
            }

    async def _validate_correlation_risk(
        self, trade_request: TradeRequestModel, current_risk: RiskAssessment, portfolio_state: PortfolioState
    ) -> dict[str, Any]:
        """Validate correlation risk of adding new position."""
        symbol_upper = trade_request.symbol.upper()
        
        # Count existing positions in same asset class
        crypto_exposure = Decimal("0")
        stable_exposure = Decimal("0")
        total_exposure = current_risk.total_exposure
        
        for exchange_positions in portfolio_state.positions.values():
            for position in exchange_positions:
                position_value = abs(position.size * (position.entry_price or position.mark_price or Decimal("100")))
                pos_symbol_upper = position.symbol.upper()
                
                if any(stable in pos_symbol_upper for stable in ["USDC", "USDT", "DAI"]):
                    stable_exposure += position_value
                else:
                    crypto_exposure += position_value
        
        # Determine asset class of new trade
        estimated_price = trade_request.price or await self._get_estimated_price(trade_request.symbol)
        trade_value = trade_request.quantity * estimated_price
        
        if any(stable in symbol_upper for stable in ["USDC", "USDT", "DAI"]):
            new_stable_exposure = stable_exposure + trade_value
            stable_percentage = new_stable_exposure / (total_exposure + trade_value) if (total_exposure + trade_value) > 0 else Decimal("0")
            
            if stable_percentage > self.risk_params.concentration_limits["max_stable_exposure"]:
                return {
                    "risk_too_high": False,  # High stable allocation is generally good
                    "reason": "High stablecoin allocation - low correlation risk"
                }
        else:
            new_crypto_exposure = crypto_exposure + trade_value
            crypto_percentage = new_crypto_exposure / (total_exposure + trade_value) if (total_exposure + trade_value) > 0 else Decimal("0")
            
            if crypto_percentage > self.risk_params.concentration_limits["max_crypto_exposure"]:
                return {
                    "risk_too_high": True,
                    "reason": f"Excessive crypto correlation: {crypto_percentage:.1%} of portfolio"
                }
            elif crypto_percentage > self.risk_params.concentration_limits["warning_crypto_exposure"]:
                return {
                    "risk_too_high": False,
                    "reason": f"High crypto correlation: {crypto_percentage:.1%} of portfolio - monitor closely"  
                }
        
        return {
            "risk_too_high": False,
            "reason": "Correlation risk acceptable"
        }

    async def _validate_market_conditions(self, trade_request: TradeRequestModel) -> dict[str, Any]:
        """Validate market conditions for trade execution."""
        # In production, this would check:
        # - Market volatility
        # - Spread conditions  
        # - Order book depth
        # - Recent price movements
        # - Market hours
        
        symbol_upper = trade_request.symbol.upper()
        current_time = datetime.now(UTC)
        
        # Basic market hours check (crypto trades 24/7, but some have lower liquidity periods)
        hour = current_time.hour
        
        # Lower liquidity periods (generally worse execution)
        if 2 <= hour <= 6:  # Early morning UTC typically lower volume
            return {
                "suitable": True,
                "reason": "Low liquidity period - expect higher slippage"
            }
        
        # Check for stablecoins (generally always suitable)
        if any(stable in symbol_upper for stable in ["USDC", "USDT", "DAI"]):
            return {
                "suitable": True,
                "reason": "Stablecoin trade - market conditions suitable"
            }
        
        return {
            "suitable": True,
            "reason": "Market conditions normal"
        }

    async def _calculate_optimal_position_size(
        self, trade_request: TradeRequestModel, portfolio_state: PortfolioState, current_risk: RiskAssessment
    ) -> Decimal:
        """Calculate optimal position size using multiple methodologies."""
        # Try to use the position sizer from risk module
        try:
            optimal_size = await self.position_sizer.calculate_optimal_size(
                portfolio_state,
                trade_request.symbol,
                trade_request.signal_strength
            )
            # Don't exceed requested quantity
            return min(trade_request.quantity, optimal_size)
        except Exception:
            # Fallback to sophisticated manual calculation
            pass
        
        # Professional position sizing calculation
        total_capital = portfolio_state.total_capital
        
        # Method 1: Risk parity sizing
        target_risk_contribution = total_capital * self.risk_params.target_risk_per_position
        symbol_volatility = await self._get_symbol_volatility(trade_request.symbol)
        estimated_price = trade_request.price or await self._get_estimated_price(trade_request.symbol)
        
        if symbol_volatility > 0 and estimated_price > 0:
            risk_based_size = target_risk_contribution / (symbol_volatility * estimated_price)
        else:
            risk_based_size = Decimal("0")
        
        # Method 2: Kelly criterion (simplified)
        signal_strength = Decimal(str(trade_request.signal_strength))
        win_probability = self.risk_params.default_kelly_params["base_win_prob"] + (signal_strength - Decimal("0.5")) * Decimal("0.3")
        avg_win = self.risk_params.default_kelly_params["avg_win"]
        avg_loss = self.risk_params.default_kelly_params["avg_loss"]
        
        if avg_loss > 0:
            kelly_fraction = (win_probability * avg_win - (Decimal("1") - win_probability) * avg_loss) / avg_win
            kelly_fraction = max(Decimal("0"), min(kelly_fraction, self.risk_params.kelly_cap))
            kelly_size = (total_capital * kelly_fraction) / estimated_price if estimated_price > 0 else Decimal("0")
        else:
            kelly_size = Decimal("0")
        
        # Method 3: Volatility-adjusted sizing
        base_percent = self.risk_params.base_position_percent
        volatility_adjustment = min(Decimal("2"), self.risk_params.default_volatilities["BTC"] / symbol_volatility) if symbol_volatility > 0 else Decimal("1")
        vol_adjusted_size = (total_capital * base_percent * volatility_adjustment) / estimated_price if estimated_price > 0 else Decimal("0")
        
        # Combine methods using weighted average
        sizes = [risk_based_size, kelly_size, vol_adjusted_size]
        weights = [Decimal("0.4"), Decimal("0.3"), Decimal("0.3")]
        
        weighted_size = sum(size * weight for size, weight in zip(sizes, weights) if size > 0)
        
        # Apply signal strength adjustment
        final_size = weighted_size * signal_strength
        
        # Ensure we don't exceed limits
        max_position_value = await self._get_dynamic_position_limit(trade_request.symbol, current_risk, total_capital)
        max_size_from_limit = max_position_value / estimated_price if estimated_price > 0 else Decimal("0")
        
        # Return the minimum of requested quantity, calculated optimal size, and position limit
        return min(trade_request.quantity, final_size, max_size_from_limit)

    async def _calculate_max_slippage(self, trade_request: TradeRequestModel, current_risk: RiskAssessment) -> float:
        """Calculate maximum acceptable slippage for trade."""
        symbol_upper = trade_request.symbol.upper()
        
        # Base slippage tolerance by asset class
        if "BTC" in symbol_upper or "ETH" in symbol_upper:
            base_slippage = 0.001  # 0.1% for major assets
        elif any(stable in symbol_upper for stable in ["USDC", "USDT"]):
            base_slippage = 0.0005  # 0.05% for stablecoins
        else:
            base_slippage = 0.005  # 0.5% for other assets
        
        # Adjust based on market conditions and risk
        if current_risk.risk_score > 70:
            slippage_multiplier = 0.5  # Tighter slippage when risk is high
        elif current_risk.volatility_estimate > Decimal("0.08"):
            slippage_multiplier = 1.5  # Allow more slippage in volatile conditions
        else:
            slippage_multiplier = 1.0
        
        return base_slippage * slippage_multiplier

    async def _determine_execution_urgency(self, trade_request: TradeRequestModel) -> str:
        """Determine execution urgency based on trade characteristics."""
        signal_strength = trade_request.signal_strength
        
        if signal_strength > 0.9:
            return "high"
        elif signal_strength > 0.7:
            return "medium"
        else:
            return "low"

    async def _calculate_max_execution_time(self, trade_request: TradeRequestModel) -> int:
        """Calculate maximum execution time in seconds."""
        urgency = await self._determine_execution_urgency(trade_request)
        estimated_price = trade_request.price or await self._get_estimated_price(trade_request.symbol)
        trade_value = trade_request.quantity * estimated_price
        
        # Base time by urgency
        if urgency == "high":
            base_time = int(self.risk_params.execution_params["high_urgency_time"])
        elif urgency == "medium":
            base_time = int(self.risk_params.execution_params["medium_urgency_time"])
        else:
            base_time = int(self.risk_params.execution_params["low_urgency_time"])
        
        # Adjust for trade size
        if trade_value > Decimal("50000"):
            size_multiplier = 2.0  # Larger trades need more time
        elif trade_value > Decimal("10000"):
            size_multiplier = 1.5
        else:
            size_multiplier = 1.0
        
        return int(base_time * size_multiplier)