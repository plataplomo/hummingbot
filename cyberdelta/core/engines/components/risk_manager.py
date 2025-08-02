"""Advanced risk management with real-time portfolio monitoring."""
from __future__ import annotations

import asyncio
from decimal import Decimal
from datetime import datetime, timedelta
from typing import Any
from dataclasses import dataclass
from enum import Enum

from cyberdelta.core.portfolio.coordinators.portfolio_risk_coordinator import PortfolioRiskCoordinator


class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class RiskLimit:
    name: str
    limit_value: Decimal
    current_value: Decimal
    risk_level: RiskLevel
    violation: bool
    description: str


@dataclass
class RiskCheckResult:
    approved: bool
    risk_score: float
    reasons: list[str]
    parameters: dict[str, Any]
    limits_checked: list[RiskLimit]


class AdvancedRiskManager:
    """Advanced risk management with comprehensive portfolio integration."""

    def __init__(self, coordinator: PortfolioRiskCoordinator):
        self.coordinator = coordinator

        # Risk limits configuration
        self.risk_limits = {
            "max_total_exposure": Decimal("100000"),  # $100k max exposure
            "max_position_size": Decimal("10000"),    # $10k max per position
            "max_leverage": Decimal("3"),             # 3x max leverage
            "max_correlation": Decimal("0.8"),        # 80% max correlation
            "max_drawdown": Decimal("0.15"),          # 15% max drawdown
            "max_var": Decimal("5000"),               # $5k max daily VaR
            "max_positions": 25,                      # Max 25 positions
            "min_liquidity_ratio": Decimal("0.1"),   # 10% min cash
            "max_sector_concentration": Decimal("0.4"), # 40% max per sector
            "max_currency_exposure": Decimal("0.6")   # 60% max single currency
        }

        # Dynamic risk adjustments
        self.volatility_regime = "normal"  # normal, high, extreme
        self.market_stress_factor = Decimal("1.0")
        self.recent_performance: list[Any] = []

        # Risk monitoring state
        self.risk_violations: list[Any] = []
        self.risk_alerts: list[Any] = []
        self.last_risk_check = datetime.utcnow()

    async def initialize(self) -> None:
        """Initialize risk manager."""
        # Load historical performance for context
        await self._load_performance_history()

        # Initialize volatility regime detection
        await self._update_volatility_regime()

    async def check_trade_risk(
        self,
        signal: Any,  # TradingSignal
        position_size: Decimal,
        portfolio_context: dict[str, Any]
    ) -> RiskCheckResult:
        """Comprehensive risk check for a potential trade."""

        reasons = []
        limits_checked = []
        risk_score = 0.0

        # 1. Position size limits
        position_limit = await self._check_position_size_limit(
            signal, position_size, portfolio_context
        )
        limits_checked.append(position_limit)
        if position_limit.violation:
            reasons.append(f"Position size exceeds limit: {position_limit.description}")
            risk_score += 30

        # 2. Total exposure limits
        exposure_limit = await self._check_exposure_limit(
            position_size, portfolio_context
        )
        limits_checked.append(exposure_limit)
        if exposure_limit.violation:
            reasons.append(f"Total exposure limit exceeded: {exposure_limit.description}")
            risk_score += 40

        # 3. Leverage limits
        leverage_limit = await self._check_leverage_limit(
            position_size, portfolio_context
        )
        limits_checked.append(leverage_limit)
        if leverage_limit.violation:
            reasons.append(f"Leverage limit exceeded: {leverage_limit.description}")
            risk_score += 35

        # 4. Correlation limits
        correlation_limit = await self._check_correlation_limit(
            signal, position_size, portfolio_context
        )
        limits_checked.append(correlation_limit)
        if correlation_limit.violation:
            reasons.append(f"Correlation limit exceeded: {correlation_limit.description}")
            risk_score += 25

        # 5. Concentration limits
        concentration_limit = await self._check_concentration_limit(
            signal, position_size, portfolio_context
        )
        limits_checked.append(concentration_limit)
        if concentration_limit.violation:
            reasons.append(f"Concentration limit exceeded: {concentration_limit.description}")
            risk_score += 20

        # 6. VaR limits
        var_limit = await self._check_var_limit(
            signal, position_size, portfolio_context
        )
        limits_checked.append(var_limit)
        if var_limit.violation:
            reasons.append(f"VaR limit exceeded: {var_limit.description}")
            risk_score += 45

        # 7. Liquidity limits
        liquidity_limit = await self._check_liquidity_limit(
            position_size, portfolio_context
        )
        limits_checked.append(liquidity_limit)
        if liquidity_limit.violation:
            reasons.append(f"Liquidity limit exceeded: {liquidity_limit.description}")
            risk_score += 30

        # 8. Market stress adjustments
        stress_adjustment = await self._apply_market_stress_adjustment(risk_score)
        risk_score += stress_adjustment

        # Determine approval
        approved = risk_score < 50  # Risk score threshold

        # Risk parameters for execution
        parameters = {
            "max_slippage": self._calculate_max_slippage(risk_score),
            "timeout_seconds": self._calculate_timeout(risk_score),
            "retry_attempts": self._calculate_retry_attempts(risk_score),
            "partial_fill_ok": risk_score < 30
        }

        return RiskCheckResult(
            approved=approved,
            risk_score=risk_score,
            reasons=reasons,
            parameters=parameters,
            limits_checked=limits_checked
        )

    async def check_all_limits(self) -> dict[str, Any]:
        """Check all portfolio-level risk limits."""

        portfolio_context = await self._get_portfolio_context()
        violations = []
        warnings = []

        # Check each limit
        for limit_name, limit_value in self.risk_limits.items():
            violation = await self._check_specific_limit(
                limit_name, limit_value, portfolio_context
            )

            if violation["violated"]:
                violations.append(violation)
            elif violation["warning"]:
                warnings.append(violation)

        return {
            "violations": violations,
            "warnings": warnings,
            "total_risk_score": self._calculate_total_risk_score(violations, warnings),
            "risk_level": self._determine_risk_level(violations, warnings)
        }

    async def _check_position_size_limit(
        self,
        signal: Any,  # TradingSignal
        position_size: Decimal,
        portfolio_context: dict[str, Any]
    ) -> RiskLimit:
        """Check individual position size limits."""

        max_position = Decimal(str(self.risk_limits["max_position_size"]))

        # Adjust for volatility regime
        if self.volatility_regime == "high":
            max_position *= Decimal("0.8")
        elif self.volatility_regime == "extreme":
            max_position *= Decimal("0.5")

        violation = position_size > max_position

        return RiskLimit(
            name="position_size",
            limit_value=max_position,
            current_value=position_size,
            risk_level=RiskLevel.HIGH if violation else RiskLevel.LOW,
            violation=violation,
            description=f"Position size {position_size} vs limit {max_position}"
        )

    async def _check_exposure_limit(
        self,
        additional_exposure: Decimal,
        portfolio_context: dict[str, Any]
    ) -> RiskLimit:
        """Check total exposure limits."""

        risk_assessment = portfolio_context["risk_assessment"]
        current_exposure = risk_assessment.get("total_exposure", Decimal("0"))
        new_total_exposure = current_exposure + additional_exposure
        max_exposure = Decimal(str(self.risk_limits["max_total_exposure"]))

        # Adjust for market stress
        adjusted_max = max_exposure * (Decimal("2") - self.market_stress_factor)

        violation = new_total_exposure > adjusted_max

        return RiskLimit(
            name="total_exposure",
            limit_value=adjusted_max,
            current_value=new_total_exposure,
            risk_level=RiskLevel.CRITICAL if violation else RiskLevel.LOW,
            violation=violation,
            description=f"Total exposure {new_total_exposure} vs limit {adjusted_max}"
        )

    async def _check_leverage_limit(
        self,
        additional_exposure: Decimal,
        portfolio_context: dict[str, Any]
    ) -> RiskLimit:
        """Check leverage limits."""

        total_capital = portfolio_context["total_capital"]
        risk_assessment = portfolio_context["risk_assessment"]
        current_exposure = risk_assessment.get("total_exposure", Decimal("0"))
        new_total_exposure = current_exposure + additional_exposure

        new_leverage = new_total_exposure / total_capital if total_capital > 0 else Decimal("0")
        max_leverage = Decimal(str(self.risk_limits["max_leverage"]))

        # Reduce leverage limit during high volatility
        if self.volatility_regime == "high":
            max_leverage *= Decimal("0.8")
        elif self.volatility_regime == "extreme":
            max_leverage *= Decimal("0.6")

        violation = new_leverage > max_leverage

        return RiskLimit(
            name="leverage",
            limit_value=max_leverage,
            current_value=new_leverage,
            risk_level=RiskLevel.HIGH if violation else RiskLevel.LOW,
            violation=violation,
            description=f"Leverage {new_leverage:.2f}x vs limit {max_leverage:.2f}x"
        )

    async def _check_correlation_limit(
        self,
        signal: Any,  # TradingSignal
        position_size: Decimal,
        portfolio_context: dict[str, Any]
    ) -> RiskLimit:
        """Check correlation limits."""
        
        # Get portfolio risk assessment
        portfolio_with_risk = await self.coordinator.get_current_portfolio_with_risk_assessment()
        risk_data = portfolio_with_risk.risk_assessment
        
        # Get correlation data from risk assessment
        # Note: RiskAssessment only has correlation_risk (Decimal), not detailed correlations dict
        correlations = {}  # Simplified: use empty dict since detailed correlations not available
        max_correlation = Decimal("0")
        
        # Find maximum correlation with existing positions
        for exchange_positions in portfolio_with_risk.portfolio_state.positions.values():
            for position in exchange_positions:
                if position.symbol != signal.symbol:
                    correlation_key = f"{signal.symbol}_{position.symbol}"
                    alt_key = f"{position.symbol}_{signal.symbol}"
                    
                    if correlation_key in correlations:
                        correlation = Decimal(str(correlations[correlation_key]))
                    elif alt_key in correlations:
                        correlation = Decimal(str(correlations[alt_key]))
                    else:
                        # Assume moderate correlation if not available
                        correlation = Decimal("0.3")
                    
                    max_correlation = max(max_correlation, abs(correlation))
        
        limit_value = Decimal(str(self.risk_limits["max_correlation"]))
        violation = max_correlation > limit_value
        
        return RiskLimit(
            name="correlation",
            limit_value=limit_value,
            current_value=max_correlation,
            risk_level=RiskLevel.HIGH if violation else RiskLevel.LOW,
            violation=violation,
            description=f"Max correlation {max_correlation:.2f} vs limit {limit_value:.2f}"
        )

    async def _check_concentration_limit(
        self,
        signal: Any,  # TradingSignal
        position_size: Decimal,
        portfolio_context: dict[str, Any]
    ) -> RiskLimit:
        """Check concentration limits."""
        
        # Check symbol concentration
        portfolio_with_risk = await self.coordinator.get_current_portfolio_with_risk_assessment()
        existing_exposure = Decimal("0")

        for exchange_positions in portfolio_with_risk.portfolio_state.positions.values():
            for position in exchange_positions:
                if position.symbol == signal.symbol:
                    existing_exposure += abs(position.size)

        total_capital = portfolio_context["total_capital"]
        concentration_ratio = existing_exposure / total_capital if total_capital > 0 else Decimal("0")
        
        max_concentration = Decimal("0.2")  # 20% max per symbol
        violation = concentration_ratio > max_concentration

        return RiskLimit(
            name="concentration",
            limit_value=max_concentration,
            current_value=concentration_ratio,
            risk_level=RiskLevel.MEDIUM if violation else RiskLevel.LOW,
            violation=violation,
            description=f"Symbol concentration {concentration_ratio:.2%} vs limit {max_concentration:.2%}"
        )

    async def _check_var_limit(
        self,
        signal: Any,  # TradingSignal
        position_size: Decimal,
        portfolio_context: dict[str, Any]
    ) -> RiskLimit:
        """Check VaR limits."""
        
        # Get current portfolio VaR from risk assessment
        portfolio_with_risk = await self.coordinator.get_current_portfolio_with_risk_assessment()
        risk_data = portfolio_with_risk.risk_assessment
        
        # Get current VaR
        current_var = risk_data.var_95
        
        # Estimate additional VaR from new position
        # Use volatility data if available
        symbol_volatility = risk_data.volatility_estimate
        
        # Estimate VaR contribution (simplified - assumes normal distribution)
        position_var_contribution = position_size * symbol_volatility * Decimal("1.645")  # 95% VaR
        
        # Total VaR after trade (simplified - ignores diversification)
        estimated_total_var = current_var + position_var_contribution
        
        max_var = Decimal(str(self.risk_limits["max_var"]))
        violation = estimated_total_var > max_var

        return RiskLimit(
            name="var",
            limit_value=max_var,
            current_value=estimated_total_var,
            risk_level=RiskLevel.HIGH if violation else RiskLevel.LOW,
            violation=violation,
            description=f"Total VaR {estimated_total_var:.2f} vs limit {max_var:.2f}"
        )

    async def _check_liquidity_limit(
        self,
        additional_exposure: Decimal,
        portfolio_context: dict[str, Any]
    ) -> RiskLimit:
        """Check liquidity limits."""
        
        total_capital = portfolio_context["total_capital"]
        risk_assessment = portfolio_context["risk_assessment"]
        current_exposure = risk_assessment.get("total_exposure", Decimal("0"))
        
        # Calculate remaining cash after trade
        remaining_cash = total_capital - current_exposure - additional_exposure
        liquidity_ratio = remaining_cash / total_capital if total_capital > 0 else Decimal("0")
        
        min_liquidity = Decimal(str(self.risk_limits["min_liquidity_ratio"]))
        violation = liquidity_ratio < min_liquidity

        return RiskLimit(
            name="liquidity",
            limit_value=min_liquidity,
            current_value=liquidity_ratio,
            risk_level=RiskLevel.MEDIUM if violation else RiskLevel.LOW,
            violation=violation,
            description=f"Liquidity ratio {liquidity_ratio:.2%} vs minimum {min_liquidity:.2%}"
        )

    async def _apply_market_stress_adjustment(self, base_risk_score: float) -> float:
        """Apply market stress factor to risk score."""
        
        stress_adjustment = float(self.market_stress_factor - Decimal("1")) * 10
        return stress_adjustment

    async def _get_portfolio_context(self) -> dict[str, Any]:
        """Get portfolio context for risk calculations."""

        portfolio_with_risk = await self.coordinator.get_current_portfolio_with_risk_assessment()

        return {
            "portfolio_state": portfolio_with_risk.portfolio_state,
            "risk_assessment": portfolio_with_risk.risk_assessment,
            "total_capital": portfolio_with_risk.portfolio_state.total_capital,
            "timestamp": portfolio_with_risk.timestamp
        }

    async def _check_specific_limit(
        self,
        limit_name: str,
        limit_value: Any,
        portfolio_context: dict[str, Any]
    ) -> dict[str, Any]:
        """Check a specific risk limit."""
        
        risk_assessment = portfolio_context["risk_assessment"]
        
        # Map limit names to risk assessment values
        current_value = Decimal("0")
        violated = False
        warning = False
        
        if limit_name == "max_total_exposure":
            current_value = risk_assessment.get("total_exposure", Decimal("0"))
            violated = current_value > Decimal(str(limit_value))
            warning = current_value > Decimal(str(limit_value)) * Decimal("0.8")
        elif limit_name == "max_leverage":
            current_value = risk_assessment.get("leverage", Decimal("0"))
            violated = current_value > Decimal(str(limit_value))
            warning = current_value > Decimal(str(limit_value)) * Decimal("0.8")
        elif limit_name == "max_drawdown":
            current_value = risk_assessment.get("drawdown", Decimal("0"))
            violated = current_value > Decimal(str(limit_value))
            warning = current_value > Decimal(str(limit_value)) * Decimal("0.8")
        elif limit_name == "max_var":
            current_value = risk_assessment.get("var_95", Decimal("0"))
            violated = current_value > Decimal(str(limit_value))
            warning = current_value > Decimal(str(limit_value)) * Decimal("0.8")
        elif limit_name == "max_positions":
            portfolio_state = portfolio_context["portfolio_state"]
            total_positions = sum(len(positions) for positions in portfolio_state.positions.values())
            current_value = Decimal(str(total_positions))
            violated = total_positions > int(limit_value)
            warning = total_positions > int(limit_value) * 0.8
        elif limit_name == "min_liquidity_ratio":
            total_capital = portfolio_context["total_capital"]
            total_exposure = risk_assessment.get("total_exposure", Decimal("0"))
            if total_capital > 0:
                liquidity_ratio = (total_capital - total_exposure) / total_capital
                current_value = liquidity_ratio
                violated = liquidity_ratio < Decimal(str(limit_value))
                warning = liquidity_ratio < Decimal(str(limit_value)) * Decimal("1.2")
        
        return {
            "limit_name": limit_name,
            "violated": violated,
            "warning": warning and not violated,
            "current_value": current_value,
            "limit_value": limit_value,
            "message": f"{limit_name}: {current_value} vs {limit_value}"
        }

    def _calculate_total_risk_score(self, violations: list[Any], warnings: list[Any]) -> float:
        """Calculate total risk score."""
        
        score = len(violations) * 10 + len(warnings) * 5
        return min(score, 100)  # Cap at 100

    def _determine_risk_level(self, violations: list[Any], warnings: list[Any]) -> RiskLevel:
        """Determine overall risk level."""
        
        if violations:
            return RiskLevel.CRITICAL
        elif len(warnings) > 3:
            return RiskLevel.HIGH
        elif warnings:
            return RiskLevel.MEDIUM
        else:
            return RiskLevel.LOW

    def _calculate_max_slippage(self, risk_score: float) -> Decimal:
        """Calculate maximum acceptable slippage based on risk score."""
        # Higher risk score = lower slippage tolerance
        base_slippage = Decimal("0.001")  # 0.1% base
        risk_factor = Decimal(str(max(0.5, 1.0 - risk_score / 100)))
        return base_slippage * risk_factor

    def _calculate_timeout(self, risk_score: float) -> int:
        """Calculate execution timeout based on risk score."""
        # Higher risk = shorter timeout
        base_timeout = 30  # 30 seconds base
        risk_factor = max(0.5, 1.0 - risk_score / 100)
        return int(base_timeout * risk_factor)

    def _calculate_retry_attempts(self, risk_score: float) -> int:
        """Calculate retry attempts based on risk score."""
        if risk_score > 70:
            return 1  # High risk = minimal retries
        elif risk_score > 40:
            return 2  # Medium risk = few retries
        else:
            return 3  # Low risk = normal retries

    async def _load_performance_history(self) -> None:
        """Load historical performance for context."""
        # In production, would load from database or performance tracker
        # For now, initialize empty list
        self.recent_performance = []

    async def _update_volatility_regime(self) -> None:
        """Update volatility regime detection."""
        # Get current portfolio state
        portfolio_context = await self._get_portfolio_context()
        risk_assessment = portfolio_context["risk_assessment"]
        
        # Determine volatility regime based on portfolio volatility
        portfolio_volatility = risk_assessment.get("portfolio_volatility", Decimal("0.02"))
        
        if portfolio_volatility > Decimal("0.05"):  # 5% daily volatility
            self.volatility_regime = "extreme"
        elif portfolio_volatility > Decimal("0.03"):  # 3% daily volatility
            self.volatility_regime = "high"
        else:
            self.volatility_regime = "normal"
            
        # Update market stress factor based on regime
        if self.volatility_regime == "extreme":
            self.market_stress_factor = Decimal("1.5")
        elif self.volatility_regime == "high":
            self.market_stress_factor = Decimal("1.25")
        else:
            self.market_stress_factor = Decimal("1.0")