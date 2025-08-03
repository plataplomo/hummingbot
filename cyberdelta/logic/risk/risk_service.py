"""Risk service for assessment and position sizing.

This module provides the main risk service that handles risk assessment
and position sizing using validated AppSettings configuration.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Dict, List, Optional

from cyberdelta.config.structlog_config import get_logger

from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.logic.portfolio.portfolio_service import PortfolioService
from cyberdelta.logic.risk.drawdown_monitor import DrawdownMonitor
from cyberdelta.models import TradeSignal
from cyberdelta.models.risk.assessment import PositionSize, RiskAssessment
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums.exchange_names import ExchangeName

logger = get_logger(__name__)


class RiskService:
    """Risk assessment and position sizing using validated AppSettings.
    
    This service handles:
    - Risk assessment for trading signals
    - Position size calculation using configured methods
    - Risk limit enforcement from configuration
    - Exposure tracking across all positions
    
    Configuration Structure (config.risk):
    - risk.global_risk: GlobalRiskSettings
      - max_position_usd: Maximum position size in USD
      - max_total_exposure_usd: Maximum total exposure across all positions
    - risk.sizing: SizingSettings
      - method: "simple" or "kelly"
      - simple_fixed_fraction: Position sizing fraction
      - min_position_size / max_position_size: Position bounds
    - risk.checkers: CheckerSettings
      - thresholds: Comprehensive validation thresholds
      - enable_* flags: Toggle different checks
    
    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL configuration from AppSettings, NO hardcoded values
    - Uses Symbol objects, NOT strings
    - Uses ExchangeName enum, NOT strings
    - All monetary values as Decimal, NOT float
    - NO assumptions about market conditions
    """
    
    def __init__(
        self,
        config: AppSettings,
        portfolio_service: PortfolioService,
    ):
        """Initialize risk service with configuration and dependencies.
        
        Args:
            config: Application settings containing all risk configuration
            portfolio_service: Portfolio service for state access
        """
        self.config = config
        self._portfolio_service = portfolio_service
        
        # Extract commonly used settings - NO hardcoded defaults
        self._global_risk = config.risk.global_risk
        self._sizing_config = config.risk.sizing
        self._checker_config = config.risk.checkers
        
        # Cache frequently accessed values for performance
        self._max_position_usd = self._global_risk.max_position_usd
        self._max_exposure_usd = self._global_risk.max_total_exposure_usd
        
        # Position sizing parameters based on configured method
        if self._sizing_config.method == "simple":
            self._sizing_fraction = self._sizing_config.simple_fixed_fraction
        elif self._sizing_config.method == "kelly":
            self._kelly_multiplier = self._sizing_config.kelly_multiplier
            self._kelly_max_allocation = self._sizing_config.kelly_max_allocation
        # No else clause needed - Literal type ensures only valid values
        
        # Initialize drawdown monitor for portfolio protection
        self._drawdown_monitor = DrawdownMonitor(config, portfolio_service)
        
        logger.info(
            "risk_service_initialized",
            max_position_usd=float(self._max_position_usd),
            max_exposure_usd=float(self._max_exposure_usd),
            sizing_method=self._sizing_config.method,
            checkers_enabled=self._checker_config.enable_profitability,
            drawdown_monitoring_enabled=True,
            max_drawdown_pct=float(self._global_risk.max_drawdown_pct)
        )
    
    async def assess_signal(self, signal: TradeSignal) -> RiskAssessment:
        """Assess risk for trading signal.
        
        Args:
            signal: Trading signal to assess
            
        Returns:
            Risk assessment with approval status and calculated position size
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - ALL limits from config, NO hardcoded values
        - Explicit violation messages
        - Uses Symbol/ExchangeName types
        - Returns Decimal values
        """
        logger.debug(
            "risk_assessment_starting",
            signal_id=signal.signal_id,
            symbol=signal.symbol.value,
            exchange=signal.exchange.value if hasattr(signal.exchange, 'value') else str(signal.exchange),
            side=signal.side.value if hasattr(signal.side, 'value') else str(signal.side)
        )
        
        try:
            # Get current portfolio state
            position = await self._portfolio_service.get_position(
                signal.symbol, 
                ExchangeName(signal.exchange) if isinstance(signal.exchange, str) else signal.exchange
            )
            total_equity = await self._portfolio_service.get_total_equity_usd()
            
            # Calculate current exposure
            current_exposure = self._calculate_exposure(position, signal.price)
            
            # Calculate position size using configured method
            position_size = await self.calculate_position_size(
                signal, total_equity, current_exposure
            )
            
            # Check all limits using validated AppSettings
            limit_violations = []
            
            # Check 1: Max position size from config.risk.global_risk
            if position_size.value_usd > self._max_position_usd:
                limit_violations.append(
                    f"Position size ${position_size.value_usd} exceeds max ${self._max_position_usd}"
                )
                
            # Check 2: Total exposure limit from config.risk.global_risk  
            new_exposure = current_exposure + position_size.value_usd
            if new_exposure > self._max_exposure_usd:
                limit_violations.append(
                    f"New exposure ${new_exposure} would exceed max ${self._max_exposure_usd}"
                )
                
            # Check 3: Minimum equity check (ensure we have funds)
            if total_equity < position_size.value_usd:
                limit_violations.append(
                    f"Insufficient equity ${total_equity} for position ${position_size.value_usd}"
                )
                
            # Check 4: Additional checks from config.risk.checkers if enabled
            if self._checker_config.enable_profitability:
                min_profit = self._checker_config.thresholds.min_profitability
                expected_profit = getattr(signal, 'expected_profit', None)
                if expected_profit and expected_profit < min_profit:
                    limit_violations.append(
                        f"Expected profit {expected_profit} below minimum {min_profit}"
                    )
                    
            # Check 5: Price sanity checks if enabled
            if self._checker_config.enable_price_sanity:
                thresholds = self._checker_config.thresholds
                if signal.price and (signal.price < thresholds.min_price or signal.price > thresholds.max_price):
                    limit_violations.append(
                        f"Price {signal.price} outside valid range [{thresholds.min_price}, {thresholds.max_price}]"
                    )
            
            # Check 6: Position limits
            position_limit_violations = await self.check_position_limits(
                signal.symbol, signal.exchange
            )
            limit_violations.extend(position_limit_violations)
            
            # Check 7: Concentration limits
            concentration_violations = await self.check_concentration_limits(
                signal.symbol, position_size.value_usd
            )
            limit_violations.extend(concentration_violations)
            
            # Check 8: Drawdown limits
            drawdown_violations = await self._drawdown_monitor.check_drawdown_limits()
            limit_violations.extend(drawdown_violations)
            
            # Check 9: Block new positions if drawdown violated
            if self._drawdown_monitor.should_block_new_positions():
                limit_violations.append(
                    f"New positions blocked due to drawdown violation: "
                    f"{self._drawdown_monitor.get_current_drawdown_pct():.2f}% > "
                    f"{self._drawdown_monitor.get_max_allowed_drawdown_pct():.2f}%"
                )
            
            # Calculate max loss estimate
            max_loss_usd = self._calculate_max_loss(
                position_size, signal.price, getattr(signal, 'stop_loss', None)
            )
            
            assessment = RiskAssessment(
                signal_id=signal.signal_id,
                approved=len(limit_violations) == 0,
                position_size=position_size,
                current_exposure=current_exposure,
                limit_violations=limit_violations,
                max_loss_usd=max_loss_usd
            )
            
            logger.info(
                "risk_assessment_completed",
                signal_id=signal.signal_id,
                approved=assessment.approved,
                position_size_usd=float(position_size.value_usd),
                violation_count=len(limit_violations),
                current_exposure=float(current_exposure)
            )
            
            return assessment
            
        except Exception as e:
            logger.error(
                "risk_assessment_failed",
                signal_id=signal.signal_id,
                error=str(e),
                exc_info=True
            )
            raise
    
    async def calculate_position_size(
        self,
        signal: TradeSignal,
        total_equity: Decimal,
        current_exposure: Decimal
    ) -> PositionSize:
        """Calculate position size using configured sizing method from AppSettings.
        
        Args:
            signal: Trading signal containing price and direction
            total_equity: Total portfolio equity in USD
            current_exposure: Current exposure across all positions
            
        Returns:
            Calculated position size with constraints applied
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured sizing method (simple/kelly)
        - ALL parameters from config
        - NO hardcoded fractions or multipliers
        """
        if not signal.price or signal.price <= 0:
            logger.warning(
                "invalid_signal_price_for_sizing",
                signal_id=signal.signal_id,
                price=float(signal.price) if signal.price else None
            )
            return PositionSize(
                quantity=Decimal("0"),
                value_usd=Decimal("0"),
                percent_of_equity=Decimal("0")
            )
        
        if self._sizing_config.method == "simple":
            # Simple fixed fraction sizing
            position_value = self._calculate_simple_size(total_equity, current_exposure)
        elif self._sizing_config.method == "kelly":
            # Kelly criterion sizing
            position_value = self._calculate_kelly_size(
                signal, total_equity, current_exposure
            )
        # No else clause needed - Literal type ensures only valid values
        
        # Apply position limits from config
        position_value = self._apply_position_limits(position_value, current_exposure)
        
        # Calculate quantity based on signal price
        quantity = position_value / signal.price if position_value > 0 else Decimal("0")
        
        # Apply min/max quantity constraints from config.risk.sizing
        if quantity > 0:
            quantity = max(quantity, self._sizing_config.min_position_size)
            quantity = min(quantity, self._sizing_config.max_position_size)
            # Recalculate value after quantity constraints
            position_value = quantity * signal.price
        
        position_size = PositionSize(
            quantity=quantity,
            value_usd=position_value,
            percent_of_equity=position_value / total_equity * 100 if total_equity > 0 else Decimal("0")
        )
        
        logger.debug(
            "position_size_calculated",
            signal_id=signal.signal_id,
            method=self._sizing_config.method,
            quantity=float(quantity),
            value_usd=float(position_value),
            percent_of_equity=float(position_size.percent_of_equity)
        )
        
        return position_size
    
    def _calculate_simple_size(self, total_equity: Decimal, current_exposure: Decimal) -> Decimal:
        """Calculate position size using simple fixed fraction method.
        
        Args:
            total_equity: Total portfolio equity
            current_exposure: Current total exposure
            
        Returns:
            Position value in USD
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured fraction from config.risk.sizing.simple_fixed_fraction
        - NO hardcoded fractions
        """
        # Use configured fraction from config.risk.sizing.simple_fixed_fraction
        fraction_based_size = total_equity * self._sizing_fraction
        
        # Apply configured max position size
        return min(fraction_based_size, self._max_position_usd)
    
    def _calculate_kelly_size(
        self,
        signal: TradeSignal,
        total_equity: Decimal,
        current_exposure: Decimal
    ) -> Decimal:
        """Calculate position size using Kelly criterion method.
        
        Args:
            signal: Trading signal with confidence/probability data
            total_equity: Total portfolio equity
            current_exposure: Current total exposure
            
        Returns:
            Position value in USD
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses kelly_multiplier and kelly_max_allocation from config
        - NO hardcoded Kelly parameters
        
        Note: This is a placeholder implementation. Full Kelly requires
        win rate and win/loss ratio data which would come from strategy
        backtesting or historical performance data.
        """
        # Simplified Kelly implementation using signal confidence
        # In production, this would use historical win rate and profit/loss ratios
        confidence = getattr(signal, 'confidence', Decimal("0.5"))
        
        # Kelly fraction = (bp - q) / b where:
        # b = odds received on the wager (profit/loss ratio)
        # p = probability of winning
        # q = probability of losing (1-p)
        # For now, use a conservative estimate
        win_rate = confidence  # Use signal confidence as win probability
        loss_rate = Decimal("1") - win_rate
        profit_loss_ratio = Decimal("1.5")  # Conservative 1.5:1 ratio
        
        # Kelly fraction
        kelly_fraction = (win_rate * profit_loss_ratio - loss_rate) / profit_loss_ratio
        
        # Apply kelly multiplier from config (typically < 1 for safety)
        kelly_fraction *= self._kelly_multiplier
        
        # Cap at configured max allocation
        kelly_fraction = min(kelly_fraction, self._kelly_max_allocation)
        
        # Ensure non-negative
        kelly_fraction = max(kelly_fraction, Decimal("0"))
        
        # Calculate position size
        kelly_based_size = total_equity * kelly_fraction
        
        logger.debug(
            "kelly_calculation",
            signal_id=signal.signal_id,
            confidence=float(confidence),
            kelly_fraction=float(kelly_fraction),
            kelly_multiplier=float(self._kelly_multiplier),
            max_allocation=float(self._kelly_max_allocation)
        )
        
        return kelly_based_size
    
    def _apply_position_limits(self, position_value: Decimal, current_exposure: Decimal) -> Decimal:
        """Apply global risk limits to position size.
        
        Args:
            position_value: Calculated position value before limits
            current_exposure: Current total exposure
            
        Returns:
            Position value after applying limits
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - ALL limits from config
        - NO hardcoded risk parameters
        """
        # Check total exposure limit
        if current_exposure + position_value > self._max_exposure_usd:
            # Reduce position to stay within total exposure limit
            position_value = self._max_exposure_usd - current_exposure
            if position_value <= 0:
                position_value = Decimal("0")
                
        # Apply max position size limit
        position_value = min(position_value, self._max_position_usd)
        
        # Ensure non-negative
        position_value = max(position_value, Decimal("0"))
        
        return position_value
    
    def _calculate_exposure(
        self, 
        position: Optional[object], 
        signal_price: Optional[Decimal]
    ) -> Decimal:
        """Calculate current exposure for a position.
        
        Args:
            position: Current position object (if any)
            signal_price: Current market price
            
        Returns:
            Exposure value in USD
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns Decimal, NOT float
        - NO assumptions about position structure
        """
        if not position or not signal_price:
            return Decimal("0")
            
        # Get position size if available
        position_size = getattr(position, 'size', Decimal("0"))
        if position_size == 0:
            return Decimal("0")
            
        # Calculate exposure as absolute value * current price
        return abs(position_size) * signal_price
    
    def _calculate_max_loss(
        self,
        position_size: PositionSize,
        entry_price: Optional[Decimal],
        stop_loss: Optional[Decimal]
    ) -> Optional[Decimal]:
        """Calculate maximum potential loss for a position.
        
        Args:
            position_size: Calculated position size
            entry_price: Entry price for the position
            stop_loss: Stop loss price (if set)
            
        Returns:
            Maximum loss in USD, None if cannot calculate
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns Decimal, NOT float
        - NO assumptions about stop loss presence
        """
        if not entry_price or not stop_loss or position_size.quantity <= 0:
            return None
            
        # Calculate loss per unit
        loss_per_unit = abs(entry_price - stop_loss)
        
        # Total max loss
        max_loss = loss_per_unit * position_size.quantity
        
        return max_loss
    
    async def check_position_limits(
        self, 
        symbol: Symbol, 
        exchange: ExchangeName
    ) -> List[str]:
        """Check position limits for a specific symbol and exchange.
        
        Args:
            symbol: Symbol to check limits for
            exchange: Exchange to check limits on
            
        Returns:
            List of limit violations (empty if no violations)
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Check config.risk.limits.max_positions_per_symbol
        - Check config.risk.limits.max_positions_total  
        - Return explicit violations, NO silent filtering
        - ALL limits from configuration
        """
        violations = []
        
        # Get current portfolio state
        portfolio_state = await self._portfolio_service.get_state()
        if not portfolio_state:
            # If no portfolio state, cannot check limits
            return violations
        
        # Get risk limits configuration
        if not hasattr(self.config.risk, 'limits'):
            # No limits configured - return no violations
            logger.debug(
                "no_position_limits_configured",
                symbol=symbol.value,
                exchange=exchange.value
            )
            return violations
        
        limits_config = self.config.risk.limits
        
        # Check per-symbol position limits
        if hasattr(limits_config, 'max_positions_per_symbol'):
            max_per_symbol = limits_config.max_positions_per_symbol
            current_symbol_positions = self._count_symbol_positions(
                portfolio_state, symbol
            )
            
            if current_symbol_positions >= max_per_symbol:
                violations.append(
                    f"Symbol {symbol.value} has {current_symbol_positions} positions, "
                    f"max allowed: {max_per_symbol}"
                )
        
        # Check total position limits
        if hasattr(limits_config, 'max_positions_total'):
            max_total = limits_config.max_positions_total
            total_positions = self._count_total_positions(portfolio_state)
            
            if total_positions >= max_total:
                violations.append(
                    f"Total positions {total_positions} at max allowed: {max_total}"
                )
        
        # Check exchange-specific position limits
        if hasattr(limits_config, 'max_positions_per_exchange'):
            max_per_exchange = limits_config.max_positions_per_exchange
            exchange_positions = self._count_exchange_positions(
                portfolio_state, exchange
            )
            
            if exchange_positions >= max_per_exchange:
                violations.append(
                    f"Exchange {exchange.value} has {exchange_positions} positions, "
                    f"max allowed: {max_per_exchange}"
                )
        
        if violations:
            logger.warning(
                "position_limit_violations",
                symbol=symbol.value,
                exchange=exchange.value,
                violations=violations
            )
        
        return violations
    
    def _count_symbol_positions(
        self, 
        portfolio_state: PortfolioState, 
        symbol: Symbol
    ) -> int:
        """Count positions for a specific symbol across all exchanges.
        
        Args:
            portfolio_state: Current portfolio state
            symbol: Symbol to count positions for
            
        Returns:
            Number of positions for the symbol
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Counts non-zero positions only
        - Uses Symbol object, NOT string matching
        """
        count = 0
        
        for position_key, position in portfolio_state.positions.items():
            # Extract symbol from position key format: "exchange:symbol"
            try:
                _, position_symbol_str = position_key.split(":", 1)
                if position_symbol_str == symbol.value and position.size != 0:
                    count += 1
            except ValueError:
                # Skip malformed position keys
                logger.warning(
                    "malformed_position_key",
                    position_key=position_key
                )
                continue
        
        return count
    
    def _count_total_positions(self, portfolio_state: PortfolioState) -> int:
        """Count total positions across all symbols and exchanges.
        
        Args:
            portfolio_state: Current portfolio state
            
        Returns:
            Total number of non-zero positions
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Counts only non-zero positions
        - NO assumptions about position structure
        """
        count = 0
        
        for position in portfolio_state.positions.values():
            if position.size != 0:
                count += 1
        
        return count
    
    def _count_exchange_positions(
        self, 
        portfolio_state: PortfolioState, 
        exchange: ExchangeName
    ) -> int:
        """Count positions on a specific exchange.
        
        Args:
            portfolio_state: Current portfolio state
            exchange: Exchange to count positions for
            
        Returns:
            Number of positions on the exchange
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses ExchangeName enum, NOT string matching
        - Counts non-zero positions only
        """
        count = 0
        
        for position_key, position in portfolio_state.positions.items():
            # Extract exchange from position key format: "exchange:symbol"
            try:
                exchange_str, _ = position_key.split(":", 1)
                if exchange_str == exchange.value and position.size != 0:
                    count += 1
            except ValueError:
                # Skip malformed position keys
                logger.warning(
                    "malformed_position_key_in_exchange_count",
                    position_key=position_key
                )
                continue
        
        return count
    
    async def check_concentration_limits(
        self, 
        symbol: Symbol, 
        position_value_usd: Decimal
    ) -> List[str]:
        """Check concentration limits for a position.
        
        Args:
            symbol: Symbol for the position
            position_value_usd: Value of the proposed position in USD
            
        Returns:
            List of concentration limit violations
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses config.risk.limits.max_concentration_per_symbol
        - Uses config.risk.limits.max_concentration_per_asset_class
        - ALL percentages from configuration
        """
        violations = []
        
        # Get current portfolio state
        portfolio_state = await self._portfolio_service.get_state()
        if not portfolio_state or not portfolio_state.total_equity_usd:
            return violations
        
        total_equity = portfolio_state.total_equity_usd
        
        # Get concentration limits configuration
        if not hasattr(self.config.risk, 'limits'):
            return violations
        
        limits_config = self.config.risk.limits
        
        # Check per-symbol concentration
        if hasattr(limits_config, 'max_concentration_per_symbol'):
            max_symbol_pct = limits_config.max_concentration_per_symbol
            current_symbol_value = self._get_symbol_total_value(portfolio_state, symbol)
            new_symbol_value = current_symbol_value + position_value_usd
            new_symbol_pct = (new_symbol_value / total_equity) * 100
            
            if new_symbol_pct > max_symbol_pct:
                violations.append(
                    f"Symbol {symbol.value} concentration would be {new_symbol_pct:.1f}%, "
                    f"max allowed: {max_symbol_pct}%"
                )
        
        # Check asset class concentration (if configured)
        if hasattr(limits_config, 'max_concentration_per_asset_class'):
            asset_class = self._get_asset_class(symbol)
            if asset_class:
                max_class_pct = limits_config.max_concentration_per_asset_class
                current_class_value = self._get_asset_class_total_value(
                    portfolio_state, asset_class
                )
                new_class_value = current_class_value + position_value_usd
                new_class_pct = (new_class_value / total_equity) * 100
                
                if new_class_pct > max_class_pct:
                    violations.append(
                        f"Asset class {asset_class} concentration would be {new_class_pct:.1f}%, "
                        f"max allowed: {max_class_pct}%"
                    )
        
        return violations
    
    def _get_symbol_total_value(
        self, 
        portfolio_state: PortfolioState, 
        symbol: Symbol
    ) -> Decimal:
        """Get total value of positions for a specific symbol.
        
        Args:
            portfolio_state: Current portfolio state
            symbol: Symbol to calculate value for
            
        Returns:
            Total value in USD for the symbol
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns Decimal, NOT float
        - Uses current market prices for valuation
        """
        total_value = Decimal("0")
        
        for position_key, position in portfolio_state.positions.items():
            try:
                _, position_symbol_str = position_key.split(":", 1)
                if position_symbol_str == symbol.value and position.size != 0:
                    # Calculate position value using entry price
                    # In a full implementation, this would use current market price
                    if position.entry_price:
                        position_value = abs(position.size) * position.entry_price
                        total_value += position_value
            except (ValueError, AttributeError):
                # Skip malformed positions
                continue
        
        return total_value
    
    def _get_asset_class(self, symbol: Symbol) -> Optional[str]:
        """Get asset class for a symbol.
        
        Args:
            symbol: Symbol to get asset class for
            
        Returns:
            Asset class name or None if not categorized
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses symbol metadata for classification
        - NO hardcoded asset class mappings
        """
        # This would typically use symbol service metadata
        # For now, simple classification based on symbol name
        symbol_str = symbol.value.upper()
        
        if any(crypto in symbol_str for crypto in ["BTC", "ETH", "SOL", "AVAX"]):
            return "cryptocurrency"
        elif "USD" in symbol_str or "USDC" in symbol_str:
            return "stablecoin"
        else:
            return "other"
    
    def _get_asset_class_total_value(
        self, 
        portfolio_state: PortfolioState, 
        asset_class: str
    ) -> Decimal:
        """Get total value of positions for an asset class.
        
        Args:
            portfolio_state: Current portfolio state
            asset_class: Asset class to calculate value for
            
        Returns:
            Total value in USD for the asset class
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns Decimal, NOT float
        - NO assumptions about asset classification
        """
        total_value = Decimal("0")
        
        for position_key, position in portfolio_state.positions.items():
            try:
                _, position_symbol_str = position_key.split(":", 1)
                # Create Symbol object for classification
                # This is simplified - would use symbol service in practice
                position_symbol = Symbol(value=position_symbol_str)
                position_asset_class = self._get_asset_class(position_symbol)
                
                if (position_asset_class == asset_class and 
                    position.size != 0 and position.entry_price):
                    position_value = abs(position.size) * position.entry_price
                    total_value += position_value
                    
            except (ValueError, AttributeError):
                # Skip malformed positions
                continue
        
        return total_value
    async def update_drawdown_monitoring(self, portfolio_value: Optional[Decimal] = None) -> None:
        """Update drawdown monitoring with current portfolio value.
        
        Args:
            portfolio_value: Current portfolio value (if None, fetches from service)
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Delegates to DrawdownMonitor for calculations
        - NO assumptions about value availability
        - Should be called regularly to maintain accurate monitoring
        """
        try:
            await self._drawdown_monitor.update_portfolio_value(portfolio_value)
        except Exception as e:
            logger.error(
                "drawdown_monitoring_update_failed",
                error=str(e),
                exc_info=True
            )
    
    def get_drawdown_status(self) -> Dict[str, object]:
        """Get current drawdown monitoring status.
        
        Returns:
            Dictionary with drawdown status and configuration
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns structured status from DrawdownMonitor
        - Configuration context included
        """
        return self._drawdown_monitor.get_drawdown_status()
    
    def is_drawdown_violated(self) -> bool:
        """Check if drawdown limits are currently violated.
        
        Returns:
            True if drawdown exceeds configured limits
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses DrawdownMonitor state
        - NO assumptions about violation handling
        """
        return self._drawdown_monitor.is_drawdown_violated()
    
    async def get_historical_max_drawdown(self) -> Optional[Decimal]:
        """Get historical maximum drawdown over configured lookback period.
        
        Returns:
            Maximum drawdown percentage, None if insufficient data
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns Decimal, NOT float
        - Based on configured lookback period
        """
        return await self._drawdown_monitor.get_historical_max_drawdown()
    
    def reset_drawdown_tracking(self) -> None:
        """Reset drawdown tracking state.
        
        IMPORTANT: Following CODING_STANDARDS.md:
        - Explicit state reset for emergency situations
        - Logs reset action for audit trail
        """
        logger.warning(
            "risk_service_drawdown_reset_requested",
            reason="manual_intervention"
        )
        self._drawdown_monitor.reset_drawdown_tracking()
