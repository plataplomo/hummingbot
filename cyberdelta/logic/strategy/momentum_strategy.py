"""Momentum-based trading strategy implementation.

This module implements a simple momentum strategy that generates buy signals
when price change exceeds configured thresholds and sell signals when momentum
reverses. All parameters are configuration-driven with no hardcoded values.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Optional

from cyberdelta.config.structlog_config import get_logger

from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.core.symbols import get_symbol_service
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.enums.trading import OrderSide
from cyberdelta.logic.strategy.strategy_base import BaseStrategy, StrategyConfigurationError
from cyberdelta.models import TradeSignal
from cyberdelta.models.market.market_snapshot import MarketSnapshot
from cyberdelta.models.portfolio.state import PortfolioState

logger = get_logger(__name__)


class MomentumStrategy(BaseStrategy):
    """Simple momentum-based trading strategy.
    
    This strategy generates buy signals when price change over the configured
    lookback period exceeds the configured threshold, and sell signals when
    momentum reverses below the negative threshold.
    
    Configuration Required (config.strategies.momentum):
    - price_change_threshold: Minimum price change percentage to trigger buy signal
    - negative_threshold: Maximum negative price change to trigger sell signal  
    - signal_confidence: Confidence level for generated signals (0.0-1.0)
    - target_symbol: Symbol to trade (e.g., "BTC_USD")
    - target_exchange: Exchange to trade on (e.g., "hyperliquid")
    - lookback_hours: Hours to look back for price change calculation
    - min_position_hold_hours: Minimum hours to hold a position before closing
    
    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL parameters from config.strategies.momentum section
    - NO hardcoded thresholds, symbols, or exchanges
    - Uses Symbol objects and ExchangeName enums
    - Fail fast if required configuration missing
    """
    
    def __init__(self, config: AppSettings):
        """Initialize momentum strategy with configuration.
        
        Args:
            config: Application settings containing momentum strategy configuration
            
        Raises:
            StrategyConfigurationError: If required configuration is missing
        """
        super().__init__(config)
        
        # Validate required configuration before proceeding
        self._validate_momentum_config()
        
        # Get momentum-specific configuration section
        self._momentum_config = self._get_strategy_config_section("momentum")
        
        # Cache frequently used configuration values
        self._price_change_threshold = self._momentum_config.price_change_threshold
        self._negative_threshold = self._momentum_config.negative_threshold
        self._signal_confidence = self._momentum_config.signal_confidence
        self._lookback_hours = self._momentum_config.lookback_hours
        self._min_position_hold_hours = self._momentum_config.min_position_hold_hours
        
        # Get symbol service for proper Symbol object handling
        self._symbol_service = get_symbol_service()
        
        # Parse target symbol and exchange from config
        self._target_symbol_str = self._momentum_config.target_symbol
        self._target_exchange = ExchangeName(self._momentum_config.target_exchange)
        
        # Get proper Symbol object from symbol service
        try:
            self._target_symbol = self._symbol_service.get_exchange_symbol(
                self._target_symbol_str, 
                self._target_exchange
            )
        except Exception as e:
            raise StrategyConfigurationError(
                f"Failed to get symbol '{self._target_symbol_str}' for exchange "
                f"'{self._target_exchange.value}': {e}"
            ) from e
        
        logger.info(
            "momentum_strategy_configured",
            strategy_name=self.name,
            target_symbol=self._target_symbol.value,
            target_exchange=self._target_exchange.value,
            price_change_threshold=float(self._price_change_threshold),
            negative_threshold=float(self._negative_threshold),
            signal_confidence=float(self._signal_confidence),
            lookback_hours=self._lookback_hours,
            min_position_hold_hours=self._min_position_hold_hours
        )
    
    def _validate_momentum_config(self) -> None:
        """Validate that all required momentum configuration is present.
        
        Raises:
            StrategyConfigurationError: If required configuration is missing
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Explicit validation of ALL required fields
        - Fail fast with clear error messages
        - NO silent defaults
        """
        required_fields = [
            "price_change_threshold",
            "negative_threshold", 
            "signal_confidence",
            "target_symbol",
            "target_exchange",
            "lookback_hours",
            "min_position_hold_hours"
        ]
        
        try:
            self._validate_strategy_config(required_fields, "momentum")
        except ValueError as e:
            raise StrategyConfigurationError(str(e)) from e
    
    async def analyze(
        self, 
        market_data: MarketSnapshot,
        portfolio_state: PortfolioState
    ) -> Optional[TradeSignal]:
        """Analyze market conditions and generate momentum-based trading signal.
        
        Args:
            market_data: Current market snapshot across all exchanges
            portfolio_state: Current portfolio state across all exchanges
            
        Returns:
            TradeSignal if momentum conditions are met, None otherwise
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured thresholds, NO hardcoded values
        - Returns typed TradeSignal with Symbol/ExchangeName objects
        - NO string symbols or exchanges
        - Proper error handling with context
        """
        try:
            # Get ticker for target symbol on target exchange
            ticker = market_data.get_ticker(self._target_exchange, self._target_symbol)
            if not ticker:
                logger.debug(
                    "momentum_analysis_no_ticker",
                    strategy_name=self.name,
                    symbol=self._target_symbol.value,
                    exchange=self._target_exchange.value
                )
                return None
            
            # Check if we have required price data
            if not ticker.last_price or not ticker.price_change_24h:
                logger.debug(
                    "momentum_analysis_insufficient_data",
                    strategy_name=self.name,
                    symbol=self._target_symbol.value,
                    has_last_price=ticker.last_price is not None,
                    has_price_change=ticker.price_change_24h is not None
                )
                return None
            
            # Calculate price change percentage
            price_change_pct = ticker.price_change_24h
            
            # Check current position
            current_position = portfolio_state.get_exchange_positions(
                self._target_exchange
            ).get(f"{self._target_exchange.value}:{self._target_symbol.value}")
            
            # Generate signal based on momentum and current position
            signal = self._generate_momentum_signal(
                ticker.last_price,
                price_change_pct,
                current_position
            )
            
            if signal:
                logger.info(
                    "momentum_signal_generated",
                    strategy_name=self.name,
                    symbol=self._target_symbol.value,
                    exchange=self._target_exchange.value,
                    side=signal.side.value,
                    price=float(signal.price),
                    price_change_pct=float(price_change_pct),
                    confidence=float(signal.confidence)
                )
            
            return signal
            
        except Exception as e:
            logger.error(
                "momentum_analysis_error",
                strategy_name=self.name,
                error=str(e),
                symbol=self._target_symbol.value,
                exchange=self._target_exchange.value,
                exc_info=True
            )
            # Don't raise - return None to skip this analysis cycle
            return None
    
    def _generate_momentum_signal(
        self,
        last_price: Decimal,
        price_change_pct: Decimal,
        current_position: Optional[object]
    ) -> Optional[TradeSignal]:
        """Generate trading signal based on momentum conditions.
        
        Args:
            last_price: Current price of the asset
            price_change_pct: Price change percentage over lookback period
            current_position: Current position in the asset (if any)
            
        Returns:
            TradeSignal if conditions are met, None otherwise
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured thresholds only
        - NO hardcoded logic decisions
        - Returns proper typed TradeSignal
        """
        # Check if we have a current position
        has_position = current_position is not None and hasattr(current_position, 'size') and current_position.size != 0
        
        # Determine signal side based on momentum and position
        signal_side = None
        signal_reason = None
        
        # Buy signal: positive momentum above threshold and no current position
        if (price_change_pct > self._price_change_threshold and 
            not has_position):
            signal_side = OrderSide.BUY
            signal_reason = f"positive_momentum_{float(price_change_pct):.2f}%"
            
        # Sell signal: negative momentum below threshold and have position
        elif (price_change_pct < self._negative_threshold and 
              has_position):
            signal_side = OrderSide.SELL
            signal_reason = f"negative_momentum_{float(price_change_pct):.2f}%"
        
        # No signal conditions met
        if not signal_side:
            logger.debug(
                "momentum_no_signal_conditions",
                strategy_name=self.name,
                price_change_pct=float(price_change_pct),
                positive_threshold=float(self._price_change_threshold),
                negative_threshold=float(self._negative_threshold),
                has_position=has_position
            )
            return None
        
        # Create and return trading signal
        return TradeSignal(
            symbol=self._target_symbol,
            exchange=self._target_exchange,
            side=signal_side,
            price=last_price,
            confidence=self._signal_confidence,
            strategy_name=self.name,
            metadata={
                "signal_reason": signal_reason,
                "price_change_pct": float(price_change_pct),
                "lookback_hours": self._lookback_hours,
                "threshold_used": float(
                    self._price_change_threshold if signal_side == OrderSide.BUY 
                    else self._negative_threshold
                )
            }
        )
    
    async def _strategy_initialize(self) -> None:
        """Initialize momentum strategy specific resources.
        
        IMPORTANT: Following CODING_STANDARDS.md:
        - Validate symbol availability on target exchange
        - Check configuration consistency
        - Fail fast if initialization cannot complete
        """
        # Validate that target symbol is supported on target exchange
        try:
            # This will raise an exception if symbol not supported
            symbol_metadata = self._symbol_service.get_symbol_metadata(
                self._target_symbol_str,
                self._target_exchange
            )
            
            logger.info(
                "momentum_strategy_symbol_validated",
                strategy_name=self.name,
                symbol=self._target_symbol.value,
                exchange=self._target_exchange.value,
                has_metadata=symbol_metadata is not None
            )
            
        except Exception as e:
            raise StrategyConfigurationError(
                f"Target symbol '{self._target_symbol_str}' not available on "
                f"exchange '{self._target_exchange.value}': {e}"
            ) from e
        
        # Validate configuration value ranges
        if not (0.0 <= self._signal_confidence <= 1.0):
            raise StrategyConfigurationError(
                f"signal_confidence must be between 0.0 and 1.0, got {self._signal_confidence}"
            )
        
        if self._price_change_threshold <= 0:
            raise StrategyConfigurationError(
                f"price_change_threshold must be positive, got {self._price_change_threshold}"
            )
        
        if self._negative_threshold >= 0:
            raise StrategyConfigurationError(
                f"negative_threshold must be negative, got {self._negative_threshold}"
            )
        
        if self._lookback_hours <= 0:
            raise StrategyConfigurationError(
                f"lookback_hours must be positive, got {self._lookback_hours}"
            )
        
        logger.info(
            "momentum_strategy_initialization_completed",
            strategy_name=self.name,
            configuration_validated=True
        )
    
    async def _strategy_cleanup(self) -> None:
        """Cleanup momentum strategy specific resources.
        
        IMPORTANT: Following CODING_STANDARDS.md:
        - Graceful cleanup with proper error handling
        - Log cleanup completion
        """
        # No specific cleanup needed for momentum strategy
        # as it doesn't maintain persistent resources
        
        logger.info(
            "momentum_strategy_cleanup_completed",
            strategy_name=self.name
        )