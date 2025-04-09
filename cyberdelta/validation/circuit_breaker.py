"""
Circuit Breaker System for the CyberDeltaEngine.

This module provides circuit breakers that can halt trading operations
when abnormal conditions are detected, preventing cascading failures
and limiting potential losses.
"""

import logging
import time
from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Dict, List, Optional, Any, Callable, Set, Union, Tuple
from datetime import datetime, timedelta

from cyberdelta.utils.config import Config

logger = logging.getLogger(__name__)


class BreakerState(Enum):
    """State of a circuit breaker."""
    CLOSED = auto()  # Normal operation, allowing trades
    OPEN = auto()    # Tripped, blocking trades
    HALF_OPEN = auto()  # Testing if system has recovered


class CircuitBreaker(ABC):
    """
    Base abstract class for all circuit breakers.
    
    Circuit breakers monitor specific conditions and "trip" (open)
    when those conditions indicate potential problems.
    """
    
    def __init__(self, name: str, cooldown_seconds: int = 300):
        """
        Initialize the circuit breaker.
        
        Args:
            name: Identifier for this circuit breaker
            cooldown_seconds: Time to wait before testing if system has recovered
        """
        self.name = name
        self.cooldown_seconds = cooldown_seconds
        self.state = BreakerState.CLOSED
        self.trip_time: Optional[datetime] = None
        self.trip_reason: Optional[str] = None
        self.trip_count = 0
        self.last_reset_time: Optional[datetime] = None
        
    def trip(self, reason: str) -> None:
        """
        Trip the circuit breaker, preventing further operations.
        
        Args:
            reason: Why the breaker was tripped
        """
        if self.state != BreakerState.OPEN:
            self.state = BreakerState.OPEN
            self.trip_time = datetime.now()
            self.trip_reason = reason
            self.trip_count += 1
            
            logger.warning(
                f"Circuit breaker '{self.name}' tripped. Reason: {reason}"
            )
    
    def reset(self) -> None:
        """Reset the circuit breaker to allow operations."""
        prev_state = self.state
        self.state = BreakerState.CLOSED
        self.last_reset_time = datetime.now()
        
        if prev_state == BreakerState.OPEN:
            trip_duration = (self.last_reset_time - self.trip_time).total_seconds() if self.trip_time else 0
            logger.info(
                f"Circuit breaker '{self.name}' reset after {trip_duration:.1f} seconds. "
                f"Previous reason: {self.trip_reason}"
            )
        
        self.trip_time = None
        self.trip_reason = None
    
    def allow_operation(self) -> bool:
        """
        Check if operations are allowed through this breaker.
        
        Returns:
            True if the operation is allowed, False if blocked
        """
        # If breaker is open, check if cooldown period has passed
        if self.state == BreakerState.OPEN:
            if self.trip_time and (datetime.now() - self.trip_time).total_seconds() >= self.cooldown_seconds:
                self.state = BreakerState.HALF_OPEN
                logger.info(f"Circuit breaker '{self.name}' entering half-open state for testing")
        
        # Only allow operations if closed or half-open
        return self.state != BreakerState.OPEN
    
    def test_recovery(self) -> bool:
        """
        Test if the system has recovered when in half-open state.
        
        This method should be called during a test operation when the breaker
        is in half-open state. If it returns True, the breaker will fully close.
        If False, it will reopen with the cooldown period.
        
        Returns:
            True if recovery was successful, False otherwise
        """
        # Only perform recovery test if in half-open state
        if self.state != BreakerState.HALF_OPEN:
            return False
            
        # Implement in subclasses to check specific recovery conditions
        recovery_successful = self._check_recovery()
        
        if recovery_successful:
            self.reset()
            return True
        else:
            # Re-trip the breaker with the same reason
            self.trip(f"Recovery failed: {self.trip_reason}")
            return False
    
    @abstractmethod
    def _check_recovery(self) -> bool:
        """
        Implement in subclasses to check if system has recovered.
        
        Returns:
            True if recovery was successful, False otherwise
        """
        pass
    
    @abstractmethod
    def check(self, *args, **kwargs) -> None:
        """
        Check if the circuit breaker should trip.
        
        Implement in subclasses to evaluate specific conditions.
        Should call self.trip() if the breaker should trip.
        """
        pass
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get the current status of this breaker.
        
        Returns:
            Status as a dictionary
        """
        return {
            'name': self.name,
            'state': self.state.name,
            'trip_count': self.trip_count,
            'trip_time': self.trip_time.isoformat() if self.trip_time else None,
            'trip_reason': self.trip_reason,
            'cooldown_seconds': self.cooldown_seconds,
            'last_reset_time': self.last_reset_time.isoformat() if self.last_reset_time else None
        }


class VolatilityBreaker(CircuitBreaker):
    """
    Circuit breaker that trips when asset volatility exceeds thresholds.
    """
    
    def __init__(
        self, 
        name: str, 
        lookback_periods: int = 12,
        volatility_threshold: float = 0.05,  # 5% volatility threshold
        cooldown_seconds: int = 300
    ):
        """
        Initialize the volatility breaker.
        
        Args:
            name: Identifier for this breaker
            lookback_periods: Number of periods to consider for volatility
            volatility_threshold: Volatility threshold as decimal (e.g., 0.05 = 5%)
            cooldown_seconds: Time to wait before testing if system has recovered
        """
        super().__init__(name, cooldown_seconds)
        self.lookback_periods = lookback_periods
        self.volatility_threshold = volatility_threshold
        self.price_history: List[float] = []
        
    def add_price(self, price: float) -> None:
        """
        Add a price point to the history.
        
        Args:
            price: The current price
        """
        self.price_history.append(price)
        
        # Keep only the required number of periods
        while len(self.price_history) > self.lookback_periods:
            self.price_history.pop(0)
            
    def check(self, current_price: Optional[float] = None) -> None:
        """
        Check if volatility exceeds the threshold.
        
        Args:
            current_price: Current price to add to history before checking
        """
        # Only check if we have enough data and are not already tripped
        if self.state == BreakerState.OPEN:
            return
            
        # Add current price if provided
        if current_price is not None:
            self.add_price(current_price)
            
        # Need at least 2 prices to calculate volatility
        if len(self.price_history) < 2:
            return
            
        # Calculate volatility as standard deviation / mean
        mean = sum(self.price_history) / len(self.price_history)
        if mean == 0:
            return
            
        variance = sum((p - mean) ** 2 for p in self.price_history) / len(self.price_history)
        volatility = (variance ** 0.5) / mean
        
        if volatility > self.volatility_threshold:
            self.trip(f"Volatility of {volatility:.4f} exceeds threshold of {self.volatility_threshold:.4f}")
    
    def _check_recovery(self) -> bool:
        """
        Check if volatility has returned to acceptable levels.
        
        Returns:
            True if volatility is now below threshold, False otherwise
        """
        if len(self.price_history) < 2:
            return False
            
        mean = sum(self.price_history) / len(self.price_history)
        if mean == 0:
            return False
            
        variance = sum((p - mean) ** 2 for p in self.price_history) / len(self.price_history)
        volatility = (variance ** 0.5) / mean
        
        return volatility <= self.volatility_threshold


class DrawdownBreaker(CircuitBreaker):
    """
    Circuit breaker that trips when drawdown exceeds thresholds.
    """
    
    def __init__(
        self, 
        name: str, 
        drawdown_threshold: float = 0.10,  # 10% drawdown threshold
        cooldown_seconds: int = 600
    ):
        """
        Initialize the drawdown breaker.
        
        Args:
            name: Identifier for this breaker
            drawdown_threshold: Maximum allowable drawdown as decimal
            cooldown_seconds: Time to wait before testing if system has recovered
        """
        super().__init__(name, cooldown_seconds)
        self.drawdown_threshold = drawdown_threshold
        self.peak_value: Optional[float] = None
        self.current_value: Optional[float] = None
        
    def check(self, current_value: float) -> None:
        """
        Check if drawdown exceeds the threshold.
        
        Args:
            current_value: Current portfolio/asset value to check
        """
        # Update current value
        self.current_value = current_value
        
        # Update peak if this is a new high or initial value
        if self.peak_value is None or current_value > self.peak_value:
            self.peak_value = current_value
            return
        
        # Calculate drawdown
        if self.peak_value == 0:
            return
            
        drawdown = (self.peak_value - current_value) / self.peak_value
        
        # Trip if drawdown exceeds threshold
        if drawdown > self.drawdown_threshold:
            self.trip(f"Drawdown of {drawdown:.2%} exceeds threshold of {self.drawdown_threshold:.2%}")
    
    def _check_recovery(self) -> bool:
        """
        Check if drawdown has returned to acceptable levels.
        
        Returns:
            True if drawdown is now below threshold, False otherwise
        """
        if self.peak_value is None or self.current_value is None or self.peak_value == 0:
            return False
            
        drawdown = (self.peak_value - self.current_value) / self.peak_value
        return drawdown <= self.drawdown_threshold


class APIErrorBreaker(CircuitBreaker):
    """
    Circuit breaker that trips when API errors exceed thresholds.
    """
    
    def __init__(
        self, 
        name: str, 
        error_threshold: int = 3,  # Number of errors to trigger
        window_seconds: int = 60,   # Time window for errors
        cooldown_seconds: int = 300
    ):
        """
        Initialize the API error breaker.
        
        Args:
            name: Identifier for this breaker
            error_threshold: Number of errors to trigger the breaker
            window_seconds: Time window to consider for errors
            cooldown_seconds: Time to wait before testing if system has recovered
        """
        super().__init__(name, cooldown_seconds)
        self.error_threshold = error_threshold
        self.window_seconds = window_seconds
        self.error_times: List[datetime] = []
        
    def record_error(self, error_message: str) -> None:
        """
        Record an API error occurrence.
        
        Args:
            error_message: Description of the error
        """
        now = datetime.now()
        self.error_times.append(now)
        
        # Remove errors outside the window
        cutoff_time = now - timedelta(seconds=self.window_seconds)
        self.error_times = [t for t in self.error_times if t >= cutoff_time]
        
        # Check if threshold is exceeded
        self.check(error_message)
    
    def check(self, error_message: Optional[str] = None) -> None:
        """
        Check if error count exceeds the threshold.
        
        Args:
            error_message: Optional error message if checking after a new error
        """
        # Remove errors outside the window
        now = datetime.now()
        cutoff_time = now - timedelta(seconds=self.window_seconds)
        self.error_times = [t for t in self.error_times if t >= cutoff_time]
        
        # Trip if threshold exceeded
        if len(self.error_times) >= self.error_threshold:
            reason = f"{len(self.error_times)} errors in {self.window_seconds}s window"
            if error_message:
                reason += f". Latest: {error_message}"
            self.trip(reason)
    
    def _check_recovery(self) -> bool:
        """
        Check if error rate has decreased.
        
        Returns:
            True if recent error count is below threshold, False otherwise
        """
        # Remove errors outside the window
        now = datetime.now()
        cutoff_time = now - timedelta(seconds=self.window_seconds)
        self.error_times = [t for t in self.error_times if t >= cutoff_time]
        
        # Consider recovered if error count is below threshold
        return len(self.error_times) < self.error_threshold


class LiquidityBreaker(CircuitBreaker):
    """
    Circuit breaker that trips when market liquidity drops below thresholds.
    """
    
    def __init__(
        self, 
        name: str, 
        min_liquidity: float,  # Minimum acceptable liquidity
        cooldown_seconds: int = 300
    ):
        """
        Initialize the liquidity breaker.
        
        Args:
            name: Identifier for this breaker
            min_liquidity: Minimum acceptable liquidity (e.g., in USD)
            cooldown_seconds: Time to wait before testing if system has recovered
        """
        super().__init__(name, cooldown_seconds)
        self.min_liquidity = min_liquidity
        self.current_liquidity: Optional[float] = None
        
    def check(self, current_liquidity: float) -> None:
        """
        Check if liquidity is below the threshold.
        
        Args:
            current_liquidity: Current market liquidity
        """
        self.current_liquidity = current_liquidity
        
        if current_liquidity < self.min_liquidity:
            self.trip(f"Liquidity of {current_liquidity} below minimum threshold of {self.min_liquidity}")
    
    def _check_recovery(self) -> bool:
        """
        Check if liquidity has returned to acceptable levels.
        
        Returns:
            True if liquidity is now above threshold, False otherwise
        """
        if self.current_liquidity is None:
            return False
            
        return self.current_liquidity >= self.min_liquidity


class CircuitBreakerSystem:
    """
    Manages a collection of circuit breakers and provides a central
    interface for checking and controlling them.
    """
    
    def __init__(self, config: Config):
        """
        Initialize the circuit breaker system.
        
        Args:
            config: Application configuration
        """
        self.config = config
        self.breakers: Dict[str, CircuitBreaker] = {}
        self.exchange_breakers: Dict[str, Dict[str, CircuitBreaker]] = {}
        
        # Load configuration
        self._load_config()
    
    def _load_config(self) -> None:
        """Load circuit breaker configuration."""
        # Load global settings
        global_enabled = self.config.get('validation.circuit_breaker.enabled', True)
        if not global_enabled:
            logger.warning("Circuit breaker system is globally disabled")
            return
            
        # Load exchange-specific settings
        for exchange_id in self.config.get('exchanges', {}).keys():
            if not self.config.get(f'exchanges.{exchange_id}.enabled', False):
                continue
                
            exchange_enabled = self.config.get(
                f'validation.circuit_breaker.exchanges.{exchange_id}.enabled', 
                True
            )
            
            if not exchange_enabled:
                logger.info(f"Circuit breakers disabled for {exchange_id}")
                continue
                
            # Initialize exchange breakers dictionary
            self.exchange_breakers[exchange_id] = {}
            
            # Create API error breaker
            if self.config.get(f'validation.circuit_breaker.exchanges.{exchange_id}.api_errors.enabled', True):
                error_threshold = self.config.get(
                    f'validation.circuit_breaker.exchanges.{exchange_id}.api_errors.threshold', 
                    3
                )
                window_seconds = self.config.get(
                    f'validation.circuit_breaker.exchanges.{exchange_id}.api_errors.window_seconds', 
                    60
                )
                cooldown_seconds = self.config.get(
                    f'validation.circuit_breaker.exchanges.{exchange_id}.api_errors.cooldown_seconds', 
                    300
                )
                
                api_breaker = APIErrorBreaker(
                    name=f"{exchange_id}_api_errors",
                    error_threshold=error_threshold,
                    window_seconds=window_seconds,
                    cooldown_seconds=cooldown_seconds
                )
                
                self.exchange_breakers[exchange_id]['api_errors'] = api_breaker
                
            # Create volatility breakers for each trading pair
            if self.config.get(f'validation.circuit_breaker.exchanges.{exchange_id}.volatility.enabled', True):
                volatility_threshold = self.config.get(
                    f'validation.circuit_breaker.exchanges.{exchange_id}.volatility.threshold', 
                    0.05
                )
                lookback_periods = self.config.get(
                    f'validation.circuit_breaker.exchanges.{exchange_id}.volatility.lookback_periods', 
                    12
                )
                cooldown_seconds = self.config.get(
                    f'validation.circuit_breaker.exchanges.{exchange_id}.volatility.cooldown_seconds', 
                    300
                )
                
                # Create breaker for each symbol
                for symbol in self.config.get(f'exchanges.{exchange_id}.symbols', []):
                    vol_breaker = VolatilityBreaker(
                        name=f"{exchange_id}_{symbol}_volatility",
                        lookback_periods=lookback_periods,
                        volatility_threshold=volatility_threshold,
                        cooldown_seconds=cooldown_seconds
                    )
                    
                    self.exchange_breakers[exchange_id][f'{symbol}_volatility'] = vol_breaker
                    
            # Create drawdown breakers
            if self.config.get(f'validation.circuit_breaker.exchanges.{exchange_id}.drawdown.enabled', True):
                drawdown_threshold = self.config.get(
                    f'validation.circuit_breaker.exchanges.{exchange_id}.drawdown.threshold', 
                    0.10
                )
                cooldown_seconds = self.config.get(
                    f'validation.circuit_breaker.exchanges.{exchange_id}.drawdown.cooldown_seconds', 
                    600
                )
                
                # Create exchange-wide drawdown breaker
                draw_breaker = DrawdownBreaker(
                    name=f"{exchange_id}_drawdown",
                    drawdown_threshold=drawdown_threshold,
                    cooldown_seconds=cooldown_seconds
                )
                
                self.exchange_breakers[exchange_id]['drawdown'] = draw_breaker
                
            # Create liquidity breakers for each trading pair
            if self.config.get(f'validation.circuit_breaker.exchanges.{exchange_id}.liquidity.enabled', True):
                # Get symbols and their minimum liquidity requirements
                for symbol in self.config.get(f'exchanges.{exchange_id}.symbols', []):
                    min_liquidity = self.config.get(
                        f'validation.circuit_breaker.exchanges.{exchange_id}.liquidity.{symbol}.min_liquidity',
                        100000  # Default to $100k if not specified
                    )
                    cooldown_seconds = self.config.get(
                        f'validation.circuit_breaker.exchanges.{exchange_id}.liquidity.cooldown_seconds', 
                        300
                    )
                    
                    liq_breaker = LiquidityBreaker(
                        name=f"{exchange_id}_{symbol}_liquidity",
                        min_liquidity=min_liquidity,
                        cooldown_seconds=cooldown_seconds
                    )
                    
                    self.exchange_breakers[exchange_id][f'{symbol}_liquidity'] = liq_breaker
        
        logger.info(f"Initialized circuit breakers for {len(self.exchange_breakers)} exchanges")
    
    def register_breaker(self, breaker: CircuitBreaker) -> None:
        """
        Register a new circuit breaker with the system.
        
        Args:
            breaker: Circuit breaker to register
        """
        self.breakers[breaker.name] = breaker
        logger.debug(f"Registered circuit breaker: {breaker.name}")
    
    def get_breaker(self, name: str) -> Optional[CircuitBreaker]:
        """
        Get a circuit breaker by name.
        
        Args:
            name: Name of the breaker
            
        Returns:
            The circuit breaker, or None if not found
        """
        return self.breakers.get(name)
    
    def get_exchange_breaker(self, exchange: str, breaker_type: str) -> Optional[CircuitBreaker]:
        """
        Get an exchange-specific circuit breaker.
        
        Args:
            exchange: Exchange identifier
            breaker_type: Type of breaker (e.g., 'api_errors', 'BTC_volatility')
            
        Returns:
            The circuit breaker, or None if not found
        """
        if exchange not in self.exchange_breakers:
            return None
            
        return self.exchange_breakers[exchange].get(breaker_type)
    
    def can_execute(self, exchange: str, symbol: Optional[str] = None) -> Tuple[bool, Optional[str]]:
        """
        Check if an operation can be executed for an exchange/symbol.
        
        Args:
            exchange: Exchange identifier
            symbol: Optional symbol for symbol-specific checks
            
        Returns:
            Tuple of (can_execute, reason_if_blocked)
        """
        if exchange not in self.exchange_breakers:
            return True, None
            
        # Check exchange-level breakers
        breakers_to_check = []
        
        # Add exchange-wide breakers
        for breaker_type in ['api_errors', 'drawdown']:
            if breaker_type in self.exchange_breakers[exchange]:
                breakers_to_check.append(self.exchange_breakers[exchange][breaker_type])
        
        # Add symbol-specific breakers if a symbol is provided
        if symbol:
            for breaker_type in [f'{symbol}_volatility', f'{symbol}_liquidity']:
                if breaker_type in self.exchange_breakers[exchange]:
                    breakers_to_check.append(self.exchange_breakers[exchange][breaker_type])
        
        # Check if any breaker is open
        for breaker in breakers_to_check:
            if not breaker.allow_operation():
                return False, f"Blocked by circuit breaker: {breaker.name} - {breaker.trip_reason}"
        
        return True, None
    
    def record_api_error(self, exchange: str, error_message: str) -> None:
        """
        Record an API error for the specified exchange.
        
        Args:
            exchange: Exchange identifier
            error_message: Description of the error
        """
        api_breaker = self.get_exchange_breaker(exchange, 'api_errors')
        if api_breaker and isinstance(api_breaker, APIErrorBreaker):
            api_breaker.record_error(error_message)
    
    def update_price(self, exchange: str, symbol: str, price: float) -> None:
        """
        Update price data for volatility monitoring.
        
        Args:
            exchange: Exchange identifier
            symbol: Trading symbol
            price: Current price
        """
        vol_breaker = self.get_exchange_breaker(exchange, f'{symbol}_volatility')
        if vol_breaker and isinstance(vol_breaker, VolatilityBreaker):
            vol_breaker.add_price(price)
            vol_breaker.check()
    
    def update_portfolio_value(self, exchange: str, value: float) -> None:
        """
        Update portfolio value for drawdown monitoring.
        
        Args:
            exchange: Exchange identifier
            value: Current portfolio value
        """
        draw_breaker = self.get_exchange_breaker(exchange, 'drawdown')
        if draw_breaker and isinstance(draw_breaker, DrawdownBreaker):
            draw_breaker.check(value)
    
    def update_liquidity(self, exchange: str, symbol: str, liquidity: float) -> None:
        """
        Update market liquidity for monitoring.
        
        Args:
            exchange: Exchange identifier
            symbol: Trading symbol
            liquidity: Current market liquidity
        """
        liq_breaker = self.get_exchange_breaker(exchange, f'{symbol}_liquidity')
        if liq_breaker and isinstance(liq_breaker, LiquidityBreaker):
            liq_breaker.check(liquidity)
    
    def reset_breaker(self, name: str) -> bool:
        """
        Reset a circuit breaker by name.
        
        Args:
            name: Name of the breaker to reset
            
        Returns:
            True if reset was successful, False if breaker not found
        """
        breaker = self.get_breaker(name)
        if breaker:
            breaker.reset()
            return True
        
        # Check exchange breakers
        for exchange, breakers in self.exchange_breakers.items():
            if name in breakers:
                breakers[name].reset()
                return True
        
        return False
    
    def reset_exchange_breakers(self, exchange: str) -> int:
        """
        Reset all circuit breakers for an exchange.
        
        Args:
            exchange: Exchange identifier
            
        Returns:
            Number of breakers reset
        """
        if exchange not in self.exchange_breakers:
            return 0
            
        reset_count = 0
        for breaker in self.exchange_breakers[exchange].values():
            breaker.reset()
            reset_count += 1
            
        return reset_count
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get the current status of all circuit breakers.
        
        Returns:
            Status as a dictionary
        """
        status = {
            'global_breakers': {},
            'exchange_breakers': {}
        }
        
        # Add global breakers
        for name, breaker in self.breakers.items():
            status['global_breakers'][name] = breaker.get_status()
        
        # Add exchange breakers
        for exchange, breakers in self.exchange_breakers.items():
            status['exchange_breakers'][exchange] = {}
            for name, breaker in breakers.items():
                status['exchange_breakers'][exchange][name] = breaker.get_status()
        
        return status
    
    def get_tripped_breakers(self) -> List[Dict[str, Any]]:
        """
        Get a list of all currently tripped breakers.
        
        Returns:
            List of tripped breaker status dictionaries
        """
        tripped_breakers = []
        
        # Check global breakers
        for name, breaker in self.breakers.items():
            if breaker.state == BreakerState.OPEN:
                tripped_breakers.append(breaker.get_status())
        
        # Check exchange breakers
        for exchange, breakers in self.exchange_breakers.items():
            for name, breaker in breakers.items():
                if breaker.state == BreakerState.OPEN:
                    tripped_breakers.append(breaker.get_status())
        
        return tripped_breakers 