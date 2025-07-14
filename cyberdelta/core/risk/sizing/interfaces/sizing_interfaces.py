"""Interfaces for position sizing."""

from abc import abstractmethod
from decimal import Decimal
from typing import Protocol

from cyberdelta.core.risk.sizing.models.sizing_result import SizingContext, SizingResult
from cyberdelta.validation.funding_data import ArbitrageOpportunity


class BaseSizerInterface(Protocol):
    """Protocol for individual position sizers."""

    @abstractmethod
    async def size(
        self,
        opportunity: ArbitrageOpportunity,
        context: SizingContext,
    ) -> SizingResult:
        """Calculate position size for an opportunity.
        
        Args:
            opportunity: The arbitrage opportunity to size
            context: Context information for sizing
            
        Returns:
            SizingResult with position size and details
        """
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the sizer."""
        ...

    @property
    @abstractmethod
    def sizing_method(self) -> str:
        """Sizing method identifier."""
        ...


class PositionSizerInterface(Protocol):
    """Protocol for position sizing orchestrator."""

    @abstractmethod
    async def size_opportunity(
        self,
        opportunity: ArbitrageOpportunity,
        available_capital: Decimal,
    ) -> SizingResult:
        """Size an opportunity with available capital.
        
        Args:
            opportunity: The arbitrage opportunity to size
            available_capital: Available capital for sizing
            
        Returns:
            SizingResult with position size and details
        """
        ...

    @abstractmethod
    async def size_opportunities(
        self,
        opportunities: list[ArbitrageOpportunity],
        available_capital: Decimal,
    ) -> list[SizingResult]:
        """Size multiple opportunities with capital allocation.
        
        Args:
            opportunities: List of arbitrage opportunities to size
            available_capital: Total available capital
            
        Returns:
            List of SizingResult objects
        """
        ...

    @abstractmethod
    def set_sizer(self, sizer: BaseSizerInterface) -> None:
        """Set the active sizer strategy."""
        ...

    @abstractmethod
    def get_sizer(self) -> BaseSizerInterface:
        """Get the current sizer strategy."""
        ...


class KellyCalculatorInterface(Protocol):
    """Protocol for Kelly criterion calculations."""

    @abstractmethod
    def calculate_kelly_fraction(
        self,
        expected_return: Decimal,
        volatility: Decimal,
        win_probability: Decimal | None = None,
    ) -> Decimal:
        """Calculate Kelly fraction for given parameters.
        
        Args:
            expected_return: Expected return of the opportunity
            volatility: Volatility of the opportunity
            win_probability: Optional win probability (defaults to calculated value)
            
        Returns:
            Kelly fraction (0-1)
        """
        ...

    @abstractmethod
    def calculate_optimal_size(
        self,
        kelly_fraction: Decimal,
        available_capital: Decimal,
        max_allocation: Decimal,
        min_allocation: Decimal,
    ) -> Decimal:
        """Calculate optimal position size from Kelly fraction.
        
        Args:
            kelly_fraction: Kelly fraction from calculation
            available_capital: Available capital for sizing
            max_allocation: Maximum allocation limit
            min_allocation: Minimum allocation limit
            
        Returns:
            Optimal position size in USD
        """
        ...


class VolatilityCalculatorInterface(Protocol):
    """Protocol for volatility calculations."""

    @abstractmethod
    async def calculate_volatility(
        self,
        opportunity: ArbitrageOpportunity,
        lookback_hours: int = 24,
    ) -> Decimal:
        """Calculate volatility for an opportunity.
        
        Args:
            opportunity: The arbitrage opportunity
            lookback_hours: Hours to look back for volatility calculation
            
        Returns:
            Volatility estimate
        """
        ...

    @abstractmethod
    async def get_historical_volatility(
        self,
        symbol: str,
        exchange: str,
        lookback_hours: int = 24,
    ) -> Decimal | None:
        """Get historical volatility for a symbol on an exchange.
        
        Args:
            symbol: Trading symbol
            exchange: Exchange name
            lookback_hours: Hours to look back
            
        Returns:
            Historical volatility or None if not available
        """
        ...


class ValidationFactorInterface(Protocol):
    """Protocol for validation factor calculations."""

    @abstractmethod
    async def calculate_validation_factor(
        self,
        opportunity: ArbitrageOpportunity,
        base_factor: Decimal = Decimal("1.0"),
    ) -> Decimal:
        """Calculate validation factor for an opportunity.
        
        Args:
            opportunity: The arbitrage opportunity
            base_factor: Base validation factor
            
        Returns:
            Validation factor (0-1)
        """
        ...

    @abstractmethod
    def apply_validation_factor(
        self,
        base_size: Decimal,
        validation_factor: Decimal,
    ) -> Decimal:
        """Apply validation factor to base size.
        
        Args:
            base_size: Base position size
            validation_factor: Validation factor to apply
            
        Returns:
            Adjusted position size
        """
        ...


class SizingConstraintInterface(Protocol):
    """Protocol for sizing constraints."""

    @abstractmethod
    def check_constraints(
        self,
        opportunity: ArbitrageOpportunity,
        proposed_size: Decimal,
        available_capital: Decimal,
    ) -> tuple[bool, str | None]:
        """Check if proposed size meets constraints.
        
        Args:
            opportunity: The arbitrage opportunity
            proposed_size: Proposed position size
            available_capital: Available capital
            
        Returns:
            Tuple of (constraint_met, reason_if_failed)
        """
        ...

    @abstractmethod
    def adjust_size_for_constraints(
        self,
        opportunity: ArbitrageOpportunity,
        proposed_size: Decimal,
        available_capital: Decimal,
    ) -> Decimal:
        """Adjust size to meet constraints.
        
        Args:
            opportunity: The arbitrage opportunity
            proposed_size: Proposed position size
            available_capital: Available capital
            
        Returns:
            Adjusted position size that meets constraints
        """
        ...
