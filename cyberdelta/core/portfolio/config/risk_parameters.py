"""Production risk parameters configuration.

This replaces all hardcoded values in the portfolio risk coordinator
with configurable, market-driven parameters.
"""

from decimal import Decimal
from typing import Dict, Any, Protocol
from dataclasses import dataclass, field

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.symbols import Symbol

logger = get_logger(__name__)


@dataclass
class RiskParameters:
    """Centralized risk parameters for portfolio management."""
    
    # Capital utilization limits
    max_capital_utilization: Decimal = Decimal("0.9")  # Max 90% of available capital
    max_var_percentage: Decimal = Decimal("0.05")      # Max 5% VaR
    
    # Position sizing parameters
    base_position_percent: Decimal = Decimal("0.1")    # 10% base allocation
    target_risk_per_position: Decimal = Decimal("0.02") # 2% risk per position
    kelly_cap: Decimal = Decimal("0.25")               # Max 25% Kelly
    
    # Leverage limits by market condition
    leverage_limits: dict[str, Decimal] = field(default_factory=lambda: {
        "base": Decimal("3.0"),
        "high_volatility": Decimal("1.8"),
        "extreme_volatility": Decimal("1.2"),
        "moderate_volatility": Decimal("2.4"),
        "low_volatility": Decimal("3.0"),
    })
    
    # Position limits by volatility
    position_limit_factors: dict[str, Decimal] = field(default_factory=lambda: {
        "very_volatile": Decimal("0.5"),    # >10% vol: 5% limit
        "moderate_volatile": Decimal("0.7"), # >6% vol: 7% limit
        "low_volatile": Decimal("1.0"),      # <6% vol: 10% limit
    })
    
    # Correlation adjustment factors
    correlation_factors: dict[str, Decimal] = field(default_factory=lambda: {
        "high": Decimal("0.6"),    # >80% correlation
        "medium": Decimal("0.8"),   # >50% correlation
        "low": Decimal("1.0"),      # <50% correlation
    })
    
    # Risk score thresholds
    risk_thresholds: dict[str, dict[str, Decimal]] = field(default_factory=lambda: {
        "leverage": {
            "critical": Decimal("4.0"),
            "high": Decimal("3.0"),
            "elevated": Decimal("2.0"),
            "moderate": Decimal("1.5"),
            "low": Decimal("1.0"),
        },
        "drawdown": {
            "critical": Decimal("0.2"),    # 20%
            "high": Decimal("0.15"),       # 15%
            "elevated": Decimal("0.1"),    # 10%
            "moderate": Decimal("0.05"),   # 5%
            "low": Decimal("0.02"),        # 2%
        },
        "var_percent": {
            "critical": Decimal("0.1"),    # 10%
            "high": Decimal("0.08"),       # 8%
            "elevated": Decimal("0.06"),   # 6%
            "moderate": Decimal("0.04"),   # 4%
            "low": Decimal("0.02"),        # 2%
        }
    })
    
    # Market impact thresholds
    market_impact: dict[str, Decimal] = field(default_factory=lambda: {
        "high_impact_threshold": Decimal("0.1"),   # 10% of daily volume
        "medium_impact_threshold": Decimal("0.02"), # 2% of daily volume
    })
    
    # Concentration limits
    concentration_limits: dict[str, Decimal] = field(default_factory=lambda: {
        "max_crypto_exposure": Decimal("0.9"),    # 90% crypto
        "max_stable_exposure": Decimal("0.8"),    # 80% stables
        "warning_crypto_exposure": Decimal("0.8"), # 80% crypto warning
    })
    
    # Liquidity risk factors
    liquidity_factors: dict[str, Decimal] = field(default_factory=lambda: {
        "btc": Decimal("0.1"),        # Very liquid
        "eth": Decimal("0.1"),        # Very liquid
        "major_alts": Decimal("0.3"), # Good liquidity
        "stablecoins": Decimal("0.05"), # Excellent liquidity
        "other": Decimal("0.6"),      # Lower liquidity
    })
    
    # Size multipliers for liquidity
    size_multipliers: dict[str, Decimal] = field(default_factory=lambda: {
        "large": Decimal("1.5"),    # >$50k position
        "medium": Decimal("1.2"),   # >$10k position
        "small": Decimal("1.0"),    # <$10k position
    })
    
    # Size thresholds
    size_thresholds: dict[str, Decimal] = field(default_factory=lambda: {
        "large": Decimal("50000"),
        "medium": Decimal("10000"),
    })
    
    # Volatility estimates (will be replaced by market data service)
    default_volatilities: dict[str, Decimal] = field(default_factory=lambda: {
        "BTC": Decimal("0.04"),       # 4% daily
        "ETH": Decimal("0.05"),       # 5% daily
        "major_alts": Decimal("0.06"), # 6% daily
        "stablecoins": Decimal("0.001"), # 0.1% daily
        "unknown": Decimal("0.08"),    # 8% daily
        "default": Decimal("0.02"),    # 2% daily
    })
    
    # Expected returns (will be replaced by strategy signals)
    default_expected_returns: dict[str, Decimal] = field(default_factory=lambda: {
        "default": Decimal("0.02"),   # 2% expected return
        "momentum": Decimal("0.03"),  # 3% for momentum strategies
        "arbitrage": Decimal("0.01"), # 1% for arbitrage
    })
    
    # Win/loss parameters for Kelly (will be replaced by actual trade history)
    default_kelly_params: dict[str, Decimal] = field(default_factory=lambda: {
        "avg_win": Decimal("0.03"),   # 3% average win
        "avg_loss": Decimal("0.02"),  # 2% average loss
        "base_win_prob": Decimal("0.5"), # Base 50% win probability
    })
    
    # Statistical parameters
    statistical_params: dict[str, Decimal] = field(default_factory=lambda: {
        "var_confidence_95": Decimal("1.645"),  # 95% confidence z-score
        "var_confidence_99": Decimal("2.326"),  # 99% confidence z-score
        "correlation_adjustment": Decimal("0.6"), # Default correlation
    })
    
    # Execution parameters
    execution_params: dict[str, Decimal] = field(default_factory=lambda: {
        "high_urgency_time": Decimal("300"),    # 5 minutes
        "medium_urgency_time": Decimal("900"),  # 15 minutes
        "low_urgency_time": Decimal("1800"),    # 30 minutes
    })


class MarketDataProvider(Protocol):
    """Interface for getting real market data instead of hardcoded values."""
    
    async def get_symbol_volatility(self, symbol: Symbol) -> Decimal:
        """Get real volatility from market data."""
        ...
    
    async def get_daily_volume(self, symbol: Symbol) -> Decimal:
        """Get real daily volume from market data."""
        ...
    
    async def get_correlation_matrix(self, symbols: list[Symbol]) -> Dict[str, Dict[str, Decimal]]:
        """Get real correlation matrix from market data."""
        ...
    
    async def get_expected_return(self, symbol: Symbol, strategy: str) -> Decimal:
        """Get expected return based on strategy and market conditions."""
        ...


class DynamicRiskParameters:
    """Dynamic risk parameters that adapt to market conditions."""
    
    def __init__(
        self,
        base_params: RiskParameters,
        market_data_provider: MarketDataProvider | None = None
    ):
        self.base_params = base_params
        self.market_data_provider = market_data_provider
        
    async def get_symbol_volatility(self, symbol: Symbol) -> Decimal:
        """Get volatility with real data fallback to defaults."""
        if self.market_data_provider:
            try:
                return await self.market_data_provider.get_symbol_volatility(symbol)
            except Exception as e:
                logger.warning(
                    "volatility_fetch_failed",
                    symbol=symbol,
                    error=str(e),
                    msg="Using default volatility"
                )
        
        # Fallback to defaults - use Symbol value for string operations
        symbol_upper = symbol.value.upper()
        if "BTC" in symbol_upper:
            return self.base_params.default_volatilities["BTC"]
        elif "ETH" in symbol_upper:
            return self.base_params.default_volatilities["ETH"]
        elif any(stable in symbol_upper for stable in ["USDC", "USDT", "DAI", "BUSD"]):
            return self.base_params.default_volatilities["stablecoins"]
        elif any(alt in symbol_upper for alt in ["SOL", "AVAX", "MATIC", "DOT"]):
            return self.base_params.default_volatilities["major_alts"]
        else:
            return self.base_params.default_volatilities["unknown"]
    
    async def get_daily_volume(self, symbol: Symbol) -> Decimal:
        """Get daily volume with estimates as fallback."""
        if self.market_data_provider:
            try:
                return await self.market_data_provider.get_daily_volume(symbol)
            except Exception as e:
                logger.warning(
                    "volume_fetch_failed",
                    symbol=symbol,
                    error=str(e),
                    msg="Using volume estimates"
                )
        
        # Fallback estimates - use Symbol value for string operations
        symbol_upper = symbol.value.upper()
        if "BTC" in symbol_upper:
            return Decimal("1000000000")  # $1B
        elif "ETH" in symbol_upper:
            return Decimal("500000000")   # $500M
        elif any(stable in symbol_upper for stable in ["USDC", "USDT"]):
            return Decimal("5000000000")  # $5B
        else:
            return Decimal("10000000")    # $10M
    
    async def get_correlation_matrix(self, symbols: list[Symbol]) -> Dict[str, Dict[str, Decimal]]:
        """Get correlation matrix with defaults as fallback."""
        if self.market_data_provider:
            try:
                return await self.market_data_provider.get_correlation_matrix(symbols)
            except Exception as e:
                logger.warning(
                    "correlation_fetch_failed",
                    symbols=symbols,
                    error=str(e),
                    msg="Using default correlations"
                )
        
        # Fallback to default correlations
        return self._get_default_correlation_matrix(symbols)
    
    async def get_expected_return(self, symbol: Symbol, strategy: str) -> Decimal:
        """Get expected return with defaults as fallback."""
        if self.market_data_provider:
            try:
                return await self.market_data_provider.get_expected_return(symbol, strategy)
            except Exception as e:
                logger.warning(
                    "expected_return_fetch_failed",
                    symbol=symbol,
                    strategy=strategy,
                    error=str(e),
                    msg="Using default expected return"
                )
        
        # Fallback to defaults
        return self.base_params.default_expected_returns.get(
            strategy, 
            self.base_params.default_expected_returns["default"]
        )
    
    def _get_default_correlation_matrix(self, symbols: list[Symbol]) -> Dict[str, Dict[str, Decimal]]:
        """Generate default correlation matrix for symbols."""
        matrix: Dict[str, Dict[str, Decimal]] = {}
        
        for symbol1 in symbols:
            symbol1_key = symbol1.value
            matrix[symbol1_key] = {}
            for symbol2 in symbols:
                symbol2_key = symbol2.value
                if symbol1 == symbol2:
                    matrix[symbol1_key][symbol2_key] = Decimal("1.0")
                else:
                    # Use statistical default correlation
                    matrix[symbol1_key][symbol2_key] = self.base_params.statistical_params["correlation_adjustment"]
        
        return matrix
    
    def get_leverage_limit(self, volatility: Decimal) -> Decimal:
        """Get leverage limit based on volatility."""
        if volatility > Decimal("0.10"):
            return self.base_params.leverage_limits["extreme_volatility"]
        elif volatility > Decimal("0.08"):
            return self.base_params.leverage_limits["high_volatility"]
        elif volatility > Decimal("0.06"):
            return self.base_params.leverage_limits["moderate_volatility"]
        else:
            return self.base_params.leverage_limits["low_volatility"]
    
    def get_position_limit_factor(self, volatility: Decimal) -> Decimal:
        """Get position limit factor based on volatility."""
        if volatility > Decimal("0.10"):
            return self.base_params.position_limit_factors["very_volatile"]
        elif volatility > Decimal("0.06"):
            return self.base_params.position_limit_factors["moderate_volatile"]
        else:
            return self.base_params.position_limit_factors["low_volatile"]
    
    def get_correlation_factor(self, correlation: Decimal) -> Decimal:
        """Get correlation adjustment factor."""
        if correlation > Decimal("0.8"):
            return self.base_params.correlation_factors["high"]
        elif correlation > Decimal("0.5"):
            return self.base_params.correlation_factors["medium"]
        else:
            return self.base_params.correlation_factors["low"]
    
    def get_liquidity_factor(self, symbol: Symbol) -> Decimal:
        """Get liquidity risk factor for symbol."""
        symbol_upper = symbol.value.upper()
        if "BTC" in symbol_upper:
            return self.base_params.liquidity_factors["btc"]
        elif "ETH" in symbol_upper:
            return self.base_params.liquidity_factors["eth"]
        elif any(stable in symbol_upper for stable in ["USDC", "USDT", "DAI", "BUSD"]):
            return self.base_params.liquidity_factors["stablecoins"]
        elif any(alt in symbol_upper for alt in ["SOL", "AVAX", "MATIC"]):
            return self.base_params.liquidity_factors["major_alts"]
        else:
            return self.base_params.liquidity_factors["other"]
    
    def get_size_multiplier(self, position_size_usd: Decimal) -> Decimal:
        """Get size multiplier based on position size."""
        if position_size_usd > self.base_params.size_thresholds["large"]:
            return self.base_params.size_multipliers["large"]
        elif position_size_usd > self.base_params.size_thresholds["medium"]:
            return self.base_params.size_multipliers["medium"]
        else:
            return self.base_params.size_multipliers["small"]