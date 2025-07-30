"""Comprehensive configuration for risk module components.

This replaces all hardcoded values in the risk module with configurable parameters.
"""

from decimal import Decimal
from typing import Dict, Any
from dataclasses import dataclass, field

from cyberdelta.config.structlog_config import get_logger

logger = get_logger(__name__)


@dataclass
class OrchestratorConfig:
    """Configuration for risk orchestrator processing."""
    
    # Processing limits
    max_concurrent_processing: int = 5
    processing_timeout: float = 60.0
    enable_parallel_processing: bool = True
    
    # Volatility calculation parameters
    use_garch: bool = False
    confidence_level: float = 0.95
    
    # Retry and error handling
    max_retries: int = 3
    retry_delay: float = 1.0
    error_threshold: int = 10
    
    # Performance tuning
    batch_size: int = 100
    cache_ttl: float = 300.0  # 5 minutes
    

@dataclass
class ConstraintValidatorConfig:
    """Configuration for constraint validation."""
    
    # Validator limits
    max_concurrent_validators: int = 4
    validation_timeout: float = 30.0
    
    # Position constraints
    max_positions_per_symbol: int = 1
    max_positions_per_exchange: int = 10
    max_total_positions: int = 50
    
    # Exposure constraints
    max_correlation_exposure: Decimal = Decimal("0.8")  # 80% max correlation
    max_sector_exposure: Decimal = Decimal("0.5")       # 50% max per sector
    max_exchange_allocation: Decimal = Decimal("0.6")   # 60% max per exchange
    
    # Risk constraints
    max_portfolio_var: Decimal = Decimal("0.1")         # 10% VaR limit
    max_position_var: Decimal = Decimal("0.05")         # 5% per position
    max_leverage: Decimal = Decimal("5.0")              # 5x max leverage
    
    # Concentration limits
    max_single_position_percent: Decimal = Decimal("0.2")  # 20% of portfolio
    min_position_percent: Decimal = Decimal("0.001")       # 0.1% minimum
    
    # Liquidity constraints
    min_daily_volume_multiple: Decimal = Decimal("0.01")   # Position must be < 1% of daily volume
    max_market_impact: Decimal = Decimal("0.002")         # 0.2% max market impact
    

@dataclass
class PositionSizerConfig:
    """Configuration for position sizing."""
    
    # Precision and rounding
    position_precision: int = 2                # Decimal places for position sizes
    price_precision: int = 8                   # Decimal places for prices
    min_position_value: Decimal = Decimal("10")  # $10 minimum position
    
    # Capital scaling
    capital_scale_factor: Decimal = Decimal("1000")
    min_capital_threshold: Decimal = Decimal("100")
    capital_buffer_percent: Decimal = Decimal("0.05")  # 5% buffer
    
    # Spread adjustments
    max_spread_percentage: Decimal = Decimal("0.02")   # 2% max spread
    min_spread_percentage: Decimal = Decimal("0.0001") # 0.01% min spread
    spread_impact_factor: Decimal = Decimal("0.5")     # 50% of spread as cost
    
    # Size scaling parameters
    urgency_low_factor: Decimal = Decimal("0.8")
    urgency_medium_factor: Decimal = Decimal("1.0")
    urgency_high_factor: Decimal = Decimal("1.2")
    confidence_scale_power: Decimal = Decimal("2.0")
    
    # Kelly specific (in addition to main Kelly parameters)
    kelly_calculation_method: str = "binary"  # "binary" or "continuous"
    kelly_lookback_periods: int = 100
    kelly_min_samples: int = 30
    

@dataclass
class RiskMetricsConfig:
    """Configuration for risk metrics calculation."""
    
    # VaR calculation
    var_confidence_levels: list[float] = field(default_factory=lambda: [0.95, 0.99])
    var_time_horizon: int = 1  # Days
    var_method: str = "parametric"  # "parametric", "historical", "monte_carlo"
    
    # Historical data requirements
    min_history_days: int = 30
    preferred_history_days: int = 252  # 1 year
    max_history_days: int = 1260  # 5 years
    
    # Correlation calculation
    correlation_method: str = "pearson"  # "pearson", "spearman", "kendall"
    correlation_window: int = 60  # Days
    min_correlation_samples: int = 20
    
    # Performance metrics
    sharpe_risk_free_rate: Decimal = Decimal("0.02")  # 2% annual
    sortino_mar: Decimal = Decimal("0.0")  # Minimum acceptable return
    calmar_lookback_years: int = 3
    
    # Drawdown parameters
    drawdown_recovery_threshold: Decimal = Decimal("0.95")  # 95% recovery
    max_drawdown_limit: Decimal = Decimal("0.3")  # 30% max drawdown
    

@dataclass
class RiskModuleConfig:
    """Master configuration for entire risk module."""
    
    orchestrator: OrchestratorConfig = field(default_factory=OrchestratorConfig)
    constraint_validator: ConstraintValidatorConfig = field(default_factory=ConstraintValidatorConfig)
    position_sizer: PositionSizerConfig = field(default_factory=PositionSizerConfig)
    risk_metrics: RiskMetricsConfig = field(default_factory=RiskMetricsConfig)
    
    # Global risk settings
    enable_risk_checks: bool = True
    enable_pre_trade_validation: bool = True
    enable_post_trade_analysis: bool = True
    enable_real_time_monitoring: bool = True
    
    # Risk levels and alerts
    risk_levels: Dict[str, float] = field(default_factory=lambda: {
        "low": 0.2,
        "medium": 0.5,
        "high": 0.7,
        "critical": 0.9
    })
    
    # Emergency controls
    emergency_stop_loss: Decimal = Decimal("0.1")  # 10% portfolio loss
    daily_loss_limit: Decimal = Decimal("0.05")    # 5% daily loss
    position_stop_loss: Decimal = Decimal("0.02")  # 2% position loss
    
    def validate(self) -> None:
        """Validate configuration consistency."""
        # Ensure position limits make sense
        if self.constraint_validator.max_single_position_percent * self.constraint_validator.max_total_positions > Decimal("1.0"):
            logger.warning(
                "config_validation_warning",
                msg="Max position percent * max positions exceeds 100%"
            )
        
        # Ensure leverage limits are consistent
        if self.orchestrator.confidence_level < 0.9 or self.orchestrator.confidence_level > 0.99:
            raise ValueError("Confidence level must be between 0.9 and 0.99")
        
        # Ensure timeouts are reasonable
        if self.orchestrator.processing_timeout < self.constraint_validator.validation_timeout:
            logger.warning(
                "config_validation_warning",
                msg="Orchestrator timeout should be greater than validation timeout"
            )


def load_risk_config_from_settings(app_settings: Any) -> RiskModuleConfig:
    """Load risk configuration from AppSettings with defaults.
    
    Args:
        app_settings: Application settings object
        
    Returns:
        RiskModuleConfig with values from settings or defaults
    """
    config = RiskModuleConfig()
    
    # Try to load from app_settings if available
    if hasattr(app_settings, 'risk_module'):
        risk_settings = app_settings.risk_module
        
        # Load orchestrator settings
        if hasattr(risk_settings, 'orchestrator'):
            orch = risk_settings.orchestrator
            config.orchestrator.max_concurrent_processing = getattr(orch, 'max_concurrent_processing', 5)
            config.orchestrator.processing_timeout = getattr(orch, 'processing_timeout', 60.0)
            config.orchestrator.enable_parallel_processing = getattr(orch, 'enable_parallel_processing', True)
        
        # Load constraint validator settings
        if hasattr(risk_settings, 'constraints'):
            constraints = risk_settings.constraints
            config.constraint_validator.max_positions_per_symbol = getattr(constraints, 'max_positions_per_symbol', 1)
            config.constraint_validator.max_positions_per_exchange = getattr(constraints, 'max_positions_per_exchange', 10)
            config.constraint_validator.max_correlation_exposure = Decimal(str(getattr(constraints, 'max_correlation_exposure', 0.8)))
        
        # Load position sizer settings
        if hasattr(risk_settings, 'position_sizing'):
            sizing = risk_settings.position_sizing
            config.position_sizer.position_precision = getattr(sizing, 'position_precision', 2)
            config.position_sizer.min_position_value = Decimal(str(getattr(sizing, 'min_position_value', 10)))
    
    # Validate the configuration
    config.validate()
    
    return config