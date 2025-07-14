"""Risk configuration module.

The old dataclass-based configuration system has been replaced with
the enhanced Pydantic-based configuration system. The new configuration
models are available in:

    from cyberdelta.config.models.config_models import (
        AppSettings,
        EnhancedRiskSettings,
        CheckerSettings,
        CheckerThresholds,
        SizingSettings,
        GlobalRiskSettings,
    )

For configuration migration utilities:

    from cyberdelta.core.risk.config.migration import ConfigurationMigrator
"""

# The migration utility is available if needed
__all__: list[str] = []
