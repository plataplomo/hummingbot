"""Test the enhanced configuration models."""

from decimal import Decimal

from cyberdelta.config.models.config_models import (
    CheckerSettings,
    CheckerThresholds,
    EnhancedRiskSettings,
    GlobalRiskSettings,
    SizingSettings,
)
from cyberdelta.config.structlog_config import get_logger


logger = get_logger(__name__)


def test_models() -> None:
    """Test that the enhanced models can be instantiated with defaults."""
    # Test CheckerThresholds
    thresholds = CheckerThresholds()
    logger.info("CheckerThresholds", min_profitability=thresholds.min_profitability)
    logger.info("CheckerThresholds", max_price_spread=thresholds.max_price_spread)

    # Test CheckerSettings
    checker_settings = CheckerSettings()
    logger.info("CheckerSettings", enable_profitability=checker_settings.enable_profitability)
    logger.info(
        "CheckerSettings",
        thresholds_min_profitability=checker_settings.thresholds.min_profitability,
    )

    # Test SizingSettings
    sizing_settings = SizingSettings()
    logger.info("SizingSettings", method=sizing_settings.method)
    logger.info("SizingSettings", kelly_multiplier=sizing_settings.kelly_multiplier)

    # Test EnhancedRiskSettings with required global_risk
    global_risk = GlobalRiskSettings(
        max_position_usd=Decimal(10000), max_total_exposure_usd=Decimal(50000)
    )

    # Use the proper constructor - the alias "global" maps to field global_risk
    risk_settings = EnhancedRiskSettings.model_validate({
        "global": global_risk,
        "checkers": CheckerSettings(),
        "sizing": SizingSettings(),
    })
    logger.info(
        "EnhancedRiskSettings",
        global_risk_max_position_usd=risk_settings.global_risk.max_position_usd,
    )
    logger.info(
        "EnhancedRiskSettings",
        checkers_enable_profitability=risk_settings.checkers.enable_profitability,
    )
    logger.info("EnhancedRiskSettings", sizing_method=risk_settings.sizing.method)

    logger.info("All models instantiated successfully!")


if __name__ == "__main__":
    test_models()
