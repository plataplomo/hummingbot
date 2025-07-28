"""Portfolio analytics and reporting service."""

from __future__ import annotations

import asyncio
import contextlib
import csv
import json
import time
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import Field, field_validator
from pydantic.dataclasses import dataclass

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.calculators.performance_calculator import PerformanceInput
from cyberdelta.core.portfolio.exceptions import StateValidationError
from cyberdelta.core.portfolio.exceptions.service import (
    AnalyticsRequiredFieldError,
    AnalyticsTypeError,
    AnalyticsValueError,
)
from cyberdelta.core.portfolio.services.base.base_service import BasePortfolioService


# Constants
MAX_DECIMAL_PLACES = 8  # Maximum decimal places for financial values
REASONABLE_VALUE_MIN = -1e15  # Minimum reasonable financial value
REASONABLE_VALUE_MAX = 1e15  # Maximum reasonable financial value


# Typed model factories for Pydantic
def _report_section_list_factory() -> list[ReportSection]:
    """Factory function that preserves list[ReportSection] type information.
    
    Returns:
        Empty list with proper ReportSection type annotation.
    """
    return []


def _decimal_dict_factory() -> dict[str, Decimal]:
    """Factory function that preserves dict[str, Decimal] type information.
    
    Returns:
        Empty dictionary with proper Decimal value type annotation.
    """
    return {}


if TYPE_CHECKING:
    from cyberdelta.core.portfolio.calculators.exposure_calculator import ExposureCalculator
    from cyberdelta.core.portfolio.calculators.performance_calculator import (
        PerformanceCalculator,
    )
    from cyberdelta.core.portfolio.calculators.pnl.realized_pnl_calculator import (
        RealizedPnLCalculator,
    )
    from cyberdelta.core.portfolio.calculators.pnl.unrealized_pnl_calculator import (
        UnrealizedPnLCalculator,
    )
    from cyberdelta.core.portfolio.portfolio_types.manager_protocols import StateManagerProtocol
    from cyberdelta.core.portfolio.portfolio_types.service_protocols import PortfolioServiceProtocol


logger = get_logger(__name__)


# Type-preserving factory functions
def _chart_data_factory() -> list[dict[str, float | str]]:
    """Factory function that preserves chart data type information.
    
    Returns:
        Empty list with proper chart data type annotation.
    """
    return []


def _table_data_factory() -> list[list[str | float | int]]:
    """Factory function that preserves table data type information.
    
    Returns:
        Empty list with proper table data type annotation.
    """
    return []


def _position_data_factory() -> list[PositionData]:
    """Factory function that preserves list[PositionData] type information.
    
    Returns:
        Empty list with proper PositionData type annotation.
    """
    return []


def _chart_data_list_factory() -> list[ChartData]:
    """Factory function that preserves list[ChartData] type information.
    
    Returns:
        Empty list with proper ChartData type annotation.
    """
    return []


def _table_data_list_factory() -> list[TableData]:
    """Factory function that preserves list[TableData] type information.
    
    Returns:
        Empty list with proper TableData type annotation.
    """
    return []


def _analytics_alert_factory() -> list[AnalyticsAlert]:
    """Factory function that preserves list[AnalyticsAlert] type information.
    
    Returns:
        Empty list with proper AnalyticsAlert type annotation.
    """
    return []


class ReportType(Enum):
    """Types of portfolio reports."""

    DAILY_SUMMARY = "daily_summary"
    WEEKLY_SUMMARY = "weekly_summary"
    MONTHLY_SUMMARY = "monthly_summary"
    QUARTERLY_SUMMARY = "quarterly_summary"
    YEARLY_SUMMARY = "yearly_summary"
    PERFORMANCE_REPORT = "performance_report"
    RISK_REPORT = "risk_report"
    TRADE_REPORT = "trade_report"
    POSITION_REPORT = "position_report"
    EXPOSURE_REPORT = "exposure_report"
    ATTRIBUTION_REPORT = "attribution_report"
    COMPLIANCE_REPORT = "compliance_report"
    CUSTOM_REPORT = "custom_report"


class OutputFormat(Enum):
    """Output formats for reports."""

    JSON = "json"
    CSV = "csv"
    PDF = "pdf"
    EXCEL = "excel"
    HTML = "html"
    MARKDOWN = "markdown"


class AnalyticsType(Enum):
    """Types of analytics."""

    PERFORMANCE = "performance"
    RISK = "risk"
    ATTRIBUTION = "attribution"
    EXPOSURE = "exposure"
    LIQUIDITY = "liquidity"
    CORRELATION = "correlation"
    STRESS_TEST = "stress_test"
    SCENARIO = "scenario"
    BACKTESTING = "backtesting"


@dataclass
class ReportFilters:
    """Filters for report generation with validation."""

    start_date: str | None = Field(default=None, description="Start date for filtering")
    end_date: str | None = Field(default=None, description="End date for filtering")
    exchange_ids: list[str] = Field(default_factory=list, description="Exchange IDs to include")
    symbols: list[str] = Field(default_factory=list, description="Symbols to include")
    position_types: list[str] = Field(default_factory=list, description="Position types to include")

    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def validate_dates(cls, v: str | None) -> str | None:
        """Validate date format if provided.
        
        Args:
            v: Date string to validate
            
        Returns:
            Validated and stripped date string, or None if input was None/empty.
            
        Raises:
            StateValidationError: If date string is too short (less than 8 characters).
        """
        if v is not None and v.strip():
            # Basic validation - could be enhanced with actual date parsing
            if len(v.strip()) < MAX_DECIMAL_PLACES:
                raise StateValidationError(
                    message="Date must be at least 8 characters (YYYY-MM-DD)",
                    validation_errors=["Date must be at least 8 characters (YYYY-MM-DD)"],
                    component="AnalyticsReport",
                )
            return v.strip()
        return None


@dataclass
class ReportMetadata:
    """Metadata for reports with validation."""

    created_by: str = Field(default="system", min_length=1, description="Report creator")
    version: str = Field(default="1.0", pattern="^[0-9]+\\.[0-9]+$", description="Report version")
    tags: list[str] = Field(default_factory=list, description="Report tags")
    priority: str = Field(
        default="normal", pattern="^(low|normal|high|urgent)$", description="Report priority"
    )

    @field_validator("tags", mode="before")
    @classmethod
    def validate_tags(cls, v: list[str] | None) -> list[str]:
        """Validate tags are non-empty strings.
        
        Args:
            v: List of tag strings to validate
            
        Returns:
            List of non-empty, stripped tag strings.
        """
        if v is None:
            return []
        # v is already validated as list[str] by Pydantic
        return [tag.strip() for tag in v if tag.strip()]


@dataclass
class PortfolioSummaryData:
    """Portfolio summary data with validation."""

    total_value: Decimal = Field(ge=0, description="Total portfolio value")
    positions_value: Decimal = Field(ge=0, description="Total positions value")
    cash_balance: Decimal = Field(ge=0, description="Cash balance")
    position_count: int = Field(ge=0, description="Number of positions")

    @field_validator("total_value", "positions_value", "cash_balance", mode="before")
    @classmethod
    def validate_decimals(cls, v: Decimal | str | float) -> Decimal:
        """Validate decimal values are finite.
        
        Args:
            v: Value to convert and validate as Decimal
            
        Returns:
            Validated finite Decimal value.
            
        Raises:
            AnalyticsValueError: If value is not finite.
        """
        value: Decimal = v if isinstance(v, Decimal) else Decimal(str(v))
        if not value.is_finite():
            raise AnalyticsValueError(
                value_type="decimal", requirement="must be finite", value=str(value)
            )
        return value


@dataclass
class PositionData:
    """Position data with validation."""

    symbol: str = Field(min_length=1, description="Position symbol")
    size: Decimal = Field(description="Position size")
    market_value: Decimal = Field(description="Market value")
    unrealized_pnl: Decimal = Field(description="Unrealized P&L")
    weight: float = Field(ge=0, le=1, description="Position weight (0-1)")

    @field_validator("size", "market_value", "unrealized_pnl", mode="before")
    @classmethod
    def validate_decimals(cls, v: Decimal | str | float) -> Decimal:
        """Validate decimal values are finite.
        
        Args:
            v: Value to convert and validate as Decimal
            
        Returns:
            Validated finite Decimal value.
            
        Raises:
            AnalyticsValueError: If value is not finite.
        """
        value: Decimal = v if isinstance(v, Decimal) else Decimal(str(v))
        if not value.is_finite():
            raise AnalyticsValueError(
                value_type="decimal", requirement="must be finite", value=str(value)
            )
        return value


@dataclass
class PerformanceMetricsData:
    """Performance metrics data with validation."""

    total_return: float = Field(default=0.0, description="Total return")
    annualized_return: float = Field(default=0.0, description="Annualized return")
    sharpe_ratio: float | None = Field(default=None, description="Sharpe ratio")
    sortino_ratio: float | None = Field(default=None, description="Sortino ratio")
    max_drawdown: float = Field(default=0.0, le=0, description="Maximum drawdown (negative)")
    volatility: float = Field(default=0.0, ge=0, description="Volatility")

    @field_validator(
        "total_return", "annualized_return", "max_drawdown", "volatility", mode="before"
    )
    @classmethod
    def validate_metrics(cls, v: float | str) -> float:
        """Validate metrics are finite.
        
        Args:
            v: Metric value to validate
            
        Returns:
            Validated finite float value within reasonable bounds.
            
        Raises:
            AnalyticsValueError: If value is not finite or outside reasonable range.
        """
        value: float = v if isinstance(v, (int, float)) else float(v)
        # Check for reasonable finite values
        if not (REASONABLE_VALUE_MIN < value < REASONABLE_VALUE_MAX):
            raise AnalyticsValueError(
                value_type="metric", requirement="must be finite and reasonable", value=str(value)
            )
        return value


@dataclass
class RiskMetricsData:
    """Risk metrics data with validation."""

    var_95: float = Field(ge=0, description="95% Value at Risk")
    var_99: float = Field(ge=0, description="99% Value at Risk")
    expected_shortfall: float = Field(ge=0, description="Expected shortfall")
    leverage_ratio: float = Field(ge=0, description="Leverage ratio")
    concentration_risk: float = Field(ge=0, le=1, description="Concentration risk (0-1)")

    @field_validator("var_95", "var_99", "expected_shortfall", "leverage_ratio", mode="before")
    @classmethod
    def validate_risk_metrics(cls, v: float | str | Decimal) -> float:
        """Validate risk metrics are finite and positive.
        
        Args:
            v: Risk metric value to validate
            
        Returns:
            Validated non-negative finite float value.
            
        Raises:
            AnalyticsValueError: If value is negative, infinite, or unreasonable.
        """
        value: float = float(v)
        if not (0 <= value < REASONABLE_VALUE_MAX):  # Must be positive and reasonable
            raise AnalyticsValueError(
                value_type="Risk metric", requirement="must be non-negative and finite"
            )
        return value


@dataclass
class AttributionData:
    """Attribution analysis data with validation."""

    by_exchange: dict[str, Decimal] = Field(
        default_factory=_decimal_dict_factory, description="Attribution by exchange"
    )
    by_symbol: dict[str, Decimal] = Field(
        default_factory=_decimal_dict_factory, description="Attribution by symbol"
    )
    by_strategy: dict[str, Decimal] = Field(
        default_factory=_decimal_dict_factory, description="Attribution by strategy"
    )
    by_sector: dict[str, Decimal] = Field(
        default_factory=_decimal_dict_factory, description="Attribution by sector"
    )

    @field_validator("by_exchange", "by_symbol", "by_strategy", "by_sector", mode="after")
    @classmethod
    def validate_attribution_dicts(
        cls, v: dict[str, Decimal | str | float | int]
    ) -> dict[str, Decimal]:
        """Validate attribution dictionaries have finite decimal values.
        
        Args:
            v: Dictionary with string keys and numeric values to validate
            
        Returns:
            Dictionary with string keys and validated finite Decimal values.
            
        Raises:
            AnalyticsValueError: If any value is not finite.
        """
        result: dict[str, Decimal] = {}
        for key, value in v.items():
            val: Decimal = value if isinstance(value, Decimal) else Decimal(str(value))
            if not val.is_finite():
                raise AnalyticsValueError(
                    value_type=f"Attribution value for {key}", requirement="must be finite"
                )
            result[key] = val
        return result


@dataclass
class ChartData:
    """Chart data with validation."""

    chart_type: str = Field(min_length=1, description="Chart type (line, bar, pie, etc.)")
    title: str = Field(min_length=1, description="Chart title")
    data: list[dict[str, float | str]] = Field(
        default_factory=_chart_data_factory, description="Chart data points"
    )
    config: dict[str, str | int | float | bool] = Field(
        default_factory=dict, description="Chart configuration"
    )

    @field_validator("chart_type", mode="before")
    @classmethod
    def validate_chart_type(cls, v: str) -> str:
        """Validate chart type is one of allowed values.
        
        Args:
            v: Chart type string to validate
            
        Returns:
            Validated lowercase chart type string.
            
        Raises:
            AnalyticsTypeError: If chart type is not in valid types list.
        """
        valid_types = {"line", "bar", "pie", "scatter", "area", "heatmap"}
        if v.lower() not in valid_types:
            raise AnalyticsTypeError(field_type="Chart type", valid_types=list(valid_types))
        return v.lower()


@dataclass
class TableData:
    """Table data with validation."""

    title: str = Field(min_length=1, description="Table title")
    headers: list[str] = Field(default_factory=list, description="Table headers")
    data: list[list[str | float | int]] = Field(
        default_factory=_table_data_factory, description="Table data rows"
    )

    @field_validator("headers", mode="before")
    @classmethod
    def validate_headers(cls, v: list[str]) -> list[str]:
        """Validate headers are non-empty strings.
        
        Args:
            v: List of header strings to validate
            
        Returns:
            List of non-empty, stripped header strings.
            
        Raises:
            AnalyticsRequiredFieldError: If headers list is empty.
        """
        if not v:
            raise AnalyticsRequiredFieldError(
                field_name="Headers", requirement="must be a non-empty list"
            )
        return [str(header).strip() for header in v if str(header).strip()]


@dataclass
class RawDataCollection:
    """Raw data collection with validation."""

    positions: dict[str, PositionData] = Field(
        default_factory=dict, description="Position data by symbol"
    )
    balances: dict[str, Decimal] = Field(
        default_factory=_decimal_dict_factory, description="Balance data"
    )
    orders: dict[str, str] = Field(default_factory=dict, description="Order data")
    trades: dict[str, str] = Field(default_factory=dict, description="Trade data")

    @field_validator("balances", mode="after")
    @classmethod
    def validate_balances(cls, v: dict[str, Decimal | str | float | int]) -> dict[str, Decimal]:
        """Validate balance values are finite decimals.
        
        Args:
            v: Dictionary with string keys and numeric balance values
            
        Returns:
            Dictionary with string keys and validated finite Decimal values.
            
        Raises:
            AnalyticsValueError: If any balance value is not finite.
        """
        result: dict[str, Decimal] = {}
        for key, value in v.items():
            val: Decimal = value if isinstance(value, Decimal) else Decimal(str(value))
            if not val.is_finite():
                raise AnalyticsValueError(
                    value_type=f"Balance value for {key}", requirement="must be finite"
                )
            result[key] = val
        return result


@dataclass
class ReportConfiguration:
    """Configuration for report generation."""

    report_type: ReportType
    output_format: OutputFormat
    time_period: str = Field(
        default="daily", description="Time period: daily, weekly, monthly, etc."
    )
    base_currency: str = Field(
        default="USD", pattern="^[A-Z]{3}$", description="Base currency code"
    )
    include_charts: bool = Field(default=True, description="Include charts in report")
    include_tables: bool = Field(default=True, description="Include tables in report")
    include_statistics: bool = Field(default=True, description="Include statistics in report")
    include_raw_data: bool = Field(default=False, description="Include raw data in report")
    filters: ReportFilters = Field(default_factory=ReportFilters, description="Report filters")
    custom_sections: list[str] = Field(default_factory=list, description="Custom report sections")
    template: str | None = Field(default=None, description="Report template")
    metadata: ReportMetadata = Field(default_factory=ReportMetadata, description="Report metadata")

    @field_validator("time_period", mode="before")
    @classmethod
    def validate_time_period(cls, v: str) -> str:
        """Validate time period is one of allowed values.
        
        Args:
            v: Time period string to validate
            
        Returns:
            Validated time period string.
            
        Raises:
            AnalyticsTypeError: If time period is not in valid periods list.
        """
        valid_periods = {"daily", "weekly", "monthly", "quarterly", "yearly"}
        if v not in valid_periods:
            raise AnalyticsTypeError(field_type="Time period", valid_types=list(valid_periods))
        return v


@dataclass
class ReportSection:
    """Individual section of a report."""

    title: str = Field(min_length=1, description="Section title")
    content_type: str = Field(description="Content type: table, chart, text, or metrics")
    data: str | dict[str, str | int | float] | list[dict[str, str | int | float]] = Field(
        description="Section data"
    )
    description: str = Field(default="", description="Section description")
    metadata: ReportMetadata = Field(default_factory=ReportMetadata, description="Section metadata")

    @field_validator("content_type", mode="before")
    @classmethod
    def validate_content_type(cls, v: str) -> str:
        """Validate content type is one of allowed values.
        
        Args:
            v: Content type string to validate
            
        Returns:
            Validated content type string.
            
        Raises:
            AnalyticsTypeError: If content type is not in valid types list.
        """
        valid_types = {"table", "chart", "text", "metrics"}
        if v not in valid_types:
            raise AnalyticsTypeError(field_type="Content type", valid_types=list(valid_types))
        return v


@dataclass
class PortfolioReport:
    """Complete portfolio report."""

    report_type: ReportType
    time_period: str = Field(description="Report time period")
    base_currency: str = Field(pattern="^[A-Z]{3}$", description="Base currency code")
    report_id: str = Field(min_length=1, description="Report identifier")
    generated_at: float = Field(gt=0, description="Report generation timestamp")

    # Report sections
    sections: list[ReportSection] = Field(default_factory=_report_section_list_factory)

    # Summary data
    summary: PortfolioSummaryData = Field(
        default_factory=lambda: PortfolioSummaryData(
            total_value=Decimal(0),
            positions_value=Decimal(0),
            cash_balance=Decimal(0),
            position_count=0,
        )
    )

    # Position data
    positions: list[PositionData] = Field(default_factory=_position_data_factory)

    # Performance metrics
    performance_metrics: PerformanceMetricsData = Field(
        default_factory=lambda: PerformanceMetricsData(
            total_return=0.0, annualized_return=0.0, max_drawdown=0.0, volatility=0.0
        )
    )

    # Risk metrics
    risk_metrics: RiskMetricsData = Field(
        default_factory=lambda: RiskMetricsData(
            var_95=0.0,
            var_99=0.0,
            expected_shortfall=0.0,
            leverage_ratio=0.0,
            concentration_risk=0.0,
        )
    )

    # Attribution analysis
    attribution: AttributionData = Field(default_factory=AttributionData)

    # Charts and visualizations
    charts: list[ChartData] = Field(default_factory=_chart_data_list_factory)

    # Tables
    tables: list[TableData] = Field(default_factory=_table_data_list_factory)

    # Raw data
    raw_data: RawDataCollection = Field(default_factory=RawDataCollection)

    # Metadata
    generation_time: float = Field(default=0.0, ge=0, description="Report generation time")
    data_quality: float = Field(default=1.0, ge=0, le=1, description="Data quality score")
    coverage: float = Field(default=1.0, ge=0, le=1, description="Data coverage score")
    warnings: list[str] = Field(default_factory=list)
    metadata: ReportMetadata = Field(default_factory=ReportMetadata)


@dataclass
class AnalyticsAlert:
    """Analytics alert with validation."""

    alert_type: str = Field(min_length=1, description="Alert type")
    severity: str = Field(pattern="^(low|medium|high|critical)$", description="Alert severity")
    message: str = Field(min_length=1, description="Alert message")
    timestamp: float = Field(gt=0, description="Alert timestamp")

    @field_validator("alert_type", mode="before")
    @classmethod
    def validate_alert_type(cls, v: str) -> str:
        """Validate alert type is non-empty.
        
        Args:
            v: Alert type string to validate
            
        Returns:
            Validated non-empty, stripped alert type string.
            
        Raises:
            AnalyticsRequiredFieldError: If alert type is empty or whitespace only.
        """
        if not v or not v.strip():
            raise AnalyticsRequiredFieldError(field_name="Alert type")
        return v.strip()


@dataclass
class AnalyticsStatistics:
    """Analytics statistics with validation."""

    mean: float = Field(description="Mean value")
    median: float = Field(description="Median value")
    std_dev: float = Field(ge=0, description="Standard deviation")
    min_value: float = Field(description="Minimum value")
    max_value: float = Field(description="Maximum value")

    @field_validator("mean", "median", "min_value", "max_value", mode="before")
    @classmethod
    def validate_statistics(cls, v: float | str | Decimal) -> float:
        """Validate statistics are finite.
        
        Args:
            v: Statistical value to validate
            
        Returns:
            Validated finite float value within reasonable bounds.
            
        Raises:
            AnalyticsValueError: If value is not finite or outside reasonable range.
        """
        value: float = float(v)
        if not (REASONABLE_VALUE_MIN < value < REASONABLE_VALUE_MAX):
            raise AnalyticsValueError(
                value_type="Statistic", requirement="must be finite and reasonable"
            )
        return value


@dataclass
class AnalyticsResults:
    """Analytics results with validation."""

    calculated: bool = Field(default=True, description="Whether calculation completed")
    result_data: dict[str, str | int | float | bool] = Field(
        default_factory=dict, description="Result data"
    )
    errors: list[str] = Field(default_factory=list, description="Calculation errors")

    @field_validator("result_data", mode="after")
    @classmethod
    def validate_result_data(
        cls, v: dict[str, str | int | float | bool]
    ) -> dict[str, str | int | float | bool]:
        """Validate result data contains only basic types.
        
        Args:
            v: Result data dictionary to validate
            
        Returns:
            Validated result data dictionary with basic types.
        """
        # Type is already constrained by annotation
        return v


@dataclass
class AnalyticsResult:
    """Result of analytics calculation."""

    analytics_type: AnalyticsType
    base_currency: str = Field(pattern="^[A-Z]{3}$", description="Base currency code")
    calculated_at: float = Field(gt=0, description="Calculation timestamp")

    # Results
    results: AnalyticsResults = Field(default_factory=AnalyticsResults)

    # Metrics
    metrics: dict[str, Decimal] = Field(default_factory=_decimal_dict_factory)

    # Statistics
    statistics: AnalyticsStatistics = Field(
        default_factory=lambda: AnalyticsStatistics(
            mean=0.0, median=0.0, std_dev=0.0, min_value=0.0, max_value=0.0
        )
    )

    # Charts data
    charts_data: list[ChartData] = Field(default_factory=_chart_data_list_factory)

    # Recommendations
    recommendations: list[str] = Field(default_factory=list)

    # Alerts
    alerts: list[AnalyticsAlert] = Field(default_factory=_analytics_alert_factory)

    # Metadata
    calculation_time: float = Field(default=0.0, ge=0, description="Calculation time in seconds")
    data_quality: float = Field(default=1.0, ge=0, le=1, description="Data quality score")
    confidence: float = Field(default=1.0, ge=0, le=1, description="Confidence score")
    metadata: ReportMetadata = Field(default_factory=ReportMetadata)


@dataclass
class DashboardMetrics:
    """Dashboard key metrics with validation."""

    total_pnl: float = Field(description="Total P&L")
    unrealized_pnl: float = Field(description="Unrealized P&L")
    total_exposure: float = Field(ge=0, description="Total exposure")
    positions_count: int = Field(ge=0, description="Number of positions")
    active_orders: int = Field(ge=0, description="Number of active orders")

    @field_validator("total_pnl", "unrealized_pnl", mode="before")
    @classmethod
    def validate_pnl_metrics(cls, v: float | str | Decimal) -> float:
        """Validate P&L metrics are finite.
        
        Args:
            v: P&L metric value to validate
            
        Returns:
            Validated finite float value within reasonable bounds.
            
        Raises:
            AnalyticsValueError: If value is not finite or outside reasonable range.
        """
        value: float = float(v)
        if not (REASONABLE_VALUE_MIN < value < REASONABLE_VALUE_MAX):
            raise AnalyticsValueError(
                value_type="P&L metric", requirement="must be finite and reasonable"
            )
        return value


@dataclass
class DashboardData:
    """Dashboard data for real-time analytics."""

    dashboard_id: str = Field(min_length=1, description="Dashboard identifier")
    updated_at: float = Field(gt=0, description="Update timestamp")

    # Key metrics
    key_metrics: DashboardMetrics = Field(
        default_factory=lambda: DashboardMetrics(
            total_pnl=0.0,
            unrealized_pnl=0.0,
            total_exposure=0.0,
            positions_count=0,
            active_orders=0,
        )
    )

    # Charts
    charts: list[ChartData] = Field(default_factory=_chart_data_list_factory)

    # Tables
    tables: list[TableData] = Field(default_factory=_table_data_list_factory)

    # Alerts
    alerts: list[AnalyticsAlert] = Field(default_factory=_analytics_alert_factory)

    # Performance summary
    performance_summary: PerformanceMetricsData = Field(
        default_factory=lambda: PerformanceMetricsData(
            total_return=0.0, annualized_return=0.0, max_drawdown=0.0, volatility=0.0
        )
    )

    # Risk summary
    risk_summary: RiskMetricsData = Field(
        default_factory=lambda: RiskMetricsData(
            var_95=0.0,
            var_99=0.0,
            expected_shortfall=0.0,
            leverage_ratio=0.0,
            concentration_risk=0.0,
        )
    )

    # Position summary
    position_summary: PortfolioSummaryData = Field(
        default_factory=lambda: PortfolioSummaryData(
            total_value=Decimal(0),
            positions_value=Decimal(0),
            cash_balance=Decimal(0),
            position_count=0,
        )
    )

    # Metadata
    refresh_rate: int = Field(default=60, gt=0, le=3600, description="Refresh rate in seconds")
    data_quality: float = Field(default=1.0, ge=0, le=1, description="Data quality score")
    metadata: ReportMetadata = Field(
        default_factory=ReportMetadata, description="Dashboard metadata"
    )


class PortfolioAnalyticsService(BasePortfolioService):
    """Comprehensive portfolio analytics and reporting service."""

    def __init__(
        self, name: str = "PortfolioAnalyticsService", config: dict[str, Any] | None = None
    ) -> None:
        """Initialize the portfolio analytics service.

        Args:
            name: Service name
            config: Configuration dictionary
        """
        super().__init__(name, config)

        # Configuration with proper type validation
        cfg = config or {}
        reports_dir = cfg.get("reports_directory", "./reports")
        self.reports_directory = str(reports_dir) if reports_dir is not None else "./reports"

        templates_dir = cfg.get("templates_directory", "./templates")
        self.templates_directory = (
            str(templates_dir) if templates_dir is not None else "./templates"
        )

        self.charts_enabled = bool(cfg.get("charts_enabled", True))
        self.real_time_analytics = bool(cfg.get("real_time_analytics", True))
        self.scheduled_reports_enabled = bool(cfg.get("scheduled_reports_enabled", True))

        export_formats = cfg.get("export_formats", ["json", "csv", "pdf"])
        self.export_formats: list[str] = (
            export_formats if isinstance(export_formats, list) else ["json", "csv", "pdf"]
        )

        max_reports = cfg.get("max_reports_history", 100)
        self.max_reports_history = (
            int(max_reports) if isinstance(max_reports, (int, float)) else 100
        )

        refresh_rate = cfg.get("dashboard_refresh_rate", 60)
        self.dashboard_refresh_rate = (
            int(refresh_rate) if isinstance(refresh_rate, (int, float)) else 60
        )

        cache_ttl = cfg.get("analytics_cache_ttl", 300)
        self.analytics_cache_ttl = int(cache_ttl) if isinstance(cache_ttl, (int, float)) else 300

        # Storage
        self.generated_reports: dict[str, PortfolioReport] = {}
        self.analytics_cache: dict[str, AnalyticsResult] = {}
        self.dashboard_data: dict[str, DashboardData] = {}
        self.report_templates: dict[str, dict[str, object]] = {}

        # Statistics
        self.analytics_statistics = {
            "total_reports_generated": 0,
            "successful_reports": 0,
            "failed_reports": 0,
            "total_analytics_calculations": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "average_report_generation_time": 0.0,
            "average_analytics_calculation_time": 0.0,
        }

        # Dependencies (will be injected)
        self.portfolio_state_manager: StateManagerProtocol | None = None
        self.metrics_aggregation_service: PortfolioServiceProtocol | None = None
        self.pnl_calculator: RealizedPnLCalculator | UnrealizedPnLCalculator | None = None
        self.exposure_calculator: ExposureCalculator | None = None
        self.performance_calculator: PerformanceCalculator | None = None

        # Background tasks
        self.dashboard_update_task: asyncio.Task[None] | None = None
        self.report_scheduler_task: asyncio.Task[None] | None = None

        # Ensure directories exist
        Path(self.reports_directory).mkdir(parents=True, exist_ok=True)
        Path(self.templates_directory).mkdir(parents=True, exist_ok=True)

        logger.info(
            "portfolio_analytics_service_initialized",
            name=name,
            reports_directory=self.reports_directory,
            real_time_analytics=self.real_time_analytics,
            scheduled_reports_enabled=self.scheduled_reports_enabled,
        )

    async def _initialize_internal(self) -> None:
        """Initialize the analytics service."""
        # Load report templates
        await self._load_report_templates()

        # Initialize default dashboards
        await self._initialize_default_dashboards()

        # Start background tasks
        if self.real_time_analytics:
            self.dashboard_update_task = asyncio.create_task(self._run_dashboard_updater())

        if self.scheduled_reports_enabled:
            self.report_scheduler_task = asyncio.create_task(self._run_report_scheduler())

        logger.info("portfolio_analytics_service_initialized_internal")

    async def _shutdown_internal(self) -> None:
        """Shutdown the analytics service."""
        # Cancel background tasks
        if self.dashboard_update_task:
            self.dashboard_update_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.dashboard_update_task

        if self.report_scheduler_task:
            self.report_scheduler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.report_scheduler_task

        logger.info("portfolio_analytics_service_shutdown_internal")

    def set_dependencies(
        self,
        portfolio_state_manager: StateManagerProtocol | None = None,
        metrics_aggregation_service: PortfolioServiceProtocol | None = None,
        pnl_calculator: RealizedPnLCalculator | UnrealizedPnLCalculator | None = None,
        exposure_calculator: ExposureCalculator | None = None,
        performance_calculator: PerformanceCalculator | None = None,
    ) -> None:
        """Set service dependencies."""
        self.portfolio_state_manager = portfolio_state_manager
        self.metrics_aggregation_service = metrics_aggregation_service
        self.pnl_calculator = pnl_calculator
        self.exposure_calculator = exposure_calculator
        self.performance_calculator = performance_calculator

    async def generate_report(
        self,
        report_type: ReportType,
        output_format: OutputFormat = OutputFormat.JSON,
        time_period: str = "daily",
        base_currency: str = "USD",
        custom_config: dict[str, object] | None = None,
    ) -> PortfolioReport:
        """Generate a portfolio report.
        
        Args:
            report_type: Type of report to generate
            output_format: Output format for the report (default: JSON)
            time_period: Time period for the report (default: "daily")
            base_currency: Base currency for calculations (default: "USD")
            custom_config: Optional custom configuration parameters
            
        Returns:
            Generated portfolio report with all sections and metrics.
            
        Raises:
            ValueError: If configuration parameters are invalid.
            TypeError: If parameter types are incorrect.
            KeyError: If required data is missing.
            AttributeError: If required attributes are missing.
            ArithmeticError: If calculations fail.
        """
        start_time = time.time()
        report_id = f"{report_type.value}_{int(start_time)}"

        try:
            # Create report configuration
            config = ReportConfiguration(
                report_type=report_type,
                output_format=output_format,
                time_period=time_period,
                base_currency=base_currency,
            )

            # Apply custom configuration by creating new instance with updated values
            if custom_config:
                # Filter out keys that don't exist in ReportConfiguration
                valid_fields = {f.name for f in config.__dataclass_fields__.values()}
                valid_config = {k: v for k, v in custom_config.items() if k in valid_fields}

                # Create new config with updated values
                if valid_config:
                    config_dict = config.__dict__.copy()
                    config_dict.update(valid_config)
                    config = ReportConfiguration(**config_dict)

            # Create report
            report = PortfolioReport(
                report_id=report_id,
                report_type=report_type,
                generated_at=start_time,
                time_period=time_period,
                base_currency=base_currency,
            )

            # Generate report sections
            await self._generate_report_sections(report, config)

            # Generate summary
            await self._generate_report_summary(report, config)

            # Generate performance metrics
            await self._generate_performance_metrics(report, config)

            # Generate risk metrics
            await self._generate_risk_metrics(report, config)

            # Generate attribution analysis
            await self._generate_attribution_analysis(report, config)

            # Generate charts and visualizations
            if config.include_charts and self.charts_enabled:
                await self._generate_charts(report, config)

            # Generate tables
            if config.include_tables:
                await self._generate_tables(report, config)

            # Add raw data if requested
            if config.include_raw_data:
                await self._add_raw_data(report, config)

            # Calculate generation time
            report.generation_time = time.time() - start_time

            # Store report
            self.generated_reports[report_id] = report

            # Export report
            await self._export_report(report, output_format)

            # Update statistics
            self.analytics_statistics["total_reports_generated"] += 1
            self.analytics_statistics["successful_reports"] += 1
            self.analytics_statistics["average_report_generation_time"] = (
                self.analytics_statistics["average_report_generation_time"] + report.generation_time
            ) / 2

            # Cleanup old reports
            if len(self.generated_reports) > self.max_reports_history:
                await self._cleanup_old_reports()

            logger.info(
                "portfolio_report_generated",
                report_id=report_id,
                report_type=report_type.value,
                output_format=output_format.value,
                generation_time=report.generation_time,
            )

        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
            self.analytics_statistics["failed_reports"] += 1
            logger.exception(
                "portfolio_report_generation_failed",
                report_id=report_id,
                report_type=report_type.value,
            )
            raise
        else:
            return report

    async def calculate_analytics(
        self,
        analytics_type: AnalyticsType,
        base_currency: str = "USD",
        parameters: dict[str, object] | None = None,
    ) -> AnalyticsResult:
        """Calculate specific analytics.
        
        Args:
            analytics_type: Type of analytics to calculate
            base_currency: Base currency for calculations (default: "USD")
            parameters: Optional calculation parameters
            
        Returns:
            Analytics result with calculated metrics and statistics.
            
        Raises:
            ValueError: If parameters or calculation inputs are invalid.
            TypeError: If parameter types are incorrect.
            KeyError: If required data is missing.
            AttributeError: If required attributes are missing.
            ArithmeticError: If calculations fail.
        """
        start_time = time.time()

        # Check cache
        cache_key = f"{analytics_type.value}_{base_currency}_{hash(str(parameters))}"
        cached_result = self._get_cached_result(cache_key)
        if cached_result is not None:
            return cached_result

        self.analytics_statistics["cache_misses"] += 1

        try:
            # Create and calculate analytics result
            result = AnalyticsResult(
                analytics_type=analytics_type, calculated_at=start_time, base_currency=base_currency
            )

            # Delegate to specific calculation method
            await self._execute_analytics_calculation(analytics_type, result, parameters)

            # Finalize result
            self._finalize_analytics_result(
                result, start_time, cache_key, analytics_type, base_currency
            )

        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
            logger.exception(
                "analytics_calculation_failed",
                analytics_type=analytics_type.value,
                base_currency=base_currency,
            )
            raise
        else:
            return result

    def _get_cached_result(self, cache_key: str) -> AnalyticsResult | None:
        """Get cached result if available and valid.
        
        Args:
            cache_key: Cache key for the analytics result
            
        Returns:
            Cached analytics result if available and not expired, None otherwise.
        """
        if cache_key in self.analytics_cache:
            cached_result = self.analytics_cache[cache_key]
            if time.time() - cached_result.calculated_at < float(self.analytics_cache_ttl):
                self.analytics_statistics["cache_hits"] += 1
                return cached_result
        return None

    async def _execute_analytics_calculation(
        self,
        analytics_type: AnalyticsType,
        result: AnalyticsResult,
        parameters: dict[str, object] | None,
    ) -> None:
        """Execute the appropriate analytics calculation based on type."""
        calculation_methods = {
            AnalyticsType.PERFORMANCE: self._calculate_performance_analytics,
            AnalyticsType.RISK: self._calculate_risk_analytics,
            AnalyticsType.ATTRIBUTION: self._calculate_attribution_analytics,
            AnalyticsType.EXPOSURE: self._calculate_exposure_analytics,
            AnalyticsType.LIQUIDITY: self._calculate_liquidity_analytics,
            AnalyticsType.CORRELATION: self._calculate_correlation_analytics,
            AnalyticsType.STRESS_TEST: self._calculate_stress_test_analytics,
            AnalyticsType.SCENARIO: self._calculate_scenario_analytics,
            AnalyticsType.BACKTESTING: self._calculate_backtesting_analytics,
        }

        calculation_method = calculation_methods.get(analytics_type)
        if calculation_method:
            await calculation_method(result, parameters)

    def _finalize_analytics_result(
        self,
        result: AnalyticsResult,
        start_time: float,
        cache_key: str,
        analytics_type: AnalyticsType,
        base_currency: str,
    ) -> None:
        """Finalize analytics result with timing, caching and statistics."""
        # Calculate calculation time
        result.calculation_time = time.time() - start_time

        # Cache result
        self.analytics_cache[cache_key] = result

        # Update statistics
        self.analytics_statistics["total_analytics_calculations"] += 1
        self.analytics_statistics["average_analytics_calculation_time"] = (
            self.analytics_statistics["average_analytics_calculation_time"]
            + result.calculation_time
        ) / 2

        logger.info(
            "analytics_calculated",
            analytics_type=analytics_type.value,
            base_currency=base_currency,
            calculation_time=result.calculation_time,
        )

    async def get_dashboard_data(self, dashboard_id: str = "main") -> DashboardData:
        """Get dashboard data.
        
        Args:
            dashboard_id: Identifier for the dashboard (default: "main")
            
        Returns:
            Dashboard data with current metrics, charts, and summaries.
        """
        if dashboard_id not in self.dashboard_data:
            await self._initialize_dashboard(dashboard_id)

        return self.dashboard_data[dashboard_id]

    async def update_dashboard(self, dashboard_id: str = "main") -> None:
        """Update dashboard data."""
        start_time = time.time()

        try:
            # Get or create dashboard
            if dashboard_id not in self.dashboard_data:
                await self._initialize_dashboard(dashboard_id)

            dashboard = self.dashboard_data[dashboard_id]

            # Update key metrics
            await self._update_dashboard_metrics(dashboard)

            # Update charts
            await self._update_dashboard_charts(dashboard)

            # Update tables
            await self._update_dashboard_tables(dashboard)

            # Update alerts
            await self._update_dashboard_alerts(dashboard)

            # Update summaries
            await self._update_dashboard_summaries(dashboard)

            # Update timestamp
            dashboard.updated_at = time.time()

            logger.debug(
                "dashboard_updated", dashboard_id=dashboard_id, update_time=time.time() - start_time
            )

        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
            logger.exception("dashboard_update_failed", dashboard_id=dashboard_id)

    async def get_report_history(
        self, report_type: ReportType | None = None, limit: int | None = None
    ) -> list[PortfolioReport]:
        """Get report history.
        
        Args:
            report_type: Optional filter by report type
            limit: Optional limit on number of reports returned
            
        Returns:
            List of historical reports, sorted by generation time (newest first).
        """
        reports = list(self.generated_reports.values())

        # Filter by type
        if report_type:
            reports = [r for r in reports if r.report_type == report_type]

        # Sort by generation time (newest first)
        reports.sort(key=lambda x: x.generated_at, reverse=True)

        # Apply limit
        if limit:
            reports = reports[:limit]

        return reports

    async def get_analytics_statistics(self) -> dict[str, object]:
        """Get analytics service statistics.
        
        Returns:
            Dictionary containing service statistics including report counts,
            cache metrics, and performance data.
        """
        return {
            **self.analytics_statistics,
            "reports_in_memory": len(self.generated_reports),
            "cached_analytics": len(self.analytics_cache),
            "active_dashboards": len(self.dashboard_data),
            "report_templates": len(self.report_templates),
        }

    async def _generate_report_sections(
        self, report: PortfolioReport, config: ReportConfiguration
    ) -> None:
        """Generate report sections."""
        # Executive summary section
        summary_section = ReportSection(
            title="Executive Summary",
            content_type="text",
            data="Portfolio overview and key highlights",
            description="High-level portfolio summary",
        )
        report.sections.append(summary_section)

        # Performance section
        performance_section = ReportSection(
            title="Performance Analysis",
            content_type="metrics",
            data={},
            description="Portfolio performance metrics and analysis",
        )
        report.sections.append(performance_section)

        # Risk section
        risk_section = ReportSection(
            title="Risk Analysis",
            content_type="metrics",
            data={},
            description="Portfolio risk metrics and analysis",
        )
        report.sections.append(risk_section)

        # Add custom sections
        for section_name in config.custom_sections:
            custom_section = ReportSection(
                title=section_name,
                content_type="custom",
                data={},
                description=f"Custom section: {section_name}",
            )
            report.sections.append(custom_section)

    async def _generate_report_summary(
        self, report: PortfolioReport, config: ReportConfiguration
    ) -> None:
        """Generate report summary."""
        # Get portfolio summary
        if self.portfolio_state_manager:
            try:
                portfolio_summary = await self.portfolio_state_manager.get_portfolio_summary()
                # Create PortfolioSummaryData from portfolio_summary
                summary_dict = portfolio_summary.model_dump()
                report.summary = PortfolioSummaryData(
                    total_value=Decimal(str(summary_dict.get("total_value", 0))),
                    positions_value=Decimal(str(summary_dict.get("positions_value", 0))),
                    cash_balance=Decimal(str(summary_dict.get("cash_balance", 0))),
                    position_count=summary_dict.get("position_count", 0),
                )
            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
                logger.warning("report_summary_generation_failed")
                # Create empty summary
                report.summary = PortfolioSummaryData(
                    total_value=Decimal(0),
                    positions_value=Decimal(0),
                    cash_balance=Decimal(0),
                    position_count=0,
                )
        else:
            # Create empty summary
            report.summary = PortfolioSummaryData(
                total_value=Decimal(0),
                positions_value=Decimal(0),
                cash_balance=Decimal(0),
                position_count=0,
            )

    async def _generate_performance_metrics(
        self, report: PortfolioReport, config: ReportConfiguration
    ) -> None:
        """Generate performance metrics."""
        if self.performance_calculator:
            try:
                # Get portfolio values for performance calculation
                portfolio_values = await self._get_portfolio_values(config.base_currency)
                if portfolio_values:
                    perf_input = PerformanceInput(portfolio_values=portfolio_values)
                    result = await self.performance_calculator.calculate(perf_input)
                    # CalculationResult has result, not data
                    performance_metrics = result.result if result.success else None
                else:
                    performance_metrics = None
                if performance_metrics:
                    report.performance_metrics = PerformanceMetricsData(
                        total_return=float(performance_metrics.total_return),
                        annualized_return=float(performance_metrics.annualized_return),
                        sharpe_ratio=(
                            float(performance_metrics.sharpe_ratio)
                            if performance_metrics.sharpe_ratio
                            else None
                        ),
                        sortino_ratio=(
                            float(performance_metrics.sortino_ratio)
                            if performance_metrics.sortino_ratio
                            else None
                        ),
                        max_drawdown=float(performance_metrics.max_drawdown),
                        volatility=float(performance_metrics.volatility),
                    )
            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
                logger.warning("performance_metrics_generation_failed")
                # Create empty performance metrics
                report.performance_metrics = PerformanceMetricsData()
        else:
            # Create empty performance metrics
            report.performance_metrics = PerformanceMetricsData()

    async def _generate_risk_metrics(
        self, report: PortfolioReport, config: ReportConfiguration
    ) -> None:
        """Generate risk metrics."""
        # Calculate actual risk metrics
        total_value = report.summary.total_value
        positions_value = report.summary.positions_value

        # Calculate leverage ratio
        leverage_ratio = float(positions_value / total_value) if total_value > 0 else 0.0

        # Calculate concentration risk (simplified - largest position as % of total)
        concentration_risk = 0.0
        if report.positions:
            largest_position_value = max(abs(float(p.market_value)) for p in report.positions)
            concentration_risk = (
                largest_position_value / float(total_value) if total_value > 0 else 0.0
            )

        report.risk_metrics = RiskMetricsData(
            var_95=0.0,  # Would need historical data for proper VaR calculation
            var_99=0.0,  # Would need historical data for proper VaR calculation
            expected_shortfall=0.0,  # Would need historical data
            leverage_ratio=leverage_ratio,
            concentration_risk=concentration_risk,
        )

    async def _generate_attribution_analysis(
        self, report: PortfolioReport, config: ReportConfiguration
    ) -> None:
        """Generate attribution analysis."""
        # Placeholder implementation
        report.attribution = AttributionData()

    async def _generate_charts(self, report: PortfolioReport, config: ReportConfiguration) -> None:
        """Generate charts and visualizations."""
        # Placeholder implementation
        report.charts = [
            ChartData(
                chart_type="line",
                title="Portfolio Performance",
                data=[],
                config={},
            ),
            ChartData(
                chart_type="pie",
                title="Asset Allocation",
                data=[],
                config={},
            ),
        ]

    async def _generate_tables(self, report: PortfolioReport, config: ReportConfiguration) -> None:
        """Generate tables."""
        # Placeholder implementation
        report.tables = [
            TableData(
                title="Top Holdings",
                headers=["Symbol", "Position", "Market Value", "Weight"],
                data=[],
            ),
            TableData(
                title="Performance by Exchange",
                headers=["Exchange", "P&L", "Return", "Allocation"],
                data=[],
            ),
        ]

    async def _add_raw_data(self, report: PortfolioReport, config: ReportConfiguration) -> None:
        """Add raw data to report."""
        # Placeholder implementation
        report.raw_data = RawDataCollection()

    async def _export_report(self, report: PortfolioReport, output_format: OutputFormat) -> None:
        """Export report to file."""
        filename = f"{report.report_id}.{output_format.value}"
        filepath = Path(self.reports_directory) / filename

        if output_format == OutputFormat.JSON:
            report_data = {
                "report_id": report.report_id,
                "report_type": report.report_type.value,
                "generated_at": report.generated_at,
                "summary": report.summary,
                "performance_metrics": report.performance_metrics,
                "risk_metrics": report.risk_metrics,
                "attribution": report.attribution,
                "charts": report.charts,
                "tables": report.tables,
                "sections": [
                    {
                        "title": section.title,
                        "content_type": section.content_type,
                        "data": section.data,
                        "description": section.description,
                    }
                    for section in report.sections
                ],
                "metadata": report.metadata,
            }

            with filepath.open("w") as f:
                json.dump(report_data, f, indent=2)

        elif output_format == OutputFormat.CSV:
            # Export as CSV (simplified)
            with filepath.open("w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["Report ID", "Type", "Generated At", "Summary"])
                writer.writerow([
                    report.report_id,
                    report.report_type.value,
                    report.generated_at,
                    str(report.summary),
                ])

    async def _cleanup_old_reports(self) -> None:
        """Clean up old reports."""
        if len(self.generated_reports) <= int(self.max_reports_history):
            return

        # Sort by generation time and remove oldest
        sorted_reports = sorted(self.generated_reports.items(), key=lambda x: x[1].generated_at)

        to_remove = sorted_reports[: len(self.generated_reports) - int(self.max_reports_history)]

        for report_id, _report in to_remove:
            del self.generated_reports[report_id]

    async def _load_report_templates(self) -> None:
        """Load report templates."""
        templates_dir = Path(self.templates_directory)
        if templates_dir.exists():
            for template_file in templates_dir.glob("*.json"):
                try:
                    with template_file.open() as f:
                        template_data = json.load(f)

                    template_name = template_file.stem
                    self.report_templates[template_name] = template_data

                except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
                    logger.exception("template_loading_failed", file=str(template_file))

    async def _initialize_default_dashboards(self) -> None:
        """Initialize default dashboards."""
        await self._initialize_dashboard("main")
        await self._initialize_dashboard("risk")
        await self._initialize_dashboard("performance")

    async def _initialize_dashboard(self, dashboard_id: str) -> None:
        """Initialize a dashboard."""
        dashboard = DashboardData(
            dashboard_id=dashboard_id,
            updated_at=time.time(),
            refresh_rate=int(self.dashboard_refresh_rate),
        )

        self.dashboard_data[dashboard_id] = dashboard
        await self.update_dashboard(dashboard_id)

    async def _update_dashboard_metrics(self, dashboard: DashboardData) -> None:
        """Update dashboard key metrics."""
        # Placeholder implementation
        dashboard.key_metrics = DashboardMetrics(
            total_pnl=0.0,
            unrealized_pnl=0.0,
            total_exposure=0.0,
            positions_count=0,
            active_orders=0,
        )

    async def _update_dashboard_charts(self, dashboard: DashboardData) -> None:
        """Update dashboard charts."""
        # Placeholder implementation
        dashboard.charts = [
            ChartData(
                chart_type="line",
                title="P&L Over Time",
                data=[],
                config={"id": "pnl_chart"},
            ),
            ChartData(
                chart_type="bar",
                title="Exposure by Exchange",
                data=[],
                config={"id": "exposure_chart"},
            ),
        ]

    async def _update_dashboard_tables(self, dashboard: DashboardData) -> None:
        """Update dashboard tables."""
        # Placeholder implementation
        dashboard.tables = [
            TableData(
                title="Active Positions",
                headers=["Symbol", "Size", "Market Value", "P&L"],
                data=[],
            ),
        ]

    async def _update_dashboard_alerts(self, dashboard: DashboardData) -> None:
        """Update dashboard alerts."""
        # Placeholder implementation
        dashboard.alerts = []

    async def _update_dashboard_summaries(self, dashboard: DashboardData) -> None:
        """Update dashboard summaries."""
        # Placeholder implementation
        dashboard.performance_summary = PerformanceMetricsData(
            total_return=0.0, annualized_return=0.0, max_drawdown=0.0, volatility=0.0
        )
        dashboard.risk_summary = RiskMetricsData(
            var_95=0.0,
            var_99=0.0,
            expected_shortfall=0.0,
            leverage_ratio=0.0,
            concentration_risk=0.0,
        )
        dashboard.position_summary = PortfolioSummaryData(
            total_value=Decimal(0),
            positions_value=Decimal(0),
            cash_balance=Decimal(0),
            position_count=0,
        )

    async def _calculate_performance_analytics(
        self, result: AnalyticsResult, parameters: dict[str, object] | None
    ) -> None:
        """Calculate performance analytics."""
        # Placeholder implementation
        result.results = AnalyticsResults(result_data={"performance_calculated": True})
        result.metrics = {}

    async def _calculate_risk_analytics(
        self, result: AnalyticsResult, parameters: dict[str, object] | None
    ) -> None:
        """Calculate risk analytics."""
        # Placeholder implementation
        result.results = AnalyticsResults(result_data={"risk_calculated": True})
        result.metrics = {}

    async def _calculate_attribution_analytics(
        self, result: AnalyticsResult, parameters: dict[str, object] | None
    ) -> None:
        """Calculate attribution analytics."""
        # Placeholder implementation
        result.results = AnalyticsResults(result_data={"attribution_calculated": True})
        result.metrics = {}

    async def _calculate_exposure_analytics(
        self, result: AnalyticsResult, parameters: dict[str, object] | None
    ) -> None:
        """Calculate exposure analytics."""
        # Placeholder implementation
        result.results = AnalyticsResults(result_data={"exposure_calculated": True})
        result.metrics = {}

    async def _calculate_liquidity_analytics(
        self, result: AnalyticsResult, parameters: dict[str, object] | None
    ) -> None:
        """Calculate liquidity analytics."""
        # Placeholder implementation
        result.results = AnalyticsResults(result_data={"liquidity_calculated": True})
        result.metrics = {}

    async def _calculate_correlation_analytics(
        self, result: AnalyticsResult, parameters: dict[str, object] | None
    ) -> None:
        """Calculate correlation analytics."""
        # Placeholder implementation
        result.results = AnalyticsResults(result_data={"correlation_calculated": True})
        result.metrics = {}

    async def _calculate_stress_test_analytics(
        self, result: AnalyticsResult, parameters: dict[str, object] | None
    ) -> None:
        """Calculate stress test analytics."""
        # Placeholder implementation
        result.results = AnalyticsResults(result_data={"stress_test_calculated": True})
        result.metrics = {}

    async def _calculate_scenario_analytics(
        self, result: AnalyticsResult, parameters: dict[str, object] | None
    ) -> None:
        """Calculate scenario analytics."""
        # Placeholder implementation
        result.results = AnalyticsResults(result_data={"scenario_calculated": True})
        result.metrics = {}

    async def _calculate_backtesting_analytics(
        self, result: AnalyticsResult, parameters: dict[str, object] | None
    ) -> None:
        """Calculate backtesting analytics."""
        # Placeholder implementation
        result.results = AnalyticsResults(result_data={"backtesting_calculated": True})
        result.metrics = {}

    async def _get_portfolio_values(self, base_currency: str) -> list[Decimal] | None:
        """Get portfolio values for performance calculation.

        Args:
            base_currency: Base currency for valuation

        Returns:
            List of portfolio values over time, or None if unavailable
        """
        if not self.portfolio_state_manager:
            return None

        try:
            # Get portfolio state and extract values
            # This is a placeholder - actual implementation would get historical values
            snapshot = await self.portfolio_state_manager.get_portfolio_snapshot()
        except (ValueError, TypeError, AttributeError, KeyError):
            logger.exception("get_portfolio_values_failed")
            return None
        else:
            # StateManagerProtocol guarantees PortfolioSnapshot return type
            return [snapshot.total_account_value]

    async def _run_dashboard_updater(self) -> None:
        """Background task for updating dashboards."""
        while True:
            try:
                await asyncio.sleep(float(self.dashboard_refresh_rate))

                # Update all dashboards
                for dashboard_id in self.dashboard_data:
                    await self.update_dashboard(dashboard_id)

            except asyncio.CancelledError:
                break
            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
                logger.exception("dashboard_updater_error")
                await asyncio.sleep(float(self.dashboard_refresh_rate))

    async def _run_report_scheduler(self) -> None:
        """Background task for scheduled reports."""
        while True:
            try:
                await asyncio.sleep(3600)  # Check every hour

                # This would implement scheduled report generation
                # For now, just a placeholder

            except asyncio.CancelledError:
                break
            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
                logger.exception("report_scheduler_error")
                await asyncio.sleep(3600)
