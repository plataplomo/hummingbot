"""Focused portfolio reporting service."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.core.portfolio.models.portfolio_state import PortfolioStateData as PortfolioState


class ReportConfiguration(BaseModel):
    """Report generation configuration."""

    format: str = Field(default="json", description="Report output format")
    include_charts: bool = Field(default=False, description="Include chart data")
    include_tables: bool = Field(default=True, description="Include table data")
    sections: list[str] = Field(default_factory=lambda: ["summary", "performance"], description="Report sections")

    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class PortfolioReport(BaseModel):
    """Portfolio report data structure."""

    report_id: str = Field(..., description="Unique report identifier")
    timestamp: str = Field(..., description="Report generation timestamp")
    summary: dict[str, Any] = Field(default_factory=dict, description="Report summary")
    performance_data: dict[str, Any] = Field(default_factory=dict, description="Performance metrics")
    charts: list[dict[str, Any]] = Field(default_factory=list, description="Chart data")
    tables: list[dict[str, Any]] = Field(default_factory=list, description="Table data")

    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class ReportingService(BaseModel):
    """Generates portfolio reports only."""

    default_format: str = Field(default="json", description="Default report format")

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    async def generate_report(
        self,
        portfolio_state: PortfolioState,
        config: ReportConfiguration | None = None
    ) -> PortfolioReport:
        """Generate portfolio report based on configuration."""
        if config is None:
            config = ReportConfiguration()

        report = PortfolioReport(
            report_id=f"report_{portfolio_state.timestamp}",
            timestamp=str(portfolio_state.timestamp),
        )

        # Generate each requested section
        if "summary" in config.sections:
            report.summary = await self._generate_summary(portfolio_state)

        if "performance" in config.sections:
            report.performance_data = await self._generate_performance_section(portfolio_state)

        if config.include_charts:
            report.charts = await self._generate_charts(portfolio_state)

        if config.include_tables:
            report.tables = await self._generate_tables(portfolio_state)

        return report

    async def export_report(
        self,
        report: PortfolioReport,
        format: str = "json"
    ) -> str:
        """Export report to specified format."""
        if format == "json":
            return report.model_dump_json(indent=2)
        elif format == "csv":
            return await self._export_to_csv(report)
        else:
            raise ValueError(f"Unsupported export format: {format}")

    async def _generate_summary(self, state: PortfolioState) -> dict[str, Any]:
        """Generate report summary section."""
        return {
            "total_exchanges": len(state.exchange_summaries),
            "total_positions": state.active_positions,
            "timestamp": str(state.updated_at),
        }

    async def _generate_performance_section(self, state: PortfolioState) -> dict[str, Any]:
        """Generate performance section."""
        return {
            "total_capital": str(state.total_account_value),
            "realized_pnl": "0.0",  # Would integrate with performance analytics
            "unrealized_pnl": "0.0",  # Would integrate with performance analytics
        }

    async def _generate_charts(self, state: PortfolioState) -> list[dict[str, Any]]:
        """Generate chart data."""
        return [
            {"type": "pie", "title": "Asset Allocation", "data": []},
            {"type": "line", "title": "Performance Over Time", "data": []},
        ]

    async def _generate_tables(self, state: PortfolioState) -> list[dict[str, Any]]:
        """Generate table data."""
        return [
            {"title": "Positions", "headers": ["Exchange", "Symbol", "Size"], "rows": []},
            {"title": "Balances", "headers": ["Exchange", "Asset", "Amount"], "rows": []},
        ]

    async def _export_to_csv(self, report: PortfolioReport) -> str:
        """Export report data to CSV format."""
        import io
        import csv
        
        output = io.StringIO()
        
        # Write summary section
        writer = csv.writer(output)
        writer.writerow(["Report Section", "Key", "Value"])
        
        # Summary data
        for key, value in report.summary.items():
            writer.writerow(["Summary", key, str(value)])
        
        # Performance data
        for key, value in report.performance_data.items():
            writer.writerow(["Performance", key, str(value)])
        
        return output.getvalue()