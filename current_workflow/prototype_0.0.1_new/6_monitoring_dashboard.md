# Monitoring Dashboard Strategy - Prototype 0.0.1

This document outlines the strategy for implementing a monitoring dashboard for CyberDeltaEngine, detailing the requirements, technology options, implementation approach, and key metrics to be monitored.

## Monitoring Goals

1. **System Health Monitoring**: Track the operational status of all system components
2. **Performance Metrics**: Monitor execution times, latency, and throughput
3. **Trading Activity Visualization**: Display current positions, orders, and recent executions
4. **Risk Monitoring**: Visualize exposure, margin utilization, and risk metrics
5. **Alerting Capabilities**: Provide timely notifications for critical events
6. **Debugging Support**: Enable quick identification of issues during development

## Dashboard Components

### 1. System Status Panel

**Purpose**: Provide at-a-glance view of component health
**Key Metrics**:
- Component status (online/offline)
- Connection status to exchanges
- WebSocket connection health
- CPU/Memory usage
- Error rate by component

**Visualization**: Status grid with color indicators (green/yellow/red)

### 2. Position Monitor

**Purpose**: Track current positions and P&L
**Key Metrics**:
- Open positions by asset/exchange
- Unrealized P&L (total and by position)
- Position size relative to account
- Entry price vs. current price
- Funding payments (upcoming and historical)

**Visualization**: Sortable table with position details

### 3. Order Tracker

**Purpose**: Monitor pending and recent orders
**Key Metrics**:
- Pending orders by status
- Recent executions
- Fill rates
- Order latency
- Rejection rates

**Visualization**: Timeline view of orders with status indicators

### 4. Opportunity Monitor

**Purpose**: Visualize detected arbitrage opportunities
**Key Metrics**:
- Current funding rate differentials
- Potential profit per opportunity
- Historical opportunity frequency
- Execution success rate
- Missed opportunities

**Visualization**: Heatmap of markets with opportunity indicators

### 5. Performance Dashboard

**Purpose**: Track system performance metrics
**Key Metrics**:
- Strategy performance metrics
- Data processing latency
- Order execution time
- API request rate/limits
- Database query performance

**Visualization**: Time-series charts with key performance indicators

### 6. Alert Console

**Purpose**: Display system alerts and notifications
**Key Metrics**:
- Critical errors
- Margin warnings
- Connectivity issues
- Performance degradation
- Unusual market conditions

**Visualization**: Prioritized alert feed with acknowledgment controls

## Technology Options

### Option 1: Lightweight Web Dashboard (Recommended for Prototype)

**Technologies**:
- FastAPI for backend API
- Chart.js for visualizations
- Simple HTML/CSS/JS frontend
- WebSocket for real-time updates

**Advantages**:
- Simple implementation
- Minimal dependencies
- Fast development cycle
- Easy to integrate with existing Python codebase

**Considerations**:
- Limited customization
- Basic visualizations
- May require manual refresh for some data

### Option 2: Full-Featured Dashboard Framework

**Technologies**:
- Grafana for visualization
- Prometheus for metrics collection
- InfluxDB for time-series data
- Alertmanager for alert handling

**Advantages**:
- Comprehensive monitoring capabilities
- Enterprise-grade visualizations
- Built-in alerting system
- Extensive customization options

**Considerations**:
- Higher complexity
- Additional infrastructure requirements
- Steeper learning curve
- Overkill for prototype phase

### Option 3: Terminal-Based Dashboard

**Technologies**:
- Rich/Textual library for TUI
- Built-in Python logging
- Direct integration with bot components

**Advantages**:
- Zero external dependencies
- Lightweight resource usage
- Tight integration with trading system
- Suitable for SSH sessions

**Considerations**:
- Limited visualization capabilities
- Less intuitive for non-technical users
- Scaling limitations for complex dashboards

## Phased Implementation Approach

### Phase 0: Logging-Based Monitoring (Current State)

- Use detailed logging for component status
- Monitor log files manually
- Basic command-line reporting of positions and orders
- No real-time visualization

### Phase 1: CLI Dashboard (Week 2-3)

Implement a terminal-based dashboard using Rich/Textual:

```python
# Example CLI dashboard implementation
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
import asyncio

class TradingDashboard:
    def __init__(self):
        self.console = Console()
        self.layout = Layout()
        self.layout.split_column(
            Layout(name="header", size=3),
            Layout(name="main"),
            Layout(name="footer", size=3)
        )
        self.layout["main"].split_row(
            Layout(name="positions"),
            Layout(name="orders")
        )
        
    async def update_position_panel(self, positions):
        """Update the positions panel with current position data."""
        table = Table(title="Current Positions")
        table.add_column("Symbol")
        table.add_column("Size")
        table.add_column("Entry Price")
        table.add_column("Current Price")
        table.add_column("PnL")
        
        for pos in positions:
            pnl = pos["current_price"] - pos["entry_price"]
            pnl_color = "green" if pnl > 0 else "red"
            table.add_row(
                pos["symbol"],
                str(pos["size"]),
                f"${pos['entry_price']:.2f}",
                f"${pos['current_price']:.2f}",
                f"[{pnl_color}]${pnl * pos['size']:.2f}"
            )
            
        self.layout["positions"].update(Panel(table))
        
    async def update_order_panel(self, orders):
        """Update the orders panel with current order data."""
        table = Table(title="Active Orders")
        table.add_column("ID")
        table.add_column("Symbol")
        table.add_column("Type")
        table.add_column("Side")
        table.add_column("Price")
        table.add_column("Size")
        table.add_column("Status")
        
        for order in orders:
            status_color = {
                "open": "yellow",
                "filled": "green",
                "canceled": "red",
                "rejected": "red"
            }.get(order["status"], "white")
            
            table.add_row(
                order["id"][:8],
                order["symbol"],
                order["type"],
                order["side"],
                f"${order['price']:.2f}" if order["price"] else "Market",
                str(order["size"]),
                f"[{status_color}]{order['status'].upper()}"
            )
            
        self.layout["orders"].update(Panel(table))
        
    async def update_system_status(self, components):
        """Update system status header."""
        table = Table(show_header=False, expand=True)
        table.add_column("Component")
        table.add_column("Status")
        
        for name, status in components.items():
            status_color = "green" if status["online"] else "red"
            table.add_row(
                name,
                f"[{status_color}]{'●' if status['online'] else '○'} {status['message']}"
            )
            
        self.layout["header"].update(Panel(table, title="System Status"))
        
    async def render_dashboard(self):
        """Render the complete dashboard."""
        self.console.clear()
        self.console.print(self.layout)
        
    async def start(self, update_interval=1.0):
        """Start the dashboard update loop."""
        while True:
            # In a real implementation, you would fetch this data from your trading system
            await self.update_system_status({
                "Data Handler": {"online": True, "message": "Connected"},
                "Execution": {"online": True, "message": "Active"},
                "Risk Manager": {"online": True, "message": "Monitoring"}
            })
            
            await self.update_position_panel([
                {"symbol": "BTC-PERP", "size": 0.5, "entry_price": 50000, "current_price": 51000},
                {"symbol": "ETH-PERP", "size": -2.0, "entry_price": 3000, "current_price": 2900}
            ])
            
            await self.update_order_panel([
                {"id": "ord12345678", "symbol": "BTC-PERP", "type": "limit", "side": "buy", 
                 "price": 49500, "size": 0.2, "status": "open"},
                {"id": "ord87654321", "symbol": "ETH-PERP", "type": "market", "side": "sell", 
                 "price": None, "size": 1.0, "status": "filled"}
            ])
            
            await self.render_dashboard()
            await asyncio.sleep(update_interval)

# Usage
async def main():
    dashboard = TradingDashboard()
    await dashboard.start()

if __name__ == "__main__":
    asyncio.run(main())
```

### Phase 2: Simple Web Dashboard (Week 4-5)

Implement a lightweight web dashboard using FastAPI and Chart.js:

```python
# Example FastAPI dashboard backend
from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import json
import asyncio
from datetime import datetime

app = FastAPI()

# Serve static files (HTML, CSS, JS)
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def get_dashboard():
    """Serve the dashboard HTML page."""
    with open("static/dashboard.html") as f:
        return HTMLResponse(content=f.read())

@app.get("/api/system-status")
async def get_system_status():
    """API endpoint for system status."""
    # In a real implementation, fetch this from your trading system
    return {
        "components": {
            "data_handler": {"status": "online", "last_update": datetime.now().isoformat()},
            "portfolio_tracker": {"status": "online", "last_update": datetime.now().isoformat()},
            "signal_generator": {"status": "online", "last_update": datetime.now().isoformat()},
            "risk_manager": {"status": "online", "last_update": datetime.now().isoformat()},
            "execution_handler": {"status": "online", "last_update": datetime.now().isoformat()}
        },
        "connections": {
            "hyperliquid": {"status": "connected", "latency_ms": 120},
            "database": {"status": "connected", "latency_ms": 5}
        },
        "resources": {
            "cpu_usage": 35.2,
            "memory_usage": 512.7,
            "disk_usage": 28.1
        }
    }

@app.get("/api/positions")
async def get_positions():
    """API endpoint for current positions."""
    # In a real implementation, fetch this from your portfolio tracker
    return {
        "positions": [
            {"symbol": "BTC-PERP", "exchange": "hyperliquid", "size": 0.5, 
             "entry_price": 50000, "current_price": 51000, "pnl": 500},
            {"symbol": "ETH-PERP", "exchange": "hyperliquid", "size": -2.0, 
             "entry_price": 3000, "current_price": 2900, "pnl": 200}
        ],
        "total_pnl": 700,
        "account_value": 25700
    }

@app.get("/api/orders")
async def get_orders():
    """API endpoint for orders."""
    # In a real implementation, fetch this from your execution handler
    return {
        "active_orders": [
            {"id": "ord12345678", "symbol": "BTC-PERP", "exchange": "hyperliquid", 
             "type": "limit", "side": "buy", "price": 49500, "size": 0.2, 
             "status": "open", "created_at": "2023-06-01T12:34:56Z"}
        ],
        "recent_orders": [
            {"id": "ord87654321", "symbol": "ETH-PERP", "exchange": "hyperliquid", 
             "type": "market", "side": "sell", "price": 2950, "size": 1.0, 
             "status": "filled", "created_at": "2023-06-01T12:30:00Z", 
             "filled_at": "2023-06-01T12:30:01Z"}
        ]
    }

@app.get("/api/opportunities")
async def get_opportunities():
    """API endpoint for current arbitrage opportunities."""
    # In a real implementation, fetch this from your signal generator
    return {
        "opportunities": [
            {"symbol": "BTC-PERP", "exchanges": ["hyperliquid", "binance"], 
             "funding_diff": 0.0012, "estimated_profit": 150, "confidence": 0.85},
            {"symbol": "ETH-PERP", "exchanges": ["hyperliquid", "okx"], 
             "funding_diff": 0.0008, "estimated_profit": 80, "confidence": 0.75}
        ]
    }

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time updates."""
    await websocket.accept()
    try:
        while True:
            # In a real implementation, you would push updates from your trading system
            await websocket.send_json({
                "type": "position_update",
                "data": {"symbol": "BTC-PERP", "size": 0.5, "current_price": 51100, "pnl": 550}
            })
            await asyncio.sleep(2)
    except Exception:
        await websocket.close()
```

### Phase 3: Full-Featured Dashboard (Post-Prototype)

- Implement Grafana/Prometheus stack
- Create custom panels and dashboards
- Set up alert rules and notifications
- Integrate with time-series database

## Implementation Priorities for Prototype 0.0.1

For Prototype 0.0.1, the monitoring dashboard will focus on:

1. **Implement Phase 1 (CLI Dashboard)** for:
   - Basic system status monitoring
   - Position and order tracking
   - Simple performance metrics
   - Error reporting

2. **Begin planning for Phase 2 (Web Dashboard)** by:
   - Defining API endpoints
   - Designing the dashboard layout
   - Evaluating technology options

## Integration Points

The monitoring dashboard will integrate with the following system components:

1. **Data Handler**: Market data statistics and connection status
2. **Portfolio Tracker**: Position information and P&L calculations
3. **Signal Generator**: Opportunity detection metrics
4. **Risk Manager**: Risk metrics and exposure calculations
5. **Execution Handler**: Order status and execution metrics
6. **Main Application**: Overall system health and resource usage

## Deployment Considerations

### Local Development
- CLI dashboard runs in same process as trading bot
- Web dashboard served locally on development machine
- Static HTML/JS/CSS files for frontend

### Production
- Dedicated monitoring process
- Secured web interface with authentication
- Automated alerts via email/Slack/Telegram

## Testing Strategy

1. **Unit Tests**: Test dashboard components in isolation
2. **Integration Tests**: Verify dashboard with mock data sources
3. **UI Tests**: Ensure dashboard renders correctly
4. **Performance Tests**: Measure dashboard impact on system resources

## Conclusion and Recommendations

For Prototype 0.0.1, we recommend implementing the **Phase 1 CLI Dashboard** using the Rich/Textual library. This approach provides essential monitoring capabilities with minimal development effort and no additional infrastructure requirements.

The CLI dashboard can be run alongside the trading system during development and testing, providing immediate feedback on system behavior and performance without the complexity of a web-based solution.

As the system matures beyond the prototype phase, transitioning to a web-based dashboard (Phase 2) will provide more comprehensive visualization and remote monitoring capabilities. 