# Portfolio Examples

This directory contains examples and demonstrations of the portfolio management system.

## Files

### `advanced_integration_example.py`
Advanced integration example showing:
- Custom portfolio strategies with rebalancing
- Risk management and monitoring  
- Performance analysis
- Advanced usage patterns with middleware
- Proper error handling and resilience

This example demonstrates sophisticated usage of the portfolio system including:
- Portfolio rebalancing strategies
- Risk limit enforcement
- Real-time risk monitoring
- Emergency risk reduction procedures
- Performance analytics and reporting

## Usage

Run the advanced integration example:

```bash
python examples/portfolio/advanced_integration_example.py
```

Note: This example requires proper AppSettings configuration to run successfully.

## Architecture

The examples follow the new portfolio architecture with:
- Direct AppSettings access (no dependency injection)
- Protocol-based dependencies (StateContainerProtocol, ValidationServiceProtocol)
- TypedStateManager and TypedCalculator base classes
- Clean separation of concerns
- Comprehensive error handling and logging