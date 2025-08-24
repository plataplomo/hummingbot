# Hummingbot Connector Implementation Documentation

## Overview
This directory contains comprehensive documentation for implementing new exchange connectors for Hummingbot, following the official v2.1 standards.

## Directory Structure

```
workflow/hummingbot_connector_clean/
├── README.md                        # This file
├── IMPLEMENTATION_GUIDE.md          # Complete implementation guide
├── spot/                           # Spot connector documentation
│   ├── spot-connector-checklist.md # Developer checklist for spot connectors
│   └── spot-connector-qa-checklist.md # QA testing checklist
└── perp/                           # Perpetual connector documentation
    ├── perp-connector-checklist.md # Developer checklist for perpetual connectors
    └── perp-connector-qa-checklist.md # QA testing checklist
```

## Documentation Contents

### 1. Implementation Guide (`IMPLEMENTATION_GUIDE.md`)
Complete guide covering:
- Architecture overview
- Development workflow
- Code patterns and best practices
- Common pitfalls and solutions
- Submission process

### 2. Spot Connector Documentation (`spot/`)

#### Developer Checklist (`spot-connector-checklist.md`)
- File structure requirements
- API endpoint requirements
- Required methods to implement
- Testing requirements
- Code examples and patterns

#### QA Checklist (`spot-connector-qa-checklist.md`)
- Comprehensive testing procedures
- Expected behaviors
- Performance benchmarks
- Test report template

### 3. Perpetual Connector Documentation (`perp/`)

#### Developer Checklist (`perp-connector-checklist.md`)
- Additional requirements for derivatives
- Position management implementation
- Leverage and funding rate handling
- Perpetual-specific methods

#### QA Checklist (`perp-connector-qa-checklist.md`)
- Position testing procedures
- Leverage and margin testing
- Funding rate validation
- Liquidation testing guidelines

## Key Differences: Spot vs Perpetual

### Spot Connectors
- Simple buy/sell orders
- Located in: `hummingbot/connector/exchange/`
- Single inheritance from `ExchangeBase`
- Focus on order book and balance management

### Perpetual Connectors
- Position-based trading with leverage
- Located in: `hummingbot/connector/derivative/`
- Multiple inheritance: `ExchangeBase` + `PerpetualTrading`
- Additional features:
  - Position tracking
  - Leverage management
  - Funding rates
  - Liquidation handling
  - Margin calculations

## Implementation Standards

### Core Principles
1. **WebSocket-First**: All real-time data via WebSocket
2. **No Hardcoding**: Everything from config or API
3. **Type Safety**: Full type hints required
4. **Fail Fast**: No silent failures
5. **Test Coverage**: Minimum 80% coverage

### Required API Features

#### Spot Minimum Requirements
- REST: Market data, trading rules, orders, balances
- WebSocket: Order book, trades, user updates

#### Perpetual Additional Requirements
- REST: Positions, leverage, funding info
- WebSocket: Position updates, funding updates

## Development Workflow

### Phase 1: Setup
1. Review exchange API documentation
2. Study reference implementations (Binance for spot, Bybit for perp)
3. Create file structure from templates

### Phase 2: Implementation
1. Constants and configuration
2. Authentication
3. Order book data source
4. User stream data source
5. Main exchange/derivative class
6. Trading operations

### Phase 3: Testing
1. Unit tests (>80% coverage)
2. Integration tests
3. Manual QA testing
4. 24-hour stability test

### Phase 4: Submission
1. Create pull request to `development` branch
2. Submit New Connector Proposal (NCP)
3. Include documentation updates

## Best Practices

### Use Existing Patterns
- Copy from established connectors
- Follow Hummingbot conventions
- Don't reinvent standard features

### Testing Approach
- Start with testnet when available
- Use small amounts on mainnet
- Test edge cases thoroughly
- Document known limitations

### Common Pitfalls to Avoid
1. Hardcoding values instead of using config
2. Using floats for financial calculations (use Decimal)
3. Silent error handling (fail fast instead)
4. Missing reconnection logic
5. Incorrect position tracking (perpetuals)
6. Wrong leverage calculations (perpetuals)

## Resources

### Official Templates
- [Spot Connector v2.1 Notion Template](https://hummingbot-foundation.notion.site/Spot-Connector-v2-1-1cc43830938445c9974f43ef861d59f1)
- [Perp Connector v2.1 Notion Template](https://hummingbot-foundation.notion.site/Perp-Connector-v2-1-57d8391eb54c40929f77067355fd551e)

### Reference Implementations
- **Best Spot Example**: `hummingbot/connector/exchange/binance/`
- **Best Perp Example**: `hummingbot/connector/derivative/binance_perpetual/`
- **Modern Example**: `hummingbot/connector/derivative/hyperliquid_perpetual/`

### Community Support
- [Discord](https://discord.gg/hummingbot)
- [GitHub Discussions](https://github.com/hummingbot/hummingbot/discussions)
- [Documentation](https://docs.hummingbot.org)

## Quick Start

### For Spot Connector Development
1. Read `IMPLEMENTATION_GUIDE.md`
2. Follow `spot/spot-connector-checklist.md`
3. Test using `spot/spot-connector-qa-checklist.md`

### For Perpetual Connector Development
1. Read `IMPLEMENTATION_GUIDE.md`
2. Follow `perp/perp-connector-checklist.md`
3. Test using `perp/perp-connector-qa-checklist.md`

## Backpack Connector Context

This documentation was prepared specifically for implementing the Backpack exchange connector for Hummingbot. Backpack requires both spot and perpetual implementations, following the patterns established in our CyberDelta codebase while adhering to Hummingbot's standards.

### Key Considerations for Backpack
- Use existing `cyberdelta/apis/backpack/` logic as reference
- Maintain consistency with Hummingbot code style
- Follow established patterns from Binance and Hyperliquid connectors
- Ensure all tests pass without fallbacks or hardcoded values
- Use proper constants and configuration

## Validation Checklist

Before considering implementation complete:

- [ ] All required files present
- [ ] No hardcoded values
- [ ] All tests passing
- [ ] >80% test coverage
- [ ] 24-hour stability test passed
- [ ] Documentation complete
- [ ] Follows Hummingbot conventions
- [ ] Consistent with CyberDelta patterns
- [ ] Manual QA completed
- [ ] Known issues documented
