# Top Open Source Crypto Trading Engines (2025)

## Ranking Criteria
- **GitHub Stars**: Primary popularity metric
- **Active Development**: Recent commits in 2024-2025
- **Crypto Focus**: Specifically designed for cryptocurrency markets
- **Community Size**: Active issues, pull requests, and discussions

---

## 1. 🥇 **Freqtrade**
### The Most Popular Open Source Crypto Bot

**GitHub**: https://github.com/freqtrade/freqtrade

**GitHub Stats:**
- ⭐ **28,000+ stars** (Most starred crypto trading bot)
- 🔄 **Very Active**: Daily commits in 2025
- 👥 **Contributors**: 300+
- 🍴 **Forks**: 6,000+

**Key Features:**
- Written in Python
- Extensive backtesting with Hyperopt optimization
- Machine Learning integration (TensorFlow, PyTorch)
- 130+ supported exchanges via CCXT
- Telegram bot integration
- Docker support
- Advanced strategy development framework

**Infrastructure:**
- **Database**: SQLite/PostgreSQL/MySQL
- **Message Queue**: None (uses Python asyncio)
- **Cache**: In-memory with optional Redis
- **Data Storage**: JSON/HDF5/Parquet
- **Web UI**: FreqUI (Vue.js frontend)

**Documentation**: https://www.freqtrade.io/

**Why #1:**
- Largest community and ecosystem
- Most comprehensive documentation
- Regular releases (monthly)
- Professional-grade features
- Active Discord with 10,000+ members

---

## 2. 🥈 **Hummingbot**
### Professional Market Making Platform

**GitHub**: https://github.com/hummingbot/hummingbot

**GitHub Stats:**
- ⭐ **8,500+ stars**
- 🔄 **Very Active**: v2.0 released in 2024
- 👥 **Contributors**: 200+
- 🍴 **Forks**: 3,500+

**Key Features:**
- Python-based with Cython performance optimizations
- Focus on market making and arbitrage
- CEX and DEX support (including Uniswap, dYdX)
- Built-in strategies (PMM, Cross-exchange MM, Arbitrage)
- Professional liquidity mining features

**Infrastructure:**
- **Database**: SQLite (default), PostgreSQL support
- **Message Queue**: Custom event-driven architecture
- **Performance**: Cython compiled modules
- **Config**: YAML-based strategy configs

**Documentation**: https://docs.hummingbot.org/

**Why #2:**
- Professional-grade market making
- Institutional backing (CoinAlpha Inc.)
- Liquidity mining rewards integration
- Strong DeFi support

---

## 3. 🥉 **Jesse**
### AI-Powered Trading Framework

**GitHub**: https://github.com/jesse-ai/jesse

**GitHub Stats:**
- ⭐ **5,500+ stars**
- 🔄 **Active**: Regular 2025 updates
- 👥 **Contributors**: 50+
- 🍴 **Forks**: 600+

**Key Features:**
- Python framework for algo trading
- AI/ML first approach (GPT integration for strategy generation)
- Advanced backtesting with genetic algorithms
- Clean API design
- Focus on futures trading

**Infrastructure:**
- **Database**: PostgreSQL (required)
- **Cache**: Redis (required)
- **Message Queue**: Custom event system
- **Data**: Custom time-series storage

**Documentation**: https://docs.jesse.trade/

**Why #3:**
- Modern architecture
- AI strategy generation
- Clean, professional codebase
- Strong futures trading support
- Active development with AI features

---

## 4. **OctoBot**
### Modular Trading Bot Platform

**GitHub**: https://github.com/Drakkar-Software/OctoBot

**GitHub Stats:**
- ⭐ **4,000+ stars**
- 🔄 **Active**: Weekly updates in 2025
- 👥 **Contributors**: 40+
- 🍴 **Forks**: 1,000+

**Key Features:**
- Python-based modular architecture
- Web interface included
- Cloud integration (OctoBot Cloud)
- Strategy marketplace
- Social trading features

**Infrastructure:**
- **Database**: MongoDB
- **Web Server**: Built-in web interface
- **Modules**: Plugin architecture
- **Cloud**: Optional cloud sync

**Documentation**: https://www.octobot.cloud/

**Why #4:**
- User-friendly web interface
- Modular plugin system
- Cloud features
- Strategy sharing marketplace

---

## 5. **Gekko** (Community Forks)
### The Original, Now Community-Maintained

**Original GitHub** (Archived): https://github.com/askmike/gekko

**Active Forks:**
- **Gekko Plus Plus**: https://github.com/gekko-plus-plus/gekko-plus-plus
- **Gekko Strategies**: https://github.com/xFFFFF/Gekko-Strategies

**GitHub Stats:**
- ⭐ **10,000+ stars** (original repo, now archived)
- 🔄 **Semi-Active**: Community forks continue
- 👥 **Historical Impact**: Huge

**Status:**
- Original repo archived in 2021
- Multiple community forks active
- Legacy documentation still valuable

**Key Features:**
- Node.js based
- Simple to understand
- Web UI included
- Paper trading support

**Why Listed:**
- Historical importance
- Large legacy user base
- Simple architecture good for learning
- Community keeps it alive

---

## 6. **Zenbot**
### Lightweight Command-Line Bot

**GitHub**: https://github.com/DeviaVir/zenbot

**GitHub Stats:**
- ⭐ **8,200+ stars**
- 🔄 **Inactive**: Minimal 2025 activity
- 👥 **Legacy Status**

**Status:**
- Development slowed significantly
- Last major update: 2023
- Community maintenance only

**Why Listed:**
- Historical significance
- Simple architecture
- Good for educational purposes

---

## 7. **Other Notable Projects**
### Honorable Mentions Using CCXT Library

**Superalgos** - Visual Strategy Designer
- **GitHub**: https://github.com/Superalgos/Superalgos
- ⭐ **3,500+ stars**
- Visual/node-based strategy creation

**Crypto-signal** - Technical Analysis Bot
- **GitHub**: https://github.com/CryptoSignal/Crypto-Signal
- ⭐ **4,800+ stars**
- TA-focused signal generation

**TradingView Webhooks Bot**
- **GitHub**: https://github.com/robswc/tradingview-webhooks-bot
- ⭐ **1,500+ stars**
- TradingView integration specialist

**CCXT** - The Universal Exchange Library
- **GitHub**: https://github.com/ccxt/ccxt
- ⭐ **32,000+ stars**
- Foundation for many bots (not a bot itself)

---

## Infrastructure Comparison Table

| Engine | Language | Database | Message Queue | Cache | Special Infra |
|--------|----------|----------|---------------|--------|--------------|
| **Freqtrade** | Python | SQLite/PostgreSQL | asyncio | In-memory/Redis | ML frameworks |
| **Hummingbot** | Python/Cython | SQLite/PostgreSQL | Custom events | In-memory | Cython performance |
| **Jesse** | Python | PostgreSQL ✓ | Custom | Redis ✓ | AI/GPT integration |
| **OctoBot** | Python | MongoDB | Plugin system | In-memory | Cloud platform |
| **Gekko** | Node.js | SQLite | EventEmitter | In-memory | Web UI |
| **Nautilus Trader** | Python/Rust | Redis/PostgreSQL | Custom/Redis Streams | Redis | Rust core |

---

## Related Trading Frameworks

### **Nautilus Trader** (Not Crypto-Only)
- **GitHub**: https://github.com/nautechsystems/nautilus_trader
- ⭐ **2,000+ stars**
- Professional multi-asset framework
- Rust performance core
- Extensive backtesting

### **Backtrader** (General Trading)
- **GitHub**: https://github.com/mementum/backtrader
- ⭐ **13,000+ stars**
- Python backtesting library
- Multi-asset support

### **Zipline** (Quantopian's Legacy)
- **GitHub**: https://github.com/stefan-jansen/zipline-reloaded
- ⭐ **1,000+ stars** (reloaded version)
- Professional quant framework

---

## Recommendations for CyberDeltaEngine

Based on this research:

### 1. **Follow Freqtrade's Lead**
- Largest community = best practices validation
- Optional Redis (like you have)
- Focus on modularity and extensibility
- Comprehensive documentation is key
- Study their code: https://github.com/freqtrade/freqtrade/tree/develop/freqtrade

### 2. **Learn from Hummingbot**
- Performance optimization matters (consider Rust like Nautilus)
- Market making focus similar to your arbitrage
- Professional features attract serious users
- Review their architecture: https://github.com/hummingbot/hummingbot/tree/master/hummingbot

### 3. **Consider Jesse's Approach**
- Required PostgreSQL + Redis is similar to your stack
- Clean architecture pays off
- AI integration is becoming standard
- Examine their structure: https://github.com/jesse-ai/jesse/tree/master/jesse

### 4. **Avoid Common Pitfalls**
- Don't require MongoDB (OctoBot) - limits adoption
- Keep infrastructure simple (unlike complex enterprise stacks)
- Focus on crypto-specific features, not general trading

---

## Key Insights for 2025

1. **Python Dominates**: 5 of 6 active engines use Python
2. **Redis Adoption**: Growing trend (Jesse, Freqtrade options)
3. **AI Integration**: New differentiator (Jesse, Freqtrade)
4. **Simple Infrastructure**: Winners avoid complex stacks
5. **Community > Features**: Large communities sustain projects

---

## Your Position

CyberDeltaEngine's infrastructure choices align well with modern trends:
- ✅ Redis (like Jesse, optional in Freqtrade)
- ✅ PostgreSQL (industry standard)
- ✅ Python base (community standard)
- ✅ Focus on specific strategies (like Hummingbot)
- ✅ Avoiding over-engineering (learning from Nautilus)

**Recommendation**: Your infrastructure is on-par with the top projects. Focus on:
1. Documentation (Freqtrade level)
2. Community building (GitHub discussions, Discord)
3. Unique features (your arbitrage focus)
4. Performance optimization where it matters

---

## Resources

### Documentation Sites
- **Freqtrade Docs**: https://www.freqtrade.io/
- **Hummingbot Academy**: https://hummingbot.org/academy/
- **Jesse Docs**: https://docs.jesse.trade/
- **OctoBot Guides**: https://www.octobot.cloud/guides

### Community Links
- **Freqtrade Discord**: https://discord.gg/freqtrade
- **Hummingbot Discord**: https://discord.gg/hummingbot
- **Jesse Discord**: https://discord.gg/jesse
- **Reddit r/algotrading**: https://reddit.com/r/algotrading

### Learning Resources
- **CCXT Documentation**: https://docs.ccxt.com/
- **Awesome Crypto Trading Bots**: https://github.com/botcrypto-io/awesome-crypto-trading-bots
- **TradingView Pine Script**: https://www.tradingview.com/pine-script-docs/

---

## Conclusion

The crypto trading bot ecosystem in 2025 is mature but still evolving. **Freqtrade** leads in community size and features, **Hummingbot** dominates professional market making, and **Jesse** pioneers AI integration.

CyberDeltaEngine is well-positioned with infrastructure choices that match the industry leaders while avoiding the complexity trap that killed many projects. Focus on your unique value proposition (arbitrage), build community, and maintain simplicity.
