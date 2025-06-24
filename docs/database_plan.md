# Database Strategy Plan

This document outlines considerations and plans for using databases within the CyberDeltaEngine trading bot infrastructure.

## Goals for Using Databases

While the core real-time state might be held in memory for performance, databases can serve several crucial purposes:

1.  **Configuration Management:** Storing certain configurations, especially if they need to be updated dynamically without restarting the bot (though `config.yaml` is the primary method currently).
2.  **State Persistence:** Saving critical state (e.g., portfolio tracker snapshots, open orders, last processed event IDs) periodically or on shutdown to allow for graceful recovery after restarts or crashes.
3.  **Historical Data Storage:** Storing market data (ticks, order books, funding rates) and bot performance data (PnL, trades, orders) for:
    *   Backtesting and simulation.
    *   Performance analysis and reporting.
    *   Training future ML models.
4.  **Logging & Auditing:** Storing structured logs or audit trails for detailed debugging, compliance, and post-mortem analysis (complementary to file/console logging).
5.  **Inter-component Communication (Optional):** Using database features like Pub/Sub (Redis) or message queues as part of the communication channel between loosely coupled components (e.g., between the core bot and a separate backend API for a UI).

## Database Technology Options

Different database types are suited for different tasks:

1.  **Time-Series Databases (TSDB):**
    *   **Examples:** InfluxDB, TimescaleDB (PostgreSQL extension).
    *   **Pros:** Optimized for high-volume, time-stamped data (market data, metrics). Efficient querying over time ranges, data compression, retention policies.
    *   **Cons:** Less flexible for relational data or complex queries not based on time.
    *   **Use Cases:** Historical market data, performance metrics (PnL over time), system monitoring data.

2.  **In-Memory Data Stores / Caches:**
    *   **Examples:** Redis.
    *   **Pros:** Extremely fast reads/writes. Good for caching frequently accessed data, managing temporary state, simple Pub/Sub messaging.
    *   **Cons:** Data is typically volatile (lost on restart unless persistence is configured). Not ideal for complex queries or large historical datasets.
    *   **Use Cases:** Caching (e.g., latest order book, tickers), quick state snapshots for recovery, potential communication channel between BFF and Core Bot (Pub/Sub), rate limiting.

3.  **Relational Databases (SQL):**
    *   **Examples:** PostgreSQL, SQLite.
    *   **Pros:** ACID compliant, mature technology, powerful querying with SQL, good for structured data with relationships.
    *   **Cons:** Can be slower for high-frequency writes compared to NoSQL/TSDB. Schema changes can be more involved.
    *   **Use Cases:** Storing configuration, user data (if applicable), structured trade/order logs, potentially audit trails. SQLite is good for simple embedded persistence.

4.  **Document Databases (NoSQL):**
    *   **Examples:** MongoDB.
    *   **Pros:** Flexible schema, good for storing JSON-like documents, scales horizontally well.
    *   **Cons:** Less strict consistency guarantees (tunable), querying can be less powerful than SQL for complex joins.
    *   **Use Cases:** Storing semi-structured logs, potentially configuration or state snapshots where flexibility is key.

## Proposed Approach & Phased Integration

It's generally best to start simple and introduce databases only when their benefits clearly outweigh the added operational complexity.

**Phase 0: No Database (Current State)**
- Core state held in-memory within components (`PortfolioTracker`, `DataHandler`).
- Configuration via `config.yaml`.
- Logging to console and files.
- **Challenge:** State is lost on restart.

**Phase 1: State Persistence & Caching (Optional, Early Consideration)**
- **Technology:** Redis.
- **Integration:**
    - `PortfolioTracker` periodically snapshots key state (balances, positions) to Redis.
    - On startup, `PortfolioTracker` attempts to load state from Redis.
    - Potentially use Redis for caching expensive API lookups or processed data.
    - Potentially use Redis Pub/Sub for communication between Core Bot and a future Backend API (BFF).
- **Benefits:** Faster restarts, potential performance gains via caching, simple communication option.
- **Drawbacks:** Adds Redis dependency and management.

**Phase 2: Historical Data for Analysis & Backtesting**
- **Technology:** Time-Series Database (InfluxDB or TimescaleDB).
- **Integration:**
    - A separate process or within the `DataHandler` / `PortfolioTracker`, data (trades, order book snapshots, PnL updates, funding rates) is written asynchronously to the TSDB.
    - Backtesting/Simulation environment reads data from the TSDB.
    - Future analysis tools or dashboards query the TSDB.
- **Benefits:** Enables robust backtesting, performance analysis, data source for ML.
- **Drawbacks:** Adds TSDB dependency, requires data pipeline implementation.

**Phase 3: Structured Logging/Auditing (If Needed)**
- **Technology:** Relational Database (PostgreSQL) or potentially a Document Database.
- **Integration:** Logging handlers write structured log/audit events (e.g., order placements, cancellations, errors, configuration changes) to the database.
- **Benefits:** Powerful querying for debugging and auditing.
- **Drawbacks:** Adds DB dependency, potential performance impact on logging path if not handled asynchronously.

**Recommendation:** Start with **Phase 0**. Evaluate the need for **Phase 1 (Redis)** based on recovery requirements and potential BFF communication needs early in development. Implement **Phase 2 (TSDB)** once the core bot is functional and the need for backtesting/analysis arises. Defer **Phase 3** unless strict auditing requirements exist.

## Integration Diagram

*(See `docs/diagrams/database_integration.mermaid` for a visual representation of potential integration points.)*

## Fail-Fast, All-or-Nothing Safety Policy for Strategy Processing

**Context:**
CyberDeltaEngine is designed to handle real financial assets and must prioritize safety, correctness, and robust error handling. In early-stage (v0.0.1) operation, partial or silent failures in trading logic are considered unacceptable risks.

**Policy Statement:**
> If any strategy's `update_historical_data` method fails during the processing of a market data batch, the entire `process_market_data` call will immediately raise an exception. No signals will be returned or processed for that batch. This is a deliberate, conservative design to prevent partial execution and ensure that all downstream consumers are protected from inconsistent or incomplete state.

**Rationale:**
- Prevents partial or inconsistent trading actions that could result from undetected strategy failures.
- Ensures that all strategies must be healthy and able to process data for the system to act, reducing the risk of silent data loss or missed risk checks.
- Provides a clear, auditable failure mode for debugging and operational monitoring.

**Scope:**
- This policy is enforced in the `StrategyManager.process_market_data` method.
- Applies to all enabled strategies for a given symbol and market data batch.
- Will be revisited in future versions as the system matures and more granular error handling is validated.

**Reference Implementation:**
- See `cyberdelta/core/strategy_manager.py`, method `process_market_data` (v0.0.1+).

**Future Considerations:**
- As the system evolves, this policy may be relaxed to allow for more granular error handling, but only after comprehensive monitoring and fallback mechanisms are in place.
