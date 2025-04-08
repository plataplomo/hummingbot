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