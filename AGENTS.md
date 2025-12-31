# Repository Guidelines

## Project Structure & Sources of Truth
- Spot connector: `hummingbot/connector/exchange/backpack/`
- Perpetual connector: `hummingbot/connector/derivative/backpack_perpetual/`
- Candle feeds: `hummingbot/data_feed/candles_feed/backpack_*_candles/`
- Tests: `test/hummingbot/connector/exchange/backpack/`, `test/hummingbot/connector/derivative/backpack_perpetual/`, `test/hummingbot/data_feed/`
- Config: `conf/connectors/backpack_perpetual.yml`
- Backpack API: `logs/openapi_backpack.json` (mirror: `examples/openapi_backpack.json`) and https://docs.backpack.exchange/

## Architecture & Connector Requirements
Follow Hummingbot connector architecture and order lifecycle rules. Always start tracking before REST submission, emit `*OrderCreated`, `OrderFilled`, `*OrderCompleted`, `OrderCancelled/OrderExpired`, and `MarketOrderFailureEvent` in the required order. Use the API throttler for every REST/WS call (`throttler.execute_task(path_url)` with `limit_id` in `*_constants.py`). Use `hummingbot/connector/exchange/binance/` (spot) and `hummingbot/connector/derivative/bybit_perpetual/` (perp) as pattern gold standards.

Required exchange APIs:
- REST: trading rules, server status/ping, active orders, create order, balances, and documented rate limits (per-endpoint and global). Trading pairs list is recommended.
- WebSocket: public orders, public trades, private order updates, private trade events, and documented subscription/global limits. Private balance events and fee details are recommended.
- Perp extras: REST endpoints for positions and leverage configuration.

If no WS balance events, set `_real_time_balance_update = False` and refresh the in-flight snapshot during REST balance updates:
```python
self._in_flight_orders_snapshot = {k: copy.copy(v) for k, v in self._in_flight_orders.items()}
self._in_flight_orders_snapshot_timestamp = self.current_timestamp
```

## Required Components
- Auth (`AuthBase`), utils (fees/config/order IDs), order book (`OrderBook`), order book data source + tracker (`OrderBookTrackerDataSource`), user stream data source + tracker (`UserStreamTrackerDataSource`), and connector (`ExchangeBase` + `ClientOrderTracker`).
- Perp connector must also implement `PerpetualTrading` behaviors: leverage, position modes, funding info, and position status updates.

## Build, Test, and Development Commands
- `make build` or `./compile`: build native extensions.
- `make install` or `./install`: set up local environment.
- `make test`: full pytest run with coverage config.
- `./run_backpack_tests.sh [path]` or `./run_single_test.sh <test_name>`: Backpack perp tests using `.conda`.
- `make development-diff-cover`: diff coverage after tests.

## Coding Style & Naming Conventions
Python, 4-space indentation, 120-char lines. Prefer double quotes, use `snake_case` for modules/functions, `PascalCase` for classes, and keep connector constants in `*_constants.py`.

## Testing Guidelines
Use pytest with `test_*.py`, `Test*`, and `test_*`. All connector components need unit tests that mock exchange I/O: REST via `aioresponses`, WS via `NetworkMockingAssistant`. Maintain the 80% unit-test coverage requirement from `CONTRIBUTING.md`.

## Commit & Pull Request Guidelines
Commit messages use prefixes like `(feat)`, `(fix)`, `(refactor)`, `(cleanup)`, `(doc)` in present tense with a ~70-char summary. Branch from `development` (e.g., `feat/<topic>`), open PRs back to `development`, enable “Allow edits by maintainers,” and include a clear description plus test evidence.
