# Frontend Development Plan

This document outlines the plan and technology considerations for developing a frontend interface for the CyberDeltaEngine trading bot.

## Goals

- Provide real-time monitoring of the bot's status, performance, positions, balances, and logs.
- Offer a user-friendly way to view key metrics (PnL, funding yield, risk exposure).
- (Future) Potentially allow for basic control actions (e.g., start/stop bot, emergency stop).
- (Future) Potentially allow viewing and modifying certain configuration parameters safely.

## Interaction with Backend

The frontend will **not** interact directly with the core trading engine components (`src/core/`). Instead, it will communicate with a dedicated **Backend API Layer** (sometimes called Backend-for-Frontend or BFF). This API layer will act as an intermediary, securely exposing necessary data and control points from the trading bot.

**Communication Methods:**

1.  **REST API:** For fetching non-real-time data (e.g., historical performance, configuration settings) and potentially triggering control actions.
2.  **WebSockets:** For pushing real-time updates from the backend API to the frontend (e.g., live PnL, position changes, ticker updates, logs).

## Proposed Architecture

```mermaid
graph TD
    subgraph User Browser
        FE[Frontend Application (React/Vue/etc.)]
    end

    subgraph Server Infrastructure
        BE_API[Backend API (Python - FastAPI/Flask)]
        subgraph Trading Engine (Separate Process/Container)
            CoreBot(Trading Bot Core - main.py)
            State[(Bot State: PortfolioTracker, etc.)]
            CoreBot -- Updates --> State
            CoreBot -- Reads --> State
        end
    end

    FE -- REST API Calls --> BE_API
    FE -- WebSocket Connection --> BE_API
    BE_API -- Reads State (e.g., via IPC/DB/API) --> State
    BE_API -- Sends Actions --> CoreBot # Via controlled mechanism
    BE_API -- Pushes Real-time Data --> FE # Via WebSocket

    style FE fill:#cff,stroke:#333,stroke-width:2px
    style BE_API fill:#cfc,stroke:#333,stroke-width:2px
    style CoreBot fill:#f9f,stroke:#333,stroke-width:1px
```
*(See `docs/diagrams/frontend_architecture.mermaid` for a more detailed view)*

**Key Principles:**

- **Decoupling:** The frontend is decoupled from the core trading logic via the Backend API.
- **Security:** The Backend API controls what data and actions are exposed, preventing direct manipulation of the core engine.
- **Scalability:** The frontend and backend API can be scaled independently of the trading engine if needed.

## Technology Stack Options

Choosing a modern JavaScript/TypeScript stack is recommended.

1.  **Framework:**
    *   **React (with Vite):** Very popular, large ecosystem, component-based. Good choice if familiar.
    *   **Vue.js (with Vite):** Often considered easier to learn, excellent documentation, performant.
    *   **Svelte (with SvelteKit):** Compiles to vanilla JS, potentially very fast, growing ecosystem.
    *   *Recommendation:* Choose based on team familiarity. **React** or **Vue** are safe bets.

2.  **Language:**
    *   **TypeScript:** Strongly recommended for type safety, improved maintainability, and better tooling, especially for larger applications.

3.  **UI Library / Styling:**
    *   **Material UI (MUI):** Comprehensive set of pre-built React components following Material Design.
    *   **Ant Design:** Another extensive component library for React/Vue.
    *   **Chakra UI:** Component library focused on accessibility and developer experience.
    *   **Tailwind CSS:** Utility-first CSS framework for rapid custom styling (requires more design effort).
    *   *Recommendation:* **MUI** or **Ant Design** provide quick ways to build a functional interface.

4.  **State Management:**
    *   **React:** Zustand (simple), Redux Toolkit (powerful, standard), Jotai/Recoil (atomic).
    *   **Vue:** Pinia (official, simple), Vuex (older standard).
    *   *Recommendation:* **Zustand** for simplicity in React, **Pinia** in Vue.

5.  **Data Fetching / Caching:**
    *   **TanStack Query (React Query / Vue Query):** Excellent for managing server state, caching, background updates for REST data.
    *   **Axios / Fetch API:** Standard libraries for making HTTP requests.
    *   *Recommendation:* **TanStack Query** alongside Axios/Fetch.

6.  **Real-time Data (WebSockets):**
    *   Native `WebSocket` API.
    *   Libraries like `socket.io-client` (if the backend API uses Socket.IO) or lightweight wrappers.
    *   *Recommendation:* Use the native API or a simple wrapper initially.

7.  **Charting:**
    *   **Recharts (React):** Composable charting library.
    *   **Chart.js:** Popular, versatile, works with most frameworks.
    *   **Plotly.js:** Powerful scientific charting library.
    *   *Recommendation:* **Recharts** for React, **Chart.js** otherwise.

## Backend API Layer

A simple Python web framework is suitable:

- **Framework:**
    *   **FastAPI:** Modern, fast, async-native, great for building APIs, automatic docs.
    *   **Flask:** Lightweight, flexible, mature.
- **WebSockets:** `FastAPI` has built-in WebSocket support. `Flask-Sockets` or similar for Flask.
- **Communication with Core Bot:** This needs careful design. Options:
    *   **Inter-Process Communication (IPC):** Queues (`asyncio.Queue`, `multiprocessing.Queue`), shared memory (complex), ZeroMQ.
    *   **Internal API/RPC:** Core bot exposes a minimal internal API (e.g., via `aiohttp` server on localhost) that the Backend API calls.
    *   **Database/Cache:** Core bot writes state changes to Redis/DB, Backend API reads from it.
    *   *Recommendation:* Start with an **internal API** or **Redis pub/sub** for simplicity and decoupling.

## Key Frontend Features (Phased Approach)

**Phase 1: Monitoring Dashboard**
- Display overall Bot Status (Running/Stopped/Error).
- Real-time PnL chart (Session/Daily/Total).
- Current Positions table (Symbol, Exchange, Size, Entry Price, Mark Price, uPnL).
- Current Balances table (Exchange, Asset, Total, Available).
- Log viewer (streaming logs from the backend).
- Key Metrics display (Total Value, Funding Yield, Current VaR vs Limit).

**Phase 2: Basic Controls**
- Start/Stop Bot button.
- Emergency Stop (Cancel all orders, potentially close positions).

**Phase 3: Configuration & Advanced Views**
- View current strategy parameters.
- (Carefully Considered) Edit certain safe parameters (e.g., Kelly Alpha).
- Historical performance view/charts.
- Detailed view of open orders.
- Transfer status monitor.

## Development Plan

1.  **Backend API Design:** Define the specific endpoints and WebSocket events needed for Phase 1 monitoring.
2.  **Backend API Implementation:** Create the FastAPI/Flask application. Implement communication mechanism with the core bot state.
3.  **Frontend Setup:** Choose framework/libraries, set up project structure (e.g., using Vite).
4.  **Frontend Component Development:** Build components for displaying data (Tables, Charts, Log viewer).
5.  **API Integration:** Connect frontend components to the Backend API (REST polling, WebSocket listeners).
6.  **Styling:** Apply UI library styles.
7.  **Testing:** Implement frontend unit and potentially integration/E2E tests (e.g., using Vitest, Testing Library, Cypress/Playwright).
8.  **Deployment:** Plan deployment for both the Backend API and the static frontend build.

This frontend is a separate sub-project and can be developed in parallel once the core bot exposes the necessary data via the chosen communication mechanism.
